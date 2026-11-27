"""Incremental, fixed-memory price series.

Every instrument keeps two ring buffers of step CLOSES on a fixed UTC grid:
  g1: 1-minute closes for the last day   (1,440 floats)
  g5: 5-minute closes for the last 35 days (10,080 floats)
plus 5-minute traded volume (quote currency, and the taker-buy part where the venue reports it).
A step's `live` flag says whether the instrument's market was OPEN during it, so quiet or closed
periods can be left out of volatility estimates. Memory is ~0.2 MB per instrument, fixed.

Derived instruments (the computed DXY, the stablecoin share, a stitched bond series) are written to
the same kind of grids from their inputs' grids, so the statistics code treats them identically.
"""
from __future__ import annotations

import math
from array import array
from collections import deque

from . import clock

NaN = float("nan")
G1_STEP, G1_SIZE = 60, 1440
G5_STEP, G5_SIZE = 300, 10080
# The shortest stretch of an open market an answer with no session bar for a symbol is read as "no
# trade" over (a start a few minutes after the open). Over a longer one that is twice the longest the
# symbol itself went without a trade lately (Instrument.quiet_limit_s: UUP's often 50 minutes after the
# open on IEX, SPY's never 5) it is more likely the venue's own gap: a 200 answer without SPY filled a
# 30-minute outage with its 9:59 price, flagged live, and cleared the hole for good.
QUIET_MAX_S = 900.0


def isnum(x) -> bool:
    return x is not None and x == x and not math.isinf(x)


class Grid:
    """Ring buffer of step closes. Step k covers [k*step, (k+1)*step); vals[k] is the last value
    seen in it. Unwritten or invalidated steps read as NaN."""

    __slots__ = ("step", "size", "vals", "live", "last_k")

    def __init__(self, step: int, size: int):
        self.step, self.size = step, size
        self.vals = array("d", [NaN]) * size
        self.live = bytearray(size)
        self.last_k: int | None = None

    def k_of(self, ts: float) -> int:
        return int(ts // self.step)

    def put(self, k: int, value: float, live: bool) -> None:
        last = self.last_k
        if last is not None and k <= last - self.size:
            return                                    # older than the buffer holds
        if last is None or k > last:
            if last is not None and k - last > 1:     # invalidate the skipped steps
                for j in range(last + 1, min(k, last + 1 + self.size)):
                    i = j % self.size
                    self.vals[i] = NaN
                    self.live[i] = 0
            self.last_k = k
        i = k % self.size
        self.vals[i] = value
        self.live[i] = 1 if live else 0

    def get(self, k: int) -> float:
        last = self.last_k
        if last is None or k > last or k <= last - self.size:
            return NaN
        return self.vals[k % self.size]

    def is_live(self, k: int) -> bool:
        last = self.last_k
        if last is None or k > last or k <= last - self.size:
            return False
        return bool(self.live[k % self.size])

    def first_k(self) -> int | None:
        return None if self.last_k is None else self.last_k - self.size + 1

    def asof(self, ts: float, max_back: int = 12) -> float:
        """Close of the latest step that ENDED at or before `ts`, skipping up to `max_back` empty
        steps (NaN = no data)."""
        k = int(ts // self.step) - 1
        for j in range(k, k - max_back - 1, -1):
            v = self.get(j)
            if v == v:
                return v
        return NaN

    def series(self, k0: int, k1: int) -> list[float]:
        """Values for steps k0..k1 inclusive (NaN where unknown)."""
        return [self.get(k) for k in range(k0, k1 + 1)]


def _newest_value(g: Grid, k_now: int) -> int | None:
    """The newest step before `k_now` holding a value."""
    if g.last_k is None:
        return None
    return next((k for k in range(min(g.last_k, k_now - 1), g.last_k - g.size, -1) if g.get(k) == g.get(k)), None)


class VolumeRing:
    """Per-step traded volume aligned with a Grid (quote currency; taker-buy part optional)."""

    __slots__ = ("step", "size", "q", "tb", "last_k")

    def __init__(self, step: int, size: int):
        self.step, self.size = step, size
        self.q = array("d", [NaN]) * size
        self.tb = array("d", [NaN]) * size
        self.last_k: int | None = None

    def put(self, k: int, q: float, tb: float = NaN) -> None:
        last = self.last_k
        if last is not None and k <= last - self.size:
            return
        if last is None or k > last:
            if last is not None and k - last > 1:
                for j in range(last + 1, min(k, last + 1 + self.size)):
                    i = j % self.size
                    self.q[i] = NaN
                    self.tb[i] = NaN
            self.last_k = k
        i = k % self.size
        self.q[i], self.tb[i] = q, tb

    def get(self, k: int) -> tuple[float, float]:
        last = self.last_k
        if last is None or k > last or k <= last - self.size:
            return NaN, NaN
        i = k % self.size
        return self.q[i], self.tb[i]


DAY = 86400
DAILY_KEEP_DAYS = 400                       # a year of daily closes (366 days) and a margin


class Daily:
    """Daily closes of about the last year (and each day's quote volume where the source gives one in
    the instrument's own units), for the 1M window's σ: the 5-minute grid holds 35 days, a month's
    yardstick wants a year. One row per day its market traded, keyed by the UTC date of the bar's
    start (a weekend or a holiday has no row), complete days only. `own`: the bars are the
    instrument's own (not a related symbol's: SPY's for the SPY perpetual), so their volume compares
    with its other windows'."""
    __slots__ = ("rows", "fetched_at", "source", "own")

    def __init__(self):
        self.rows: dict[int, tuple[float, float]] = {}      # day -> (close, quote volume or NaN)
        self.fetched_at: float | None = None
        self.source: str | None = None
        self.own = True

    def merge(self, rows, now: float) -> int:
        """rows: [(bar_start_ts, close, quote_volume)] in any order; a bar whose day has not ended by
        `now` is left out (its close is not final). Returns the rows taken."""
        n = 0
        for t, c, *rest in rows:
            if not (isnum(c) and c > 0) or t + DAY > now:
                continue
            q = rest[0] if rest and isnum(rest[0]) else NaN
            self.rows[int(t // DAY)] = (float(c), float(q))
            n += 1
        old = int(now // DAY) - DAILY_KEEP_DAYS
        for d in [d for d in self.rows if d < old]:
            del self.rows[d]
        return n

    def newest(self) -> int | None:
        return max(self.rows) if self.rows else None

    def returns(self, now: float, days: int = 366) -> list[float]:
        """Log returns of consecutive daily closes over the last `days` calendar days (each row to
        the next one: a weekend or a holiday between two sessions is one move, as the market made it)."""
        d0 = int(now // DAY) - days
        ks = sorted(d for d in self.rows if d >= d0)
        out = []
        for a, b in zip(ks, ks[1:]):
            pa, pb = self.rows[a][0], self.rows[b][0]
            out.append(math.log(pb / pa))
        return out

    def volume(self, d0: int, d1: int) -> tuple[float, int]:
        """Sum of the quote volumes of the days in [d0, d1) and how many days had one."""
        s, n = 0.0, 0
        for d in range(d0, d1):
            r = self.rows.get(d)
            if r is not None and isnum(r[1]):
                s += r[1]
                n += 1
        return s, n


class Instrument:
    """One price series from one source (e.g. binance_fut:QQQUSDT index price)."""

    computed = False                        # its price is recomputed from other instruments (Derived)

    def __init__(self, key: str, source: str, symbol: str, hours: str = "24/7", *,
                 volume: bool = False, label: str | None = None, delayed_s: float = 0.0):
        self.key, self.source, self.symbol = key, source, symbol
        self.label = label or symbol
        self.hours = hours                  # market-hours profile, see clock.market_open
        self._shuts = clock.has_shut_hours(hours)
        self.calendar = clock.default_calendar(hours)   # days comparable for σ, see clock.CALENDARS
        self.delayed_s = delayed_s          # known publication delay of the source (e.g. 900 s)
        self.g1 = Grid(G1_STEP, G1_SIZE)
        self.g5 = Grid(G5_STEP, G5_SIZE)
        self.v5 = VolumeRing(G5_STEP, G5_SIZE) if volume else None
        self.daily = Daily()                # a year of daily closes: the 1M window's σ
        self.price = NaN
        self.ts: float | None = None        # exchange timestamp of the latest price
        self.recv_ts: float | None = None   # local (NTP-corrected) receive time
        self.extra: dict = {}               # bid/ask, mark/index, funding, open interest ...
        self.lat_ms: deque = deque(maxlen=400)
        self.msgs = 0
        self._live_seen = False             # a LIVE message arrived this session (cache/history do not count)
        # the newest moment real data covered (a message for it, or its feed's heartbeat): a step its
        # feed did not cover (a sleep, a network outage) is unknown, never the old price carried as live
        self._seen_ts = NaN
        self.holes: dict[str, float | None] = {"g1": None, "g5": None}   # first such step, until back-filled
        self._gap_open = False              # the last closed step was uncovered...
        self.recovered = False              # ...and since then one was covered: the feed is back
        # the first open minute its feed missed since its price was last current (a print, or for an
        # ETF a download that covered the gap): the close after it is unknown (roll). Any market with
        # shut hours: an ETF's sessions, the FX legs' week
        self._stale_from: float | None = None
        self._k1 = None                     # open (not yet closed) 1-minute step
        self._k5 = None                     # open 5-minute step
        self._open_any = False              # did the open steps see a real update?
        self._vol_min: dict[int, tuple[float, float]] = {}   # minute -> (quote vol, taker-buy quote)
        self._vol_acc: dict[int, list] = {}                   # minute -> [q, tb] from trades
        self._quiet: tuple = (None, QUIET_MAX_S)              # (day, quiet_limit_s), worked out once a day

    # ── live updates ────────────────────────────────────────────────────────────────────────
    def on_price(self, price: float, ev_ts: float | None, recv_ts: float, measure: bool = True) -> None:
        """A live price. `measure=False` for polled values (a REST "latest trade" can be minutes old
        on a thin ETF: that is data age, not network latency, so it must not enter the latency stats)."""
        if not isnum(price) or price <= 0:
            return
        ev = ev_ts if ev_ts is not None else recv_ts
        if self.hours == "us_regular" and not clock.market_open("us_regular", ev):
            # pre/post-market prints on IEX are thin: keep them aside, the series stays on the session
            self.extra["ext_price"], self.extra["ext_ts"] = price, ev
            self.recv_ts = recv_ts
            self.msgs += 1
            self.feed_alive(recv_ts)
            return
        if self._k1 is not None and int(ev // G1_STEP) > self._k1:
            self.roll(ev)                   # close finished steps with the previous price
        if self.ts is not None and ev < self.ts - 5:
            return                          # a stale, out-of-order message
        self.price = price
        self.ts = ev
        self.recv_ts = recv_ts
        self.msgs += 1
        if not self._live_seen and any(h is not None for h in self.holes.values()):
            # the first live price after a start whose download failed: the network is back, so the
            # back-fill's back-off (up to 15 min) is cut short, as after any outage
            self.recovered = True
        self._live_seen = True
        self._stale_from = None             # a session print: the price is current again
        self.feed_alive(recv_ts)
        if ev_ts is not None and measure:
            lat = (recv_ts - ev_ts) * 1000.0
            if lat <= 60_000:
                self.lat_ms.append(lat)
            else:                           # a late-reported print (e.g. an IEX trade reported minutes later)
                self.extra["late_prints"] = self.extra.get("late_prints", 0) + 1
        if self._k1 is None:
            self._k1, self._k5 = int(ev // G1_STEP), int(ev // G5_STEP)
        self._open_any = True

    @property
    def live_seen(self) -> bool:
        return self._live_seen

    def feed_alive(self, ts: float | None) -> None:
        """Real data, or the feed's own heartbeat, reached us at `ts` (local, NTP-corrected)."""
        if ts is not None and not (self._seen_ts >= ts):
            self._seen_ts = ts

    def heard_until(self, ts: float) -> None:
        """Its feed counts as heard until `ts` at most: after a sleep, what came in since the wake says
        nothing about the time asleep (Engine.tick, the first tick after it)."""
        if self._seen_ts > ts:
            self._seen_ts = ts

    def _hole(self, grid: str, t0: float) -> None:
        h = self.holes.get(grid)
        if h is None or t0 < h:
            self.holes[grid] = t0

    def note_message(self, recv_ts: float, ev_ts: float | None = None) -> None:
        """A message that proves the feed is alive without a new price (e.g. an unchanged quote)."""
        self.recv_ts = recv_ts
        self.msgs += 1
        self.feed_alive(recv_ts)
        if ev_ts is not None:
            self.lat_ms.append((recv_ts - ev_ts) * 1000.0)

    def on_minute_volume(self, minute_start: float, quote_vol: float, taker_buy_quote: float = NaN) -> None:
        """Cumulative volume of the minute starting at `minute_start` (kline style: replace)."""
        if self.v5 is None:
            return
        self._vol_min[int(minute_start // G1_STEP)] = (quote_vol, taker_buy_quote)

    def on_trade_volume(self, ts: float, notional: float, taker_buy: float = NaN) -> None:
        """Incremental volume from a trade (trade style: add)."""
        if self.v5 is None:
            return
        k = int(ts // G1_STEP)
        acc = self._vol_acc.setdefault(k, [0.0, 0.0 if taker_buy == taker_buy else NaN])
        acc[0] += notional
        if taker_buy == taker_buy and acc[1] == acc[1]:
            acc[1] += taker_buy

    # ── step closing ────────────────────────────────────────────────────────────────────────
    def roll(self, now: float) -> None:
        """Close every 1- and 5-minute step that ended before `now` (carry forward when idle)."""
        k1_now = int(now // G1_STEP)
        if self._k1 is None:
            if self.price == self.price:
                self._k1, self._k5 = k1_now, int(now // G5_STEP)
                self._carry_shut(now)
            return
        if k1_now <= self._k1:
            return
        p = self.price
        # A carried price counts as trading only once live data has arrived this session. Carrying a
        # cached or downloaded close through market hours (TLT without an Alpaca key) is not trading:
        # flagged live, it froze the bonds node at that close for the whole US session instead of
        # letting it follow its 24/7 proxy.
        traded = self._live_seen
        # ...and only for a step its feed covered: data or a heartbeat at or after the step's start. A
        # step that passed while the PC slept or the network was down is UNKNOWN (NaN, not live) until
        # it is back-filled from the exchange. Carried as live, the 25 Nov night (Bitcoin flat for
        # 26 steps) made σ too low and every Binance series differ from the exchange.
        seen = self._seen_ts
        # That holds before its first live price too, once its feed has reported this run (any message:
        # trades, or the stream's login on a weekend): an ETF started on a weekend or before the open
        # whose PC then slept over one or more sessions had them carried from the cache's close, not
        # live but with no gap, so they were never back-filled (TODAY and 1H +3.0% where the truth was
        # 0, after a sleep from a Monday's pre-market to the Friday). With no feed at all (TLT without
        # a key) its steps are carried, as above.
        fed = traded or seen == seen
        # before any live data, a step after an unfilled hole would only carry the CACHE's last close
        # (the app was off, the first download failed): unknown too, not that old price
        h1, h5 = self.holes["g1"], self.holes["g5"]
        # A session minute its feed missed leaves an ETF's price stale: the close after it is unknown,
        # so the shut steps after it are too (not live, no gap: the session's hole is the gap), until a
        # session print or a download that covers the gap (seed). Carried, they held the price from
        # before the gap all night; with an answer that had no bar for the ETF they stayed, and TODAY
        # read the move since the gap began, status live, until 17:00 New York. Found before both
        # loops: after a sleep of over a day the 1-minute loop starts past the first missed session.
        # The same for any market with shut hours: an FX leg that slept over Friday's close holds an
        # older price than the close all weekend (its dollar takes ICE's close then: DXY).
        if fed and seen == seen and self._stale_from is None and self._shuts:
            m = max(self._k1, int(seen // G1_STEP) + 1)
            while m < k1_now:
                if clock.market_open(self.hours, (m + 1) * G1_STEP - 1):
                    self._stale_from = m * G1_STEP
                    break
                m += 5 if (m * G1_STEP) % G5_STEP == 0 else 1     # (sessions open on 5-minute steps)
        start = max(self._k1, k1_now - G1_SIZE)
        for k in range(start, k1_now):
            t0 = k * G1_STEP
            open_ = clock.market_open(self.hours, t0 + G1_STEP - 1)
            if fed and open_ and not (seen >= t0):
                self.g1.put(k, NaN, False)
                self._hole("g1", t0)
                self._gap_open = True
                continue
            if self._gap_open and open_:
                # its feed is back, traded or not yet (a step after the hole stays unknown until then:
                # below), so the back-fill's back-off is cut short
                self._gap_open, self.recovered = False, True
            if open_ and not traded and h1 is not None and t0 >= h1:
                self.g1.put(k, NaN, False)
            elif not open_ and self._stale_from is not None and t0 > self._stale_from:
                self.g1.put(k, NaN, False)
            else:
                self.g1.put(k, p, traded and open_)
        k5_now = int(now // G5_STEP)
        if k5_now > self._k5:
            for k in range(max(self._k5, k5_now - G5_SIZE), k5_now):
                t0 = k * G5_STEP
                open_ = clock.market_open(self.hours, t0 + G5_STEP - 1)
                stale = open_ and not traded and h5 is not None and t0 >= h5
                covered = not stale and not (fed and open_ and not (seen >= t0))
                if covered and not open_ and self._stale_from is not None and t0 > self._stale_from:
                    self.g5.put(k, NaN, False)          # the close after a missed session minute (above)
                elif covered:
                    self.g5.put(k, p, traded and open_)
                else:
                    self.g5.put(k, NaN, False)
                    if not stale:
                        self._hole("g5", t0)
                if self.v5 is not None:
                    self._close_volume(k, covered)
            self._k5 = k5_now
        self._k1 = k1_now
        self._open_any = False

    def _carry_shut(self, now: float) -> None:
        """The first roll after a start: the price (the cache's last close) over the steps its market
        was shut in from the grids' newest value on, as the roll writes them while it runs. Up to the
        first open step (a hole from loading the cache, for the download): nothing traded before it.
        Left empty, a start-up download that failed until after the first 5-minute roll never filled
        them: its retries start from the newest value, the start itself, and nothing marks shut hours
        as missing. A Monday 7:30 start kept the weekend empty for good, every ETF's TODAY unknown."""
        g5, g1 = self.g5, self.g1
        k5_now = int(now // G5_STEP)
        kv = _newest_value(g5, k5_now)
        if kv is None:
            return
        k = kv + 1
        while k < k5_now and not clock.market_open(self.hours, (k + 1) * G5_STEP - 1):
            g5.put(k, self.price, False)
            k += 1
        # the 1-minute grid over the same shut stretch (sessions open and close on 5-minute steps), and
        # on through the minutes of the 5-minute step under way when it is shut too: the roll goes on
        # from this minute (a start at 15:36 left the 15:35 minute empty)
        k1_now, k1_end = int(now // G1_STEP), k * (G5_STEP // G1_STEP)
        if k == k5_now:
            while k1_end < k1_now and not clock.market_open(self.hours, (k1_end + 1) * G1_STEP - 1):
                k1_end += 1
        k1v = _newest_value(g1, k1_now)
        k1 = max((kv + 1) * (G5_STEP // G1_STEP), k1_now - G1_SIZE + 1, -1 if k1v is None else k1v + 1)
        for j in range(k1, min(k1_end, k1_now)):
            g1.put(j, self.price, False)

    def _close_volume(self, k5: int, covered: bool = True) -> None:
        m0 = k5 * (G5_STEP // G1_STEP)
        q = tb = 0.0
        seen = False
        tb_known = True
        for m in range(m0, m0 + G5_STEP // G1_STEP):
            if m in self._vol_min:
                a, b = self._vol_min.pop(m)
                q += a
                seen = True
                if b == b:
                    tb += b
                else:
                    tb_known = False
            if m in self._vol_acc:
                a, b = self._vol_acc.pop(m)
                q += a
                seen = True
                if b == b:
                    tb += b
                else:
                    tb_known = False
        if seen:
            self.v5.put(k5, q, tb if tb_known else NaN)
        elif covered and self._live_seen and clock.market_open(self.hours, (k5 + 1) * G5_STEP - 1):
            self.v5.put(k5, 0.0, 0.0)       # a quiet step on a live feed (no feed: unknown, not zero)
        # drop anything older than this step (late messages)
        for d in (self._vol_min, self._vol_acc):
            for m in [m for m in d if m < m0]:
                del d[m]

    # ── history ─────────────────────────────────────────────────────────────────────────────
    def seed(self, grid: str, closes: list[tuple[float, float]], until: float,
             volumes: list[tuple[float, float, float]] | None = None, since: float | None = None,
             last_trade: float | None = None) -> int:
        """Load history: `closes` = [(step_start_ts, close)], sorted. Fills from the first close up to
        `until` with carry-forward; the live flag follows the market-hours profile. `since`: where a
        download from a venue with bars only for steps that traded (IEX) began: no bar between there
        and its first one means no trade (the last close carried), not a gap to fetch again; that
        holds for an answer with no bar at all too (a weekend, a holiday, before an ETF's first print),
        over shut hours and over open ones up to the symbol's own limit (quiet_limit_s), or of any
        length from after `last_trade` (the venue's own latest trade of the symbol, asked after the
        answer: nothing traded since)."""
        g = self.g1 if grid == "g1" else self.g5
        if not closes and since is None:
            return 0
        step = g.step
        k_until = int(until // step)
        k0 = int(closes[0][0] // step) if closes else k_until
        k_first = max(k0, k_until - g.size)
        # from where the download began: a cache saved on a Saturday left the weekend empty until
        # Monday's first bar (TODAY unknown all Monday)
        k_from = k_first if since is None else max(min(k_first, -int(-since // step)), k_until - g.size)
        by_k = {int(t // step): c for t, c in closes if isnum(c) and c > 0}
        # An ETF's grids hold its regular session, as its live series does: a pre- or post-market bar
        # (IEX, thin) is kept aside, and the session's last close is carried over those hours. Taken
        # as closes, Friday evening's prints were carried all weekend and became Monday's TODAY
        # reference (0.46% off a price that stays on the session).
        session = self.hours == "us_regular"
        # No session bar at all for the symbol: "no trade" holds for shut hours and for a stretch of an
        # open market up to the symbol's own limit (quiet_limit_s). A longer one with no value of its
        # own is left unknown, and so is what follows it up to the grid's next value of its own (the
        # close it would carry, also into the shut hours after it, is unknown); the hole is from the
        # first such stretch, for the back-fill (which gives up on it if the next answer is the same).
        # A stretch reaching the download's end runs on to `until` when that is open: both grids then
        # measure the same time (the 1-minute one said unknown at 16 minutes, the 5-minute one saw 15).
        unknown: set[int] = set()
        empty = since is not None and not any(k_from <= k < k_until and (not session or clock.market_open(
            self.hours, (k + 1) * step - 1)) for k in by_k)
        if empty:
            limit, start = None, None
            for k in range(k_from, k_until + 1):
                if k < k_until and g.get(k) != g.get(k) and clock.market_open(self.hours, (k + 1) * step - 1):
                    start = k if start is None else start
                    continue
                if start is not None:
                    end = k * step
                    if k == k_until and clock.market_open(self.hours, until - 1e-6):
                        end = until
                    # (the venue's latest trade came before the stretch: it did not trade in it. A
                    # thin ETF that traded every minute lately and first printed at 10:00 got a hole
                    # for 9:30-9:50 at a 9:50 start, one more download and "no exchange data")
                    quiet = last_trade is not None and last_trade < start * step
                    if end - start * step > QUIET_MAX_S and not quiet:
                        limit = self.quiet_limit_s() if limit is None else limit
                        if end - start * step > limit:
                            unknown.add(start)
                start = None
        last, k_last = NaN, None
        for k in range(k_from - 1, k0 - 1, -1):            # the carry value: the newest close before
            if k in by_k and (not session or clock.market_open(self.hours, (k + 1) * step - 1)):
                last, k_last = by_k[k], k
                break
        if (session or since is not None) and last != last and g.last_k is not None:
            # or the grid's own (that session's close, carried): the first bars can be the morning's
            # pre-market ones, or come hours after the download's start
            last = g.asof(k_from * step, max_back=g.size)
        if session and last != last and grid == "g1":
            last = self.g5.asof(k_from * step, max_back=12)   # an empty 1-day grid: the 5-minute one's
        # Shut hours in the 1-minute grid as the 5-minute one has them, before this answer's first bar:
        # its download reaches back to the gap, this one is a day long. After a wake in a session a day
        # or more after an ETF's close the 1-minute grid kept the pre-sleep price the roll had carried
        # over the time off, swapped price or not: TODAY read the move to the close slept over all
        # session, and 1H till 10:30. (Not for a computed series: it keeps its own steps. After the
        # carry above is taken: across the empty open minutes of an empty grid it would reach an
        # older close than the 5-minute grid's.)
        from_g5 = grid == "g1" and not self.computed
        if from_g5:
            for k in range(k_until - g.size + 1, min(k_from, k_first)):
                self._shut_from_g5(k)
        n, k_ext, blocked = 0, None, False
        for k in range(k_from, k_until):
            open_ = clock.market_open(self.hours, (k + 1) * step - 1)
            blocked = blocked or k in unknown
            if k in by_k:
                if open_ or not session:
                    last, k_last = by_k[k], k
                else:
                    k_ext = k
            elif k < k_first:
                v = self._shut_from_g5(k, open_) if from_g5 else g.get(k)
                if v == v:                           # up to the first bar the cache's own steps stay
                    last, blocked = v, False
                    continue
            if last == last and not blocked:
                # (an empty answer's "no trade" over an open market is ours, not the exchange's: not live,
                # so it never counts as a quiet stretch of the symbol's own)
                g.put(k, last, open_ and not empty)
                n += 1
        if session and last == last and not blocked and g.last_k is not None:
            # the shut steps the roll closed during the download, after its end, while the price was
            # stale (roll: unknown), hold the close the download ends on (left empty, one minute closed
            # while a wake's download ran stayed so for good)
            for k in range(k_until, g.last_k + 1):
                if clock.market_open(self.hours, (k + 1) * step - 1):
                    break
                if g.get(k) != g.get(k):
                    g.put(k, last, False)
        if k_ext is not None and (k_ext + 1) * step > (self.extra.get("ext_ts") or 0):
            self.extra["ext_price"], self.extra["ext_ts"] = by_k[k_ext], (k_ext + 1) * step
        if grid == "g5" and volumes and self.v5 is not None:
            for t, q, tb in volumes:
                k = int(t // step)
                if k < k_until:
                    self.v5.put(k, q, tb)
        h = self.holes.get(grid)
        # covered: the download began at or before the gap (`since`: whatever IEX has no bar for did
        # not trade, so a thin ETF's first trade after the start, or none at all, is no gap) and ends
        # after its start; or the data starts at the gap, or at the oldest step the grid holds
        # (anything older falls out of it anyway: a 1-day grid's first minute is never downloaded again)
        covered = h is not None and (
            (since is not None and h < until and since <= max(h, (k_until - g.size + 2) * step))
            or (by_k and max(by_k) * step >= h
                and (k_first * step <= h or k_first <= k_until - g.size + 2)))
        if unknown:                          # the gap is from what this answer left unknown
            t_u = min(unknown) * step
            self.holes[grid] = t_u if h is None or covered else min(h, t_u)
        elif covered:
            # the exchange's own data now covers the gap. A step the roll closed during the download,
            # after its end, is still empty if the feed missed it or, before the first live price, if
            # it came after the gap: the next attempt's (cleared with the gap, a minute closed while a
            # wake's download ran stayed empty for good). Not without a feed (no key: carried as before)
            nxt = None
            if self._seen_ts == self._seen_ts and not self.computed and g.last_k is not None:
                nxt = next((k * step for k in range(k_until, g.last_k + 1)
                            if g.get(k) != g.get(k) and clock.market_open(self.hours, (k + 1) * step - 1)), None)
            self.holes[grid] = nxt
            if session:
                self._stale_from = nxt          # its price is current again (stale from that next gap)
        # the price is the newest close known: a downloaded bar newer than it takes over, also after
        # live data (a sleep or an outage over an ETF's close left the pre-outage price all night and
        # weekend: nothing after the close corrects it). Only a bar the grid took (before `until`)
        # counts: one still open would date the price in the future, and live prices older than that
        # would be dropped as out of order. For an ETF, its session's. Not for a computed series once
        # it has a price: the next tick computes it from its inputs again (the dollar from FX legs a
        # sleep left stale), and what was carried from it would differ from the price by their move.
        if k_last is not None:
            t_last = (k_last + 1) * step
            if self.price != self.price:             # (also with no bar inside the grid: the last close)
                self.price, self.ts = by_k[k_last], t_last
            elif k_last >= k_from and not self.computed and (self.ts is None or t_last > self.ts):
                old = self.price
                self.price, self.ts = by_k[k_last], t_last
                if old != self.price:
                    self._recarry(old, t_last)
        return n

    def _shut_from_g5(self, k: int, open_: bool | None = None) -> float:
        """The 1-minute step k, set to the 5-minute grid's close when its market was shut then (seed: the
        5-minute grid's download reaches back to the gap; a shut step is never live); its value."""
        g = self.g1
        v = g.get(k)
        if open_ is None:
            open_ = clock.market_open(self.hours, (k + 1) * G1_STEP - 1)
        if not open_:
            v5 = self.g5.get(k // (G5_STEP // G1_STEP))
            if v5 == v5 and v5 != v:
                g.put(k, v5, False)
                return v5
        return v

    def quiet_limit_s(self) -> float:
        """How long its open market can go without a trade before an answer with no bar for it is more
        likely the venue's gap than no trade: twice the longest run of session steps without one in the
        35 days its 5-minute grid holds, QUIET_MAX_S at least. A step without a trade: live (a bar's
        session, or a live feed's), no traded volume (none, or 0) and its close unchanged. An empty
        answer's own "no trade" fill is not live, so it never counts (each one raised the limit: SPY's
        went from 15 to 20 and 30 minutes over two); nor does a price that held while it traded (HYG got
        50 minutes from six equal closes, five of them with volume). On IEX, UUP often prints nothing
        for 50 minutes after the open; in this PC's cache SPY never went a 5-minute step without a
        trade, HYG one. A fixed 15 minutes left UUP's quiet opening unknown, fetched it again and gave
        it up."""
        g, vol = self.g5, self.v5
        if g.last_k is None:
            return QUIET_MAX_S
        day = g.last_k // (86400 // G5_STEP)
        if self._quiet[0] == day:
            return self._quiet[1]
        best = run = 0
        prev = NaN
        for k in range(g.first_k(), g.last_k + 1):
            v = g.get(k)
            if not g.is_live(k):                   # shut (the close carried) or not traded: a new run
                run, prev = 0, v
                continue
            traded = v != prev or (vol is not None and vol.get(k)[0] > 0)
            run = 0 if traded else run + 1
            best = max(best, run)
            prev = v
        self._quiet = (day, max(QUIET_MAX_S, 2.0 * best * g.step))
        return self._quiet[1]

    def _recarry(self, old: float, after: float) -> None:
        """A replaced price, in the steps carried from it, from the end of the close that replaced it
        (`after`) on: the new close. Nothing traded after that close (the newest the download has;
        the old price is older), so a step from there holding the old price, not live, carried it: the
        roll (each tick), or a seed's carry from the grid's own steps. From the download's end only,
        a sleep over a close that woke over a day later kept the old price in every 1-minute step
        (its download is one day long: no session bar), and TODAY and 1H read the move to the close.
        Live steps are left: their feed covered them."""
        for g in (self.g1, self.g5):
            if g.last_k is not None:
                for k in range(int(after // g.step), g.last_k + 1):
                    if g.get(k) == old and not g.is_live(k):
                        g.put(k, self.price, False)

    # ── summaries ───────────────────────────────────────────────────────────────────────────
    def latency_ms(self) -> float | None:
        if not self.lat_ms:
            return None
        xs = sorted(self.lat_ms)
        return xs[len(xs) // 2]

    def age(self, now: float) -> float | None:
        return None if self.ts is None else max(0.0, now - self.ts)


class Derived(Instrument):
    """An instrument computed from other instruments (written at each step close)."""

    computed = True

    def __init__(self, key: str, inputs: list[Instrument], hours: str = "24/7", label: str | None = None):
        super().__init__(key, "computed", key, hours, label=label)
        self.inputs = inputs

    def value_live(self, now: float) -> float:
        raise NotImplementedError

    def value_step(self, grid: str, k: int) -> float:
        raise NotImplementedError

    def inputs_live(self, grid: str, k: int) -> bool:
        """A computed step is live only if what it is computed from was (a step its inputs carried
        through a sleep or an outage is not trading, whatever the market hours say)."""
        return all(getattr(i, grid).is_live(k) for i in self.inputs)

    def refresh(self, now: float) -> None:
        """Recompute the live value and close finished steps from the inputs' grids."""
        k1_now = int(now // G1_STEP)
        if self._k1 is not None and k1_now > self._k1:
            k5_now = int(now // G5_STEP)
            if k5_now > self._k5:            # 5-minute steps first: a Stitched 1-minute chain anchors on them
                for k in range(max(self._k5, k5_now - G5_SIZE), k5_now):
                    v = self.value_step("g5", k)
                    if isnum(v):
                        self.g5.put(k, v, clock.market_open(self.hours, (k + 1) * G5_STEP - 1)
                                    and self.inputs_live("g5", k))
                self._k5 = k5_now
            for k in range(max(self._k1, k1_now - G1_SIZE), k1_now):
                v = self.value_step("g1", k)
                if isnum(v):
                    self.g1.put(k, v, clock.market_open(self.hours, (k + 1) * G1_STEP - 1)
                                and self.inputs_live("g1", k))
        if self._k1 is None or k1_now > self._k1:
            self._k1, self._k5 = k1_now, int(now // G5_STEP)
        v = self.value_live(now)
        if isnum(v):
            ts = [i.ts for i in self.inputs if i.ts is not None]
            self.price = v
            self.ts = min(ts) if ts else now       # as old as the OLDEST input (honest age)
            rts = [i.recv_ts for i in self.inputs if i.recv_ts is not None]
            self.recv_ts = max(rts) if rts else now

    def rebuild(self, until: float, since: float | None = None) -> None:
        """Recompute both grids from the inputs' grids (after history was seeded), from `since` if given
        (a back-fill: only what it refilled; the whole 35 days took seconds of the event loop). The
        5-minute grid goes first: series that chain from step to step (Stitched) anchor the 1-day
        1-minute grid on it, so the two grids agree."""
        for gname, g in (("g5", self.g5), ("g1", self.g1)):
            firsts = [getattr(i, gname).first_k() for i in self.inputs if getattr(i, gname).last_k is not None]
            if not firsts:
                continue
            k0 = max(min(firsts), int(until // g.step) - g.size)
            if since is not None:
                k0 = max(k0, int(since // g.step) - 1)
            for k in range(k0, int(until // g.step)):
                v = self.value_step(gname, k)
                if isnum(v):
                    g.put(k, v, clock.market_open(self.hours, (k + 1) * g.step - 1) and self.inputs_live(gname, k))
        self._k1, self._k5 = int(until // G1_STEP), int(until // G5_STEP)
        v = self.value_live(until)
        if isnum(v):
            self.price = v
            ts = [i.ts for i in self.inputs if i.ts is not None]
            self.ts = min(ts) if ts else until

    def latency_ms(self) -> float | None:
        lats = [i.latency_ms() for i in self.inputs]
        lats = [x for x in lats if x is not None]
        return max(lats) if lats else None

    @property
    def live_seen(self) -> bool:
        return all(i.live_seen for i in self.inputs)


class DXY(Derived):
    """ICE U.S. Dollar Index formula on six FX pairs:
    DXY = 50.14348112 * EURUSD^-0.576 * USDJPY^0.136 * GBPUSD^-0.119 * USDCAD^0.091 * USDSEK^0.042 * USDCHF^0.036
    `pairs` maps EURUSD/USDJPY/GBPUSD/USDCAD/USDSEK/USDCHF to their instruments."""

    WEIGHTS = {"EURUSD": -0.576, "USDJPY": 0.136, "GBPUSD": -0.119, "USDCAD": 0.091, "USDSEK": 0.042, "USDCHF": 0.036}
    CONST = 50.14348112

    def __init__(self, key: str, pairs: dict[str, Instrument], label: str = "DXY (computed)"):
        missing = set(self.WEIGHTS) - set(pairs)
        if missing:
            raise ValueError(f"DXY needs {sorted(missing)}")
        self.pairs = pairs
        self._seeds = 0                         # history seeds so far (own_close's cache)
        self._own: tuple = (None, NaN)
        super().__init__(key, list(pairs.values()), hours="fx", label=label)

    def seed(self, grid, closes, until, volumes=None, since=None, last_trade=None) -> int:
        self._seeds += 1
        return super().seed(grid, closes, until, volumes, since, last_trade)

    def legs_stale(self, now: float) -> bool:
        """FX is shut and a leg's price is from before an open minute its feed missed (a sleep over
        Friday's 17:00 close): the legs' prices are not the close, all weekend (they print again on
        Sunday at 17:00)."""
        return not clock.market_open(self.hours, now) and any(i._stale_from is not None for i in self.inputs)

    def _newest_close(self, now: float) -> tuple[float, float]:
        """(when, value): the newest close in either of its own grids before `now` (the 1-minute grid
        holds a day: after a longer sleep its newest value is older than the 5-minute grid's)."""
        best, v = -1.0, NaN
        for g in (self.g1, self.g5):
            k = _newest_value(g, int(now // g.step))
            if k is not None and (k + 1) * g.step > best:
                best, v = (k + 1) * g.step, g.get(k)
        return best, v

    def own_close(self, now: float) -> float:
        """The newest close of its own grids before `now`: ICE's once downloaded, then held over the
        shut hours. (Worked out again when its grids change, or once a minute.)"""
        key = (self.g1.last_k, self.g5.last_k, self._seeds, int(now // G1_STEP))
        if self._own[0] != key:
            self._own = (key, self._newest_close(now)[1])
        return self._own[1]

    def hold_close(self, now: float) -> int:
        """After ICE's history is seeded: the unknown shut steps from its newest close up to `now` hold
        that close (not live), in both grids. A sleep over Friday's FX close left them unknown (the
        legs' prices are stale: Instrument.roll), and the steps it closed meanwhile are not computed
        again. Never across an open step: what traded there is unknown. Returns the steps written."""
        t, v = self._newest_close(now)
        if v != v:
            return 0
        n = 0
        for g in (self.g5, self.g1):
            k_now = int(now // g.step)
            k = -int(-t // g.step)                  # the first step from the close on
            while k < k_now and not clock.market_open(self.hours, (k + 1) * g.step - 1):
                if k > k_now - g.size and g.get(k) != g.get(k):
                    g.put(k, v, False)
                    n += 1
                k += 1
        return n

    @classmethod
    def formula(cls, px: dict[str, float]) -> float:
        v = cls.CONST
        for name, w in cls.WEIGHTS.items():
            p = px.get(name)
            if not isnum(p) or p <= 0:
                return NaN
            v *= p ** w
        return v

    def value_live(self, now: float) -> float:
        # FX shut, a leg's price older than the close (legs_stale): the dollar's own close, ICE's once
        # downloaded. From the legs, a sleep from Friday 15:00 over the 17:00 close read -0.30% all
        # weekend against ICE's close 0.3% higher (TODAY from Friday 17:00, 1W, 1M).
        if self.legs_stale(now):
            v = self.own_close(now)
            if isnum(v):
                return v
        return self.formula({n: i.price for n, i in self.pairs.items()})

    def value_step(self, grid: str, k: int) -> float:
        v = self.formula({n: getattr(i, grid).get(k) for n, i in self.pairs.items()})
        if v != v:
            g = getattr(self, grid)
            if not clock.market_open(self.hours, (k + 1) * g.step - 1):
                return g.get(k - 1)             # a shut step its legs do not know: the close holds
        return v


class StablecoinShare(Derived):
    """Stablecoins' share of total crypto market cap, in percent, in real time:

        share(t) = S(t) / (S(t) + NS0 * B(t) / B0) * 100

    S(t)  total USD-stablecoin supply (DefiLlama, slow; history interpolated daily),
    NS0   the non-stablecoin market cap at the anchor time (CoinGecko /global total minus S),
    B(t)  a cap-weighted basket of live Binance prices (BTC, ETH, ...), B0 its value at the anchor.
    Between CoinGecko polls the non-stable cap is assumed to move with the basket (it holds ~80% of
    it). Most of a share move is MECHANICAL (crypto prices falling raise it with no new money), so
    the supply change is reported separately as the real "new cash" signal."""

    def __init__(self, key: str, basket: dict[str, Instrument], label: str = "Stablecoin share of crypto"):
        self.basket = basket                     # name -> instrument
        self.weights = {n: 1.0 / len(basket) for n in basket}
        self.supply_now = NaN                    # USD
        self.supply_hist: list[tuple[float, float]] = []   # (ts, supply) daily, sorted
        self.ns0 = NaN                           # non-stable cap at the anchor
        self.anchor_px: dict[str, float] = {}    # basket prices at the anchor
        self.anchor_ts: float | None = None
        self._now_ts: float | None = None        # when supply_now was observed
        super().__init__(key, list(basket.values()), hours="24/7", label=label)

    def set_weights(self, caps: dict[str, float]) -> None:
        tot = sum(v for n, v in caps.items() if n in self.basket and isnum(v))
        if tot > 0:
            self.weights = {n: (caps.get(n, 0.0) or 0.0) / tot for n in self.basket}

    def set_anchor(self, total_cap: float, ts: float, prices: dict[str, float] | None = None) -> None:
        """Anchor to an observed total market cap (CoinGecko) at time `ts`."""
        S = self.supply_at(ts)
        if not (isnum(total_cap) and isnum(S)) or total_cap <= S:
            return
        px = prices or {n: (i.g1.asof(ts) if isnum(i.g1.asof(ts)) else i.price) for n, i in self.basket.items()}
        if not all(isnum(v) and v > 0 for v in px.values()):
            return
        self.ns0, self.anchor_px, self.anchor_ts = total_cap - S, dict(px), ts

    def supply_at(self, ts: float) -> float:
        h = self.supply_hist
        if not h:
            return self.supply_now
        if ts >= h[-1][0]:
            # between the last daily point and now: interpolate toward the live supply
            if isnum(self.supply_now) and self._now_ts and self._now_ts > h[-1][0]:
                f = min(1.0, (ts - h[-1][0]) / (self._now_ts - h[-1][0]))
                return h[-1][1] + f * (self.supply_now - h[-1][1])
            return h[-1][1]
        if ts <= h[0][0]:
            return h[0][1]
        lo, hi = 0, len(h) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if h[mid][0] <= ts:
                lo = mid
            else:
                hi = mid
        (t0, v0), (t1, v1) = h[lo], h[hi]
        return v0 + (v1 - v0) * (ts - t0) / (t1 - t0)

    def set_supply(self, supply_now: float, ts: float, history: list[tuple[float, float]] | None = None) -> None:
        self.supply_now, self._now_ts = supply_now, ts
        if history:
            self.supply_hist = sorted(history)

    def _basket_ratio(self, px: dict[str, float]) -> float:
        if not self.anchor_px:
            return NaN
        r = 0.0
        for n, w in self.weights.items():
            p, p0 = px.get(n), self.anchor_px.get(n)
            if not (isnum(p) and isnum(p0) and p0 > 0):
                return NaN
            r += w * p / p0
        return r

    def _share(self, S: float, px: dict[str, float]) -> float:
        b = self._basket_ratio(px)
        if not (isnum(S) and isnum(b) and isnum(self.ns0)):
            return NaN
        return S / (S + self.ns0 * b) * 100.0

    def value_live(self, now: float) -> float:
        return self._share(self.supply_at(now), {n: i.price for n, i in self.basket.items()})

    def value_step(self, grid: str, k: int) -> float:
        step = G1_STEP if grid == "g1" else G5_STEP
        return self._share(self.supply_at((k + 1) * step), {n: getattr(i, grid).get(k) for n, i in self.basket.items()})


class Stitched(Derived):
    """The real instrument while its market is open, a 24/7 proxy otherwise (level-anchored):

        S_k = P_k                                  if the primary is live in step k
        S_k = S_{k-1} * (1 + (X_k / X_{k-1} - 1) / beta)   otherwise (X = proxy, beta = its leverage)

    e.g. TLT (Alpaca IEX) during US hours, the TMF perpetual's index / 3 overnight and at weekends.
    At the open the series snaps back to the real price, so returns that span the open use the real
    opening gap."""

    def __init__(self, key: str, primary: Instrument, proxy: Instrument, beta: float = 1.0,
                 label: str | None = None, primary_max_age: float = 600.0):
        self.primary, self.proxy, self.beta = primary, proxy, float(beta)
        self.primary_max_age = primary_max_age
        self._live_val = NaN
        self._live_base = (NaN, NaN)     # (S at the last closed step, proxy price then)
        super().__init__(key, [primary, proxy], hours="24/7", label=label or primary.label)
        self.daily = primary.daily       # the real instrument's daily closes (TLT's: the proxy is thin)

    def primary_live(self, now: float) -> bool:
        p = self.primary
        return (p.live_seen and clock.market_open(p.hours, now) and p.ts is not None
                and now - p.ts <= self.primary_max_age and isnum(p.price))

    @property
    def live_seen(self) -> bool:
        return (self._active or self.proxy).live_seen

    def inputs_live(self, grid: str, k: int) -> bool:
        return getattr(self.primary, grid).is_live(k) or getattr(self.proxy, grid).is_live(k)

    def value_step(self, grid: str, k: int) -> float:
        g = getattr(self, grid)
        gp, gx = getattr(self.primary, grid), getattr(self.proxy, grid)
        p = gp.get(k)
        if gp.is_live(k) and isnum(p):
            return p
        prev = g.get(k - 1)
        if not isnum(prev) and grid == "g1" and isnum(self.g5.asof(k * gp.step, max_back=3)):
            # Start of the 1-minute chain (it spans only a day): begin where a 5-minute step ends and
            # the proxy's own 1-minute close is known, at the 5-minute stitched value there, which
            # already carries every proxy move since the primary's last session. Starting from the
            # previous 5-minute close instead dropped the proxy's moves until its 1-minute bars began
            # (0.04% off, seen live).
            end = (k + 1) * gp.step
            return self.g5.get(end // G5_STEP - 1) if end % G5_STEP == 0 and isnum(gx.get(k)) else NaN
        if not isnum(prev):
            prev = gp.get(k - 1) if isnum(gp.get(k - 1)) else gp.asof((k) * gp.step, max_back=600)
        x1 = gx.get(k)
        if isnum(prev) and isnum(x1):
            # the move since the proxy's LAST KNOWN close: a gap in its data must not drop a move,
            # which would leave the series off by it for good
            x0 = gx.asof(k * gx.step, max_back=gx.size)
            if isnum(x0) and x0 > 0:
                return prev * (1.0 + (x1 / x0 - 1.0) / self.beta)
        return prev

    def value_live(self, now: float) -> float:
        if self.primary_live(now):
            return self.primary.price
        k1 = int(now // G1_STEP) - 1
        base = self.g1.get(k1)
        xb = self.proxy.g1.get(k1)
        if isnum(base) and not isnum(xb):
            # the proxy's step is empty (its feed was down): the chain carried its value from the
            # proxy's last known close, so the move runs from there
            xb = self.proxy.g1.asof((k1 + 1) * G1_STEP, max_back=G1_SIZE)
        x = self.proxy.price
        if not isnum(base):
            base = self.primary.price
            xb = self.proxy.g1.asof(self.primary.ts or now) if self.primary.ts else NaN
        if isnum(base) and isnum(x) and isnum(xb) and xb > 0:
            return base * (1.0 + (x / xb - 1.0) / self.beta)
        return base

    def active_source(self, now: float) -> Instrument:
        return self.primary if self.primary_live(now) else self.proxy

    _active: Instrument | None = None

    def refresh(self, now: float) -> None:
        super().refresh(now)
        src = self.active_source(now)
        self._active = src
        if src.ts is not None:
            self.ts, self.recv_ts = src.ts, src.recv_ts

    def latency_ms(self) -> float | None:
        return self._active.latency_ms() if self._active is not None else None
