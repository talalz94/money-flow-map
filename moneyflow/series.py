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


class Instrument:
    """One price series from one source (e.g. binance_fut:QQQUSDT index price)."""

    def __init__(self, key: str, source: str, symbol: str, hours: str = "24/7", *,
                 volume: bool = False, label: str | None = None, delayed_s: float = 0.0):
        self.key, self.source, self.symbol = key, source, symbol
        self.label = label or symbol
        self.hours = hours                  # market-hours profile, see clock.market_open
        self.calendar = clock.default_calendar(hours)   # days comparable for σ, see clock.CALENDARS
        self.delayed_s = delayed_s          # known publication delay of the source (e.g. 900 s)
        self.g1 = Grid(G1_STEP, G1_SIZE)
        self.g5 = Grid(G5_STEP, G5_SIZE)
        self.v5 = VolumeRing(G5_STEP, G5_SIZE) if volume else None
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
        self._k1 = None                     # open (not yet closed) 1-minute step
        self._k5 = None                     # open 5-minute step
        self._open_any = False              # did the open steps see a real update?
        self._vol_min: dict[int, tuple[float, float]] = {}   # minute -> (quote vol, taker-buy quote)
        self._vol_acc: dict[int, list] = {}                   # minute -> [q, tb] from trades

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
        self._live_seen = True
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
        # before any live data, a step after an unfilled hole would only carry the CACHE's last close
        # (the app was off, the first download failed): unknown too, not that old price
        h1, h5 = self.holes["g1"], self.holes["g5"]
        start = max(self._k1, k1_now - G1_SIZE)
        for k in range(start, k1_now):
            t0 = k * G1_STEP
            open_ = clock.market_open(self.hours, t0 + G1_STEP - 1)
            if open_ and not traded and h1 is not None and t0 >= h1:
                self.g1.put(k, NaN, False)
            elif traded and open_ and not (seen >= t0):
                self.g1.put(k, NaN, False)
                self._hole("g1", t0)
                self._gap_open = True
            else:
                self.g1.put(k, p, traded and open_)
                if self._gap_open and traded and open_:
                    self._gap_open, self.recovered = False, True
        k5_now = int(now // G5_STEP)
        if k5_now > self._k5:
            for k in range(max(self._k5, k5_now - G5_SIZE), k5_now):
                t0 = k * G5_STEP
                open_ = clock.market_open(self.hours, t0 + G5_STEP - 1)
                stale = open_ and not traded and h5 is not None and t0 >= h5
                covered = not stale and not (traded and open_ and not (seen >= t0))
                if covered:
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
             volumes: list[tuple[float, float, float]] | None = None) -> int:
        """Load history: `closes` = [(step_start_ts, close)], sorted. Fills from the first close up to
        `until` with carry-forward; the live flag follows the market-hours profile."""
        g = self.g1 if grid == "g1" else self.g5
        if not closes:
            return 0
        step = g.step
        k_until = int(until // step)
        k_first = max(int(closes[0][0] // step), k_until - g.size)
        by_k = {int(t // step): c for t, c in closes if isnum(c) and c > 0}
        last = NaN
        for k in range(int(closes[0][0] // step), k_first):     # warm the carry value
            if k in by_k:
                last = by_k[k]
        n = 0
        for k in range(k_first, k_until):
            if k in by_k:
                last = by_k[k]
            if last == last:
                g.put(k, last, clock.market_open(self.hours, (k + 1) * step - 1))
                n += 1
        if grid == "g5" and volumes and self.v5 is not None:
            for t, q, tb in volumes:
                k = int(t // step)
                if k < k_until:
                    self.v5.put(k, q, tb)
        h = self.holes.get(grid)
        # covered: the data starts at the gap, or at the oldest step the grid holds (anything older
        # falls out of it anyway: a 1-day grid's first minute is never downloaded again)
        if h is not None and by_k and max(by_k) * step >= h and (k_first * step <= h or k_first <= k_until - g.size + 2):
            self.holes[grid] = None          # the exchange's own data now covers the gap
        if self.price != self.price and last == last:            # no live price yet: use the last close
            self.price = last
            self.ts = closes[-1][0] + step
        return n

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
        super().__init__(key, list(pairs.values()), hours="fx", label=label)

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
        return self.formula({n: i.price for n, i in self.pairs.items()})

    def value_step(self, grid: str, k: int) -> float:
        return self.formula({n: getattr(i, grid).get(k) for n, i in self.pairs.items()})


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
