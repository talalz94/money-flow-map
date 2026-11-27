"""The "notable stock" slot(s): which single large-cap US stock stands out right now (a heuristic).

The map's fixed nodes are asset classes. A stock is not one: the example map showed whichever stock
was moving at that moment. So `[stocks] slots` extra nodes (default 1) show the stocks whose move is
most unusual in the window, and only when one clears the threshold. The slot may be empty.

Universe: the configured large caps (S&P 100 / Nasdaq-100 names) that have a Binance 24/7 TradFi
perpetual. The chosen stock is then read around the clock like the Nasdaq-100 and S&P 500 nodes
(the perpetual's index price, every second), and no key is needed.

Screen, cheap and periodic (`scan_interval_s`, default 5 min). Prices are Binance's INDEX of each
stock, the price its node on the map reads, never a thin perpetual's last trade:
  * one bulk request for every index price (weight 10, all ~800 perpetuals, ~25 KB compressed);
  * each stock's hourly index closes and the perpetual's hourly quote volume, fetched once (31 days,
    weight 5 each) and then topped up hourly (weight 1 each).
For each window:
    r     = ln(price now / price at the window start)
    σ     = the map's own estimator (stats.py), here on hourly bars, on the previous 20 NYSE trading
            days: 1H the same clock hour; TODAY the same time since 17:00 New York; 1W the daily
            moves of the last 30 days × √5; 1M the same × √(the ~21 sessions of 30 days), and no
            volume ratio (the hourly bars hold no earlier month). A single one-off day (an
            earnings gap) is capped.
    z     = r / σ
    rvol  = the window's quote volume on the perpetual / the median of the same on previous trading days
    score = |z| · √clip(rvol, 0.5, 4)     (unusual volume strengthens a move, thin volume weakens it)
A stock qualifies with |z| ≥ z_min and score ≥ score_min; the highest scores fill the slots. The
chosen stock then becomes a full node (35-day 5-minute history and the live index price) read with
the same estimator, and its reason text quotes the node's own numbers, so the screen, the hover and
the panel agree. The screen's numbers (which chose it) stay in the API under `slot.screen`.
"""
from __future__ import annotations

import asyncio
import math
import statistics
import time
from array import array
from collections import deque

from . import clock, stats
from .connectors.base import PollConnector

H = 3600.0
D = 86400.0
NaN = float("nan")
MAX_BARS = 24 * 31              # 31 days of hourly bars per stock: 20 trading days of same-clock samples
CALENDAR = "us_equity"          # σ and volume baselines on NYSE trading days only (stats.py)


def _ok(x) -> bool:
    return x is not None and x == x and not math.isinf(x)


def perp_symbol(ticker: str) -> str:
    """'BRK.B' -> 'BRKBUSDT' (Binance's naming for its TradFi perpetuals)."""
    return ticker.replace(".", "").replace("-", "").upper() + "USDT"


class Hourly:
    """Complete hourly bars of one stock: index closes and the perpetual's quote volumes, oldest first."""
    __slots__ = ("h0", "close", "qvol")

    def __init__(self):
        self.h0: float | None = None        # open time of bar 0 (hour-aligned)
        self.close = array("d")
        self.qvol = array("d")

    @property
    def end(self) -> float | None:
        """End time of the newest bar."""
        return None if self.h0 is None else self.h0 + len(self.close) * H

    def merge(self, rows) -> None:
        """rows: [(open_ts, close, quote_volume)] of COMPLETE bars, any order; newer data wins."""
        for t, c, q in sorted(rows):
            t = float(t)
            if self.h0 is None:
                self.h0 = t
            k = int(round((t - self.h0) / H))
            if k < 0:
                continue                       # older than what we keep
            while len(self.close) < k:         # a gap: unknown bars
                self.close.append(NaN)
                self.qvol.append(NaN)
            if k == len(self.close):
                self.close.append(float(c))
                self.qvol.append(float(q))
            else:
                self.close[k], self.qvol[k] = float(c), float(q)
        extra = len(self.close) - MAX_BARS
        if extra > 0:
            del self.close[:extra]
            del self.qvol[:extra]
            self.h0 += extra * H

    def price_at(self, t: float) -> float:
        """Close of the newest bar ending at or before `t`."""
        if self.h0 is None:
            return NaN
        k = int((t - self.h0) // H) - 1
        for j in range(min(k, len(self.close) - 1), max(-1, k - 3), -1):
            if j >= 0 and self.close[j] == self.close[j]:
                return self.close[j]
        return NaN

    def price_interp(self, t: float) -> float:
        """The price at `t` between the two hourly closes around it (interpolated in log): the node
        reads 5-minute prices, so a window starting at 10:40 must not start from the 10:00 close."""
        e = math.floor(t / H) * H
        a, f = self.price_at(e), (t - e) / H
        if f <= 0 or not _ok(a):
            return a
        b = self.price_at(e + H)
        return math.exp((1 - f) * math.log(a) + f * math.log(b)) if _ok(b) and a > 0 and b > 0 else a

    def volume(self, t1: float, t2: float) -> float:
        """Quote volume of the bars lying fully inside [t1, t2) (NaN if any of them is missing)."""
        if self.h0 is None or t2 - t1 < H:
            return NaN
        k1, k2 = math.ceil((t1 - self.h0) / H - 1e-9), int((t2 - self.h0) // H)
        if k1 < 0 or k2 > len(self.qvol):
            return NaN
        s = 0.0
        for k in range(k1, k2):
            v = self.qvol[k]
            if v != v:
                return NaN
            s += v
        return s


def _ratio(v: float, base: list[float]) -> float | None:
    base = [b for b in base if _ok(b) and b > 0]
    if not _ok(v) or len(base) < 5:
        return None
    return v / statistics.median(base)


def _lr(a: float, b: float) -> float:
    return math.log(a / b) if _ok(a) and _ok(b) and a > 0 and b > 0 else NaN


def ring_price(ring, t: float, tol: float) -> float:
    """The scan price closest to time `t` (within `tol` seconds), from the bulk scans' ring."""
    best = None
    for ts, p in ring or ():
        if abs(ts - t) <= tol and (best is None or abs(ts - t) < abs(best[0] - t)):
            best = (ts, p)
    return best[1] if best else NaN


def _sigma_now(sig_at, now: float, offsets=(0.0,), stop: float | None = None) -> float:
    """σ at `now` from hourly bars, read the way the node reads it at this minute. For each window end
    now + offset (the node's ±30 minutes for 1H; just now for TODAY), the variance at the whole hours
    either side is interpolated (exact when variance accrues evenly within an hour), and the offsets
    are averaged. Without this the screen's yardstick lagged the node's by up to an hour (at 08:59 New
    York it still left out a past day's jump at 08:55 that the node counted a minute later: PANW
    −2.5σ on the screen, −2.0σ on the map), and its 1H read one exact clock hour where the node reads
    five windows around now. `stop` (TODAY: the next 17:00 roll) is never interpolated across."""
    memo: dict[float, float] = {}

    def var(x: float) -> float:
        if x not in memo:
            s = sig_at(x)
            memo[x] = s * s if _ok(s) else NaN
        return memo[x]

    vs = []
    for o in offsets:
        t = now + o
        e = math.floor(t / H) * H
        v, f = var(e), (t - e) / H
        if f > 0 and _ok(v) and (stop is None or e + H < stop):
            b = var(e + H)
            v = (1 - f) * v + f * b if _ok(b) else v
        if _ok(v):
            vs.append(v)
    return math.sqrt(sum(vs) / len(vs)) if vs else NaN


def screen_one(h: Hourly, price: float, now: float, tf: str, ring=None, tol: float = 450.0,
               calendar: str = CALENDAR) -> dict | None:
    """One stock in one window: {z, change_pct, sigma_pct, rvol, score} (None without enough data).
    σ comes from the SAME estimator as the map's nodes (stats.py: same clock, trading days only,
    robust RMS), here on hourly bars, so the stock picked and its node on the map tell the same story."""
    if not _ok(price) or price <= 0 or h.h0 is None:
        return None
    e = math.floor(now / H) * H                     # the last hour boundary: history is hour-aligned
    days = stats.SAME_CLOCK_DAYS.get(calendar, 20)
    if tf == "1h":
        p0 = ring_price(ring, now - H, tol)         # the scans' ring (seeded from 5-minute bars at a start)
        if not _ok(p0):
            p0 = h.price_interp(now - H)

        def sig_at(t: float) -> float:
            rs = stats.same_clock_hour_returns(h.price_at, t, calendar, stats.SAME_CLOCK_DAYS_1H.get(calendar, 20),
                                               offsets=(0,))
            return stats.robust_rms(rs) if sum(len(d) for d in rs) >= 10 else NaN  # rs: grouped by day
        sig = _sigma_now(sig_at, now, offsets=stats.HOUR_OFFSETS)
        rvol = _ratio(h.volume(e - H, e),
                      [h.volume(t - H, t) for t in stats.comparable_ends(e, calendar, days, span=H)])
    elif tf == "today":
        roll = clock.day_roll(now)
        p0 = h.price_at(roll)

        def sig_at(t: float) -> float:
            rs = stats.same_clock_today_returns(h.price_at, t, calendar, days)
            return stats.robust_rms(rs) if len(rs) >= 8 else NaN
        sig = _sigma_now(sig_at, now, stop=clock.day_roll(roll + 90000.0))     # never across the next roll
        done = e - roll
        rolls = [r for r in (clock.roll_n_days_before(roll, n) for n in range(1, stats.MAX_BACK_DAYS + 1))
                 if clock.trading_day(calendar, r + 1)][:days]
        rvol = _ratio(h.volume(roll, e), [h.volume(r, r + done) for r in rolls]) if done >= H else None
    elif tf == "1w":
        p0 = h.price_interp(now - 7 * D)            # the node's start is to the minute, not the hour
        rs = stats.roll_daily_returns(h.price_at, e, calendar, max_days=stats.DAILY_DAYS)
        sig = stats.robust_rms(rs) * math.sqrt(stats.week_k(calendar, now)) if len(rs) >= 10 else NaN
        week, prior = h.volume(e - 7 * D, e), h.volume(e - 28 * D, e - 7 * D)
        rvol = week / (prior / 3.0) if _ok(week) and _ok(prior) and prior > 0 else None
    elif tf == "1m":
        # the 31 days of hourly bars: the month's move, and its σ from their daily moves as 1W's (the
        # node reads a year of daily closes: a stock it shows is ranked on the node's own numbers); no
        # earlier month to compare its volume with
        p0 = h.price_interp(now - stats.MONTH_S)
        rs = stats.roll_daily_returns(h.price_at, e, calendar, max_days=stats.DAILY_DAYS)
        sig = stats.robust_rms(rs) * math.sqrt(stats.month_k(calendar, now)) if len(rs) >= 10 else NaN
        rvol = None
    else:
        raise ValueError(tf)
    r = _lr(price, p0)
    if not _ok(r) or not _ok(sig) or sig <= 0:
        return None
    z = r / sig
    vf = math.sqrt(min(4.0, max(0.5, rvol))) if rvol is not None else 1.0
    return {"z": z, "change_pct": (math.exp(r) - 1) * 100, "sigma_pct": (math.exp(sig) - 1) * 100,
            "rvol": rvol, "score": abs(z) * vf, "price": price}


def from_node(r: float, sig: float, rvol, price: float) -> dict | None:
    """A screen row from the map's own reading of a stock it already shows (5-minute data, live price)."""
    if not (_ok(r) and _ok(sig) and sig > 0):
        return None
    z = r / sig
    vf = math.sqrt(min(4.0, max(0.5, rvol))) if rvol is not None else 1.0
    return {"z": z, "change_pct": math.expm1(r) * 100, "sigma_pct": math.expm1(sig) * 100, "rvol": rvol,
            "score": abs(z) * vf, "price": price, "exact": True}


def screen(book: "StockBook", tf: str, now: float, z_min: float, score_min: float, slots: int,
           exact: dict | None = None) -> dict:
    """Rank the universe for one window: {"picks": [...], "top": [...], "screened": n}. `exact` holds the
    map's own numbers, {perpetual: (log return, σ, rvol, price)}, for stocks it already reads (the
    current and recent picks): those are ranked on exactly what their node shows, so a pick never
    disagrees with its node after the next screen. The others use the hourly bars."""
    rows = []
    for sym, h in book.hourly.items():
        px = book.last.get(sym)
        if px is None or now - px[1] > 3 * book.scan_interval + 120:
            continue
        s = from_node(*exact[sym]) if exact and sym in exact else None
        if s is None:
            s = screen_one(h, px[0], now, tf, book.ring.get(sym), tol=book.scan_interval * 1.5)
        if s is None:
            continue
        s.update(symbol=sym, ticker=book.ticker.get(sym, sym.removesuffix("USDT")))
        rows.append(s)
    rows.sort(key=lambda s: -s["score"])
    picks = [s for s in rows if abs(s["z"]) >= z_min and s["score"] >= score_min][:max(0, slots)]
    for i, s in enumerate(picks):
        s["rank"] = i + 1
        s["why"] = why_text(s, tf, len(rows))
    return {"picks": picks, "top": rows[:5], "screened": len(rows)}


TF_PHRASE = {"1h": "in the last hour", "today": "today", "1w": "this week", "1m": "this month"}


def _vol_phrase(rvol) -> str:
    return ("" if rvol is None else
            f" on {rvol:.1f}× its usual volume" if rvol >= 1.05 else
            f" on only {rvol:.1f}× its usual volume")


def eased_text(node: dict, pick: dict, tf: str, n: int, ago_s: float | None) -> str:
    """The picked stock's move on the map no longer clears the rule: a 1H move can fade in the minutes
    between two screens. Say so, with the screen's numbers that picked it, rather than calling it the
    most unusual move."""
    when = "At the last screen" if ago_s is None else f"At the screen {max(1, round(ago_s / 60))} min ago"
    rank = (f"the most unusual move among {n} large caps" if node.get("rank", 1) == 1
            else f"number {node['rank']} of {n} large caps")
    return (f"{node['ticker']} {node['change_pct']:+.2f}% {TF_PHRASE[tf]} ({node['z']:+.1f}σ)"
            f"{_vol_phrase(node['rvol'])}. {when} it was {rank} ({pick['change_pct']:+.2f}%, "
            f"{pick['z']:+.1f}σ); the move has eased since.")


def why_text(s: dict, tf: str, n: int) -> str:
    vol = _vol_phrase(s["rvol"])
    score = f" (screen score {s['score']:.1f})" if s.get("score") is not None else ""
    return (f"{s['ticker']} {s['change_pct']:+.2f}% {TF_PHRASE[tf]} ({s['z']:+.1f}σ){vol}: the most unusual "
            f"move among {n} large caps{score}.") if s.get("rank", 1) == 1 else (
            f"{s['ticker']} {s['change_pct']:+.2f}% {TF_PHRASE[tf]} ({s['z']:+.1f}σ){vol}: number {s['rank']} "
            f"of {n} large caps{score}.")


class StockBook:
    """What the screen needs: each stock's hourly bars, its latest bulk price and a ring of recent ones."""

    def __init__(self, scan_interval: float = 300.0):
        self.scan_interval = scan_interval
        self.hourly: dict[str, Hourly] = {}
        self.last: dict[str, tuple[float, float]] = {}      # symbol -> (price, ts)
        self.ring: dict[str, deque] = {}
        self.ticker: dict[str, str] = {}

    RING_CAP = 400                          # points per stock, a safety bound (an hour holds ~30)

    def _trim(self, ring: deque, now: float) -> None:
        """Keep what a 1H start can need: the points of the last hour, plus a scan's tolerance. By TIME,
        not by count: after a 15-40 minute gap the seeded 5-minute closes and the scans since then
        share the ring, and keeping only the newest 16 dropped the ones near now - 1 h (the 1H start
        then stayed on the line between two hourly closes, re-seeded and truncated at every scan)."""
        oldest = now - H - 2 * self.scan_interval * 1.5
        while ring and (ring[0][0] < oldest or len(ring) > self.RING_CAP):
            ring.popleft()

    def on_prices(self, prices: dict[str, float], ts: float) -> None:
        for sym, p in prices.items():
            if sym in self.hourly and _ok(p) and p > 0:
                self.last[sym] = (p, ts)
                ring = self.ring.setdefault(sym, deque())
                ring.append((ts, p))
                self._trim(ring, ts)

    def seed_ring(self, sym: str, points, now: float | None = None) -> None:
        """Merge earlier prices [(ts, price)] into a stock's ring, in time order (after a start or a wake)."""
        pts = sorted({(float(t), float(p)) for t, p in list(points) + list(self.ring.get(sym, ()))
                      if _ok(p) and p > 0})
        ring = deque(pts)
        if pts:
            self._trim(ring, max(now or 0.0, pts[-1][0]))
        self.ring[sym] = ring


class StockScanner(PollConnector):
    """Keeps the StockBook current and hands each window's ranking to the engine."""
    name = "stocks"
    title = "Notable-stock screen (Binance)"
    interval = 300.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.interval = float(cfg.get("scan_interval_s", 300))
        self.book = StockBook(self.interval)
        self.universe_cfg = [str(t).upper() for t in cfg.get("universe", [])]
        self.universe_at = 0.0
        self.missing: list[str] = []          # configured tickers without a perpetual
        self._seed_retry_at = 0.0             # after a failed 5-minute seed, wait before asking again
        self._seed_backoff = self.interval
        self.soon = False                     # screen again in a few seconds (a scan begun before 17:00)

    def available(self):
        if not self.cfg.get("enabled", True) or int(self.cfg.get("slots", 1)) <= 0:
            return False, "stock slot switched off"
        return (bool(self.universe_cfg), "empty universe")

    async def _universe(self) -> None:
        from .connectors import binance as bn
        if self.book.hourly and time.time() - self.universe_at < D:
            return
        info = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/exchangeInfo", {}, bn.FUT_GATE)
        perps = {s["symbol"] for s in info.get("symbols", []) if s.get("status") == "TRADING"
                 and s.get("contractType") == "TRADIFI_PERPETUAL" and s.get("underlyingType") == "EQUITY"}
        keep, self.missing = {}, []
        for t in self.universe_cfg:
            (keep.__setitem__(perp_symbol(t), t) if perp_symbol(t) in perps else self.missing.append(t))
        for sym in [s for s in self.book.hourly if s not in keep]:
            self.book.hourly.pop(sym, None)
            self.book.last.pop(sym, None)
        for sym, t in keep.items():
            self.book.hourly.setdefault(sym, Hourly())
            self.book.ticker[sym] = t
        self.universe_at = time.time()

    async def _hourly(self, now: float) -> None:
        """Top up every stock's hourly bars: Binance's index closes (the price the map's node reads) and
        the perpetual's quote volume, one small request of each per hour (31 days the first time)."""
        from .connectors import binance as bn
        e = math.floor(now / H) * H
        sem = asyncio.Semaphore(4)

        async def one(sym: str, h: Hourly):
            if h.end is not None and h.end >= e:
                return
            async with sem:
                p = {"interval": "1h", "limit": MAX_BARS}
                if h.end is not None:
                    p.update(startTime=int(h.end * 1000), limit=min(MAX_BARS, int((e - h.end) / H) + 2))
                idx = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/indexPriceKlines", dict(p, pair=sym),
                                        bn.FUT_GATE)
                vol = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/klines", dict(p, symbol=sym), bn.FUT_GATE)
                q = {r[0]: float(r[7]) for r in vol}
                h.merge([(r[0] / 1000.0, float(r[4]), q.get(r[0], NaN)) for r in idx if r[0] / 1000.0 + H <= now])

        await asyncio.gather(*(one(s, h) for s, h in list(self.book.hourly.items())))

    async def _seed_ring(self, now: float) -> None:
        """At a start, or after a sleep, the ring of scan prices does not reach back an hour, and a 1H
        move would start from a line drawn between two hourly closes (±0.4σ against the node, for up to
        52 minutes). Fetch such stocks' last 70 minutes of 5-minute index closes instead (weight 1 each)."""
        from .connectors import binance as bn
        tol = self.book.scan_interval * 1.5
        need = [s for s in self.book.hourly if not _ok(ring_price(self.book.ring.get(s), now - H, tol))]
        if not need or now < self._seed_retry_at:
            return
        sem = asyncio.Semaphore(4)

        async def one(sym: str) -> None:
            async with sem:
                rows = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/indexPriceKlines",
                                         {"pair": sym, "interval": "5m", "startTime": int((now - H - 600) * 1000),
                                          "limit": 20}, bn.FUT_GATE)
            self.book.seed_ring(sym, [(r[0] / 1000.0 + 300.0, float(r[4])) for r in rows
                                      if r[0] / 1000.0 + 300.0 <= now], now)

        res = await asyncio.gather(*(one(s) for s in need), return_exceptions=True)
        bad = [r for r in res if isinstance(r, Exception)]
        if bad:
            # those keep the hourly fallback until the ring fills from the scans (within the hour);
            # do not ask again at every scan while the network is down: 5, 10, ... 30 minutes
            self.error(bad[0], f"5-minute start prices for {len(bad)} of {len(need)} stocks")
            self._seed_retry_at = now + self._seed_backoff
            self._seed_backoff = min(1800.0, self._seed_backoff * 2)
        else:
            self._seed_backoff = self.interval

    async def _prices(self, now: float) -> None:
        """Every stock's index price in one request (weight 10, ~25 KB compressed): the price of the
        underlying stock, as the node shows it, not the perpetual's last trade. A thin perpetual's last
        trade can sit well off it (MDB, 25 Nov: 0.6% above its index at the 17:00 roll, 0.4% below at
        09:00), which made the screen's move a whole point bigger than the map's."""
        from .connectors import binance as bn
        rows = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/premiumIndex", {}, bn.FUT_GATE)
        prices = {}
        for r in rows:
            try:
                if r.get("symbol") in self.book.hourly and float(r.get("indexPrice") or 0) > 0:
                    prices[r["symbol"]] = float(r["indexPrice"])
            except (TypeError, ValueError):
                continue
        self.book.on_prices(prices, now)

    async def poll(self) -> None:
        if self.engine.seed_state.get("state") != "done":
            return                              # the map's own history first (and Binance's budget)
        now = self.now()
        await self._universe()
        await self._hourly(now)
        await self._seed_ring(now)
        await self._prices(now)
        self.health.mark_msg(now)
        self.engine.on_stock_screen(self.book, now, missing=self.missing)

    def next_interval(self) -> float:
        if self.soon:
            self.soon = False
            return 5.0
        # until the first screen has run (history still loading), look again soon
        return self.interval if self.book.last else 20.0
