"""The "notable stock" slot(s): which single large-cap US stock stands out right now (a heuristic).

The map's fixed nodes are asset classes. A stock is not one: the example map showed whichever stock
was moving at that moment. So `[stocks] slots` extra nodes (default 1) show the stocks whose move is
most unusual in the window, and only when one clears the threshold. The slot may be empty.

Universe: the configured large caps (S&P 100 / Nasdaq-100 names) that have a Binance 24/7 TradFi
perpetual. The chosen stock is then read around the clock like the Nasdaq-100 and S&P 500 nodes
(the perpetual's index price, every second), and no key is needed.

Screen, cheap and periodic (`scan_interval_s`, default 5 min):
  * one bulk request for every last price (weight 2, all ~800 perpetuals in ~50 KB);
  * each stock's hourly bars, fetched once (20 days, weight 2) and then topped up hourly (weight 1).
For each window:
    r     = ln(price now / price at the window start)
    σ     = same clock: RMS of the same window's move on each of the previous 20 days
            (1H: the same clock hour; TODAY: the same time since 17:00 New York; 1W: daily × √7)
    z     = r / σ
    rvol  = the window's quote volume on the perpetual / the median of the same on previous days
    score = |z| · √clip(rvol, 0.5, 4)     (unusual volume strengthens a move, thin volume weakens it)
A stock qualifies with |z| ≥ z_min and score ≥ score_min; the highest scores fill the slots. The
chosen stock then becomes a full node (35-day 5-minute history and the live index price), so its z
on the map comes from the same estimators as every other node. The screen's numbers only choose it.
"""
from __future__ import annotations

import asyncio
import math
import statistics
import time
from array import array
from collections import deque

from . import clock
from .connectors.base import PollConnector

H = 3600.0
D = 86400.0
NaN = float("nan")
MAX_BARS = 24 * 21              # 21 days of hourly bars per stock
DAYS = 20                       # same-clock samples for σ and the volume baseline


def _ok(x) -> bool:
    return x is not None and x == x and not math.isinf(x)


def perp_symbol(ticker: str) -> str:
    """'BRK.B' -> 'BRKBUSDT' (Binance's naming for its TradFi perpetuals)."""
    return ticker.replace(".", "").replace("-", "").upper() + "USDT"


class Hourly:
    """Complete hourly bars of one perpetual: closes and quote volumes, oldest first."""
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


def _rms(xs) -> float:
    xs = [x for x in xs if _ok(x)]
    return math.sqrt(sum(x * x for x in xs) / len(xs)) if len(xs) >= 5 else NaN


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


def screen_one(h: Hourly, price: float, now: float, tf: str, ring=None, tol: float = 450.0) -> dict | None:
    """One stock in one window: {z, change_pct, sigma_pct, rvol, score} (None without enough data)."""
    if not _ok(price) or price <= 0 or h.h0 is None:
        return None
    e = math.floor(now / H) * H                     # the last hour boundary: history is hour-aligned
    if tf == "1h":
        p0 = ring_price(ring, now - H, tol)
        if not _ok(p0):                             # first hour after a start: interpolate the hourly closes
            a, b = h.price_at(now - H), h.price_at(now - H + H)
            frac = ((now - H) % H) / H
            p0 = a + (b - a) * frac if _ok(a) and _ok(b) else a
        sig = _rms([_lr(h.price_at(e - d * D), h.price_at(e - d * D - H)) for d in range(1, DAYS + 1)])
        rvol = _ratio(h.volume(e - H, e), [h.volume(e - H - d * D, e - d * D) for d in range(1, DAYS + 1)])
    elif tf == "today":
        roll = clock.day_roll(now)
        p0 = h.price_at(roll)
        el = max(math.floor((now - roll) / H) * H, H)       # 1-hour floor, as for every node
        rolls = [clock.roll_n_days_before(roll, d) for d in range(1, DAYS + 1)]
        sig = _rms([_lr(h.price_at(r + el), h.price_at(r)) for r in rolls])
        done = e - roll
        rvol = _ratio(h.volume(roll, e), [h.volume(r, r + done) for r in rolls]) if done >= H else None
    elif tf == "1w":
        p0 = h.price_at(now - 7 * D)
        # previous days only: today's own move is what is being measured
        daily = _rms([_lr(h.price_at(e - d * D), h.price_at(e - (d + 1) * D)) for d in range(1, DAYS)])
        sig = daily * math.sqrt(7.0) if _ok(daily) else NaN
        week, prior = h.volume(e - 7 * D, e), h.volume(e - DAYS * D, e - 7 * D)
        rvol = week / (prior * 7.0 / (DAYS - 7)) if _ok(week) and _ok(prior) and prior > 0 else None
    else:
        raise ValueError(tf)
    r = _lr(price, p0)
    if not _ok(r) or not _ok(sig) or sig <= 0:
        return None
    z = r / sig
    vf = math.sqrt(min(4.0, max(0.5, rvol))) if rvol is not None else 1.0
    return {"z": z, "change_pct": (math.exp(r) - 1) * 100, "sigma_pct": (math.exp(sig) - 1) * 100,
            "rvol": rvol, "score": abs(z) * vf, "price": price}


def screen(book: "StockBook", tf: str, now: float, z_min: float, score_min: float, slots: int) -> dict:
    """Rank the universe for one window: {"picks": [...], "top": [...], "screened": n}."""
    rows = []
    for sym, h in book.hourly.items():
        px = book.last.get(sym)
        if px is None or now - px[1] > 3 * book.scan_interval + 120:
            continue
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


TF_PHRASE = {"1h": "in the last hour", "today": "today", "1w": "this week"}


def why_text(s: dict, tf: str, n: int) -> str:
    vol = ("" if s["rvol"] is None else
           f" on {s['rvol']:.1f}× its usual volume" if s["rvol"] >= 1.05 else
           f" on only {s['rvol']:.1f}× its usual volume")
    return (f"{s['ticker']} {s['change_pct']:+.2f}% {TF_PHRASE[tf]} ({s['z']:+.1f}σ){vol}: the most unusual "
            f"move among {n} large caps (screen score {s['score']:.1f}).") if s.get("rank", 1) == 1 else (
            f"{s['ticker']} {s['change_pct']:+.2f}% {TF_PHRASE[tf]} ({s['z']:+.1f}σ){vol}: number {s['rank']} "
            f"of {n} large caps (screen score {s['score']:.1f}).")


class StockBook:
    """What the screen needs: each stock's hourly bars, its latest bulk price and a ring of recent ones."""

    def __init__(self, scan_interval: float = 300.0):
        self.scan_interval = scan_interval
        self.hourly: dict[str, Hourly] = {}
        self.last: dict[str, tuple[float, float]] = {}      # symbol -> (price, ts)
        self.ring: dict[str, deque] = {}
        self.ticker: dict[str, str] = {}

    def on_prices(self, prices: dict[str, float], ts: float) -> None:
        keep = int(H / self.scan_interval) + 4
        for sym, p in prices.items():
            if sym in self.hourly and _ok(p) and p > 0:
                self.last[sym] = (p, ts)
                self.ring.setdefault(sym, deque(maxlen=keep)).append((ts, p))


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
        """Top up every stock's hourly bars (one small request each per hour; 20 days the first time)."""
        from .connectors import binance as bn
        e = math.floor(now / H) * H
        sem = asyncio.Semaphore(4)

        async def one(sym: str, h: Hourly):
            if h.end is not None and h.end >= e:
                return
            async with sem:
                p = {"symbol": sym, "interval": "1h", "limit": 499}
                if h.end is not None:
                    p.update(startTime=int(h.end * 1000), limit=min(499, int((e - h.end) / H) + 2))
                rows = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/klines", p, bn.FUT_GATE)
                h.merge([(r[0] / 1000.0, float(r[4]), float(r[7])) for r in rows if r[0] / 1000.0 + H <= now])

        await asyncio.gather(*(one(s, h) for s, h in list(self.book.hourly.items())))

    async def poll(self) -> None:
        from .connectors import binance as bn
        if self.engine.seed_state.get("state") != "done":
            return                              # the map's own history first (and Binance's budget)
        now = self.now()
        await self._universe()
        await self._hourly(now)
        rows = await bn.rest_get(self.session, bn.FUT_REST, "/fapi/v1/ticker/price", {}, bn.FUT_GATE)
        self.book.on_prices({r["symbol"]: float(r["price"]) for r in rows if r.get("symbol") in self.book.hourly},
                            now)
        self.health.mark_msg(now)
        self.engine.on_stock_screen(self.book, now, missing=self.missing)

    def next_interval(self) -> float:
        # until the first screen has run (history still loading), look again soon
        return self.interval if self.book.last else 20.0
