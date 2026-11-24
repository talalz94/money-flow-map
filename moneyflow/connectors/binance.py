"""Binance public market data (no key; market data only - nothing here can trade).

Spot   wss://stream.binance.com:9443/stream          <sym>@aggTrade (price) and <sym>@kline_1m (volume)
USD-M  wss://fstream.binance.com/market/stream      <sym>@kline_1m (volume, last) and <sym>@markPrice@1s
       (mark, INDEX price of the underlying, funding). Measured 2026-11-24: the legacy /stream path
       accepts the connection but delivers nothing; /market/stream works.
REST   klines / indexPriceKlines for history, openInterest + openInterestHist for leverage.
Limits: REST weight 6,000/min (spot) and 2,400/min (futures) per IP; this module reads the
X-MBX-USED-WEIGHT-1M header and pauses well below the limit. Websocket: 1,024 streams per
connection, connections recycled by Binance every 24 h (handled by the reconnect loop).
"""
from __future__ import annotations

import asyncio
import math
import time
from collections import deque

import aiohttp

from .. import jsonutil
from .base import PollConnector, StreamConnector

SPOT_REST = "https://api.binance.com"
FUT_REST = "https://fapi.binance.com"


class WeightGate:
    """Keeps REST usage far below Binance's per-minute weight limit (a 429/418 can ban the IP)."""

    def __init__(self, limit: int, header: str):
        self.limit, self.header = limit, header.lower()
        self.used = 0
        self.at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            if self.used >= self.limit * 0.5 and time.time() - self.at < 60:
                await asyncio.sleep(max(1.0, 61 - (time.time() % 60)))
                self.used = 0

    def update(self, headers) -> None:
        for k, v in headers.items():
            if k.lower() == self.header:
                try:
                    self.used, self.at = int(v), time.time()
                except ValueError:
                    pass


SPOT_GATE = WeightGate(6000, "x-mbx-used-weight-1m")
FUT_GATE = WeightGate(2400, "x-mbx-used-weight-1m")


async def rest_get(session: aiohttp.ClientSession, base: str, path: str, params: dict, gate: WeightGate,
                   timeout: float = 30):
    await gate.wait()
    async with session.get(base + path, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        gate.update(r.headers)
        body = await r.read()
        if r.status in (418, 429):
            retry = int(r.headers.get("Retry-After", "60") or 60)
            gate.used, gate.at = gate.limit, time.time()
            raise RuntimeError(f"Binance rate limit (HTTP {r.status}); retry after {retry}s")
        if r.status != 200:
            raise RuntimeError(f"Binance HTTP {r.status}: {body[:160]!r}")
        return jsonutil.loads(body)


async def fetch_klines(session, market: str, symbol: str, interval: str, start_ms: int, end_ms: int,
                       price: str = "last") -> list[tuple]:
    """-> [(open_ts_s, close, quote_vol, taker_buy_quote)]; price="index" uses indexPriceKlines (no volume)."""
    if market == "spot":
        base, path, lim, gate = SPOT_REST, "/api/v3/klines", 1000, SPOT_GATE
    else:
        base, gate, lim = FUT_REST, FUT_GATE, 1500
        path = "/fapi/v1/indexPriceKlines" if price == "index" else "/fapi/v1/klines"
    step_ms = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000}[interval]
    out = []
    t = start_ms
    while t < end_ms:
        p = {"interval": interval, "startTime": t, "endTime": end_ms, "limit": lim}
        p["pair" if price == "index" and market != "spot" else "symbol"] = symbol
        rows = await rest_get(session, base, path, p, gate)
        if not rows:
            break
        for r in rows:
            q = float(r[7]) if len(r) > 7 and price != "index" else math.nan
            tb = float(r[10]) if len(r) > 10 and price != "index" else math.nan
            out.append((r[0] / 1000.0, float(r[4]), q, tb))
        nt = rows[-1][0] + step_ms
        if nt <= t or len(rows) < 2:
            break
        t = nt
    # the last kline is still open: keep it out of closed history
    now_ms = time.time() * 1000
    return [r for r in out if (r[0] * 1000 + step_ms) <= now_ms]


async def fetch_oi_hist(session, symbol: str, period: str, limit: int) -> list[tuple[float, float]]:
    rows = await rest_get(session, FUT_REST, "/futures/data/openInterestHist",
                          {"symbol": symbol, "period": period, "limit": limit}, FUT_GATE)
    return [(r["timestamp"] / 1000.0, float(r["sumOpenInterest"])) for r in rows]


class _BinanceStream(StreamConnector):
    market = "spot"

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, dict] = {}     # SYMBOL -> {"inst": Instrument|None, "field": str, "aux": bool}

    def add(self, symbol: str, inst, field: str = "last", fast: bool = True, volume: bool = True) -> None:
        """Subscribe `symbol`. fast=True (spot) adds the trade stream for ~100 ms prices; otherwise the
        1-minute kline (updated every 1-2 s) carries the price. volume=False (futures) skips the kline."""
        s = symbol.upper()
        cur = self.subs.get(s)
        if cur and cur.get("inst") is not None and inst is None:
            return
        self.subs[s] = {"inst": inst, "field": field, "fast": fast, "volume": volume}

    def streams(self) -> list[str]:
        raise NotImplementedError

    def url(self) -> str:
        return f"{self.cfg.get('ws_url')}?streams={'/'.join(self.streams())}"

    def available(self):
        return (bool(self.subs), "no symbols configured")


class BinanceSpot(_BinanceStream):
    name = "binance_spot"
    title = "Binance spot"
    market = "spot"
    idle_timeout = 60.0

    def streams(self):
        out = []
        for s, v in sorted(self.subs.items()):
            out.append(f"{s.lower()}@kline_1m")
            if v["field"] == "last" and v.get("fast", True):
                out.append(f"{s.lower()}@aggTrade")
        return out

    def on_text(self, data, recv):
        j = jsonutil.loads(data)
        d = j.get("data") or {}
        e = d.get("e")
        if e == "aggTrade":
            sub = self.subs.get(d["s"])
            if sub and sub["inst"] is not None:
                sub["inst"].on_price(float(d["p"]), d["T"] / 1000.0, recv)
        elif e == "kline":
            k = d["k"]
            sub = self.subs.get(d["s"])
            if not sub or sub["inst"] is None:
                return
            inst = sub["inst"]
            inst.on_minute_volume(k["t"] / 1000.0, float(k["q"]), float(k["Q"]))
            if not sub.get("fast", True) or sub["field"] != "last" or inst.ts is None:
                inst.on_price(float(k["c"]), d["E"] / 1000.0, recv)
            else:
                inst.note_message(recv)


class BinanceFutures(_BinanceStream):
    name = "binance_fut"
    title = "Binance USD-M futures"
    market = "fut"
    idle_timeout = 45.0

    def streams(self):
        out = []
        for s, v in sorted(self.subs.items()):
            out.append(f"{s.lower()}@markPrice@1s")
            if v.get("volume", True):
                out.append(f"{s.lower()}@kline_1m")
        return out

    def on_text(self, data, recv):
        j = jsonutil.loads(data)
        d = j.get("data") or {}
        e = d.get("e")
        sub = self.subs.get(d.get("s", ""))
        if not sub:
            return
        inst = sub["inst"]
        if inst is None:
            return
        if e == "markPriceUpdate":
            ev = d["E"] / 1000.0
            x = inst.extra
            x["mark"] = float(d["p"])
            x["index"] = float(d["i"]) if d.get("i") else math.nan
            if d.get("r") not in (None, ""):
                x["funding_rate"] = float(d["r"])
                x["next_funding_ms"] = d.get("T")
            if sub["field"] == "index" and x["index"] == x["index"] and x["index"] > 0:
                inst.on_price(x["index"], ev, recv)
            elif sub["field"] == "mark":
                inst.on_price(x["mark"], ev, recv)
            else:
                inst.note_message(recv, ev)
        elif e == "kline":
            k = d["k"]
            inst.on_minute_volume(k["t"] / 1000.0, float(k["q"]), float(k["Q"]))
            inst.extra["last"] = float(k["c"])
            if sub["field"] == "last":
                inst.on_price(float(k["c"]), d["E"] / 1000.0, recv)


class BinanceOpenInterest(PollConnector):
    """Open interest (contracts) for a few perps, polled; history seeded from openInterestHist."""
    name = "binance_oi"
    title = "Binance open interest (REST)"
    interval = 60.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.interval = float(cfg.get("oi_interval_s", 60))
        self.symbols = [s.upper() for s in cfg.get("oi_symbols", [])]
        self.hist: dict[str, deque] = {s: deque(maxlen=3000) for s in self.symbols}
        self._seeded = False

    def available(self):
        return (bool(self.symbols), "no oi_symbols")

    async def seed(self) -> None:
        for s in self.symbols:
            try:
                h1 = await fetch_oi_hist(self.session, s, "1h", 200)
                m5 = await fetch_oi_hist(self.session, s, "5m", 500)
            except Exception as e:  # noqa: BLE001
                self.error(e, f"history {s}")
                continue
            cut = m5[0][0] if m5 else float("inf")
            rows = [r for r in h1 if r[0] < cut] + m5
            self.hist[s].extend(rows)
        self._seeded = True

    async def poll(self):
        if not self._seeded:
            await self.seed()
        for s in self.symbols:
            j = await rest_get(self.session, FUT_REST, "/fapi/v1/openInterest", {"symbol": s}, FUT_GATE)
            self.hist[s].append((j["time"] / 1000.0, float(j["openInterest"])))
            self.health.mark_msg(self.now())
        self.engine.on_open_interest({s: list(self.hist[s]) for s in self.symbols})

    def change_pct(self, symbol: str, since: float) -> float | None:
        h = self.hist.get(symbol.upper())
        if not h:
            return None
        ref = None
        for t, v in h:
            if t <= since:
                ref = v
            else:
                break
        if ref is None or ref <= 0:
            return None
        return (h[-1][1] / ref - 1.0) * 100.0
