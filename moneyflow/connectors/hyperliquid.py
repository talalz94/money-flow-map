"""Hyperliquid public API (no key), optional: 24/7 HIP-3 perpetuals as a fallback source.

`activeAssetCtx` pushes mark / oracle / mid prices, funding and open interest (~1 update/s).
Messages carry no timestamp, so latency is unknown and the age is measured from receipt.
Measured 2026-11-24: trades arrive ~300-450 ms after their timestamp; xyz:XYZ100 (Nasdaq 100) and
xyz:SP500 traded ~$600M-1B/day, while xyz:DXY and xyz:VIX had NO volume (not usable).
Limits: 1,000 websocket subscriptions and 1,200 REST weight/minute per IP.
"""
from __future__ import annotations

import time

from .. import jsonutil
from .base import StreamConnector


async def fetch_candles(conn, coin: str, interval: str, start: float, end: float) -> list[tuple]:
    url = conn.cfg.get("rest_url", "https://api.hyperliquid.xyz/info")
    body = {"type": "candleSnapshot", "req": {"coin": coin, "interval": interval,
                                              "startTime": int(start * 1000), "endTime": int(end * 1000)}}
    async with conn.session.post(url, json=body) as r:
        if r.status != 200:
            raise RuntimeError(f"Hyperliquid HTTP {r.status}")
        rows = jsonutil.loads(await r.read())
    now = time.time()
    step = {"1m": 60, "5m": 300, "1h": 3600}[interval]
    return [(c["t"] / 1000.0, float(c["c"]), float(c.get("v", 0)) * float(c["c"]), float("nan"))
            for c in rows if c["t"] / 1000.0 + step <= now]


class HyperliquidStream(StreamConnector):
    name = "hyperliquid"
    title = "Hyperliquid (HIP-3 perps)"
    idle_timeout = 60.0
    heartbeat = None          # Hyperliquid expects app-level {"method":"ping"}; we rely on idle_timeout

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, tuple] = {}

    def add(self, coin: str, inst, field: str = "oracle") -> None:
        self.subs[coin] = (inst, field)

    def available(self):
        return (bool(self.subs), "no symbols")

    def url(self):
        return self.cfg.get("ws_url", "wss://api.hyperliquid.xyz/ws")

    async def on_open(self, ws):
        for coin in sorted(self.subs):
            await ws.send_str(jsonutil.dumps({"method": "subscribe",
                                              "subscription": {"type": "activeAssetCtx", "coin": coin}}))

    def on_text(self, data, recv):
        j = jsonutil.loads(data)
        if j.get("channel") != "activeAssetCtx":
            return
        d = j.get("data") or {}
        sub = self.subs.get(d.get("coin"))
        if not sub:
            return
        inst, field = sub
        ctx = d.get("ctx") or {}
        key = {"oracle": "oraclePx", "mark": "markPx", "mid": "midPx"}.get(field, "oraclePx")
        px = ctx.get(key) or ctx.get("markPx")
        if px:
            inst.extra.update({"mark": float(ctx.get("markPx") or 0), "oracle": float(ctx.get("oraclePx") or 0),
                               "funding_rate": float(ctx.get("funding") or 0)})
            inst.on_price(float(px), None, recv)
