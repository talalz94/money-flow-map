"""Coinbase Exchange public websocket (no key): BTC-USD / ETH-USD reference prices.

Uses the `ticker_batch` channel (one update per product every ~5 s) to stay light. The gap to
Binance's USDT price is the "Coinbase premium", a common gauge of US buyers' demand.
Measured 2026-11-24 on the full `ticker` channel: ~120 ms median latency.
"""
from __future__ import annotations

from .. import clock, jsonutil
from .base import StreamConnector


class CoinbaseStream(StreamConnector):
    name = "coinbase"
    title = "Coinbase (reference)"
    idle_timeout = 60.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, object] = {}

    def add(self, symbol: str, inst) -> None:
        self.subs[symbol.upper()] = inst

    def available(self):
        return (bool(self.subs), "no symbols")

    def url(self):
        return self.cfg.get("ws_url", "wss://ws-feed.exchange.coinbase.com")

    async def on_open(self, ws):
        await ws.send_str(jsonutil.dumps({"type": "subscribe", "product_ids": sorted(self.subs),
                                          "channels": [self.cfg.get("channel", "ticker_batch")]}))

    def on_text(self, data, recv):
        j = jsonutil.loads(data)
        t = j.get("type")
        if t == "ticker":
            inst = self.subs.get(j.get("product_id", ""))
            if inst is not None and j.get("price"):
                ts = clock.parse_iso(j["time"]) if j.get("time") else None
                # the batch channel delivers the last trade every ~5 s: its age is not transport latency
                inst.on_price(float(j["price"]), ts, recv,
                              measure=self.cfg.get("channel", "ticker_batch") == "ticker")
        elif t == "error":
            self.error(f"Coinbase: {j.get('message')} {j.get('reason', '')}")
