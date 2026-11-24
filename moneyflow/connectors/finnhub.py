"""Finnhub (FINNHUB_KEY, free plan): real-time OANDA FX over websocket, and general news over REST.

Free plan limits: 60 REST calls/minute, 50 websocket symbols, one websocket. Measured 2026-11-24:
OANDA FX ticks arrive ~260-290 ms (median) after their timestamp, 1-2 per second per pair; the
DXY computed from them matched ICE's DXY within 0.002 (sd) over 323 matched quotes.
The token travels in the websocket URL, so every error string is passed through keys.redact().
"""
from __future__ import annotations

from .. import clock, jsonutil
from .base import PollConnector, StreamConnector


class FinnhubStream(StreamConnector):
    name = "finnhub"
    title = "Finnhub (OANDA FX)"
    idle_timeout = 120.0     # Finnhub sends {"type":"ping"} even when FX is closed
    heartbeat = 30.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, object] = {}

    def add(self, symbol: str, inst) -> None:
        self.subs[symbol] = inst

    def idle_limit(self) -> float:
        # Finnhub pings every 60 s (measured), but FX is shut from Friday to Sunday 17:00 New York:
        # do not treat a quiet weekend as a dead feed (the websocket heartbeat checks the link)
        return self.idle_timeout if clock.fx_open(self.now()) else 3600.0

    def available(self):
        if not self.keys.has("FINNHUB_KEY"):
            return False, "no FINNHUB_KEY"
        return (bool(self.subs), "no symbols")

    def url(self):
        return f"{self.cfg.get('ws_url', 'wss://ws.finnhub.io')}?token={self.keys.get('FINNHUB_KEY')}"

    async def on_open(self, ws):
        for s in sorted(self.subs):
            await ws.send_str(jsonutil.dumps({"type": "subscribe", "symbol": s}))

    def on_text(self, data, recv):
        j = jsonutil.loads(data)
        t = j.get("type")
        if t == "trade":
            for d in j.get("data") or ():
                inst = self.subs.get(d.get("s"))
                if inst is not None and d.get("p"):
                    inst.on_price(float(d["p"]), d["t"] / 1000.0, recv)
        elif t == "error":
            self.error(f"Finnhub: {j.get('msg')}")


class FinnhubNews(PollConnector):
    name = "finnhub_news"
    title = "Finnhub news (REST)"
    interval = 150.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.interval = float(cfg.get("news_interval_s", 150))
        self.cats = ["general", "crypto"]
        self.i = 0

    def available(self):
        if not self.cfg.get("news", True):
            return False, "news disabled"
        return (self.keys.has("FINNHUB_KEY"), "no FINNHUB_KEY")

    async def poll(self):
        cat = self.cats[self.i % len(self.cats)]
        self.i += 1
        url = f"{self.cfg.get('rest_url', 'https://finnhub.io/api/v1')}/news"
        j = await self.get_json(url, params={"category": cat, "token": self.keys.get("FINNHUB_KEY")})
        items = [{"title": x.get("headline", "").strip(), "url": x.get("url", ""), "ts": float(x.get("datetime") or 0),
                  "source": f"{x.get('source') or 'Finnhub'} (via Finnhub)"} for x in j if x.get("headline")]
        self.health.mark_msg(self.now(), len(items))
        self.engine.on_news([i for i in items if i["ts"] > 0])
