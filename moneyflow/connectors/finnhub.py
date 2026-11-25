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

    RETRY_FIRST, RETRY_MAX = 60.0, 900.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, object] = {}
        self.last_trade: dict[str, float] = {}     # symbol -> local time of its latest trade message
        self.refused_at: float | None = None       # Finnhub answered "Invalid symbol" (refused subscriptions)
        self.retry_at: float | None = None
        # how long to wait before reconnecting after a refusal (NOT `retry_delay`: that name is the
        # base class's reconnect method, and shadowing it killed the feed on its first reconnect)
        self.refusal_wait = self.RETRY_FIRST

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
                    self.last_trade[d["s"]] = recv
        elif t == "error":
            msg = str(j.get("msg") or "")
            self.error(f"Finnhub: {msg}")
            if "invalid symbol" in msg.lower() and self.retry_at is None:
                # Seen live (25 Nov): after a 502 on reconnecting, Finnhub refused all six pairs and kept
                # the link up with pings, so no FX tick ever came and the dollar went stale for good.
                # A fresh connection a little later gets them back: retry, backing off to 15 minutes.
                self.refused_at = recv
                self.retry_at = recv + self.refusal_wait
                self.refusal_wait = min(self.RETRY_MAX, self.refusal_wait * 2)
        self.note_silent()
        self.retry_if_refused(recv)

    def retry_if_refused(self, now: float) -> None:
        """Reconnect when refused pairs have not ticked since the refusal (checked on every message;
        Finnhub pings every minute)."""
        if self.refused_at is None:
            return
        quiet = [s for s in self.subs if self.last_trade.get(s, 0.0) < self.refused_at]
        if not quiet:                                    # all pairs flow again
            self.refused_at = self.retry_at = None
            self.refusal_wait = self.RETRY_FIRST
            return
        if self.retry_at is not None and now >= self.retry_at:
            self.log.warning("Finnhub refused %d pair(s) and they stayed silent: reconnecting", len(quiet))
            self.health.detail = f"subscriptions refused (Invalid symbol): reconnected at {clock.iso(now)[11:19]} UTC"
            self.retry_at = None                         # the next refusal schedules the next try
            self.kick()

    def note_silent(self) -> None:
        """Name the pairs with no tick yet since the start. The DXY needs all six, and a thin pair
        (USD/SEK) can stay silent for many minutes in the 17:00 New York rollover lull."""
        silent = [s.split(":")[-1] for s, inst in sorted(self.subs.items()) if not inst.live_seen]
        detail = f"no tick yet from {', '.join(silent)}" if silent and clock.fx_open(self.now()) else ""
        if self.health.state == "connected" and self.health.detail != detail and (
                detail or self.health.detail.startswith("no tick yet")):
            self.health.detail = detail


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
