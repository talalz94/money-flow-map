"""Alpaca market data, free "Basic" plan (ALPACA_KEY_ID / ALPACA_SECRET; data only, no orders).

IEX real-time stream: trades for every configured symbol, bid/ask for a few thin ones (mid price).
IEX is one exchange (~2-3% of US volume), so prices are real-time but volume is a SAMPLE.
News stream (v1beta1/news): real-time headlines.
History: 5-minute and 1-minute IEX bars over REST (the free SIP feed has a 15-minute embargo).
The free plan allows one connection per stream; if Alpaca refuses it (code 406, e.g. another app
holds it), the connector waits and retries every few minutes, and prices fall back to REST polls
of the latest IEX trade every 20 s meanwhile.
"""
from __future__ import annotations

import datetime as dt
import math

from .. import clock, jsonutil
from .base import STANDBY, PollConnector, StreamConnector, Unauthorized


def _hdr(keys) -> dict:
    return {"APCA-API-KEY-ID": keys.get("ALPACA_KEY_ID") or "", "APCA-API-SECRET-KEY": keys.get("ALPACA_SECRET") or ""}


KEYS_REFUSED = "Alpaca refused the keys: ETFs are read without a key (Yahoo history, no live prints)"


class StreamRefused(Exception):
    """Alpaca answered the login with an error (406 connection limit, 402 bad keys ...)."""

    def __init__(self, code, msg):
        super().__init__(f"Alpaca error {code}: {msg}")
        self.code = code


async def authenticate(conn, ws) -> None:
    """Send the login and WAIT for its answer before subscribing. (Subscribing straight away makes
    Alpaca answer a refused login with '401 not authenticated', which hides the real reason.)"""
    import aiohttp
    await ws.send_str(jsonutil.dumps({"action": "auth", "key": conn.keys.get("ALPACA_KEY_ID"),
                                      "secret": conn.keys.get("ALPACA_SECRET")}))
    deadline = conn.now() + 15
    while conn.now() < deadline:
        msg = await ws.receive(timeout=max(0.5, deadline - conn.now()))
        if msg.type != aiohttp.WSMsgType.TEXT:
            raise RuntimeError("Alpaca closed the connection during login")
        for m in jsonutil.loads(msg.data):
            if m.get("T") == "success" and m.get("msg") == "authenticated":
                return
            if m.get("T") == "error":
                raise StreamRefused(m.get("code"), m.get("msg"))
    raise RuntimeError("Alpaca login timed out")


async def fetch_bars(conn, symbols: list[str], timeframe: str, start: float, end: float | None = None,
                     feed: str = "iex") -> dict[str, list[tuple]]:
    """-> {symbol: [(bar_start_ts, close, quote_volume)]} via the multi-symbol bars endpoint."""
    url = f"{conn.cfg.get('rest_url', 'https://data.alpaca.markets')}/v2/stocks/bars"
    params = {"symbols": ",".join(symbols), "timeframe": timeframe, "limit": 10000, "feed": feed,
              "start": clock.at(start, dt.timezone.utc).isoformat().replace("+00:00", "Z"),
              "adjustment": "raw"}
    if end:
        params["end"] = clock.at(end, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    out: dict[str, list] = {s: [] for s in symbols}
    token = None
    for _ in range(60):
        if token:
            params["page_token"] = token
        j = await conn.get_json(url, params=params, headers=_hdr(conn.keys), timeout=60)
        for sym, bars in (j.get("bars") or {}).items():
            for b in bars:
                vw = b.get("vw") or b.get("c")
                out.setdefault(sym, []).append((clock.parse_iso(b["t"]), float(b["c"]), float(b.get("v", 0)) * float(vw)))
        token = j.get("next_page_token")
        if not token:
            break
    for s in out:
        out[s].sort()
    return out


async def fetch_latest_trades(conn, symbols: list[str], feed: str = "iex") -> dict[str, float]:
    """-> {symbol: the time of its latest trade on the feed} (any session; a symbol with none is left out)."""
    url = f"{conn.cfg.get('rest_url', 'https://data.alpaca.markets')}/v2/stocks/trades/latest"
    j = await conn.get_json(url, params={"symbols": ",".join(symbols), "feed": feed}, headers=_hdr(conn.keys))
    return {s: clock.parse_iso(t["t"]) for s, t in (j.get("trades") or {}).items() if t and t.get("t")}


class AlpacaStream(StreamConnector):
    name = "alpaca"
    title = "Alpaca IEX (real-time)"
    idle_timeout = 600.0      # quiet outside US hours; heartbeat pings keep the socket checked
    heartbeat = 25.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.subs: dict[str, object] = {}        # symbol -> Instrument
        self.quote_syms = {s.upper() for s in cfg.get("quote_symbols", [])}
        self.refused_until = 0.0
        self.auth_failed = False

    def add(self, symbol: str, inst) -> None:
        self.subs[symbol.upper()] = inst

    def idle_limit(self) -> float:
        # overnight and at weekends IEX is silent by design: rely on the heartbeat, not on silence
        if clock.market_open("us_extended", self.now()):
            return self.idle_timeout
        return 3600.0

    def available(self):
        if not self.keys.has("ALPACA_KEY_ID", "ALPACA_SECRET"):
            return False, "no ALPACA_KEY_ID / ALPACA_SECRET"
        if self.auth_failed:
            return False, self.health.detail or KEYS_REFUSED
        return (bool(self.subs), "no symbols")

    def keys_refused(self, why: str) -> None:
        """Alpaca refused the keys (the stream's login: 402; REST: HTTP 401/403). From then on the ETFs
        are read as without a key: their history from Yahoo, carried, never covered by the stream or
        the REST fallback. Asked again, every back-fill and start-up retry failed all day."""
        if not self.auth_failed:
            self.log.warning("Alpaca refused the keys (%s): ETFs read without a key from now on", why[:160])
        self.auth_failed = True
        self.health.detail = KEYS_REFUSED
        # ...and what they had heard of their feed is forgotten: the refusal comes a round trip after the
        # first tick, which had handed them the fallback's first round, and with that time every session
        # minute after it became a gap, back-filled from Yahoo each minute (20 requests and a full cache
        # save each time), all session
        for inst in self.subs.values():
            inst._seen_ts, inst._live_seen = float("nan"), False

    def url(self):
        return self.cfg.get("ws_url", "wss://stream.data.alpaca.markets/v2/iex")

    def retry_delay(self):
        if self.auth_failed:
            return 3600.0
        if self.now() < self.refused_until:
            return max(60.0, self.refused_until - self.now())
        return super().retry_delay()

    async def on_open(self, ws):
        try:
            await authenticate(self, ws)
        except StreamRefused as e:
            self.refused(e.code, str(e))
            raise
        if self.auth_failed:                # refused before, accepted now: the ETFs are read with the key again
            self.auth_failed = False
            self.log.warning("Alpaca accepts the keys again: ETFs read with the key")
        self.health.detail = "authenticated"
        syms = sorted(self.subs)
        await ws.send_str(jsonutil.dumps({"action": "subscribe", "trades": syms,
                                          "quotes": sorted(self.quote_syms & set(syms))}))

    def refused(self, code, text: str) -> None:
        if code == 406:          # connection limit exceeded: another app (or instance) holds the stream
            self.refused_until = self.now() + 300
            self.health.detail = "stream refused: connection limit (another app?) - REST fallback every 20 s"
        elif code == 402:        # auth failed: wrong keys
            self.keys_refused(f"login: {text}")
        else:
            self.health.detail = text[:120]

    def on_text(self, data, recv):
        for m in jsonutil.loads(data):
            T = m.get("T")
            if T in ("t", "q") and self.auth_failed:
                # the keys refused: the ETFs are read without a key, whatever a session opened before
                # still sends. Taken, its last prints armed their feed again, and once it closed every
                # session minute was a gap, back-filled from Yahoo each minute (keys revoked mid-run)
                continue
            if T == "t":
                inst = self.subs.get(m["S"])
                if inst is None:
                    continue
                ts = clock.parse_iso(m["t"])
                p, size = float(m["p"]), float(m.get("s", 0))
                inst.extra["last_trade"] = p
                inst.on_trade_volume(ts, p * size)
                if m["S"] not in self.quote_syms or not self._mid_ok(inst):
                    inst.on_price(p, ts, recv)
                else:
                    inst.note_message(recv, ts)
            elif T == "q":
                inst = self.subs.get(m["S"])
                if inst is None:
                    continue
                bp, ap = float(m.get("bp") or 0), float(m.get("ap") or 0)
                inst.extra["bid"], inst.extra["ask"] = bp, ap
                if self._mid_ok(inst):
                    inst.on_price((bp + ap) / 2.0, clock.parse_iso(m["t"]), recv)
            elif T == "error":
                code, msg = m.get("code"), m.get("msg")
                self.error(f"Alpaca error {code}: {msg}")
                self.refused(code, f"Alpaca error {code}: {msg}")

    @staticmethod
    def _mid_ok(inst) -> bool:
        b, a = inst.extra.get("bid", 0), inst.extra.get("ask", 0)
        return b > 0 and a > 0 and a >= b and (a - b) / ((a + b) / 2) < 0.002


class AlpacaNews(StreamConnector):
    name = "alpaca_news"
    title = "Alpaca news (real-time)"
    idle_timeout = 3600.0      # news can be quiet for long stretches; the heartbeat checks the link
    heartbeat = 25.0

    def available(self):
        if not self.cfg.get("news", True):
            return False, "news disabled"
        if not self.keys.has("ALPACA_KEY_ID", "ALPACA_SECRET"):
            return False, "no ALPACA_KEY_ID / ALPACA_SECRET"
        return True, ""

    def url(self):
        return self.cfg.get("news_ws_url", "wss://stream.data.alpaca.markets/v1beta1/news")

    async def on_open(self, ws):
        try:
            await authenticate(self, ws)
        except StreamRefused as e:
            self.health.detail = str(e)[:120]
            if e.code == 406:
                self.refused_until = self.now() + 300
            raise
        await ws.send_str(jsonutil.dumps({"action": "subscribe", "news": ["*"]}))

    refused_until = 0.0

    def retry_delay(self):
        if self.now() < self.refused_until:
            return max(60.0, self.refused_until - self.now())
        return super().retry_delay()

    # The stream carries every company press release (measured: ~2 a second at the US open, mostly
    # micro-caps). Only items about the map's tickers or macro themes reach the news wire.
    MACRO_TAGS = {"FED", "MACRO", "BONDS", "USD", "OIL", "GOLD", "BTC", "ETH", "STABLE", "ETF"}

    def on_text(self, data, recv):
        from ..news import tags_for
        items = []
        watch = self.engine.watch_symbols()
        for m in jsonutil.loads(data):
            if m.get("T") == "n" and m.get("headline"):
                head = m["headline"].strip()
                if not (set(m.get("symbols") or []) & watch) and not (set(tags_for(head)) & self.MACRO_TAGS):
                    self.health.detail = "filtering company press releases"
                    continue
                items.append({"title": head, "url": m.get("url") or "",
                              "ts": clock.parse_iso(m.get("created_at") or m.get("updated_at")),
                              "source": f"{m.get('source') or 'Alpaca'} (via Alpaca)"})
            elif m.get("T") == "error":
                self.error(f"Alpaca news error {m.get('code')}: {m.get('msg')}")
        if items:
            self.engine.on_news(items)


class AlpacaFallback(PollConnector):
    """REST latest-trade polling, active only while the stream is down or refused."""
    name = "alpaca_rest"
    title = "Alpaca IEX (REST fallback)"
    interval = 20.0

    def __init__(self, ctx, cfg, stream: AlpacaStream):
        super().__init__(ctx, cfg)
        self.stream = stream
        self.interval = 20.0
        self.started = self.now()

    def available(self):
        return self.stream.available()

    @property
    def auth_failed(self) -> bool:
        return bool(getattr(self.stream, "auth_failed", False))

    # a stream connected but silent this long in a session is asked about (one REST request a round): a
    # stalled stream, or a day nothing trades (whose old trades are a real answer all the same)
    STREAM_QUIET_S = 60.0

    def heard(self, result, now: float) -> float | None:
        """A round that asked: now. One that stood by (PollConnector.run): only as far as the network is
        known up, the newest real message of any stream (the Alpaca stream's, Binance's ...) or a trade
        answer of this one (other pollers' data can come from their cache). Counted as now, a round in the
        half-minute before a dropped connection is noticed covered the next minute, and one across 16:00
        the 15:59 minute and the night after it: the price from before the outage, flagged live, never
        fetched. None: nothing heard yet."""
        if result != STANDBY:
            return now
        best = None
        for c in getattr(getattr(self.ctx, "engine", None), "connectors", {}).values():
            if getattr(c, "auth_failed", False) or (isinstance(c, PollConnector) and c is not self):
                continue
            t = getattr(getattr(c, "health", None), "last_msg", None)
            if t is not None and (best is None or t > best):
                best = t
        return None if best is None else min(best, now)

    async def poll(self):
        # (a round that stands by asks nothing: it counts as the feed heard as far as the network is known
        # up, heard(); with the keys refused not at all: Engine._feed_seen)
        if self.auth_failed:
            self.health.detail = KEYS_REFUSED
            return STANDBY
        now, subs = self.now(), self.stream.subs.values()
        if self.stream.health.state == "connected" and self.stream.ws is not None:
            if now - (getattr(self.stream.health, "last_msg", None) or 0.0) <= self.STREAM_QUIET_S:
                self.health.detail = "standby (stream connected)"
                return STANDBY
            self.health.detail = "asking (stream connected but silent)"     # (the market shut: below)
        if self.now() - self.started < 30 and not self.stream.refused_until:
            self.health.detail = "standby (stream connecting)"     # give the stream time at start-up
            return STANDBY
        if not any(clock.market_open(i.hours, self.now()) for i in self.stream.subs.values()):
            self.health.detail = "standby (US market closed)"
            return STANDBY
        url = f"{self.cfg.get('rest_url', 'https://data.alpaca.markets')}/v2/stocks/trades/latest"
        try:
            j = await self.get_json(url, params={"symbols": ",".join(sorted(self.stream.subs)),
                                                 "feed": self.cfg.get("feed", "iex")}, headers=_hdr(self.keys))
        except Unauthorized as e:
            self.stream.keys_refused(f"REST fallback: {e}")
            self.health.detail = KEYS_REFUSED
            return STANDBY
        recv = self.now()
        for sym, t in (j.get("trades") or {}).items():
            inst = self.stream.subs.get(sym)
            if inst is not None and t.get("p"):
                inst.on_price(float(t["p"]), clock.parse_iso(t["t"]), recv, measure=False)
        self.health.mark_msg(recv)
        if not self.health.detail.startswith("asking"):
            self.health.detail = "polling (stream down)"

    def next_interval(self):
        return 20.0 if self.stream.health.state != "connected" else 60.0


def nan_to_none(x):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else x
