"""Connector framework: websocket streams and REST pollers that survive sleep and network loss.

Every connector runs one coroutine forever:
  * websockets reconnect with exponential back-off and jitter (1 s doubling to 60 s), reset after a
    connection has stayed healthy for a minute; a feed that goes silent for `idle_timeout` seconds
    is treated as dead (half-open TCP after Wi-Fi loss) and reconnected; aiohttp's heartbeat pings
    detect dead peers;
  * pollers sleep `interval` between polls and back off on errors;
  * `kick()` (called after the PC wakes from sleep) closes the socket / ends the wait immediately.
No busy loops: every wait is an awaitable with a timeout.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque

import aiohttp

from .. import jsonutil

# what a poll() returns when it stood by without asking (the Alpaca fallback while the stream is up):
# no successful poll, so no `last_ok`. Counted as one, its round right after a wake, which can run
# before the first tick, covered the whole time asleep: no gap, the pre-sleep price kept.
STANDBY = "standby"


class Backoff:
    def __init__(self, base: float = 1.0, cap: float = 60.0):
        self.base, self.cap, self.n = base, cap, 0

    def next(self) -> float:
        d = min(self.cap, self.base * (2 ** self.n))
        self.n += 1
        return d * random.uniform(0.7, 1.3)

    def reset(self) -> None:
        self.n = 0


class Health:
    def __init__(self, name: str, kind: str, title: str):
        self.name, self.kind, self.title = name, kind, title
        self.state = "starting"
        self.detail = ""
        self.connects = 0
        self.errors = 0
        self.last_error = None
        self.last_error_at = None
        self.last_msg = None          # local receive time of the last message (NTP-corrected)
        self.last_ok = None           # pollers: last successful poll
        self.msgs = 0
        self._rate = deque(maxlen=120)  # (t, msgs) samples for a rate estimate
        self.connected_since = None
        self.last_poll_ms = None        # pollers: duration of the last successful poll (all requests)

    def mark_msg(self, now: float, n: int = 1) -> None:
        self.msgs += n
        self.last_msg = now

    def rate(self, now: float) -> float | None:
        self._rate.append((now, self.msgs))
        while len(self._rate) > 2 and now - self._rate[0][0] > 60:
            self._rate.popleft()
        if len(self._rate) < 2:
            return None
        (t0, m0), (t1, m1) = self._rate[0], self._rate[-1]
        return (m1 - m0) / (t1 - t0) if t1 > t0 else None

    def as_dict(self, now: float) -> dict:
        return {
            "id": self.name, "title": self.title, "kind": self.kind, "state": self.state, "detail": self.detail,
            "connects": self.connects, "reconnects": max(0, self.connects - 1), "errors": self.errors,
            "msgs": self.msgs, "msgs_per_s": None if (r := self.rate(now)) is None else round(r, 2),
            "last_message_age_s": None if self.last_msg is None else round(now - self.last_msg, 1),
            "last_ok_age_s": None if self.last_ok is None else round(now - self.last_ok, 1),
            "last_poll_ms": self.last_poll_ms,
            "connected_for_s": None if self.connected_since is None else round(now - self.connected_since),
            "last_error": self.last_error,
            "last_error_age_s": None if self.last_error_at is None else round(now - self.last_error_at),
        }


class Connector:
    """Base: holds the shared context (engine, HTTP session, keys, clock, config)."""
    name = "base"
    title = "base"
    kind = "stream"

    def __init__(self, ctx, cfg: dict):
        self.ctx = ctx
        self.cfg = cfg
        self.engine = ctx.engine
        self.clock = ctx.clock
        self.keys = ctx.keys
        self.log = logging.getLogger(f"moneyflow.{self.name}")
        self.health = Health(self.name, self.kind, self.title)
        self._wake = asyncio.Event()
        self._task = None

    @property
    def session(self) -> aiohttp.ClientSession:
        return self.ctx.session

    def available(self) -> tuple[bool, str]:
        """(can this connector run, why not). Override to require keys."""
        return True, ""

    def now(self) -> float:
        return self.clock.now()

    def error(self, e, detail: str = "") -> None:
        msg = self.keys.redact(f"{type(e).__name__}: {e}" if isinstance(e, BaseException) else str(e))
        if detail:
            msg = f"{detail}: {msg}"
        self.health.errors += 1
        self.health.last_error = msg[:300]
        self.health.last_error_at = self.now()
        self.log.warning("%s", msg[:300])

    async def sleep(self, seconds: float) -> None:
        """Interruptible sleep: kick() ends it early, also a kick that came while the connector was
        busy (a poll in flight at the 17:00 roll lost the roll's re-screen kick)."""
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=max(0.0, seconds))
        except asyncio.TimeoutError:
            pass
        self._wake.clear()

    def kick(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        raise NotImplementedError

    async def get_json(self, url: str, *, params=None, headers=None, timeout: float = 30):
        async with self.session.get(url, params=params, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.read()
            if r.status == 429:
                try:
                    wait = float(r.headers.get("Retry-After", "") or 900)
                except ValueError:
                    wait = 900.0
                raise RateLimited(url.split("?")[0], min(max(wait, 300.0), 3600.0))
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status} {body[:160]!r}")
            return jsonutil.loads(body)


class StreamConnector(Connector):
    kind = "stream"
    idle_timeout = 90.0       # seconds without any message -> reconnect
    heartbeat = 20.0          # websocket ping interval (None = rely on app-level pings)
    healthy_after = 60.0      # a connection this old resets the back-off

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.ws = None
        self.backoff = Backoff()

    def url(self) -> str:
        raise NotImplementedError

    def idle_limit(self) -> float:
        """Seconds of silence after which the feed is treated as dead. Streams whose market can be
        closed override this: a silent feed is normal then, and the websocket heartbeat (ping/pong)
        still detects a dead connection, so there is no need to reconnect every few minutes."""
        return self.idle_timeout

    def ws_headers(self) -> dict | None:
        return None

    async def on_open(self, ws) -> None:
        pass

    def on_text(self, data: str, recv: float) -> None:
        raise NotImplementedError

    def on_binary(self, data: bytes, recv: float) -> None:
        pass

    def kick(self) -> None:
        super().kick()
        ws = self.ws
        if ws is not None and not ws.closed:
            asyncio.ensure_future(ws.close(code=1000, message=b"resume"))

    async def run(self) -> None:
        ok, why = self.available()
        if not ok:
            self.health.state, self.health.detail = "disabled", why
            return
        while True:
            started = time.monotonic()
            try:
                await self._session_once()
            except asyncio.CancelledError:
                raise
            except aiohttp.WSServerHandshakeError as e:
                self.error(e, "connection")
                if e.status == 429:
                    # e.g. Finnhub's free plan allows one stream per key: another app (or a second
                    # copy of this one) holds it. Asking every few seconds would only prolong that.
                    self.refused_until = max(self.refused_until, self.now() + 120)
                    self.health.detail = "stream refused (HTTP 429: key already in use elsewhere, or too many attempts)"
            except Exception as e:  # noqa: BLE001 - any failure means reconnect
                self.error(e, "connection")
            finally:
                self.ws = None
                self.health.connected_since = None
            if time.monotonic() - started >= self.healthy_after:
                self.backoff.reset()
            delay = self.retry_delay()
            self.health.state = "reconnecting"
            reason = self.health.detail if any(w in self.health.detail for w in ("refused", "rejected", "error")) else ""
            self.health.detail = f"{reason} - retry in {delay:.0f}s" if reason else f"retry in {delay:.0f}s"
            await self.sleep(delay)

    refused_until = 0.0

    def retry_delay(self) -> float:
        if self.now() < self.refused_until:
            return max(self.backoff.next(), self.refused_until - self.now())
        return self.backoff.next()

    async def _session_once(self) -> None:
        self.health.state = "connecting"
        async with self.session.ws_connect(
                self.url(), heartbeat=self.heartbeat, headers=self.ws_headers(), max_msg_size=8 * 1024 * 1024,
                timeout=aiohttp.ClientWSTimeout(ws_close=10), autoping=True) as ws:
            self.ws = ws
            self.health.connects += 1
            self.health.state = "connected"
            self.health.detail = ""
            self.health.connected_since = self.now()
            await self.on_open(ws)
            while True:
                limit = self.idle_limit()
                try:
                    msg = await ws.receive(timeout=limit)
                except asyncio.TimeoutError:
                    self.health.detail = f"silent for {limit:.0f}s, reconnecting"
                    self.log.info("%s silent for %.0fs: reconnecting", self.name, limit)
                    return
                recv = self.now()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self.health.mark_msg(recv)
                    try:
                        self.on_text(msg.data, recv)
                    except Exception as e:  # noqa: BLE001 - one bad message must not kill the stream
                        self.error(e, "parse")
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    self.health.mark_msg(recv)
                    try:
                        self.on_binary(msg.data, recv)
                    except Exception as e:  # noqa: BLE001
                        self.error(e, "parse")
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    self.health.detail = f"closed by peer ({ws.close_code})"
                    return
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise ws.exception() or RuntimeError("websocket error")


class RateLimited(RuntimeError):
    """The server answered 429 (too many requests): wait `retry_after` seconds before asking again."""

    def __init__(self, what: str, retry_after: float = 900.0):
        super().__init__(f"{what}: HTTP 429 rate limited; next try in {retry_after:.0f}s")
        self.retry_after = retry_after


class PollConnector(Connector):
    """A REST source polled every `interval` seconds.

    Subclasses either override `poll()`, or implement `fetch()` (download -> JSON-able data) and
    `apply(data)` (hand it to the engine). With `cache_ttl` set, fetched data is also kept on disk
    (data/cache/poll_<name>.json): a restart within the TTL reuses it instead of downloading again,
    which keeps frequent restarts far from the sources' rate limits.
    """
    kind = "poll"
    interval = 300.0
    cache_ttl: float = 0.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.interval = float(cfg.get("interval_s", self.interval))
        self.backoff = Backoff(base=10.0, cap=900.0)

    async def fetch(self):
        raise NotImplementedError

    def apply(self, data) -> None:
        raise NotImplementedError

    async def poll(self) -> None:
        data = await self.fetch()
        self.apply(data)
        if self.cache_ttl:
            self._cache_write(data)

    def _cache_path(self):
        return self.ctx.data_dir / "cache" / f"poll_{self.name}.json"

    def _cache_write(self, data) -> None:
        import json
        try:
            p = self._cache_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps({"saved_at": time.time(), "data": data}), encoding="utf-8")
        except (OSError, TypeError, ValueError) as e:
            self.log.debug("cache write failed: %s", e)

    def cached(self):
        """(data, age_s) from the disk cache, or (None, None)."""
        import json
        try:
            d = json.loads(self._cache_path().read_text(encoding="utf-8"))
            return d["data"], max(0.0, time.time() - float(d["saved_at"]))
        except (OSError, ValueError, KeyError, TypeError):
            return None, None

    def next_interval(self) -> float:
        return self.interval

    async def run(self) -> None:
        ok, why = self.available()
        if not ok:
            self.health.state, self.health.detail = "disabled", why
            return
        if self.cache_ttl:
            data, age = self.cached()
            if data is not None and age < self.cache_ttl:
                try:
                    self.apply(data)
                    self.health.state, self.health.last_ok = "ok", self.now() - age
                    self.health.detail = f"from cache ({age / 60:.0f} min old)"
                    await self.sleep(max(1.0, self.cache_ttl - age))
                except Exception as e:  # noqa: BLE001 - a bad cache must not stop the poller
                    self.error(e, "cache")
        while True:
            try:
                self.health.state = "polling"
                t0 = time.perf_counter()
                standby = await self.poll() == STANDBY
                self.health.last_poll_ms = round((time.perf_counter() - t0) * 1000)
                self.health.state = "ok"
                if not standby:
                    self.health.last_ok = self.now()
                self.health.detail = ""
                self.backoff.reset()
                delay = self.next_interval()
            except asyncio.CancelledError:
                raise
            except RateLimited as e:
                self.error(e, "poll")
                self.health.state = "error"
                self.health.detail = "rate limited by the source: waiting before the next request"
                delay = max(e.retry_after, self.next_interval())
            except Exception as e:  # noqa: BLE001
                self.error(e, "poll")
                self.health.state = "error"
                delay = min(self.next_interval(), self.backoff.next())
            await self.sleep(delay)
