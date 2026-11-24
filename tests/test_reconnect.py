"""Reconnection after drops, silence and sleep/resume, against a local flaky websocket server."""
import asyncio
import types

import aiohttp
from aiohttp import web

from moneyflow import clock
from moneyflow.connectors.base import Backoff, PollConnector, StreamConnector
from moneyflow.engine import Engine
from moneyflow.keys import KeyStore


def ctx_for(session):
    eng = types.SimpleNamespace()
    return types.SimpleNamespace(engine=eng, clock=clock.Clock(), keys=KeyStore(), session=session)


class Probe(StreamConnector):
    name = "probe"
    title = "probe"
    idle_timeout = 5.0
    heartbeat = None

    def __init__(self, ctx, url):
        super().__init__(ctx, {})
        self._url = url
        self.got = []
        self.backoff = Backoff(base=0.05, cap=0.2)

    def url(self):
        return self._url

    def on_text(self, data, recv):
        self.got.append(data)


async def start_server(mode):
    hits = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        hits.append(1)
        await ws.send_str(f"hello {len(hits)}")
        if mode == "drop":
            await ws.close()                       # the server drops every connection at once
        else:
            async for _ in ws:                     # stay open and silent
                pass
        return ws

    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"ws://127.0.0.1:{port}/ws", hits


def test_reconnects_after_server_drops():
    async def go():
        runner, url, hits = await start_server("drop")
        async with aiohttp.ClientSession() as s:
            p = Probe(ctx_for(s), url)
            t = asyncio.create_task(p.run())
            await asyncio.sleep(1.5)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        await runner.cleanup()
        assert p.health.connects >= 3 and len(hits) >= 3
        assert p.got[:2] == ["hello 1", "hello 2"]
    asyncio.run(go())


def test_silent_feed_is_treated_as_dead():
    async def go():
        runner, url, hits = await start_server("silent")
        async with aiohttp.ClientSession() as s:
            p = Probe(ctx_for(s), url)
            p.idle_timeout = 0.3
            t = asyncio.create_task(p.run())
            await asyncio.sleep(1.6)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        await runner.cleanup()
        assert p.health.connects >= 2                  # reconnected after each 0.3 s of silence
    asyncio.run(go())


def test_network_down_then_back():
    """Nothing reachable for a while (like Wi-Fi off), then the server appears: the connector backs
    off without crashing and connects as soon as it can."""
    import socket

    with socket.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        port = sk.getsockname()[1]

    async def go():
        async with aiohttp.ClientSession() as s:
            p = Probe(ctx_for(s), f"ws://127.0.0.1:{port}/ws")
            p.backoff = Backoff(base=0.05, cap=0.3)
            t = asyncio.create_task(p.run())
            await asyncio.sleep(5.0)          # "offline": every attempt fails (Windows takes ~2 s to refuse)
            failed_attempts, state_offline = p.health.errors, p.health.state
            hits = []

            async def handler(request):
                ws = web.WebSocketResponse()
                await ws.prepare(request)
                hits.append(1)
                await ws.send_str("back online")
                async for _ in ws:
                    pass
                return ws

            app = web.Application()
            app.router.add_get("/ws", handler)
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", port).start()   # "online" again
            await asyncio.sleep(3.0)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
            await runner.cleanup()
            return p, failed_attempts, state_offline, hits

    p, failed, state_offline, hits = asyncio.run(go())
    assert failed >= 1 and state_offline in ("reconnecting", "connecting")
    assert hits and p.got[-1] == "back online" and p.health.connects == 1


def test_handshake_429_waits_instead_of_hammering():
    """Finnhub answers a second stream on the same key with HTTP 429 (measured 24 Nov 2026)."""
    async def go():
        attempts = []

        async def handler(request):
            attempts.append(1)
            return web.Response(status=429, text="Too Many Requests")

        app = web.Application()
        app.router.add_get("/ws", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as s:
            p = Probe(ctx_for(s), f"ws://127.0.0.1:{port}/ws")
            t = asyncio.create_task(p.run())
            await asyncio.sleep(1.0)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        await runner.cleanup()
        return p, attempts

    p, attempts = asyncio.run(go())
    assert len(attempts) == 1                                   # no retry storm within the second
    assert p.retry_delay() >= 100 and "HTTP 429" in p.health.detail


def test_quiet_markets_do_not_cause_reconnect_storms(keys):
    """Overnight and at weekends IEX and FX are silent by design: the silence limit stretches to an
    hour (the websocket heartbeat still detects a dead link), instead of reconnecting every 2-10 min."""
    from conftest import ny_ts
    from moneyflow.connectors.alpaca import AlpacaNews, AlpacaStream
    from moneyflow.connectors.finnhub import FinnhubStream

    def ctx_at(ts):
        return types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(fixed=ts), keys=keys, session=None)

    open_ts, night_ts, saturday = ny_ts(2026, 11, 24, 11), ny_ts(2026, 11, 24, 22), ny_ts(2026, 11, 28, 12)
    assert AlpacaStream(ctx_at(open_ts), {}).idle_limit() == 600.0
    assert AlpacaStream(ctx_at(night_ts), {}).idle_limit() == 3600.0
    assert AlpacaStream(ctx_at(saturday), {}).idle_limit() == 3600.0
    assert FinnhubStream(ctx_at(open_ts), {}).idle_limit() == 120.0
    assert FinnhubStream(ctx_at(saturday), {}).idle_limit() == 3600.0
    assert AlpacaNews(ctx_at(open_ts), {}).idle_limit() == 3600.0
    for cls in (AlpacaStream, AlpacaNews, FinnhubStream):
        assert cls.heartbeat and cls.heartbeat <= 30          # the link itself is always checked


def test_finnhub_names_pairs_with_no_tick_yet(keys):
    """The DXY needs all six pairs. After a restart in the 17:00 New York rollover lull one thin pair
    stayed silent for 9+ minutes and the dollar just read "stale": the feed now says which pair."""
    import json
    from conftest import ny_ts
    from moneyflow.connectors.finnhub import FinnhubStream
    from moneyflow.series import Instrument
    t = ny_ts(2026, 11, 24, 17, 10)
    ctx = types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(fixed=t), keys=keys, session=None)
    f = FinnhubStream(ctx, {})
    for s in ("OANDA:EUR_USD", "OANDA:USD_SEK"):
        f.add(s, Instrument(f"finnhub:{s}", "finnhub", s, "fx"))
    f.health.state = "connected"
    f.on_text(json.dumps({"type": "ping"}), t)
    assert f.health.detail == "no tick yet from EUR_USD, USD_SEK"
    f.on_text(json.dumps({"type": "trade", "data": [{"s": "OANDA:EUR_USD", "p": 1.09, "t": t * 1000}]}), t + 0.3)
    assert f.health.detail == "no tick yet from USD_SEK"
    f.on_text(json.dumps({"type": "trade", "data": [{"s": "OANDA:USD_SEK", "p": 10.2, "t": t * 1000}]}), t + 0.3)
    assert f.health.detail == ""


def test_kick_forces_an_immediate_reconnect():
    async def go():
        runner, url, hits = await start_server("silent")
        async with aiohttp.ClientSession() as s:
            p = Probe(ctx_for(s), url)
            p.idle_timeout = 60
            t = asyncio.create_task(p.run())
            await asyncio.sleep(0.4)
            assert p.health.connects == 1
            p.kick()                                    # what the engine does after sleep/resume
            await asyncio.sleep(0.6)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        await runner.cleanup()
        assert p.health.connects == 2
    asyncio.run(go())


def _alpaca_like_server(answer_code=None):
    """A fake Alpaca stream: 'connected', then the login answer (success, or an error code)."""
    seen = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_str('[{"T":"success","msg":"connected"}]')
        async for m in ws:
            j = __import__("json").loads(m.data)
            seen.append(j.get("action"))
            if j.get("action") == "auth":
                if answer_code:
                    await ws.send_str(f'[{{"T":"error","code":{answer_code},"msg":"connection limit exceeded"}}]')
                    await ws.close()
                    break
                await ws.send_str('[{"T":"success","msg":"authenticated"}]')
            elif j.get("action") == "subscribe":
                await ws.send_str('[{"T":"subscription","trades":["TLT"]}]')
                await ws.send_str('[{"T":"t","S":"TLT","p":81.5,"s":100,"t":"2026-11-24T16:30:00.123456789Z"}]')
        return ws
    return handler, seen


def test_alpaca_waits_for_login_and_treats_406_as_refused_not_bad_keys(cfg, keys):
    from moneyflow.connectors.alpaca import AlpacaStream
    from moneyflow.series import Instrument

    async def go(code):
        handler, seen = _alpaca_like_server(code)
        app = web.Application()
        app.router.add_get("/ws", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as s:
            ctx = types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(), keys=keys, session=s)
            a = AlpacaStream(ctx, {"ws_url": f"ws://127.0.0.1:{port}/ws", "quote_symbols": []})
            tlt = Instrument("alpaca:TLT", "alpaca", "TLT", "24/7")
            a.add("TLT", tlt)
            t = asyncio.create_task(a.run())
            await asyncio.sleep(0.8)
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        await runner.cleanup()
        return a, seen, tlt

    a, seen, tlt = asyncio.run(go(406))
    assert seen == ["auth"]                                  # never subscribed on a refused login
    assert not a.auth_failed and a.refused_until > 0         # 406 = connection limit, keys are fine
    assert "connection limit" in a.health.detail and a.retry_delay() >= 60
    a, seen, tlt = asyncio.run(go(None))
    assert seen == ["auth", "subscribe"] and tlt.price == 81.5


def test_poller_backs_off_on_errors_and_recovers():
    class Flaky(PollConnector):
        name = "flaky"
        title = "flaky"

        def __init__(self, ctx):
            super().__init__(ctx, {"interval_s": 0.05})
            self.backoff = Backoff(base=0.01, cap=0.05)
            self.calls = 0

        async def poll(self):
            self.calls += 1
            if self.calls <= 2:
                raise RuntimeError("HTTP 503 token=abcdef123456")

    async def go():
        p = Flaky(ctx_for(None))
        t = asyncio.create_task(p.run())
        await asyncio.sleep(0.5)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return p

    p = asyncio.run(go())
    assert p.calls >= 4 and p.health.errors == 2 and p.health.state in ("ok", "polling")
    assert "abcdef123456" not in p.health.last_error           # errors are redacted


def test_gap_after_sleep_is_redownloaded_not_left_flat(cfg, keys, monkeypatch, tmp_path):
    """After a sleep the 1-second tick carries the last pre-sleep price across the gap. The refill
    must re-download from the moment the PC went to sleep and overwrite that flat stretch."""
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP

    now = 1795528800.0                                    # 2026-11-24 13:20 UTC, on a 5-minute boundary
    eng = Engine(cfg, keys, clock.Clock(fixed=now))
    eng.cache_dir = tmp_path
    inst = eng.instruments["binance_spot:BTCUSDT"]
    sleep_at = now - 3600
    k0 = int((now - 86400) // G5_STEP)
    inst.seed("g5", [(k * G5_STEP, 100.0) for k in range(k0, int(now // G5_STEP))], now)   # flat = stale gap

    async def fake_klines(session, market, symbol, interval, start_ms, end_ms, price="last"):
        step = 300 if interval == "5m" else 60
        t = (start_ms // 1000 // step) * step
        rows = []
        while t + step <= end_ms / 1000:
            rows.append((t, 110.0 if t >= sleep_at else 100.0, 1.0, 0.5))
            t += step
        return rows

    monkeypatch.setattr(bn, "fetch_klines", fake_klines)
    eng.connectors = {}
    eng.cfg = dict(cfg, connectors=dict(cfg["connectors"], yahoo={"enabled": False}))

    async def go():
        await eng.seed_history(incremental=True, since=sleep_at)
    asyncio.run(go())
    k_mid = int((sleep_at + 1800) // G5_STEP)
    assert inst.g5.get(k_mid) == 110.0                    # the gap now holds the real (downloaded) prices
    assert inst.g5.get(int((sleep_at - 3 * 3600) // G5_STEP)) == 100.0


def test_rate_limit_waits_and_restart_reuses_the_disk_cache(tmp_path):
    from moneyflow.connectors.base import RateLimited

    class Src(PollConnector):
        name = "src"
        title = "src"
        cache_ttl = 3600.0

        def __init__(self, ctx, mode):
            super().__init__(ctx, {"interval_s": 0.05})
            self.mode, self.fetches, self.applied, self.delays = mode, 0, [], []

        async def fetch(self):
            self.fetches += 1
            if self.mode == "limited":
                raise RateLimited("https://example.org/feed", 900)
            return {"value": 42}

        def apply(self, data):
            self.applied.append((data, self.fetches))

        async def sleep(self, seconds):
            self.delays.append(seconds)
            await asyncio.sleep(0.01)

    def ctx():
        c = ctx_for(None)
        c.data_dir = tmp_path
        return c

    async def go(src, t=0.15):
        task = asyncio.create_task(src.run())
        await asyncio.sleep(t)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return src

    limited = asyncio.run(go(Src(ctx(), "limited")))
    assert limited.delays and limited.delays[0] >= 900               # waits out the rate limit
    assert "rate limited" in limited.health.detail
    first = asyncio.run(go(Src(ctx(), "ok")))                        # downloads and caches
    assert first.fetches >= 1 and first.applied[0] == ({"value": 42}, 1)
    again = asyncio.run(go(Src(ctx(), "ok"), 0.05))                  # a restart: served from the cache
    assert again.applied[0] == ({"value": 42}, 0)                    # applied before any download
    assert again.delays[0] > 3000                                    # then waits out the cache's life


def test_engine_resume_resyncs_and_kicks_every_connector(cfg, keys):
    async def go():
        eng = Engine(cfg, keys, clock.Clock(fixed=1795500000.0))
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())) as s:
            eng.ctx.session = s
            kicked = []
            eng.connectors = {n: types.SimpleNamespace(kick=lambda n=n: kicked.append(n)) for n in ("a", "b", "c")}
            await eng.on_resume(gap=60)
        return kicked
    assert sorted(asyncio.run(go())) == ["a", "b", "c"]
