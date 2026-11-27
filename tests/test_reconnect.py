"""Reconnection after drops, silence and sleep/resume, against a local flaky websocket server."""
import asyncio
import types

import aiohttp
import pytest
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


def test_finnhub_reconnects_when_its_pairs_are_refused(keys):
    """Seen live on 25 Nov: after a 502 on reconnecting, Finnhub answered "Invalid symbol" for every
    pair and kept the link up with pings alone, so the dollar went stale for good. It now reconnects
    60 s later (doubling to 15 min) while the refused pairs stay silent, and resets once they flow."""
    import json
    from conftest import ny_ts
    from moneyflow.connectors.finnhub import FinnhubStream
    from moneyflow.series import Instrument
    t = ny_ts(2026, 11, 25, 7, 30)
    ctx = types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(fixed=t), keys=keys, session=None)
    f = FinnhubStream(ctx, {})
    for s in ("OANDA:EUR_USD", "OANDA:USD_SEK"):
        f.add(s, Instrument(f"finnhub:{s}", "finnhub", s, "fx"))
    f.health.state = "connected"
    kicks = []
    f.kick = lambda: kicks.append(1)
    msg = lambda **m: json.dumps(m)  # noqa: E731
    both = lambda ts: msg(type="trade", data=[{"s": "OANDA:EUR_USD", "p": 1.09, "t": ts * 1000},  # noqa: E731
                                             {"s": "OANDA:USD_SEK", "p": 10.2, "t": ts * 1000}])
    f.on_text(both(t), t)
    f.on_text(msg(type="error", msg="Invalid symbol OANDA:EUR_USD"), t + 10)
    f.on_text(msg(type="error", msg="Invalid symbol OANDA:USD_SEK"), t + 10)
    f.on_text(msg(type="ping"), t + 50)
    assert kicks == []                                        # waits 60 s first
    f.on_text(msg(type="ping"), t + 75)
    assert kicks == [1] and "refused" in f.health.detail
    f.on_text(msg(type="error", msg="Invalid symbol OANDA:EUR_USD"), t + 80)   # refused again
    f.on_text(msg(type="ping"), t + 180)
    assert kicks == [1]                                       # now waits twice as long (120 s)
    f.on_text(msg(type="ping"), t + 205)
    assert kicks == [1, 1]
    f.on_text(both(t + 300), t + 300)                         # the pairs flow again
    assert f.refused_at is None and f.refusal_wait == f.RETRY_FIRST
    f.on_text(msg(type="ping"), t + 2000)
    assert kicks == [1, 1]


def _all_stream_classes():
    from moneyflow.connectors import alpaca, binance, coinbase, finnhub, hyperliquid  # noqa: F401 (registers them)

    def walk(c):
        for s in c.__subclasses__():
            yield s
            yield from walk(s)
    return sorted({c for c in walk(StreamConnector) if c.__module__.startswith("moneyflow.")
                   and not c.__name__.startswith("_")}, key=lambda c: c.__name__)


def test_every_stream_connector_keeps_reconnecting_through_its_real_run_loop(keys):
    """Each stream's REAL run() loop through a session that ends (closed by the peer, or kick()) and one
    that fails (network down): both must lead to another try. On 25 Nov Finnhub's loop died on its first
    reconnect (an attribute named like the base's retry_delay() method: TypeError), so the dollar stayed
    stale until a restart; the old test of that feature mocked kick() and never ran the loop."""
    classes = _all_stream_classes()
    assert {"AlpacaNews", "AlpacaStream", "BinanceFutures", "BinanceSpot", "CoinbaseStream", "FinnhubStream",
            "HyperliquidStream"} <= {c.__name__ for c in classes}
    for cls in classes:
        ctx = types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(), keys=keys, session=None)
        c = cls(ctx, {})
        calls = []

        async def session_once(c=c, calls=calls):
            calls.append(c.health.state)
            if len(calls) == 1:
                return                                         # the peer closed it, or kick()
            if len(calls) == 2:
                raise OSError("network down")
            raise asyncio.CancelledError                       # end the test

        async def no_sleep(seconds):
            return None

        c.available = lambda: (True, "")
        c._session_once = session_once
        c.sleep = no_sleep

        async def go(c=c):
            try:
                await c.run()
            except asyncio.CancelledError:
                pass
        asyncio.run(go())
        assert len(calls) == 3, cls.__name__                   # reconnected after each, without crashing
        assert c.health.state == "reconnecting" and "network down" in (c.health.last_error or ""), cls.__name__


def test_no_connector_attribute_shadows_a_base_method(keys):
    """The Finnhub bug's class of error, for every connector: an instance attribute set in __init__ must
    not hide a method of the class (the loop then calls a number)."""
    from moneyflow import stocks  # noqa: F401 - every connector class must exist before the walk
    from moneyflow.connectors import alpaca, binance, coinbase, finnhub, hyperliquid, pollers  # noqa: F401
    from moneyflow.connectors.base import Connector

    def walk(c):
        for s in c.__subclasses__():
            yield s
            yield from walk(s)
    for cls in {c for c in walk(Connector) if c.__module__.startswith("moneyflow.")}:
        ctx = types.SimpleNamespace(engine=types.SimpleNamespace(), clock=clock.Clock(), keys=keys, session=None)
        try:
            c = cls(ctx, {})
        except TypeError:                                      # needs more arguments (e.g. AlpacaFallback)
            continue
        hidden = [a for a, v in vars(c).items() if callable(getattr(cls, a, None)) and not callable(v)]
        assert not hidden, (cls.__name__, hidden)


def test_a_crashed_connector_is_logged_and_restarted(cfg, keys, caplog):
    """Whatever the bug, a crashed feed comes back: the engine logs the crash with its traceback, shows it
    on the source and restarts the connector with back-off."""
    eng = Engine(cfg, keys, clock.Clock())
    eng.RESTART_FIRST = 0.01
    p = Probe(ctx_for(None), "ws://unused")
    runs = []

    async def run():
        runs.append(1)
        if len(runs) == 1:
            raise TypeError("'float' object is not callable")
        p.health.state = "connected"                           # the second run keeps going normally

    p.run = run
    with caplog.at_level("ERROR", logger="moneyflow.engine"):
        asyncio.run(eng._supervise("probe", p))
    assert runs == [1, 1] and p.health.errors == 1 and "crashed" in p.health.last_error
    rec = next(r for r in caplog.records if "connector probe crashed" in r.getMessage())
    assert "TypeError" in rec.getMessage() or "TypeError" in (rec.exc_text or "") or rec.exc_info


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
    """After a sleep the 1-second tick used to carry the last pre-sleep price across the gap. Through
    the production path (housekeeping sees the wall clock jump, on_resume starts the back-fill), the
    gap is downloaded again from the moment the PC went to sleep: the exchange's prices, not flat."""
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP

    t0 = 1795528800.0                                     # 2026-11-24 13:20 UTC, on a 5-minute boundary
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    inst = eng.instruments["binance_spot:BTCUSDT"]
    k0 = int((t0 - 86400) // G5_STEP)
    inst.seed("g5", [(k * G5_STEP, 100.0) for k in range(k0, int(t0 // G5_STEP))], t0)
    _tick(eng, clk, t0, t0 + 300, feed=lambda t: inst.on_price(100.0, t, t))     # awake, live
    sleep_at = t0 + 300
    clk.set_fixed(sleep_at + 3600)                        # an hour asleep
    eng.tick()                                            # the first tick after waking
    klines, _ = _exchange(lambda t: 110.0 if t > sleep_at else 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)

    async def go():
        assert await eng._housekeep_once(3600.0) is True  # the wall clock jumped: the PC slept
        await asyncio.wait_for(eng._backfill_task, timeout=5)
    asyncio.run(go())
    k_mid = int((sleep_at + 1800) // G5_STEP)
    assert inst.g5.get(k_mid) == 110.0 and inst.g5.is_live(k_mid)   # the real (downloaded) prices
    assert inst.g5.get(int((t0 - 3 * 3600) // G5_STEP)) == 100.0    # before the sleep: untouched
    assert eng.backfill_state["state"] == "done" and eng.backfill_state["reason"] == "wake"


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


# ── a sleep or a network outage: empty steps, never the old price as live; back-filled with retries ──
def _gap_engine(cfg, keys, t0, tmp_path):
    clk = clock.Clock(fixed=t0)
    eng = Engine(cfg, keys, clk)
    eng.cache_dir = tmp_path
    eng.connectors = {}
    eng.cfg = dict(cfg, connectors=dict(cfg["connectors"], yahoo={"enabled": False}))
    eng.seed_state["state"] = "done"
    eng.BACKFILL_SETTLE, eng.BACKFILL_FIRST, eng.BACKFILL_MAX = 0.0, 0.01, 0.02
    return eng, clk


def _exchange(truth, fail):
    """Binance's klines of `truth(t)` (the close at the step's end); `fail()` says when DNS is down."""
    calls = []
    starts: dict = {}

    async def klines(session, market, symbol, interval, start_ms, end_ms, price="last"):
        calls.append(symbol)
        starts.setdefault((symbol, interval), []).append(start_ms / 1000)
        if fail():
            raise OSError(11001, "getaddrinfo failed")
        step = 300 if interval == "5m" else 60
        t = -(-start_ms // 1000 // step) * step if start_ms % (step * 1000) else start_ms // 1000
        rows = []                                            # (like Binance: bars opening at or after startTime)
        while t + step <= end_ms / 1000:
            rows.append((t, truth(t + step), 1.0, 0.5))
            t += step
        return rows
    klines.starts = starts
    return klines, calls


def test_wake_with_dns_down_then_back_fills_the_gap_and_leaves_nothing_carried_as_live(cfg, keys, monkeypatch, tmp_path):
    """25 Nov: the PC woke, DNS was down, every refill request failed and the one-shot refill never
    tried again, so the night stayed in the history as the pre-sleep price, flagged live (Bitcoin flat
    for 26 steps, σ too low). Now the steps that passed asleep are EMPTY (not live), and the back-fill
    retries with back-off until DNS is back, then fills them with the exchange's own prices."""
    from moneyflow import stats
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    t0 = 1795528800.0                                      # on a 5-minute boundary
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    truth = lambda t: 100.0 + (t - t0) / 3600.0             # noqa: E731 - the exchange: +1 an hour
    for s in range(0, 601, 10):                             # 10 minutes of live ticks
        clk.set_fixed(t0 + s)
        btc.on_price(truth(t0 + s), t0 + s, t0 + s)
        eng.tick()
    wake = t0 + 600 + 7200                                   # two hours asleep
    clk.set_fixed(wake)
    eng.tick()                                               # the first tick after waking closes those steps
    asleep = range(int((t0 + 900) // G5_STEP), int((wake - 300) // G5_STEP))
    assert all(btc.g5.get(k) != btc.g5.get(k) and not btc.g5.is_live(k) for k in asleep)   # empty, not carried
    assert btc.holes["g5"] is not None and btc.holes["g1"] is not None
    assert stats.price_at(btc, wake - 1800, wake) != stats.price_at(btc, wake - 1800, wake)  # unknown, not 100.17
    attempts = lambda: eng.backfill_state.get("attempt", 0)  # noqa: E731
    klines, calls = _exchange(truth, fail=lambda: attempts() <= 2)                 # DNS down for two tries
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done" and eng.backfill_state["attempts"] == 3
    assert btc.holes == {"g1": None, "g5": None}
    assert set(calls) == {"BTCUSDT"}                         # only what had holes was downloaded again
    for k in asleep:                                         # the exchange's prices, live
        assert btc.g5.get(k) == truth((k + 1) * G5_STEP) and btc.g5.is_live(k)
    assert stats.price_at(btc, wake - 1800, wake) == truth(wake - 1800)
    assert len(set(btc.g5.get(k) for k in asleep)) == len(asleep)                   # no flat stretch left


def test_an_outage_while_awake_is_back_filled_once_the_feed_is_back(cfg, keys, monkeypatch, tmp_path):
    """No sleep, the network just drops: the feed falls silent (no heartbeat), its steps are empty, and
    the back-fill starts when it delivers again (a recovered feed cuts the back-off short once)."""
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G1_STEP
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    truth = lambda t: 200.0 - (t - t0) / 600.0              # noqa: E731
    for s in range(0, 1801, 5):                              # live, then silent 10:00-25:00, then live again
        clk.set_fixed(t0 + s)
        if not 600 <= s < 1500:
            btc.on_price(truth(t0 + s), t0 + s, t0 + s)
        eng.tick()
    silent = range(int((t0 + 660) // G1_STEP), int((t0 + 1500) // G1_STEP))
    assert all(btc.g1.get(k) != btc.g1.get(k) for k in silent) and btc.recovered
    klines, _ = _exchange(truth, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng._backfill("the feeds are back"))
    assert all(btc.g1.get(k) == truth((k + 1) * G1_STEP) and btc.g1.is_live(k) for k in silent)


def test_housekeeping_starts_the_backfill_and_a_recovered_feed_cuts_its_back_off_short(cfg, keys, tmp_path):
    """Every 5 s: holes start the back-fill; while it waits out a back-off, a feed that delivers again
    wakes it, once per recovery (not every 5 s while the exchange's REST may still be failing)."""
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    started = []
    eng.want_backfill = started.append
    eng.check_holes()
    assert started == []                                      # no holes, nothing to do
    btc.holes["g5"] = 1795528800.0 - 3600
    eng.check_holes()
    assert started == ["network outage"]
    eng._backfill_task = types.SimpleNamespace(done=lambda: False)      # running, waiting out a back-off
    eng.check_holes()
    assert started == ["network outage"]                      # not restarted, not woken
    btc.recovered = True                                      # the feed delivers again
    eng.check_holes()
    eng.check_holes()
    assert started == ["network outage", "the feeds are back"] and not btc.recovered


def test_a_gap_the_exchange_has_no_data_for_is_given_up_not_retried_forever(cfg, keys, monkeypatch, tmp_path):
    from moneyflow.connectors import binance as bn
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    btc.on_price(100.0, t0, t0)
    eng.tick()
    clk.set_fixed(t0 + 3600)
    eng.tick()
    calls = []

    async def nothing(session, market, symbol, interval, start_ms, end_ms, price="last"):
        calls.append(symbol)
        return []                                            # answered, but no data for that range
    monkeypatch.setattr(bn, "fetch_klines", nothing)
    asyncio.run(asyncio.wait_for(eng._backfill("wake"), timeout=5))
    assert btc.holes == {"g1": None, "g5": None} and eng.backfill_state["state"] == "done"


def test_an_old_cache_with_a_flat_live_night_is_downloaded_again(cfg, keys, monkeypatch, tmp_path):
    """A cache from before this fix (version 1) can hold a sleep's flat stretch flagged live, like
    the 25 Nov night. Its whole range is downloaded again at the next start; a newer cache's empty
    steps (a past outage) are downloaded too, not left as holes for good."""
    import json
    from moneyflow import history
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = 1795528800.0 + 37.5                                # not on a minute: as a real start
    truth = lambda t: 100.0 + (t - now) / 86400.0           # noqa: E731
    old, _ = _gap_engine(cfg, keys, now, tmp_path)
    btc = old.instruments["binance_spot:BTCUSDT"]
    k_now = int(now // G5_STEP)
    rows = [(k * G5_STEP, truth((k + 1) * G5_STEP)) for k in range(k_now - 2000, k_now)]
    flat = range(k_now - 300, k_now - 274)                    # 26 steps carried at one price
    rows = [(t, rows[int(t // G5_STEP) - k_now + 2000 - 1][1] if int(t // G5_STEP) in flat else c)
            for t, c in rows]
    btc.seed("g5", rows, now)
    k1 = int(now // 60)                                      # and a day of 1-minute closes (its first minute
    btc.seed("g1", [(k * 60, truth((k + 1) * 60)) for k in range(k1 - 1440, k1)], now)   # is never downloaded)
    assert all(btc.g5.is_live(k) for k in flat)
    history.save(tmp_path, btc)
    f = next(tmp_path.glob("binance_spot_BTCUSDT.json"))
    d = json.loads(f.read_text())
    d["v"] = 1                                               # written by the old code
    f.write_text(json.dumps(d))
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    klines, calls = _exchange(truth, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    b2 = eng.instruments["binance_spot:BTCUSDT"]
    assert all(b2.g5.get(k) == truth((k + 1) * G5_STEP) for k in flat)                # the real prices now
    assert b2.holes == {"g1": None, "g5": None}
    assert json.loads(f.read_text())["v"] == history.VERSION == 5
    # a version-2 cache with empty steps (a past outage): marked, and filled at the next start
    k_hole = k_now - 100
    b2.g5.put(k_hole, float("nan"), False)
    history.save(tmp_path, b2)
    eng3, _ = _gap_engine(cfg, keys, now, tmp_path)
    b3 = eng3.instruments["binance_spot:BTCUSDT"]
    history.load(tmp_path, b3, now)
    assert b3.holes["g5"] == k_hole * G5_STEP
    asyncio.run(eng3.seed_history(incremental=True))
    assert b3.g5.get(k_hole) == truth((k_hole + 1) * G5_STEP) and b3.holes["g5"] is None



def test_a_quiet_market_on_a_live_feed_is_covered_by_the_feeds_heartbeat(cfg, keys, tmp_path):
    """No new price for minutes is normal for a quiet market: while the instrument's connector keeps
    delivering (pings, other symbols), its steps carry the price and stay live, with no hole."""
    from moneyflow.series import G1_STEP
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    health = types.SimpleNamespace(last_msg=None, last_ok=None)
    eng.connectors = {"binance_spot": types.SimpleNamespace(health=health)}
    btc = eng.instruments["binance_spot:BTCUSDT"]
    btc.on_price(100.0, t0, t0)
    for s in range(0, 901, 5):                                # 15 quiet minutes, the feed pinging
        clk.set_fixed(t0 + s)
        health.last_msg = t0 + s
        eng.tick()
    ks = range(int(t0 // G1_STEP), int((t0 + 840) // G1_STEP))
    assert all(btc.g1.get(k) == 100.0 and btc.g1.is_live(k) for k in ks) and btc.holes == {"g1": None, "g5": None}



# ── the back-fill, round 8: the dollar, a failed first download, long sleeps, the event loop ─────
def _tick(eng, clk, t0, t1, step=10.0, feed=None):
    t = t0
    while t <= t1:
        clk.set_fixed(t)
        if feed:
            feed(t)
        eng.tick()
        t += step


def test_the_dollar_is_back_filled_from_ice_when_its_fx_legs_have_a_gap(cfg, keys, monkeypatch, tmp_path, caplog):
    """The dollar is computed from six FX legs with no history of their own; its history is ICE's DXY.
    A sleep over 16:25-17:00 New York left the legs' steps empty, and the back-fill (downloading only
    what has holes) never asked ICE: TODAY had no start price till the next roll, ~20 h later. ICE's
    bars over the legs' gap fill it: back-filled, not given up as "no exchange data"."""
    import logging
    caplog.set_level(logging.INFO, logger="moneyflow.engine")
    from conftest import ny_ts
    from moneyflow import stats
    from moneyflow.connectors import pollers
    t0 = ny_ts(2026, 11, 24, 16, 15)
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))
    usd = eng.series["usd"].inst
    legs = list(usd.inputs)
    _tick(eng, clk, t0, t0 + 600, feed=lambda t: [leg.on_price(1.0 + i / 10, t, t) for i, leg in enumerate(legs)])
    wake = ny_ts(2026, 11, 24, 17, 30)
    clk.set_fixed(wake)
    eng.tick()
    roll = ny_ts(2026, 11, 24, 17, 0)
    assert all(leg.holes["g5"] is not None for leg in legs)
    ice = lambda t: 100.0 + (t - t0) / 86400                  # noqa: E731
    before = stats.price_at(usd, roll, wake)                  # TODAY's start: none, or the stale 16:25 value
    assert before != ice(roll)

    async def chart(session, symbol, interval, rng, now=None):
        assert symbol == "DX-Y.NYB"
        step = 300 if interval == "5m" else 60
        span = 60 * 86400 if rng == "60d" else 86400
        k0, k1 = int((wake - span) // step), int((wake - 900) // step)       # ICE: 10-15 minutes late
        return [(k * step, ice((k + 1) * step)) for k in range(k0, k1)]
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done" and all(leg.holes == {"g1": None, "g5": None} for leg in legs)
    assert stats.price_at(usd, roll, wake) == ice(roll)                                 # TODAY starts again
    assert "back-filled" in caplog.text and "no exchange data" not in caplog.text


def test_a_failed_first_download_keeps_the_time_off_as_a_hole_not_the_cached_price(cfg, keys, monkeypatch, tmp_path):
    """The app was off for 6 hours; at the start the first download fails (DNS down for a minute).
    The time it was off stays a hole, so the retry fetches it (it fetched only the minutes since the
    start, and the cache's last close stood in as TODAY's start price for the whole session); and
    the steps after the start are unknown, not that old close carried as data."""
    from moneyflow import history
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = 1795528800.0
    closed = now - 6 * 3600
    truth = lambda t: 100.0 + (t - now) / 3600              # noqa: E731
    old, _ = _gap_engine(cfg, keys, closed, tmp_path)
    b0 = old.instruments["binance_spot:BTCUSDT"]
    k_c = int(closed // G5_STEP)
    b0.seed("g5", [(k * G5_STEP, truth((k + 1) * G5_STEP)) for k in range(k_c - 2000, k_c)], closed)
    history.save(tmp_path, b0)
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    klines, _ = _exchange(truth, fail=lambda: eng.seed_state.get("retry", 0) == 0)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                        # the first download: DNS down
    btc = eng.instruments["binance_spot:BTCUSDT"]
    assert btc.holes["g5"] == closed
    _tick(eng, clk, now, now + 900)                        # 15 minutes, no live data yet
    k_now = int(now // G5_STEP)
    assert all(btc.g5.get(k) != btc.g5.get(k) for k in range(k_now, k_now + 2))        # not the cached close
    eng.seed_state["retry"] = 1
    asyncio.run(eng.seed_history(incremental=True))        # the retry, the network back
    gap = range(k_c + 1, k_now + 2)
    assert all(btc.g5.get(k) == truth((k + 1) * G5_STEP) for k in gap) and btc.holes["g5"] is None


def test_the_time_off_is_a_gap_only_from_when_the_market_was_open(cfg, keys, tmp_path):
    """An ETF's cache written on a Saturday. Started on Sunday nothing is missing: the market stayed
    shut (no download, no "no exchange data" give-up). Started on Monday at 10:00 the gap begins at
    Monday's open, 09:30 New York."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP
    sat = ny_ts(2026, 11, 21, 15, 0)
    old, _ = _gap_engine(cfg, keys, sat, tmp_path)
    etf = old.instruments["alpaca:SPY"]
    k_s = int(sat // G5_STEP)
    etf.seed("g5", [(k * G5_STEP, 600.0) for k in range(k_s - 2000, k_s)], sat)
    history.save(tmp_path, etf)
    for when, hole in ((ny_ts(2026, 11, 22, 12, 0), None), (ny_ts(2026, 11, 23, 10, 0), ny_ts(2026, 11, 23, 9, 30))):
        eng, _ = _gap_engine(cfg, keys, when, tmp_path)
        inst = eng.instruments["alpaca:SPY"]
        history.load(tmp_path, inst, when)
        assert inst.holes["g5"] == hole, clock.iso(when)


def test_after_a_long_sleep_each_instrument_downloads_from_its_own_gap_not_35_days(cfg, keys, monkeypatch, tmp_path):
    """After a sleep of over 4 hours the newest 50 steps are all empty: the start of the download is
    the newest value anywhere (not 35 days ago), and each instrument starts from its own gap."""
    from moneyflow.connectors import binance as bn
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc, eth = eng.instruments["binance_spot:BTCUSDT"], eng.instruments["binance_spot:ETHUSDT"]
    truth = lambda t: 100.0 + (t - t0) / 3600               # noqa: E731
    _tick(eng, clk, t0, t0 + 600, feed=lambda t: (btc.on_price(truth(t), t, t), eth.on_price(truth(t), t, t)))
    _tick(eng, clk, t0 + 610, t0 + 3 * 3600, feed=lambda t: btc.on_price(truth(t), t, t))   # ETH's feed dies
    clk.set_fixed(t0 + 3 * 3600 + 5 * 3600)                # then 5 hours asleep
    eng.tick()
    holes = eng.holes()
    klines, _ = _exchange(truth, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng._backfill("wake"))
    st = klines.starts
    assert st[("BTCUSDT", "5m")][0] == holes["binance_spot:BTCUSDT"] - 600            # its own gap...
    assert st[("ETHUSDT", "5m")][0] == holes["binance_spot:ETHUSDT"] - 600            # ...not the earliest
    assert st[("ETHUSDT", "5m")][0] > t0 - 3600                                        # nor 35 days


def test_a_backfill_that_downloads_nothing_does_not_stall_the_loop(cfg, keys, monkeypatch, tmp_path):
    """With DNS down after a wake every attempt fails at once; each used to save every cache file and
    rebuild 35 days of the derived series on the event loop (4-8 s of frozen ticks and pushes). Now a
    failed attempt does neither, and a good one rebuilds only what its downloads feed, each from
    its own inputs' gap (not from another instrument's older one)."""
    from moneyflow.connectors import binance as bn
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    _tick(eng, clk, t0, t0 + 300, feed=lambda t: btc.on_price(100.0, t, t))
    clk.set_fixed(t0 + 3600)
    eng.tick()
    saves, rebuilds = [], []
    eng.save_cache = lambda insts=None: saves.append(1)
    for d in eng.derived:
        d.rebuild = lambda until, since=None, d=d: rebuilds.append((type(d).__name__, since))
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: eng.backfill_state.get("attempt", 0) <= 2)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    hole = min(h for h in btc.holes.values() if h is not None)
    eng.instruments["binance_fut:UVXYUSDT"].holes["g5"] = t0 - 10 * 86400    # an old gap, in no computed series
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["attempts"] == 3 and saves == [1]                         # only the good one
    users = {type(d).__name__ for d in eng.derived if btc in d.inputs}
    assert {n for n, _ in rebuilds} == users and all(s is not None and t0 - 3600 < s <= hole for _, s in rebuilds)


def test_a_backfill_rebuilds_the_computed_series_over_the_gap(cfg, keys, monkeypatch, tmp_path):
    """The coins slept with the PC; once their gap is back-filled, the stablecoin share computed from
    them is rebuilt over the gap (the rebuild must start where the downloads did: taken after them,
    with the holes gone and the newest value recent, it covered only the last minutes)."""
    from conftest import filled_engine_at
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP, isnum
    t0 = 1795528800.0
    eng = filled_engine_at(cfg, keys, t0)
    clk = eng.clock
    eng.cache_dir, eng.connectors = tmp_path, {}
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": False}))
    eng.BACKFILL_SETTLE, eng.BACKFILL_FIRST, eng.BACKFILL_MAX = 0.0, 0.01, 0.02
    coins = [i for i in eng.instruments.values() if i.source == "binance_spot"]
    truth = lambda t: 50.0 * (1 + (t - t0) / 86400)          # noqa: E731
    _tick(eng, clk, t0, t0 + 600, feed=lambda t: [c.on_price(truth(t), t, t) for c in coins])
    wake = t0 + 600 + 3600
    clk.set_fixed(wake)
    eng.tick()
    klines, _ = _exchange(truth, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done"
    share = next(d for d in eng.derived if type(d).__name__ == "StablecoinShare")
    gap = range(int((t0 + 900) // G5_STEP), int((wake - 300) // G5_STEP))
    want = [share.value_step("g5", k) for k in gap]
    assert all(isnum(v) for v in want) and len(set(want)) > 1
    assert [share.g5.get(k) for k in gap] == want


def test_a_wake_and_housekeeping_are_what_start_the_backfill(cfg, keys, tmp_path):
    """The production entry points: a wall-clock jump (the PC slept) starts the back-fill; awake,
    housekeeping finds the holes an outage left and starts it too."""
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    started = []
    eng.want_backfill = started.append

    async def go():
        assert await eng._housekeep_once(600.0) is True          # the PC slept 10 minutes
        assert await eng._housekeep_once(5.0) is False           # awake, nothing missing
        eng.instruments["binance_spot:BTCUSDT"].holes["g5"] = 1795528800.0 - 600
        await eng._housekeep_once(5.0)
    asyncio.run(go())
    assert started == ["wake", "network outage"] and eng.resumes == 1


def test_a_gap_that_appears_during_a_download_is_fetched_next_not_given_up(cfg, keys, monkeypatch, tmp_path):
    """A hole registered while a download is in flight was never asked for: it is not "no exchange
    data", it is the next attempt's job."""
    from moneyflow.connectors import binance as bn
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc, eth = eng.instruments["binance_spot:BTCUSDT"], eng.instruments["binance_spot:ETHUSDT"]
    btc.holes["g5"] = t0 - 3600
    klines, calls = _exchange(lambda t: 100.0, fail=lambda: False)

    async def during(session, market, symbol, interval, start_ms, end_ms, price="last"):
        if symbol == "BTCUSDT" and eth.holes["g5"] is None and "ETHUSDT" not in calls:
            eth.holes["g5"] = t0 - 1800                      # ETH's feed just dropped
        return await klines(session, market, symbol, interval, start_ms, end_ms, price)
    monkeypatch.setattr(bn, "fetch_klines", during)
    asyncio.run(eng._backfill("wake"))
    assert "ETHUSDT" in calls and eng.holes() == {} and eng.backfill_state["attempts"] == 2


def test_feeds_coming_back_during_a_download_cut_the_back_off_short(cfg, keys, monkeypatch, tmp_path):
    """A "feeds are back" that arrives while an attempt is downloading is kept: the retry follows
    at once instead of after the full back-off (up to 15 minutes)."""
    import time
    from moneyflow.connectors import binance as bn
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    eng.BACKFILL_FIRST = eng.BACKFILL_MAX = 60.0
    eng.instruments["binance_spot:BTCUSDT"].holes["g5"] = 1795528800.0 - 3600
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: eng.backfill_state.get("attempt", 0) == 1)

    async def during(*a, **k):
        if eng.backfill_state.get("attempt") == 1:
            eng.want_backfill("the feeds are back")          # the websockets reconnected meanwhile
        return await klines(*a, **k)
    monkeypatch.setattr(bn, "fetch_klines", during)

    async def go():                                         # as in production: one back-fill task
        eng.want_backfill("wake")
        await asyncio.wait_for(eng._backfill_task, timeout=20)
    t = time.monotonic()
    asyncio.run(go())
    assert time.monotonic() - t < 10 and eng.backfill_state["attempts"] == 2


def test_a_kick_while_a_connector_is_busy_is_not_lost():
    """A kick that arrives while the connector works (a scan in flight at the 17:00 roll) still ends
    its next sleep at once; the sleep after that is a normal one."""
    import time
    p = Probe(ctx_for(None), "ws://unused")

    async def go():
        p.kick()                                             # during "work"
        t = time.monotonic()
        await p.sleep(5)
        first = time.monotonic() - t
        t = time.monotonic()
        await p.sleep(0.2)
        return first, time.monotonic() - t
    first, second = asyncio.run(go())
    assert first < 0.5 and second >= 0.19


def test_an_fx_or_rest_fed_market_is_covered_by_its_own_feeds_heartbeat(cfg, keys, tmp_path):
    """US ETFs come from Alpaca's stream or, when that is refused, its REST poller: either one's
    activity covers a quiet ETF (FEEDS), so it gets no hole."""
    from conftest import ny_ts
    t0 = ny_ts(2026, 11, 24, 11, 0)
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    health = types.SimpleNamespace(last_msg=None, last_ok=None)
    eng.connectors = {"alpaca_rest": types.SimpleNamespace(health=health)}
    etf = next(i for i in eng.instruments.values() if i.source == "alpaca")
    etf.on_price(90.0, t0, t0)

    def poll(t):
        health.last_ok = t                                   # the REST poller answered (no new trade)
    _tick(eng, clk, t0, t0 + 900, feed=poll)
    assert etf.holes == {"g1": None, "g5": None}



def test_keyless_etfs_download_60_days_when_a_gap_is_older_than_the_5_day_range(cfg, keys, monkeypatch, tmp_path):
    """Without an Alpaca key the ETFs' history comes from Yahoo: 5 days for a recent cache, 60 when a
    gap in it is older than that."""
    from moneyflow.connectors import pollers
    now = 1795528800.0
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))
    etf = next(i for i in eng.instruments.values() if i.source == "alpaca")
    k = int(now // 300)
    etf.seed("g5", [(j * 300, 90.0) for j in range(k - 3000, k)], now)    # a recent cache...
    etf.holes["g5"] = now - 10 * 86400                                    # ...with a 10-day-old gap
    asked = []

    async def chart(session, symbol, interval, rng, now=None):
        asked.append((symbol, interval, rng))
        return []
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng.seed_history(incremental=True, only={etf.key}, state={}))
    assert (etf.symbol, "5m", "60d") in asked



# ── round 9: the cache written off the loop, the downloaded price, a day or more off ────────────────
def _slow_save(monkeypatch, seconds=0.004):
    """history.save replaced by a stand-in as slow as a file write; records which threads ran it and
    how many saves overlapped."""
    import threading
    import time
    from moneyflow import history
    rec = {"threads": set(), "keys": [], "active": 0, "peak": 0}
    lock = threading.Lock()

    def save(dirpath, inst):
        with lock:
            rec["active"] += 1
            rec["peak"] = max(rec["peak"], rec["active"])
            rec["threads"].add(threading.get_ident())
        time.sleep(seconds)
        with lock:
            rec["active"] -= 1
            rec["keys"].append(inst.key)
        return True
    monkeypatch.setattr(history, "save", save)
    return rec


def test_a_save_off_the_loop_survives_stocks_picked_and_dropped_meanwhile(cfg, keys, monkeypatch, tmp_path):
    """The cache is written in a worker thread while the loop picks and drops stocks. Iterating the
    live dict there raised "dictionary changed size during iteration" (after 6 of 26 files), which
    once ended housekeeping for good; the save works on the list taken on the loop."""
    import threading
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    rec = _slow_save(monkeypatch)
    before = list(eng.instruments)

    async def go():
        task = asyncio.create_task(eng._save_cache_off_loop())
        await asyncio.sleep(0.03)                                   # the thread is writing...
        eng.instrument("binance_fut", "NVDAUSDT", "24/7", True, "NVDA", "index")   # ...a stock is picked
        eng.instruments.pop("binance_spot:XRPUSDT")                 # ...and one is dropped
        await task
    asyncio.run(go())
    assert sorted(rec["keys"]) == sorted(before) and threading.get_ident() not in rec["threads"]


def test_two_saves_at_once_take_turns(cfg, keys, monkeypatch, tmp_path):
    """Housekeeping's save and a back-fill's can overlap: both wrote the same temporary files."""
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    rec = _slow_save(monkeypatch, 0.002)

    async def go():
        await asyncio.gather(eng._save_cache_off_loop(), eng._save_cache_off_loop())
    asyncio.run(go())
    assert rec["peak"] == 1 and len(rec["keys"]) == 2 * len(eng.instruments)


def test_housekeeping_goes_on_after_an_error(cfg, keys, tmp_path, caplog):
    """Nothing supervises housekeeping: one error (a failed save) ended wake detection, back-fills,
    hole checks, NTP and saves for the rest of the run. Now it is logged and the next round runs."""
    import logging
    caplog.set_level(logging.ERROR, logger="moneyflow.engine")
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    eng.HOUSEKEEP_EVERY = 0.01
    rounds = []

    async def once(gap):
        rounds.append(gap)
        if len(rounds) == 1:
            raise RuntimeError("dictionary changed size during iteration")
        return False
    eng._housekeep_once = once

    async def go():
        task = asyncio.create_task(eng._housekeeping())
        await asyncio.sleep(0.3)
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())
    assert len(rounds) >= 3 and "housekeeping round failed" in caplog.text


def test_a_backfill_writes_the_cache_in_a_worker_thread(cfg, keys, monkeypatch, tmp_path):
    """Writing every cache file on the event loop stalled ticks and pushes for a second or more
    (the other tests replace save_cache, so they cannot tell which thread runs it)."""
    import threading
    from moneyflow.connectors import binance as bn
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    _tick(eng, clk, t0, t0 + 300, feed=lambda t: btc.on_price(100.0, t, t))
    clk.set_fixed(t0 + 3600)
    eng.tick()
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    rec = _slow_save(monkeypatch, 0.0)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done"
    assert rec["keys"] and threading.get_ident() not in rec["threads"]


def test_a_gap_of_the_same_instrument_opening_after_its_download_is_fetched_next(cfg, keys, monkeypatch, tmp_path, caplog):
    """BTC's download has landed; while the cache is written its feed misses a step. That newer gap
    was not asked for, so the next attempt fetches it (with any answered instrument counted as
    "asked", it was given up as "no exchange data")."""
    import logging
    from moneyflow.connectors import binance as bn
    caplog.set_level(logging.INFO, logger="moneyflow.engine")
    t0 = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    btc.holes["g5"] = t0 - 3600
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    saves = []

    def save(insts=None):
        if not saves:                                         # the loop ticks on meanwhile
            clk.set_fixed(t0 + 900)
            btc._hole("g5", t0 + 300)
        saves.append(1)
    eng.save_cache = save
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["attempts"] == 2 and btc.holes == {"g1": None, "g5": None}
    assert "no exchange data" not in caplog.text


def test_a_failed_first_download_leaves_the_minutes_since_the_start_unknown_too(cfg, keys, monkeypatch, tmp_path):
    """As with the 5-minute steps, with a cache that holds 1-minute steps too (as every real one
    does): until the first live price the minutes after the start are unknown, not the 6-hour-old
    close, so TODAY waits for data instead of reading the night's move from that old price."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G1_STEP, G5_STEP
    now = ny_ts(2026, 11, 24, 16, 50)
    closed = now - 6 * 3600
    truth = lambda t: 100.0 + (t - now) / 3600 * 3           # noqa: E731
    old, _ = _gap_engine(cfg, keys, closed, tmp_path)
    b0 = old.instruments["binance_spot:BTCUSDT"]
    k5, k1 = int(closed // G5_STEP), int(closed // G1_STEP)
    b0.seed("g5", [(k * G5_STEP, truth((k + 1) * G5_STEP)) for k in range(k5 - 2000, k5)], closed)
    b0.seed("g1", [(k * G1_STEP, truth((k + 1) * G1_STEP)) for k in range(k1 - 1400, k1)], closed)
    history.save(tmp_path, b0)
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    klines, _ = _exchange(truth, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    btc = eng.instruments["binance_spot:BTCUSDT"]
    _tick(eng, clk, now, now + 1200)                           # 20 minutes, no live data
    k = int((now + 300) // G1_STEP)
    assert btc.g1.get(k) != btc.g1.get(k) and not btc.g1.is_live(k)
    t1 = now + 1500
    _tick(eng, clk, now + 1210, t1, feed=lambda t: btc.on_price(truth(t), t, t))
    pct = stats.window_return(btc, stats.window_for("today", t1), t1)[1]
    assert pct != pct                                          # unknown start: the window waits


@pytest.mark.parametrize("off_hours,tf,live_after,read_after", [(30, "today", 1200, 1800), (48, "1h", 600, 3690)])
def test_after_a_day_or_more_off_a_failed_download_leaves_the_minutes_unknown(off_hours, tf, live_after, read_after,
                                                                            cfg, keys, monkeypatch, tmp_path):
    """Off longer than the 1-day grid holds, the 1-minute grid loads nothing and got no hole, so
    until the first live price its minutes carried the cache's 30-48 h old close: with the download
    failing, TODAY read +43.6% and 1H +94%. Now the time off is a hole there too."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = ny_ts(2026, 11, 24, 16, 50)
    closed = now - off_hours * 3600
    truth = lambda t: 100.0 + (t - now) / 3600              # noqa: E731
    old, _ = _gap_engine(cfg, keys, closed, tmp_path)
    b0 = old.instruments["binance_spot:BTCUSDT"]
    k_c = int(closed // G5_STEP)
    b0.seed("g5", [(k * G5_STEP, truth((k + 1) * G5_STEP)) for k in range(k_c - 2000, k_c)], closed)
    b0.seed("g1", [(k * 60, truth((k + 1) * 60)) for k in range(int(closed // 60) - 1000, int(closed // 60))], closed)
    history.save(tmp_path, b0)
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    klines, _ = _exchange(truth, fail=lambda: True)          # the network is down
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    btc = eng.instruments["binance_spot:BTCUSDT"]
    assert btc.g1.last_k is None and btc.holes["g1"] is not None
    t1 = now + read_after
    _tick(eng, clk, now, t1, feed=lambda t: btc.on_price(truth(t), t, t) if t >= now + live_after else None)
    pct = stats.window_return(btc, stats.window_for(tf, t1), t1)[1]
    assert pct != pct                                          # was +43.6% (TODAY), +94% (1H)


def test_the_first_live_price_after_a_failed_start_cuts_the_back_off_short(cfg, keys, monkeypatch, tmp_path):
    """Started without network: the download fails and the back-fill backs off (up to 15 min). No
    gap ever opened in a live feed, so nothing said the network was back; the first live price does."""
    import time
    from moneyflow import history
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = 1795528800.0
    closed = now - 6 * 3600
    old, _ = _gap_engine(cfg, keys, closed, tmp_path)
    b0 = old.instruments["binance_spot:BTCUSDT"]
    k_c = int(closed // G5_STEP)
    b0.seed("g5", [(k * G5_STEP, 100.0) for k in range(k_c - 2000, k_c)], closed)
    history.save(tmp_path, b0)
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    eng.BACKFILL_FIRST = eng.BACKFILL_MAX = 60.0
    up = [False]
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: not up[0])
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                            # the first download fails
    btc = eng.instruments["binance_spot:BTCUSDT"]
    assert btc.holes["g5"] is not None

    async def go():
        eng.check_holes()                                      # housekeeping: the back-fill starts...
        for _ in range(100):
            await asyncio.sleep(0.01)
            if eng.backfill_state.get("state") == "retrying":
                break
        assert eng.backfill_state.get("state") == "retrying"   # ...fails, and waits 60 s
        up[0] = True
        btc.on_price(100.0, now + 1, now + 1)                  # the first live price
        eng.check_holes()                                      # the next housekeeping round
        await asyncio.wait_for(eng._backfill_task, timeout=5)
    t = time.monotonic()
    asyncio.run(go())
    assert time.monotonic() - t < 5 and eng.backfill_state["state"] == "done" and btc.holes["g5"] is None


@pytest.mark.parametrize("kind", ["alpaca", "yahoo", "perp"])
def test_a_download_newer_than_the_cache_becomes_the_price(kind, cfg, keys, monkeypatch, tmp_path):
    """The cache was written on Thursday evening (600); the app starts on Saturday and the download
    brings Friday's session (612). The price is the download's newest close: it stayed the cache's
    600 all weekend (TODAY -1.96%, 1W off by Friday's move) until the first live trade. Checked for
    an ETF from Alpaca, the same ETF from Yahoo without a key (with the dollar, from ICE), and a perp."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.connectors import pollers
    from moneyflow.series import G5_STEP
    thu, fri, sat = ny_ts(2026, 11, 19, 18, 0), ny_ts(2026, 11, 20, 0, 0), ny_ts(2026, 11, 21, 12, 0)
    truth = lambda t: 600.0 if t < fri else 612.0           # noqa: E731
    key = "binance_fut:SPYUSDT" if kind == "perp" else "alpaca:SPY"
    old, _ = _gap_engine(cfg, keys, thu, tmp_path)
    k = int(thu // G5_STEP)
    old.instruments[key].seed("g5", [(j * G5_STEP, 600.0) for j in range(k - 2000, k)], thu)
    history.save(tmp_path, old.instruments[key])
    eng, _ = _gap_engine(cfg, keys, sat, tmp_path)

    def session(start, step):
        t, rows = int(start // step) * step, []
        while t + step <= sat:
            if kind == "perp" or clock.market_open("us_regular", t):
                rows.append((t, truth(t)))
            t += step
        return rows
    klines, _ = _exchange(truth, fail=lambda: kind != "perp")
    monkeypatch.setattr(bn, "fetch_klines", klines)
    if kind == "alpaca":
        eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})

        async def bars(conn, symbols, tf, start, end=None, feed="iex"):
            return {s: [(t, c, 1e6) for t, c in session(start, 300 if tf == "5Min" else 60)] for s in symbols}
        monkeypatch.setattr(alp, "fetch_bars", bars)
    elif kind == "yahoo":
        eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))

        async def chart(session_, symbol, interval, rng, now=None):
            return session(sat - int(rng[:-1]) * 86400, 300 if interval == "5m" else 60)
        monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng.seed_history())
    inst = eng.instruments[key]
    last_bar_end = sat if kind == "perp" else ny_ts(2026, 11, 20, 16, 0)
    assert inst.price == 612.0 and inst.ts == last_bar_end
    assert stats.window_return(inst, stats.window_for("today", sat), sat)[1] == 0.0
    if kind == "yahoo":
        usd = eng.series["usd"].inst                           # computed, history from ICE: same rule
        assert usd.price == 612.0 and usd.ts == last_bar_end



def test_a_delayed_download_is_not_carried_over_open_hours(cfg, keys, monkeypatch, tmp_path):
    """Without a key, an ETF's bars come from Yahoo, 15 minutes late in a session: those minutes
    stay unknown (carried, they would be the last close flagged as trading). Only shut hours after
    the last bar are carried (the weekend after Friday's close, in the test above)."""
    from conftest import ny_ts
    from moneyflow.connectors import binance as bn
    from moneyflow.connectors import pollers
    from moneyflow.series import G5_STEP
    now = ny_ts(2026, 11, 24, 11, 0)
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))

    async def chart(session_, symbol, interval, rng, now=None):
        step = 300 if interval == "5m" else 60
        t, rows = int((now - 2 * 86400) // step) * step, []
        while t + step <= now - 900:                            # 15 minutes late
            if clock.market_open("us_regular", t):
                rows.append((t, 600.0))
            t += step
        return rows
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    spy = eng.instruments["alpaca:SPY"]
    late = range(int((now - 900) // G5_STEP), int(now // G5_STEP))
    assert all(spy.g5.get(k) != spy.g5.get(k) and not spy.g5.is_live(k) for k in late)
    assert spy.price == 600.0 and spy.ts == now - 900



def test_a_cache_dates_its_price_at_its_last_data_not_at_the_close_carried_after_it(cfg, keys, tmp_path):
    """A cache saved in the evening ends in hours of steps carried after the close; one written by an
    older version could even carry a stale price there (a start whose download had not landed). Its
    price is its last data, dated then: dated at the save, the carried value outranked the download
    of the same session (seen live: SPY at 704.12 instead of its 716.70 close)."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP
    close, saved = ny_ts(2026, 11, 25, 16, 0), ny_ts(2026, 11, 25, 19, 55)
    old, _ = _gap_engine(cfg, keys, saved, tmp_path)
    spy = old.instruments["alpaca:SPY"]
    k_c, k_s = int(close // G5_STEP), int(saved // G5_STEP)
    spy.seed("g5", [(k * G5_STEP, 716.7) for k in range(k_c - 2000, k_c)], close)   # live in session hours
    for k in range(k_c, k_s):
        spy.g5.put(k, 704.12, False)                                 # a stale price carried after it
    history.save(tmp_path, spy)
    now = ny_ts(2026, 11, 25, 21, 20)
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    inst = eng.instruments["alpaca:SPY"]
    history.load(tmp_path, inst, now)
    assert inst.price == 716.7 and inst.ts == close


def test_a_download_is_carried_over_shut_hours_only():
    """On to now through the hours the market was shut since the download's last bar, never over
    open ones, to the minute when the bar does not end on a 5-minute step (a 1-minute bar at 15:58)."""
    from conftest import ny_ts
    from moneyflow.series import Instrument
    etf = Instrument("x:SPY", "x", "SPY", "us_regular")
    close, sat, mon_open = ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 30)
    assert Engine._through_closed(etf, close, sat) == sat                         # the weekend: carried
    assert Engine._through_closed(etf, close, mon_open + 600) == mon_open         # up to Monday's open
    assert Engine._through_closed(etf, close - 120, sat) == close - 120           # 15:58: open minutes left
    assert Engine._through_closed(etf, close - 600, close - 60) == close - 600    # in the session: left


# ── tenth round: an ETF's extended hours, the 1-minute gap's start, two test gaps, the stop ──────
def _iex_bars(clk, close, thin=False):
    """Alpaca's IEX bars of an unchanged ETF: the session at `close`, pre- and post-market prints 0.5%
    above it; `thin`: no bar before 9:45 (no trade yet), none outside the session."""
    import datetime as dt
    calls = []

    async def bars(conn, symbols, tf, start, end=None, feed="iex"):
        calls.append(tf)
        step = 300 if tf == "5Min" else 60
        t, rows = -(-int(start) // step) * step, []
        while t + step <= clk.now():
            s = clock.us_equity_session(t + step - 1)
            x = dt.datetime.fromtimestamp(t, clock.NY)
            if s == "regular" and not (thin and (x.hour, x.minute) < (9, 45)):
                rows.append((t, close, 1e6))
            elif s in ("pre", "post") and not thin:
                rows.append((t, close * 1.005, 1e4))
            t += step
        return {sym: list(rows) for sym in symbols}
    bars.calls = calls
    return bars


@pytest.mark.parametrize("saved,thin", [((2026, 11, 20, 16, 30), False), ((2026, 11, 21, 12, 0), False),
                                        ((2026, 11, 21, 12, 0), True)])
def test_an_etfs_extended_hours_bars_leave_today_on_the_session_close(saved, thin, cfg, keys, monkeypatch, tmp_path):
    """Alpaca's IEX bars include pre- and post-market prints. Started on a Monday at 9:00 with a cache
    from Friday's close, the download put Friday evening's prints in the grids as closes: an unchanged
    ETF read TODAY -0.5% and 1H -0.5% at 10:00. With a cache from Saturday (the app ran over the
    weekend) the weekend was left empty until Monday's first bar: TODAY unknown all day; with no bar
    at all since (`thin`: no print before 9:45), even after the first fix."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    close = 80.0
    saved_ts, now = ny_ts(*saved), ny_ts(2026, 11, 23, 9, 0)
    old, _ = _gap_engine(cfg, keys, saved_ts, tmp_path)
    k_s = int(saved_ts // G5_STEP)
    for etf in [i for i in old.instruments.values() if i.source == "alpaca"]:   # every ETF cached, as in use
        for k in range(k_s - 2000, k_s):              # as the live series writes it: the close, live in session
            etf.g5.put(k, close, clock.market_open("us_regular", (k + 1) * G5_STEP - 1))
        history.save(tmp_path, etf)
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})
    monkeypatch.setattr(alp, "fetch_bars", _iex_bars(clk, close, thin))
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    inst = eng.instruments["alpaca:TLT"]
    assert inst.price == close and inst.holes == {"g1": None, "g5": None}
    assert inst.extra.get("ext_price") == (None if thin else close * 1.005)
    t1 = ny_ts(2026, 11, 23, 10, 0)
    _tick(eng, clk, now, t1, feed=lambda t: inst.on_price(close, t, t) if clock.market_open("us_regular", t) else None)
    for tf in ("today", "1h", "1w"):
        assert stats.window_return(inst, stats.window_for(tf, t1), t1)[1] == 0.0


@pytest.mark.parametrize("version", [1, 2])
def test_an_etf_cache_from_before_version_3_is_rewritten_to_its_session_closes(version, cfg, keys, tmp_path):
    """Older caches hold an ETF's pre- and post-market prints as closes (the real one, version 1:
    Friday's, up to 693.89, carried over the weekend against a 690.68 close), and empty shut hours (a
    download with no bar). Loaded, every step outside the session takes the session's close, so TODAY
    starts from it; before the first session step a grid holds, they are unknown. A version-1 cache is
    downloaded again too; with its 1-minute grid not rewritten, that download carried the old print
    over Sunday (TODAY -0.46% on Monday)."""
    import json
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.series import G1_STEP, G5_STEP, NaN
    close, saved, now = ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 20, 20, 0), ny_ts(2026, 11, 20, 21, 0)
    empty = (ny_ts(2026, 11, 20, 18, 0), ny_ts(2026, 11, 20, 19, 0))
    old, _ = _gap_engine(cfg, keys, saved, tmp_path)
    spy = old.instruments["alpaca:SPY"]
    for g, step in ((spy.g5, G5_STEP), (spy.g1, G1_STEP)):
        for k in range(int(saved // step) - g.size + 1, int(saved // step)):
            session = clock.market_open("us_regular", (k + 1) * step - 1)
            g.put(k, NaN if empty[0] <= k * step < empty[1] else 690.68 if session else 693.89, session)
    history.save(tmp_path, spy)
    f = tmp_path / "alpaca_SPY.json"
    d = json.loads(f.read_text())
    d["v"] = version                                            # written before this version
    f.write_text(json.dumps(d))
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    inst = eng.instruments["alpaca:SPY"]
    history.load(tmp_path, inst, now)
    for g, step in ((inst.g5, G5_STEP), (inst.g1, G1_STEP)):
        assert all(g.get(k) == 690.68 for k in range(int(close // step), int(saved // step)))
    assert stats.window_return(inst, stats.window_for("today", now), now)[2] == 690.68
    k_pre = int(ny_ts(2026, 11, 20, 8, 0) // G1_STEP)           # before the 1-minute grid's first session step
    assert inst.g1.get(k_pre) != inst.g1.get(k_pre)
    assert inst.price == 690.68 and inst.ts == close
    for name, g in (("g1", inst.g1), ("g5", inst.g5)):         # and downloaded again (format 4 or older)
        first = next(k for k in range(g.first_k(), g.last_k + 1) if g.get(k) == g.get(k))
        assert inst.holes[name] == first * g.step


@pytest.mark.parametrize("version", [3, 4, "current"])
def test_an_etf_cache_with_an_empty_night_has_it_filled_on_loading(version, cfg, keys, tmp_path):
    """A download with no bar for an ETF (a weekend, a holiday, before its first print) carried nothing
    before version 4, so a cache can hold an empty night or weekend: TODAY's start (17:00) and 1W read
    nothing there. Loaded, those steps take the session's close. A cache of version 4 or later too: a
    start-up download that failed until after the first roll left them empty (only the minutes after
    the start were downloaded again), and later starts download only from the newest value."""
    import json
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.series import G5_STEP
    close, saved, now = ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 21, 13, 0)
    old, _ = _gap_engine(cfg, keys, saved, tmp_path)
    spy = old.instruments["alpaca:SPY"]
    empty = range(int(ny_ts(2026, 11, 20, 16, 30) // G5_STEP), int(saved // G5_STEP) - 1)
    for k in range(int(saved // G5_STEP) - 2000, int(saved // G5_STEP)):
        session = clock.market_open("us_regular", (k + 1) * G5_STEP - 1)
        spy.g5.put(k, float("nan") if k in empty else 690.68, session)
    history.save(tmp_path, spy)
    f = tmp_path / "alpaca_SPY.json"
    d = json.loads(f.read_text())
    d["v"] = history.VERSION if version == "current" else version
    f.write_text(json.dumps(d))
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    inst = eng.instruments["alpaca:SPY"]
    history.load(tmp_path, inst, now)
    assert all(inst.g5.get(k) == 690.68 and not inst.g5.is_live(k) for k in empty)
    ref = stats.price_at(inst, ny_ts(2026, 11, 20, 20, 0), now + 7 * 86400)    # a week later's 1W start
    assert ref == 690.68


def test_two_processes_saving_the_same_cache_file_do_not_share_a_temporary_file(tmp_path, monkeypatch):
    """The port is freed before the final save, so a copy started at once can save an instrument while
    the stopping one still does: each writes its own temporary file. Sharing one, the second replace
    found it gone (and a write could land in the other's half-written file)."""
    import json
    import os
    import threading
    from pathlib import Path
    from moneyflow import history
    from moneyflow.series import Instrument
    inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    inst.g5.put(5985096, 600.0, True)
    local, written, first_done = threading.local(), threading.Barrier(2), threading.Event()
    monkeypatch.setattr(os, "getpid", lambda: local.pid)
    real_replace = Path.replace

    def replace(self, target):
        written.wait(timeout=5)                                # both temporary files written
        if local.pid == 222:
            first_done.wait(timeout=5)                         # then one replace after the other
        try:
            return real_replace(self, target)
        finally:
            if local.pid == 111:
                first_done.set()
    monkeypatch.setattr(Path, "replace", replace)
    errors = []

    def run(pid):
        local.pid = pid
        try:
            history.save(tmp_path, inst)
        except OSError as e:
            errors.append(e)
    threads = [threading.Thread(target=run, args=(pid,)) for pid in (111, 222)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors and json.loads((tmp_path / "alpaca_SPY.json").read_text())["key"] == "alpaca:SPY"
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("saved,now,hole", [
    ((2026, 11, 23, 12, 0), (2026, 11, 25, 2, 0), (2026, 11, 24, 9, 30)),    # Wednesday 2:00, off since Monday noon
    ((2026, 11, 19, 12, 0), (2026, 11, 22, 12, 0), None),                    # a Sunday
    ((2026, 11, 19, 12, 0), (2026, 11, 23, 8, 0), None)])                    # Monday before the open
def test_an_empty_minute_grid_takes_the_time_off_from_its_first_open_minute(saved, now, hole, tmp_path):
    """More than a day off leaves an ETF's 1-minute grid empty: its share of the time off starts at the
    first minute the market was open (none if it stayed shut). Placed at the grid's first minute (a
    Sunday, 2:00), the download never covered it."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP, Instrument
    s, n = ny_ts(*saved), ny_ts(*now)
    a = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    k = int(s // G5_STEP)
    a.seed("g5", [(j * G5_STEP, 500.0) for j in range(k - 2000, k)], s)
    history.save(tmp_path, a)
    b = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    history.load(tmp_path, b, n)
    assert b.holes == {"g1": ny_ts(*hole) if hole else None, "g5": s}


@pytest.mark.parametrize("saved,now,thin", [((2026, 11, 23, 12, 0), (2026, 11, 25, 2, 0), False),
                                            ((2026, 11, 20, 12, 0), (2026, 11, 23, 11, 0), True),
                                            ((2026, 11, 20, 12, 0), (2026, 11, 23, 9, 40), True),
                                            ((2026, 11, 23, 9, 25), (2026, 11, 23, 9, 40), True)])
def test_an_etfs_time_off_downloaded_at_the_start_is_not_fetched_again(saved, now, thin, cfg, keys, monkeypatch,
                                                                        tmp_path, caplog):
    """The start-up download covers an ETF's time off, so no back-fill follows. One did after every
    start after more than a day off (each ETF downloaded again, "no exchange data for the gap ...;
    left empty" logged for each), when the gap began at a closed minute, or (`thin`) before a thin
    ETF's first trade of the day, also with the start before it (9:40: no bar yet at all, also with a
    cache from just before the open: an answer with no bar for any ETF): IEX has bars only for minutes
    that traded."""
    import logging
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    caplog.set_level(logging.INFO)
    klines, _ = _exchange(lambda t: 600.0, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    s, n = ny_ts(*saved), ny_ts(*now)
    old, _ = _gap_engine(cfg, keys, s, tmp_path)
    k = int(s // G5_STEP)
    for key in [x for x in old.instruments if x.startswith("alpaca:")]:
        old.instruments[key].seed("g5", [(j * G5_STEP, 600.0) for j in range(k - 2000, k)], s)
        history.save(tmp_path, old.instruments[key])
    eng, clk = _gap_engine(cfg, keys, n, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})
    bars = _iex_bars(clk, 610.0, thin)
    monkeypatch.setattr(alp, "fetch_bars", bars)

    async def go():
        await eng.seed_history()
        assert not eng.holes()
        eng.check_holes()                                     # housekeeping, 5 s later
        if eng._backfill_task is not None:
            await asyncio.wait_for(eng._backfill_task, timeout=10)
    asyncio.run(go())
    assert bars.calls == ["5Min", "1Min"] and "no exchange data" not in caplog.text


@pytest.mark.parametrize("fails", [False, True])
def test_an_etf_with_no_bar_in_the_answer_is_asked_when_it_last_traded(fails, cfg, keys, monkeypatch, tmp_path, caplog):
    """Started on Monday at 9:50 from a cache saved on Saturday: UUP's close changed every 5 minutes
    in its history (its quiet limit: 15 minutes) but it first prints at 10:00, so the answer has no
    bar for it over 20 open minutes, read as the venue's gap (a hole, one more download, then "no
    exchange data"). The venue's latest trade of it (Friday) says it did not trade since: no hole, the
    close carried (not live). Asked once, for the symbols with no session bar only; when that request
    fails, as before."""
    import logging
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    caplog.set_level(logging.INFO)
    klines, _ = _exchange(lambda t: 600.0, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    saved, start, opened = ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 50), ny_ts(2026, 11, 23, 9, 30)
    old, _ = _gap_engine(cfg, keys, saved, tmp_path)
    k = int(saved // G5_STEP)
    for key in [x for x in old.instruments if x.startswith("alpaca:")]:
        old.instruments[key].seed("g5", [(j * G5_STEP, 27.0 + 0.01 * (j % 13)) for j in range(k - 3000, k)], saved)
        history.save(tmp_path, old.instruments[key])
    close = old.instruments["alpaca:UUP"].g5.get(k - 1)
    eng, clk = _gap_engine(cfg, keys, start, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})
    asked = []

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        step = 300 if tf == "5Min" else 60
        out, t = {s: [] for s in symbols}, -(-int(since) // step) * step
        while t + step <= start:
            if clock.market_open("us_regular", t + step - 1):
                for s in symbols:
                    if s != "UUP":
                        out[s].append((t, 27.5, 1e5))
            t += step
        return out

    async def latest(conn, symbols, feed="iex"):
        asked.append(sorted(symbols))
        if fails:
            raise OSError("down")
        return {s: ny_ts(2026, 11, 20, 15, 59) + 30 for s in symbols}
    monkeypatch.setattr(alp, "fetch_bars", bars)
    monkeypatch.setattr(alp, "fetch_latest_trades", latest)
    asyncio.run(eng.seed_history())
    uup = eng.instruments["alpaca:UUP"]
    assert asked == [["UUP"]]
    session = range(int(opened // G5_STEP), int(start // G5_STEP))
    if fails:
        assert uup.holes["g5"] == opened and all(uup.g5.get(j) != uup.g5.get(j) for j in session)
        assert "latest trades: down" in caplog.text
    else:
        assert uup.holes == {"g1": None, "g5": None}
        assert all(uup.g5.get(j) == close and not uup.g5.is_live(j) for j in session)
    # an answer over a night or a weekend has no session bar to miss: nothing asked
    from moneyflow import engine as engmod
    assert not engmod._had_session(saved, ny_ts(2026, 11, 23, 9, 30)) and engmod._had_session(saved, start)


@pytest.mark.parametrize("when", [(2026, 11, 21, 16, 45), (2026, 11, 22, 16, 45)])
def test_without_a_key_an_etf_keeps_its_today_start_all_weekend(when, cfg, keys, monkeypatch, tmp_path):
    """On a Saturday or Sunday afternoon TODAY starts more than 23.5 hours back, where it is read from
    the 5-minute grid: Yahoo's 5-minute bars are carried over the shut hours too (TODAY was unknown from
    16:30), and so are its 1-minute ones (the 1H chart reads them)."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import binance as bn
    from moneyflow.connectors import pollers
    from moneyflow.series import G1_STEP, G5_STEP
    thu, fri, now = ny_ts(2026, 11, 19, 18, 0), ny_ts(2026, 11, 20, 0, 0), ny_ts(*when)
    truth = lambda t: 600.0 if t < fri else 612.0              # noqa: E731
    old, _ = _gap_engine(cfg, keys, thu, tmp_path)
    k = int(thu // G5_STEP)
    old.instruments["alpaca:SPY"].seed("g5", [(j * G5_STEP, 600.0) for j in range(k - 2000, k)], thu)
    history.save(tmp_path, old.instruments["alpaca:SPY"])
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))

    async def chart(session_, symbol, interval, rng, now=None):           # Yahoo's ranges are trading days, and
        step = 300 if interval == "5m" else 60                  # its last point is a 16:00 closing print
        t, rows = int((now - (int(rng[:-1]) + 2) * 86400) // step) * step, []
        while t + step <= now:
            if clock.market_open("us_regular", t):
                rows.append((t, truth(t)))
            t += step
        return rows + [(fri + 16 * 3600, 612.5)]
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    klines, _ = _exchange(truth, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    spy = eng.instruments["alpaca:SPY"]
    assert stats.window_return(spy, stats.window_for("today", now), now)[1] == 0.0
    assert spy.g1.get(int(now // G1_STEP) - 1) == 612.0 and spy.extra["ext_price"] == 612.5


def test_live_prices_while_downloads_fail_wake_the_backfill_once_not_each_time(cfg, keys, monkeypatch, tmp_path):
    """Only the first live price after a failed start says the network is back. Counted for every live
    price, a back-fill whose downloads keep failing (REST blocked, the streams fine) ran again each
    housekeeping round (40 attempts here) instead of waiting out its back-off."""
    from moneyflow import history
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = 1795528800.0
    closed = now - 6 * 3600
    old, _ = _gap_engine(cfg, keys, closed, tmp_path)
    b0 = old.instruments["binance_spot:BTCUSDT"]
    k_c = int(closed // G5_STEP)
    b0.seed("g5", [(k * G5_STEP, 100.0) for k in range(k_c - 2000, k_c)], closed)
    history.save(tmp_path, b0)
    eng, _ = _gap_engine(cfg, keys, now, tmp_path)
    eng.BACKFILL_FIRST = eng.BACKFILL_MAX = 60.0
    klines, calls = _exchange(lambda t: 100.0, fail=lambda: True)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    btc = eng.instruments["binance_spot:BTCUSDT"]
    n0 = len(calls)

    async def go():
        for i in range(40):                                    # housekeeping rounds, a live price before each
            btc.on_price(100.0, now + i, now + i)
            eng.check_holes()
            await asyncio.sleep(0.02)
        eng._backfill_task.cancel()
        await asyncio.gather(eng._backfill_task, return_exceptions=True)
    asyncio.run(go())
    assert eng.backfill_state.get("attempt") == 1 and len(calls) - n0 == 1


def test_housekeeping_writes_the_cache_in_a_worker_thread(cfg, keys, monkeypatch, tmp_path):
    """The save every 10 minutes runs in a worker thread too (on the event loop it stalled ticks)."""
    import threading
    from moneyflow import engine as engmod
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    eng.HOUSEKEEP_EVERY = 0.01
    rec = _slow_save(monkeypatch, 0.0)
    t0, n = engmod.time.time(), [0]

    def clock_():                                              # 10 minutes on after the start: a save is due
        n[0] += 1
        return t0 if n[0] <= 2 else t0 + 601.0
    monkeypatch.setattr(engmod.time, "time", clock_)

    async def go():
        task = asyncio.create_task(eng._housekeeping())
        for _ in range(200):
            await asyncio.sleep(0.01)
            if rec["keys"]:
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())
    assert rec["keys"] and threading.get_ident() not in rec["threads"]


def test_a_refused_replace_is_retried_and_stale_temporary_files_are_removed(cfg, keys, tmp_path, monkeypatch):
    """Windows refuses to replace a file another process has open (a copy loading the cache at its
    start): the replace is tried again shortly, and if it stays refused its temporary file is removed.
    A temporary file no save writes any more (its process ended, or ten minutes old) is removed at the
    next start; each one used to stay for good."""
    import os
    import time
    from pathlib import Path
    from moneyflow import history
    from moneyflow.series import Instrument
    inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    inst.g5.put(5985096, 600.0, True)
    real, refusals = Path.replace, [2]

    def replace(self, target):
        if refusals[0] > 0:
            refusals[0] -= 1
            raise PermissionError(13, "the file is open in another process")
        return real(self, target)
    monkeypatch.setattr(Path, "replace", replace)
    assert history.save(tmp_path, inst) and (tmp_path / "alpaca_SPY.json").exists() and refusals == [0]
    refusals[0] = 99
    with pytest.raises(PermissionError):
        history.save(tmp_path, inst)
    assert not list(tmp_path.glob("*.tmp"))
    monkeypatch.setattr(Path, "replace", real)
    import subprocess
    import sys
    ended = subprocess.Popen([sys.executable, "-c", "pass"])
    ended.wait()                                               # a copy killed in its save: its process is gone
    old = tmp_path / f"binance_spot_BTCUSDT.{os.getpid()}.tmp"
    dead = tmp_path / f"binance_spot_SOLUSDT.{ended.pid}.tmp"
    busy = tmp_path / f"binance_spot_ETHUSDT.{os.getpid()}.tmp"
    for f in (old, dead, busy):
        f.write_text("{}")
    os.utime(old, (time.time() - 3600, time.time() - 3600))
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: True)
    from moneyflow.connectors import binance as bn
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                            # a start
    assert not old.exists() and not dead.exists() and busy.exists()   # a live process may still write that one


def test_a_backfill_over_an_etfs_close_corrects_the_price_the_night_carries(cfg, keys, monkeypatch, tmp_path):
    """SPY traded at 600 until 15:30, then the PC slept until 16:30; the session closed at 606. The
    back-fill wrote the close into the grid but left the price at 600 (live data had come, and after
    the close nothing else corrects it), and the roll carried 600 over the night and the weekend:
    Monday's TODAY read +1.0% instead of 0%, HYG and IEF alike (the risk meter's credit input -8.9)."""
    from conftest import ny_ts
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    t0, sleep, wake = ny_ts(2026, 11, 20, 9, 30), ny_ts(2026, 11, 20, 15, 30), ny_ts(2026, 11, 20, 16, 30)
    truth = lambda t: 600.0 if t <= sleep else 600.0 + 6.0 * min(1.0, (t - sleep) / 1800)   # noqa: E731
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})

    async def bars(conn, symbols, tf, start, end=None, feed="iex"):
        step = 300 if tf == "5Min" else 60
        t, rows = -(-int(start) // step) * step, []
        while t + step <= clk.now():
            if clock.market_open("us_regular", t + step - 1):
                rows.append((t, truth(t + step), 1e6))
            t += step
        return {sym: list(rows) for sym in symbols}
    monkeypatch.setattr(alp, "fetch_bars", bars)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                            # the morning's start
    spy = eng.instruments["alpaca:SPY"]
    _tick(eng, clk, t0, sleep, step=30.0, feed=lambda t: spy.on_price(truth(t), t, t))
    clk.set_fixed(wake)                                        # awake again (asleep from 15:30)
    eng.tick()
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done"
    assert spy.price == 606.0 and spy.ts == ny_ts(2026, 11, 20, 16, 0)
    _tick(eng, clk, wake + 30, wake + 3600, step=30.0)         # the evening, carried on
    k = int((wake + 1800) // G5_STEP)
    assert spy.g5.get(k) == 606.0 and spy.g1.get(int((wake + 1800) // 60)) == 606.0


# ── twelfth round: a wake days after a close, the computed dollar, an empty answer, a failed start ──
def _sleep_over_the_close(cfg, keys, monkeypatch, tmp_path, day, wake, trade_first=False):
    """SPY at 600 until the PC sleeps at 15:30 on `day`; the session closes at 606. Awake at `wake`,
    the back-fill runs (IEX bars: only for steps that traded); `trade_first`: a live trade comes before
    it (in a session the stream is back within seconds, the back-fill waits 10). Returns (engine,
    clock, spy, truth)."""
    from conftest import ny_ts
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    t0, sleep = ny_ts(*day, 9, 30), ny_ts(*day, 15, 30)
    truth = lambda t: 600.0 if t <= sleep else 600.0 + 6.0 * min(1.0, (t - sleep) / 1800)   # noqa: E731
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})

    async def bars(conn, symbols, tf, start, end=None, feed="iex"):
        step = 300 if tf == "5Min" else 60
        t, rows = -(-int(start) // step) * step, []
        while t + step <= clk.now():
            if clock.market_open("us_regular", t + step - 1):
                rows.append((t, truth(t + step), 1e6))
            t += step
        return {sym: list(rows) for sym in symbols}
    monkeypatch.setattr(alp, "fetch_bars", bars)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                            # the morning's start
    spy = eng.instruments["alpaca:SPY"]
    _tick(eng, clk, t0, sleep, step=30.0, feed=lambda t: spy.on_price(truth(t), t, t))
    clk.set_fixed(ny_ts(*wake))                                # awake again (asleep from 15:30)
    eng.tick()
    if trade_first:
        clk.set_fixed(ny_ts(*wake) + 2)
        spy.on_price(truth(ny_ts(*wake)), ny_ts(*wake) + 2, ny_ts(*wake) + 2)
        eng.tick()
    asyncio.run(eng._backfill("wake"))
    return eng, clk, spy, truth


@pytest.mark.parametrize("day,wake,then", [
    ((2026, 11, 20), (2026, 11, 23, 8, 0), (2026, 11, 23, 10, 30)),      # Friday's close, awake Monday 8:00
    ((2026, 11, 20), (2026, 11, 21, 17, 30), (2026, 11, 21, 18, 30)),    # ... Saturday 17:30
    ((2026, 11, 25), (2026, 11, 27, 8, 0), (2026, 11, 27, 10, 30))])     # Thanksgiving: Wednesday to Friday
def test_a_sleep_over_an_etfs_close_that_wakes_days_later_leaves_no_old_price_in_the_minutes(
        day, wake, then, cfg, keys, monkeypatch, tmp_path):
    """The back-fill took the close (606) as the price, but its 1-minute download is one day long: no
    session bar in it after a wake more than a day after the close, so the 1-minute grid kept the
    pre-sleep price (600) the roll had carried over the day before the wake. TODAY and 1H read +1.0%
    while the market was shut, and TODAY all through the next session (true: 0)."""
    from conftest import ny_ts
    from moneyflow import stats
    from moneyflow.series import G1_SIZE
    eng, clk, spy, truth = _sleep_over_the_close(cfg, keys, monkeypatch, tmp_path, day, wake)
    w = ny_ts(*wake)
    assert eng.backfill_state["state"] == "done" and spy.price == 606.0 and spy.ts == ny_ts(*day, 16, 0)
    k1 = int(w // 60)
    assert all(spy.g1.get(k) == 606.0 and not spy.g1.is_live(k) for k in range(k1 - G1_SIZE + 1, k1))
    for tf in ("today", "1h"):
        assert stats.window_return(spy, stats.window_for(tf, w), w)[1] == 0.0
    t1 = ny_ts(*then)                                          # on to the next session (unchanged at 606)
    _tick(eng, clk, w + 30, t1, step=30.0,
          feed=lambda t: spy.on_price(truth(t), t, t) if clock.market_open("us_regular", t) else None)
    for tf in ("today", "1h"):
        assert stats.window_return(spy, stats.window_for(tf, t1), t1)[1] == 0.0


def test_an_fx_leg_that_slept_over_fridays_close_has_an_unknown_weekend_not_its_old_price():
    """The stale-close rule of the ETFs, for an FX leg: it quoted until the PC slept on Friday at 15:00;
    awake on Saturday, the minutes its feed missed before the 17:00 close are a gap, and the shut hours
    after them are unknown (not live, no gap), not the 15:00 price carried as if it were the close.
    Its next print (Sunday at 17:00) makes its price current again."""
    from conftest import ny_ts
    from moneyflow.series import G1_STEP, G5_STEP, Instrument
    leg = Instrument("finnhub:OANDA:EUR_USD", "finnhub", "OANDA:EUR_USD", "fx")
    t0, sleep, wake = ny_ts(2026, 11, 20, 14, 0), ny_ts(2026, 11, 20, 15, 0), ny_ts(2026, 11, 21, 1, 0)
    t = t0
    while t <= sleep:
        leg.on_price(1.1, t, t)
        leg.roll(t)
        t += 30
    leg.roll(wake)
    assert leg._stale_from == sleep + 60 and leg.holes == {"g1": sleep + 60, "g5": sleep + 300}
    for g in (leg.g1, leg.g5):
        k = int((wake - 900) // g.step)                       # Saturday 00:45: FX shut
        assert g.get(k) != g.get(k) and not g.is_live(k)
    assert leg.g1.get(int(sleep // G1_STEP)) == 1.1 and leg.g5.get(int(sleep // G5_STEP)) == 1.1
    sunday = ny_ts(2026, 11, 22, 17, 0) + 5
    leg.on_price(1.2, sunday, sunday)
    assert leg._stale_from is None and leg.price == 1.2


@pytest.mark.parametrize("wake_hm,one_min", [((21, 18, 0), True), ((21, 1, 0), True), ((21, 18, 0), False)])
def test_the_computed_dollar_takes_ices_close_over_a_weekend_its_fx_legs_slept_through(
        wake_hm, one_min, cfg, keys, monkeypatch, tmp_path):
    """The dollar is computed from six FX legs on every tick; ICE's DXY is its history. The legs quoted
    until the PC slept on Friday at 15:00 and ICE closed 0.3% higher at 17:00. Awake on Saturday, the
    legs' prices are older than the close all weekend: computed from them, the price read -0.30%
    against ICE's close (TODAY from Friday 17:00 after a wake before Saturday 17:00; 1W and 1M all
    weekend). In the eleventh round the back-fill took ICE's close as the price and the next tick
    computed it from the legs again, so the weekend's steps keep their computed value since. With FX
    shut and a leg stale the dollar's own close (ICE's) is the price, held over the shut hours, until
    the legs print again on Sunday at 17:00 (one still stale then: the legs' formula, FX is open). Also
    with no 1-minute bars from ICE (the 1-minute grid's newest value is then the one before the sleep,
    older than the 5-minute grid's close)."""
    from conftest import ny_ts
    from moneyflow import stats
    from moneyflow.connectors import pollers
    from moneyflow.series import DXY
    t0, sleep = ny_ts(2026, 11, 20, 14, 45), ny_ts(2026, 11, 20, 15, 0)
    d, h, m = wake_hm
    wake = ny_ts(2026, 11, d, h, m)
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))
    usd = eng.series["usd"].inst
    legs = list(usd.inputs)

    def quote(t, n=6):
        if clock.market_open("fx", t):
            for i, leg in enumerate(legs[:n]):
                leg.on_price(1.0 + i / 10, t, t)
    _tick(eng, clk, t0, sleep, feed=quote)
    computed = usd.price
    fx_close = ny_ts(2026, 11, 20, 17, 0)
    clk.set_fixed(wake)
    eng.tick()
    assert all(leg._stale_from is not None for leg in legs)

    async def chart(session, symbol, interval, rng, now=None):           # ICE: up to the 17:00 close, 0.3% higher
        step = 300 if interval == "5m" else 60
        if step == 60 and not one_min:
            return []
        span = 60 * 86400 if rng == "60d" else 86400
        k0, k1 = int((fx_close - span) // step), int(fx_close // step)
        return [(k * step, computed * (1.0 + 0.003 * (k + 1 - k0) / (k1 - k0))) for k in range(k0, k1)]
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done"
    ice_close = computed * 1.003
    assert usd.g1.get(int((wake - 600) // 60)) == pytest.approx(ice_close, rel=1e-12)   # the shut hours: ICE's close
    for t in (wake + 60, wake + 1800, ny_ts(2026, 11, 22, 16, 55)):
        _tick(eng, clk, clk.now() + 10, t, step=300.0)
        assert usd.price == pytest.approx(ice_close, rel=1e-12), t
        for tf in ("today", "1h"):
            assert stats.window_return(usd, stats.window_for(tf, t), t)[1] == pytest.approx(0.0, abs=1e-9), (t, tf)
    sunday = ny_ts(2026, 11, 22, 17, 0) + 5                   # FX opens; one leg has not printed yet
    _tick(eng, clk, sunday, sunday + 60, feed=lambda t: quote(t, 5))
    assert legs[5]._stale_from is not None and usd.price == pytest.approx(computed, rel=1e-12)
    assert usd.price == pytest.approx(DXY.formula({n: i.price for n, i in usd.pairs.items()}), rel=1e-12)
    _tick(eng, clk, sunday + 70, sunday + 120, feed=quote)
    assert all(leg._stale_from is None for leg in legs) and usd.price == pytest.approx(computed, rel=1e-12)


def test_the_dollars_close_is_never_held_across_an_open_step(cfg, keys, tmp_path):
    """ICE's history can end before Friday's 17:00 FX close (it is 10 minutes late): the open minutes
    after its last bar are unknown (what traded there is not known), and so is everything after them;
    ending at the close, its close is held over the shut hours up to now."""
    from conftest import ny_ts
    from moneyflow.series import G1_STEP, G5_STEP
    wake = ny_ts(2026, 11, 21, 1, 0)
    for last_bar_end, held in ((ny_ts(2026, 11, 20, 16, 45), False), (ny_ts(2026, 11, 20, 17, 0), True)):
        eng, _ = _gap_engine(cfg, keys, wake, tmp_path)
        usd = eng.series["usd"].inst
        for g, step in ((usd.g5, G5_STEP), (usd.g1, G1_STEP)):          # (both grids: one close per moment)
            k_end = int(last_bar_end // step)
            for k in range(k_end - 30, k_end):
                g.put(k, 100.0 + ((k + 1) * step // 300) % 7, False)
        close = 100.0 + (int(last_bar_end) // 300) % 7
        n = usd.hold_close(wake)
        k5 = int((wake - 600) // G5_STEP)
        assert (n > 0) is held and (usd.g5.get(k5) == close) is held and (usd.g1.get(int((wake - 60) // G1_STEP)) == close) is held
        if not held:
            assert all(usd.g5.get(k) != usd.g5.get(k) for k in range(int(last_bar_end // G5_STEP), k5 + 1))


def test_an_empty_answer_for_a_traded_etf_leaves_its_outage_unknown_not_flat_and_live(
        cfg, keys, monkeypatch, tmp_path, caplog):
    """SPY trades every few seconds; the network is down 10:00-10:30 with the PC awake. The back-fill
    gets an answer with no SPY bar at all (the venue's own gap): the 30 minutes were written as the
    9:59 price carried, flagged live, and the hole was cleared for good (also in the cache). An empty
    answer is "no trade" only over shut hours and a short stretch of an open market: here they stay
    unknown, and the gap is given up as "no exchange data" (downloaded again at the next start)."""
    import logging
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    caplog.set_level(logging.INFO, logger="moneyflow.engine")
    t0, down, up = ny_ts(2026, 11, 24, 9, 0), ny_ts(2026, 11, 24, 10, 0), ny_ts(2026, 11, 24, 10, 30)
    truth = lambda t: 600.0 + (t - t0) / 600                  # noqa: E731
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})
    empty = {"on": False}

    async def bars(conn, symbols, tf, start, end=None, feed="iex"):
        step = 300 if tf == "5Min" else 60
        t, rows = -(-int(start) // step) * step, []
        while t + step <= clk.now():
            if clock.market_open("us_regular", t + step - 1):
                rows.append((t, truth(t + step), 1e6))
            t += step
        return {sym: list(rows) for sym in symbols if not (empty["on"] and sym == "SPY")}
    monkeypatch.setattr(alp, "fetch_bars", bars)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    spy, qqq = eng.instruments["alpaca:SPY"], eng.instruments["alpaca:QQQ"]

    def feed(t):
        if not down <= t < up:
            for i in (spy, qqq):
                i.on_price(truth(t), t, t)
    _tick(eng, clk, t0, up + 15, step=5.0, feed=feed)
    assert spy.holes["g1"] == down
    empty["on"] = True
    asyncio.run(eng._backfill("network outage"))
    gap = range(int(down // 60), int(up // 60))
    assert all(spy.g1.get(k) != spy.g1.get(k) and not spy.g1.is_live(k) for k in gap)   # unknown, not flat live
    assert all(qqq.g1.get(k) == truth((k + 1) * 60) for k in gap)                      # the others filled
    assert "no exchange data for the gap since" in caplog.text and eng.backfill_state["state"] == "done"
    eng.save_cache()
    again = Engine(cfg, keys, clock.Clock(fixed=up + 3600))
    history.load(tmp_path, again.instruments["alpaca:SPY"], up + 3600)
    assert again.instruments["alpaca:SPY"].holes["g1"] == down                          # fetched at the next start


@pytest.mark.parametrize("network_up", [(7, 33, 30), (7, 36, 0)])
def test_a_start_whose_first_downloads_fail_past_the_first_roll_keeps_the_weekend_carried(
        network_up, cfg, keys, monkeypatch, tmp_path):
    """Cache saved on Saturday at noon, started on Monday at 7:30 while the network was still coming up.
    The first roll (7:35) closed a step at the start; the retries' downloads then began there, nothing
    marks shut hours as missing, and the weekend stayed empty for good (saved as format 4, never
    filled on loading): every ETF's TODAY unknown all Monday, 1W unknown the next weekend. The first
    roll after a start carries the cached close over the shut hours since the cache's last value."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    saved, start = ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 7, 30)
    up = ny_ts(2026, 11, 23, *network_up[:2]) + network_up[2]
    old, _ = _gap_engine(cfg, keys, saved, tmp_path)
    k_s = int(saved // G5_STEP)
    for etf in [i for i in old.instruments.values() if i.source == "alpaca"]:
        for k in range(k_s - 2000, k_s):
            etf.g5.put(k, 80.0, clock.market_open("us_regular", (k + 1) * G5_STEP - 1))
        history.save(tmp_path, etf)
    eng, clk = _gap_engine(cfg, keys, start, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={})
    bars = _iex_bars(clk, 80.0, thin=True)                    # no print before 9:45: empty answers

    async def iex(*a, **k):
        if clk.now() < up:
            raise OSError(11001, "getaddrinfo failed")
        return await bars(*a, **k)
    monkeypatch.setattr(alp, "fetch_bars", iex)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: clk.now() < up)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())                            # 7:30: fails
    t = start
    for wait in (60, 120, 240, 480):                           # _seed_then_track's retries
        _tick(eng, clk, t, t + wait, step=5.0)
        t += wait
        asyncio.run(eng.seed_history(incremental=True))
        if clk.now() >= up:
            break
    spy = eng.instruments["alpaca:SPY"]
    assert spy.price == 80.0
    shut = range(k_s, int(start // G5_STEP))
    assert all(spy.g5.get(k) == 80.0 and not spy.g5.is_live(k) for k in shut)
    t1 = ny_ts(2026, 11, 23, 10, 30)
    _tick(eng, clk, t, t1, step=30.0,
          feed=lambda x: spy.on_price(80.0, x, x) if clock.market_open("us_regular", x) else None)
    assert stats.window_return(spy, stats.window_for("today", t1), t1)[1] == 0.0


def test_the_first_roll_after_a_start_carries_the_cached_close_over_shut_hours_only(tmp_path):
    """Up to the first open step: from there it is a hole (the download fetches it)."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G1_STEP, G5_STEP, Instrument
    saved, now = ny_ts(2026, 11, 21, 12, 3), ny_ts(2026, 11, 23, 10, 0)
    a = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    k = int(saved // G5_STEP)
    for j in range(k - 2000, k):
        a.g5.put(j, 500.0, clock.market_open("us_regular", (j + 1) * G5_STEP - 1))
    for j in range(int(saved // G1_STEP) - 300, int(saved // G1_STEP)):
        a.g1.put(j, 500.0, False)
    history.save(tmp_path, a)
    b = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    history.load(tmp_path, b, now)
    opened = ny_ts(2026, 11, 23, 9, 30)
    assert b.holes["g5"] == opened
    b.roll(now)
    assert all(b.g5.get(j) == 500.0 and not b.g5.is_live(j) for j in range(k, int(opened // G5_STEP)))
    assert all(b.g5.get(j) != b.g5.get(j) for j in range(int(opened // G5_STEP), int(now // G5_STEP)))
    k1 = int(now // G1_STEP)
    assert all(b.g1.get(j) == 500.0 for j in range(k1 - 1439, int(opened // G1_STEP)))
    assert all(b.g1.get(j) != b.g1.get(j) for j in range(int(opened // G1_STEP), k1))
    btc = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT")     # 24/7: never shut, nothing carried
    for j in range(k - 2000, k):
        btc.g5.put(j, 100.0, True)
    btc.price, btc.ts = 100.0, k * G5_STEP
    btc.roll(now)
    assert all(btc.g5.get(j) != btc.g5.get(j) for j in range(k, int(now // G5_STEP)))


def test_the_final_save_writes_what_the_next_start_loads_first(cfg, keys, monkeypatch, tmp_path):
    """A window close gives the save about 4.5 s, and on a busy PC it took longer: the files the next
    start reads go first (the FX legs' and Coinbase's are read only by tools/replay_flows.py), several
    at once (SAVE_THREADS: most of a file's time is Windows closing it). The cache is saved every 10
    minutes, so what a cut save leaves is at most that old."""
    import threading
    import time
    from moneyflow import engine as engmod
    from moneyflow import history
    order, threads, at_once = [], set(), [0, 0]
    lock = threading.Lock()

    def save(d, i):
        with lock:
            at_once[0] += 1
            at_once[1] = max(at_once[1], at_once[0])
        order.append(i.source)
        threads.add(threading.get_ident())
        time.sleep(0.02)
        with lock:
            at_once[0] -= 1
        return True
    monkeypatch.setattr(history, "save", save)
    eng, _ = _gap_engine(cfg, keys, 1795528800.0, tmp_path)
    eng.save_cache()
    assert len(order) == len(eng.instruments) and engmod.SAVE_THREADS == 8
    assert len(threads) > 1 and at_once[1] > 1                          # files at once (counted, not timed)
    order.clear()
    monkeypatch.setattr(engmod, "SAVE_THREADS", 1)                    # the order they start in
    eng.save_cache()
    n = sum(s in engmod.HISTORY_SOURCES for s in order)
    assert 0 < n < len(order) and all(s in engmod.HISTORY_SOURCES for s in order[:n])
    assert eng.SAVE_EVERY == 600.0


@pytest.mark.parametrize("start", [(2026, 11, 21, 15, 36, 1), (2026, 11, 21, 15, 39, 59), (2026, 11, 21, 15, 35, 0),
                                   (2026, 11, 23, 9, 31, 30)])
def test_the_first_roll_leaves_no_minute_empty_in_the_5_minute_step_under_way(start, tmp_path):
    """The first roll after a start fills the 1-minute grid on through the minutes of the 5-minute step
    under way when that step is shut too: it stopped at the step's start, while the roll goes on from
    the minute of the start, so a start at 15:36 left the 15:35 minute empty for good (every ETF, in a
    start from a real cache). Not over open minutes (9:30 at a 9:31 start: the download's)."""
    import datetime as dt
    from moneyflow import history
    from moneyflow.series import G1_STEP, G5_STEP, Instrument
    from conftest import ny_ts
    saved = ny_ts(2026, 11, 21, 12, 0)
    now = dt.datetime(*start, tzinfo=clock.NY).timestamp()
    a = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    k = int(saved // G5_STEP)
    for j in range(k - 300, k):
        a.g5.put(j, 500.0, clock.market_open("us_regular", (j + 1) * G5_STEP - 1))
    for j in range(int(saved // G1_STEP) - 300, int(saved // G1_STEP)):
        a.g1.put(j, 500.0, False)
    history.save(tmp_path, a)
    b = Instrument("alpaca:QQQ", "alpaca", "QQQ", "us_regular")
    history.load(tmp_path, b, now)
    b.roll(now)
    b.roll(now + 120)                                          # the roll goes on
    k1 = int(now // G1_STEP)
    held = range(max(int(saved // G1_STEP), k1 + 2 - 1439), k1 + 2)     # (as far back as the 1-day grid holds)
    shut = [j for j in held if not clock.market_open("us_regular", (j + 1) * G1_STEP - 1)]
    assert all(b.g1.get(j) == 500.0 and not b.g1.is_live(j) for j in shut)
    opened = [j for j in range(int(saved // G1_STEP), k1) if clock.market_open("us_regular", (j + 1) * G1_STEP - 1)]
    assert all(b.g1.get(j) != b.g1.get(j) for j in opened)     # (none but at 9:31: the 9:30 minute)


@pytest.mark.parametrize("day,wake,later,trade_first", [
    ((2026, 11, 20), (2026, 11, 23, 10, 0), (2026, 11, 23, 13, 0), True),    # awake Monday 10:00, a trade first
    ((2026, 11, 20), (2026, 11, 23, 10, 0), (2026, 11, 23, 13, 0), False),   # ... the back-fill first
    ((2026, 11, 25), (2026, 11, 27, 10, 0), (2026, 11, 27, 12, 30), True)])  # Thanksgiving: Wednesday to Friday 10:00
def test_a_wake_in_a_session_days_after_an_etfs_close_leaves_no_old_price_in_the_minutes(
        day, wake, later, trade_first, cfg, keys, monkeypatch, tmp_path):
    """Awake in the next session a day or more after the close slept over: the 1-minute download (one
    day long) begins at that session's first bar, so the minutes before it kept the pre-sleep price the
    roll had carried over the time off, swapped price or not (a trade first: no swap; the back-fill
    first: rewritten from today's bar on only). TODAY read +1.0% all session and 1H until 10:30 (true
    0), and a restart the same day kept it. The 1-minute grid's shut hours take the 5-minute one's."""
    from conftest import ny_ts
    from moneyflow import history, stats
    from moneyflow.series import G1_SIZE
    eng, clk, spy, truth = _sleep_over_the_close(cfg, keys, monkeypatch, tmp_path, day, wake, trade_first)
    w = ny_ts(*wake)
    assert eng.backfill_state["state"] == "done" and spy.price == 606.0
    k1 = int(w // 60)
    shut = [k for k in range(k1 - G1_SIZE + 1, k1) if not clock.market_open("us_regular", (k + 1) * 60 - 1)]
    assert shut and all(spy.g1.get(k) == 606.0 for k in shut)
    for t in (w + 60, ny_ts(*later)):
        _tick(eng, clk, clk.now() + 10, t, step=30.0,
              feed=lambda x: spy.on_price(truth(x), x, x) if clock.market_open("us_regular", x) else None)
        for tf in ("today", "1h"):
            assert stats.window_return(spy, stats.window_for(tf, t), t)[1] == 0.0, (t, tf)
    eng.save_cache()                                           # and a restart the same day reads the same
    again = Engine(cfg, keys, clock.Clock(fixed=ny_ts(*later) + 60))
    inst = again.instruments["alpaca:SPY"]
    history.load(tmp_path, inst, ny_ts(*later) + 60)
    t = ny_ts(*later) + 60
    assert stats.price_at(inst, stats.window_for("today", t).start, t) == 606.0


# ── fourteenth round: a sleep or an outage from before an ETF's first print of the run ─────────────
def _session_levels():
    """The price as of t: each session at its own level from its open, its close carried till the next."""
    from conftest import ny_ts
    opens = [(ny_ts(2026, 11, 23, 9, 30), 606.0), (ny_ts(2026, 11, 24, 9, 30), 612.0),
             (ny_ts(2026, 11, 25, 9, 30), 618.0), (ny_ts(2026, 11, 27, 9, 30), 618.0)]

    def truth(t):
        p = 600.0                                                             # up to Friday 20 Nov's close
        for o, v in opens:
            if t >= o:
                p = v
        return p
    return truth


def _fed_engine(cfg, keys, monkeypatch, tmp_path, saved, start, truth, fail=None, write_cache=True, drop=None,
                ext=False):
    """An engine started at `start` from a cache saved at `saved` (each ETF's closes as the live series
    writes them), with an Alpaca stream whose messages are its feed's coverage, and IEX bars of `truth`
    (a bar for each session step; `ext`: pre- and post-market ones too, 0.1% above). `fail()`: downloads
    fail then; `drop(symbol)`: the answer has no bar for it (Alpaca's anomaly). Returns (engine, clock)."""
    from moneyflow import history
    from moneyflow.connectors import alpaca as alp
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G1_STEP, G5_STEP
    if write_cache:
        old, _ = _gap_engine(cfg, keys, saved, tmp_path)
        for etf in [i for i in old.instruments.values() if i.source == "alpaca"]:
            for g, step in ((etf.g5, G5_STEP), (etf.g1, G1_STEP)):
                for k in range(int(saved // step) - g.size + 1, int(saved // step)):
                    g.put(k, truth((k + 1) * step - 1), clock.market_open("us_regular", (k + 1) * step - 1))
            history.save(tmp_path, etf)
    eng, clk = _gap_engine(cfg, keys, start, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={},
                                                     health=types.SimpleNamespace(last_msg=None, last_ok=None))

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        if fail is not None and fail():
            raise OSError(11001, "getaddrinfo failed")
        step = 300 if tf == "5Min" else 60
        t, rows = -(-int(since) // step) * step, []
        while t + step <= clk.now():
            if clock.market_open("us_regular", t + step - 1):
                rows.append((t, truth(t + step - 1), 1e6))
            elif ext and clock.market_open("us_extended", t + step - 1):
                rows.append((t, truth(t + step - 1) * 1.001, 1e3))
            t += step
        return {sym: list(rows) for sym in symbols if not (drop is not None and drop(sym))}
    monkeypatch.setattr(alp, "fetch_bars", bars)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    return eng, clk


def _stream_up(eng, clk, t0, t1, truth, step=30.0):
    """The stream up from t0 to t1: its messages (the feed's coverage), and SPY's prints in the session."""
    spy = eng.instruments["alpaca:SPY"]

    def feed(t):
        eng.connectors["alpaca"].health.last_msg = t
        if clock.market_open("us_regular", t):
            spy.on_price(truth(t), t, t)
    _tick(eng, clk, t0, t1, step=step, feed=feed)


@pytest.mark.parametrize("case", ["saturday_start_asleep_to_monday_10", "monday_premarket_start_asleep_to_friday",
                                  "premarket_start_network_down_over_the_open"])
def test_a_sleep_or_outage_from_before_an_etfs_first_print_is_back_filled_not_carried(
        case, cfg, keys, monkeypatch, tmp_path):
    """An ETF's steps count as trading only after its first live price of the run, so a sleep or a
    network outage that began before it (a start on a weekend or before the open) and went on over one
    or more sessions had them carried from the cache's close: not live, but with no gap, so they were
    never back-filled. 1H read the night's move as the last hour's (+1.0%, true 0), and after days away
    TODAY the whole move since the cache (+3.0%, true 0). Once its feed has reported (a login, any
    message), a session step it did not cover is a gap, back-filled from the exchange."""
    from conftest import ny_ts
    from moneyflow import stats
    truth = _session_levels()
    saved = ny_ts(2026, 11, 20, 18, 0)                                       # Friday's close, 600
    start, asleep, wake, read = {
        "saturday_start_asleep_to_monday_10": ((2026, 11, 21, 10), (2026, 11, 21, 12), (2026, 11, 23, 10), (2026, 11, 23, 10, 30)),
        # Monday's pre-market to Friday after Thanksgiving: three sessions slept over
        "monday_premarket_start_asleep_to_friday": ((2026, 11, 23, 4), (2026, 11, 23, 5, 16), (2026, 11, 27, 9, 40), (2026, 11, 27, 10, 30)),
        "premarket_start_network_down_over_the_open": ((2026, 11, 23, 8), (2026, 11, 23, 9, 20), (2026, 11, 23, 9, 50), (2026, 11, 23, 10, 30)),
    }[case]
    start, asleep, wake, read = ny_ts(*start), ny_ts(*asleep), ny_ts(*wake), ny_ts(*read) + 20
    eng, clk = _fed_engine(cfg, keys, monkeypatch, tmp_path, saved, start, truth)
    asyncio.run(eng.seed_history())
    _stream_up(eng, clk, start, asleep, truth)                                # up, no session print yet
    clk.set_fixed(wake)                                                       # asleep (or the network down) since
    eng.tick()
    spy = eng.instruments["alpaca:SPY"]
    assert spy.holes["g5"] is not None and spy.holes["g1"] is not None
    _stream_up(eng, clk, wake, wake + 10, truth, step=5.0)
    asyncio.run(eng._backfill("wake"))
    assert eng.backfill_state["state"] == "done" and spy.holes == {"g1": None, "g5": None}
    _stream_up(eng, clk, wake + 15, read, truth)
    for tf in ("1h", "today", "1w"):
        w = stats.window_for(tf, read)
        want = (truth(read) / truth(w.start) - 1.0) * 100.0
        assert stats.window_return(spy, w, read)[1] == pytest.approx(want, abs=1e-9), (tf, case)


def test_a_failed_back_fill_after_such_a_sleep_leaves_gaps_the_next_start_fetches(cfg, keys, monkeypatch, tmp_path):
    """The same sleep with the network still down after the wake: the session slept over stays unknown
    and is saved so, and the next start downloads it. Carried, it was saved as if it were real data and
    never fetched again."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP
    truth = _session_levels()
    saved, start, asleep, wake = ny_ts(2026, 11, 20, 18), ny_ts(2026, 11, 21, 10), ny_ts(2026, 11, 21, 12), ny_ts(2026, 11, 23, 11)
    down = {"on": False}
    eng, clk = _fed_engine(cfg, keys, monkeypatch, tmp_path, saved, start, truth, fail=lambda: down["on"])
    asyncio.run(eng.seed_history())
    _stream_up(eng, clk, start, asleep, truth)
    clk.set_fixed(wake)
    eng.tick()
    down["on"] = True
    asyncio.run(eng.seed_history(incremental=True, only={"alpaca:SPY"}))     # the back-fill's download fails
    spy = eng.instruments["alpaca:SPY"]
    session = range(int(ny_ts(2026, 11, 23, 9, 30) // G5_STEP), int(wake // G5_STEP))
    assert all(spy.g5.get(k) != spy.g5.get(k) and not spy.g5.is_live(k) for k in session)
    eng.save_cache()
    again, _ = _fed_engine(cfg, keys, monkeypatch, tmp_path, saved, wake + 600, truth, write_cache=False)
    history.load(tmp_path, again.instruments["alpaca:SPY"], wake + 600)
    assert again.instruments["alpaca:SPY"].holes["g5"] == ny_ts(2026, 11, 23, 9, 30)


def test_a_holiday_start_asleep_over_the_next_session_is_back_filled(cfg, keys, monkeypatch, tmp_path):
    """Opened on Thanksgiving (no ETF trades that day), asleep from 22:00 through Friday's short session
    (SPY +1.0%), woken on Monday at 10:00: the wake's back-fill skipped every ETF (none had a gap), and
    TODAY read +1.0% all Monday (true 0), HYG and IEF pushing the risk meter's credit input. Now Friday's
    session is a gap, back-filled."""
    from conftest import ny_ts
    from moneyflow import stats

    def truth(t):
        return 606.0 if t >= ny_ts(2026, 11, 27, 9, 30) else 600.0
    saved, start = ny_ts(2026, 11, 25, 18, 0), ny_ts(2026, 11, 26, 10, 0)
    asleep, wake, read = ny_ts(2026, 11, 26, 22, 0), ny_ts(2026, 11, 30, 10, 0), ny_ts(2026, 11, 30, 10, 30) + 20
    eng, clk = _fed_engine(cfg, keys, monkeypatch, tmp_path, saved, start, truth)
    asyncio.run(eng.seed_history())
    _stream_up(eng, clk, start, asleep, truth, step=300.0)
    clk.set_fixed(wake)
    eng.tick()
    spy = eng.instruments["alpaca:SPY"]
    assert spy.holes["g5"] == ny_ts(2026, 11, 27, 9, 30)
    _stream_up(eng, clk, wake, wake + 10, truth, step=5.0)
    asyncio.run(eng._backfill("wake"))
    _stream_up(eng, clk, wake + 15, read, truth)
    w = stats.window_for("today", read)
    assert stats.window_return(spy, w, read)[1] == 0.0 and stats.price_at(spy, w.start, read) == 606.0


def test_an_etf_cache_from_before_format_5_is_downloaded_again_once(cfg, keys, tmp_path):
    """A cache written before this can hold sessions a sleep from before the ETF's first trade passed
    over, carried from its last close (not live, no gap): an ETF's cache of format 4 or older is
    downloaded again from its first step, once (other instruments' are not)."""
    import json
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP, Instrument
    saved, now = ny_ts(2026, 11, 27, 18, 0), ny_ts(2026, 11, 28, 12, 0)
    for key, hours in (("alpaca:SPY", "us_regular"), ("binance_spot:BTCUSDT", "24/7")):
        a = Instrument(key, key.split(":")[0], key.split(":")[1], hours)
        k = int(saved // G5_STEP)
        for j in range(k - 2000, k):
            a.g5.put(j, 600.0, clock.market_open(hours, (j + 1) * G5_STEP - 1))
        history.save(tmp_path, a)
        f = tmp_path / f"{key.replace(':', '_')}.json"
        for version, whole in ((4, hours == "us_regular"), (5, False)):
            d = json.loads(f.read_text())
            d["v"] = version
            f.write_text(json.dumps(d))
            b = Instrument(key, key.split(":")[0], key.split(":")[1], hours)
            history.load(tmp_path, b, now)
            first = (k - 2000) * G5_STEP
            assert (b.holes["g5"] == first) is whole, (key, version)


def test_a_standby_round_of_the_alpaca_fallback_counts_as_far_as_the_network_is_known_up():
    """A round of the REST fallback that asks is its feed heard (now). One that stands by (the stream up,
    the market closed, the stream connecting or refused: 406, another copy holds the key's one stream)
    counts as far as the network is known up: the newest message of any stream, or a trade answer of its
    own. Counted not at all (the fifteenth round), a stream refused or unreachable left the ETFs no feed
    signal outside a session: a session slept over was carried, not back-filled (TODAY +1.13% all Monday,
    true 0). Counted as now (the sixteenth), a round in the half-minute before a dropped connection is
    noticed covered the next minute, and one across 16:00 the 15:59 minute and the night after it. A
    stream connected but silent for a minute in a session is asked about (a stall; a day nothing
    trades, whose old trades are a real answer). What each round says shows."""
    from conftest import ny_ts
    from moneyflow.connectors.alpaca import AlpacaFallback
    from moneyflow.series import Instrument
    t = ny_ts(2026, 11, 24, 10, 0)                                     # a session
    stream = types.SimpleNamespace(health=types.SimpleNamespace(state="connected", last_msg=t - 5), ws=object(),
                                   refused_until=0.0, available=lambda: (True, ""),
                                   subs={"SPY": Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")})
    other = types.SimpleNamespace(health=types.SimpleNamespace(last_msg=t - 2))      # another stream (Binance's)
    ctx = ctx_for(None)
    ctx.clock = clock.Clock(fixed=t)
    fb = AlpacaFallback(ctx, {}, stream)
    cached = PollConnector(ctx, {})                          # a poller's data can come from its cache:
    cached.health.last_msg = t + 10**6                       # no sign of the network
    ctx.engine.connectors = {"alpaca": stream, "alpaca_rest": fb, "binance_spot": other, "coingecko": cached}
    fb.started -= 60                                         # (past its first 30 s)
    asked = []

    async def get_json(url, params=None, headers=None, timeout=30):
        asked.append(url)
        return {"trades": {"SPY": {"p": 600.5, "t": "2026-11-24T15:00:00Z"}}}

    async def fast(seconds):
        await asyncio.sleep(0.01)
    fb.get_json, fb.sleep = get_json, fast

    def one_round(at=None):
        if at is not None:
            ctx.clock.set_fixed(at)
        fb.health.last_ok = None

        async def go():
            task = asyncio.create_task(fb.run())
            await asyncio.sleep(0.1)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        asyncio.run(go())
    other.health.last_msg = t + 50                           # a message stamped ahead (a clock corrected back)
    one_round()
    assert fb.health.last_ok == t and not asked              # never ahead of now
    other.health.last_msg = t - 2
    one_round()                                              # the stream up: as far as the newest message
    assert fb.health.state == "ok" and fb.health.last_ok == t - 2 and not asked
    assert fb.health.detail == "standby (stream connected)"              # shown (it was wiped)
    stream.health.last_msg = other.health.last_msg = t - 40  # the network down since: not noticed yet
    one_round()
    assert fb.health.last_ok == t - 40 and fb.health.detail == "standby (stream connected)" and not asked
    stream.health.last_msg, other.health.last_msg = t - 90, t - 1        # the stream silent a minute and more
    one_round()
    assert asked and fb.health.last_ok == t and fb.health.detail == "asking (stream connected but silent)"
    assert stream.subs["SPY"].price == 600.5
    asked.clear()
    fb.health.last_msg = stream.health.last_msg = None
    stream.health.state, stream.ws, stream.refused_until = "reconnecting", None, t + 300   # refused (406)
    sat = ny_ts(2026, 11, 21, 12, 0)
    other.health.last_msg = sat - 1
    one_round(sat)
    assert fb.health.last_ok == sat - 1 and fb.health.detail == "standby (US market closed)" and not asked
    other.health.last_msg = stream.health.last_msg = None     # nothing heard of the network at all
    one_round()
    assert fb.health.last_ok is None and fb.health.detail == "standby (US market closed)"
    other.health.last_msg = t - 1
    fb.started, stream.refused_until = t - 5, 0.0                        # just started, the stream connecting
    one_round(t)
    assert fb.health.last_ok == t - 1 and fb.health.detail == "standby (stream connecting)" and not asked
    fb.started -= 60                                         # the stream down in a session: real rounds
    one_round(t)
    assert asked and fb.health.last_ok == t and fb.health.last_msg == t
    assert fb.health.detail == "polling (stream down)"


def test_a_standby_round_after_the_close_leaves_a_minute_the_network_was_down_a_gap(cfg, keys, tmp_path):
    """SPY trades every few seconds; the network drops at 15:58:30 with the PC awake, the stream not yet
    aware of it. The fallback's round at 16:00:05 stands by (the market closed): counted as now, it
    covered the 15:59 minute (no gap, nothing stale), and the night and Tuesday's TODAY reference held the
    price from before the outage (the night after a gap, fixed in the fifteenth round). It counts as far
    as the network is known up, 15:58:30: 15:59 is a gap, the night after it unknown until downloaded."""
    from conftest import ny_ts
    from moneyflow.connectors.alpaca import AlpacaFallback
    from moneyflow.connectors.base import Health
    from moneyflow.series import G1_STEP
    t0, down = ny_ts(2026, 11, 23, 15, 50), ny_ts(2026, 11, 23, 15, 58) + 30
    eng, clk = _gap_engine(cfg, keys, t0, tmp_path)
    spy = eng.instruments["alpaca:SPY"]
    stream = types.SimpleNamespace(health=Health("alpaca", "stream", "x"), ws=object(), refused_until=0.0,
                                   subs={"SPY": spy}, available=lambda: (True, ""))
    stream.health.state = "connected"
    other = types.SimpleNamespace(health=Health("binance_spot", "stream", "x"))
    fb = AlpacaFallback(eng.ctx, {}, stream)
    fb.started -= 60
    eng.connectors = {"alpaca": stream, "alpaca_rest": fb, "binance_spot": other}

    def trade(t):
        if t < down:
            spy.on_price(600.0 + (t - t0) / 600, t, t)
            stream.health.mark_msg(t)
            other.health.mark_msg(t)
    _tick(eng, clk, t0, down + 20, step=5.0, feed=trade)
    clk.set_fixed(ny_ts(2026, 11, 23, 16, 0) + 5)            # the fallback's round: it stands by

    async def go():
        task = asyncio.create_task(fb.run())
        await asyncio.sleep(0.1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())
    assert down - 10 < fb.health.last_ok < down              # the last message before the drop
    _tick(eng, clk, clk.now() + 5, ny_ts(2026, 11, 23, 16, 30), step=10.0)
    m = ny_ts(2026, 11, 23, 15, 59)
    assert spy.holes["g1"] == m and spy._stale_from == m
    assert spy.g1.get(int(m // G1_STEP)) != spy.g1.get(int(m // G1_STEP))                     # unknown
    night = int(ny_ts(2026, 11, 23, 16, 20) // G1_STEP)
    assert spy.g1.get(night) != spy.g1.get(night)            # (not the pre-outage price carried)


def test_the_first_tick_after_a_sleep_takes_nothing_heard_since_the_wake_as_cover(cfg, keys, tmp_path):
    """After a wake every overdue timer fires in deadline order: the fallback's round or a REST answer
    can come before the first tick, and with the wake as its feed's time every slept step counted as
    covered. SPY that had traded held its 9:46 price over the time asleep, flagged live, with no gap
    (1H +0.43% at 11:56, true 0). The first tick after a sleep takes the feed as heard until the tick
    before it only: those steps are gaps."""
    from conftest import ny_ts
    t0 = ny_ts(2026, 11, 23, 9, 46)
    spys = {}
    for woke in (False, True):
        eng, clk = _gap_engine(cfg, keys, t0 - 300, tmp_path)
        health = types.SimpleNamespace(last_msg=None, last_ok=None)
        eng.connectors = {"alpaca": types.SimpleNamespace(health=health)}
        spy, hyg = eng.instruments["alpaca:SPY"], eng.instruments["alpaca:HYG"]

        def feed(t):
            health.last_msg = t
            spy.on_price(603.4, t, t)
            hyg.on_price(80.1, t, t)
        _tick(eng, clk, t0 - 300, t0, step=1.0, feed=feed)    # awake: a tick a second, both trading
        wake = t0 + 2 * 3600
        health.last_msg = wake                                 # (came in before the first tick...)
        hyg.note_message(wake)                                 # (...and an unchanged quote of HYG's)
        clk.set_fixed(wake)
        eng.tick(woke=woke)
        spys[woke] = spy, hyg
    k = int((t0 + 3600) // 60)
    for told_not in spys[False]:                               # (as the tick saw it unaided)
        assert told_not.holes["g1"] is None and told_not.g1.get(k) == told_not.g1.get(k)
    assert spys[False][0].g1.get(k) == 603.4 and spys[False][0].g1.is_live(k)
    for inst in spys[True]:
        assert inst.holes["g1"] == t0 + 60 and inst.g1.get(k) != inst.g1.get(k) and not inst.g1.is_live(k)
        assert inst.holes["g5"] is not None


def test_the_tick_loop_tells_the_first_tick_after_a_wall_clock_jump(cfg, keys, monkeypatch):
    """The tick loop's rounds come a second apart: a jump of the wall clock between two (over 25 s, as
    the watchdog's) is a sleep, and only the first tick after it is told so."""
    import moneyflow.engine as engine_mod
    eng = Engine(cfg, keys, clock.Clock(fixed=1795528800.0))
    walls = iter([1000.0, 1001.0, 1002.0, 1100.0, 1101.0] + [1102.0 + i for i in range(100)])
    mono = iter(float(i) for i in range(1000))
    monkeypatch.setattr(engine_mod, "time", types.SimpleNamespace(time=lambda: next(walls), monotonic=lambda: next(mono)))
    eng._last_wall = 999.0
    told = []
    eng.tick = lambda now=None, woke=False: told.append(woke)

    async def go():
        task = asyncio.create_task(eng._loop())
        while len(told) < 5:
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())
    assert told[:5] == [False, False, False, True, False]


def test_a_gap_older_than_the_history_starts_at_its_oldest_step_and_is_downloaded(cfg, keys, monkeypatch, tmp_path):
    """An old cache's one-time whole download is a gap at its first value (history.load), which the
    35-day horizon passes within 5 minutes of the start. A back-fill attempt after that dropped it as
    older than the history: a first start whose downloads failed for some minutes (the network down for
    6, or an Alpaca error for 40 s near a 5-minute step's end) lost that download for good, and the
    cache was saved in the new format. Such a gap starts at the horizon now: downloaded once the
    network is back."""
    from moneyflow.connectors import binance as bn
    from moneyflow.series import G5_STEP
    now = 1795528800.0
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    btc = eng.instruments["binance_spot:BTCUSDT"]
    k = int(now // G5_STEP)
    btc.seed("g5", [(j * G5_STEP, 100.0) for j in range(k - 10079, k)], now)
    horizon = now - 35 * 86400
    btc.holes["g5"] = horizon - 120                            # passed by the horizon (minutes in)
    attempts = lambda: eng.backfill_state.get("attempt", 0)    # noqa: E731
    klines, _ = _exchange(lambda t: 101.0, fail=lambda: attempts() <= 1)     # the network down at first
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng._backfill("network outage"))
    assert eng.backfill_state["state"] == "done" and eng.backfill_state["attempts"] == 2
    assert klines.starts[("BTCUSDT", "5m")][-1] <= horizon + 600                # the whole range
    assert btc.g5.get(k - 10000) == 101.0 and btc.holes == {"g1": None, "g5": None}


def test_a_gap_a_download_has_yet_to_fill_is_saved_with_the_cache(tmp_path):
    """An old cache's one-time whole download is a gap that nothing in the data shows: stopped before it
    succeeded (the network down at the first start), the cache was saved in the new format and every
    later start trusted it. The gaps a download has yet to fill are saved with the cache and loaded (at
    the oldest step the grid holds at most)."""
    from moneyflow import history
    from moneyflow.series import G5_STEP, Instrument
    now = 1795528800.0
    k = int(now // G5_STEP)

    def reloaded(hole):
        a = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT", "24/7")
        for j in range(k - 3000, k):
            a.g5.put(j, 100.0, True)
        a.holes["g5"] = hole
        history.save(tmp_path, a)
        b = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT", "24/7")
        history.load(tmp_path, b, now)
        return b.holes["g5"]
    assert reloaded(None) is None
    assert reloaded((k - 3000) * G5_STEP) == (k - 3000) * G5_STEP                # still to download
    assert reloaded(now - 40 * 86400) == (k - 10080 + 1) * G5_STEP               # (the grid's oldest step)


@pytest.mark.parametrize("answer", ["no_bar_for_spy", "full"])
def test_after_a_gap_over_a_close_the_night_is_not_the_price_from_before_the_gap(answer, cfg, keys, monkeypatch,
                                                                                 tmp_path):
    """Asleep from Monday 12:30 (SPY at 600, 606 from 13:00) to Tuesday 10:00: the roll carried 600 into
    every shut step from Monday 16:00, and with a back-fill answer that had no bar for SPY (an Alpaca
    anomaly: given up after that one attempt) they stayed, so TODAY read +1.0% (true 0), status live,
    until 17:00. The shut steps after a session minute the feed missed are unknown until a session
    print or a download that covers the gap: unknown with that answer, the close with a full one."""
    from conftest import ny_ts
    from moneyflow import stats
    from moneyflow.series import G5_STEP
    asleep, close, wake = ny_ts(2026, 11, 23, 12, 30), ny_ts(2026, 11, 23, 16, 0), ny_ts(2026, 11, 24, 10, 0)

    def truth(t):                     # (its history moves every 5 minutes, as SPY's does: a 15-minute limit)
        return 606.0 if t >= ny_ts(2026, 11, 23, 13, 0) else 600.0 + 0.01 * ((t // 300) % 7)
    leaving_out = []
    eng, clk = _fed_engine(cfg, keys, monkeypatch, tmp_path, ny_ts(2026, 11, 20, 18, 0), ny_ts(2026, 11, 23, 9, 0),
                           truth, drop=lambda sym: sym in leaving_out)
    asyncio.run(eng.seed_history())
    _stream_up(eng, clk, ny_ts(2026, 11, 23, 9, 0), asleep, truth)
    clk.set_fixed(wake)
    eng.tick()
    spy = eng.instruments["alpaca:SPY"]
    night = range(int(close // G5_STEP), int(ny_ts(2026, 11, 24, 9, 30) // G5_STEP))
    assert all(spy.g5.get(k) != spy.g5.get(k) for k in night)          # unknown, not 12:30's carried
    if answer == "no_bar_for_spy":
        leaving_out.append("SPY")
    asyncio.run(eng._backfill("wake"))
    leaving_out.clear()
    _stream_up(eng, clk, wake + 15, wake + 90, truth)
    read = wake + 90
    got = stats.window_return(spy, stats.window_for("today", read), read)[1]
    if answer == "full":
        assert abs(got) < 1e-9 and all(spy.g5.get(k) == 606.0 for k in night)
    else:
        assert got != got and all(spy.g5.get(k) != spy.g5.get(k) for k in night)   # unknown, not +1.0%


def test_a_wake_after_the_close_takes_the_night_from_the_5_minute_grid(cfg, keys, monkeypatch, tmp_path):
    """Asleep from Monday 12:30 to Tuesday 16:10 (SPY 600, 606 from 13:00): the back-fill's 1-minute
    download is a day long and began on an IEX post-market bar, so its carry over Monday night was the
    1-minute grid's own step before it, which held 600 carried from before the gap; TODAY read +1.0%
    (true 0) until 16:30, when it reads the 5-minute grid. That step is unknown now (the close after a
    missed session minute), and the carry falls back to the 5-minute grid's close."""
    from conftest import ny_ts
    from moneyflow import stats
    asleep, wake = ny_ts(2026, 11, 23, 12, 30), ny_ts(2026, 11, 24, 16, 10)

    def truth(t):
        return 606.0 if t >= ny_ts(2026, 11, 23, 13, 0) else 600.0
    eng, clk = _fed_engine(cfg, keys, monkeypatch, tmp_path, ny_ts(2026, 11, 20, 18, 0), ny_ts(2026, 11, 23, 9, 0),
                           truth, ext=True)
    asyncio.run(eng.seed_history())
    _stream_up(eng, clk, ny_ts(2026, 11, 23, 9, 0), asleep, truth)
    clk.set_fixed(wake)
    eng.tick()
    asyncio.run(eng._backfill("wake"))
    read = ny_ts(2026, 11, 24, 16, 21) + 30
    _stream_up(eng, clk, wake + 15, read, truth)
    spy = eng.instruments["alpaca:SPY"]
    w = stats.window_for("today", read)
    assert stats.price_at(spy, w.start, read) == 606.0 and abs(stats.window_return(spy, w, read)[1]) < 1e-9


def test_a_cache_does_not_carry_a_close_across_an_empty_session(tmp_path):
    """Loading fills an ETF's empty shut steps with the session's close (a start-up download that failed
    left them empty), but not across an empty session step: after a gap over Friday's close the night
    held the price from before the gap again, until a download. It stays unknown, the gap a hole."""
    from conftest import ny_ts
    from moneyflow import history
    from moneyflow.series import G5_STEP, Instrument, NaN
    gap, opened, saved = ny_ts(2026, 11, 20, 15, 0), ny_ts(2026, 11, 23, 9, 30), ny_ts(2026, 11, 23, 10, 0)
    a = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    for k in range(int(saved // G5_STEP) - 2000, int(saved // G5_STEP)):
        t = k * G5_STEP
        missed = gap <= t < opened
        a.g5.put(k, NaN if missed else 690.0 if t < gap else 700.0,
                 not missed and clock.market_open("us_regular", t + G5_STEP - 1))
    history.save(tmp_path, a)
    b = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    history.load(tmp_path, b, saved + 300)
    night = range(int(ny_ts(2026, 11, 20, 16, 0) // G5_STEP), int(opened // G5_STEP))
    assert all(b.g5.get(k) != b.g5.get(k) for k in night) and b.holes["g5"] == gap


def test_a_one_minute_download_begins_on_a_5_minute_boundary(cfg, keys, monkeypatch, tmp_path):
    """A quiet ETF printed at 10:01 (601, from 600) and next at 10:07. Started on Wednesday at 10:01:30,
    the day-long 1-minute download began at Tuesday 10:02:30, inside a 5-minute step: up to its first
    bar the carry came from the 5-minute grid's close before that step (600), so 10:02-10:06 read 600.
    It begins at 10:00 now, and takes the 10:01 print."""
    from conftest import ny_ts
    from moneyflow.connectors import alpaca as alp, binance as bn
    from moneyflow.series import G1_STEP, G5_STEP
    start = ny_ts(2026, 11, 25, 10, 1) + 30
    t10 = ny_ts(2026, 11, 24, 10, 0)

    def price(t):                                   # the last print at or before t
        return 600.0 if t < t10 + 60 else 601.0 if t < t10 + 420 else 602.0
    starts = []

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        starts.append((tf, since))
        step = 300 if tf == "5Min" else 60
        out, t = [], -(-int(since) // step) * step
        while t + step <= start:
            if clock.market_open("us_regular", t + step - 1) and (tf == "5Min" or t in (t10 + 60, t10 + 420) or t > t10 + 420):
                out.append((t, price(t + step - 1), 1e5))
            t += step
        return {s: list(out) for s in symbols}
    eng, clk = _gap_engine(cfg, keys, start, tmp_path)
    eng.connectors["alpaca"] = types.SimpleNamespace(available=lambda: (True, ""), cfg={},
                                                     health=types.SimpleNamespace(last_msg=None, last_ok=None))
    monkeypatch.setattr(alp, "fetch_bars", bars)
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    asyncio.run(eng.seed_history())
    uup = eng.instruments["alpaca:UUP"]
    assert [uup.g1.get(int((t10 + 60 * m) // G1_STEP)) for m in range(2, 7)] == [601.0] * 5
    since1 = next(s for tf, s in starts if tf == "1Min")
    assert since1 % G5_STEP == 0 and since1 <= start - 86400 + 60


def test_refused_alpaca_keys_read_the_etfs_as_without_a_key(cfg, keys, monkeypatch, tmp_path):
    """Alpaca refused the keys (HTTP 401 on its REST API, 402 at the stream's login): every start-up
    retry and every back-fill asked again and failed, all day (32 failed requests over a morning in the
    review's run), and a message of the stream's or the fallback's could count as the ETFs' coverage.
    The first refusal marks the keys: the ETFs' history comes from Yahoo, as without a key, their steps
    are carried, not covered, nothing asks Alpaca again, and the sources say why. The refusal comes a
    round trip after the first tick, which hands the ETFs the fallback's first round: that feed time is
    forgotten (kept, every session minute was a gap, back-filled from Yahoo each minute, all session)."""
    from conftest import ny_ts
    from moneyflow.connectors import alpaca as alp, binance as bn, pollers
    from moneyflow.connectors.base import STANDBY, Unauthorized
    now = ny_ts(2026, 11, 24, 10, 0)
    eng, clk = _gap_engine(cfg, keys, now, tmp_path)
    eng.cfg = dict(eng.cfg, connectors=dict(eng.cfg["connectors"], yahoo={"enabled": True}))
    eng.make_connectors()
    asked = []

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        asked.append(tf)
        raise Unauthorized("HTTP 401 b'{\"message\": \"unauthorized.\"}'")

    async def chart(session, symbol, interval="5m", rng="60d", now=None):
        step = {"5m": 300, "1m": 60, "1d": 86400}[interval]
        t0 = int((now - (5 if rng == "5d" else 60) * 86400) // step) * step
        return [(t, 80.0) for t in range(t0, int(now) - step, step) if clock.market_open("us_regular", t + step - 1)]
    klines, _ = _exchange(lambda t: 100.0, fail=lambda: False)
    monkeypatch.setattr(alp, "fetch_bars", bars)
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    monkeypatch.setattr(bn, "fetch_klines", klines)
    stream, rest = eng.connectors["alpaca"], eng.connectors["alpaca_rest"]
    rest.health.last_ok = now - 1                            # its first round counted at the first tick
    eng.tick()
    assert eng.instruments["alpaca:HYG"]._seen_ts == now - 1
    asyncio.run(eng.seed_history())
    assert stream.auth_failed and not stream.available()[0] and "refused the keys" in stream.health.detail
    assert asked == ["5Min"]                                            # asked once, then not again
    hyg = eng.instruments["alpaca:HYG"]
    assert hyg.extra["history_source"] == "Yahoo chart (Alpaca refused the keys)" and hyg.price == 80.0
    stream.health.last_msg = rest.health.last_ok = rest.health.last_msg = now + 30   # (no coverage)
    for t in range(int(now), int(now) + 1800, 30):
        clk.set_fixed(t)
        eng.tick()
    assert hyg.holes == {"g1": None, "g5": None} and hyg._seen_ts != hyg._seen_ts
    assert hyg.g1.get(int((now + 900) // 60)) == 80.0 and not hyg.g1.is_live(int((now + 900) // 60))
    assert asyncio.run(rest.poll()) == STANDBY and "refused the keys" in rest.health.detail
    assert asyncio.run(eng.seed_history(incremental=True)) is None and asked == ["5Min"]
    # keys revoked mid-run: a session opened before still sends a print; taken, it armed the feed again
    # and every minute after the session closed was a gap (a Yahoo back-fill each minute)
    stream.on_text('[{"T": "t", "S": "HYG", "p": 81.0, "s": 100, "t": "2026-11-24T15:31:00Z"}]', now + 1830)
    assert not hyg.live_seen and hyg._seen_ts != hyg._seen_ts and hyg.price == 80.0


def test_a_401_or_403_is_the_keys_refused_and_other_errors_are_not():
    """get_json tells a refusal of the keys (HTTP 401, 403: asking again changes nothing) from other
    errors (a 500 is retried as before)."""
    from moneyflow.connectors.base import Unauthorized

    class Resp:
        def __init__(self, status):
            self.status, self.headers = status, {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def read(self):
            return b'{"message": "no"}'
    status = {"v": 401}
    session = types.SimpleNamespace(get=lambda url, params=None, headers=None, timeout=None: Resp(status["v"]))
    p = PollConnector(ctx_for(session), {})
    for code, refused in ((401, True), (403, True), (500, False), (404, False)):
        status["v"] = code
        with pytest.raises(RuntimeError) as e:
            asyncio.run(p.get_json("https://example.invalid/x"))
        assert isinstance(e.value, Unauthorized) is refused and f"HTTP {code}" in str(e.value)


def test_a_rounds_text_goes_when_the_next_round_starts():
    """What a round says of itself stays until the next round starts: a rate limit's "waiting" text
    went away with the next good round (it stayed, the source shown as waiting after it recovered)."""
    from moneyflow.connectors.base import RateLimited
    p = PollConnector(ctx_for(None), {})
    rounds = []

    async def poll():
        rounds.append(1)
        if len(rounds) == 1:
            raise RateLimited("x", 300.0)

    async def fast(seconds):
        await asyncio.sleep(0.01)
    p.poll, p.sleep = poll, fast

    async def go():
        task = asyncio.create_task(p.run())
        while len(rounds) < 3:
            await asyncio.sleep(0.005)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(go())
    assert p.health.state == "ok" and p.health.detail == "" and p.health.last_error.startswith("poll")
