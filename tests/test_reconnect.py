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
    assert json.loads(f.read_text())["v"] == history.VERSION == 2
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

    async def chart(session, symbol, interval, rng):
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
    eng.save_cache = lambda: saves.append(1)
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

    async def chart(session, symbol, interval, rng):
        asked.append((symbol, interval, rng))
        return []
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng.seed_history(incremental=True, only={etf.key}, state={}))
    assert (etf.symbol, "5m", "60d") in asked
