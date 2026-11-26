"""Auto-stop when unused: the idle timer's rules, and the real `serve()` stopping by itself (or not)."""
import asyncio
import socket
import sys
import time

import aiohttp
import pytest

from moneyflow import cli, config as cfgmod
from moneyflow.engine import Engine
from moneyflow.idle import IdleGuard

G = 300.0                                         # the default grace: 5 minutes


def test_started_but_never_opened_stops_after_the_grace():
    g = IdleGuard(G, now=0.0)
    assert not g.should_stop(G - 1) and g.should_stop(G)
    assert g.info(10.0)["stops_in_s"] == pytest.approx(G - 10) and "not opened" in g.reason(G)


def test_an_open_dashboard_or_stream_keeps_it_running():
    g = IdleGuard(G, now=0.0)
    g.stream_opened(5.0)
    assert not g.should_stop(10 * G) and g.info(10 * G)["stops_in_s"] is None
    g.stream_closed(10 * G)                       # the tab is closed: the grace starts now
    assert not g.should_stop(11 * G - 1) and g.should_stop(11 * G)
    assert "no dashboard tab or app connected" in g.reason(11 * G)


def test_a_page_refresh_reconnects_within_the_grace():
    g = IdleGuard(G, now=0.0)
    g.stream_opened(1.0)
    g.stream_closed(100.0)                        # refresh: the WebSocket closes ...
    g.stream_opened(101.5)                        # ... and the new page reconnects 1.5 s later
    assert not g.should_stop(100.0 + 2 * G)
    g.stream_closed(1000.0)
    assert not g.should_stop(1000.0 + G - 1) and g.should_stop(1000.0 + G)


def test_two_viewers_one_leaves():
    g = IdleGuard(G, now=0.0)
    g.stream_opened(1.0)                          # a dashboard tab
    g.stream_opened(2.0)                          # and an app's SSE stream
    g.stream_closed(3.0)
    assert not g.should_stop(3.0 + 5 * G) and g.streams == 1


def test_api_activity_keeps_it_alive_and_then_the_grace_runs():
    g = IdleGuard(G, now=0.0)
    for t in range(0, 3000, 200):                 # an app polling the REST API every 200 s
        g.touch(float(t), "request /api/risk")
        assert not g.should_stop(t + 199.0)
    assert g.should_stop(2800.0 + G) and not g.should_stop(2800.0 + G - 1)
    g2 = IdleGuard(G, now=0.0)
    g2.touch(250.0, "webhook delivered")          # a webhook delivery counts too
    assert not g2.should_stop(400.0) and g2.should_stop(550.0)


def test_opt_out_never_stops():
    g = IdleGuard(0, now=0.0)
    assert not g.should_stop(1e9) and g.info(1e9)["stops_in_s"] is None


def test_config_and_flags(tmp_path, monkeypatch):
    """Defaults only: this PC's git-ignored moneyflow.local.toml and key files are not read."""
    from moneyflow.keys import KeyStore
    monkeypatch.setattr(cfgmod, "USER_FILES", ())
    monkeypatch.setattr(cli, "load_keys", lambda cfg: KeyStore())
    assert cfgmod.load(paths=[])["server"]["idle_shutdown_minutes"] == 5          # auto-stop is the default
    bad = tmp_path / "bad.toml"
    bad.write_text("[server]\nidle_shutdown_minutes = -1\n", encoding="utf-8")
    with pytest.raises(cfgmod.ConfigError):
        cfgmod.load(paths=[bad])
    seen = {}

    async def fake_serve(cfg, keys, dashboard, open_browser):
        seen["idle"] = cfg["server"]["idle_shutdown_minutes"]

    monkeypatch.setattr(cli, "serve", fake_serve)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    for argv, want in ((["run"], 5), (["run", "--stay-running"], 0), (["run", "--idle-minutes", "2"], 2)):
        cli.main(argv)
        assert seen["idle"] == want, argv


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _serve_cfg(tmp_path, port: int, minutes: float) -> dict:
    return cfgmod.load(paths=[], overrides={"general": {"data_dir": str(tmp_path / "data")},
                                            "server": {"port": port, "idle_shutdown_minutes": minutes}})


@pytest.fixture
def no_feeds(monkeypatch):
    """serve() without the network: the engine's feeds and history are not started."""
    async def noop(self, *args, **kwargs):
        return None
    monkeypatch.setattr(Engine, "start", noop)
    monkeypatch.setattr(Engine, "stop", noop)


def _port_free(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def test_serve_stops_by_itself_when_never_opened_and_frees_the_port(tmp_path, keys, no_feeds):
    port = _free_port()
    cfg = _serve_cfg(tmp_path, port, 0.02)        # a 1.2 s grace
    t0 = time.monotonic()
    asyncio.run(asyncio.wait_for(cli.serve(cfg, keys, dashboard=False, open_browser=False), timeout=20))
    took = time.monotonic() - t0
    assert 1.2 <= took < 6, took                  # returned on its own, shortly after the grace
    assert _port_free(port)


def test_serve_stays_up_while_an_app_streams_then_stops(tmp_path, keys, no_feeds):
    port = _free_port()
    cfg = _serve_cfg(tmp_path, port, 0.02)
    events = {}

    async def go():
        task = asyncio.create_task(cli.serve(cfg, keys, dashboard=False, open_browser=False))
        await asyncio.sleep(0.5)
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(f"http://127.0.0.1:{port}/ws?tf=today") as ws:
                await ws.receive(timeout=10)                       # the first snapshot
                async with s.get(f"http://127.0.0.1:{port}/api/health") as r:
                    events["health"] = (await r.json())["idle"]
                await asyncio.sleep(4.0)                           # > 3x the grace, streaming
                events["alive_while_streaming"] = not task.done()
            t_closed = time.monotonic()
        await asyncio.wait_for(task, timeout=15)                   # stops once the app has left
        events["stopped_after_s"] = time.monotonic() - t_closed

    asyncio.run(go())
    assert events["alive_while_streaming"]
    assert events["health"]["streams"] == 1 and events["health"]["stops_in_s"] is None
    assert 1.0 <= events["stopped_after_s"] < 6
    assert _port_free(port)


def test_auto_stop_still_works_while_a_backfill_keeps_retrying(tmp_path, keys, monkeypatch):
    """A back-fill retrying against a dead network (DNS down) must not keep the app up, block the
    loop, or hang the stop: the auto-stop fires on time and stop() cancels the back-fill."""
    from moneyflow.connectors import binance as bn
    calls = []

    async def dns_down(*a, **k):
        calls.append(1)
        raise OSError(11001, "getaddrinfo failed")
    monkeypatch.setattr(bn, "fetch_klines", dns_down)

    engines = []

    async def start(self):                     # no live feeds: just a gap, and its failing back-fill
        engines.append(self)
        self.cfg = dict(self.cfg, connectors=dict(self.cfg["connectors"], yahoo={"enabled": False}))
        self.BACKFILL_SETTLE, self.BACKFILL_FIRST, self.BACKFILL_MAX = 0.0, 0.05, 0.1
        self.seed_state["state"] = "done"
        self.instruments["binance_spot:BTCUSDT"].holes["g5"] = self.clock.now() - 3600
        self.want_backfill("test")
    monkeypatch.setattr(Engine, "start", start)
    port = _free_port()
    cfg = _serve_cfg(tmp_path, port, 0.02)        # a 1.2 s grace
    t0 = time.monotonic()
    asyncio.run(asyncio.wait_for(cli.serve(cfg, keys, dashboard=False, open_browser=False), timeout=20))
    took = time.monotonic() - t0
    assert 1.2 <= took < 6, took                  # stopped on time, the back-fill did not hold it up
    assert len(calls) >= 3                        # ...while it kept retrying
    bf = engines[0].backfill_state                # stopped mid-retry: cancelled, after several attempts
    assert engines[0]._backfill_task.done() and bf.get("state") in ("retrying", "running") and bf.get("attempt", 0) >= 2
    assert _port_free(port)


# ── the stop: the port freed, the cache saved, the streams closed ─────────────────────────────────
def _stop_probe(monkeypatch, port: int) -> dict:
    """Engine.start a no-op; save_cache and the server's shutdown (AppRunner.cleanup) recorded: when
    they ran, and whether the port could be bound during the save (what start.bat needs)."""
    from aiohttp import web

    async def noop(self, *args, **kwargs):
        return None
    monkeypatch.setattr(Engine, "start", noop)
    seen: dict = {"saves": 0}

    def save_cache(self, insts=None):
        seen["saves"] += 1
        seen["save_at"] = time.monotonic()
        seen["port_free"] = _port_free(port)
    monkeypatch.setattr(Engine, "save_cache", save_cache)
    real_cleanup = web.AppRunner.cleanup

    async def cleanup(self):
        seen.setdefault("cleanup_at", time.monotonic())
        await real_cleanup(self)
    monkeypatch.setattr(web.AppRunner, "cleanup", cleanup)
    return seen


def test_the_final_save_runs_at_once_with_the_port_free_while_streams_are_open(tmp_path, keys, monkeypatch):
    """At a stop the feeds stop, the port is freed (start.bat can open a new copy at once), the history
    cache is saved, and then the server shuts down, closing the dashboards' streams first. With a
    WebSocket tab and an SSE app connected, the save waited behind them 15-45 s and about 2 minutes
    (a second Ctrl+C meanwhile skipped it), and the process lingered as long; before that, the port was
    held during the save."""
    import signal
    port = _free_port()
    cfg = _serve_cfg(tmp_path, port, 0)                       # no auto-stop: a signal stops it
    seen = _stop_probe(monkeypatch, port)

    async def go():
        task = asyncio.create_task(cli.serve(cfg, keys, dashboard=False, open_browser=False))
        await asyncio.sleep(0.5)
        async with aiohttp.ClientSession() as s:
            ws = await s.ws_connect(f"http://127.0.0.1:{port}/ws?tf=today")
            await ws.receive(timeout=10)                       # the tab's first snapshot
            sse = await s.get(f"http://127.0.0.1:{port}/sse?tf=today")
            await asyncio.wait_for(sse.content.readuntil(b"\n\n"), 10)   # the app's first event
            t_stop = time.monotonic()
            signal.raise_signal(signal.SIGTERM)                 # a kill (or Ctrl+Break): the graceful stop
            msg = await ws.receive(timeout=10)                  # the tab is told the service goes away
            seen["ws_close"] = (msg.type, msg.data)
            seen["sse_ended"] = await asyncio.wait_for(sse.content.read(), 10) is not None
            await asyncio.wait_for(task, timeout=10)
            seen["stopped_after_s"] = time.monotonic() - t_stop
            seen["save_after_s"] = seen["save_at"] - t_stop
            await ws.close()
            sse.close()
    asyncio.run(go())
    assert seen["saves"] == 1 and seen["port_free"] and seen["save_after_s"] < 2
    assert seen["save_at"] < seen["cleanup_at"]                 # saved before the server waits for anything
    assert seen["ws_close"] == (aiohttp.WSMsgType.CLOSE, aiohttp.WSCloseCode.GOING_AWAY) and seen["sse_ended"]
    assert seen["stopped_after_s"] < 5 and _port_free(port)


def test_closing_the_console_window_saves_the_cache_first(tmp_path, keys, monkeypatch):
    """Windows ends a console process about 5 s after its window is closed, whatever its Python signal
    handlers do (it was gone within 60 ms, unsaved). serve()'s console handler starts the stop and holds
    the process until the cache is saved: on that path the save comes first, once."""
    port = _free_port()
    cfg = _serve_cfg(tmp_path, port, 0)
    seen = _stop_probe(monkeypatch, port)
    got = {}
    real_stop_feeds = Engine.stop_feeds

    async def stop_feeds(self):
        got.setdefault("feeds_stopped_at", time.monotonic())
        await real_stop_feeds(self)
    monkeypatch.setattr(Engine, "stop_feeds", stop_feeds)
    monkeypatch.setattr(cli, "_on_console_close",
                        lambda loop, stop, closing, saved: got.update(loop=loop, stop=stop, closing=closing, saved=saved))

    async def go():
        task = asyncio.create_task(cli.serve(cfg, keys, dashboard=False, open_browser=False))
        await asyncio.sleep(0.5)
        got["closing"].set()                                   # what the handler does on CTRL_CLOSE_EVENT
        got["loop"].call_soon_threadsafe(got["stop"].set)
        saved = await asyncio.get_running_loop().run_in_executor(None, got["saved"].wait, 5)
        got["saved_in_time"] = saved
        await asyncio.wait_for(task, timeout=10)
    asyncio.run(go())
    assert got["saved_in_time"] and seen["saves"] == 1 and seen["save_at"] < got["feeds_stopped_at"]


@pytest.mark.skipif(sys.platform != "win32", reason="a Windows console handler")
def test_the_console_handler_takes_the_close_events_and_leaves_ctrl_c_to_python():
    """The handler serve() registers: for a closing window (and logoff, shutdown) it sets the stop and
    waits until the cache is saved, then tells Windows it is done; Ctrl+C and Ctrl+Break go on to the
    signal handlers (False)."""
    import threading

    async def go():
        loop, stop = asyncio.get_running_loop(), asyncio.Event()
        closing, saved = threading.Event(), threading.Event()
        h = cli._on_console_close(loop, stop, closing, saved)
        assert h is not None
        try:
            assert await loop.run_in_executor(None, h, 0) == 0 and not closing.is_set()   # Ctrl+C
            res = loop.run_in_executor(None, h, 2)                                           # the window closes
            await asyncio.wait_for(stop.wait(), 5)
            await asyncio.sleep(0.5)
            assert closing.is_set() and not res.done()          # it waits for the save...
            saved.set()
            assert await asyncio.wait_for(res, 5) == 1          # ...then lets Windows end the process
        finally:
            cli._console_handler_off(h)
    asyncio.run(go())
