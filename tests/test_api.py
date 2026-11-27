"""The HTTP/WebSocket/SSE API, against an engine filled with synthetic history (no network)."""
import asyncio
import json
import math
from pathlib import Path

import jsonschema
from aiohttp.test_utils import TestClient, TestServer

from conftest import DATA, ny_ts
from moneyflow import clock, fed
from moneyflow.engine import Engine
from moneyflow.series import G5_STEP
from moneyflow.server import HUB_KEY, make_app

SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "moneyflow" / "schema" / "snapshot.schema.json").read_text())
NOW = ny_ts(2026, 11, 24, 11, 0)


def filled_engine(cfg, keys) -> Engine:
    eng = Engine(cfg, keys, clock.Clock(fixed=NOW))
    k_now = int(NOW // G5_STEP)
    for n, inst in enumerate(eng.instruments.values()):
        drift = (-1) ** n * 0.0002 * (1 + n % 3)
        closes = [(k * G5_STEP, 50.0 * math.exp(drift * (k - k_now) + 0.002 * math.sin(k / 7.0 + n)))
                  for k in range(k_now - 35 * 288, k_now)]
        inst.seed("g5", closes, NOW, [(t, 1e6, 5e5) for t, _ in closes] if inst.v5 is not None else None)
        inst.price, inst.ts, inst.recv_ts = closes[-1][1], NOW - 0.5, NOW - 0.4
        inst._live_seen = True                      # as if a live message had just arrived
    eng.fomc = fed.parse_fomc((DATA / "fomc_excerpt.html").read_text(encoding="utf-8"))
    eng.news.add([{"title": "Fed holds rates steady", "url": "https://example.org/a", "ts": NOW - 120, "source": "Test"}], NOW)
    for d in eng.derived:
        d.rebuild(NOW)
    return eng


def run(coro):
    return asyncio.run(coro)


def test_rest_endpoints_and_schema(cfg, keys):
    async def go():
        eng = filled_engine(cfg, keys)
        async with TestClient(TestServer(make_app(eng, cfg, dashboard=True))) as c:
            r = await c.get("/api")
            assert r.status == 200 and "/api/snapshot?tf=today|1h|1w|1m|all" in (await r.json())["endpoints"]
            for tf in ("1h", "today", "1w", "1m"):
                r = await c.get(f"/api/snapshot?tf={tf}")
                assert r.status == 200 and r.headers["Access-Control-Allow-Origin"] == "*"
                snap = await r.json()
                jsonschema.validate(snap, SCHEMA)
                assert snap["timeframe"] == tf and snap["meta"]["heuristic"] is True
                assert {n["id"] for n in snap["nodes"]} >= {"btc", "eth", "spx", "ndx", "gold", "bonds"}
            r = await c.get("/api/snapshot?tf=all")
            allv = await r.json()
            assert set(allv["timeframes"]) == {"1h", "today", "1w", "1m"}
            month = allv["timeframes"]["1m"]
            assert month["label"] == "1M" and month["window"]["label"] == "last 30 days"
            assert all(n["sigma_pct"] is not None for n in month["nodes"] if n["id"] in ("btc", "spx", "gold"))
            assert (await c.get("/api/snapshot?tf=5m")).status == 400
            n = await (await c.get("/api/nodes/btc?tf=1h")).json()
            assert n["id"] == "btc" and n["source"]["symbol"] == "BTCUSDT"
            assert (await c.get("/api/nodes/nope")).status == 404
            assert isinstance(await (await c.get("/api/edges?tf=today")).json(), list)
            assert "score" in await (await c.get("/api/risk?tf=today")).json()
            h = await (await c.get("/api/history/btc?grid=5m&hours=2")).json()
            assert h["step_s"] == 300 and len(h["closes"]) == 24 and h["closes"][-1][0] == NOW
            assert all(len(r) == 3 and r[1] > 0 for r in h["closes"])
            assert (await c.get("/api/history/btc?grid=2m")).status == 400
            assert (await c.get("/api/history/nope")).status == 404
            news = await (await c.get("/api/news?limit=5")).json()
            assert news[0]["title"] == "Fed holds rates steady" and news[0]["age_s"] == 120
            f = await (await c.get("/api/fed")).json()
            assert f["decision_at"] == "2026-12-09T19:00:00.000Z"
            h = await (await c.get("/api/health")).json()
            assert h["status"] == "ok"
            assert (await c.get("/api/schema")).status == 200
            page = await c.get("/")
            assert page.status == 200 and "MONEY FLOW MAP" in await page.text()
            assert (await c.get("/static/app.js")).status == 200
            # keys never leave the process
            body = await (await c.get("/api/snapshot?tf=all")).text()
            for v in ("TESTKEYID12345", "TESTSECRET67890", "finnhubtesttoken"):
                assert v not in body
    run(go())


def test_webhooks_deliver_events_and_skip_startup_noise(cfg, keys):
    import aiohttp
    from aiohttp import web
    from moneyflow.server import Webhooks

    got = []

    async def go():
        async def hook(request):
            got.append(await request.json())
            return web.Response(status=204)
        app = web.Application()
        app.router.add_post("/hook", hook)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        eng = filled_engine(cfg, keys)
        async with aiohttp.ClientSession() as s:
            eng.ctx.session = s
            wh = Webhooks(eng, {"targets": [{"url": f"http://127.0.0.1:{port}/hook",
                                             "events": ["regime_change", "strongest_flow_change", "extreme_move"]}],
                                "min_interval_s": 0, "extreme_z": 1.0})
            eng.seed_state["state"] = "running"
            assert not wh.due(NOW)                                  # history not loaded: nothing sent
            eng.seed_state["state"] = "done"
            assert wh.due(NOW)
            snap = eng.snapshot()
            tf = snap["timeframes"][snap["default_timeframe"]]
            tf["risk"]["label"] = "NO DATA"
            for n in tf["nodes"]:
                if n["id"] == "btc":
                    n["z"] = 0.0                                    # btc calm at the start
            wh.check(snap, NOW)                                     # first read: sets the baseline only
            snap2 = json.loads(json.dumps(snap))
            t2 = snap2["timeframes"][snap2["default_timeframe"]]
            t2["risk"]["label"], t2["risk"]["score"] = "RISK-OFF", -70
            wh.check(snap2, NOW + 6)                                # NO DATA -> RISK-OFF: start-up, not news
            snap3 = json.loads(json.dumps(snap2))
            t3 = snap3["timeframes"][snap3["default_timeframe"]]
            t3["risk"]["label"], t3["risk"]["score"] = "RISK-ON", 65
            for n in t3["nodes"]:
                if n["id"] == "btc":
                    n["z"] = 3.5
            wh.check(snap3, NOW + 12)
            # the notable-stock slot: an extreme stock appears, then the slot switches to another one
            snap4 = json.loads(json.dumps(snap3))
            t4 = snap4["timeframes"][snap4["default_timeframe"]]
            spx = next(n for n in t4["nodes"] if n["id"] == "spx")
            t4["nodes"].append(dict(spx, id="stock1", label="DELL (notable stock)", z=-3.3,
                                    slot={"index": 1, "symbol": "DELL", "why": ""}))
            wh.check(snap4, NOW + 18)
            snap5 = json.loads(json.dumps(snap4))
            s1 = next(n for n in snap5["timeframes"][snap5["default_timeframe"]]["nodes"] if n["id"] == "stock1")
            s1.update(label="WMT (notable stock)", z=-3.1, slot={"index": 1, "symbol": "WMT", "why": ""})
            wh.check(snap5, NOW + 24)
            await asyncio.sleep(0.5)
        await runner.cleanup()

    run(go())
    events = [g["event"] for g in got]
    assert "regime_change" in events and events.count("regime_change") == 1
    # assets already extreme at start-up are a baseline, not news: only btc (made extreme later) fires,
    # and the slot fires once per stock in it (the same slot id holding DELL, then WMT)
    assert {g["id"] for g in got if g["event"] == "extreme_move"} == {"btc", "stock1"}
    assert [g["symbol"] for g in got if g["event"] == "extreme_move" and g["id"] == "stock1"] == ["DELL", "WMT"]
    rc = next(g for g in got if g["event"] == "regime_change")
    assert rc["from"] == "RISK-OFF" and rc["to"] == "RISK-ON" and rc["schema"] == "moneyflow.webhook/1"
    assert any(g["event"] == "extreme_move" and g["id"] == "btc" for g in got)


def test_python_client_stream_survives_a_service_restart(cfg, keys):
    """examples/client.py's stream keeps going when the service is restarted."""
    import socket
    from aiohttp import web
    from moneyflow.client import MoneyFlowClient

    with socket.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        port = sk.getsockname()[1]

    async def go():
        eng = filled_engine(cfg, keys)

        async def start():
            runner = web.AppRunner(make_app(eng, cfg, dashboard=False))
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", port).start()
            return runner

        runner = await start()
        got = []
        client = MoneyFlowClient(f"http://127.0.0.1:{port}")

        async def consume():
            async for snap in client.stream("1h", max_backoff=0.5):
                got.append(snap["generated_ts"])
                if len(got) >= 2:
                    return

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.5)
        assert len(got) == 1                               # the snapshot sent on connect
        await runner.cleanup()                             # the service goes away ...
        await asyncio.sleep(0.8)
        eng.clock.advance(5)
        runner = await start()                             # ... and comes back
        await asyncio.wait_for(task, timeout=10)
        await runner.cleanup()
        return got

    got = run(go())
    assert len(got) == 2 and got[1] > got[0]


def test_cli_read_rendering(cfg, keys):
    from moneyflow.cli import fmt_read
    from moneyflow.server import one_tf
    eng = filled_engine(cfg, keys)
    text = fmt_read(one_tf(eng.snapshot(), "today"))
    assert text.startswith("MONEY FLOW  TODAY") and "Next Fed decision" in text and "Bitcoin" in text
    assert "not advice" in text


def test_host_guard_token_and_headless(cfg, keys):
    async def go():
        eng = filled_engine(cfg, keys)
        async with TestClient(TestServer(make_app(eng, cfg, dashboard=False))) as c:
            r = await c.get("/api/health", headers={"Host": "evil.example:8765"})
            assert r.status == 403                                       # DNS-rebinding guard
            for h in ("localhost.evil.example", "127.0.0.1.nip.io:8765"):
                assert (await c.get("/api/health", headers={"Host": h})).status == 403
            for h in ("LOCALHOST:8765", "[::1]:8765", "127.0.0.1"):    # host names are case-insensitive
                assert (await c.get("/api/health", headers={"Host": h})).status == 200
            for p in ("/static/../moneyflow.local.toml", "/static/%2e%2e/.env"):
                assert (await c.get(p)).status in (403, 404)
            r = await c.get("/")
            assert (await r.json())["dashboard"].startswith("disabled")
        cfg2 = dict(cfg, server=dict(cfg["server"], token="s3cret-token"))
        async with TestClient(TestServer(make_app(eng, cfg2, dashboard=False))) as c:
            assert (await c.get("/api/health")).status == 401
            assert (await c.get("/api/health", headers={"X-Moneyflow-Token": "s3cret-token"})).status == 200
            assert (await c.get("/api/health?token=s3cret-token")).status == 200
    run(go())


def test_websocket_and_sse_streams(cfg, keys):
    async def go():
        eng = filled_engine(cfg, keys)
        app = make_app(eng, cfg, dashboard=False)
        async with TestClient(TestServer(app)) as c:
            ws = await c.ws_connect("/ws?tf=today")
            m = json.loads((await ws.receive(timeout=5)).data)
            assert m["type"] == "snapshot" and m["tf"] == "today" and m["data"]["timeframe"] == "today"
            await ws.send_str(json.dumps({"tf": "1h"}))
            m = json.loads((await ws.receive(timeout=5)).data)
            assert m["tf"] == "1h" and m["data"]["timeframe"] == "1h"
            hub = app[HUB_KEY]
            assert hub.n_clients == 1
            same = eng.snapshot()
            await hub.broadcast(same)                                     # already sent: not repeated
            eng.clock.advance(1.0)
            await hub.broadcast(eng.snapshot())                           # a new snapshot reaches the client
            m = json.loads((await ws.receive(timeout=5)).data)
            assert m["tf"] == "1h" and m["data"]["generated_ts"] != same["generated_ts"]
            await ws.close()
            r = await c.get("/sse?tf=1w")
            assert r.headers["Content-Type"].startswith("text/event-stream")
            chunk = await asyncio.wait_for(r.content.readuntil(b"\n\n"), timeout=5)
            assert chunk.startswith(b"event: snapshot\ndata: ")
            payload = json.loads(chunk.split(b"data: ", 1)[1])
            assert payload["data"]["timeframe"] == "1w"
            r.close()
    run(go())
