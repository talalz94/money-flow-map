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
            assert r.status == 200 and "/api/snapshot?tf=today|1h|1w|all" in (await r.json())["endpoints"]
            for tf in ("1h", "today", "1w"):
                r = await c.get(f"/api/snapshot?tf={tf}")
                assert r.status == 200 and r.headers["Access-Control-Allow-Origin"] == "*"
                snap = await r.json()
                jsonschema.validate(snap, SCHEMA)
                assert snap["timeframe"] == tf and snap["meta"]["heuristic"] is True
                assert {n["id"] for n in snap["nodes"]} >= {"btc", "eth", "spx", "ndx", "gold", "bonds"}
            r = await c.get("/api/snapshot?tf=all")
            allv = await r.json()
            assert set(allv["timeframes"]) == {"1h", "today", "1w"}
            assert (await c.get("/api/snapshot?tf=5m")).status == 400
            n = await (await c.get("/api/nodes/btc?tf=1h")).json()
            assert n["id"] == "btc" and n["source"]["symbol"] == "BTCUSDT"
            assert (await c.get("/api/nodes/nope")).status == 404
            assert isinstance(await (await c.get("/api/edges?tf=today")).json(), list)
            assert "score" in await (await c.get("/api/risk?tf=today")).json()
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
            await hub.broadcast(eng.snapshot())                           # a push reaches the client
            m = json.loads((await ws.receive(timeout=5)).data)
            assert m["tf"] == "1h"
            await ws.close()
            r = await c.get("/sse?tf=1w")
            assert r.headers["Content-Type"].startswith("text/event-stream")
            chunk = await asyncio.wait_for(r.content.readuntil(b"\n\n"), timeout=5)
            assert chunk.startswith(b"event: snapshot\ndata: ")
            payload = json.loads(chunk.split(b"data: ", 1)[1])
            assert payload["data"]["timeframe"] == "1w"
            r.close()
    run(go())
