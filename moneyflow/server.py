"""The local API and dashboard, served by the same process (aiohttp).

REST   GET /api                      index of endpoints
       GET /api/snapshot?tf=today    the whole read for one timeframe (tf=all: every timeframe)
       GET /api/nodes?tf=  /api/nodes/{id}?tf=  /api/edges?tf=  /api/risk?tf=  /api/narrative?tf=
       GET /api/news?limit=30   /api/fed   /api/events   /api/sources   /api/health   /api/schema
Stream GET /ws                       WebSocket: {"type":"snapshot","data":{...}} every push interval;
                                     send {"tf":"1h"} to receive one timeframe only, {"tf":"all"} for all
       GET /sse?tf=today             Server-Sent Events, same messages ("event: snapshot")
Page   GET /                         the dashboard (unless --no-dashboard)
Everything is read-only. By default the server binds to 127.0.0.1; requests whose Host header is
not localhost are refused (protects against DNS-rebinding), and an optional token can be required.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from . import SCHEMA_ID, __version__, jsonutil, stats

log = logging.getLogger("moneyflow.server")
STATIC = Path(__file__).resolve().parent / "static"
SCHEMA_FILE = Path(__file__).resolve().parent / "schema" / "snapshot.schema.json"
TFS = stats.TIMEFRAMES
ENGINE_KEY = web.AppKey("engine", object)
HUB_KEY = web.AppKey("hub", object)


def one_tf(snap: dict, tf: str) -> dict:
    """Flatten an all-timeframes snapshot into the single-timeframe shape (documented in the schema)."""
    out = {k: v for k, v in snap.items() if k != "timeframes"}
    out.update(snap["timeframes"][tf])
    return out


class Hub:
    """Fan-out of snapshots to WebSocket / SSE clients and webhook targets."""

    def __init__(self, engine, push_interval: float = 1.0, webhooks: dict | None = None):
        self.engine = engine
        self.push_interval = push_interval
        self.ws_clients: dict = {}      # ws -> tf ("all" or one tf)
        self.sse_clients: dict = {}     # queue -> tf
        self.last_sent: dict = {}       # ws -> (generated_ts, tf) of the last snapshot it received
        self.last_push = 0.0
        self.webhooks = Webhooks(engine, webhooks or {})
        engine.listeners.append(self.on_tick)

    @property
    def n_clients(self) -> int:
        return len(self.ws_clients) + len(self.sse_clients)

    async def on_tick(self, engine) -> None:
        now = time.time()
        need_push = self.n_clients and now - self.last_push >= self.push_interval - 0.05
        need_hook = self.webhooks.due(now)
        if not (need_push or need_hook):
            return
        snap = engine.snapshot()
        if need_hook:
            self.webhooks.check(snap, now)
        if need_push:
            self.last_push = now
            await self.broadcast(snap)

    async def broadcast(self, snap: dict) -> None:
        cache = {}

        def payload(tf):
            if tf not in cache:
                data = snap if tf == "all" else one_tf(snap, tf)
                cache[tf] = jsonutil.dumps({"type": "snapshot", "tf": tf, "data": data})
            return cache[tf]

        gen = snap.get("generated_ts")
        for ws, tf in list(self.ws_clients.items()):
            if ws.closed:
                self.ws_clients.pop(ws, None)
                continue
            if self.last_sent.get(ws) == (gen, tf):         # this client already has this snapshot
                continue
            try:
                await asyncio.wait_for(ws.send_str(payload(tf)), timeout=5)
                self.last_sent[ws] = (gen, tf)
            except (ConnectionResetError, asyncio.TimeoutError, RuntimeError):
                self.ws_clients.pop(ws, None)
        for ws in [w for w in self.last_sent if w not in self.ws_clients]:
            self.last_sent.pop(ws, None)
        for q, tf in list(self.sse_clients.items()):
            if q.qsize() > 5:                     # slow client: drop stale updates
                while not q.empty():
                    q.get_nowait()
            q.put_nowait(payload(tf))


class Webhooks:
    """POSTs JSON events to configured URLs: regime_change, strongest_flow_change, extreme_move, snapshot."""

    def __init__(self, engine, cfg: dict):
        self.engine = engine
        self.targets = [t for t in cfg.get("targets", []) if t.get("url")]
        self.min_interval = float(cfg.get("min_interval_s", 60))
        self.extreme_z = float(cfg.get("extreme_z", 3.0))
        self.prev: dict = {}
        self.last_sent: dict = {}
        self.last_check = 0.0
        self.sent = 0
        self.failed = 0

    def due(self, now: float) -> bool:
        # nothing is sent until the history is in: a read without it would only produce a spurious
        # "NO DATA -> RISK-OFF" regime change at every start-up
        return (bool(self.targets) and now - self.last_check >= 5.0
                and self.engine.seed_state.get("state") == "done")

    def check(self, snap: dict, now: float) -> None:
        self.last_check = now
        tf = snap.get("default_timeframe", "today")
        cur = snap["timeframes"][tf]
        events = []
        regime = (cur["risk"] or {}).get("label")
        if self.prev.get("regime") not in (None, regime, "NO DATA") and regime != "NO DATA":
            events.append({"event": "regime_change", "from": self.prev["regime"], "to": regime,
                           "score": cur["risk"].get("score")})
        top = (cur["edges"][0]["from"], cur["edges"][0]["to"]) if cur["edges"] else None
        if top and self.prev.get("top") not in (None, top):
            events.append({"event": "strongest_flow_change", "from": top[0], "to": top[1], "edge": cur["edges"][0]})
        ext = {n["id"] for n in cur["nodes"] if n.get("z") is not None and abs(n["z"]) >= self.extreme_z}
        for nid in ext - self.prev.get("extreme", set()):
            n = next(x for x in cur["nodes"] if x["id"] == nid)
            events.append({"event": "extreme_move", "id": nid, "label": n["label"], "z": n["z"],
                           "change_pct": n["change_pct"]})
        if not self.prev:                       # first read after start-up: a baseline, not a change
            events = []
        events.append({"event": "snapshot"})
        self.prev = {"regime": regime, "top": top, "extreme": ext}
        for t in self.targets:
            wanted = set(t.get("events", ["regime_change", "strongest_flow_change", "extreme_move"]))
            for ev in events:
                if ev["event"] not in wanted:
                    continue
                key = (t["url"], ev["event"], ev.get("id"))
                if now - self.last_sent.get(key, 0) < self.min_interval:
                    continue
                self.last_sent[key] = now
                body = {"schema": "moneyflow.webhook/1", "timeframe": tf, "generated_at": snap["generated_at"],
                        **ev, "risk": cur["risk"].get("score"), "narrative": [b["text"] for b in cur["narrative"][:5]]}
                if ev["event"] == "snapshot":
                    body["snapshot"] = one_tf(snap, tf)
                asyncio.ensure_future(self._post(t, body))

    async def _post(self, target: dict, body: dict) -> None:
        sess = self.engine.ctx.session
        for attempt in range(3):
            try:
                async with sess.post(target["url"], data=jsonutil.dumps(body),
                                     headers={"Content-Type": "application/json", **(target.get("headers") or {})},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status < 300:
                        self.sent += 1
                        return
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(2 ** attempt)
        self.failed += 1
        log.warning("webhook to %s failed after 3 attempts", target["url"].split("?")[0])


def _tf(request) -> str:
    tf = request.query.get("tf") or request.app[ENGINE_KEY].cfg.get("general", {}).get("default_timeframe", "today")
    tf = tf.lower()
    if tf not in TFS and tf != "all":
        raise web.HTTPBadRequest(text=json.dumps({"error": f"tf must be one of {list(TFS)} or all"}),
                                 content_type="application/json")
    return tf


def _json(data, status: int = 200) -> web.Response:
    return web.Response(text=jsonutil.dumps(data), status=status, content_type="application/json")


def make_app(engine, cfg: dict, dashboard: bool = True) -> web.Application:
    scfg = cfg.get("server", {})
    token = scfg.get("token") or ""
    allowed = {"localhost", "127.0.0.1", "[::1]", "::1"} | set(scfg.get("allowed_hosts", []))
    if scfg.get("host") not in (None, "127.0.0.1", "localhost", "::1"):
        allowed.add(scfg.get("host"))
    cors = scfg.get("cors_origin", "*")

    @web.middleware
    async def guard(request, handler):
        host = (request.host or "").rsplit(":", 1)[0] if not (request.host or "").startswith("[") else request.host.split("]")[0] + "]"
        if host not in allowed and "*" not in allowed:
            return _json({"error": "host not allowed"}, 403)
        path = request.path
        if token and (path.startswith("/api") or path in ("/ws", "/sse")):
            got = request.headers.get("X-Moneyflow-Token") or request.query.get("token")
            if got != token:
                return _json({"error": "token required"}, 401)
        if request.method == "OPTIONS":
            resp = web.Response(status=204)
        else:
            resp = await handler(request)
        if cors and path.startswith("/api") and not resp.prepared:
            resp.headers["Access-Control-Allow-Origin"] = cors
            resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = "X-Moneyflow-Token"
        if not resp.prepared:
            resp.headers.setdefault("Cache-Control", "no-store")
        return resp

    app = web.Application(middlewares=[guard])
    app[ENGINE_KEY] = engine
    hub = Hub(engine, float(scfg.get("push_interval_s", 1.0)), cfg.get("webhooks", {}))
    app[HUB_KEY] = hub

    async def index_api(request):
        return _json({"name": "moneyflow", "version": __version__, "schema": SCHEMA_ID, "timeframes": list(TFS),
                      "endpoints": {
                          "/api/snapshot?tf=today|1h|1w|all": "everything for one (or all) timeframes",
                          "/api/nodes?tf=": "the assets on the map", "/api/nodes/{id}?tf=": "one asset",
                          "/api/edges?tf=": "inferred flows", "/api/risk?tf=": "risk-on/off composite",
                          "/api/narrative?tf=": "what the flow says", "/api/news?limit=": "headlines",
                          "/api/fed": "next FOMC decision", "/api/events": "scheduled high-impact releases",
                          "/api/sources": "connector health and latency", "/api/health": "process health",
                          "/api/schema": "JSON Schema of the snapshot",
                          "/ws": "WebSocket stream (send {\"tf\":\"1h\"} to filter)", "/sse?tf=": "Server-Sent Events",
                      }})

    def snap(request):
        return engine.snapshot()

    async def snapshot(request):
        tf = _tf(request)
        s = snap(request)
        return _json(s if tf == "all" else one_tf(s, tf))

    async def nodes(request):
        tf = _tf(request)
        tf = engine.cfg.get("general", {}).get("default_timeframe", "today") if tf == "all" else tf
        return _json(snap(request)["timeframes"][tf]["nodes"])

    async def node(request):
        tf = _tf(request)
        tf = "today" if tf == "all" else tf
        nid = request.match_info["id"]
        tfd = snap(request)["timeframes"][tf]
        for n in tfd["nodes"] + tfd["aux"]:
            if n["id"] == nid:
                return _json(n)
        return _json({"error": f"unknown id {nid!r}"}, 404)

    def part(name):
        async def h(request):
            tf = _tf(request)
            s = snap(request)
            if tf == "all":
                return _json({t: s["timeframes"][t][name] for t in TFS})
            return _json(s["timeframes"][tf][name])
        return h

    async def news(request):
        try:
            n = max(1, min(80, int(request.query.get("limit", "30"))))
        except ValueError:
            n = 30
        return _json(engine.news.latest(n, engine.clock.now()))

    async def fed(request):
        return _json(engine.fed_info(engine.clock.now()) or {"error": "FOMC calendar not loaded yet"})

    async def events(request):
        return _json(engine.upcoming_events(engine.clock.now(), hours=24 * 7, limit=30))

    async def sources(request):
        return _json(engine.sources(engine.clock.now()))

    async def health(request):
        now = engine.clock.now()
        srcs = engine.sources(now)
        return _json({"status": "ok", "version": __version__, "process": engine.process_info(),
                      "clock": engine.clock.info(), "history": engine.seed_state, "resumes": engine.resumes,
                      "clients": hub.n_clients, "webhooks": {"targets": len(hub.webhooks.targets),
                                                             "sent": hub.webhooks.sent, "failed": hub.webhooks.failed},
                      "connectors": {s["id"]: s["state"] for s in srcs}})

    async def schema(request):
        return web.FileResponse(SCHEMA_FILE, headers={"Content-Type": "application/schema+json"})

    async def ws_handler(request):
        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=64 * 1024)
        await ws.prepare(request)
        tf = request.query.get("tf", "all")
        hub.ws_clients[ws] = tf if tf in TFS or tf == "all" else "all"
        try:
            s = engine.snapshot()
            data = s if hub.ws_clients[ws] == "all" else one_tf(s, hub.ws_clients[ws])
            await ws.send_str(jsonutil.dumps({"type": "snapshot", "tf": hub.ws_clients[ws], "data": data}))
            hub.last_sent[ws] = (s.get("generated_ts"), hub.ws_clients[ws])
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        j = json.loads(msg.data)
                    except ValueError:
                        continue
                    t = str(j.get("tf", "")).lower()
                    if t in TFS or t == "all":
                        hub.ws_clients[ws] = t
                        s = engine.snapshot()
                        await ws.send_str(jsonutil.dumps({"type": "snapshot", "tf": t,
                                                          "data": s if t == "all" else one_tf(s, t)}))
                        hub.last_sent[ws] = (s.get("generated_ts"), t)
        finally:
            hub.ws_clients.pop(ws, None)
            hub.last_sent.pop(ws, None)
        return ws

    async def sse_handler(request):
        tf = request.query.get("tf", "all")
        tf = tf if tf in TFS or tf == "all" else "all"
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-store",
                                           "X-Accel-Buffering": "no"})
        if cors:
            resp.headers["Access-Control-Allow-Origin"] = cors
        await resp.prepare(request)
        q: asyncio.Queue = asyncio.Queue()
        hub.sse_clients[q] = tf
        try:
            s = engine.snapshot()
            first = jsonutil.dumps({"type": "snapshot", "tf": tf, "data": s if tf == "all" else one_tf(s, tf)})
            await resp.write(f"event: snapshot\ndata: {first}\n\n".encode())
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    await resp.write(f"event: snapshot\ndata: {msg}\n\n".encode())
                except asyncio.TimeoutError:
                    await resp.write(b": keep-alive\n\n")
        except (ConnectionResetError, asyncio.CancelledError, RuntimeError):
            pass
        finally:
            hub.sse_clients.pop(q, None)
        return resp

    app.router.add_get("/api", index_api)
    app.router.add_get("/api/", index_api)
    app.router.add_get("/api/snapshot", snapshot)
    app.router.add_get("/api/nodes", nodes)
    app.router.add_get("/api/nodes/{id}", node)
    app.router.add_get("/api/edges", part("edges"))
    app.router.add_get("/api/risk", part("risk"))
    app.router.add_get("/api/narrative", part("narrative"))
    app.router.add_get("/api/tiles", part("tiles"))
    app.router.add_get("/api/news", news)
    app.router.add_get("/api/fed", fed)
    app.router.add_get("/api/events", events)
    app.router.add_get("/api/sources", sources)
    app.router.add_get("/api/health", health)
    app.router.add_get("/api/schema", schema)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/sse", sse_handler)
    app.router.add_route("OPTIONS", "/api/{tail:.*}", index_api)

    if dashboard:
        async def page(request):
            return web.FileResponse(STATIC / "index.html")
        app.router.add_get("/", page)
        app.router.add_static("/static/", STATIC, show_index=False)
    else:
        async def no_page(request):
            return _json({"dashboard": "disabled (--no-dashboard)", "api": "/api"})
        app.router.add_get("/", no_page)
    return app
