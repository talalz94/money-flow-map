"""A tiny client for the moneyflow API (standard library only), for scripts and other apps.

    from moneyflow.client import MoneyFlowClient
    c = MoneyFlowClient()                     # http://127.0.0.1:8765
    snap = c.snapshot("today")                # dict, see docs / /api/schema
    for e in snap["edges"][:3]:
        print(e["from"], "->", e["to"], e["weight"])
    print(c.risk("1h")["score"])

Streaming (needs aiohttp):  async for msg in c.stream("today"): ...
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class MoneyFlowClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8765", token: str | None = None, timeout: float = 10):
        self.base = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _get(self, path: str, **params):
        q = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base}{path}" + (f"?{urllib.parse.urlencode(q)}" if q else "")
        req = urllib.request.Request(url, headers={"X-Moneyflow-Token": self.token} if self.token else {})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode("utf-8"))

    def snapshot(self, tf: str = "today") -> dict:
        return self._get("/api/snapshot", tf=tf)

    def nodes(self, tf: str = "today") -> list:
        return self._get("/api/nodes", tf=tf)

    def node(self, node_id: str, tf: str = "today") -> dict:
        return self._get(f"/api/nodes/{node_id}", tf=tf)

    def edges(self, tf: str = "today") -> list:
        return self._get("/api/edges", tf=tf)

    def risk(self, tf: str = "today") -> dict:
        return self._get("/api/risk", tf=tf)

    def narrative(self, tf: str = "today") -> list:
        return self._get("/api/narrative", tf=tf)

    def news(self, limit: int = 20) -> list:
        return self._get("/api/news", limit=limit)

    def fed(self) -> dict:
        return self._get("/api/fed")

    def sources(self) -> list:
        return self._get("/api/sources")

    def health(self) -> dict:
        return self._get("/api/health")

    async def stream(self, tf: str = "today"):
        """Yield snapshots pushed over the WebSocket (requires aiohttp)."""
        import aiohttp
        url = self.base.replace("http", "ws", 1) + "/ws?" + urllib.parse.urlencode({"tf": tf, **({"token": self.token} if self.token else {})})
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(url, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        yield json.loads(msg.data)["data"]


def is_running(base_url: str = "http://127.0.0.1:8765") -> bool:
    try:
        MoneyFlowClient(base_url, timeout=2).health()
        return True
    except (urllib.error.URLError, OSError, ValueError):
        return False
