"""Edge cases: a dead webhook target, a start with no data at all, bounded memory for headlines."""
import asyncio
import json
from pathlib import Path

import aiohttp
import jsonschema

from moneyflow import clock
from moneyflow.engine import Engine
from moneyflow.news import NewsBook
from moneyflow.server import Webhooks, one_tf

SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "moneyflow" / "schema" / "snapshot.schema.json").read_text())


def test_dead_webhook_target_fails_cleanly(cfg, keys, monkeypatch):
    real_sleep = asyncio.sleep

    async def fast(d):
        await real_sleep(0)

    async def go():
        eng = Engine(cfg, keys, clock.Clock(fixed=1795530000.0))
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())) as s:
            eng.ctx.session = s
            wh = Webhooks(eng, {"targets": [{"url": "http://127.0.0.1:9/hook"}]})   # nothing listens on port 9
            monkeypatch.setattr(asyncio, "sleep", fast)
            await wh._post(wh.targets[0], {"event": "test"})
        return wh

    wh = asyncio.run(go())
    assert wh.failed == 1 and wh.sent == 0


def test_snapshot_with_no_data_at_all_is_valid_and_honest(cfg, keys):
    eng = Engine(cfg, keys, clock.Clock(fixed=1795530000.0))     # nothing connected, nothing seeded
    snap = eng.snapshot()
    for tf in ("1h", "today", "1w"):
        t = one_tf(snap, tf)
        jsonschema.validate(t, SCHEMA)
        assert all(n["status"] == "no_data" and n["z"] is None for n in t["nodes"])
        assert t["edges"] == [] and t["risk"]["score"] is None and t["risk"]["label"] == "NO DATA"
    assert snap["fed"] is None and snap["news"] == []


def test_headline_memory_stays_bounded():
    book = NewsBook(maxlen=80)
    now = 1795530000.0
    for batch in range(60):
        items = [{"title": f"Headline {batch}-{i} about bonds", "url": "", "ts": now - 3600 + batch * 10 + i * 0.01,
                  "source": "T"} for i in range(100)]
        book.add(items, now)
    assert len(book.items) == 80 and len(book.seen) <= 4100
    assert book.latest(5, now)[0]["title"].startswith("Headline 59-")
