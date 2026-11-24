"""Record raw websocket frames from the live connectors, for offline replay tests.

    python tools/record.py --seconds 300 --out tests/data/recorded.jsonl.gz

Each line: {"c": connector, "t": local receive time (NTP-corrected), "d": frame text}.
Only RECEIVED market-data frames are stored (never what is sent, so no credentials); the
recorder refuses to run while another moneyflow instance holds the single Alpaca stream.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moneyflow import config as cfgmod  # noqa: E402
from moneyflow.cli import load_keys  # noqa: E402
from moneyflow.client import is_running  # noqa: E402
from moneyflow.connectors.base import StreamConnector  # noqa: E402
from moneyflow.engine import Engine  # noqa: E402


async def record(seconds: float, out: Path) -> dict:
    cfg = cfgmod.load()
    keys = load_keys(cfg)
    eng = Engine(cfg, keys)
    import aiohttp
    eng.ctx.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver()))
    await eng.clock.sync()
    eng.make_connectors()
    counts: dict[str, int] = {}
    fh = gzip.open(out, "wt", encoding="utf-8")
    streams = {n: c for n, c in eng.connectors.items() if isinstance(c, StreamConnector)}
    for name, c in streams.items():
        orig = c.on_text

        def wrapped(data, recv, _orig=orig, _name=name):
            fh.write(json.dumps({"c": _name, "t": round(recv, 4), "d": data}, separators=(",", ":")) + "\n")
            counts[_name] = counts.get(_name, 0) + 1
            return _orig(data, recv)
        c.on_text = wrapped
    tasks = [asyncio.create_task(c.run()) for c in streams.values()]
    t0 = time.time()
    try:
        await asyncio.sleep(seconds)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await eng.ctx.session.close()
        fh.close()
    return {"seconds": round(time.time() - t0), "frames": counts, "file": str(out)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=180)
    ap.add_argument("--out", default="data/recorded.jsonl.gz")
    a = ap.parse_args()
    if is_running():
        sys.exit("moneyflow is running on :8765 - stop it first (Alpaca allows one stream connection).")
    print(json.dumps(asyncio.run(record(a.seconds, Path(a.out))), indent=1))


if __name__ == "__main__":
    main()
