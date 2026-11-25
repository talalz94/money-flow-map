"""Replay the flow map over the service's cached history (offline, the dashboard's own engine code):
for each asset, how often it had an arrow in each window, and when it had none, which rule left it out.

    python tools/replay_flows.py                      # last 24 h every 15 min, all windows, focus on gold
    python tools/replay_flows.py --hours 168 --step-min 60 --tf today 1w --focus gold silver

Reads data/cache (written by the running service every few minutes). The dollar's history is
fetched from Yahoo (ICE DXY, 2 requests) unless --no-network; the notable-stock slot is not replayed.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from moneyflow import clock, config as cfgmod, history  # noqa: E402
from moneyflow.engine import Engine  # noqa: E402
from moneyflow.keys import KeyStore  # noqa: E402
from moneyflow.series import DXY, StablecoinShare, Stitched  # noqa: E402

NaN = float("nan")


def build_engine(data_dir: Path, network: bool) -> tuple[Engine, float]:
    cfg = cfgmod.load(paths=[], overrides={"general": {"data_dir": tempfile.mkdtemp()}, "stocks": {"slots": 0}})
    now = time.time()
    eng = Engine(cfg, KeyStore(), clock.Clock(fixed=now))
    for inst in eng.instruments.values():
        history.load(data_dir / "cache", inst, now + 600)
    if network:
        import aiohttp
        from moneyflow.connectors import pollers

        async def dxy():
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())) as s:
                return (await pollers.yahoo_chart(s, "DX-Y.NYB", "5m", "60d"),
                        await pollers.yahoo_chart(s, "DX-Y.NYB", "1m", "1d"))
        try:
            r5, r1 = asyncio.run(dxy())
            for d in eng.derived:
                if isinstance(d, DXY) and r5:
                    d.seed("g5", r5, r5[-1][0] + 300)
                    if r1:
                        d.seed("g1", r1, r1[-1][0] + 60)
        except Exception as e:  # noqa: BLE001
            print(f"(no dollar history: {e})")
    try:
        cg = json.loads((data_dir / "cache" / "poll_coingecko.json").read_text())["data"]
        dl = json.loads((data_dir / "cache" / "poll_defillama.json").read_text())["data"]
        for d in eng.derived:
            if isinstance(d, StablecoinShare):
                d.set_supply(dl["now"]["total"], dl["now"].get("observed_at", cg["updated_at"]), [tuple(h) for h in dl["hist"]])
                d.set_weights({n: cg["pct"].get(n.lower()) for n in d.basket})
                d.set_anchor(cg["total"], cg["updated_at"])
    except (OSError, KeyError, ValueError) as e:
        print(f"(no stablecoin share: {e})")
    return eng, now


def at(eng: Engine, t: float, now: float) -> None:
    """Put every series at time t: prices as of t, derived series rebuilt up to t."""
    for inst in eng.instruments.values():
        v = inst.g1.asof(t) if now - t < 23 * 3600 else NaN
        if v != v:
            v = inst.g5.asof(t)
        if v == v:
            inst.price, inst.ts, inst.recv_ts, inst._live_seen = v, t - 1, t - 0.9, True
    for d in eng.derived:
        if isinstance(d, DXY):
            v = d.g1.asof(t) if now - t < 23 * 3600 else NaN
            d.price = v if v == v else d.g5.asof(t)
            d.ts, d.recv_ts = t - 1, t - 0.9
            for i in d.inputs:              # its FX legs have no free history: the replayed DXY is ICE's
                i._live_seen, i.ts, i.recv_ts = True, t - 1, t - 0.9
        else:
            d.rebuild(t)
            if isinstance(d, Stitched):
                d._active = d.active_source(t)
                d.ts, d.recv_ts = t - 1, t - 0.9
    eng._cache.clear()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--step-min", type=float, default=15)
    ap.add_argument("--tf", nargs="+", default=["1h", "today", "1w"], choices=["1h", "today", "1w"])
    ap.add_argument("--focus", nargs="+", default=["gold"], help="assets whose missing arrows are explained")
    ap.add_argument("--data", default=str(ROOT / "data"))
    ap.add_argument("--no-network", action="store_true")
    ap.add_argument("--end", type=float, help="replay up to this epoch time (default: 2 minutes ago)")
    a = ap.parse_args()
    eng, now = build_engine(Path(a.data), not a.no_network)
    end, step = (a.end if a.end else now - 120), a.step_min * 60
    times = [end - k * step for k in range(int(a.hours * 60 / a.step_min), -1, -1)]
    with_arrow = {tf: collections.Counter() for tf in a.tf}
    reasons = {tf: collections.Counter() for tf in a.tf}
    example = {}
    for t in times:
        at(eng, t, now)
        for tf in a.tf:
            for n in eng.build_tf(tf, t)["nodes"]:
                has = n.get("no_edge") is None
                with_arrow[tf][(n["id"], has)] += 1
                if not has and n["id"] in a.focus:
                    reasons[tf][(n["id"], n["no_edge"]["code"])] += 1
                    example.setdefault((tf, n["id"], n["no_edge"]["code"]), (clock.iso(t), n["no_edge"]["text"]))
    print(f"{len(times)} moments over the last {a.hours:g} h, every {a.step_min:g} min")
    ids = sorted({k[0] for c in with_arrow.values() for k in c})
    for tf in a.tf:
        share = {i: with_arrow[tf][(i, True)] / max(1, with_arrow[tf][(i, True)] + with_arrow[tf][(i, False)]) for i in ids}
        print(f"  {tf:5s} with an arrow: " + ", ".join(f"{i} {100 * v:.0f}%" for i, v in share.items()))
        for f in a.focus:
            rs = {c: k for (i, c), k in reasons[tf].items() if i == f}
            print(f"        {f} without one, by reason: {dict(sorted(rs.items(), key=lambda kv: -kv[1])) or 'none'}")
    for (tf, i, code), (ts, txt) in sorted(example.items()):
        print(f"  e.g. {tf} {i} {code} at {ts}: {txt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
