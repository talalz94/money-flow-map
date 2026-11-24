"""Latency and freshness check: runs the real connectors for N seconds (no history download) and
reports, per instrument and per source, what the program actually experiences:

  latency   = local receive time − the exchange's own timestamp of the update, with the PC clock
              corrected by NTP (the development PC ran 0.95 s fast; uncorrected that is fake lag)
  age       = now − timestamp of the newest data (for pollers: of the quote/headline they returned)
  msgs/s    = message rate actually received

Usage:  python -m moneyflow check --seconds 120 [--out report.json]
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from pathlib import Path

from . import clock, config as cfgmod
from .engine import Engine


def _pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * (len(xs) - 1)))]


async def run_check(cfg: dict, keys, seconds: float) -> dict:
    eng = Engine(cfg, keys)
    t_start = time.time()
    import aiohttp
    conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(), ttl_dns_cache=900, limit=40)
    eng.ctx.session = aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=45, connect=20))
    await eng.clock.sync()
    eng.make_connectors()
    tasks = [asyncio.create_task(c.run()) for c in eng.connectors.values()]
    tick = asyncio.create_task(_ticker(eng))
    try:
        await asyncio.sleep(seconds)
    finally:
        for t in tasks + [tick]:
            t.cancel()
        await asyncio.gather(*tasks, tick, return_exceptions=True)
        await eng.ctx.session.close()
    now = eng.clock.now()
    el = time.time() - t_start
    rows = []
    for inst in sorted(eng.instruments.values(), key=lambda i: (i.source, i.symbol)):
        lat = list(inst.lat_ms)
        rows.append({"source": inst.source, "symbol": inst.symbol, "field": inst.extra.get("field", "last"),
                     "hours": inst.hours, "market_open": clock.market_open(inst.hours, now),
                     "msgs": inst.msgs, "msgs_per_s": round(inst.msgs / el, 2),
                     "latency_ms_median": None if not lat else round(_pct(lat, 0.5), 1),
                     "latency_ms_p10": None if not lat else round(_pct(lat, 0.1), 1),
                     "latency_ms_p90": None if not lat else round(_pct(lat, 0.9), 1),
                     "latency_samples": len(lat),
                     "age_s": None if inst.ts is None else round(now - inst.ts, 2)})
    polled = {}
    if eng.vix:
        polled["vix"] = {"source": eng.vix.get("source"), "quote_time": eng.vix.get("quote_time"),
                         "age_s": None if not eng.vix.get("quote_ts") else round(now - eng.vix["quote_ts"])}
    if eng.crypto_global:
        polled["coingecko_global"] = {"age_s": round(now - eng.crypto_global["updated_at"])}
    if eng.stable_supply:
        polled["defillama_supply"] = {"observed": True}
    for sid, f in eng.fred.items():
        polled[f"fred_{sid}"] = {"latest_date": f["date"]}
    if eng.fomc:
        polled["fed_calendar"] = {"meetings": len(eng.fomc)}
    news_age = {}
    for it in eng.news.items:
        news_age.setdefault(it["source"], []).append(now - it["ts"])
    polled["news_newest_age_s"] = {k: round(min(v)) for k, v in news_age.items()}
    return {"measured_at": clock.iso(now), "seconds": round(el), "clock": eng.clock.info(),
            "market": clock.market_status(now), "instruments": rows,
            "sources": [c.health.as_dict(now) for c in eng.connectors.values()], "polled": polled}


async def _ticker(eng):
    while True:
        eng.tick()
        await asyncio.sleep(1)


def render(rep: dict) -> str:
    out = [f"Measured {rep['measured_at']} for {rep['seconds']} s; clock offset {rep['clock']['offset_ms']} ms "
           f"(NTP {rep['clock']['ntp_server']}); US equities: {rep['market']['us_equities']}, FX: {rep['market']['fx']}", "",
           f"{'source':13s} {'symbol':14s} {'msgs/s':>7s} {'median':>8s} {'p10':>8s} {'p90':>8s} {'age':>8s}  market",
           "-" * 80]
    for r in rep["instruments"]:
        f = lambda v: "-" if v is None else f"{v:.0f}ms"
        age = "-" if r["age_s"] is None else (f"{r['age_s']:.1f}s" if r["age_s"] < 600 else f"{r['age_s'] / 3600:.1f}h")
        out.append(f"{r['source']:13s} {r['symbol']:14s} {r['msgs_per_s']:7.2f} {f(r['latency_ms_median']):>8s} "
                   f"{f(r['latency_ms_p10']):>8s} {f(r['latency_ms_p90']):>8s} {age:>8s}  {'open' if r['market_open'] else 'closed'}")
    out += ["", "Sources:"]
    for s in rep["sources"]:
        out.append(f"  {s['id']:14s} {s['state']:12s} msgs={s['msgs']:<7} poll={s.get('last_poll_ms')}ms "
                   f"errors={s['errors']} {s['last_error'] or ''}"[:150])
    out += ["", "Polled data: " + json.dumps(rep["polled"])]
    return "\n".join(out)


async def main(cfg: dict, keys, seconds: float, out: str | None) -> int:
    rep = await run_check(cfg, keys, seconds)
    print(render(rep))
    path = Path(out) if out else cfgmod.data_dir(cfg) / f"latency_{dt.datetime.now(dt.timezone.utc):%Y%m%d_%H%M}.json"
    path.write_text(json.dumps(rep, indent=1), encoding="utf-8")
    print(f"\nreport: {path}")
    return 0
