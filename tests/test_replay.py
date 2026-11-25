"""Offline replay of real recorded websocket frames (tests/data/replay_sample.jsonl.gz, recorded live
on 2026-11-24 around the US open by tools/record.py) through the real connector parsers, the
engine and the snapshot builder. No network: the connectors are created but never started."""
import gzip
import json
import math
from collections import defaultdict
from pathlib import Path

import jsonschema
import pytest

from conftest import DATA
from moneyflow import clock
from moneyflow.engine import Engine
from moneyflow.series import DXY
from moneyflow.server import one_tf

SAMPLE = DATA / "replay_sample.jsonl.gz"
if not SAMPLE.exists():
    pytest.skip("tests/data/replay_sample.jsonl.gz is local only (raw provider data is not published); "
                "record one with tools/record.py", allow_module_level=True)
SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "moneyflow" / "schema" / "snapshot.schema.json").read_text())


def frames():
    with gzip.open(SAMPLE, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def expected_last_prices():
    """Re-derive, independently of the connectors, the last price per (source, symbol)."""
    out = {}
    for fr in frames():
        c, d = fr["c"], json.loads(fr["d"])
        if c == "binance_spot":
            x = d.get("data", {})
            if x.get("e") == "aggTrade":
                out[("binance_spot", x["s"])] = float(x["p"])
        elif c == "binance_fut":
            x = d.get("data", {})
            if x.get("e") == "markPriceUpdate" and x.get("i"):
                out[("binance_fut", x["s"], "index")] = float(x["i"])
        elif c == "finnhub" and d.get("type") == "trade":
            for t in d["data"]:
                out[("finnhub", t["s"])] = float(t["p"])
    return out


@pytest.mark.skipif(not SAMPLE.exists(), reason="replay sample not recorded")
def test_replay_through_connectors_engine_and_snapshot(cfg, keys):
    first = next(frames())["t"]
    eng = Engine(cfg, keys, clock.Clock(fixed=first))
    eng.make_connectors()
    counts = defaultdict(int)
    last_tick = first
    for fr in frames():
        conn = eng.connectors.get(fr["c"])
        if conn is None:
            continue
        eng.clock.set_fixed(fr["t"])
        conn.on_text(fr["d"], fr["t"])
        counts[fr["c"]] += 1
        if fr["t"] - last_tick >= 1.0:
            eng.tick(fr["t"])
            last_tick = fr["t"]
    assert counts["binance_spot"] > 100 and counts["binance_fut"] > 100 and counts["finnhub"] > 20

    # 1. every price the engine holds equals the last one in the recording
    exp = expected_last_prices()
    for key, px in exp.items():
        if key[0] == "binance_fut":
            inst = eng.instruments.get(f"binance_fut:{key[1]}")
            if inst is not None and inst.extra.get("field") == "index":
                assert inst.price == pytest.approx(px), key
        else:
            inst = eng.instruments.get(f"{key[0]}:{key[1]}")
            if inst is not None:
                assert inst.price == pytest.approx(px), key

    # 2. the DXY is the ICE formula on the last six FX prices
    dxy = next(d for d in eng.derived if isinstance(d, DXY))
    px = {n: i.price for n, i in dxy.pairs.items()}
    assert all(v == v for v in px.values())
    assert dxy.price == pytest.approx(DXY.formula(px)) and 90 < dxy.price < 120

    # 3. latency is measured from exchange timestamps and is plausible (recorded with NTP correction)
    for key in ("binance_spot:BTCUSDT", "binance_fut:QQQUSDT", "finnhub:OANDA:EUR_USD"):
        lat = eng.instruments[key].latency_ms()
        assert lat is not None and 0 < lat < 3000, (key, lat)

    # 4. the snapshot builds for every timeframe and matches the published schema
    snap = eng.snapshot()
    for tf in ("1h", "today", "1w"):
        one = one_tf(snap, tf)
        jsonschema.validate(one, SCHEMA)
        st = {n["id"]: n["status"] for n in one["nodes"]}
        assert st["btc"] in ("live", "warming_up") and st["spx"] in ("live", "warming_up")
    btc = next(n for n in one_tf(snap, "today")["nodes"] if n["id"] == "btc")
    assert btc["age_s"] is not None and btc["age_s"] < 60
    assert btc["source"]["symbol"] == "BTCUSDT" and btc["source"]["name"] == "Binance spot"


@pytest.mark.skipif(not SAMPLE.exists(), reason="replay sample not recorded")
def test_replay_volume_and_taker_split_from_klines(cfg, keys):
    """Binance 1-minute klines carry quote volume and taker-buy quote volume: both reach the grid."""
    first = next(frames())["t"]
    eng = Engine(cfg, keys, clock.Clock(fixed=first))
    eng.make_connectors()
    last = first
    for fr in frames():
        conn = eng.connectors.get(fr["c"])
        if conn is not None:
            conn.on_text(fr["d"], fr["t"])
            last = fr["t"]
            eng.tick(fr["t"]) if int(fr["t"]) % 5 == 0 else None
    eng.tick(last + 600)                                  # close the open 5-minute steps
    inst = eng.instruments["binance_spot:BTCUSDT"]
    vols = [inst.v5.get(k) for k in range(inst.v5.last_k - 5, inst.v5.last_k + 1)]
    seen = [(q, tb) for q, tb in vols if q == q and q > 0]
    assert seen, "no volume recorded"
    for q, tb in seen:
        assert 0 <= tb <= q * 1.0001
