"""The map on a US holiday and at a weekend: what is closed, what stays live, what the read says."""
from conftest import filled_engine_at, ny_ts
from moneyflow.server import one_tf


def statuses(snap, tf="today"):
    t = one_tf(snap, tf)
    return t, {n["id"]: n for n in t["nodes"]}, {n["id"]: n for n in t["aux"]}


def test_thanksgiving_us_closed_fx_and_crypto_open(cfg, keys):
    now = ny_ts(2026, 11, 26, 11, 0)                      # Thanksgiving: NYSE shut, FX and crypto trade
    eng = filled_engine_at(cfg, keys, now)
    snap = eng.snapshot()
    assert snap["market"]["us_equities"] == "closed" and snap["market"]["fx"] == "open"
    t, nodes, aux = statuses(snap)
    assert aux["hyg"]["status"] == "closed" and aux["ief"]["status"] == "closed"
    assert aux["hyg"]["z"] is None                        # no session since yesterday's 17:00 roll
    assert nodes["bonds"]["source"]["proxy"] and "market closed" in nodes["bonds"]["source"]["description"]
    for nid in ("ndx", "spx", "ant", "gold", "btc", "eth", "usd"):
        assert nodes[nid]["status"] == "live", nid        # 24/7 perpetuals, crypto, FX keep running
    credit = next(c for c in t["risk"]["components"] if c["id"] == "credit")
    assert credit["available"] is False and t["risk"]["coverage"] == 0.9
    cav = " ".join(b["text"] for b in t["narrative"] if b["id"] == "caveat")
    assert "US stock market closed" in cav and "Binance's 24/7 perpetuals" in cav
    assert "until US trading resumes" in cav


def test_saturday_fx_closed(cfg, keys):
    now = ny_ts(2026, 11, 28, 12, 0)
    eng = filled_engine_at(cfg, keys, now)
    snap = eng.snapshot()
    assert snap["market"]["fx"] == "closed" and snap["market"]["us_equities"] == "closed"
    t, nodes, _ = statuses(snap)
    assert nodes["usd"]["status"] == "closed" and nodes["usd"]["z"] is None      # FX shut all window
    assert nodes["btc"]["status"] == "live"
    cav = " ".join(b["text"] for b in t["narrative"] if b["id"] == "caveat")
    assert "FX market closed" in cav
    t1w, n1w, _ = statuses(snap, "1w")
    assert n1w["usd"]["z"] is not None                    # the week did trade


def test_early_close_day_after_thanksgiving(cfg, keys):
    after = ny_ts(2026, 11, 27, 13, 30)                   # NYSE closed at 13:00 on 27 Nov 2026
    eng = filled_engine_at(cfg, keys, after)
    t, nodes, aux = statuses(eng.snapshot())
    assert eng.snapshot()["market"]["us_equities"] == "post"
    assert aux["ief"]["status"] == "closed" and nodes["bonds"]["source"]["proxy"]
