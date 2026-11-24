import math

import pytest

from moneyflow import flow


def test_edge_weight_formula_and_correlation_effect():
    z = {"eth": -3.6, "btc": -3.1, "stables": 5.4, "bonds": 1.9, "usd": -0.2}
    corr = {("eth", "stables"): -0.95, ("btc", "stables"): -0.9, ("eth", "bonds"): 0.2, ("btc", "bonds"): None}
    edges = flow.infer_edges(z, lambda a, b: corr.get((a, b)), theta=0.5, max_edges=10, per_node=5, min_weight=0.0)
    w = {(e.src, e.dst): e for e in edges}
    assert ("usd", "stables") not in w                                 # |z| below θ: not an outflow
    assert w[("eth", "stables")].weight == pytest.approx(math.sqrt(3.6 * 5.4) * (1 + 0.95) / 2)
    assert w[("btc", "bonds")].weight == pytest.approx(math.sqrt(3.1 * 1.9) * 0.5)    # unknown ρ -> 0.5
    assert edges[0].src == "eth" and edges[0].dst == "stables"          # strongest first
    assert sum(e.share for e in edges) == pytest.approx(1.0)


def test_co_moving_assets_get_no_edge():
    z = {"a": -2.0, "b": 2.0}
    assert flow.infer_edges(z, lambda a, b: 1.0, min_weight=0.01) == []


def test_edge_caps():
    z = {f"o{i}": -2.0 for i in range(6)} | {f"i{j}": 2.0 for j in range(6)}
    edges = flow.infer_edges(z, lambda a, b: -0.5, max_edges=8, per_node=2, min_weight=0.0)
    assert len(edges) == 8
    for n in z:
        assert sum(1 for e in edges if n in (e.src, e.dst)) <= 2


def test_risk_composite_known_values():
    z = {"spx": -2.0, "bonds": 2.0, "vix_proxy": 3.0, "btc": -2.0, "eth": -2.0, "hyg": -1.0, "ief": 1.0,
         "usd": 1.0, "stables": 2.0, "gold": 0.0, "ndx": -2.5}
    r = flow.risk_composite(z)
    # components: eq/bonds -4, fear -3, crypto -2, credit -2, dollar -1, stables -2, gold 0, tech -0.5
    s = (0.25 * -4 + 0.20 * -3 + 0.15 * -2 + 0.10 * -2 + 0.10 * -1 + 0.10 * -2 + 0.05 * 0 + 0.05 * -0.5)
    assert r["s"] == pytest.approx(s, abs=1e-3)
    assert r["score"] == round(100 * math.tanh(s / 1.5))
    assert r["label"] == "RISK-OFF" and r["coverage"] == 1.0


def test_risk_composite_renormalises_missing_inputs_and_clips():
    r = flow.risk_composite({"spx": 10.0, "bonds": -10.0})            # only equities_vs_bonds available
    eq = next(c for c in r["components"] if c["id"] == "equities_vs_bonds")
    assert eq["value"] == 4.0                                           # clipped
    assert r["coverage"] == pytest.approx(0.25)
    assert r["s"] == pytest.approx(4.0)
    assert r["label"] == "RISK-ON" and r["score"] == round(100 * math.tanh(4 / 1.5))
    assert flow.risk_composite({})["score"] is None


def test_regime_labels():
    assert flow.regime_label(-61) == "RISK-OFF"
    assert flow.regime_label(-30) == "LEANING RISK-OFF"
    assert flow.regime_label(0) == "MIXED"
    assert flow.regime_label(45) == "LEANING RISK-ON"
    assert flow.regime_label(100) == "RISK-ON"
