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


def test_caps_limit_the_extra_edges_never_an_assets_only_one():
    z = {"a": -3.0, "b": -2.0, "x": 3.0, "y": 2.0}
    pairs = lambda **kw: {(e.src, e.dst) for e in flow.infer_edges(z, lambda a, b: None, min_weight=0.0, **kw)}  # noqa: E731
    # every asset's strongest edge: a->x (1.5), b->x and a->y (1.22 each); b->y (1.0) is an extra
    assert pairs(per_node=5) == {("a", "x"), ("b", "x"), ("a", "y"), ("b", "y")}
    assert pairs(per_node=1) == {("a", "x"), ("b", "x"), ("a", "y")}    # cap: no extras, nobody dropped
    assert pairs(max_edges=1) == {("a", "x"), ("b", "x"), ("a", "y")}
    many = {f"o{i}": -2.0 - i / 10 for i in range(6)} | {f"i{j}": 2.0 + j / 10 for j in range(6)}
    edges = flow.infer_edges(many, lambda a, b: -0.5, max_edges=8, per_node=2, min_weight=0.0)
    assert {n for e in edges for n in (e.src, e.dst)} == set(many)      # all 12 assets keep an arrow


def test_an_asset_sold_into_a_crowded_haven_keeps_its_arrow():
    """Gold on the map: five stronger sellers already fill the haven's 5 arrows. Gold used to lose its
    only arrow ("crowded out"); every clearly sold asset now keeps its strongest one."""
    z = {"spx": -3.0, "ndx": -3.0, "eth": -1.6, "btc": -1.5, "silver": -0.9, "gold": -0.7,
         "bonds": 4.0, "stables": 1.85, "usd": 0.76}
    edges = flow.infer_edges(z, lambda a, b: None, per_node=5, max_edges=16, min_weight=0.3)
    gold = [e for e in edges if "gold" in (e.src, e.dst)]
    assert gold and gold[0].dst == "bonds" and gold[0].weight == pytest.approx(math.sqrt(0.7 * 4.0) * 0.5)


def test_no_edge_reasons_are_given_in_plain_words():
    why = {}
    z = {"a": -2.0, "b": 2.0, "flat": 0.2, "seller": -1.0, "twin": -0.6}
    corr = {("seller", "b"): 0.9, ("twin", "b"): None, ("a", "b"): -0.5}
    flow.infer_edges(z, lambda a, b: corr.get((a, b)), min_weight=0.6, why=why)
    assert why["flat"]["code"] == "neutral"
    assert why["seller"]["code"] == "co_moved" and why["seller"]["partner"] == "b"   # 0.07 with ρ 0.9, 0.71 without
    assert why["twin"]["code"] == "too_weak"                            # sqrt(0.6*2)*0.5 = 0.55 < 0.6
    assert "a" not in why and "b" not in why
    names = {"b": "Bonds"}
    assert "inside ±0.5σ" in flow.no_edge_text(why["flat"], names)
    assert "Bonds, moved in step with it (ρ +0.90)" in flow.no_edge_text(why["seller"], names)
    assert "under the 0.6 minimum" in flow.no_edge_text(why["twin"], names)
    lone = {}
    flow.infer_edges({"g": -1.2, "h": 0.1}, lambda a, b: None, why=lone)
    assert lone["g"]["code"] == "no_counterpart"
    assert "nothing on the map was bought" in flow.no_edge_text(lone["g"], {})


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
