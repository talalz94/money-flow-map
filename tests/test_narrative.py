from moneyflow import flow, narrative


def node(label, z, pct, node=True, status="live"):
    return {"label": label, "z": z, "change_pct": pct, "status": status, "node": node}


def example_nodes():
    # the reference screenshot's read
    return {
        "bonds": node("US Bonds (TLT)", 1.9, 1.28), "usd": node("US Dollar", -0.2, -0.07),
        "silver": node("Silver", -1.6, -4.02), "oil": node("Crude Oil", 0.3, 0.95),
        "ndx": node("Nasdaq 100", -2.5, -3.79), "spx": node("S&P 500", -2.4, -2.30),
        "btc": node("Bitcoin", -3.1, -6.37), "eth": node("Ethereum", -3.6, -9.45),
        "stables": node("Stablecoins (cash in crypto)", 5.4, 6.45), "gold": node("Gold", -1.4, -2.97),
        "vix_proxy": node("VIX proxy", 2.6, 11.7, node=False),
    }


def test_example_read_produces_the_expected_bullets():
    nodes = example_nodes()
    edges = [e.as_dict() for e in flow.infer_edges({k: v["z"] for k, v in nodes.items() if v["node"]},
                                                     lambda a, b: -0.9)]
    risk = flow.risk_composite({k: v["z"] for k, v in nodes.items()})
    out = narrative.build("today", nodes, edges, risk, {"stable_supply": {"d1": 9.7e8, "w1": -2.6e9}})
    ids = [b["id"] for b in out]
    text = " ".join(b["text"] for b in out)
    assert ids[0] == "strongest_flow" and "Ethereum → Stablecoins" in out[0]["text"]
    assert "Biggest inflow: Stablecoins (cash in crypto) (+5.4σ, +6.45% today)" in text
    assert "Biggest outflow: Ethereum (−3.6σ" in text or "Biggest outflow: Ethereum (-3.6σ" in text
    assert "bonds_bid_safety" in ids and "crypto_to_cash" in ids and "fear_spike" in ids
    assert "fresh cash is parked" in text
    assert risk["label"] == "RISK-OFF"


def test_aux_series_never_named_biggest_inflow():
    nodes = {"spx": node("S&P 500", -1.0, -0.5), "vix_proxy": node("VIX proxy", 3.0, 9.0, node=False),
             "bonds": node("Bonds", 0.8, 0.2)}
    out = narrative.build("1h", nodes, [], {"score": None})
    inflow = [b for b in out if b["id"] == "biggest_inflow"]
    assert inflow and "Bonds" in inflow[0]["text"]


def test_quiet_market_and_stale_nodes():
    nodes = {"spx": node("S&P 500", 0.2, 0.05), "btc": node("Bitcoin", -0.1, -0.02),
             "eth": node("Ethereum", -4.0, -5.0, status="stale")}
    out = narrative.build("1h", nodes, [], {"score": None})
    assert any(b["id"] == "quiet" for b in out)
    assert not any("Ethereum" in b["text"] for b in out)            # stale data is not narrated


def test_fed_and_caveats():
    out = narrative.build("today", {}, [], {"score": None},
                          {"fed": {"days": 1.5, "countdown": "1d 12h 00m", "when_ny": "Wed, Dec 9, 2:00 PM New York"},
                           "caveats": ["US stock market closed."]})
    assert out[0]["id"] == "fed_close" and out[0]["level"] == "warn"
    assert out[-1]["id"] == "caveat"
