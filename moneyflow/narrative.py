"""Rule-based "what the flow says". Every bullet names the rule that produced it, so the text can be
traced to numbers; nothing here is a forecast. Thresholds are in σ units (z-scores)."""
from __future__ import annotations

import math

TF_PHRASE = {"1h": "in the last hour", "today": "today", "1w": "this week"}
Z1, Z2 = 1.0, 2.0


def _ok(x) -> bool:
    return x is not None and x == x and not math.isinf(x)


def fmt_z(z: float) -> str:
    return f"{z:+.1f}σ"


def fmt_pct(p: float) -> str:
    return f"{p:+.2f}%"


def fmt_usd(v: float) -> str:
    a = abs(v)
    sign = "+" if v >= 0 else "−"
    if a >= 1e9:
        return f"{sign}${a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.0f}M"
    return f"{sign}${a / 1e3:.0f}K"


def build(tf: str, nodes: dict[str, dict], edges: list[dict], risk: dict, ctx: dict | None = None,
          max_items: int = 8) -> list[dict]:
    """`nodes`: id -> {label, z, change_pct, status, ...} for this timeframe (live or not).
    `ctx`: optional extras -- stable_supply {d1, w1}, btc_deriv {funding_apr_pct, oi_change_pct},
    etf {btc: {...}}, fed {days, label, when_ny}, events [...], market {...}."""
    ctx = ctx or {}
    phrase = TF_PHRASE.get(tf, tf)
    out: list[dict] = []

    def add(rule: str, text: str, level: str = "info"):
        out.append({"id": rule, "rule": rule, "text": text, "level": level})

    live = {k: v for k, v in nodes.items() if _ok(v.get("z")) and v.get("status") not in ("no_data", "warming_up", "stale")}
    z = {k: v["z"] for k, v in live.items()}
    zn = {k: v for k, v in z.items() if nodes[k].get("node", True)}     # map nodes only

    def name(i):
        return nodes[i].get("label", i)

    # 1. strongest flow, biggest inflow / outflow
    if edges:
        e = edges[0]
        add("strongest_flow", f"Strongest flow: {name(e['from'])} → {name(e['to'])} "
                              f"({fmt_z(e['from_z'])} into {fmt_z(e['to_z'])}).")
    if zn:
        hi = max(zn, key=zn.get)
        lo = min(zn, key=zn.get)
        if zn[hi] >= 0.5:
            add("biggest_inflow", f"Biggest inflow: {name(hi)} ({fmt_z(zn[hi])}, {fmt_pct(live[hi]['change_pct'])} {phrase}).",
                "up")
        if zn[lo] <= -0.5:
            add("biggest_outflow", f"Biggest outflow: {name(lo)} ({fmt_z(zn[lo])}, {fmt_pct(live[lo]['change_pct'])} {phrase}).",
                "down")
        if not out or (abs(zn[hi]) < 0.5 and abs(zn[lo]) < 0.5):
            add("quiet", f"No asset on the map has moved more than half its usual range {phrase}: no clear flow.")

    g = z.get
    spx, bonds, usd, gold = g("spx"), g("bonds"), g("usd"), g("gold")
    btc, eth, stables, oil, ndx, vixp = g("btc"), g("eth"), g("stables"), g("oil"), g("ndx"), g("vix_proxy")
    crypto = None
    if _ok(btc) and _ok(eth):
        crypto = (btc + eth) / 2

    # 2. cross-asset regime rules
    if _ok(bonds) and _ok(spx):
        if bonds >= Z1 and spx <= -Z1:
            add("bonds_bid_safety", "Bonds are bid (yields falling) while stocks fall: a classic safety trade.", "down")
        elif bonds <= -Z1 and spx <= -Z1:
            add("stocks_and_bonds_sold", "Stocks and bonds are both being sold: rate or inflation pressure; "
                                         "Treasuries are not working as a haven.", "down")
        elif bonds <= -Z1 and spx >= Z1:
            add("bonds_sold_risk_on", "Bonds are sold (yields rising) while stocks rally: money leaving safety for risk.", "up")
        elif bonds >= Z1 and spx >= Z1:
            add("everything_bid", "Stocks and bonds are both bid: falling-yield relief rally.", "up")
    if _ok(usd) and _ok(spx):
        if usd >= Z1 and spx <= -Z1:
            add("dollar_haven", "The dollar is bid as stocks fall: global de-risking / dollar demand.", "down")
        elif usd <= -Z1 and spx >= Z1:
            add("dollar_weak_risk_on", "The dollar weakens while stocks rise: a classic risk-on mix.", "up")
    if _ok(gold):
        if _ok(spx) and gold >= Z1 and spx <= -Z1:
            add("gold_haven", "Gold is bid as a haven while stocks fall.", "info")
        elif gold <= -1.5 and _ok(spx) and spx <= -1.5 and _ok(crypto) and crypto <= -1.5:
            add("broad_liquidation", "Gold is sold together with stocks and crypto: broad liquidation "
                                     "(selling what can be sold, often to raise cash or meet margin).", "down")
    if _ok(vixp) and vixp >= Z2:
        add("fear_spike", f"Fear is spiking: the VIX proxy is {fmt_z(vixp)} {phrase}.", "down")
    elif _ok(vixp) and vixp <= -Z2:
        add("fear_fading", f"Fear is fading fast: the VIX proxy is {fmt_z(vixp)} {phrase}.", "up")

    # 3. crypto-specific
    sup = ctx.get("stable_supply") or {}
    if _ok(stables) and _ok(crypto) and stables >= 1.5 and crypto <= -1.5:
        txt = "Stablecoin share is rising: crypto traders are moving to cash."
        d = sup.get("d1") if tf != "1w" else sup.get("w1")
        if _ok(d):
            span = "7 days" if tf == "1w" else "24 h"
            if d > 0:
                txt += f" Supply is also up {fmt_usd(d)} in {span}, so fresh cash is parked, not leaving."
            else:
                txt += f" Supply is {fmt_usd(d)} in {span}: the rise is falling prices, and some cash is leaving crypto."
        add("crypto_to_cash", txt, "down")
    elif _ok(stables) and _ok(crypto) and stables <= -1.5 and crypto >= 1.5:
        add("cash_to_crypto", "Stablecoin share is falling: cash on the sidelines is being deployed into crypto.", "up")
    if _ok(btc) and _ok(eth):
        d = eth - btc
        if d <= -1.5:
            add("eth_lags_btc", "Ether is weaker than Bitcoin: risk appetite inside crypto is shrinking.", "down")
        elif d >= 1.5:
            add("eth_leads_btc", "Ether is stronger than Bitcoin: risk appetite inside crypto is growing.", "up")
    der = ctx.get("btc_deriv") or {}
    oi = der.get("oi_change_pct")
    if _ok(oi) and _ok(btc):
        if btc <= -Z1 and oi <= -2:
            add("btc_deleveraging", f"Bitcoin futures open interest is {fmt_pct(oi)} as price falls: longs are being "
                                    "closed or liquidated (deleveraging).", "down")
        elif btc <= -Z1 and oi >= 2:
            add("btc_new_shorts", f"Open interest is {fmt_pct(oi)} while Bitcoin falls: new shorts are being added.", "down")
        elif btc >= Z1 and oi >= 2:
            add("btc_new_longs", f"Open interest is {fmt_pct(oi)} with Bitcoin up: leveraged buyers are adding.", "up")
        elif btc >= Z1 and oi <= -2:
            add("btc_short_squeeze", f"Open interest is {fmt_pct(oi)} with Bitcoin up: shorts are covering.", "up")
    fund = der.get("funding_apr_pct")
    if _ok(fund) and fund <= -5:
        add("negative_funding", f"Bitcoin perpetual funding is negative ({fund:+.1f}% a year): shorts are paying longs.", "down")
    etf = (ctx.get("etf") or {}).get("btc")
    if etf and _ok(etf.get("last_total_musd")):
        v = etf["last_total_musd"]
        streak = etf.get("streak", 0)
        word = "outflows" if v < 0 else "inflows"
        s = f" ({abs(streak)} days in a row)" if abs(streak) >= 2 else ""
        add("btc_etf_flows", f"US spot Bitcoin ETFs: {fmt_usd(v * 1e6)} on {etf.get('last_date')}, {word}{s}.",
            "down" if v < 0 else "up")
    for k, v in live.items():                      # the notable-stock slot, if a stock stands out
        sl = v.get("slot")
        if not sl or sl.get("index") != 1 or not nodes[k].get("node", True):
            continue
        rv = (sl.get("screen") or {}).get("rvol")
        vol = f" on {rv:.1f}× its usual volume" if rv is not None and rv >= 1.05 else ""
        add("notable_stock", f"Notable stock: {sl['symbol']} is {fmt_z(v['z'])} {phrase} "
                             f"({fmt_pct(v['change_pct'])}){vol}, the most unusual move among "
                             f"{sl.get('universe') or 'the'} large caps.", "down" if v["z"] < 0 else "up")
    if _ok(oil) and abs(oil) >= Z2:
        add("oil_move", f"Crude oil is {fmt_z(oil)} {phrase}: "
                        f"{'an inflation / supply risk' if oil > 0 else 'a demand-scare or supply-glut signal'}.", "info")
    if _ok(ndx) and _ok(spx) and ndx - spx <= -1.0 and spx < 0:
        add("tech_lags", "Tech is weaker than the broad market: money is rotating out of growth.", "down")

    # 4. risk summary, the Fed, scheduled data, data caveats
    if risk.get("score") is not None:
        add("risk_regime", f"Risk meter: {risk['label']} {risk['score']:+d} "
                           f"({int(round(risk.get('coverage', 0) * 100))}% of inputs live).",
            "down" if risk["score"] < -20 else ("up" if risk["score"] > 20 else "info"))
    fed = ctx.get("fed") or {}
    if fed.get("days") is not None:
        d = fed["days"]
        when = fed.get("when_ny", "")
        if fed.get("when_local") and fed.get("local_label"):
            when += f" = {fed['when_local']} {fed['local_label']}"
        if d <= 2:
            add("fed_close", f"FOMC decision in {fed.get('countdown', f'{d:.1f} days')} ({when}): rate decisions can move "
                             "every asset on this map within minutes.", "warn")
        else:
            add("fed_next", f"Next FOMC decision in {d:.1f} days ({when}).", "info")
    for ev in [e for e in (ctx.get("events") or []) if e.get("country", "USD") == "USD"][:2]:
        add("macro_event", f"Scheduled: US {ev['title']}, {ev.get('when_text', '')}.", "info")
    for c in ctx.get("caveats") or []:
        add("caveat", c, "muted")
    return out[:max_items] + [b for b in out[max_items:] if b["id"] == "caveat"][:2]
