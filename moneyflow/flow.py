"""The money-flow model: flow edges and the risk-on / risk-off composite. A heuristic read.

Per asset the "flow score" is its z-score: the window's log return divided by the asset's own
typical move for that window (see stats.py). Prices are the fastest public evidence of net order
flow (price impact rises with net buying or selling), and dividing by volatility makes a bond, a
stock index and a coin comparable. Where the venue reports it, volume confirms the read:
relative volume (participation) and net taker flow (aggressive buyers minus sellers, USD).

Edges ("money moving from A to B") are INFERRED, never observed:
  for every pair with A clearly offered (z_A <= -θ) and B clearly bid (z_B >= +θ) in the same window
      weight(A→B) = sqrt(|z_A| · z_B) · (1 − ρ_AB) / 2
  ρ_AB is the correlation of their step returns inside the window. Opposite co-movement (ρ → −1)
  is what a rotation looks like (one sold while the other is bought, step by step), so it keeps
  the full weight; assets that merely moved together (ρ → +1) get no edge. Unknown ρ counts 0.5.
  Every asset with a qualifying edge (weight ≥ `min_weight`) keeps its strongest one; then the
  strongest of the rest are added, up to `max_edges` in all and `per_node` per asset.

Risk composite: a weighted average of documented components, each in σ units and clipped to ±4,
mapped to −100…+100 with 100·tanh(S / scale). Missing inputs drop out and the weights are
renormalised over what is available; `coverage` reports the share of weight that was live.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

NaN = float("nan")


def _ok(x) -> bool:
    return x is not None and x == x and not math.isinf(x)


@dataclass
class Edge:
    src: str
    dst: str
    weight: float
    corr: float | None
    src_z: float
    dst_z: float
    share: float = 0.0

    def as_dict(self) -> dict:
        return {"from": self.src, "to": self.dst, "weight": round(self.weight, 3),
                "share": round(self.share, 3), "corr": None if self.corr is None else round(self.corr, 3),
                "from_z": round(self.src_z, 2), "to_z": round(self.dst_z, 2)}


def infer_edges(z: dict[str, float], corr_fn, theta: float = 0.5, max_edges: int = 16,
                per_node: int = 5, min_weight: float = 0.3, why: dict | None = None) -> list[Edge]:
    """`z`: node id -> z-score (live nodes only). `corr_fn(a, b)` -> correlation or None.
    `why`, if given, receives for every node WITHOUT an edge the rule that left it out (see
    `no_edge_text`): an asset with no arrow is a fact of the window, never a mystery."""
    outs = [i for i, v in z.items() if _ok(v) and v <= -theta]
    ins = [j for j, v in z.items() if _ok(v) and v >= theta]
    cands = []
    for i in outs:
        for j in ins:
            rho = corr_fn(i, j)
            f = 0.5 if rho is None else (1.0 - rho) / 2.0
            w = math.sqrt(abs(z[i]) * z[j]) * f
            if w >= min_weight:
                cands.append(Edge(i, j, w, rho, z[i], z[j]))
    cands.sort(key=lambda e: -e.weight)
    count: dict[str, int] = {}
    kept = []
    # 1. every asset that has a qualifying edge keeps its strongest one, whatever the caps: otherwise
    #    an asset sold into the same haven as five stronger sellers lost its only arrow (gold, often)
    best: dict[str, Edge] = {}
    for e in cands:
        for n in (e.src, e.dst):
            best.setdefault(n, e)
    for e in sorted({id(e): e for e in best.values()}.values(), key=lambda e: -e.weight):
        kept.append(e)
        count[e.src] = count.get(e.src, 0) + 1
        count[e.dst] = count.get(e.dst, 0) + 1
    kept_ids = {id(e) for e in kept}
    # 2. then the strongest of the rest, within the caps
    for e in cands:
        if len(kept) >= max_edges:
            break
        if id(e) in kept_ids or count.get(e.src, 0) >= per_node or count.get(e.dst, 0) >= per_node:
            continue
        kept.append(e)
        count[e.src] = count.get(e.src, 0) + 1
        count[e.dst] = count.get(e.dst, 0) + 1
    tot = sum(e.weight for e in kept)
    for e in kept:
        e.share = e.weight / tot if tot > 0 else 0.0
    if why is not None:
        linked = {n for e in kept for n in (e.src, e.dst)}
        for n, v in z.items():
            if n not in linked and _ok(v):
                why[n] = _no_edge(n, v, z, outs, ins, corr_fn, theta, min_weight)
    return kept


def _no_edge(n, v, z, outs, ins, corr_fn, theta, min_weight) -> dict:
    if abs(v) < theta:
        return {"code": "neutral", "z": v, "theta": theta}
    side = "out" if v < 0 else "in"
    others = ins if side == "out" else outs
    if not others:
        return {"code": "no_counterpart", "z": v, "side": side, "theta": theta}
    # every pairing stayed under the minimum weight (step 1 keeps any that qualifies): report the
    # best one and what held it down
    best = None
    for m in others:
        a, b = (n, m) if side == "out" else (m, n)
        rho = corr_fn(a, b)
        base = math.sqrt(abs(z[a]) * z[b])
        w = base * (0.5 if rho is None else (1.0 - rho) / 2.0)
        if best is None or w > best[1]:
            best = (m, w, rho, base * 0.5)
    m, w, rho, w_neutral = best
    code = "co_moved" if rho is not None and rho > 0 and w_neutral >= min_weight else "too_weak"
    return {"code": code, "z": v, "partner": m, "weight": w, "corr": rho, "min_weight": min_weight}


def no_edge_text(r: dict, names: dict[str, str]) -> str:
    """Plain-language reason for a node that has no arrow (shown when hovering over it)."""
    nm = lambda i: names.get(i, i)  # noqa: E731
    c, z = r["code"], r.get("z")
    if c == "neutral":
        return (f"No arrow: it moved {z:+.1f}σ, inside ±{r['theta']:g}σ, too small to count as money "
                f"moving in or out.")
    if c == "no_counterpart":
        return (f"No arrow: it was {'sold' if r['side'] == 'out' else 'bought'} ({z:+.1f}σ), but nothing on "
                f"the map was {'bought' if r['side'] == 'out' else 'sold'} by {r['theta']:g}σ or more in this "
                f"window to pair it with.")
    if c == "co_moved":
        return (f"No arrow: its best match, {nm(r['partner'])}, moved in step with it (ρ {r['corr']:+.2f}). "
                f"Assets that move together are not a rotation, so the arrow would weigh only "
                f"{r['weight']:.2f} (minimum {r['min_weight']:g}).")
    return (f"No arrow: its strongest possible arrow ({'to' if z < 0 else 'from'} {nm(r['partner'])}) weighs "
            f"{r['weight']:.2f}, under the {r['min_weight']:g} minimum.")


# ── risk-on / risk-off ──────────────────────────────────────────────────────────────────────────
@dataclass
class RiskComponent:
    id: str
    kind: str                 # "spread" (a − b), "neg" (−a), "pos" (a), "mean" (mean of a list)
    inputs: list[str]
    weight: float
    label: str = ""

    def value(self, z: dict[str, float]) -> float | None:
        vals = [z.get(i) for i in self.inputs]
        if self.kind == "mean":
            vs = [v for v in vals if _ok(v)]
            return sum(vs) / len(vs) if vs else None
        if not all(_ok(v) for v in vals):
            return None
        if self.kind == "spread":
            return vals[0] - vals[1]
        if self.kind == "neg":
            return -vals[0]
        if self.kind == "pos":
            return vals[0]
        raise ValueError(f"unknown component kind {self.kind!r}")


DEFAULT_COMPONENTS = [
    RiskComponent("equities_vs_bonds", "spread", ["spx", "bonds"], 0.25, "S&P 500 vs Treasuries (TLT)"),
    RiskComponent("fear", "neg", ["vix_proxy"], 0.20, "Volatility (UVXY perp, inverted)"),
    RiskComponent("crypto", "mean", ["btc", "eth"], 0.15, "Bitcoin and Ether"),
    RiskComponent("credit", "spread", ["hyg", "ief"], 0.10, "High-yield credit vs Treasuries (HYG vs IEF)"),
    RiskComponent("dollar", "neg", ["usd"], 0.10, "US dollar (inverted)"),
    RiskComponent("stablecoin_share", "neg", ["stables"], 0.10, "Stablecoin share of crypto (inverted)"),
    RiskComponent("gold", "neg", ["gold"], 0.05, "Gold (haven demand, inverted)"),
    RiskComponent("tech_leadership", "spread", ["ndx", "spx"], 0.05, "Nasdaq 100 vs S&P 500"),
]

REGIMES = [(-100, -60, "RISK-OFF"), (-60, -20, "LEANING RISK-OFF"), (-20, 20, "MIXED"),
           (20, 60, "LEANING RISK-ON"), (60, 101, "RISK-ON")]


def regime_label(score: float) -> str:
    for lo, hi, name in REGIMES:
        if lo <= score < hi:
            return name
    return "RISK-OFF" if score < 0 else "RISK-ON"


def risk_composite(z: dict[str, float], components=None, scale: float = 1.5, clip: float = 4.0) -> dict:
    comps = components or DEFAULT_COMPONENTS
    total_w = sum(c.weight for c in comps)
    used_w = 0.0
    acc = 0.0
    rows = []
    for c in comps:
        v = c.value(z)
        avail = _ok(v)
        cv = max(-clip, min(clip, v)) if avail else None
        if avail:
            used_w += c.weight
            acc += c.weight * cv
        rows.append({"id": c.id, "label": c.label, "inputs": c.inputs, "weight": c.weight,
                     "value": None if cv is None else round(cv, 3), "available": avail})
    if used_w <= 0:
        return {"score": None, "label": "NO DATA", "coverage": 0.0, "s": None, "components": rows,
                "method": f"100·tanh(S/{scale}), S = weighted mean of components (σ units, clipped ±{clip})"}
    s = acc / used_w
    score = round(100.0 * math.tanh(s / scale))
    for r in rows:
        r["contribution"] = None if r["value"] is None else round(r["weight"] * r["value"] / used_w, 3)
    return {"score": score, "label": regime_label(score), "coverage": round(used_w / total_w, 3), "s": round(s, 3),
            "components": rows,
            "method": f"100·tanh(S/{scale}), S = weighted mean of components (σ units, clipped ±{clip})"}
