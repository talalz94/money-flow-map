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
  The strongest edges are kept (at most `max_edges`, `per_node` per asset).

Risk composite: a weighted average of documented components, each in σ units and clipped to ±4,
mapped to −100…+100 with 100·tanh(S / scale). Missing inputs drop out and the weights are
renormalised over what is available; `coverage` reports the share of weight that was live.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

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
                per_node: int = 5, min_weight: float = 0.3) -> list[Edge]:
    """`z`: node id -> z-score (live nodes only). `corr_fn(a, b)` -> correlation or None."""
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
    for e in cands:
        if len(kept) >= max_edges:
            break
        if count.get(e.src, 0) >= per_node or count.get(e.dst, 0) >= per_node:
            continue
        kept.append(e)
        count[e.src] = count.get(e.src, 0) + 1
        count[e.dst] = count.get(e.dst, 0) + 1
    tot = sum(e.weight for e in kept)
    for e in kept:
        e.share = e.weight / tot if tot > 0 else 0.0
    return kept


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
