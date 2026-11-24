"""On-disk cache of each instrument's grids, so a restart only downloads the missing tail
(this keeps start-up fast and far below Binance's REST weight limits). Plain JSON, git-ignored."""
from __future__ import annotations

import json
import math
import re
from pathlib import Path

from .series import Instrument, NaN

VERSION = 1


def _file(dirpath: Path, inst: Instrument) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", inst.key)
    return dirpath / f"{safe}.json"


def _pack(vals) -> list:
    return [None if (v != v) else round(v, 10) for v in vals]


def save(dirpath: Path, inst: Instrument) -> bool:
    if inst.g5.last_k is None:
        return False
    dirpath.mkdir(parents=True, exist_ok=True)
    out = {"v": VERSION, "key": inst.key}
    for name in ("g1", "g5"):
        g = getattr(inst, name)
        if g.last_k is None:
            continue
        k0 = g.last_k - g.size + 1
        vals = [g.get(k) for k in range(k0, g.last_k + 1)]
        live = [1 if g.is_live(k) else 0 for k in range(k0, g.last_k + 1)]
        # trim leading unknowns
        i = next((n for n, v in enumerate(vals) if v == v), len(vals))
        out[name] = {"k0": k0 + i, "c": _pack(vals[i:]), "l": live[i:]}
    if inst.v5 is not None and inst.v5.last_k is not None:
        v = inst.v5
        k0 = v.last_k - v.size + 1
        qs, tbs = [], []
        for k in range(k0, v.last_k + 1):
            q, tb = v.get(k)
            qs.append(q)
            tbs.append(tb)
        i = next((n for n, x in enumerate(qs) if x == x), len(qs))
        out["v5"] = {"k0": k0 + i, "q": _pack(qs[i:]), "tb": _pack(tbs[i:])}
    tmp = _file(dirpath, inst).with_suffix(".tmp")
    tmp.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    tmp.replace(_file(dirpath, inst))
    return True


def load(dirpath: Path, inst: Instrument, now: float) -> int | None:
    """Load cached grids; returns the newest cached 5-minute step (or None)."""
    p = _file(dirpath, inst)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if d.get("v") != VERSION or d.get("key") != inst.key:
        return None
    for name in ("g1", "g5"):
        part = d.get(name)
        if not part:
            continue
        g = getattr(inst, name)
        k_now = int(now // g.step)
        for n, (v, lv) in enumerate(zip(part["c"], part["l"])):
            k = part["k0"] + n
            if k >= k_now or k <= k_now - g.size:
                continue
            g.put(k, NaN if v is None else float(v), bool(lv))
    if inst.v5 is not None and d.get("v5"):
        part = d["v5"]
        k_now = int(now // inst.v5.step)
        for n, (q, tb) in enumerate(zip(part["q"], part["tb"])):
            k = part["k0"] + n
            if k >= k_now or k <= k_now - inst.v5.size:
                continue
            inst.v5.put(k, NaN if q is None else q, NaN if tb is None else tb)
    if inst.g5.last_k is not None:
        last = inst.g5.get(inst.g5.last_k)
        if last == last and not (inst.price == inst.price):
            inst.price = last
            inst.ts = (inst.g5.last_k + 1) * inst.g5.step
    return inst.g5.last_k


def last_good_ts(inst: Instrument) -> float | None:
    """End time of the newest step with a value, in the 5-minute grid."""
    g = inst.g5
    if g.last_k is None:
        return None
    for k in range(g.last_k, g.last_k - 50, -1):
        v = g.get(k)
        if v == v and not math.isinf(v):
            return (k + 1) * g.step
    return None
