"""Every node on the map, gold and the notable-stock slot included, can be the strongest outflow AND
the strongest inflow, in every window: nothing in the data path, the statuses, the filters or the
edge selection shuts a particular node out (the question behind "gold never gets an arrow")."""
import math

import pytest

from moneyflow import stats
from test_stocks import NOW, engine_with_pick


@pytest.fixture
def eng(cfg, keys):
    return engine_with_pick(cfg, keys)


def force(eng, monkeypatch, tf, z_of: dict):
    """Window returns of r = z·σ (each node's own σ) for the given nodes, 0 for all the others;
    correlations unknown, so an edge's weight is exactly sqrt(|z_a|·z_b)·0.5."""
    real = stats.window_return
    owner = {id(s.inst): sid for sid, s in eng.series.items()}
    owner.update({id(ent["inst"]): sd.id for sd, _, ent in eng.slot_series(tf)})

    def window_return(inst, w, now):
        sid = owner.get(id(inst))
        if sid is None:
            return real(inst, w, now)
        r = z_of.get(sid, 0.0) * stats.sigma_for(w.tf, inst, now)[0]
        return r, (math.exp(r) - 1) * 100, inst.price / math.exp(r)

    monkeypatch.setattr(stats, "window_return", window_return)
    monkeypatch.setattr(stats, "corr", lambda a, b, min_n=8: None)
    eng._cache.clear()


@pytest.mark.parametrize("tf", ["1h", "today", "1w", "1m"])
def test_every_node_can_be_the_strongest_outflow_and_inflow(eng, monkeypatch, tf):
    ids = [n["id"] for n in eng.build_tf(tf, NOW)["nodes"]]
    assert "gold" in ids and "stock1" in ids and len(ids) == 11
    for nid in ids:
        partner = "bonds" if nid != "bonds" else "gold"
        for z_node, expect in ((-6.0, (nid, partner)), (6.0, (partner, nid))):
            force(eng, monkeypatch, tf, {nid: z_node, partner: -z_node})
            td = eng.build_tf(tf, NOW)
            node = next(n for n in td["nodes"] if n["id"] == nid)
            assert node["z"] == pytest.approx(z_node, abs=1e-6) and node["status"] == "live", (tf, nid)
            assert td["edges"] and (td["edges"][0]["from"], td["edges"][0]["to"]) == expect, (tf, nid, td["edges"])
            assert td["edges"][0]["weight"] == pytest.approx(3.0)        # sqrt(6·6)·0.5
            assert node["no_edge"] is None
            others = [n for n in td["nodes"] if n["id"] not in (nid, partner)]
            assert all(n["no_edge"]["code"] == "neutral" for n in others), (tf, nid)
