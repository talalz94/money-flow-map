import datetime as dt
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from moneyflow import config as cfgmod  # noqa: E402
from moneyflow.clock import NY  # noqa: E402
from moneyflow.keys import KeyStore  # noqa: E402

DATA = Path(__file__).resolve().parent / "data"


def ny_ts(y, m, d, hh=0, mm=0) -> float:
    return dt.datetime(y, m, d, hh, mm, tzinfo=NY).timestamp()


@pytest.fixture
def cfg(tmp_path):
    """Defaults only (no user files), data in a temp folder."""
    c = cfgmod.load(paths=[], overrides={"general": {"data_dir": str(tmp_path / "data")}})
    return c


def filled_engine_at(cfg, keys, now):
    """An engine whose every instrument has 35 days of synthetic 5-minute history up to `now` and a
    live price just received (as if every stream were connected)."""
    import math
    from moneyflow import clock
    from moneyflow.engine import Engine
    from moneyflow.series import G5_STEP
    eng = Engine(cfg, keys, clock.Clock(fixed=now))
    k_now = int(now // G5_STEP)
    for n, inst in enumerate(eng.instruments.values()):
        drift = (-1) ** n * 0.0002 * (1 + n % 3)
        closes = [(k * G5_STEP, 50.0 * math.exp(drift * (k - k_now) + 0.002 * math.sin(k / 7.0 + n)))
                  for k in range(k_now - 35 * 288, k_now)]
        inst.seed("g5", closes, now, [(t, 1e6, 5e5) for t, _ in closes] if inst.v5 is not None else None)
        inst.price, inst.ts, inst.recv_ts = closes[-1][1], now - 0.5, now - 0.4
        inst._live_seen = True
    eng.seed_state["state"] = "done"
    eng.on_crypto_global(2.08e12, {"btc": 57.9, "eth": 11.0, "bnb": 3.5, "sol": 3.0, "xrp": 5.0}, now - 60)
    eng.on_stable_supply({"total": 2.85e11, "d1": 9.7e8, "w1": -2.6e9, "m1": -1.7e10, "observed_at": now - 120},
                         [(now - 86400 * d, 2.85e11 - d * 1e8) for d in range(40, 0, -1)])
    for d in eng.derived:
        d.rebuild(now)
    eng.tick(now)                       # anchors the stablecoin share, refreshes derived series
    return eng


@pytest.fixture
def keys():
    k = KeyStore()
    k.set("ALPACA_KEY_ID", "TESTKEYID12345")
    k.set("ALPACA_SECRET", "TESTSECRET67890")
    k.set("FINNHUB_KEY", "finnhubtesttoken")
    return k
