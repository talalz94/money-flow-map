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


@pytest.fixture
def keys():
    k = KeyStore()
    k.set("ALPACA_KEY_ID", "TESTKEYID12345")
    k.set("ALPACA_SECRET", "TESTSECRET67890")
    k.set("FINNHUB_KEY", "finnhubtesttoken")
    return k
