"""Engine behaviour that needs no network: statuses, staleness limits, closed markets, history retry."""
import asyncio

from conftest import ny_ts
from moneyflow import clock
from moneyflow.engine import Engine
from moneyflow.series import G5_STEP


def engine_at(cfg, keys, ts):
    return Engine(cfg, keys, clock.Clock(fixed=ts))


def test_thin_iex_etf_is_live_for_minutes_but_binance_goes_stale(cfg, keys):
    now = ny_ts(2026, 11, 24, 11, 30)                     # US regular session
    eng = engine_at(cfg, keys, now)
    ief, gold = eng.series["ief"], eng.series["gold"]
    ief.inst.on_price(95.0, now - 300, now - 300)         # last IEX trade 5 minutes ago
    gold.inst.on_price(3600.0, now - 300, now - 300)      # Binance index silent for 5 minutes
    assert eng.status_of(ief, now) == "live"
    assert eng.status_of(gold, now) == "stale"
    ief.inst.ts = now - 700
    assert eng.status_of(ief, now) == "stale"


def test_cached_history_is_never_shown_as_live(cfg, keys):
    """A price loaded from the disk cache (or REST history) is not live data: until a live message
    arrives the asset is STALE, and the bonds node does not claim real-time TLT."""
    now = ny_ts(2026, 11, 24, 11, 30)
    eng = engine_at(cfg, keys, now)
    tlt = eng.instruments["alpaca:TLT"]
    k = int(now // G5_STEP)
    tlt.seed("g5", [(t * G5_STEP, 81.0) for t in range(k - 50, k)], now)   # recent, but only history
    hyg = eng.series["hyg"]
    hyg.inst.seed("g5", [(t * G5_STEP, 80.0) for t in range(k - 50, k)], now)
    assert eng.status_of(hyg, now) == "stale"
    bonds = eng.series["bonds"]
    tmf = eng.instruments["binance_fut:TMFUSDT"]
    tmf.on_price(30.0, now - 1, now - 0.9)
    assert not bonds.inst.primary_live(now)
    info = eng.source_info(bonds, now)
    assert info["proxy"] and "no live TLT data" in info["description"]
    tlt.on_price(81.2, now - 1, now - 0.9)                                 # first live TLT trade
    assert bonds.inst.primary_live(now) and not eng.source_info(bonds, now)["proxy"]


def test_closed_markets_and_their_z_in_windows_without_trading(cfg, keys):
    sat = ny_ts(2026, 11, 28, 12, 0)
    eng = engine_at(cfg, keys, sat)
    hyg = eng.series["hyg"]
    k_now = int(sat // G5_STEP)
    hyg.inst.seed("g5", [(k * G5_STEP, 80.0 + (k % 7) * 0.01) for k in range(k_now - 35 * 288, k_now)], sat)
    assert eng.status_of(hyg, sat) == "closed"
    from moneyflow import stats
    r1h = eng.read(hyg, stats.window_for("1h", sat), sat)
    assert r1h["z"] is None and r1h["status"] == "closed"         # no trading in the last hour: no z
    r1w = eng.read(hyg, stats.window_for("1w", sat), sat)
    assert r1w["status"] == "closed" and r1w["change_pct"] is not None


def test_stablecoin_share_anchors_even_when_cached_data_arrives_before_prices(cfg, keys):
    """At start-up the cached CoinGecko / DefiLlama values arrive before any price: the share must
    still come up once the basket's history is in (it used to stay NO DATA for up to 10 minutes)."""
    now = ny_ts(2026, 11, 24, 11, 30)
    eng = engine_at(cfg, keys, now)
    eng.seed_state["state"] = "running"
    eng.on_crypto_global(2.08e12, {"btc": 57.9, "eth": 11.0, "bnb": 3.5, "sol": 3.0, "xrp": 5.0}, now - 300)
    eng.on_stable_supply({"total": 2.85e11, "d1": 9e8, "w1": -2.6e9, "m1": -1.6e10, "observed_at": now - 120}, [])
    share = eng.series["stables"]
    assert not share.inst.anchor_px                         # no prices yet: cannot anchor
    k = int(now // G5_STEP)
    for inst in share.inst.basket.values():                 # history arrives
        inst.seed("g5", [(t * G5_STEP, 100.0) for t in range(k - 400, k)], now)
        inst.seed("g1", [(t * 60, 100.0) for t in range(int(now // 60) - 600, int(now // 60))], now)
        inst.on_price(100.0, now - 1, now - 0.9)
    eng.seed_state["state"] = "done"
    eng.tick(now)
    assert share.inst.anchor_px and 13.0 < share.inst.price < 14.5   # 285B / 2.08T = 13.7%
    assert eng.status_of(share, now) == "live"


def test_history_retries_until_complete(cfg, keys, monkeypatch, tmp_path):
    """If the network is down at start, history is retried rather than left empty for the session."""
    from moneyflow.connectors import binance as bn
    eng = engine_at(cfg, keys, 1795528800.0)
    eng.cache_dir = tmp_path
    eng.connectors = {}
    eng.cfg = dict(cfg, connectors=dict(cfg["connectors"], yahoo={"enabled": False}))
    eng.keys = type(keys)()                                # no Alpaca keys: alpaca history not expected
    calls = {"n": 0}

    async def flaky(session, market, symbol, interval, start_ms, end_ms, price="last"):
        calls["n"] += 1
        if calls["n"] < 60:
            raise RuntimeError("Cannot connect to host api.binance.com")
        step = 300 if interval == "5m" else 60
        return [(t, 100.0, 1.0, 0.5) for t in range(int(start_ms / 1000) // step * step, int(end_ms / 1000) - step, step)][-50:]

    monkeypatch.setattr(bn, "fetch_klines", flaky)
    sleeps = []
    real_sleep = asyncio.sleep

    async def fast_sleep(d):
        sleeps.append(d)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    asyncio.run(eng._seed_then_track())
    assert sleeps and sleeps[0] == 60.0                   # retried after the first failure
    assert eng.seed_state["state"] == "done" and not eng.seed_state["errors"]
    assert eng.instruments["binance_spot:BTCUSDT"].g5.last_k is not None
