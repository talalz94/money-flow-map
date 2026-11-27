"""Engine behaviour that needs no network: statuses, staleness limits, closed markets, history retry."""
import asyncio

import pytest

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


def test_keyless_bonds_follow_the_proxy_from_tlts_real_close(cfg, monkeypatch, tmp_path):
    """No Alpaca key: TLT's history comes from Yahoo's chart. The bonds node anchors on TLT's real
    last bar and follows TMF / 3 through the US session (it once sat frozen at that bar all session),
    and the Yahoo prices never count as live TLT."""
    from moneyflow.connectors import binance as bn, pollers
    from moneyflow.keys import KeyStore
    start = ny_ts(2026, 11, 24, 10, 0)                     # Tuesday, US session open
    eng = engine_at(cfg, KeyStore(), start)
    eng.cache_dir = tmp_path
    eng.connectors = {}

    async def klines(session, market, symbol, interval, start_ms, end_ms, price="last"):
        step = 300 if interval == "5m" else 60
        t1 = int(end_ms / 1000) // step * step
        return [(t, 30.0 if symbol == "TMFUSDT" else 100.0, 1.0, 0.5) for t in range(t1 - 500 * step, t1, step)]

    asked = []

    async def chart(session, symbol, interval="5m", rng="60d", now=None):
        asked.append((symbol, interval, rng))
        if symbol != "TLT":
            return []
        bars = range(int(start) - 1800, int(start), 60 if interval == "1m" else G5_STEP)   # 9:30 on
        return [(t, 82.0 if t == bars[-1] else 81.0) for t in bars]                         # last close 82

    monkeypatch.setattr(bn, "fetch_klines", klines)
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    asyncio.run(eng.seed_history())
    tlt, tmf, bonds = eng.instruments["alpaca:TLT"], eng.instruments["binance_fut:TMFUSDT"], eng.series["bonds"]
    assert ("TLT", "5m", "60d") in asked and ("TLT", "1m", "2d") in asked
    assert tlt.extra["history_source"] == "Yahoo chart (no Alpaca key)"
    assert bonds.inst.g1.asof(start) == bonds.inst.g5.asof(start) == 82.0   # both grids: TLT's last bar
    for m in range(1, 31):                                 # 30 minutes: TMF (3x TLT) climbs 3%
        t = start + 60 * m
        tmf.on_price(30.0 * (1 + 0.001 * m), t - 0.5, t - 0.4)
        eng.tick(t)
    t_end = start + 1800
    assert not tlt.live_seen and not bonds.inst.primary_live(t_end)
    assert bonds.inst.price == pytest.approx(82.0 * 1.03 ** (1 / 3), rel=2e-4)   # +1%, not frozen at 82.0
    assert bonds.inst.active_source(t_end) is tmf


def test_share_and_its_own_basket_are_not_a_rotation(cfg, keys, monkeypatch):
    """The stablecoin share is computed from BTC and ETH prices, so it correlates with them at about
    -1 by arithmetic. Flow inference must treat that as unknown (weight factor 0.5): it once drew
    'Bitcoin -> Stablecoins, corr -1.00' as the strongest kind of rotation."""
    from conftest import filled_engine_at
    from moneyflow import flow
    now = ny_ts(2026, 11, 24, 12, 0)
    eng = filled_engine_at(cfg, keys, now)
    assert eng._linked_pairs() == {("btc", "stables"), ("stables", "btc"), ("eth", "stables"), ("stables", "eth")}
    seen = {}
    real = flow.infer_edges

    def spy(z, corr_fn, **kw):
        seen.update(btc=corr_fn("btc", "stables"), eth=corr_fn("stables", "eth"), gold=corr_fn("gold", "stables"))
        return real(z, corr_fn, **kw)

    monkeypatch.setattr(flow, "infer_edges", spy)
    eng.build_tf("today", now)
    assert seen["btc"] is None and seen["eth"] is None
    assert isinstance(seen["gold"], float)                  # unrelated pairs keep their correlation


def test_daily_roll_drops_values_cached_for_the_old_day(cfg, keys):
    """At 17:00 New York TODAY starts a new window. Values cached for the old one (σ for up to a
    minute) were still served after it: z-scores ~6x too small, seen live on 24 Nov."""
    import math
    from conftest import filled_engine_at
    from moneyflow import stats
    before = ny_ts(2026, 11, 24, 16, 59) + 30
    eng = filled_engine_at(cfg, keys, before)
    btc = eng.series["btc"]
    old = eng.read(btc, stats.window_for("today", before), before)["sigma_pct"]   # fills the cache
    after = ny_ts(2026, 11, 24, 17, 0) + 5
    eng.tick(after)
    r = eng.read(btc, stats.window_for("today", after), after)
    fresh, _, _ = stats.sigma_for("today", btc.inst, after)
    assert r["sigma_pct"] == pytest.approx((math.exp(fresh) - 1) * 100, abs=1e-4)
    assert abs(r["sigma_pct"] - old) > 1e-3                 # the new day's σ really differs


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
