import math

import pytest

from conftest import ny_ts
from moneyflow.series import DXY, G1_STEP, G5_STEP, Grid, Instrument, NaN, StablecoinShare, Stitched


def test_grid_put_get_asof_and_gap_invalidation():
    g = Grid(60, 10)
    for k in range(100, 105):
        g.put(k, float(k), True)
    assert g.get(104) == 104.0 and g.get(99) != g.get(99)       # NaN outside
    assert g.asof(105 * 60) == 104.0                             # close of the step that ended at 105*60
    assert g.asof(104 * 60 + 30) == 103.0                        # step 104 has not ended yet
    g.put(108, 108.0, False)                                     # skip 105..107 -> unknown
    assert math.isnan(g.get(106)) and not g.is_live(108)
    assert g.asof(108 * 60, max_back=1) != g.asof(108 * 60, max_back=1)   # 107 unknown, 106 too far back
    assert g.asof(108 * 60, max_back=5) == 104.0
    for k in range(109, 125):                                    # ring wrap
        g.put(k, float(k), True)
    assert math.isnan(g.get(110)) and g.get(124) == 124.0 and g.first_k() == 115


def test_instrument_rolls_steps_and_carries_forward():
    t0 = ny_ts(2026, 11, 24, 10, 0)                              # a Tuesday, 24/7 instrument
    inst = Instrument("x:A", "x", "A", "24/7", volume=True)
    inst.on_price(100.0, t0 + 1, t0 + 1.1)
    inst.on_price(101.0, t0 + 50, t0 + 50.1)
    inst.on_price(102.0, t0 + 61, t0 + 61.1)                     # next minute closes the first at 101
    assert inst.g1.get(int(t0 // 60)) == 101.0
    inst.feed_alive(t0 + 5 * 60)                                 # a quiet market: the feed's heartbeat
    inst.roll(t0 + 5 * 60 + 1)                                   # idle on a live feed: carried forward
    assert inst.g1.get(int(t0 // 60) + 4) == 102.0
    assert inst.g5.get(int(t0 // 300)) == 102.0 and inst.g5.is_live(int(t0 // 300))
    assert inst.latency_ms() == pytest.approx(100.0, abs=1)
    # the feed goes quiet with no heartbeat (asleep, or the network down): unknown, not the old price
    inst.roll(t0 + 20 * 60 + 1)
    for k in range(int(t0 // 60) + 6, int(t0 // 60) + 20):
        assert inst.g1.get(k) != inst.g1.get(k) and not inst.g1.is_live(k)
    assert inst.g5.get(int(t0 // 300) + 2) != inst.g5.get(int(t0 // 300) + 2)
    assert not inst.g5.is_live(int(t0 // 300) + 2) and inst.holes["g5"] == t0 + 600
    assert inst.v5.get(int(t0 // 300) + 2)[0] != inst.v5.get(int(t0 // 300) + 2)[0]     # volume unknown, not 0
    inst.on_price(103.0, t0 + 20 * 60 + 5, t0 + 20 * 60 + 5.1)  # the feed is back
    inst.roll(t0 + 21 * 60 + 1)
    assert inst.g1.get(int(t0 // 60) + 20) == 103.0 and inst.g1.is_live(int(t0 // 60) + 20) and inst.recovered


def test_latency_excludes_polled_prices_and_late_prints():
    t0 = ny_ts(2026, 11, 24, 10, 0)
    inst = Instrument("x:A", "x", "A", "24/7")
    inst.on_price(10.0, t0 - 90, t0, measure=False)          # REST "latest trade", 90 s old: data age only
    assert inst.latency_ms() is None and inst.live_seen and inst.age(t0) == 90
    thin = Instrument("x:B", "x", "B", "24/7")
    thin.on_price(20.0, t0 - 200, t0)                         # a print reported 200 s late
    assert thin.latency_ms() is None and thin.extra["late_prints"] == 1 and thin.price == 20.0
    thin.on_price(20.1, t0 + 0.9, t0 + 1.0)                   # a live tick, 100 ms in transit
    assert thin.latency_ms() == pytest.approx(100.0, abs=1)
    thin.on_price(19.9, t0 - 30, t0 + 2.0)                    # out of order (older than the last tick): ignored
    assert thin.price == 20.1


def test_minute_volumes_sum_into_five_minute_steps():
    t0 = ny_ts(2026, 11, 24, 10, 0)
    inst = Instrument("x:A", "x", "A", "24/7", volume=True)
    inst.on_price(1.0, t0, t0)
    for m in range(5):
        inst.on_minute_volume(t0 + 60 * m, 10.0 + m, 6.0)       # kline-style cumulative per minute
        inst.on_minute_volume(t0 + 60 * m, 20.0 + m, 12.0)      # later update of the same minute replaces it
    inst.roll(t0 + 300 + 1)
    q, tb = inst.v5.get(int(t0 // 300))
    assert q == pytest.approx(sum(20.0 + m for m in range(5))) and tb == pytest.approx(60.0)


def test_us_regular_instrument_ignores_extended_hours_prints():
    inst = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular")
    inst.on_price(80.0, ny_ts(2026, 11, 24, 8, 0), ny_ts(2026, 11, 24, 8, 0))   # pre-market
    assert math.isnan(inst.price) and inst.extra["ext_price"] == 80.0
    inst.on_price(81.0, ny_ts(2026, 11, 24, 9, 31), ny_ts(2026, 11, 24, 9, 31))
    assert inst.price == 81.0


def test_carried_price_is_not_trading_until_live_data_arrives():
    """A downloaded close carried through market hours is not trading. Only live data makes a session
    step count as traded; otherwise the bonds node sat frozen at TLT's close all session without a key."""
    inst = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular", volume=True)
    t0 = ny_ts(2026, 11, 24, 10, 0)
    inst.seed("g5", [(t0 - 600, 80.0), (t0 - 300, 80.5)], t0)
    assert inst.g5.is_live(int((t0 - 300) // G5_STEP))            # real bars keep their flags
    inst.roll(t0 + 1)                                             # starts the step clock
    inst.roll(t0 + 1801)                                          # 30 session minutes, no live message
    k = int((t0 + 600) // G5_STEP)
    assert inst.g5.get(k) == 80.5 and not inst.g5.is_live(k)
    assert not inst.g1.is_live(int((t0 + 600) // G1_STEP))
    assert math.isnan(inst.v5.get(k)[0])                          # volume unknown, not zero (no RVOL 0)
    inst.on_price(81.0, t0 + 1810, t0 + 1810)                     # the first live trade
    inst.roll(t0 + 2101)
    k_live = int((t0 + 1800) // G5_STEP)
    assert inst.g5.get(k_live) == 81.0 and inst.g5.is_live(k_live)
    assert inst.v5.get(k_live) == (0.0, 0.0)                      # a quiet step on a live feed


def test_seed_fills_with_carry_forward_and_market_flags():
    inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    start = ny_ts(2026, 11, 23, 15, 50)
    closes = [(start, 10.0), (start + 300, 11.0)]                 # 15:50 and 15:55 bars
    until = ny_ts(2026, 11, 23, 16, 30)
    n = inst.seed("g5", closes, until)
    assert n == (until - start) // 300
    k_close = int(ny_ts(2026, 11, 23, 16, 0) // 300)
    assert inst.g5.get(k_close + 3) == 11.0                       # carried after the close
    assert inst.g5.is_live(k_close - 1) and not inst.g5.is_live(k_close + 1)
    assert inst.price == 11.0                                     # no live price yet: last close


def _inst(price, sym="P"):
    i = Instrument(f"t:{sym}", "t", sym, "24/7")
    i.price, i.ts = price, 0.0
    return i


def test_dxy_formula_elasticities():
    px = {"EURUSD": 1.0960, "USDJPY": 159.0, "GBPUSD": 1.27, "USDCAD": 1.38, "USDSEK": 10.24, "USDCHF": 0.80}
    base = DXY.formula(px)
    assert 95 < base < 115
    up = DXY.formula({**px, "EURUSD": px["EURUSD"] * 1.01})
    assert math.log(up / base) == pytest.approx(-0.576 * math.log(1.01), rel=1e-9)
    assert math.isnan(DXY.formula({**px, "USDSEK": NaN}))
    d = DXY("dxy", {k: _inst(v, k) for k, v in px.items()})
    assert d.value_live(0) == pytest.approx(base)


def test_stablecoin_share_moves_with_the_basket():
    b = {"BTC": _inst(100.0, "BTC"), "ETH": _inst(10.0, "ETH")}
    s = StablecoinShare("share", b)
    s.set_weights({"BTC": 3.0, "ETH": 1.0})
    s.set_supply(100.0, 0.0)
    s.set_anchor(1000.0, 0.0, prices={"BTC": 100.0, "ETH": 10.0})
    assert s.value_live(0) == pytest.approx(10.0)                 # 100 / 1000
    b["BTC"].price, b["ETH"].price = 90.0, 9.0                    # basket -10%
    assert s.value_live(0) == pytest.approx(100 / (100 + 900 * 0.9) * 100)


def test_stitched_one_minute_and_five_minute_grids_agree():
    """The 1-minute grid spans only a day: when it starts after the primary's last session, its chain
    must continue from the 5-minute series (which includes the proxy's moves since the close), and
    no proxy move may be dropped where the proxy's 1-minute bars begin or have a hole. Each of these
    once left the bonds series off for good (0.1%, 0.04%, 0.012%), skewing TODAY's bond move."""
    close = ny_ts(2026, 11, 23, 16, 0)
    now = close + 86400 + 2040                 # the next day, 16:34 New York
    prim = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular")
    prox = Instrument("bf:TMF", "bf", "TMF", "24/7")
    k5_open, k5_now = int(ny_ts(2026, 11, 23, 9, 30) // G5_STEP), int(now // G5_STEP)
    prim.seed("g5", [(k * G5_STEP, 80.0) for k in range(k5_open, int(close // G5_STEP))], close)
    prim.seed("g1", [(k * 60, 80.0) for k in range(int(close // 60) - 390, int(close // 60))], close)
    # the proxy rises 0.3% an hour, every hour, so the stitched series rises 0.1% an hour after the close
    px = lambda t: 30.0 * math.exp(0.003 * (t - close) / 3600) if t > close else 30.0
    prox.seed("g5", [(k * G5_STEP, px((k + 1) * G5_STEP)) for k in range(k5_open, k5_now)], now)
    # as the engine downloads them, the proxy's 1-minute bars begin a minute into the grid's day
    # (at 16:35, right after a 5-minute boundary) ...
    prox.seed("g1", [(k * 60, px((k + 1) * 60)) for k in range(int(now // 60) - 1439, int(now // 60))], now)
    prox.g1.put(int((close + 3000) // 60), NaN, True)                 # ... and the 16:50 bar is missing
    st = Stitched("s", prim, prox, beta=3.0)
    st.rebuild(now)
    t_check = close + 3600                                            # 17:00, inside the 1-minute day
    a, b = st.g1.asof(t_check), st.g5.asof(t_check)
    assert a == a and b == b
    assert a == pytest.approx(b, rel=1e-6)                            # the grids agree
    assert a == pytest.approx(80.0 * math.exp(0.001), rel=1e-6)      # an hour after the close at 0.1%/h


def test_stitched_uses_primary_when_open_and_scaled_proxy_otherwise():
    prim = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular")
    prox = Instrument("bf:TMF", "bf", "TMF", "24/7")
    st = Stitched("s", prim, prox, beta=3.0)
    t_open = ny_ts(2026, 11, 24, 15, 0)
    prim.on_price(80.0, t_open, t_open)
    prox.on_price(30.0, t_open, t_open)
    st.refresh(t_open + 1)
    assert st.price == 80.0 and st.active_source(t_open + 1) is prim
    # after the close: proxy +3% -> stitched +1%
    t_eve = ny_ts(2026, 11, 24, 18, 0)
    for k in range(int((t_open + 60) // G1_STEP), int(t_eve // G1_STEP) + 1):
        prox.roll(k * G1_STEP + 1)
        prim.roll(k * G1_STEP + 1)
        st.refresh(k * G1_STEP + 1)
    prox.on_price(30.9, t_eve + 61, t_eve + 61)
    st.refresh(t_eve + 62)
    assert st.active_source(t_eve + 62) is prox
    assert st.price == pytest.approx(80.0 * (1 + 0.03 / 3), rel=1e-6)



def test_a_computed_series_is_not_live_while_its_inputs_were_down():
    """Bonds outside US hours follow the TMF proxy. While the proxy's feed is down the chain carries
    its last value: a known value, but not trading, so not live (σ must not count it)."""
    prim = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular")
    prox = Instrument("bf:TMF", "bf", "TMF", "24/7")
    st = Stitched("s", prim, prox, beta=3.0)
    t0, quiet_from = ny_ts(2026, 11, 24, 15, 58), ny_ts(2026, 11, 24, 18, 5)
    prim.on_price(80.0, t0, t0)                               # TLT's close anchors the chain
    prox.on_price(30.0, t0, t0)
    st.refresh(t0)
    for tt in range(int(t0) + 60, int(ny_ts(2026, 11, 24, 18, 20)) + 1, 60):
        if tt <= quiet_from:                                  # the proxy ticks, then its feed goes down
            prox.on_price(30.0 + (tt - t0) / 1e5, tt, tt)
        prox.roll(tt + 1)
        prim.roll(tt + 1)
        st.refresh(tt + 1)
    k_live, k_down = int(ny_ts(2026, 11, 24, 17, 30) // G1_STEP), int(ny_ts(2026, 11, 24, 18, 15) // G1_STEP)
    assert st.g1.is_live(k_live) and prox.g1.is_live(k_live)             # TLT closed, the proxy live
    assert not prox.g1.is_live(k_down) and not st.g1.is_live(k_down)     # the proxy down: carried, not live
    assert st.g1.get(k_down) == st.g1.get(k_down)                         # (still a value: the chain's last)



def test_a_computed_series_counts_as_trading_only_where_all_its_inputs_do():
    """By default a computed step is live only if every input's step is (one input carried from its
    history, not trading: the computed step is not either)."""
    from moneyflow.series import Derived

    class Avg(Derived):
        def value_live(self, now):
            return (self.inputs[0].price + self.inputs[1].price) / 2

        def value_step(self, grid, k):
            a, b = (getattr(i, grid).get(k) for i in self.inputs)
            return (a + b) / 2

    a, b = Instrument("x:A", "x", "A", "24/7"), Instrument("x:B", "x", "B", "24/7")
    t0 = ny_ts(2026, 11, 24, 10, 0)
    b.seed("g1", [(t0 - 3600 + 60 * i, 50.0) for i in range(60)], t0)      # history only, never live
    d = Avg("avg", [a, b])
    for s in range(0, 601, 10):
        a.on_price(100.0, t0 + s, t0 + s)
        a.feed_alive(t0 + s)
        a.roll(t0 + s)
        b.roll(t0 + s)
        d.refresh(t0 + s)
    k = int((t0 + 300) // G1_STEP)
    assert d.g1.get(k) == 75.0 and a.g1.is_live(k) and not b.g1.is_live(k) and not d.g1.is_live(k)



def test_a_seeded_close_is_the_price_until_live_data_and_is_never_dated_ahead():
    """Before any live price this session, a download newer than the price (the cache's close) takes
    over. A bar still open is not used (it would date the price ahead, and live prices older than
    that would be dropped as out of order). Once live data has come, a later download leaves it."""
    t0 = 1795528800.0                                       # on a 5-minute boundary
    inst = Instrument("x:A", "x", "A", "24/7")
    inst.price, inst.ts = 50.0, t0 - 7200                    # the cache's last close
    inst.seed("g5", [(t0 - 3600 + 300 * i, 60.0 + i) for i in range(12)], t0)
    assert inst.price == 71.0 and inst.ts == t0
    rows = [(t0 - 600 + 60 * i, 80.0 + i) for i in range(12)]          # the last one, [t0+60, t0+120), is open
    inst.seed("g1", rows, t0 + 90)
    assert inst.price == 90.0 and inst.ts == t0 + 60
    inst.on_price(100.0, t0 + 70, t0 + 70)                    # live, older than the open bar's end
    assert inst.price == 100.0
    inst.seed("g5", [(t0 + 300 * i, 70.0) for i in range(3)], t0 + 900)   # a back-fill after live data
    assert inst.price == 100.0 and inst.ts == t0 + 70



def test_a_session_instruments_seeded_price_is_its_session_close_like_a_live_one():
    """An ETF's live price stays on the regular session (pre/post-market prints are kept aside); a
    download with extended-hours bars leaves it on the session's last close the same way."""
    close = ny_ts(2026, 11, 24, 16, 0)
    inst = Instrument("x:SPY", "x", "SPY", "us_regular")
    rows = [(close - 3600 + 300 * i, 600.0 + i) for i in range(12)]          # 15:00-16:00
    rows += [(close + 300 * i, 590.0) for i in range(24)]                     # after hours, 16:00-18:00
    inst.seed("g5", rows, close + 7200)
    assert inst.price == 611.0 and inst.ts == close


def test_a_seed_with_no_bar_inside_the_grid_still_gives_a_missing_price_the_last_close():
    """Bars that all fall before what the grid holds still give an instrument without a price their
    last close (as before), dated at that bar."""
    t0 = 1795528800.0
    old = t0 - 3 * 86400                                    # the 1-minute grid holds one day
    inst = Instrument("x:A", "x", "A", "24/7")
    inst.seed("g1", [(old + 60 * i, 40.0 + i) for i in range(10)], t0)
    assert inst.price == 49.0 and inst.ts == old + 600


def test_an_etfs_pre_and_post_market_bars_are_kept_aside_and_its_session_close_carried():
    """An ETF's grids hold its regular session, as its live series does: a download's pre- and
    post-market bars (IEX, thin) are kept aside and the session's close is carried over those hours.
    Taken as closes, Friday evening's prints were carried all weekend: Monday's TODAY reference was an
    after-hours print (0.46% off in the real cache), while the price stayed on the session."""
    from moneyflow import stats
    inst = Instrument("x:SPY", "x", "SPY", "us_regular")
    close, sat, mon = ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 0)
    rows = [(close - 3600 + 300 * i, 690.0 + i) for i in range(12)]           # the last hour: 701 at 15:55
    rows += [(close + 300 * i, 693.89) for i in range(48)]                    # post-market prints, 16:00-20:00
    inst.seed("g5", rows, sat)
    shut = range(int(close // G5_STEP), int(sat // G5_STEP))
    assert all(inst.g5.get(k) == 701.0 and not inst.g5.is_live(k) for k in shut)
    assert inst.price == 701.0 and inst.ts == close
    assert inst.extra["ext_price"] == 693.89 and inst.extra["ext_ts"] == ny_ts(2026, 11, 20, 20, 0)
    # Monday 9:00: downloaded from the cache's end (Saturday), only the morning's pre-market prints came
    pre = [(ny_ts(2026, 11, 23, 4, 0) + 300 * i, 707.0) for i in range(60)]
    inst.seed("g5", pre, mon, since=sat - 600)
    assert all(inst.g5.get(k) == 701.0 for k in range(int(close // G5_STEP), int(mon // G5_STEP)))
    assert stats.price_at(inst, ny_ts(2026, 11, 22, 17, 0), mon) == 701.0    # TODAY's start: Friday's close
    assert inst.price == 701.0 and inst.extra["ext_price"] == 707.0
    inst.seed("g1", [(ny_ts(2026, 11, 23, 8, 0) + 60 * i, 707.5) for i in range(60)], mon)
    assert inst.g1.get(int(ny_ts(2026, 11, 23, 8, 30) // G1_STEP)) == 701.0   # an empty 1-minute grid: from the 5-minute one
    # Yahoo's last point of a day is a 16:00 closing print: by Saturday afternoon older than the 1-day
    # grid, still not the close carried into it
    qqq, sat_pm = Instrument("x:QQQ", "x", "QQQ", "us_regular"), ny_ts(2026, 11, 21, 16, 45)
    qqq.seed("g1", [(close - 120, 600.0), (close - 60, 601.0), (close, 600.5)], sat_pm)
    assert qqq.g1.get(int(sat_pm // G1_STEP) - 1) == 601.0 and qqq.price == 601.0 and qqq.ts == close


def test_a_thin_etfs_first_trade_after_a_gap_still_covers_it():
    """IEX has bars only for minutes that traded, so a thin ETF's first bar can come minutes after the
    open. Downloaded from before the gap (`since`), the minutes up to it had no trade (the cache's last
    close, carried), not a gap fetched again and given up ("no exchange data"); the cache's own steps
    after `since` stay."""
    inst = Instrument("x:UUP", "x", "UUP", "us_regular")
    t9, t930, t10 = ny_ts(2026, 11, 23, 9, 0), ny_ts(2026, 11, 23, 9, 30), ny_ts(2026, 11, 23, 10, 0)
    inst.g1.put(int(t9 // G1_STEP) - 1, 27.0, False)                        # the cache ends at 9:00
    inst._hole("g1", t930)                                                   # the time off: from the open
    inst.seed("g1", [(t930 + 900, 27.1), (t930 + 1200, 27.2)], t10, since=t9 - 600)
    assert inst.holes["g1"] is None
    k = int((t930 + 300) // G1_STEP)
    assert inst.g1.get(k) == 27.0 and inst.g1.is_live(k)                    # open, no trade yet: the close
    assert inst.g1.get(int((t930 + 900) // G1_STEP)) == 27.1 and inst.g1.get(int(t9 // G1_STEP) - 1) == 27.0
    # a gap older than the 1-day grid: covered by a download from the grid's first minute, though the
    # first bar came hours later (at 9:45, after a night without trades)
    inst._hole("g1", t9 - 2 * 86400)
    t11 = ny_ts(2026, 11, 23, 11, 0)
    inst.seed("g1", [(t930 + 900, 27.1), (t930 + 1200, 27.2)], t11, since=t11 - 86400 + 60)
    assert inst.holes["g1"] is None
