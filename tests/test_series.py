import math

import pytest

from conftest import ny_ts
from moneyflow import clock
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
    assert inst.holes == {"g1": None, "g5": None}                 # no feed at all (no key): no gap either
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
    """A download newer than the price takes over: the cache's close, or a live price from before a
    sleep or an outage (the feed silent since). A bar still open is not used (it would date the price
    ahead, and live prices older than that would be dropped as out of order); a download older than a
    live price leaves it."""
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
    inst.seed("g5", [(t0 - 300, 60.0)], t0 + 900)            # a download ending before the live price
    assert inst.price == 100.0 and inst.ts == t0 + 70
    inst.seed("g5", [(t0 + 300 * i, 70.0) for i in range(3)], t0 + 900)   # after it: the feed was silent
    assert inst.price == 70.0 and inst.ts == t0 + 900



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


def test_a_download_with_no_bar_still_carries_the_close_and_covers_its_gap():
    """IEX has bars only for minutes that traded, so an answer with none at all (a weekend, a holiday,
    a thin ETF before its first trade) still means the last close held since the download began.
    Seeded with nothing, the time off stayed empty (TODAY unknown all day) and its gap was fetched
    again and given up. A gap opening after the download's end is still the next download's."""
    from moneyflow import stats
    inst = Instrument("x:UUP", "x", "UUP", "us_regular")
    sat, mon = ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 40)
    k = int(sat // G5_STEP)
    for j in range(k - 100, k):
        inst.g5.put(j, 27.0, False)                                # the cache: Friday's close, to Saturday noon
    inst._hole("g5", ny_ts(2026, 11, 23, 9, 30))                   # the time off, from Monday's open
    assert inst.seed("g5", [], mon) == 0 and inst.g5.last_k == k - 1   # no `since`: nothing to go by
    inst.seed("g5", [], mon, since=sat - 600)                     # an answer without a bar: no trade since
    assert all(inst.g5.get(j) == 27.0 for j in range(k, int(mon // G5_STEP)))
    assert inst.holes["g5"] is None and stats.price_at(inst, ny_ts(2026, 11, 22, 17, 0), mon) == 27.0
    inst._hole("g5", mon + 300)                                    # after the download's end
    inst.seed("g5", [], mon, since=sat - 600)
    assert inst.holes["g5"] == mon + 300


def test_a_newer_download_replaces_a_stale_live_price_and_what_the_roll_carried_of_it():
    """SPY traded at 600 until the PC slept at 15:30; it woke at 16:30, after a close at 605.5. The
    roll carried 600 on from the wake, and the back-fill's newer bars left the price alone (live data
    had come; after the close nothing else corrects it): 600 all night and weekend. The download's
    newer close takes over, also in the steps the roll had carried meanwhile."""
    inst = Instrument("x:SPY", "x", "SPY", "us_regular")
    t0, close, wake = ny_ts(2026, 11, 20, 15, 0), ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 20, 16, 30)
    for t in range(int(t0), int(t0) + 1800, 10):                   # live until 15:30
        inst.on_price(600.0, t, t)
        inst.roll(t)
    for t in range(int(wake), int(wake) + 180, 10):                # awake: the old price carried on
        inst.feed_alive(t)
        inst.roll(t)
    k1 = int((wake + 60) // G1_STEP)
    assert inst.g1.get(k1) == 600.0
    k_pre, k_live = int(ny_ts(2026, 11, 20, 9, 0) // G1_STEP), int((wake + 120) // G1_STEP)
    inst.g1.put(k_pre, 600.0, False)                               # the morning carried 600 as well: real
    inst.g1.put(k_live, 600.0, True)                               # a live step at 600 is data too
    k_mid, k_other = int((close + 900) // G1_STEP), int((close + 1200) // G1_STEP)
    inst.g1.put(k_other, 599.0, False)                             # another value: not carried from 600
    assert inst.g1.get(k_mid) == 600.0
    inst.seed("g5", [(t0 + 300 * i, 600.0 + 0.5 * i) for i in range(12)], wake + 30, since=t0)
    assert inst.price == 605.5 and inst.ts == close
    assert inst.g1.get(k1) == 605.5 and inst.g5.get(int((wake - 300) // G5_STEP)) == 605.5
    # from the new close's end on, before the download's end too (the 1-day download of a wake days
    # later has no session bar: it keeps what the grid holds); only what was carried from the old price
    assert inst.g1.get(k_mid) == 605.5 and inst.g1.get(k_other) == 599.0
    assert inst.g1.get(k_pre) == 600.0 and inst.g1.get(k_live) == 600.0
    assert inst.holes["g5"] is None


def test_an_answer_with_no_bar_over_a_long_open_stretch_leaves_it_unknown():
    """IEX has bars only for minutes that traded, but an answer with no session bar at all is "no trade"
    only over shut hours and a short stretch of an open market (a start a few minutes after the open,
    before a thin ETF's first print). Over a longer one it is more likely the venue's own gap (a 30-
    minute outage of SPY was written as its 9:59 price, flagged live, the hole cleared for good): what
    has no value there stays unknown, and so do the shut hours after it (their close is unknown too);
    the hole is from there. With a session bar in the answer, a quiet stretch is a thin ETF's."""
    sat = ny_ts(2026, 11, 21, 12, 0)
    for grid, step in (("g5", G5_STEP), ("g1", G1_STEP)):
        for start, carried in ((ny_ts(2026, 11, 23, 9, 44), True), (ny_ts(2026, 11, 23, 9, 50), False)):
            inst = Instrument("x:UUP", "x", "UUP", "us_regular")
            g = getattr(inst, grid)
            k = int(sat // step)
            for j in range(k - 100, k):
                g.put(j, 27.0, False)                              # the cache: Friday's close, to Saturday noon
            opened = ny_ts(2026, 11, 23, 9, 30)
            inst._hole(grid, opened)
            inst.seed(grid, [], start, since=sat - 600)
            weekend = range(max(k, int(start // step) - g.size + 1), int(opened // step))   # (as far as it holds)
            session = range(int(opened // step), int(start // step))
            assert all(g.get(j) == 27.0 for j in weekend), (grid, start)
            assert all((g.get(j) == 27.0) is carried and not (g.is_live(j) and not carried) for j in session)
            assert inst.holes[grid] == (None if carried else opened), (grid, start)
    # an outage over Friday's close: nothing from 15:00 on (the weekend would carry the 14:55 price);
    # SPY's close changed every 5 minutes before it
    fri, mon = ny_ts(2026, 11, 20, 15, 0), ny_ts(2026, 11, 23, 8, 0)
    k_fri = int(fri // G5_STEP)
    for closes, carried in (([], False), ([(fri - 300, 27.5)], True)):
        inst = Instrument("x:SPY", "x", "SPY", "us_regular")
        for j in range(k_fri - 50, k_fri):
            inst.g5.put(j, 27.5 if j == k_fri - 1 else 27.0 + 0.01 * (j % 13), True)
        inst.seed("g5", closes, mon, since=fri - 600)
        after = range(k_fri, int(mon // G5_STEP))
        assert all((inst.g5.get(j) == 27.5) is carried for j in after)
        assert inst.holes["g5"] == (None if carried else fri)
    # an older stretch with no value (a gap given up before): unknown only up to the grid's next value
    inst = Instrument("x:SPY", "x", "SPY", "us_regular")
    gap, close = range(k_fri - 12, k_fri - 6), int(ny_ts(2026, 11, 20, 16, 0) // G5_STEP)
    for j in range(k_fri - 50, close):
        inst.g5.put(j, NaN if j in gap else 27.5 if j == close - 1 else 27.0 + 0.01 * (j % 13), j not in gap)
    sat = ny_ts(2026, 11, 21, 12, 0)
    inst.seed("g5", [], sat, since=fri - 4200)
    assert all(inst.g5.get(j) != inst.g5.get(j) for j in gap) and inst.holes["g5"] == gap[0] * G5_STEP
    assert all(inst.g5.get(j) == 27.5 for j in range(close, int(sat // G5_STEP)))   # the night carried


def test_an_empty_answer_over_a_long_open_stretch_is_no_trade_when_the_venue_last_traded_before_it():
    """An ETF whose close changed every 5 minutes lately (its quiet limit: 15 minutes) and that first
    prints at 10:00: at a 9:50 start the answer has no bar for it, and 9:30-9:50 was read as the venue's
    gap (a hole, one more download, "no exchange data"). The venue's own latest trade tells: before 9:30
    (Friday), nothing traded since, so the stretch is no trade (the close carried, not live, no hole);
    inside the stretch, the answer missed a trade: unknown, as without it."""
    sat, opened, start = ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 30), ny_ts(2026, 11, 23, 9, 50)
    for grid, step in (("g5", G5_STEP), ("g1", G1_STEP)):
        for last_trade, carried in ((ny_ts(2026, 11, 20, 15, 59) + 30, True), (None, False),
                                    (ny_ts(2026, 11, 23, 9, 36), False)):
            inst = Instrument("x:SPY", "x", "SPY", "us_regular")
            g = getattr(inst, grid)
            k = int(sat // step)
            for j in range(k - 100, k):
                g.put(j, 27.0, False)                              # the cache: Friday's close, to Saturday noon
            inst._hole(grid, opened)
            assert inst.quiet_limit_s() == 900.0
            inst.seed(grid, [], start, since=sat - 600, last_trade=last_trade)
            session = range(int(opened // step), int(start // step))
            assert all((g.get(j) == 27.0) is carried and not g.is_live(j) for j in session), (grid, last_trade)
            assert inst.holes[grid] == (None if carried else opened), (grid, last_trade)


def test_a_symbol_that_often_goes_quiet_has_its_empty_answer_read_as_no_trade_for_longer():
    """The open stretch an answer with no bar is read as "no trade" over is the symbol's own: twice the
    longest run of unchanged 5-minute closes in its sessions, 15 minutes at least. UUP often prints
    nothing on IEX for 50 minutes after the open, so a fixed 15 minutes left its quiet opening unknown
    (fetched again, given up as "no exchange data", its 1H blank an hour later); SPY never goes 5. Both
    grids measure the same stretch: one reaching the download's end runs on to it (at 9:47 the 1-minute
    grid saw 17 minutes, the 5-minute one 15)."""
    mon_open, fri = ny_ts(2026, 11, 23, 9, 30), ny_ts(2026, 11, 20, 16, 0)

    def etf(sym, quiet_steps):
        # 35 days to Saturday noon: each session opens quiet for `quiet_steps` steps (the last close
        # carried), then the close changes every step; shut hours carry the session's close
        inst = Instrument(f"x:{sym}", "x", sym, "us_regular")
        prev, n = 27.0, 0
        for j in range(int(fri // G5_STEP) - 35 * 288, int(ny_ts(2026, 11, 21, 12, 0) // G5_STEP)):
            live = clock.market_open("us_regular", (j + 1) * G5_STEP - 1)
            n = (0 if not clock.market_open("us_regular", j * G5_STEP - 1) else n + 1) if live else n
            closing = quiet_steps and not clock.market_open("us_regular", (j + 3) * G5_STEP - 1)   # its last 2: quiet too
            if live and n >= quiet_steps and not closing:
                prev = 27.0 + 0.01 * (j % 13) + (0.005 if 27.0 + 0.01 * (j % 13) == prev else 0.0)
            inst.g5.put(j, prev, live)
        inst.price, inst.ts = prev, fri
        return inst
    for sym, quiet, start, carried in (("UUP", 10, ny_ts(2026, 11, 23, 10, 5), True),    # 35 minutes: within its 100
                                       ("SPY", 0, ny_ts(2026, 11, 23, 10, 5), False),    # SPY: over 15
                                       ("SPY", 0, ny_ts(2026, 11, 23, 9, 47), False),    # 17 minutes, both grids
                                       ("SPY", 0, ny_ts(2026, 11, 23, 9, 44), True)):    # 14 minutes
        inst = etf(sym, quiet)
        assert inst.quiet_limit_s() == max(900.0, 2 * quiet * G5_STEP)   # (a run ends at the close)
        for grid, step in (("g5", G5_STEP), ("g1", G1_STEP)):
            g = getattr(inst, grid)
            inst._hole(grid, mon_open)
            inst.seed(grid, [], start, since=ny_ts(2026, 11, 20, 15, 50))
            session = range(int(mon_open // step), int(start // step))
            assert all((g.get(j) == g.get(j)) is carried for j in session), (sym, start, grid)
            assert inst.holes[grid] == (None if carried else mon_open), (sym, start, grid)


def test_a_seed_gives_the_1_minute_shut_hours_the_5_minute_close_but_not_a_computed_series():
    """After a download the 1-minute grid's shut hours before its first bar take the 5-minute grid's
    close (its download reaches back to the gap; the 1-minute one is a day long). A computed series
    keeps its own: its price is computed from its inputs again at the next tick."""
    sat, mon = ny_ts(2026, 11, 21, 12, 0), ny_ts(2026, 11, 23, 9, 45)
    for inst, kept in ((Instrument("x:SPY", "x", "SPY", "us_regular"), False),
                       (DXY("dxy:usd", {n: Instrument(f"fx:{n}", "fx", n, "fx") for n in DXY.WEIGHTS}), True)):
        hours = inst.hours
        for j in range(int(sat // G5_STEP), int(mon // G5_STEP)):
            inst.g5.put(j, 101.0, False)                      # the 5-minute grid: the close slept over
        for j in range(int(mon // G1_STEP) - 1439, int(mon // G1_STEP)):
            inst.g1.put(j, 100.0, False)                      # the 1-minute one: the pre-sleep price carried
        first = ny_ts(2026, 11, 23, 9, 30) if hours == "us_regular" else ny_ts(2026, 11, 22, 17, 0)
        inst.seed("g1", [(first, 101.0)], mon, since=mon - 86400 + 60)
        shut = [j for j in range(int(mon // G1_STEP) - 1439, int(first // G1_STEP))
                if not clock.market_open(hours, (j + 1) * G1_STEP - 1)]
        assert shut and all(inst.g1.get(j) == (100.0 if kept else 101.0) for j in shut), inst.key


def test_an_empty_1_minute_grid_carries_the_5_minute_close_from_an_open_start_not_an_older_one():
    """A download that begins in a session, before its first bar, with the 1-minute grid empty there:
    the minutes up to that bar carry the 5-minute grid's close of the hour (not the close before the
    open the shut hours hold, an older one)."""
    inst = Instrument("x:UUP", "x", "UUP", "us_regular")
    fri, mon = ny_ts(2026, 11, 20, 16, 0), ny_ts(2026, 11, 23, 9, 30)
    for j in range(int(fri // G5_STEP) - 78, int(ny_ts(2026, 11, 23, 11, 0) // G5_STEP)):
        live = clock.market_open("us_regular", (j + 1) * G5_STEP - 1)
        inst.g5.put(j, 27.5 if j * G5_STEP >= mon else 27.0, live)   # Friday 27.0 (carried); Monday 27.5
    start, first, until = ny_ts(2026, 11, 23, 11, 0), ny_ts(2026, 11, 23, 11, 20), ny_ts(2026, 11, 23, 12, 0)
    inst.seed("g1", [(first, 27.6)], until, since=start)
    assert all(inst.g1.get(k) == 27.5 for k in range(int(start // G1_STEP), int(first // G1_STEP)))
    shut = [k for k in range(int(until // G1_STEP) - 1439, int(mon // G1_STEP))
            if not clock.market_open("us_regular", (k + 1) * G1_STEP - 1)]
    assert all(inst.g1.get(k) == 27.0 for k in shut)                # the shut hours: the 5-minute grid's


def test_before_its_first_trade_a_session_step_its_feed_did_not_cover_is_a_gap():
    """Once an ETF's feed has reported (any message: its stream's login counts), a session step the feed
    did not cover is a gap, before the ETF's first trade of the run too: a sleep or an outage from before
    it had the sessions carried from the cache's close, never back-filled. A step the feed covered (a
    quiet market before the first trade) is the close carried, not live, as before."""
    t0 = ny_ts(2026, 11, 24, 10, 0)
    for covered in (False, True):
        inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular", volume=True)
        inst.seed("g5", [(t0 - 600, 80.0), (t0 - 300, 80.5)], t0)
        inst.feed_alive(t0 - 60)                                  # the stream logged in, no trade yet
        inst.roll(t0 + 1)
        for t in range(int(t0) + 60, int(t0) + 1801, 60):
            if covered:
                inst.feed_alive(t)                                # its messages (other ETFs' trades)
            inst.roll(t + 1)
        k = int((t0 + 600) // G5_STEP)
        if covered:
            assert inst.g5.get(k) == 80.5 and not inst.g5.is_live(k) and inst.holes == {"g1": None, "g5": None}
        else:
            assert inst.g5.get(k) != inst.g5.get(k) and not inst.g5.is_live(k)
            assert inst.holes == {"g1": t0, "g5": t0}


def _sessions(inst, quiet, volume_in_quiet, until):
    """35 days of 5-minute steps to `until`: each session's close changes every step except a quiet stretch
    of `quiet` steps from 11:00 (its close held), with traded volume there if `volume_in_quiet`; shut
    hours carry the session's close."""
    prev, n = 50.0, 0
    for j in range(int(until // G5_STEP) - 35 * 288, int(until // G5_STEP)):
        t = j * G5_STEP
        live = clock.market_open("us_regular", t + G5_STEP - 1)
        n = (0 if not clock.market_open("us_regular", t - 1) else n + 1) if live else n
        held = live and 18 <= n < 18 + quiet                      # 11:00 is the 19th step of a session
        if live and not held:
            prev = 50.0 + 0.01 * (j % 11) + (0.005 if 50.0 + 0.01 * (j % 11) == prev else 0.0)
        inst.g5.put(j, prev, live)
        if live and inst.v5 is not None:
            inst.v5.put(j, 2e5 if (not held or volume_in_quiet) else 0.0, NaN)


def test_the_quiet_limit_counts_steps_without_a_trade_not_equal_closes_nor_its_own_fills():
    """The quiet limit is twice the longest stretch of session steps without a trade: no traded volume and
    the close unchanged. Equal closes with volume are a price that held while it traded (HYG got 50
    minutes from six equal closes, five of them with volume); and an empty answer's own "no
    trade" fill is not live, so it does not count (on this PC's cache SPY's limit went from 15 to 20
    and 30 minutes over a 14- and a 19-minute empty answer, each filled with its last price)."""
    until = ny_ts(2026, 11, 25, 9, 0)
    hyg = Instrument("x:HYG", "x", "HYG", "us_regular", volume=True)
    _sessions(hyg, 6, True, until)
    assert hyg.quiet_limit_s() == 900.0                           # held, but traded: 15 minutes
    uup = Instrument("x:UUP", "x", "UUP", "us_regular", volume=True)
    _sessions(uup, 6, False, until)
    assert uup.quiet_limit_s() == 2 * 6 * 300                     # 30 minutes without a trade: 60
    spy = Instrument("x:SPY", "x", "SPY", "us_regular", volume=True)
    _sessions(spy, 0, False, until)
    assert spy.quiet_limit_s() == 900.0
    opened = ny_ts(2026, 11, 25, 9, 30)
    for k in range(int(until // G5_STEP), int(opened // G5_STEP)):
        spy.g5.put(k, spy.g5.get(k - 1), False)                   # (the pre-market, carried)
    start = opened + 14 * 60
    spy.seed("g5", [], start, since=opened - 600)                 # a 14-minute empty answer: no trade...
    session = range(int(opened // G5_STEP), int(start // G5_STEP))
    assert all(spy.g5.get(k) == spy.g5.get(k) and not spy.g5.is_live(k) for k in session)   # ...ours, not live
    spy._quiet = (None, 900.0)                                    # (worked out again, as the next day)
    assert spy.quiet_limit_s() == 900.0                           # still 15 minutes


def test_a_gap_before_the_first_trade_counts_as_recovered_once_the_feed_is_back():
    """A gap opened before the ETF's first trade of the run: once its feed covers a session step again
    (the stream is back, no trade yet), the back-fill's back-off is cut short, as after any outage."""
    inst = Instrument("alpaca:UUP", "alpaca", "UUP", "us_regular")
    t0 = ny_ts(2026, 11, 24, 10, 0)
    inst.seed("g5", [(t0 - 600, 27.0), (t0 - 300, 27.1)], t0)
    inst.feed_alive(t0 - 60)
    inst.roll(t0 + 1)
    inst.roll(t0 + 1201)                                          # 20 minutes the feed did not cover
    assert inst.holes["g1"] == t0 and not inst.recovered
    for t in range(int(t0) + 1260, int(t0) + 1500, 60):
        inst.feed_alive(t)                                        # the stream is back: other ETFs' messages
        inst.roll(t + 1)
    assert inst.recovered and not inst.live_seen


def test_a_step_closed_while_the_back_fill_downloaded_is_the_next_attempts_gap():
    """Before its first live price, a step after an unfilled gap is empty (it would carry the price from
    before the gap). One the roll closed while the back-fill's download ran, after the download's end,
    stayed empty for good once the answer cleared the gap (a thin pair in the lifecycle harness, after a
    wake): it is the next attempt's gap now, and that attempt fills it."""
    inst = Instrument("binance_spot:XUSDT", "binance_spot", "XUSDT", "24/7")
    t0 = 1795521600.0                                             # a 5-minute boundary
    inst.seed("g1", [(t0 - 600 + 60 * j, 10.0) for j in range(10)], t0)
    inst.seed("g5", [(t0 - 600, 10.0), (t0 - 300, 10.0)], t0)
    inst.feed_alive(t0 - 1)
    inst.roll(t0 + 1)
    inst.roll(t0 + 601)                                           # asleep for 10 minutes
    assert inst.holes["g1"] == t0
    asked = t0 + 650                                              # awake: the back-fill asks at 10:50
    inst.feed_alive(asked)
    inst.roll(asked)
    inst.feed_alive(t0 + 660)                                     # its download still running at 11:00
    inst.roll(t0 + 661)
    k = int((t0 + 600) // 60)
    assert inst.g1.get(k) != inst.g1.get(k)                       # after the gap: empty
    bars = [(t0 - 600 + 60 * j, 10.0 + 0.01 * j) for j in range(21)]   # (the bar at 10:00 still open)
    inst.seed("g1", [b for b in bars if b[0] < asked], asked)
    assert inst.holes["g1"] == t0 + 600 and not inst.live_seen    # the 10:00 minute: next attempt's
    inst.seed("g1", bars, t0 + 670)                               # the next attempt
    assert inst.holes["g1"] is None and inst.g1.get(k) == 10.2
    keyless = Instrument("alpaca:TLT", "alpaca", "TLT", "us_regular")   # no feed (no key): carried, no gap
    t1 = ny_ts(2026, 11, 24, 11, 0)
    keyless.holes["g1"] = t1
    keyless.g1.put(int((t1 + 600) // 60), NaN, False)
    keyless.seed("g1", [(t1 - 600 + 60 * j, 88.0) for j in range(20)], t1 + 650)
    assert keyless.holes["g1"] is None


def test_a_gap_up_to_the_close_is_not_recovered_by_the_shut_hours_after_it():
    """Shut steps are not checked against the feed (a quiet stream is normal then), so they say nothing
    about it: a gap up to the close stays open over the night, and only a session step the feed covers
    again counts as the feed being back (a shut step would cut the back-fill's back-off short with the
    network still down)."""
    inst = Instrument("alpaca:UUP", "alpaca", "UUP", "us_regular")
    t0 = ny_ts(2026, 11, 24, 15, 40)
    inst.seed("g5", [(t0 - 600, 27.0), (t0 - 300, 27.1)], t0)
    inst.feed_alive(t0 - 60)
    inst.roll(t0 + 1)
    inst.roll(t0 + 1801)                                          # the feed lost from 15:40 to past the close
    assert inst.holes["g1"] == t0 and not inst.recovered
    opened = ny_ts(2026, 11, 25, 9, 30)
    inst.roll(opened - 59)                                        # the night
    assert not inst.recovered
    inst.feed_alive(opened + 5)
    inst.roll(opened + 61)
    assert inst.recovered


def test_the_close_after_a_session_minute_the_feed_missed_is_unknown_until_a_download_covers_it():
    """Out from 15:30 to 16:20 (a sleep or an outage over the close): the session minutes are gaps, and
    the shut minutes after them are unknown, not 600 carried (the price from before the gap), until a
    download covers the gap: it fills them with the session's close, also the minute the roll closed
    while it ran, after its end. An answer with no bar for the ETF leaves them unknown."""
    t0, back = ny_ts(2026, 11, 24, 15, 30), ny_ts(2026, 11, 24, 16, 20)
    night = range(int((t0 + 1800) // G1_STEP), int(back // G1_STEP))
    for answer in ("bars", "none"):
        inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
        inst.seed("g1", [(t0 - 600 + 60 * j, 600.0) for j in range(10)], t0)
        inst.on_price(600.0, t0 - 1, t0 - 1)                          # traded; its feed heard till 15:29:59
        inst.roll(back)
        assert inst.holes["g1"] == t0 and all(inst.g1.get(k) != inst.g1.get(k) for k in night)
        asked = back + 30
        inst.feed_alive(back + 60)
        inst.roll(back + 61)                                          # the download still running
        bars = [(t0 - 600 + 60 * j, 601.0) for j in range(40)] if answer == "bars" else []
        inst.seed("g1", bars, asked, since=t0 - 600)
        after = range(int(back // G1_STEP), int((back + 61) // G1_STEP))
        if answer == "bars":
            assert all(inst.g1.get(k) == 601.0 and not inst.g1.is_live(k) for k in list(night) + list(after))
            inst.roll(back + 121)
            assert inst.g1.get(int((back + 60) // G1_STEP)) == 601.0             # current again
        else:
            assert all(inst.g1.get(k) != inst.g1.get(k) for k in list(night) + list(after))
            inst.roll(back + 121)
            assert inst.g1.get(int((back + 60) // G1_STEP)) != 600.0             # still unknown


def test_a_minute_closed_while_a_download_ran_after_a_weekend_wake_holds_the_close():
    """Asleep from Thursday 15:00 to Saturday 23:16: the 1-minute download (a day long) has no session
    in it, so it covers no gap of its own, and the minute the roll closed while it ran, unknown (after a
    session minute the feed missed), stayed so for good. It takes the close the download ends on."""
    thu, wake = ny_ts(2026, 11, 19, 15, 0), ny_ts(2026, 11, 21, 23, 16)
    inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    inst.seed("g5", [(thu - 600, 600.0), (thu - 300, 600.0)], thu)
    inst.seed("g1", [(thu - 600 + 60 * j, 600.0) for j in range(10)], thu)
    inst.on_price(600.0, thu - 1, thu - 1)
    inst.roll(wake)
    assert inst.holes["g1"] is None and inst.holes["g5"] == thu
    asked = wake + 15
    inst.feed_alive(wake + 60)
    inst.roll(wake + 61)                                          # the download still running
    k = int(wake // G1_STEP)
    assert inst.g1.get(k) != inst.g1.get(k)
    bars = [(t, 601.0) for t in range(int(thu), int(ny_ts(2026, 11, 20, 16, 0)), int(G5_STEP))
            if clock.market_open("us_regular", t + G5_STEP - 1)]
    inst.seed("g5", bars, asked, since=thu - 600)
    inst.seed("g1", [], asked, since=asked - 86400 + 60)
    assert inst.g1.get(k) == 601.0 and inst.g1.get(k - 60) == 601.0 and inst.price == 601.0


def test_a_session_print_after_a_missed_minute_makes_the_price_current_again():
    """A session minute the feed missed leaves the price stale (the close after it unknown) until a
    download covers the gap or the ETF prints again: out from 15:00 to 15:10, then printing till the
    close, the night carries that close as ever (the missed minutes stay a gap, for the back-fill)."""
    t0 = ny_ts(2026, 11, 24, 15, 0)
    inst = Instrument("alpaca:SPY", "alpaca", "SPY", "us_regular")
    inst.seed("g1", [(t0 - 600 + 60 * j, 600.0) for j in range(10)], t0)
    inst.on_price(600.0, t0 - 1, t0 - 1)
    inst.roll(t0 + 600)
    assert inst.holes["g1"] == t0
    for t in range(int(t0) + 600, int(t0) + 4200, 30):           # back at 15:10: prints till the close
        if clock.market_open("us_regular", t):
            inst.on_price(601.0, t, t)
        inst.feed_alive(t)
        inst.roll(t + 1)
    night = range(int((t0 + 3600) // G1_STEP), int((t0 + 4170) // G1_STEP))
    assert all(inst.g1.get(k) == 601.0 and not inst.g1.is_live(k) for k in night)
    assert inst.holes["g1"] == t0
