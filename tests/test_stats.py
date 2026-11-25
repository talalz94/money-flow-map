import math

import pytest

from conftest import ny_ts
from moneyflow import clock, stats
from moneyflow.series import G5_STEP, Instrument

A = 0.0005          # log drift per 5-minute step: makes every σ estimate exact


def drifting(now, days=35, hours="24/7", volume=False):
    """A 24/7 instrument whose log price rises by A every 5 minutes (exact, deterministic)."""
    inst = Instrument("t:D", "t", "D", hours, volume=volume)
    k_now = int(now // G5_STEP)
    k0 = k_now - int(days * 288)
    closes = [(k * G5_STEP, 100.0 * math.exp(A * (k - k0))) for k in range(k0, k_now)]
    vols = [(k * G5_STEP, 1000.0, 600.0) for k in range(k0, k_now)] if volume else None
    inst.seed("g5", closes, now, vols)
    inst.price = closes[-1][1]
    inst.ts = now
    return inst


NOW = ny_ts(2026, 11, 24, 10, 0)            # on a 5-minute boundary, 17 h after the roll


def test_sigma_1h_is_rms_of_same_clock_hour_returns():
    inst = drifting(NOW)
    s, n, method = stats.sigma_1h(inst, NOW)
    # crypto calendar: the same clock hour (±30 min in 15-min steps) on each of the previous 10 days
    assert s == pytest.approx(12 * A, rel=1e-9) and n == 10 * 5 and method == "same-clock hour"


def _shaped(now, calendar, hourly_move, days=34):
    """An instrument whose log price moves by hourly_move(t) in each 5-minute step (per hour)."""
    inst = Instrument("t:S", "t", "S", "24/7")
    inst.calendar = calendar
    k_now = int(now // G5_STEP)
    lp, closes = 0.0, []
    for k in range(k_now - days * 288, k_now):
        lp += hourly_move((k + 1) * G5_STEP) / 12
        closes.append((k * G5_STEP, 100.0 * math.exp(lp)))
    inst.seed("g5", closes, now)
    inst.price = closes[-1][1]
    return inst


def test_quiet_weekends_do_not_dilute_an_exchange_traded_assets_sigma():
    """A perpetual on the S&P trades all weekend but barely moves: its σ must come from the days the
    S&P trades, or every weekday move looks bigger than it is (seen: SPX TODAY σ 0.45% vs 0.5-0.6%)."""
    def move(t):              # a steady 0.1% an hour on NYSE trading days, nothing at weekends
        return 0.001 if clock.trading_day("us_equity", t) else 0.0
    eq = _shaped(NOW, "us_equity", move)
    s1, n1, _ = stats.sigma_1h(eq, NOW)
    assert s1 == pytest.approx(0.001, rel=0.05)             # every sample a 0.1% hour, no weekend zeros
    every_day = _shaped(NOW, "all", move)                   # the same prices read as a 24/7 asset
    s_all, _, _ = stats.sigma_1h(every_day, NOW)
    assert s_all < 0.9 * s1                                 # weekends pulled the old yardstick down
    st, nt, _ = stats.sigma_today(eq, NOW)
    assert nt == 20 and st > stats.sigma_today(every_day, NOW)[0]


def test_one_off_day_does_not_dominate_sigma_but_real_volatility_stays():
    """A −18% earnings gap among ±1% days once tripled a stock's σ (−2.9σ moves showed as −1.1σ).
    The one day more than twice the size of any other now counts as the next-largest day; a stock
    with several big days keeps them (that is its volatility, seen live with ZS)."""
    gap = [0.01 if i % 2 else -0.01 for i in range(19)] + [-0.18]
    assert stats._rms(gap) == pytest.approx(math.sqrt((19 * 1e-4 + 0.0324) / 20))      # 4.1%: the gap rules
    assert stats.robust_rms(gap) == pytest.approx(0.01)                                # the gap counts as a 1% day
    volatile = [0.01 if i % 2 else -0.01 for i in range(16)] + [0.08, -0.10, 0.11, -0.08]
    assert stats.robust_rms(volatile) == pytest.approx(stats._rms(volatile))          # 11% is not 2x 10%: kept
    assert stats.robust_rms([0.01, -0.01] * 10) == pytest.approx(0.01)                 # ordinary days: untouched
    # per day for overlapping samples: one event seen by five 1-hour windows is still one day
    days = [[0.002] * 5 for _ in range(19)] + [[0.03, 0.025, 0.02, 0.03, 0.028]]
    assert stats.robust_rms(days) == pytest.approx(0.002)


def test_same_clock_hour_tells_a_quiet_pre_open_from_the_opening_hour():
    """Pre-open hours are judged against pre-open hours, the opening hour against opening hours."""
    def move(t):
        import datetime as _dt
        h = _dt.datetime.fromtimestamp(t, clock.NY).hour
        size = 0.004 if 9 <= h < 11 else 0.0005             # busy around the open, quiet otherwise
        return size if int(t // 3600) % 2 else -size
    inst = _shaped(NOW, "us_equity", move)
    pre, _, _ = stats.sigma_1h(inst, ny_ts(2026, 11, 24, 7, 0))
    opening, _, _ = stats.sigma_1h(inst, ny_ts(2026, 11, 24, 10, 0))
    assert opening > 4 * pre


def test_sigma_today_same_clock_and_z_of_one():
    inst = drifting(NOW)
    s, n, method = stats.sigma_today(inst, NOW)
    steps = (NOW - clock.day_roll(NOW)) / G5_STEP
    assert method == "same-clock" and n == 30
    assert s == pytest.approx(steps * A, rel=1e-6)
    w = stats.window_for("today", NOW)
    r, pct, p0 = stats.window_return(inst, w, NOW)
    assert r == pytest.approx(steps * A, rel=1e-9)
    assert r / s == pytest.approx(1.0, rel=1e-6)                                # a perfectly typical day


def test_sigma_today_right_after_the_roll_is_not_zero():
    """30 s after 17:00 New York every same-clock window would be shorter than one 5-minute step:
    σ must not collapse to 0 (it once did, blanking TODAY's z-scores for 5 minutes every day)."""
    roll = clock.day_roll(NOW)
    just_after = roll + 30
    inst = drifting(just_after)
    s, n, method = stats.sigma_today(inst, just_after)
    s_hour, _, _ = stats.sigma_today(inst, roll + 3600)
    assert s > 0 and method == "same-clock"
    assert s == pytest.approx(12 * A, rel=1e-6)            # one hour of drift: the floored yardstick
    assert s_hour == pytest.approx(s, rel=1e-6)


def test_sigma_1w_from_daily_returns():
    inst = drifting(NOW)
    s, n, method = stats.sigma_1w(inst, NOW)
    # one of the 34 days is the 25-hour day when US clocks fell back (1 Nov 2026)
    assert s == pytest.approx(288 * A * math.sqrt(7), rel=0.01) and n >= 30 and method == "daily RMS × √7"


def test_sigma_1w_new_listing_uses_realized_variance():
    inst = drifting(NOW, days=7)                 # e.g. a stock that listed a week ago
    s, n, method = stats.sigma_1w(inst, NOW)
    assert method == "realized-variance fallback" and n < 14
    assert s == pytest.approx(math.sqrt(288 * A * A * 7), rel=0.02)


def test_realized_variance_fallback_for_short_history():
    inst = drifting(NOW, days=5)
    s, n, method = stats.sigma_today(inst, NOW)
    assert method == "realized-variance fallback"
    frac = (NOW - clock.day_roll(NOW)) / 86400
    assert s == pytest.approx(math.sqrt(288 * A * A * frac), rel=0.05)


def test_insufficient_history_gives_nan():
    inst = drifting(NOW, days=0.5)
    s, n, method = stats.sigma_1w(inst, NOW)
    assert s != s and method == "insufficient history"


def test_traded_in_window_for_closed_markets():
    inst = Instrument("a:HYG", "a", "HYG", "us_regular")
    sat = ny_ts(2026, 11, 28, 12, 0)
    closes = [(k * G5_STEP, 80.0) for k in range(int((sat - 3 * 86400) // G5_STEP), int(sat // G5_STEP))]
    inst.seed("g5", closes, sat)
    assert not stats.traded_in_window(inst, stats.window_for("1h", sat), sat)    # Saturday hour: no session
    assert stats.traded_in_window(inst, stats.window_for("1w", sat), sat)        # the week had sessions


def test_correlation():
    a = {k: (1.0 if k % 2 else -1.0) for k in range(20)}
    b = {k: -v for k, v in a.items()}
    assert stats.corr(a, b) == pytest.approx(-1.0)
    assert stats.corr(a, a) == pytest.approx(1.0)
    assert stats.corr(a, {k: 0.0 for k in range(20)}) is None                    # no variance
    assert stats.corr({1: 1.0}, {1: 2.0}) is None                                # too few points


def test_volume_read_relative_volume_and_taker_flow():
    inst = drifting(NOW, volume=True)
    w = stats.window_for("today", NOW)
    v = stats.volume_read(inst, w, NOW)
    assert v["rvol"] == pytest.approx(1.0)
    assert v["taker_buy_ratio"] == pytest.approx(0.6)
    assert v["net_taker"] == pytest.approx(0.2 * v["quote_volume"])


def test_sparkline_is_percent_from_window_start():
    inst = drifting(NOW)
    w = stats.window_for("today", NOW)
    sp = stats.sparkline(inst, w, NOW)
    _, pct, _ = stats.window_return(inst, w, NOW)
    assert 30 <= len(sp) <= 80
    assert abs(sp[0]) < 0.1 and sp[-1] == pytest.approx(pct, abs=1e-3)
