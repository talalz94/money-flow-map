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


def test_sigma_1h_is_rms_of_rolling_hour_returns():
    inst = drifting(NOW)
    s, n = stats.sigma_1h(inst, NOW)
    assert s == pytest.approx(12 * A, rel=1e-9) and n > 2000


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
