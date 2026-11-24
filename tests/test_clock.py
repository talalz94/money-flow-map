import datetime as dt

from conftest import ny_ts
from moneyflow import clock


def test_day_roll_is_17h_new_york_and_dst_safe():
    # 10:00 New York on Mon 2 Nov 2026 (EST) -> roll = Sun 1 Nov 17:00 EST = 22:00 UTC
    r = clock.day_roll(ny_ts(2026, 11, 2, 10))
    assert dt.datetime.fromtimestamp(r, dt.timezone.utc) == dt.datetime(2026, 11, 1, 22, 0, tzinfo=dt.timezone.utc)
    # Sat 31 Oct 2026 18:00 (EDT) -> roll = 31 Oct 17:00 EDT = 21:00 UTC
    r = clock.day_roll(ny_ts(2026, 10, 31, 18))
    assert dt.datetime.fromtimestamp(r, dt.timezone.utc) == dt.datetime(2026, 10, 31, 21, 0, tzinfo=dt.timezone.utc)
    # exactly at the roll
    assert clock.day_roll(ny_ts(2026, 11, 24, 17)) == ny_ts(2026, 11, 24, 17)
    # n days before, across the DST change
    assert clock.roll_n_days_before(ny_ts(2026, 11, 2, 17), 2) == ny_ts(2026, 10, 31, 17)


def test_us_equity_sessions_holidays_and_early_close():
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 10)) == "regular"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 8)) == "pre"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 17)) == "post"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 21)) == "closed"
    assert clock.us_equity_session(ny_ts(2026, 11, 26, 11)) == "closed"      # Thanksgiving
    assert clock.us_equity_session(ny_ts(2026, 11, 27, 12, 59)) == "regular"  # early close day
    assert clock.us_equity_session(ny_ts(2026, 11, 27, 13, 30)) == "post"
    assert clock.us_equity_session(ny_ts(2026, 11, 28, 11)) == "closed"      # Saturday
    assert dt.date(2026, 7, 3) in clock.nyse_holidays(2026)                    # 4 July on a Saturday
    assert dt.date(2027, 12, 24) in clock.nyse_holidays(2027)                  # Christmas on a Saturday
    assert dt.date(2025, 1, 9) in clock.nyse_holidays(2025)                    # unscheduled closure
    assert dt.date(2026, 4, 3) in clock.nyse_holidays(2026)                    # Good Friday


def test_fx_and_vix_hours():
    assert not clock.fx_open(ny_ts(2026, 11, 28, 12))     # Saturday
    assert not clock.fx_open(ny_ts(2026, 11, 29, 16))     # Sunday before 17:00
    assert clock.fx_open(ny_ts(2026, 11, 29, 17, 5))
    assert not clock.fx_open(ny_ts(2026, 11, 27, 17, 1))  # Friday after 17:00
    assert clock.vix_calc_hours(ny_ts(2026, 11, 24, 4))
    assert not clock.vix_calc_hours(ny_ts(2026, 11, 24, 9, 20))
    assert clock.vix_calc_hours(ny_ts(2026, 11, 24, 16, 10))
    assert clock.market_open("24/7", ny_ts(2026, 11, 28, 3))


def test_parse_iso_nanoseconds_and_offsets():
    assert abs(clock.parse_iso("2026-11-24T13:19:06.823262736Z") - clock.parse_iso("2026-11-24T13:19:06.823262Z")) < 1e-6
    assert abs(clock.parse_iso("2026-11-24T08:19:06-05:00") - clock.parse_iso("2026-11-24T13:19:06Z")) < 1e-6
    # the fast path and the general parser agree
    fast = clock.parse_iso("2026-11-24T13:19:06.5Z")
    slow = clock.parse_iso("2026-11-24T13:19:06.500+00:00")
    assert abs(fast - slow) < 1e-6 and fast == 1795526346.5


def test_fixed_clock_for_replay():
    c = clock.Clock(fixed=1000.0)
    assert c.now() == 1000.0
    c.advance(5)
    assert c.now() == 1005.0
