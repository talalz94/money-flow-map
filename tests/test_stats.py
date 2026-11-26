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


# ── calendars, outliers and grouping, through the real estimators (kills the mutants that survived a2f3856) ──
import datetime as _dt                                         # noqa: E402

HOLIDAYS = {_dt.date(2026, 11, 26)}                            # Thanksgiving: the only NYSE holiday in the window


def _session_of(t):
    """The test's own calendar (independent of clock.py): the session ends at 17:00 New York."""
    return (_dt.datetime.fromtimestamp(t, clock.NY) + _dt.timedelta(hours=7)).date()


def _trades(t):
    d = _session_of(t)
    return d.weekday() < 5 and d not in HOLIDAYS


def _sessions(now, per_session, days=34, spike=None):
    """A perpetual on a US stock: on each NYSE session it moves `per_session` evenly (1/288 of it every
    5 minutes); weekends and holidays are flat. `spike`: (session date, factor) for one one-off day."""
    def move(t):
        t -= G5_STEP / 2                                            # the step's midpoint: 16:55-17:00 is the old session's
        if not _trades(t):
            return 0.0
        f = spike[1] if spike and _session_of(t) == spike[0] else 1.0
        return per_session * f / 24                                 # per hour
    return _shaped(now, "us_equity", move, days=days)


def test_week_sigma_counts_the_sessions_in_the_window():
    """1W adds up the daily moves of the trading sessions in the window: 5 in a normal week, 4 in
    Thanksgiving week (√4, not √5: z was 11% too small), 7 for crypto; trading days only (M2, M5)."""
    m = 0.01
    normal = ny_ts(2026, 11, 18, 9, 0)                          # Wed, the week before Thanksgiving
    assert stats.sessions_in("us_equity", normal - 7 * 86400, normal) == pytest.approx(5.0)
    s, n, method = stats.sigma_1w(_sessions(normal, m), normal)
    assert s == pytest.approx(m * math.sqrt(5), rel=1e-6) and method == "daily RMS × √5"
    thanks = ny_ts(2026, 12, 2, 9, 0)                           # Wed after: Thu 26 Nov was shut
    assert stats.sessions_in("us_equity", thanks - 7 * 86400, thanks) == pytest.approx(4.0)
    assert stats.sessions_in("weekdays", thanks - 7 * 86400, thanks) == pytest.approx(5.0)   # gold trades it
    assert stats.sessions_in("all", thanks - 7 * 86400, thanks) == pytest.approx(7.0)
    s, n, method = stats.sigma_1w(_sessions(thanks, m), thanks)
    assert s == pytest.approx(m * 2.0, rel=1e-6) and method == "daily RMS × √4"


def test_today_and_week_sigma_cap_a_one_off_day():
    """The one-off cap works through the real TODAY and 1W estimators, not only in robust_rms (M9)."""
    m, now = 0.01, ny_ts(2026, 11, 24, 9, 0)                   # 16 h into the session
    plain = _sessions(now, m)
    spiky = _sessions(now, m, spike=(_dt.date(2026, 11, 18), 12.0))   # one 12x day (an earnings gap)
    t_plain, t_spiky = stats.sigma_today(plain, now)[0], stats.sigma_today(spiky, now)[0]
    assert t_plain == pytest.approx(m * 16 / 24, rel=1e-6) and t_spiky == pytest.approx(t_plain, rel=1e-6)
    w_plain, w_spiky = stats.sigma_1w(plain, now)[0], stats.sigma_1w(spiky, now)[0]
    assert w_spiky == pytest.approx(w_plain, rel=1e-6)


def test_one_off_day_seen_by_all_five_hour_windows_is_one_day():
    """1H samples are grouped by day: one day's event, seen by all five windows around the clock time,
    is capped as ONE day (M10); flattened, five big samples would outvote the cap."""
    m, now = 0.012, ny_ts(2026, 11, 24, 10, 0)
    plain = stats.sigma_1h(_sessions(now, m), now)[0]
    assert plain == pytest.approx(m / 24, rel=1e-6)               # every hour of a session: m/24
    spiky = stats.sigma_1h(_sessions(now, m, spike=(_dt.date(2026, 11, 18), 12.0)), now)[0]
    assert spiky == pytest.approx(plain, rel=1e-6)


def test_week_and_today_ignore_a_holiday_and_the_weekend():
    """Thanksgiving and weekends are not samples: a flat holiday would otherwise pull σ down (M2, M5)."""
    m, now = 0.01, ny_ts(2026, 12, 3, 9, 0)                    # Thu after Thanksgiving week
    inst = _sessions(now, m)
    assert stats.sigma_today(inst, now)[0] == pytest.approx(m * 16 / 24, rel=1e-6)
    assert stats.sigma_1w(inst, now)[0] == pytest.approx(m * math.sqrt(stats.week_k("us_equity", now)), rel=1e-6)
    assert all(abs(r) == pytest.approx(m) for r in stats.daily_returns(inst, now))   # no zero days


def test_an_hour_belongs_to_the_session_of_its_midpoint():
    """At 17:00 New York the hour 16:00-17:00 is the closing session's: Friday's counts, Sunday's does
    not (by its END, 17:00 is already the next session: Sunday counted, Friday dropped, σ ~10% low)."""
    mon = ny_ts(2026, 11, 23, 17, 0)
    ends = stats.comparable_ends(mon, "us_equity", 20, span=3600)
    days = {_dt.datetime.fromtimestamp(t, clock.NY).date() for t in ends}
    assert _dt.date(2026, 11, 20) in days and _dt.date(2026, 11, 22) not in days     # Fri in, Sun out
    by_end = {_dt.datetime.fromtimestamp(t, clock.NY).date() for t in stats.comparable_ends(mon, "us_equity", 20)}
    assert _dt.date(2026, 11, 22) in by_end and _dt.date(2026, 11, 20) not in by_end


def test_the_first_five_minutes_of_a_session_read_the_opening_step():
    """At 9:32 each past day's same-clock move and the 1W start fall inside the session's first
    5-minute step. The step before holds the shut market's close, so reading it left the opening move
    out: an ETF's TODAY σ was exactly 0 (z missing, the risk meter without its credit input) and 1W
    reached back past the week-old opening gap. They read the opening step's own close."""
    hyg = Instrument("a:HYG", "a", "HYG", "us_regular")
    now = ny_ts(2026, 11, 23, 9, 32)
    t, rows, day0 = int((now - 35 * 86400) // 300) * 300, [], ny_ts(2026, 10, 1)
    while t + 300 <= now:
        if clock.market_open("us_regular", t + 299):
            rows.append((t, 80.0 + 0.3 * (int((t - day0) // 86400) % 5)))   # each session at a new level
        t += 300
    hyg.seed("g5", rows, now, since=now - 35 * 86400)
    sig, n, how = stats.sigma_today(hyg, now)
    assert how == "same-clock" and n >= 8 and sig > 0
    week = now - 7 * 86400
    assert stats.price_at(hyg, week, now) == hyg.g5.get(int(week // 300)) != hyg.g5.get(int(week // 300) - 1)


def test_an_hour_after_the_open_1h_starts_at_the_opening_minute():
    """From 10:30:00 to 10:31 1H starts inside the session's first 1-minute step. The step before holds
    the shut market's close, so for that minute of every session 1H took in the night's opening gap
    (an ETF that opened 1% up and stayed there read +1.0%, true 0). At 10:30:00 exactly the start is
    before the first print: the gap is in."""
    spy = Instrument("a:SPY", "a", "SPY", "us_regular")
    opened = ny_ts(2026, 11, 23, 9, 30)
    now = opened + 3600
    for k in range(int(now // 60) - 1439, int(now // 60)):
        live = clock.market_open("us_regular", (k + 1) * 60 - 1)
        spy.g1.put(k, 101.0 if live else 100.0, live)
    spy.price, spy.ts = 101.0, now
    assert stats.window_return(spy, stats.window_for("1h", now), now)[1] == pytest.approx(1.0)
    for t in (now + 15, now + 30, now + 45, now + 60):
        assert stats.window_return(spy, stats.window_for("1h", t), t)[1] == 0.0, t
