"""Every place a calendar or a clock rule enters σ and the volume baselines, on the map's nodes and
on the stock screen, each checked against a value computed here from first principles (each was a
change the other tests let through): NYSE trading days, an hour belonging to the session of its
midpoint, DST, the 1H live filter, the screen's TODAY stop at the roll, √(sessions) and the screen's
1H read of the node's five window ends."""
import datetime as dt
import math

import pytest

from conftest import ny_ts
from moneyflow import clock, stats, stocks
from moneyflow.series import G5_STEP, Instrument

H, D = 3600.0, 86400.0
HOLIDAYS = {dt.date(2026, 11, 26)}             # Thanksgiving: the only NYSE holiday in these windows


def session(t: float) -> dt.date:
    """The session a moment belongs to, by this file's own rule: sessions end at 17:00 New York."""
    return (dt.datetime.fromtimestamp(t, clock.NY) + dt.timedelta(hours=7)).date()


def trades(t: float) -> bool:
    d = session(t)
    return d.weekday() < 5 and d not in HOLIDAYS


def parity(t: float) -> int:
    return session(t).toordinal() % 2


# ── builders: a log price that moves `rate(t)` per hour, sampled at steps or at whole hours ───────
def node(now: float, rate, hours: str = "24/7", calendar: str = "us_equity", vol=None, days: int = 34) -> Instrument:
    inst = Instrument("t:S", "t", "S", hours, volume=vol is not None)
    inst.calendar = calendar
    k_now = int(now // G5_STEP)
    lp, closes, vols = 0.0, [], []
    for k in range(k_now - days * 288, k_now):
        mid = k * G5_STEP + G5_STEP / 2                       # a step belongs to the session of its midpoint
        lp += rate(mid) / 12
        closes.append((k * G5_STEP, 100.0 * math.exp(lp)))
        if vol is not None:
            vols.append((k * G5_STEP, vol(mid), float("nan")))
    inst.seed("g5", closes, now, vols or None)
    inst.price = closes[-1][1]
    return inst


def bars(now: float, rate, vol=lambda t: 1000.0, days: int = 31) -> stocks.Hourly:
    """Hourly bars of the same kind of path (close at the bar's end, the bar's quote volume)."""
    e = math.floor(now / H) * H
    lp, rows = 0.0, []
    for k in range(int(days * D / H)):
        t = e - days * D + k * H
        lp += sum(rate(t + (i + 0.5) * 300) / 12 for i in range(12))
        rows.append((t, 100.0 * math.exp(lp), sum(vol(t + (i + 0.5) * 300) for i in range(12)) / 12))
    h = stocks.Hourly()
    h.merge(rows)
    return h


def steady(h_per_hour: float):
    return lambda t: h_per_hour if trades(t) else 0.0


TUE_10 = ny_ts(2026, 11, 24, 10, 0)
MON_17 = ny_ts(2026, 11, 23, 17, 0)


# ── the node ─────────────────────────────────────────────────────────────────────────────────────
def test_node_1h_hours_belong_to_the_session_of_their_midpoint_inside_sigma():
    """At 17:00 Monday the five windows around the clock time are judged by their day's 16:00-17:00
    hour: Friday's counts, Sunday's does not. Friday's two later windows reach into the quiet
    Saturday session (0.75 and 0.5 of an hour's move), the other days' do not."""
    h = 0.001
    inst = node(MON_17, steady(h))
    days, n = [], 0
    while len(days) < 20:
        n += 1
        t = clock.same_clock_before(MON_17, n)
        if trades(t - 1800):
            nxt = trades(t + 1)                               # the next session trades too
            days.append([h, h, h, h * (0.75 + 0.25 * nxt), h * (0.5 + 0.5 * nxt)])
    want = math.sqrt(sum(x * x for d in days for x in d) / sum(len(d) for d in days))
    assert stats.sigma_1h(inst, MON_17)[0] == pytest.approx(want, rel=1e-9)


def test_same_clock_is_the_same_wall_clock_time_across_dst():
    """DST ended on 1 Nov 2026: three days before Tuesday 3 Nov 10:00 is Saturday 31 Oct 10:00 New
    York, 25 hours earlier, not 72 hours."""
    t = ny_ts(2026, 11, 3, 10, 0)
    assert clock.same_clock_before(t, 3) == ny_ts(2026, 10, 31, 10, 0) == t - 3 * D - H


def test_node_1h_counts_only_windows_when_its_market_was_open():
    """An ETF moves only from 09:30 to 16:00. At 10:00 the window ending 09:30 was shut the whole
    hour: no sample (not a zero move); the others carry 1/4 to 4/4 of an hour's move."""
    h = 0.002
    rate = lambda t: h if trades(t) and clock.market_open("us_regular", t) else 0.0   # noqa: E731
    inst = node(TUE_10, rate, hours="us_regular")
    want = h * math.sqrt((0.25 ** 2 + 0.5 ** 2 + 0.75 ** 2 + 1.0) / 4)
    assert stats.sigma_1h(inst, TUE_10)[0] == pytest.approx(want, rel=1e-9)


def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def test_node_volume_baseline_is_the_same_hour_on_trading_days_by_its_midpoint():
    """Relative volume against the same hour on previous TRADING days, an hour belonging to the
    session of its midpoint: weekend hours (thin) and Sunday's 16:00-17:00 are not the baseline."""
    # volume grows a little every session, and weekend hours are (artificially) heavy: had any weekend
    # hour been counted, or any session swapped for another, the median would move
    vol = lambda t: 1000.0 + session(t).toordinal() % 1000 if trades(t) else 1e6   # noqa: E731
    inst = node(MON_17, steady(0.001), vol=vol)
    w = stats.window_for("1h", MON_17)
    base, n = [], 0
    while len(base) < 20:
        n += 1
        t0 = clock.same_clock_before(w.start, n)
        if trades(t0 + 1800):
            base.append(12 * vol(t0 + 1800))
    want = 12 * vol(MON_17 - 1800) / sorted(base)[len(base) // 2]        # the node's median: the upper one
    assert stats.volume_read(inst, w, MON_17)["rvol"] == pytest.approx(want, rel=1e-9)


# ── the screen ───────────────────────────────────────────────────────────────────────────────────
def test_screen_sigma_on_trading_days_only_in_1h_and_today():
    """A steady move on NYSE sessions, nothing at weekends: the screen's 1H σ is that hour's move and
    TODAY's the move since 17:00, with no quiet weekend day in either."""
    h = 0.001
    hb = bars(TUE_10, steady(h))
    px = hb.price_at(TUE_10)
    s1 = stocks.screen_one(hb, px, TUE_10, "1h", ring=[(TUE_10 - H, hb.price_at(TUE_10 - H))])
    st = stocks.screen_one(hb, px, TUE_10, "today")
    assert math.log1p(s1["sigma_pct"] / 100) == pytest.approx(h, rel=1e-9)
    assert math.log1p(st["sigma_pct"] / 100) == pytest.approx(17 * h, rel=1e-9)


def test_screen_1h_reads_the_nodes_five_window_ends():
    """The node reads the five hours ending 09:30 to 10:30; the screen, on hourly bars, interpolates
    each between the whole hours around it. Here the 09:00-10:00 hour moves 3x the others."""
    h = 0.001
    rate = lambda t: (3 * h if dt.datetime.fromtimestamp(t, clock.NY).hour == 9 else h) if trades(t) else 0.0  # noqa: E731
    hb = bars(TUE_10, rate)
    s = stocks.screen_one(hb, hb.price_at(TUE_10), TUE_10, "1h", ring=[(TUE_10 - H, hb.price_at(TUE_10 - H))])
    v9, v10, v11 = h * h, 9 * h * h, h * h                          # the hours ending 09:00, 10:00, 11:00
    ends = [0.5 * v9 + 0.5 * v10, 0.25 * v9 + 0.75 * v10, v10, 0.75 * v10 + 0.25 * v11, 0.5 * v10 + 0.5 * v11]
    assert math.log1p(s["sigma_pct"] / 100) == pytest.approx(math.sqrt(sum(ends) / 5), rel=1e-9)


def test_screen_today_never_interpolates_across_the_roll():
    """At 16:30 the next whole hour is 17:00, where TODAY restarts: σ stays the 16:00 one (23 hours of
    the session's move), never half-way to a new session's first hour."""
    h = 0.001
    t = ny_ts(2026, 11, 24, 16, 30)
    hb = bars(t, steady(h))
    s = stocks.screen_one(hb, hb.price_at(t), t, "today")
    assert math.log1p(s["sigma_pct"] / 100) == pytest.approx(23 * h, rel=1e-9)


def test_screen_week_scales_by_the_sessions_in_the_window():
    """Thanksgiving week holds 4 NYSE sessions: the screen's 1W σ is the daily σ x √4, like the node's."""
    h = 0.001
    t = ny_ts(2026, 12, 2, 9, 0)
    hb = bars(t, steady(h))
    s = stocks.screen_one(hb, hb.price_at(t), t, "1w")
    assert math.log1p(s["sigma_pct"] / 100) == pytest.approx(24 * h * 2.0, rel=1e-9)


def test_screen_volume_baselines_are_trading_days_by_midpoint():
    """The screen's 1H volume ratio compares the last whole hour with the same hour on previous NYSE
    trading days (by its midpoint), and TODAY's the volume since the roll with the same span on
    previous sessions: thin weekend hours are never the baseline."""
    vol = lambda t: (1000.0 if parity(t) else 2000.0) if trades(t) else 10.0   # noqa: E731
    hb = bars(MON_17, steady(0.001), vol=vol)
    s1 = stocks.screen_one(hb, hb.price_at(MON_17), MON_17, "1h", ring=[(MON_17 - H, hb.price_at(MON_17 - H))])
    base, n = [], 0
    while len(base) < 20:
        n += 1
        t_end = clock.same_clock_before(MON_17, n)
        if trades(t_end - 1800):
            base.append(vol(t_end - 1800))
    assert s1["rvol"] == pytest.approx(vol(MON_17 - 1800) / _median(base), rel=1e-9)
    t = TUE_10
    hb = bars(t, steady(0.001), vol=vol)
    st = stocks.screen_one(hb, hb.price_at(t), t, "today")
    roll, done = clock.day_roll(t), math.floor(t / H) * H - clock.day_roll(t)
    rolls = [r for r in (clock.roll_n_days_before(roll, n) for n in range(1, 35)) if trades(r + 1)][:20]
    want = vol(roll + 1) / _median([vol(r + 1) for r in rolls])   # one session's hours share a volume
    assert st["rvol"] == pytest.approx(want, rel=1e-9)
    assert done > H



# ── off the whole hour, TODAY's volume baseline, TODAY's live filter ──────────────────────────────
def test_the_midpoint_rule_holds_between_whole_hours_too():
    """At 17:20 Monday the hour 16:20-17:20 is mostly the closing session's: Sunday's (weekend) does
    not count and Friday's does, as at 17:00 (one second before the end is not the midpoint)."""
    mon = ny_ts(2026, 11, 23, 17, 20)
    days = {dt.datetime.fromtimestamp(t, clock.NY).date() for t in stats.comparable_ends(mon, "us_equity", 20, span=3600)}
    assert dt.date(2026, 11, 20) in days and dt.date(2026, 11, 22) not in days


HEAVY_WEEKEND = lambda t: 1000.0 + session(t).toordinal() % 1000 if trades(t) else 1e6   # noqa: E731


def test_node_today_volume_baseline_is_the_previous_trading_sessions():
    """TODAY's volume against the same span since 17:00 on previous TRADING sessions: each roll is
    judged by the session it starts (Sunday 17:00 starts Monday's), not the one it ends."""
    inst = node(TUE_10, steady(0.001), vol=HEAVY_WEEKEND)
    w = stats.window_for("today", TUE_10)
    steps = int((TUE_10 - w.start) // G5_STEP)
    base, n = [], 0
    while len(base) < 20:
        n += 1
        r0 = clock.roll_n_days_before(clock.day_roll(TUE_10), n)
        if trades(r0 + 1):
            base.append(sum(HEAVY_WEEKEND(r0 + (i + 0.5) * G5_STEP) for i in range(steps)))
    q = sum(HEAVY_WEEKEND(w.start + (i + 0.5) * G5_STEP) for i in range(steps))
    assert stats.volume_read(inst, w, TUE_10)["rvol"] == pytest.approx(q / sorted(base)[len(base) // 2], rel=1e-9)


def lopsided_weekend(t: float) -> float:
    """Weekend volume heavy on Sunday afternoon and light from Friday evening: Sunday's 16:45 hour
    lands above every weekday's and Friday's below, so taking in the wrong one moves the median."""
    if trades(t):
        return 1000.0 + session(t).toordinal() % 1000
    return 1e6 if dt.datetime.fromtimestamp(t, clock.NY).weekday() == 6 else 1.0


def test_node_hour_volume_baseline_by_the_hours_midpoint_off_the_hour():
    """At 17:45 the hour 16:45-17:45 is mostly the NEW session's: Sunday's counts (it is Monday's
    session from 17:00) and Friday's does not (Saturday's), judged by its midpoint, not by its start."""
    t = ny_ts(2026, 11, 23, 17, 45)
    inst = node(t, steady(0.001), vol=lopsided_weekend)
    w = stats.window_for("1h", t)
    base, n = [], 0
    while len(base) < 20:
        n += 1
        t0 = clock.same_clock_before(w.start, n)
        if trades(t0 + 1800):
            base.append(sum(lopsided_weekend(t0 + (i + 0.5) * G5_STEP) for i in range(12)))
    q = sum(lopsided_weekend(w.start + (i + 0.5) * G5_STEP) for i in range(12))
    assert stats.volume_read(inst, w, t)["rvol"] == pytest.approx(q / sorted(base)[len(base) // 2], rel=1e-9)


def test_today_sigma_skips_days_its_market_was_shut_for_the_whole_window():
    """At 08:00 an ETF has not traded since 16:00: every same-clock TODAY window was shut, so there is
    no sample (twenty zero moves would make σ 0); σ comes from the fallback instead."""
    t = ny_ts(2026, 11, 24, 8, 0)
    rate = lambda u: 0.002 if trades(u) and clock.market_open("us_regular", u) else 0.0   # noqa: E731
    inst = node(t, rate, hours="us_regular")
    s, n, method = stats.sigma_today(inst, t)
    assert method != "same-clock" and s > 0
