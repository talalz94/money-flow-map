"""Statistics on the price grids: window returns, volatility (σ), relative volume, correlation.

Timeframes (all assets share the same clock instants, so moves are comparable):
  1h     the last 60 minutes (rolling)
  today  since the global trading-day roll, 17:00 New York (= the US close for US-listed assets)
  1w     the last 7 days (rolling)

σ is the typical size of a move over the SAME kind of window, from the asset's own recent history,
on COMPARABLE days only: the asset's trading days (clock.CALENDARS: every day for crypto, weekdays
for FX and CME futures, NYSE days for US stocks and the perpetuals on them; a quiet weekend would
otherwise drag an equity's σ down and make every weekday move look bigger than it is):
  1h     1-hour moves ending at the same clock time (±30 min) on the previous 20 trading days
         (the last 10 days for crypto, so its yardstick follows the current regime): pre-open
         hours against pre-open hours, the opening hour against opening hours. An hour belongs to
         the session of its midpoint (16:00-17:00 is the closing session's, not the next one's).
         Fallback for a recent listing: every live 60-minute move of 10 days
  today  the move from each previous trading day's roll to the SAME clock time ("same-clock"),
         20 trading days (30 for crypto); fallback when < 8 such days exist: daily realized
         variance scaled by elapsed time
  1w     daily (roll-to-roll) moves on the trading days of the last 30 days × √(the trading
         sessions in the window: 5 in a normal week, 4 in Thanksgiving week, 7 for crypto)
Every σ is robust to ONE one-off: the single day whose moves are more than twice any other day's
(an earnings gap, a crash day) counts as that next-largest day; a stock with several big days keeps
them. The notable-stock screen (stocks.py) uses the same functions on hourly bars.
z = log return / σ  (signed: + inflow / bid, − outflow / offered). RMS is used rather than the
standard deviation (the mean of short-horizon returns is ~0 and estimating it adds noise).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from . import clock
from .series import G5_STEP, Instrument, isnum

NaN = float("nan")
TIMEFRAMES = ("1h", "today", "1w")
TF_LABELS = {"1h": "1H", "today": "TODAY", "1w": "1W"}


@dataclass
class Window:
    tf: str
    start: float
    end: float
    label: str


def window_for(tf: str, now: float) -> Window:
    import datetime as dt
    if tf == "1h":
        return Window(tf, now - 3600, now, "last 60 minutes")
    if tf == "today":
        r = clock.day_roll(now)
        d = dt.datetime.fromtimestamp(r, clock.NY)
        return Window(tf, r, now, f"since 17:00 New York {d:%a %d %b}")
    if tf == "1w":
        return Window(tf, now - 7 * 86400, now, "last 7 days")
    raise ValueError(f"unknown timeframe {tf!r}")


def price_at(inst: Instrument, ts: float, now: float) -> float:
    """The instrument's price as of `ts` (close of the last step ending at or before `ts`)."""
    if now - ts <= 23.5 * 3600:
        v = close_at(inst.g1, inst.hours, ts, 10)
        if isnum(v):
            return v
    return close_at(inst.g5, inst.hours, ts, 6)


def close_at(g, hours: str, t: float, max_back: int) -> float:
    """g.asof(t), except inside the first step of a session (its market shut in the step before):
    that step's own close. The step before holds the shut market's last close, so a read from it
    left the opening move out: an ETF's TODAY σ read 0 for the first 5 minutes of every session (each
    past day's move from the roll to the same minute was 0), its 1W change reached back to the
    week-old close before that day's opening gap, and from 10:30:00 to 10:31 its 1H change took in
    the night's gap (the first 1-minute step)."""
    k = int(t // g.step)
    if t % g.step and clock.market_open(hours, t) and not clock.market_open(hours, k * g.step - 1):
        v = g.get(k)
        if isnum(v):
            return v
    return g.asof(t, max_back=max_back)


def traded_in_window(inst: Instrument, w: Window, now: float) -> bool:
    """Was the instrument's market open at any point of the window? (A market closed for the whole
    window carries no information for it: its z-score is then reported as missing, not as 0.)"""
    if clock.market_open(inst.hours, now):
        return True
    g = inst.g1 if w.tf == "1h" else inst.g5
    k0, k1 = g.k_of(w.start), g.k_of(now)
    step = max(1, (k1 - k0) // 400)
    for k in range(k1, k0 - 1, -step):
        if g.is_live(k):
            return True
    return False


def first_data_ts(inst: Instrument) -> float | None:
    """Start of the oldest 5-minute step holding a price (when an asset's history begins)."""
    g = inst.g5
    if g.last_k is None:
        return None
    for k in range(g.first_k(), g.last_k + 1):
        v = g.get(k)
        if v == v:
            return k * g.step
    return None


def window_return(inst: Instrument, w: Window, now: float) -> tuple[float, float, float]:
    """(log return, percent change, reference price) from the window start to the live price."""
    p1 = inst.price
    p0 = price_at(inst, w.start, now)
    if not (isnum(p0) and isnum(p1) and p0 > 0 and p1 > 0):
        return NaN, NaN, p0
    return math.log(p1 / p0), (p1 / p0 - 1.0) * 100.0, p0


def _rms(xs: list[float]) -> float:
    return math.sqrt(sum(x * x for x in xs) / len(xs)) if xs else NaN


# ── the shared same-clock estimator (map nodes and the notable-stock screen) ────────────────────
SAME_CLOCK_DAYS = {"all": 30, "weekdays": 20, "us_equity": 20}   # previous comparable days sampled
# 1H: crypto keeps a short lookback so its yardstick follows the current regime (it trades every
# day: 10 days already give 50 samples); exchange-traded assets use 20 trading days, like the screen
SAME_CLOCK_DAYS_1H = {"all": 10, "weekdays": 20, "us_equity": 20}
HOUR_OFFSETS = (-1800, -900, 0, 900, 1800)    # 1-hour windows ending up to 30 min either side of now
MAX_BACK_DAYS = 34                            # the 5-minute history holds 35 days
DAILY_DAYS = 30                               # 1W: the daily moves of the last 30 days (screen and nodes)
ONE_OFF_RATIO = 2.0
TODAY_MIN_WINDOW_S = 3600.0   # in the first hour after the roll, TODAY is judged against a 1-hour move


def robust_rms(days) -> float:
    """RMS of samples grouped by day (a plain list of numbers counts each as its own day), robust to
    ONE one-off: the single day whose moves are more than twice the size of any other day's (an
    earnings gap, a crash day) counts as that next-largest day. A −18% gap among ±1.4% days once
    more than doubled a stock's σ (3.5% instead of 1.4%) and showed a −2.5σ move as −1.1σ. A stock
    with several big days keeps them: that is its real volatility, not an accident."""
    gs = [[x for x in (g if isinstance(g, (list, tuple)) else [g]) if isnum(x)] for g in days]
    gs = [g for g in gs if g]
    if not gs:
        return NaN
    ms = [sum(x * x for x in g) / len(g) for g in gs]          # mean square per day
    if len(gs) >= 5:
        order = sorted(range(len(ms)), key=ms.__getitem__)
        top, second = ms[order[-1]], ms[order[-2]]
        if second > 0 and top > ONE_OFF_RATIO ** 2 * second:
            ms[order[-1]] = second
    n = [len(g) for g in gs]
    return math.sqrt(sum(m * k for m, k in zip(ms, n)) / sum(n))


def comparable_ends(now: float, calendar: str, days: int, max_back: int = MAX_BACK_DAYS,
                    span: float = 0.0) -> list[float]:
    """The same New York clock time on the previous `days` comparable days (see clock.CALENDARS). A
    window of `span` seconds ending then belongs to the session of its MIDPOINT: the hour 16:00-17:00
    is Friday's closing hour, not Saturday's, and Sunday's 16:00-17:00 is a weekend hour, not Monday's."""
    out, n = [], 0
    while len(out) < days and n < max_back:
        n += 1
        t = clock.same_clock_before(now, n)
        if clock.trading_day(calendar, t - span / 2):
            out.append(t)
    return out


def same_clock_hour_returns(price_at, now: float, calendar: str, days: int, offsets=HOUR_OFFSETS,
                            live=None) -> list[list[float]]:
    """1-hour log returns ending at the same clock time (± `offsets`), grouped by previous comparable
    day: the hour before the US open is judged against pre-open hours, the first hour of trading
    against first hours, and a quiet weekend never counts for an exchange-traded asset."""
    out = []
    for t_end in comparable_ends(now, calendar, days, span=3600.0):
        rs = []
        for off in offsets:
            t1 = t_end + off
            t0 = t1 - 3600
            if live is not None and not live(t0, t1):
                continue                     # its market was shut then (e.g. an ETF overnight)
            a, b = price_at(t1), price_at(t0)
            if isnum(a) and isnum(b) and a > 0 and b > 0:
                rs.append(math.log(a / b))
        if rs:
            out.append(rs)
    return out


def same_clock_today_returns(price_at, now: float, calendar: str, days: int, live=None) -> list[float]:
    """Moves from each previous comparable day's roll (17:00 New York) to the same clock time."""
    roll = clock.day_roll(now)
    # floor at one hour: right after the roll every same-clock window on previous days would be
    # shorter than one 5-minute step and read as a zero move (σ = 0)
    off = max(now - roll, TODAY_MIN_WINDOW_S)
    rs = []
    for n in range(1, MAX_BACK_DAYS + 1):
        if len(rs) >= days:
            break
        r0 = clock.roll_n_days_before(roll, n)
        if not clock.trading_day(calendar, r0 + 1):
            continue
        t1 = r0 + off
        if live is not None and not live(r0, t1 + G5_STEP):
            continue
        a, b = price_at(t1), price_at(r0)
        if isnum(a) and isnum(b) and a > 0 and b > 0:
            rs.append(math.log(a / b))
    return rs


def roll_daily_returns(price_at, now: float, calendar: str, max_days: int = MAX_BACK_DAYS) -> list[float]:
    """Roll-to-roll (17:00 New York) log returns of the comparable days among the last `max_days`."""
    roll = clock.day_roll(now)
    out = []
    for n in range(1, max_days + 1):
        t1 = clock.roll_n_days_before(roll, n - 1)
        t0 = clock.roll_n_days_before(roll, n)
        if not clock.trading_day(calendar, t0 + 1):
            continue
        a, b = price_at(t1), price_at(t0)
        if isnum(a) and isnum(b) and a > 0 and b > 0:
            out.append(math.log(a / b))
    return out


def days_per_week(calendar: str) -> int:
    return 7 if calendar == "all" else 5


def sessions_in(calendar: str, t0: float, t1: float) -> float:
    """Trading sessions (17:00-to-17:00 New York days of `calendar`) inside [t0, t1], the partial ones
    at either end by the share of their 24 hours inside. A normal week holds 5 for an exchange, 7 for
    crypto; Thanksgiving week holds 4, so a week's σ is √4 days, not √5 (z was 11% too small)."""
    if calendar == "all":
        return (t1 - t0) / 86400.0
    total, r = 0.0, clock.day_roll(t0)
    while r < t1:
        nxt = clock.day_roll(r + 90000.0)               # the next roll (a DST day lasts 23 or 25 hours)
        inside = min(nxt, t1) - max(r, t0)
        if inside > 0 and clock.trading_day(calendar, r + 1):
            total += inside / (nxt - r)
        r = nxt
    return total


def week_k(calendar: str, now: float) -> float:
    """The number of daily moves a 1W window adds up (√k scales the daily σ), rounded to 0.1."""
    return round(sessions_in(calendar, now - 7 * 86400.0, now), 1) or float(days_per_week(calendar))


def sigma_1h(inst: Instrument, now: float, min_n: int = 30) -> tuple[float, int, str]:
    g = inst.g5
    cal = getattr(inst, "calendar", "all")
    days = same_clock_hour_returns(lambda t: g.asof(t, max_back=2), now, cal, SAME_CLOCK_DAYS_1H.get(cal, 20),
                                   live=lambda a, b: _any_live(g, a, b))
    n = sum(len(d) for d in days)
    if n >= min_n:
        return robust_rms(days), n, "same-clock hour"
    # too little history (a recent listing): every live 60-minute return of the last 10 days
    k_now = g.k_of(now) - 1
    n12 = 3600 // G5_STEP
    rs = []
    for k in range(k_now - 10 * 288, k_now + 1):
        if g.is_live(k) and g.is_live(k - n12 + 1):
            a, b = g.get(k), g.get(k - n12)
            if isnum(a) and isnum(b) and a > 0 and b > 0:
                rs.append(math.log(a / b))
    if len(rs) < 60:
        return NaN, len(rs), "insufficient history"
    return _rms(rs), len(rs), "rolling 60-min returns, 10 days"


def _any_live(g, t0: float, t1: float) -> bool:
    # newest first: an open market is found within a step or two; only fully closed windows scan all
    for k in range(int(t1 // g.step) - 1, int(t0 // g.step) - 1, -1):
        if g.is_live(k):
            return True
    return False


def daily_returns(inst: Instrument, now: float, days: int = DAILY_DAYS) -> list[float]:
    """Roll-to-roll log returns of the instrument's comparable days among the last `days`."""
    g = inst.g5
    return roll_daily_returns(lambda t: g.asof(t, max_back=24), now, getattr(inst, "calendar", "all"), days)


def sigma_daily(inst: Instrument, now: float, min_n: int = 10) -> tuple[float, int]:
    rs = daily_returns(inst, now)
    if len(rs) < min_n:
        return NaN, len(rs)
    return robust_rms(rs), len(rs)


def sigma_today(inst: Instrument, now: float, days: int | None = None, min_n: int = 8) -> tuple[float, int, str]:
    g = inst.g5
    cal = getattr(inst, "calendar", "all")
    rs = same_clock_today_returns(lambda t: close_at(g, inst.hours, t, 24), now, cal,
                                  days or SAME_CLOCK_DAYS.get(cal, 20), live=lambda a, b: _any_live(g, a, b))
    if len(rs) >= min_n:
        return robust_rms(rs), len(rs), "same-clock"
    # fallback: realized variance of 5-minute returns, scaled to the elapsed part of the day
    var_day = realized_var_per_day(inst, now)
    if not isnum(var_day):
        return NaN, len(rs), "insufficient history"
    off = max(now - clock.day_roll(now), TODAY_MIN_WINDOW_S)
    frac = max(off / 86400.0, 1.0 / 24.0)
    return math.sqrt(var_day * frac), len(rs), "realized-variance fallback"


def realized_var_per_day(inst: Instrument, now: float, max_days: int = 20) -> float:
    """Sum of squared 5-minute log returns per calendar day with data (the variance a day usually
    accumulates). Used when an asset is too new for return-based σ (e.g. a fresh listing)."""
    g = inst.g5
    k_now = g.k_of(now) - 1
    sq, live_steps, first_valid = 0.0, 0, None
    k0 = max(g.first_k() or k_now, k_now - max_days * 288)
    for k in range(k0 + 1, k_now + 1):
        if g.is_live(k):
            a, b = g.get(k), g.get(k - 1)
            if isnum(a) and isnum(b) and a > 0 and b > 0:
                sq += math.log(a / b) ** 2
                live_steps += 1
                if first_valid is None:
                    first_valid = k
    days_cov = 0.0 if first_valid is None else (k_now - first_valid + 1) / 288.0   # calendar days with data
    if live_steps < 100 or days_cov < 1:
        return NaN
    return sq / days_cov


def sigma_1w(inst: Instrument, now: float) -> tuple[float, int, str]:
    rs = daily_returns(inst, now)
    k = week_k(getattr(inst, "calendar", "all"), now)      # trading days in the window: 5, 4 in a holiday week
    if len(rs) >= 14:
        return robust_rms(rs) * math.sqrt(k), len(rs), f"daily RMS × √{k:g}"
    var_day = realized_var_per_day(inst, now)
    days = len(rs)
    if isnum(var_day) and days >= 2:
        return math.sqrt(var_day * 7), days, "realized-variance fallback"
    return NaN, days, "insufficient history"


def sigma_for(tf: str, inst: Instrument, now: float) -> tuple[float, int, str]:
    if tf == "1h":
        return sigma_1h(inst, now)
    if tf == "today":
        return sigma_today(inst, now)
    if tf == "1w":
        return sigma_1w(inst, now)
    raise ValueError(tf)


# ── volume ──────────────────────────────────────────────────────────────────────────────────────
def _vol_sum(inst: Instrument, t0: float, t1: float) -> tuple[float, float, int]:
    """Quote volume and taker-buy quote volume over 5-minute steps fully inside [t0, t1)."""
    v = inst.v5
    q = tb = 0.0
    n = 0
    tb_ok = True
    for k in range(int(math.ceil(t0 / G5_STEP)), int(t1 // G5_STEP)):
        a, b = v.get(k)
        if isnum(a):
            q += a
            n += 1
            if isnum(b):
                tb += b
            else:
                tb_ok = False
    return q, (tb if tb_ok else NaN), n


def volume_read(inst: Instrument, w: Window, now: float) -> dict | None:
    """Relative volume against the same window on previous days, and the taker (aggressor) split.
    net taker flow = taker-buy quote volume - taker-sell quote volume (where the venue reports it)."""
    if inst.v5 is None or inst.v5.last_k is None:
        return None
    q, tb, n = _vol_sum(inst, w.start, now)
    if n == 0:
        return None
    span = now - w.start
    past = []
    if w.tf == "1w":
        for wk in range(1, 5):
            a, _, m = _vol_sum(inst, w.start - wk * 7 * 86400, now - wk * 7 * 86400)
            if m >= n * 0.8:
                past.append(a)
    else:                                  # the same window on previous comparable (trading) days
        cal = getattr(inst, "calendar", "all")
        for d in range(1, MAX_BACK_DAYS + 1):
            if len(past) >= 20:
                break
            t0 = clock.same_clock_before(w.start, d) if w.tf == "1h" else clock.roll_n_days_before(clock.day_roll(now), d)
            if not clock.trading_day(cal, t0 + (span / 2 if w.tf == "1h" else 1)):   # midpoint / the session
                continue
            a, _, m = _vol_sum(inst, t0, t0 + span)
            if m >= max(1, n * 0.8):
                past.append(a)
    past = [p for p in past if p > 0]
    base = sorted(past)[len(past) // 2] if len(past) >= 3 else NaN
    out = {"quote_volume": q, "rvol": (q / base) if isnum(base) and base > 0 else None,
           "baseline_days": len(past)}
    if isnum(tb) and q > 0:
        out["taker_buy_ratio"] = tb / q
        out["net_taker"] = tb - (q - tb)
    return out


# ── co-movement ─────────────────────────────────────────────────────────────────────────────────
def step_returns(inst: Instrument, w: Window, now: float) -> dict[int, float]:
    """Log returns per step inside the window: 1-minute steps (1h), 5-minute (today), hourly (1w)."""
    if w.tf == "1h":
        g, stride = inst.g1, 1
    elif w.tf == "today":
        g, stride = inst.g5, 1
    else:
        g, stride = inst.g5, 3600 // G5_STEP
    k1 = g.k_of(now) - 1
    k0 = g.k_of(w.start)
    out = {}
    k = k1
    while k - stride >= k0 - 1:
        a, b = g.get(k), g.get(k - stride)
        if isnum(a) and isnum(b) and a > 0 and b > 0 and g.is_live(k):
            out[k] = math.log(a / b)
        k -= stride
    return out


def corr(a: dict[int, float], b: dict[int, float], min_n: int = 8) -> float | None:
    ks = a.keys() & b.keys()
    if len(ks) < min_n:
        return None
    xs = [a[k] for k in ks]
    ys = [b[k] for k in ks]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return max(-1.0, min(1.0, sxy / math.sqrt(sxx * syy)))


def sparkline(inst: Instrument, w: Window, now: float, points: int = 40) -> list[float]:
    """Percent change from the window start, downsampled to about `points` values."""
    if w.tf == "1h":
        g = inst.g1
    else:
        g = inst.g5
    k0, k1 = g.k_of(w.start), g.k_of(now) - 1
    if k1 <= k0:
        return []
    stride = max(1, (k1 - k0) // points)
    base = price_at(inst, w.start, now)
    if not isnum(base) or base <= 0:
        return []
    out = []
    for k in range(k0, k1 + 1, stride):
        v = g.get(k)
        out.append(round((v / base - 1) * 100, 3) if isnum(v) else None)
    if isnum(inst.price):
        out.append(round((inst.price / base - 1) * 100, 3))
    return out
