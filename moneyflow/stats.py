"""Statistics on the price grids: window returns, volatility (σ), relative volume, correlation.

Timeframes (all assets share the same clock instants, so moves are comparable):
  1h     the last 60 minutes (rolling)
  today  since the global trading-day roll, 17:00 New York (= the US close for US-listed assets)
  1w     the last 7 days (rolling)

σ is the typical size of a move over the SAME kind of window, from the asset's own recent history:
  1h     RMS of rolling 60-minute log returns over the last 10 days, open-market windows only
  today  RMS, over the previous 30 days, of the move from that day's roll to the SAME clock time
         ("same-clock" σ: at 10:00 New York, compare with 10:00 on previous days, so overnight gaps
         and the intraday volatility pattern are handled for every asset class)
         fallback when < 8 such days exist: daily realized variance scaled by elapsed time
  1w     RMS of daily (roll-to-roll) log returns over calendar days × √7
z = log return / σ  (signed: + inflow / bid, − outflow / offered). RMS is used rather than the
standard deviation (the mean of short-horizon returns is ~0 and estimating it adds noise).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from . import clock
from .series import G1_STEP, G5_STEP, Instrument, isnum

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
        v = inst.g1.asof(ts, max_back=10)
        if isnum(v):
            return v
    return inst.g5.asof(ts, max_back=6)


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


def sigma_1h(inst: Instrument, now: float, days: int = 10, min_n: int = 60) -> tuple[float, int]:
    g = inst.g5
    k_now = g.k_of(now) - 1
    n12 = 3600 // G5_STEP
    rs = []
    for k in range(k_now - days * 288, k_now + 1):
        if g.is_live(k) and g.is_live(k - n12 + 1):
            a, b = g.get(k), g.get(k - n12)
            if isnum(a) and isnum(b) and a > 0 and b > 0:
                rs.append(math.log(a / b))
    if len(rs) < min_n:
        return NaN, len(rs)
    return _rms(rs), len(rs)


def _any_live(g, t0: float, t1: float) -> bool:
    # newest first: an open market is found within a step or two; only fully closed windows scan all
    for k in range(int(t1 // g.step) - 1, int(t0 // g.step) - 1, -1):
        if g.is_live(k):
            return True
    return False


def daily_returns(inst: Instrument, now: float, days: int = 34) -> list[float]:
    """Roll-to-roll log returns for the last `days` calendar days (closed days give 0)."""
    g = inst.g5
    roll = clock.day_roll(now)
    out = []
    for n in range(1, days + 1):
        t1 = clock.roll_n_days_before(roll, n - 1)
        t0 = clock.roll_n_days_before(roll, n)
        a, b = g.asof(t1, max_back=24), g.asof(t0, max_back=24)
        if isnum(a) and isnum(b) and a > 0 and b > 0:
            out.append(math.log(a / b))
    return out


def sigma_daily(inst: Instrument, now: float, min_n: int = 10) -> tuple[float, int]:
    rs = daily_returns(inst, now)
    if len(rs) < min_n:
        return NaN, len(rs)
    return _rms(rs), len(rs)


def sigma_today(inst: Instrument, now: float, days: int = 30, min_n: int = 8) -> tuple[float, int, str]:
    g = inst.g5
    roll = clock.day_roll(now)
    off = now - roll
    rs = []
    for n in range(1, days + 1):
        r0 = clock.roll_n_days_before(roll, n)
        t1 = r0 + off
        if not _any_live(g, r0, t1 + G5_STEP):
            continue
        a, b = g.asof(t1, max_back=3), g.asof(r0, max_back=24)
        if isnum(a) and isnum(b) and a > 0 and b > 0:
            rs.append(math.log(a / b))
    if len(rs) >= min_n:
        return _rms(rs), len(rs), "same-clock"
    # fallback: realized variance of 5-minute returns, scaled to the elapsed part of the day
    var_day = realized_var_per_day(inst, now)
    if not isnum(var_day):
        return NaN, len(rs), "insufficient history"
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
    if len(rs) >= 14:
        return _rms(rs) * math.sqrt(7), len(rs), "daily RMS × √7"
    var_day = realized_var_per_day(inst, now)
    days = len(rs)
    if isnum(var_day) and days >= 2:
        return math.sqrt(var_day * 7), days, "realized-variance fallback"
    return NaN, days, "insufficient history"


def sigma_for(tf: str, inst: Instrument, now: float) -> tuple[float, int, str]:
    if tf == "1h":
        s, n = sigma_1h(inst, now)
        return s, n, "rolling 60-min returns, 10 days"
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
    else:
        for d in range(1, 21):
            t0 = w.start - d * 86400 if w.tf == "1h" else clock.roll_n_days_before(clock.day_roll(now), d)
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
