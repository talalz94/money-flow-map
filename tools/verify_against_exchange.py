"""Recompute the service's numbers independently from raw exchange data and compare.

    python tools/verify_against_exchange.py [--url http://127.0.0.1:8765]   (the service must be running)

For BTC (Binance spot), Nasdaq 100 (QQQUSDT index) and gold (XAUUSDT index) it downloads 35 days of
5-minute klines straight from Binance's REST API and checks:
  1. the service's stored closes match the exchange's, step by step (no flat gaps after restarts/sleep)
  2. σ for 1H / TODAY / 1W recomputed from the raw klines with the documented formulas (README 3.2),
     written here independently of stats.py: trading days only, the same clock hour (±30 min, grouped
     by day) for 1H, same-clock for TODAY, daily moves × √(sessions in the window) for 1W, and the
     single one-off day capped
  3. the window % changes and z-scores
  4. the correlation behind the strongest edge (when both ends are Binance series)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import statistics
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from moneyflow import clock  # noqa: E402

BASE = "http://127.0.0.1:8765"
SERIES = {"btc": ("spot", "BTCUSDT", "last"), "ndx": ("fut", "QQQUSDT", "index"), "gold": ("fut", "XAUUSDT", "index"),
          "spx": ("fut", "SPYUSDT", "index"), "eth": ("spot", "ETHUSDT", "last"), "oil": ("fut", "CLUSDT", "index")}
CALENDAR = {"btc": "all", "eth": "all", "ndx": "us_equity", "spx": "us_equity", "gold": "weekdays", "oil": "weekdays"}
DAYS_TODAY = {"all": 30, "weekdays": 20, "us_equity": 20}
DAYS_1H = {"all": 10, "weekdays": 20, "us_equity": 20}
FAILS = []


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (moneyflow verify)"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def klines(market, symbol, field, start_ms, end_ms):
    out = {}
    t = start_ms
    while t < end_ms:
        if market == "spot":
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&startTime={t}&endTime={end_ms}&limit=1000"
        elif field == "index":
            url = f"https://fapi.binance.com/fapi/v1/indexPriceKlines?pair={symbol}&interval=5m&startTime={t}&endTime={end_ms}&limit=1500"
        else:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&startTime={t}&endTime={end_ms}&limit=1500"
        rows = get(url)
        if not rows:
            break
        for r in rows:
            out[int(r[0] / 1000 // 300)] = float(r[4])         # step index -> close
        t = rows[-1][0] + 300_000
        time.sleep(0.15)
    return out


def check(ok, name, detail=""):
    print(f"[{'ok  ' if ok else 'FAIL'}] {name}{'' if ok else ' - ' + detail}")
    if not ok:
        FAILS.append(name)


def rms(xs):
    return math.sqrt(sum(x * x for x in xs) / len(xs))


def robust(groups):
    """RMS over days (each a list of samples), the single day more than twice the size of any other
    counting as the next-largest (README 3.2)."""
    ms = [sum(x * x for x in g) / len(g) for g in groups if g]
    n = [len(g) for g in groups if g]
    if len(ms) >= 5:
        order = sorted(range(len(ms)), key=ms.__getitem__)
        if ms[order[-2]] > 0 and ms[order[-1]] > 4 * ms[order[-2]]:
            ms[order[-1]] = ms[order[-2]]
    return math.sqrt(sum(m * k for m, k in zip(ms, n)) / sum(n))


NY = clock.NY


def trades(calendar, ts):
    """A trading day by the session's date: sessions end at 17:00 New York (Sunday evening is Monday)."""
    if calendar == "all":
        return True
    d = (dt.datetime.fromtimestamp(ts, NY) + dt.timedelta(hours=7)).date()
    if d.weekday() >= 5:
        return False
    return calendar == "weekdays" or d not in clock.nyse_holidays(d.year)


def wall_back(ts, days):
    return (dt.datetime.fromtimestamp(ts, NY) - dt.timedelta(days=days)).timestamp()


def sessions_in(calendar, t0, t1):
    if calendar == "all":
        return (t1 - t0) / 86400
    total, r = 0.0, clock.day_roll(t0)
    while r < t1:
        nxt = clock.day_roll(r + 90000)
        inside = min(nxt, t1) - max(r, t0)
        if inside > 0 and trades(calendar, r + 1):
            total += inside / (nxt - r)
        r = nxt
    return round(total, 1)


def main():
    now = time.time()
    k_now = int(now // 300)
    data = {}
    for sid, (mk, sym, field) in SERIES.items():
        hist = get(f"{BASE}/api/history/{sid}?grid=5m&hours=840")
        svc = {int(t // 300) - 1: v for t, v, _ in hist["closes"]}          # rows are step END times
        ref = klines(mk, sym, field, (k_now - 35 * 288) * 300_000, (k_now - 1) * 300_000)
        data[sid] = (svc, ref)
        common = sorted(set(svc) & set(ref) - {max(ref)})                   # the newest kline may still move
        diffs = [abs(svc[k] / ref[k] - 1) * 100 for k in common]
        recent = [k for k in common if k >= k_now - 36]                     # the last 3 hours (restart gap)
        bad = [k for k in common if abs(svc[k] / ref[k] - 1) * 100 > 0.05]
        print(f"\n{sid} ({sym} {field}): {len(common)} common 5-min steps, median diff "
              f"{statistics.median(diffs):.5f}%, max {max(diffs):.4f}%, steps off by >0.05%: {len(bad)}")
        check(len(common) > 0.9 * min(len(svc), len(ref)), f"{sid}: stored history covers the exchange's steps",
              f"{len(common)} of {len(ref)}")
        check(len(bad) <= max(2, len(common) // 2000), f"{sid}: stored closes match the exchange",
              f"{len(bad)} steps differ, e.g. {[(clock.iso(k * 300), svc[k], ref[k]) for k in bad[:3]]}")
        check(all(abs(svc[k] / ref[k] - 1) < 0.0005 for k in recent), f"{sid}: last 3 hours match (no flat gap)")

        # σ and z recomputed from the raw klines (documented formulas, stats.py)
        def P(ts, back=3):
            k = int(ts // 300) - 1
            for j in range(k, k - back - 1, -1):
                if j in ref:
                    return ref[j]
            return None

        cal = CALENDAR[sid]
        n = get(f"{BASE}/api/nodes/{sid}?tf=today")
        live_px = n["price"]
        roll = clock.day_roll(now)
        off = max(now - roll, 3600.0)
        rs = []
        for d in range(1, 35):
            r0 = clock.roll_n_days_before(roll, d)
            if not trades(cal, r0 + 1):
                continue
            a, b = P(r0 + off, 24), P(r0, 24)
            if a and b:
                rs.append([math.log(a / b)])
            if len(rs) >= DAYS_TODAY[cal]:
                break
        sig_today = robust(rs)
        r_today = math.log(live_px / P(roll, 24))
        exp_sig_pct = (math.exp(sig_today) - 1) * 100
        print(f"  TODAY: σ service {n['sigma_pct']:.4f}% vs recomputed {exp_sig_pct:.4f}% ({len(rs)} days, {cal}); "
              f"z service {n['z']} vs recomputed {r_today / sig_today:.3f}; change service {n['change_pct']:.4f}% vs "
              f"{(live_px / P(roll, 24) - 1) * 100:.4f}%")
        check(abs(n["sigma_pct"] / exp_sig_pct - 1) < 0.03, f"{sid}: TODAY σ matches (within 3%)")
        check(n["z"] is not None and abs(n["z"] - r_today / sig_today) < 0.1 + 0.03 * abs(n["z"]), f"{sid}: TODAY z matches")
        n1 = get(f"{BASE}/api/nodes/{sid}?tf=1h")
        days1 = []
        for d in range(1, 35):
            t_end = wall_back(now, d)
            if not trades(cal, t_end - 1800):               # an hour belongs to the session of its midpoint
                continue
            g = []
            for o in (-1800, -900, 0, 900, 1800):
                a, b = P(t_end + o, 2), P(t_end + o - 3600, 2)
                if a and b:
                    g.append(math.log(a / b))
            if g:
                days1.append(g)
            if len(days1) >= DAYS_1H[cal]:
                break
        s1 = (math.exp(robust(days1)) - 1) * 100
        print(f"  1H: σ service {n1['sigma_pct']:.4f}% vs recomputed {s1:.4f}% ({len(days1)} days, same clock hour)")
        check(abs(n1["sigma_pct"] / s1 - 1) < 0.05, f"{sid}: 1H σ matches (within 5%)")
        nw = get(f"{BASE}/api/nodes/{sid}?tf=1w")
        dr = []
        for d in range(1, 31):
            t0 = clock.roll_n_days_before(roll, d)
            if not trades(cal, t0 + 1):
                continue
            a, b = P(clock.roll_n_days_before(roll, d - 1), 24), P(t0, 24)
            if a and b:
                dr.append([math.log(a / b)])
        k = sessions_in(cal, now - 7 * 86400, now)
        sw = (math.exp(robust(dr) * math.sqrt(k)) - 1) * 100
        print(f"  1W: σ service {nw['sigma_pct']:.4f}% vs recomputed {sw:.4f}% ({len(dr)} days × √{k:g})")
        check(abs(nw["sigma_pct"] / sw - 1) < 0.03, f"{sid}: 1W σ matches (within 3%)")

    # the strongest TODAY edge between two Binance-sourced series
    snap = get(f"{BASE}/api/snapshot?tf=today")
    for e in snap["edges"]:
        if e["from"] in data and e["to"] in data and e.get("corr") is not None:
            roll = clock.day_roll(now)
            ra, rb = data[e["from"]][1], data[e["to"]][1]
            ks = [k for k in range(int(roll // 300) + 1, k_now - 1) if k in ra and k - 1 in ra and k in rb and k - 1 in rb]
            xa = [math.log(ra[k] / ra[k - 1]) for k in ks]
            xb = [math.log(rb[k] / rb[k - 1]) for k in ks]
            c = statistics.correlation(xa, xb)
            print(f"\nedge {e['from']} -> {e['to']}: corr service {e['corr']} vs recomputed {c:.3f} ({len(ks)} steps)")
            check(abs(e["corr"] - c) < 0.1, f"edge {e['from']}->{e['to']}: correlation matches (within 0.1)")
            w = math.sqrt(abs(e["from_z"]) * e["to_z"]) * (1 - e["corr"]) / 2
            check(abs(w - e["weight"]) < 0.01, "edge weight follows the documented formula")
            break
    print(f"\n{'ALL MATCH' if not FAILS else str(len(FAILS)) + ' MISMATCHES: ' + ', '.join(FAILS)}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    for s in (sys.stdout,):
        try:
            s.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=BASE, help="the running service (default %(default)s)")
    BASE = ap.parse_args().url.rstrip("/")
    raise SystemExit(main())
