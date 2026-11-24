"""Recompute the service's numbers independently from raw exchange data and compare.

    python tools/verify_against_exchange.py            (the service must be running)

For BTC (Binance spot), Nasdaq 100 (QQQUSDT index) and gold (XAUUSDT index) it downloads 35 days of
5-minute klines straight from Binance's REST API and checks:
  1. the service's stored closes match the exchange's, step by step (no flat gaps after restarts/sleep)
  2. σ for 1H / TODAY / 1W recomputed from the raw klines with the documented formulas
  3. the window % changes and z-scores
  4. the correlation behind the strongest edge (when both ends are Binance series)
"""
from __future__ import annotations

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

        n = get(f"{BASE}/api/nodes/{sid}?tf=today")
        live_px = n["price"]
        roll = clock.day_roll(now)
        off = now - roll
        rs = []
        for d in range(1, 31):
            r0 = clock.roll_n_days_before(roll, d)
            a, b = P(r0 + off), P(r0, 24)
            if a and b:
                rs.append(math.log(a / b))
        sig_today = rms(rs)
        r_today = math.log(live_px / P(roll, 24))
        exp_sig_pct = (math.exp(sig_today) - 1) * 100
        print(f"  TODAY: σ service {n['sigma_pct']:.4f}% vs recomputed {exp_sig_pct:.4f}% ({len(rs)} days); "
              f"z service {n['z']} vs recomputed {r_today / sig_today:.3f}; change service {n['change_pct']:.4f}% vs "
              f"{(live_px / P(roll, 24) - 1) * 100:.4f}%")
        check(abs(n["sigma_pct"] / exp_sig_pct - 1) < 0.03, f"{sid}: TODAY σ matches (within 3%)")
        check(n["z"] is not None and abs(n["z"] - r_today / sig_today) < 0.1 + 0.03 * abs(n["z"]), f"{sid}: TODAY z matches")
        n1 = get(f"{BASE}/api/nodes/{sid}?tf=1h")
        r1 = [math.log(ref[k] / ref[k - 12]) for k in range(k_now - 1 - 10 * 288, k_now) if k in ref and k - 12 in ref]
        s1 = (math.exp(rms(r1)) - 1) * 100
        print(f"  1H: σ service {n1['sigma_pct']:.4f}% vs recomputed {s1:.4f}% ({len(r1)} windows)")
        check(abs(n1["sigma_pct"] / s1 - 1) < 0.05, f"{sid}: 1H σ matches (within 5%)")
        nw = get(f"{BASE}/api/nodes/{sid}?tf=1w")
        dr = []
        for d in range(1, 35):
            a, b = P(clock.roll_n_days_before(roll, d - 1), 24), P(clock.roll_n_days_before(roll, d), 24)
            if a and b:
                dr.append(math.log(a / b))
        sw = (math.exp(rms(dr) * math.sqrt(7)) - 1) * 100
        print(f"  1W: σ service {nw['sigma_pct']:.4f}% vs recomputed {sw:.4f}% ({len(dr)} days)")
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
    raise SystemExit(main())
