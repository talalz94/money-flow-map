"""Self-test of a RUNNING moneyflow service: endpoints, schema, statuses, streams, and data sanity
against independent sources. Prints PASS / WARN / FAIL per check; exit code 1 if anything FAILs.

    python tools/selftest.py            (service on http://127.0.0.1:8765)
    python tools/selftest.py --url http://127.0.0.1:8800 --no-external
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

RESULTS: list[tuple[str, str, str]] = []


def rec(level: str, name: str, detail: str = "") -> None:
    RESULTS.append((level, name, detail))
    sym = {"PASS": "ok  ", "WARN": "WARN", "FAIL": "FAIL"}[level]
    print(f"[{sym}] {name}{(' - ' + detail) if detail else ''}", flush=True)


def check(cond: bool, name: str, detail: str = "", warn_only: bool = False) -> bool:
    rec("PASS" if cond else ("WARN" if warn_only else "FAIL"), name, "" if cond else detail)
    return cond


def http(url: str, headers: dict | None = None, timeout: float = 15):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def jget(url: str, **kw):
    st, hd, body = http(url, **kw)
    return st, hd, (json.loads(body) if body and body[:1] in (b"{", b"[") else body)


def external(url: str, headers=None):
    try:
        st, _, body = http(url, headers={"User-Agent": "Mozilla/5.0 (moneyflow selftest)", **(headers or {})})
        return json.loads(body) if st == 200 else None
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8765")
    ap.add_argument("--no-external", action="store_true", help="skip comparisons with outside sources")
    a = ap.parse_args()
    base = a.url.rstrip("/")
    try:
        st, _, health = jget(base + "/api/health")
    except OSError as e:
        rec("FAIL", "service reachable", f"{base}: {e}. Start it with start.bat")
        return 1
    check(st == 200 and health.get("status") == "ok", "service reachable and healthy")
    hist = health.get("history", {})
    check(hist.get("state") == "done", "history loaded", f"state={hist.get('state')}")
    check(not hist.get("errors"), "history loaded without errors", "; ".join(hist.get("errors") or [])[:300], warn_only=True)
    bad = {k: v for k, v in (health.get("connectors") or {}).items() if v not in ("connected", "ok", "polling")}
    check(not bad, "every connector connected / ok", json.dumps(bad), warn_only=True)
    proc = health.get("process") or {}
    if proc.get("rss_mb") is not None:
        check(proc["rss_mb"] < 300, "memory below 300 MB", f"rss={proc['rss_mb']} MB")
    clk = health.get("clock") or {}
    check(clk.get("ntp_server") is not None, "clock synchronised by NTP", "no NTP sync (ages may be off by the PC clock error)", warn_only=True)

    # ── schema, per timeframe ───────────────────────────────────────────────────────────
    import jsonschema
    schema = json.loads((ROOT / "moneyflow" / "schema" / "snapshot.schema.json").read_text(encoding="utf-8"))
    snaps = {}
    for tf in ("1h", "today", "1w"):
        st, _, s = jget(f"{base}/api/snapshot?tf={tf}")
        if not check(st == 200, f"/api/snapshot?tf={tf} answers"):
            continue
        try:
            jsonschema.validate(s, schema)
            rec("PASS", f"{tf}: snapshot matches the published JSON schema")
        except jsonschema.ValidationError as e:
            rec("FAIL", f"{tf}: snapshot matches the published JSON schema", f"{e.message} at {list(e.absolute_path)}")
        snaps[tf] = s
    if not snaps:
        return 1
    market = snaps["today"]["market"]
    print(f"      market now: US stocks {market['us_equities']}, FX {market['fx']}, VIX {market['vix']}")

    # ── nodes ───────────────────────────────────────────────────────────────────────────
    just_started = (proc.get("uptime_s") or 1e9) < 300
    if just_started:
        print("      note: the service started under 5 minutes ago: a feed that has not ticked yet (e.g. a quiet"
              " FX pair around 17:00 New York) is reported as a warning, not a failure")
    for tf, s in snaps.items():
        nodes = {n["id"]: n for n in s["nodes"]}
        for nid, n in nodes.items():
            stt = n["status"]
            if stt == "warming_up" and n.get("history_from"):
                rec("PASS", f"{tf}: {nid} status", "")
                print(f"      note: {nid} is too new for the {tf} window (its history starts {n['history_from'][:10]})")
                continue
            ok_status = stt in ("live", "delayed", "closed")
            check(ok_status, f"{tf}: {nid} status", f"status={stt} age={n['age_s']}s",
                  warn_only=stt == "warming_up" or (stt == "stale" and just_started))
            if stt == "live":
                limit = 120 if n["source"]["id"] in ("finnhub", "alpaca", "computed") else 30
                check(n["age_s"] is not None and n["age_s"] < limit, f"{tf}: {nid} data fresh", f"age={n['age_s']}s")
                check(n["z"] is not None, f"{tf}: {nid} has a z-score", f"sigma method {n.get('sigma_method')}",
                      warn_only=True)
            if n["price"] is not None:
                check(n["price"] > 0, f"{tf}: {nid} price positive", str(n["price"]))
            if n["z"] is not None:
                check(abs(n["z"]) < 15, f"{tf}: {nid} z-score plausible", f"z={n['z']}")
                check(n["sigma_pct"] is not None and 0 < n["sigma_pct"] < 60, f"{tf}: {nid} sigma plausible",
                      f"sigma={n['sigma_pct']}%")
        ids = set(nodes)
        e_ok = all(e["from"] in ids and e["to"] in ids and e["weight"] > 0 for e in s["edges"])
        check(e_ok, f"{tf}: edges connect real nodes with positive weights")
        if s["edges"]:
            check(abs(sum(e["share"] for e in s["edges"]) - 1) < 0.01, f"{tf}: edge shares sum to 1")
            check(all(nodes[e["from"]]["z"] < 0 < nodes[e["to"]]["z"] for e in s["edges"]),
                  f"{tf}: every edge runs from an outflow to an inflow")
        linked = {n for e in s["edges"] for n in (e["from"], e["to"])}
        check(all((n.get("no_edge") is None) == (nid in linked) for nid, n in nodes.items()),
              f"{tf}: every node without an arrow says why (and only those)",
              "; ".join(f"{nid}: {n.get('no_edge')}" for nid, n in nodes.items() if (n.get("no_edge") is None) != (nid in linked)))
        stk = s.get("stocks") or {}
        if check(bool(stk), f"{tf}: notable-stock screen reported"):
            rule = stk.get("rule") or {}
            picks = stk.get("picks") or []
            check(len(picks) <= stk.get("slots", 0), f"{tf}: no more stocks than slots", f"{len(picks)} > {stk.get('slots')}")
            check(all(abs(p["z"]) >= rule.get("z_min", 0) and p["score"] >= rule.get("score_min", 0) for p in picks),
                  f"{tf}: every picked stock clears the rule", json.dumps(picks)[:300])
            shown = {n["slot"]["symbol"] for n in s["nodes"] if n.get("slot")}
            check(all(p["symbol"] in shown for p in picks), f"{tf}: every picked stock is on the map",
                  f"picked {[p['symbol'] for p in picks]}, shown {sorted(shown)}", warn_only=just_started)
            if stk.get("scanned_at"):
                check(stk.get("universe", 0) >= 20, f"{tf}: large-cap universe screened", f"universe={stk.get('universe')}")
            print(f"      {tf}: notable stock: {', '.join(p['symbol'] + ' ' + format(p['z'], '+.1f') + 'σ' for p in picks) or 'none'}"
                  f" ({stk.get('note', '')[:110]})")
        r = s["risk"]
        check(r["score"] is None or -100 <= r["score"] <= 100, f"{tf}: risk score within -100..100", str(r["score"]))
        check(0 <= r["coverage"] <= 1, f"{tf}: risk coverage within 0..1")
        check(len(s["narrative"]) >= 2, f"{tf}: narrative has sentences")

    # ── fed, news, sources ──────────────────────────────────────────────────────────────
    st, _, fed = jget(base + "/api/fed")
    if check(st == 200 and "decision_ts" in fed, "/api/fed gives the next FOMC decision"):
        check(fed["decision_ts"] > time.time() - 1800, "next FOMC decision is not in the past", fed.get("decision_at", ""))
        check(fed["days"] < 75, "next FOMC decision within 75 days (8 meetings a year)", str(fed["days"]))
    st, _, news = jget(base + "/api/news?limit=40")
    check(st == 200 and len(news) >= 5, "news wire has headlines", f"{len(news) if isinstance(news, list) else news}")
    if isinstance(news, list):
        titles = [n["title"].lower() for n in news]
        check(len(set(titles)) == len(titles), "no duplicate headlines")
        check(all(n["age_s"] >= -600 for n in news), "no headline dated in the future")
    st, _, srcs = jget(base + "/api/sources")
    check(st == 200 and len(srcs) >= 10, "/api/sources lists the connectors")
    for sname in ("binance_spot", "binance_fut", "alpaca", "finnhub"):
        s = next((x for x in srcs if x["id"] == sname), None)
        if s:
            check(s["state"] == "connected", f"{sname} stream connected", s["state"])
            if s.get("latency_ms_median") is not None:
                check(s["latency_ms_median"] < 2000, f"{sname} median latency under 2 s", f"{s['latency_ms_median']} ms")

    # ── endpoint contract ───────────────────────────────────────────────────────────────
    for path in ("/api", "/api/nodes?tf=1h", "/api/nodes/btc", "/api/edges?tf=today", "/api/risk?tf=1w",
                 "/api/narrative", "/api/tiles", "/api/events", "/api/schema", "/api/snapshot?tf=all"):
        st, hd, _ = jget(base + path)
        check(st == 200, f"GET {path} -> 200", str(st))
    check(jget(base + "/api/nodes/nope")[0] == 404, "unknown node -> 404")
    check(jget(base + "/api/snapshot?tf=5m")[0] == 400, "bad timeframe -> 400")
    st, hd, _ = jget(base + "/api/risk")
    check(hd.get("Access-Control-Allow-Origin") == "*", "CORS header for other local apps")
    st, _, _ = jget(base + "/api/health", headers={"Host": "evil.example"})
    check(st == 403, "requests for a foreign Host are refused (DNS-rebinding guard)", str(st))
    st, _, _ = http(base + "/")
    check(st == 200, "dashboard page or headless notice served")

    # ── streams ─────────────────────────────────────────────────────────────────────────
    async def streams():
        import aiohttp
        async with aiohttp.ClientSession() as s:
            got = []
            async with s.ws_connect(base.replace("http", "ws", 1) + "/ws?tf=today") as ws:
                t0 = time.time()
                while time.time() - t0 < 4 and len(got) < 3:
                    m = await ws.receive(timeout=4)
                    if m.type == aiohttp.WSMsgType.TEXT:
                        got.append(json.loads(m.data))
                await ws.send_str(json.dumps({"tf": "1h"}))
                m = await ws.receive(timeout=4)
                switched = json.loads(m.data).get("tf") == "1h"
            ts = [g["data"]["generated_ts"] for g in got]
            async with s.get(base + "/sse?tf=1w") as r:
                line = await asyncio.wait_for(r.content.readuntil(b"\n\n"), 5)
            return got, ts, switched, line
    try:
        got, ts, switched, line = asyncio.run(streams())
        check(len(got) >= 2, "WebSocket pushes snapshots", f"{len(got)} in 4 s")
        check(len(ts) == len(set(ts)), "WebSocket never repeats the same snapshot", str(ts), warn_only=True)
        check(switched, "WebSocket switches timeframe on request")
        check(line.startswith(b"event: snapshot"), "SSE stream delivers snapshots")
    except Exception as e:  # noqa: BLE001
        rec("FAIL", "streams", repr(e))

    # ── data sanity against independent sources ─────────────────────────────────────────
    if not a.no_external:
        print("      comparing with independent sources ...")
        nodes = {n["id"]: n for n in snaps["today"]["nodes"]}
        _, _, live = jget(base + "/api/snapshot?tf=today")
        lnodes = {n["id"]: n for n in live["nodes"]}
        for sym, nid in (("BTCUSDT", "btc"), ("ETHUSDT", "eth")):
            j = external(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}")
            if j and lnodes[nid]["price"]:
                d = abs(float(j["price"]) / lnodes[nid]["price"] - 1) * 100
                check(d < 0.3, f"{nid} price matches Binance REST", f"diff {d:.3f}%")
        pairs = [("QQQUSDT", "ndx"), ("SPYUSDT", "spx"), ("XAUUSDT", "gold"), ("CLUSDT", "oil")]
        pairs += [(n["slot"]["perpetual"], n["id"]) for n in live["nodes"] if n.get("slot") and n.get("price")]
        for sym, nid in pairs:
            j = external(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}")
            if j and lnodes[nid]["price"]:
                d = abs(float(j["indexPrice"]) / lnodes[nid]["price"] - 1) * 100
                check(d < 0.3, f"{nid} matches Binance's index price over REST", f"diff {d:.3f}%")
        y = external("https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1m&range=1d")
        if y and lnodes.get("usd", {}).get("price"):
            px = y["chart"]["result"][0]["meta"]["regularMarketPrice"]
            d = abs(px / lnodes["usd"]["price"] - 1) * 100
            check(d < 0.3, "computed DXY within 0.3% of ICE's (10-min delayed) DXY", f"ICE {px} vs {lnodes['usd']['price']:.3f}, diff {d:.3f}%")
        v = live["tiles"].get("vix") or {}
        y = external("https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1m&range=1d")
        if y and v.get("level"):
            px = y["chart"]["result"][0]["meta"]["regularMarketPrice"]
            check(abs(px / v["level"] - 1) < 0.03, "VIX tile within 3% of the latest delayed quote", f"{px} vs {v['level']}")
        g = external("https://api.coingecko.com/api/v3/global")
        if g and lnodes.get("stables", {}).get("price"):
            pct = g["data"]["market_cap_percentage"]
            low = pct.get("usdt", 0) + pct.get("usdc", 0)
            check(low * 0.95 <= lnodes["stables"]["price"] <= low + 8,
                  "stablecoin share consistent with CoinGecko (>= USDT+USDC share)",
                  f"{lnodes['stables']['price']:.2f}% vs USDT+USDC {low:.2f}%")
        try:
            from moneyflow.keys import KeyStore
            import tomllib
            files = [".env"]
            for f in ("moneyflow.toml", "moneyflow.local.toml"):
                p = ROOT / f
                if p.exists():
                    files = tomllib.loads(p.read_text(encoding="utf-8")).get("keys", {}).get("env_files", files)
            ks = KeyStore().load(files, base=ROOT)
            if ks.has("ALPACA_KEY_ID", "ALPACA_SECRET") and market["us_equities"] == "regular":
                j = external("https://data.alpaca.markets/v2/stocks/trades/latest?symbols=TLT,SPY&feed=iex",
                             {"APCA-API-KEY-ID": ks.get("ALPACA_KEY_ID"), "APCA-API-SECRET-KEY": ks.get("ALPACA_SECRET")})
                if j and lnodes["bonds"]["source"]["symbol"] == "TLT":
                    px = j["trades"]["TLT"]["p"]
                    check(abs(px / lnodes["bonds"]["price"] - 1) < 0.003, "bonds node matches TLT's latest IEX trade",
                          f"{px} vs {lnodes['bonds']['price']}")
                    spy = next((r for r in lnodes["spx"]["reference"] if r["symbol"] == "SPY"), None)
                    if spy and spy["basis_pct"] is not None:
                        check(abs(spy["basis_pct"]) < 0.5, "S&P perp index within 0.5% of SPY", f"basis {spy['basis_pct']}%")
        except Exception as e:  # noqa: BLE001
            rec("WARN", "Alpaca comparison skipped", repr(e)[:120])

    fails = [r for r in RESULTS if r[0] == "FAIL"]
    warns = [r for r in RESULTS if r[0] == "WARN"]
    print(f"\n{len(RESULTS)} checks: {len(RESULTS) - len(fails) - len(warns)} passed, {len(warns)} warnings, {len(fails)} failed"
          f"  ({dt.datetime.now(dt.timezone.utc):%H:%M UTC})")
    return 1 if fails else 0


if __name__ == "__main__":
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    raise SystemExit(main())
