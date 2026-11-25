"""The notable-stock slot: screen maths on exact synthetic hourly bars, the universe filter, and the
slot as a map node (snapshot, schema, narrative, history API id)."""
import asyncio
import json
import math
from pathlib import Path

import jsonschema
import pytest

from conftest import filled_engine_at, ny_ts
from moneyflow import clock, stocks
from moneyflow.series import G5_STEP
from moneyflow.server import one_tf
from moneyflow.stocks import H, Hourly, StockBook, perp_symbol, screen, screen_one

D = 86400.0
NOW = ny_ts(2026, 11, 24, 10, 30)                # Tuesday 10:30 New York; the day rolled Mon 17:00
ROLL = clock.day_roll(NOW)
EL = 17 * H                                      # whole hours since the roll
M = 0.01                                         # every past day moves exactly ±1% over the same span
SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "moneyflow" / "schema" / "snapshot.schema.json").read_text())


def logp(t: float, today_moves: float) -> float:
    """Log price: on each past day +-M from 17:00 to 10:00 New York, back to 0 by the next 17:00;
    today `today_moves` x M since the roll (linear up to NOW)."""
    r = clock.day_roll(t)
    if r >= ROLL:
        return today_moves * M * min(1.0, (t - ROLL) / (NOW - ROLL))
    d = round((ROLL - r) / D)
    sign = 1 if d % 2 else -1
    return sign * M * ((t - r) / EL if t - r <= EL else 1 - (t - r - EL) / (D - EL))


def bars(today_moves: float, vol_today: float = 1000.0) -> tuple[Hourly, float]:
    h = Hourly()
    e = math.floor(NOW / H) * H
    rows = [(t, 100 * math.exp(logp(t + H, today_moves)), vol_today if t >= ROLL else 1000.0)
            for t in (e - 21 * D + k * H for k in range(int(21 * D / H)))]
    h.merge(rows)
    return h, 100 * math.exp(logp(NOW, today_moves))


def test_perp_symbols():
    assert perp_symbol("NVDA") == "NVDAUSDT" and perp_symbol("BRK.B") == "BRKBUSDT"


def test_hourly_bars_price_and_volume():
    h = Hourly()
    t0 = 1_800_000_000 // 3600 * 3600
    h.merge([(t0, 10.0, 1.0), (t0 + H, 11.0, 2.0), (t0 + 3 * H, 13.0, 4.0)])        # bar 2 missing
    assert h.price_at(t0 + H) == 10.0 and h.price_at(t0 + 2 * H - 1) == 10.0      # close of bar ending ≤ t
    assert h.price_at(t0 + 3 * H) == 11.0                                         # skips the missing bar
    assert h.volume(t0, t0 + 2 * H) == 3.0 and math.isnan(h.volume(t0, t0 + 4 * H))
    h.merge([(t0 + 2 * H, 12.0, 3.0), (t0 + H, 11.5, 2.5)])                       # fills and corrects
    assert h.volume(t0, t0 + 4 * H) == 10.5 and h.end == t0 + 4 * H
    h.merge([(t0 + k * H, 1.0, 1.0) for k in range(4, stocks.MAX_BARS + 10)])
    assert len(h.close) == stocks.MAX_BARS                                         # bounded memory


def test_today_screen_is_exact_on_known_data():
    h, px = bars(-3.0, vol_today=3000.0)
    s = screen_one(h, px, NOW, "today")
    assert s["z"] == pytest.approx(-3.0, rel=1e-6)                     # -3% against a typical ±1%
    assert s["change_pct"] == pytest.approx((math.exp(-0.03) - 1) * 100)
    assert s["rvol"] == pytest.approx(3.0)                              # 3x the same hours' usual volume
    assert s["score"] == pytest.approx(3.0 * math.sqrt(3.0))


def test_week_screen_scales_the_daily_move():
    h, px = bars(-3.0)
    s = screen_one(h, px, NOW, "1w")
    # same-clock daily moves alternate +-1% -> +-2% day to day: σ_1w = 2% x sqrt(7); a week ago was +1%
    assert s["z"] == pytest.approx(math.log(px / (100 * math.exp(M))) / (2 * M * math.sqrt(7)), rel=1e-6)


def test_hour_screen_uses_the_scan_ring_when_it_covers_the_hour():
    h, px = bars(-3.0)
    ring = [(NOW - 3600, px * math.exp(0.02)), (NOW - 1800, px)]
    s = screen_one(h, px, NOW, "1h", ring=ring, tol=450)
    assert s["change_pct"] == pytest.approx((math.exp(-0.02) - 1) * 100)          # from the ring, exactly
    assert screen_one(h, px, NOW, "1h", ring=[], tol=450) is not None            # falls back to hourly bars


def _book(spec):
    book = StockBook(300)
    for sym, (moves, vol) in spec.items():
        h, px = bars(moves, vol)
        book.hourly[sym] = h
        book.ticker[sym] = sym.removesuffix("USDT")
        book.on_prices({sym: px}, NOW)
    return book


def test_only_unusual_moves_on_real_volume_fill_the_slot():
    book = _book({"NVDAUSDT": (-3.0, 3000.0),        # z -3 on 3x volume: picked
                  "AAPLUSDT": (-0.5, 1000.0),        # quiet
                  "TSLAUSDT": (-2.2, 300.0)})        # |z| 2.2 but on 0.3x volume: score 1.6 < 2.5
    r = screen(book, "today", NOW, z_min=2.0, score_min=2.5, slots=1)
    assert [p["ticker"] for p in r["picks"]] == ["NVDA"] and r["screened"] == 3
    assert [t["ticker"] for t in r["top"]] == ["NVDA", "TSLA", "AAPL"]          # transparency: the runners-up
    assert r["picks"][0]["why"].startswith("NVDA -2.96% today (-3.0σ) on 3.0× its usual volume: the most unusual")
    assert screen(book, "today", NOW, z_min=3.5, score_min=2.5, slots=1)["picks"] == []   # may be none
    two = screen(book, "today", NOW, z_min=1.0, score_min=1.0, slots=2)["picks"]
    assert [p["ticker"] for p in two] == ["NVDA", "TSLA"] and "number 2 of 3" in two[1]["why"]


def test_stale_scan_prices_are_not_screened():
    book = _book({"NVDAUSDT": (-3.0, 3000.0)})
    assert screen(book, "today", NOW + 3600, 2.0, 2.5, 1)["screened"] == 0


def test_universe_keeps_only_listed_equity_perpetuals(monkeypatch):
    from moneyflow.connectors import binance as bn
    info = {"symbols": [
        {"symbol": "NVDAUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "EQUITY"},
        {"symbol": "BRKBUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "EQUITY"},
        {"symbol": "XAUUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "COMMODITY"},
        {"symbol": "PGUSDT", "status": "SETTLING", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "EQUITY"}]}

    async def fake(session, base, path, params, gate, timeout=30):
        assert path == "/fapi/v1/exchangeInfo"
        return info

    monkeypatch.setattr(bn, "rest_get", fake)
    import types
    ctx = types.SimpleNamespace(engine=None, clock=clock.Clock(fixed=NOW), keys=None, session=None)
    sc = stocks.StockScanner(ctx, {"universe": ["NVDA", "BRK.B", "PG", "MA"]})
    asyncio.run(sc._universe())
    assert sorted(sc.book.hourly) == ["BRKBUSDT", "NVDAUSDT"] and sc.book.ticker["BRKBUSDT"] == "BRK.B"
    assert sc.missing == ["PG", "MA"]


def engine_with_pick(cfg, keys, now=NOW, move=0.9):
    """An engine with the fixed nodes filled and NVDA picked for every window, its history loaded."""
    eng = filled_engine_at(cfg, keys, now)
    book = _book({"NVDAUSDT": (-3.0, 3000.0), "AAPLUSDT": (-0.5, 1000.0)})
    book.on_prices({"NVDAUSDT": book.last["NVDAUSDT"][0], "AAPLUSDT": book.last["AAPLUSDT"][0]}, now)
    eng.cfg = dict(eng.cfg, stocks=dict(eng.stock_cfg, z_min=0.1, score_min=0.1))
    eng.stock_cfg = eng.cfg["stocks"]
    eng.on_stock_screen(book, now)
    ent = eng._stocks["NVDAUSDT"]
    inst, k_now = ent["inst"], int(now // G5_STEP)
    closes = [(k * G5_STEP, 150 * math.exp(0.003 * math.sin(k / 5.0))) for k in range(k_now - 35 * 288, k_now)]
    inst.seed("g5", closes, now, [(t, 1e6, 5e5) for t, _ in closes])
    inst.price, inst.ts, inst.recv_ts, inst._live_seen = closes[-1][1] * move, now - 0.5, now - 0.4, True
    ent["ready"] = True
    eng._cache.clear()
    return eng


def test_the_picked_stock_is_a_full_node_on_the_map(cfg, keys):
    eng = engine_with_pick(cfg, keys)
    snap = eng.snapshot(NOW)
    for tf in ("1h", "today", "1w"):
        td = snap["timeframes"][tf]
        node = next(n for n in td["nodes"] if n["id"] == "stock1")
        assert node["slot"]["symbol"] == "NVDA" and node["short"] == "NVDA" and (node["x"], node["y"]) == (0.92, 0.29)
        assert node["source"]["symbol"] == "NVDAUSDT" and node["status"] == "live" and node["z"] < -2
        assert td["stocks"]["picks"][0]["symbol"] == "NVDA" and td["stocks"]["universe"] == 2
        jsonschema.validate(one_tf(snap, tf), SCHEMA)
    today = snap["timeframes"]["today"]
    assert any(b["id"] == "notable_stock" and "NVDA" in b["text"] for b in today["narrative"])
    assert "NVDA" in eng.watch_symbols()
    assert [s.id for s, _, _ in eng.slot_series("today")] == ["stock1"]


def test_an_empty_slot_says_why(cfg, keys):
    eng = filled_engine_at(cfg, keys, NOW)
    eng.on_stock_screen(_book({"AAPLUSDT": (-0.5, 1000.0)}), NOW)
    td = eng.snapshot(NOW)["timeframes"]["today"]
    assert not any(n["id"].startswith("stock") for n in td["nodes"])
    assert td["stocks"]["picks"] == [] and "No large-cap stock stands out" in td["stocks"]["note"]
    jsonschema.validate(one_tf(eng.snapshot(NOW), "today"), SCHEMA)


def test_a_stock_no_longer_picked_is_dropped_after_keep_s(cfg, keys):
    eng = engine_with_pick(cfg, keys)
    key = eng._stocks["NVDAUSDT"]["inst"].key
    quiet = _book({"AAPLUSDT": (-0.5, 1000.0)})
    eng.on_stock_screen(quiet, NOW + 600)
    assert "NVDAUSDT" in eng._stocks                                  # kept a while (it may come back)
    eng.on_stock_screen(quiet, NOW + 7300)
    assert "NVDAUSDT" not in eng._stocks and key not in eng.instruments
