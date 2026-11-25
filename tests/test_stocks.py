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
    """Log price, exact by construction. Each day (17:00 to 17:00 New York) sits at a level of ±M/2,
    alternating, so every roll-to-roll move is ±M; within a past day it moves a further ±M from 17:00
    to 10:00 and back by the next 17:00, so every same-clock move to 10:00 is ±M; today it moves
    `today_moves` x M from the roll (linear up to NOW)."""
    r = clock.day_roll(t)
    if r >= ROLL:
        return -M / 2 + today_moves * M * min(1.0, (t - ROLL) / (NOW - ROLL))
    d = round((ROLL - r) / D)
    sign = 1 if d % 2 else -1
    return sign * M / 2 + sign * M * ((t - r) / EL if t - r <= EL else 1 - (t - r - EL) / (D - EL))


def bars(today_moves: float, vol_today: float = 1000.0) -> tuple[Hourly, float]:
    h = Hourly()
    e = math.floor(NOW / H) * H
    rows = [(t, 100 * math.exp(logp(t + H, today_moves)), vol_today if t >= ROLL else 1000.0)
            for t in (e - 31 * D + k * H for k in range(int(31 * D / H)))]
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
    # at 10:30 the same-clock move on past days is M at 10:00 and 6/7 M at 11:00 (the hourly bars'
    # two sides), interpolated in variance half-way; the node, on 5-minute data, has 13/14 M (0.3% apart)
    sig = M * math.sqrt(0.5 + 0.5 * (6 / 7) ** 2)
    assert s["z"] == pytest.approx(-0.03 / sig, rel=1e-6)              # -3% against a typical ±0.93%
    assert s["change_pct"] == pytest.approx((math.exp(-0.03) - 1) * 100)
    assert s["rvol"] == pytest.approx(3.0)                              # 3x the same hours' usual volume
    assert s["score"] == pytest.approx(0.03 / sig * math.sqrt(3.0))


def test_week_screen_scales_the_trading_day_move():
    """1W: roll-to-roll moves on NYSE trading days (±M each), × √5 sessions a week."""
    h, px = bars(-3.0)
    s = screen_one(h, px, NOW, "1w")
    week_ago = 100 * math.exp(logp(math.floor((NOW - 7 * D) / H) * H, -3.0))   # close of the bar then
    assert s["z"] == pytest.approx(math.log(px / week_ago) / (M * math.sqrt(5)), rel=1e-6)


def test_screen_and_node_share_one_estimator():
    """The same prices give the same σ whether read from hourly bars (the screen) or from the map's
    5-minute grid (the node): one estimator, trading days only, robust to an outlier."""
    from moneyflow import stats
    from moneyflow.series import Instrument
    h, px = bars(-3.0)
    at = math.floor(NOW / H) * H                    # on an hour: the bars' resolution, so both see the same samples
    inst = Instrument("binance_fut:XUSDT", "binance_fut", "XUSDT", "24/7")
    inst.calendar = "us_equity"
    k_now = int(at // G5_STEP)
    inst.seed("g5", [(k * G5_STEP, 100 * math.exp(logp((k + 1) * G5_STEP, -3.0))) for k in range(k_now - 34 * 288, k_now)], at)
    inst.price = px
    for tf in ("today", "1w"):
        node_sigma = stats.sigma_for(tf, inst, at)[0]
        screen_sigma = math.log(1 + screen_one(h, px, at, tf)["sigma_pct"] / 100)
        assert node_sigma == pytest.approx(screen_sigma, rel=1e-6), tf
    # between two whole hours the screen interpolates its hourly σ, so it keeps time with the node
    # (here the same-clock move shrinks after 10:00: M at 10:00, 0.93 M at 10:30, 0.86 M at 11:00)
    node_sigma = stats.sigma_for("today", inst, NOW)[0]
    screen_sigma = math.log(1 + screen_one(h, px, NOW, "today")["sigma_pct"] / 100)
    at_10 = math.log(1 + screen_one(h, px, at, "today")["sigma_pct"] / 100)
    assert node_sigma == pytest.approx(screen_sigma, rel=0.01) and node_sigma < 0.95 * at_10


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
    assert r["picks"][0]["why"].startswith("NVDA -2.96% today (-3.2σ) on 3.0× its usual volume: the most unusual")
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


def test_the_screen_prices_stocks_by_their_index_like_the_map(monkeypatch):
    """The screen reads Binance's index price (what the stock's node shows), never the perpetual's last
    trade: a thin perpetual's last trade sat 0.6% off its index at the 17:00 roll (MDB, 25 Nov) and made
    the screen's move a point bigger than the map's. Volume stays the perpetual's own."""
    import types
    from moneyflow.connectors import binance as bn
    e = math.floor(NOW / H) * H
    calls = []

    async def fake(session, base, path, params, gate, timeout=30):
        calls.append(path)
        opens = [int((e - k * H) * 1000) for k in (3, 2, 1)]               # three complete bars, ending at e
        if path == "/fapi/v1/indexPriceKlines":
            assert params["pair"] in ("NVDAUSDT", "AAPLUSDT") and "symbol" not in params
            return [[t, "0", "0", "0", str(100.0 + i), "0"] for i, t in enumerate(opens)]
        if path == "/fapi/v1/klines":
            assert params["symbol"] in ("NVDAUSDT", "AAPLUSDT")
            return [[t, "0", "0", "0", "999.0", "0", 0, str(5000.0 + i)] for i, t in enumerate(opens)]
        if path == "/fapi/v1/premiumIndex":
            return [{"symbol": "NVDAUSDT", "markPrice": "151.0", "indexPrice": "150.0"},
                    {"symbol": "AAPLUSDT", "markPrice": "1.0", "indexPrice": ""},       # no index: skipped
                    {"symbol": "XAUUSDT", "markPrice": "4000", "indexPrice": "4001"}]   # not a stock
        raise AssertionError(path)

    monkeypatch.setattr(bn, "rest_get", fake)
    ctx = types.SimpleNamespace(engine=None, clock=clock.Clock(fixed=NOW), keys=None, session=None)
    sc = stocks.StockScanner(ctx, {"universe": ["NVDA", "AAPL"]})
    for sym in ("NVDAUSDT", "AAPLUSDT"):
        sc.book.hourly[sym], sc.book.ticker[sym] = Hourly(), sym.removesuffix("USDT")
    asyncio.run(sc._hourly(NOW))
    h = sc.book.hourly["NVDAUSDT"]
    assert h.price_at(e) == 102.0 and h.price_at(e - H) == 101.0           # index closes, not the last trade (999)
    assert h.volume(e - 3 * H, e) == 15003.0                               # the perpetual's quote volume
    asyncio.run(sc._prices(NOW))
    assert sc.book.last == {"NVDAUSDT": (150.0, NOW)}                      # the index, not the mark (151)
    assert "/fapi/v1/ticker/price" not in calls


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


def test_each_node_says_which_days_its_sigma_is_measured_on(cfg, keys):
    eng = engine_with_pick(cfg, keys)
    expect = {"spx": "NYSE trading days", "ndx": "NYSE trading days", "stock1": "NYSE trading days",
              "gold": "weekdays", "btc": "all days"}
    for tf in ("1h", "today", "1w"):
        nodes = {n["id"]: n for n in eng.build_tf(tf, NOW)["nodes"]}
        for nid, days in expect.items():
            n = nodes[nid]
            assert n["sigma_method"].startswith(("same-clock", "daily RMS")), (tf, nid, n["sigma_method"])
            assert n["sigma_days"] == days, (tf, nid)


@pytest.mark.parametrize("tf", ["1h", "today", "1w"])
def test_a_pick_whose_move_has_eased_says_so(cfg, keys, monkeypatch, tf):
    """The screen runs every few minutes and a 1H move can fade in between (seen live: NKE −2.4σ at the
    screen, −0.9σ on the map 2½ minutes later). The slot then says so, with the numbers that picked it,
    instead of calling a −0.5σ move the most unusual one; "what the flow says" leaves it out."""
    from moneyflow import stats
    eng = engine_with_pick(cfg, keys)
    eng.stock_cfg = dict(eng.stock_cfg, z_min=2.0)                  # the real rule (the fixture lowers it to pick)
    inst, real = eng._stocks["NVDAUSDT"]["inst"], stats.window_return

    def window_return(i, w, now):
        if i is not inst:
            return real(i, w, now)
        r = -0.5 * stats.sigma_for(w.tf, i, now)[0]
        return r, (math.exp(r) - 1) * 100, i.price / math.exp(r)

    monkeypatch.setattr(stats, "window_return", window_return)
    eng._cache.clear()
    snap = eng.snapshot(NOW)
    jsonschema.validate(one_tf(snap, tf), SCHEMA)
    td = snap["timeframes"][tf]
    node = next(n for n in td["nodes"] if n["id"] == "stock1")
    pick = td["stocks"]["picks"][0]
    why = node["slot"]["why"]
    assert node["z"] == pytest.approx(-0.5, abs=1e-6) and node["slot"]["eased"] is True
    assert "(-0.5σ)" in why and f"{pick['z']:+.1f}σ)" in why and why.endswith("the move has eased since.")
    assert "At the screen" in why and pick["why"] == why and td["stocks"]["note"] == why
    assert not any(b["id"] == "notable_stock" for b in td["narrative"])
    monkeypatch.setattr(stats, "window_return", real)               # the move is back: the usual text
    eng._cache.clear()
    node = next(n for n in eng.build_tf(tf, NOW)["nodes"] if n["id"] == "stock1")
    assert node["slot"]["eased"] is False and "the most unusual move among" in node["slot"]["why"]


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


def test_a_new_pick_is_shown_only_once_its_data_is_in(cfg, keys):
    """Just picked: the slot shows the stock once its history and live feed are in (never "no data");
    meanwhile the API lists it as loading. Seen live when the TODAY pick switched from DELL to SHOP."""
    eng = engine_with_pick(cfg, keys)
    eng._stocks["NVDAUSDT"]["ready"] = False
    eng._cache.clear()
    snap = eng.snapshot(NOW)
    td = snap["timeframes"]["today"]
    assert not any(n.get("slot") for n in td["nodes"]) and td["stocks"]["loading"] == ["NVDA"]
    assert td["stocks"]["picks"][0]["symbol"] == "NVDA"
    jsonschema.validate(one_tf(snap, "today"), SCHEMA)
    eng._stocks["NVDAUSDT"]["ready"] = True
    eng._cache.clear()
    td = eng.snapshot(NOW + 1)["timeframes"]["today"]
    assert any(n.get("slot") for n in td["nodes"]) and td["stocks"]["loading"] == []
