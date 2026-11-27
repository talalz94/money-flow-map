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


def test_month_screen_is_exact_on_known_data():
    """1M on the stock's 31 days of hourly bars: the move from the price 30 days back (not 7), and σ
    the daily moves of those bars times the root of the NYSE sessions in the 30 days (about 21, not the
    week's 5), as the map's node reads 1M; no volume ratio (no earlier month in the bars)."""
    from moneyflow import stats
    h, px = bars(-3.0, vol_today=3000.0)
    s = screen_one(h, px, NOW, "1m")
    e = math.floor(NOW / H) * H
    p0 = h.price_interp(NOW - 30 * D)
    k = stats.month_k("us_equity", NOW)
    sig = stats.robust_rms(stats.roll_daily_returns(h.price_at, e, "us_equity", max_days=stats.DAILY_DAYS)) * math.sqrt(k)
    assert 19 < k < 23 and abs(p0 / h.price_interp(NOW - 7 * D) - 1) > 1e-4
    assert s["change_pct"] == pytest.approx((px / p0 - 1) * 100, rel=1e-9)
    assert s["z"] == pytest.approx(math.log(px / p0) / sig, rel=1e-9) and s["rvol"] is None


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
    # the start is the price at now - 7 days to the minute, as the node reads it (interpolated between the
    # hourly closes around 10:30; exact here, since the synthetic price moves linearly within that hour)
    week_ago = 100 * math.exp(logp(NOW - 7 * D, -3.0))
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


def test_after_a_start_the_hour_starts_from_real_prices_not_a_line(monkeypatch):
    """After a start or a wake the scans' ring does not reach back an hour, and the 1H start price came
    from a straight line between two hourly closes (±0.4σ against the node for ~52 minutes). The scanner
    now fetches the last 70 minutes of 5-minute index closes (weight 1 each), once."""
    import types
    from moneyflow.connectors import binance as bn
    now = NOW + 20 * 60                                                  # 10:50 New York
    calls = []

    async def fake(session, base, path, params, gate, timeout=30):
        assert path == "/fapi/v1/indexPriceKlines" and params["interval"] == "5m" and params["limit"] <= 99
        calls.append(params["pair"])
        t0 = params["startTime"] // 1000
        return [[(t0 + 300 * i) * 1000, "0", "0", "0", str(200.0 + i), "0"] for i in range(20)]

    monkeypatch.setattr(bn, "rest_get", fake)
    ctx = types.SimpleNamespace(engine=None, clock=clock.Clock(fixed=now), keys=None, session=None)
    sc = stocks.StockScanner(ctx, {"universe": ["NVDA"]})
    h, _ = bars(-3.0)
    sc.book.hourly["NVDAUSDT"], sc.book.ticker["NVDAUSDT"] = h, "NVDA"
    asyncio.run(sc._seed_ring(now))
    ring = sc.book.ring["NVDAUSDT"]
    assert stocks.ring_price(ring, now - H, 450) == 201.0               # the 5-minute close at now - 1 h
    assert max(t for t, _ in ring) <= now                                # no bar still in progress
    asyncio.run(sc._seed_ring(now))
    assert calls == ["NVDAUSDT"]                                         # the ring covers the hour: no refetch
    e = math.floor(now / H) * H                                          # and the fallback is no longer a line in
    a, b = h.price_at(e - H), h.price_at(e)                              # price but the same path in log
    assert h.price_interp(e - H / 2) == pytest.approx(math.sqrt(a * b))


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


@pytest.mark.parametrize("tf", ["1h", "today", "1w", "1m"])
def test_a_pick_whose_move_has_eased_says_so(cfg, keys, monkeypatch, tf):
    """'Eased' means the MOVE faded since the screen picked the stock (a 1H move can, in the minutes between
    two screens): the node's move under 3/4 of the screen's, and its |z| under the rule. The slot then says
    so with the numbers that picked it, "what the flow says" leaves it out, and that holds until the next
    screen. An unchanged move whose z differs only through σ (hourly bars against 5-minute data) is NOT
    eased: under the first rule (|z| alone) it was, in 6-7 of 10 cases at 10:45 New York."""
    from moneyflow import stats
    eng = engine_with_pick(cfg, keys)
    inst, real = eng._stocks["NVDAUSDT"]["inst"], stats.window_return
    pick = eng.stock_screen[tf]["picks"][0]
    r_screen = math.log1p(pick["change_pct"] / 100)
    move = {"r": r_screen}

    def window_return(i, w, now):
        if i is not inst:
            return real(i, w, now)
        return move["r"], math.expm1(move["r"]) * 100, i.price / math.exp(move["r"])

    monkeypatch.setattr(stats, "window_return", window_return)

    def read():
        eng._cache.clear()
        eng._snap_cache = None                                         # (a snapshot is reused for 0.5 s)
        snap = eng.snapshot(NOW)
        jsonschema.validate(one_tf(snap, tf), SCHEMA)
        td = snap["timeframes"][tf]
        return td, next(n for n in td["nodes"] if n["id"] == "stock1")

    # 1. the very move the screen picked, but the node's σ puts its |z| under the rule: not eased
    sig = stats.sigma_for(tf, inst, NOW)[0]
    eng.stock_cfg = dict(eng.stock_cfg, z_min=abs(r_screen / sig) + 0.5)
    td, node = read()
    assert abs(node["z"]) < eng.stock_cfg["z_min"] and node["slot"]["eased"] is False
    assert "the most unusual move among" in node["slot"]["why"]
    assert any(b["id"] == "notable_stock" for b in td["narrative"])
    # 2. the move fades to a third: eased, said with the numbers that picked it
    move["r"] = r_screen / 3
    td, node = read()
    why, p = node["slot"]["why"], td["stocks"]["picks"][0]
    assert node["slot"]["eased"] is True and why.endswith("the move has eased since.")
    assert f"{p['screen']['change_pct']:+.2f}%, {p['screen']['z']:+.1f}σ)" in why and "At the screen" in why
    assert p["why"] == why and td["stocks"]["note"] == why
    assert not any(b["id"] == "notable_stock" for b in td["narrative"])
    # 3. it holds until the next screen: the move coming back does not flip it at every push
    move["r"] = r_screen
    assert read()[1]["slot"]["eased"] is True
    # 4. the next screen judges afresh
    eng.stock_meta["at"] += 300
    assert read()[1]["slot"]["eased"] is False


def test_a_stock_the_map_already_reads_is_screened_on_the_maps_own_numbers(cfg, keys):
    """Hourly bars cannot place where in an hour the variance came (the 09:30 open sits inside the 09:00
    bar), so the screen's σ can differ from the node's by ~10% at some clock times. Once the map reads a
    stock (the current and recent picks), the screen ranks it on the node's own numbers: after the next
    screen the pick and its node agree exactly."""
    eng = engine_with_pick(cfg, keys)
    eng.on_stock_screen(_book({"NVDAUSDT": (-3.0, 3000.0), "AAPLUSDT": (-0.5, 1000.0)}), NOW)   # the next screen
    snap = eng.snapshot(NOW)
    for tf in ("1h", "today", "1w"):
        td = snap["timeframes"][tf]
        node = next(n for n in td["nodes"] if n["id"] == "stock1")
        pick = td["stocks"]["picks"][0]
        assert pick["symbol"] == "NVDA" and pick["screen"]["from"] == "map", tf
        assert pick["screen"]["z"] == pytest.approx(node["z"], abs=1e-3), tf
        assert pick["screen"]["change_pct"] == pytest.approx(node["change_pct"], abs=1e-3), tf
        assert node["slot"]["screen"]["z"] == pytest.approx(node["z"], abs=1e-3), tf
        other = next(c for c in td["stocks"]["candidates"] if c["symbol"] == "AAPL")
        assert other["screen"]["from"] == "hourly bars"                       # not on the map: hourly bars
        jsonschema.validate(one_tf(snap, tf), SCHEMA)


def test_the_flow_summary_counts_and_ranks_the_stock_like_its_slot(cfg, keys):
    """'What the flow says' uses the same count (stocks screened, not the whole universe) and the same
    rank as the slot's own text: slot 1 can hold the runner-up while the top pick is still loading."""
    eng = engine_with_pick(cfg, keys)
    eng.stock_screen["today"]["screened"] = 75
    eng.stock_meta["universe"] = 78
    eng.stock_screen["today"]["picks"][0]["rank"] = 2
    eng._cache.clear()
    td = eng.snapshot(NOW)["timeframes"]["today"]
    node = next(n for n in td["nodes"] if n["id"] == "stock1")
    text = next(b["text"] for b in td["narrative"] if b["id"] == "notable_stock")
    assert "number 2 of 75 large caps" in text and "most unusual" not in text and "78" not in text
    assert "number 2 of 75 large caps" in node["slot"]["why"] and node["slot"]["rank"] == 2


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



# ── the 1H start after a gap, the seed's back-off, and poll() really seeding ──────────────────────
def _scan_path(t):
    return 100.0 * math.exp(0.004 * math.sin(t / 1700.0) + 0.002 * math.sin(t / 290.0))


def _scanner(monkeypatch, fail_first=0):
    import types
    from moneyflow.connectors import binance as bn
    calls = {"n": 0}

    async def fake(session, base, path, params, gate, timeout=30):
        assert path == "/fapi/v1/indexPriceKlines" and params["interval"] == "5m"
        calls["n"] += 1
        if calls["n"] <= fail_first:
            raise OSError(11001, "getaddrinfo failed")
        t0 = params["startTime"] / 1000.0
        first = math.ceil(t0 / 300.0) * 300.0
        return [[int((first + 300 * i) * 1000), "0", "0", "0", f"{_scan_path(first + 300 * (i + 1)):.6f}", "0"]
                for i in range(params["limit"])]

    from moneyflow.keys import KeyStore
    monkeypatch.setattr(bn, "rest_get", fake)
    ctx = types.SimpleNamespace(engine=None, clock=clock.Clock(fixed=NOW), keys=KeyStore(), session=None)
    sc = stocks.StockScanner(ctx, {"universe": ["NVDA"], "scan_interval_s": 300})
    sc.book.hourly["NVDAUSDT"], sc.book.ticker["NVDAUSDT"] = Hourly(), "NVDA"
    sc.book.hourly["NVDAUSDT"].merge([(NOW - 40 * H + k * H, 100.0, 1.0) for k in range(30)])
    return sc, calls


def _polls(sc, start, n, skip=(), interval=300.0):
    """Polls every 5 minutes (seed, then this scan's price); skip = indices of polls lost to a gap."""
    out = []
    for i in range(n):
        t = start + i * interval
        if i in skip:
            continue
        asyncio.run(sc._seed_ring(t))
        p0 = stocks.ring_price(sc.book.ring.get("NVDAUSDT"), t - H, sc.book.scan_interval * 1.5)
        out.append((i, p0))
        sc.book.on_prices({"NVDAUSDT": _scan_path(t)}, t)
    return out


def test_the_hour_start_survives_a_short_gap_and_is_not_reseeded_every_scan(monkeypatch):
    """A 20-minute sleep or outage: the seeded 5-minute closes and the scans since share the ring.
    Bounded by count (16), it dropped the ones near now - 1 h, so the 1H start stayed on the line
    between two hourly closes for the rest of the session and every scan re-seeded 78 stocks."""
    sc, calls = _scanner(monkeypatch)
    start = ny_ts(2026, 11, 25, 9, 2) + 10
    res = _polls(sc, start, 40, skip=range(6, 10))             # no scan 09:32-09:47
    assert all(p0 == p0 for _, p0 in res)                      # every 1H start from a real price
    assert calls["n"] <= 2                                      # the start, and at most once after the gap
    assert len(sc.book.ring["NVDAUSDT"]) <= 40                  # an hour and a bit, not everything


def test_a_failing_seed_backs_off_and_the_scans_fill_the_hour(monkeypatch):
    """The 5-minute seed failing (no network) is retried after 5, 10, 20... minutes, not at every scan;
    within an hour the scans themselves cover it."""
    sc, calls = _scanner(monkeypatch, fail_first=10 ** 6)
    start = ny_ts(2026, 11, 25, 9, 2) + 10
    res = _polls(sc, start, 16)
    assert calls["n"] <= 4                                      # not 12
    assert all(p0 == p0 for i, p0 in res if i >= 12)           # an hour of scans covers it


def test_poll_seeds_the_ring_before_the_first_screen(monkeypatch):
    """poll() itself seeds the 5-minute start prices (a screen right after a start has its 1H start)."""
    import types
    from moneyflow.connectors import binance as bn
    screens = []

    async def fake(session, base, path, params, gate, timeout=30):
        if path == "/fapi/v1/exchangeInfo":
            return {"symbols": [{"symbol": "NVDAUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL",
                                 "underlyingType": "EQUITY"}]}
        if path in ("/fapi/v1/indexPriceKlines", "/fapi/v1/klines"):
            step = 300 if params["interval"] == "5m" else H
            t0 = params.get("startTime", (NOW - 30 * D) * 1000) / 1000.0
            first = math.ceil(t0 / step) * step
            return [[int((first + step * i) * 1000), "0", "0", "0", "100.0", "0", 0, "1000.0"]
                    for i in range(min(params["limit"], int((NOW - first) // step)))]
        if path == "/fapi/v1/premiumIndex":
            return [{"symbol": "NVDAUSDT", "indexPrice": "100.5"}]
        raise AssertionError(path)

    monkeypatch.setattr(bn, "rest_get", fake)
    eng = types.SimpleNamespace(seed_state={"state": "done"}, on_stock_screen=lambda book, now, missing=(): screens.append(now))
    ctx = types.SimpleNamespace(engine=eng, clock=clock.Clock(fixed=NOW), keys=None, session=None)
    sc = stocks.StockScanner(ctx, {"universe": ["NVDA"]})
    asyncio.run(sc.poll())
    ring = sc.book.ring["NVDAUSDT"]
    assert screens == [NOW] and len(ring) > 10 and stocks.ring_price(ring, NOW - H, 450) == 100.0


# ── the "eased" rule: each of its conditions ──────────────────────────────────────────────────────
@pytest.mark.parametrize("tf", ["1h", "today"])
def test_eased_needs_both_a_smaller_move_and_z_under_the_rule_and_lets_go_when_the_move_is_back(cfg, keys, monkeypatch, tf):
    from moneyflow import stats
    eng = engine_with_pick(cfg, keys)
    inst, real = eng._stocks["NVDAUSDT"]["inst"], stats.window_return
    pick = eng.stock_screen[tf]["picks"][0]
    rs = math.log1p(pick["change_pct"] / 100)
    sig = stats.sigma_for(tf, inst, NOW)[0]
    move = {"r": rs}

    def window_return(i, w, now):
        if i is not inst:
            return real(i, w, now)
        return move["r"], math.expm1(move["r"]) * 100, i.price / math.exp(move["r"])
    monkeypatch.setattr(stats, "window_return", window_return)

    def eased(r, z_min, new_screen=True):
        move["r"] = r
        eng.stock_cfg = dict(eng.stock_cfg, z_min=z_min)
        if new_screen:
            eng.stock_meta["at"] += 300                          # judged afresh
        eng._cache.clear()
        eng._snap_cache = None
        return next(n for n in eng.build_tf(tf, NOW)["nodes"] if n["id"] == "stock1")["slot"]["eased"]

    z = lambda r: abs(r / sig)                                    # noqa: E731
    assert eased(0.7 * rs, z(0.7 * rs) - 0.1) is False            # shrank, but |z| still clears the rule
    assert eased(0.8 * rs, z(0.8 * rs) + 0.1) is False            # |z| under the rule, but 0.8 of the move
    assert eased(0.7 * rs, z(0.7 * rs) + 0.1) is True             # both: eased
    assert eased(-rs, z(rs) + 0.1) is True                        # turned around: eased, whatever its size
    assert eased(0.7 * rs, z(0.7 * rs) + 0.1) is True             # (latched)
    assert eased(0.9 * rs, z(0.9 * rs) - 0.1, new_screen=False) is True    # 0.9 back: still eased
    assert eased(-1.05 * rs, z(1.05 * rs) - 0.1, new_screen=False) is True  # a rally the OTHER way: still
    assert eased(1.05 * rs, z(1.05 * rs) - 0.1, new_screen=False) is False  # fully back and over the rule


# ── the 17:00 roll, a pick whose history fails, which stocks are ranked on the map ────────────────
def test_the_roll_clears_todays_pick_and_screens_again(cfg, keys):
    """At 17:00 New York TODAY restarts: the old session's pick is not shown as "eased" until the next
    screen; it goes, the note says why, and the screen runs again at once."""
    import types
    eng = engine_with_pick(cfg, keys)
    kicked = []
    eng.connectors["stocks"] = types.SimpleNamespace(kick=lambda: kicked.append(1))
    eng.tick(NOW)
    after = ny_ts(2026, 11, 24, 17, 0) + 30
    eng.tick(after)
    assert eng.stock_screen["today"]["picks"] == [] and kicked == [1]
    assert eng.stock_screen["1h"]["picks"] and eng.stock_screen["1w"]["picks"]      # the other windows stay
    st = eng.build_tf("today", after)["stocks"]
    assert "new session" in st["note"] and st["picks"] == []


def test_a_pick_whose_history_fails_is_retried_not_left_loading(cfg, keys):
    eng = filled_engine_at(cfg, keys, NOW)
    eng.STOCK_RETRY_FIRST = 0.01
    eng.connectors = {}
    inst = eng.instrument("binance_fut", "NVDAUSDT", "24/7", True, "NVDA", "index")
    ent = eng._stocks["NVDAUSDT"] = {"inst": inst, "ticker": "NVDA", "picked_at": NOW, "owned": True, "ready": False}
    tries = []

    async def history(i, since5, since1, now):
        tries.append(1)
        if len(tries) < 3:
            raise OSError(11001, "getaddrinfo failed")
    eng._binance_history = history
    asyncio.run(asyncio.wait_for(eng._start_stock("NVDAUSDT"), timeout=5))
    assert len(tries) == 3 and ent["ready"] and "retry_in_s" not in ent


def test_only_the_shown_picks_with_complete_history_are_ranked_on_the_map(cfg, keys):
    eng = engine_with_pick(cfg, keys)
    assert set(eng._node_reads("today", NOW)) == {"NVDAUSDT"}
    other = eng.instrument("binance_fut", "AAPLUSDT", "24/7", True, "AAPL", "index")
    k_now = int(NOW // G5_STEP)                                       # a full history: readable on the map
    closes = [(k * G5_STEP, 200 * math.exp(0.002 * math.sin(k / 7.0))) for k in range(k_now - 35 * 288, k_now)]
    other.seed("g5", closes, NOW, [(t, 1e6, 5e5) for t, _ in closes])
    other.price = closes[-1][1] * 1.01
    eng._stocks["AAPLUSDT"] = {"inst": other, "ticker": "AAPL", "picked_at": NOW, "owned": True, "ready": True}
    eng.stock_screen["today"]["picks"].append(dict(eng.stock_screen["today"]["picks"][0], symbol="AAPLUSDT"))
    assert set(eng._node_reads("today", NOW)) == {"NVDAUSDT", "AAPLUSDT"}    # readable when shown...
    eng.stock_screen["today"]["picks"].pop()
    assert set(eng._node_reads("today", NOW)) == {"NVDAUSDT"}        # kept for 2 h, not shown: not re-read
    eng._stocks["NVDAUSDT"]["inst"].holes["g5"] = NOW - 3600        # a sleep's gap, not back-filled yet
    assert eng._node_reads("today", NOW) == {}



# ── round 8: kept picks on the map's numbers, a pick held through a back-fill, the roll race, retries ──
def test_a_kept_pick_near_the_top_is_ranked_on_the_maps_numbers_again(cfg, keys):
    """A stock picked lately and still on the map (kept for 2 h) is ranked on the map's own numbers
    when it is among the best: on hourly bars alone a borderline one went in and out of the slot at
    every screen with unchanged prices."""
    eng = engine_with_pick(cfg, keys)
    eng.stock_screen = {tf: {"picks": [], "top": [], "screened": 2} for tf in ("1h", "today", "1w")}   # not shown now
    eng.on_stock_screen(_book({"NVDAUSDT": (-3.0, 3000.0), "AAPLUSDT": (-0.5, 1000.0)}), NOW)
    for tf in ("1h", "today", "1w"):
        row = next(r for r in eng.stock_screen[tf]["top"] if r["symbol"] == "NVDAUSDT")
        assert row.get("exact"), tf


def test_a_shown_pick_waiting_for_its_backfill_is_held_not_dropped(cfg, keys):
    """After a wake the shown pick's grid still has the sleep's gap: the screen cannot rank it on the
    map, and on hourly bars alone it was dropped (for good: once off the slot it was not re-read).
    It is held until its history is back, then judged again."""
    eng = engine_with_pick(cfg, keys)
    eng.stock_cfg = dict(eng.stock_cfg, z_min=2.0, score_min=2.5)
    quiet = _book({"NVDAUSDT": (0.0, 1000.0), "AAPLUSDT": (-0.5, 1000.0)})     # nothing qualifies by the bars
    eng._stocks["NVDAUSDT"]["inst"].holes["g5"] = NOW - 3600                    # the back-fill is pending
    eng.on_stock_screen(quiet, NOW)
    assert [p["symbol"] for p in eng.stock_screen["today"]["picks"]] == ["NVDAUSDT"]
    assert eng.stock_screen["today"]["picks"][0]["held"]
    eng._stocks["NVDAUSDT"]["inst"].holes["g5"] = None                          # back-filled
    eng.on_stock_screen(quiet, NOW)
    assert all(p["symbol"] != "NVDAUSDT" or not p.get("held") for p in eng.stock_screen["today"]["picks"])


def test_a_scan_that_began_before_the_roll_does_not_bring_back_the_old_session(cfg, keys):
    """A scan started at 16:59 that ends after 17:00 must not re-install the old session's TODAY pick,
    and a re-screen follows within seconds."""
    import types
    eng = engine_with_pick(cfg, keys)
    kicked = []
    scanner = types.SimpleNamespace(kick=lambda: kicked.append(1), soon=False)
    eng.connectors["stocks"] = scanner
    eng.clock.set_fixed(ny_ts(2026, 11, 24, 17, 0) + 20)       # now: just after the roll
    eng.on_stock_screen(_book({"NVDAUSDT": (-3.0, 3000.0), "AAPLUSDT": (-0.5, 1000.0)}), NOW)   # begun at 10:30
    assert eng.stock_screen["today"]["picks"] == [] and eng.stock_screen["today"]["rolled"]
    assert scanner.soon and kicked == [1]
    assert eng.stock_screen["1h"]["picks"]                                     # the other windows stand


def test_a_failed_pick_is_retried_as_soon_as_the_feeds_are_back_and_stops_once_dropped(cfg, keys):
    eng = filled_engine_at(cfg, keys, NOW)
    eng.STOCK_RETRY_FIRST = 60.0
    eng.connectors = {}
    inst = eng.instrument("binance_fut", "NVDAUSDT", "24/7", True, "NVDA", "index")
    ent = eng._stocks["NVDAUSDT"] = {"inst": inst, "ticker": "NVDA", "picked_at": NOW, "owned": True, "ready": False}
    tries = []

    async def history(i, since5, since1, now):
        tries.append(1)
        if len(tries) < 2:
            raise OSError(11001, "getaddrinfo failed")
    eng._binance_history = history

    async def go():
        task = asyncio.create_task(eng._start_stock("NVDAUSDT"))
        await asyncio.sleep(0.1)
        eng.instruments["binance_spot:BTCUSDT"].recovered = True     # a feed delivers again...
        eng.check_holes()                                    # ...housekeeping sees it: no 60 s wait
        await asyncio.wait_for(task, timeout=2)
    asyncio.run(go())
    assert ent["ready"] and len(tries) == 2
    # a pick dropped while it is retrying: the retries stop
    eng.STOCK_RETRY_FIRST = 0.01
    ent2 = eng._stocks["AAPLUSDT"] = {"inst": eng.instrument("binance_fut", "AAPLUSDT", "24/7", True, "AAPL", "index"),
                                      "ticker": "AAPL", "picked_at": NOW, "owned": True, "ready": False}

    async def never(i, since5, since1, now):
        tries.append(2)
        if len(tries) > 6:
            eng._stocks.pop("AAPLUSDT", None)                # dropped from the screen
        raise OSError(11001, "getaddrinfo failed")
    eng._binance_history = never
    asyncio.run(asyncio.wait_for(eng._start_stock("AAPLUSDT"), timeout=5))
    assert not ent2["ready"] and "AAPLUSDT" not in eng._stocks


def test_a_picked_stocks_cached_gap_is_downloaded_from_the_gap(cfg, keys, tmp_path):
    from moneyflow import history
    eng = filled_engine_at(cfg, keys, NOW)
    eng.cache_dir = tmp_path
    eng.connectors = {}
    inst = eng.instrument("binance_fut", "NVDAUSDT", "24/7", True, "NVDA", "index")
    k_now = int(NOW // G5_STEP)
    inst.seed("g5", [(k * G5_STEP, 100.0) for k in range(k_now - 2000, k_now)], NOW)
    hole = (k_now - 500) * G5_STEP
    inst.g5.put(k_now - 500, float("nan"), False)            # a past outage, in the cache
    history.save(tmp_path, inst)
    fresh = eng.instruments.pop("binance_fut:NVDAUSDT")
    fresh = eng.instrument("binance_fut", "NVDAUSDT", "24/7", True, "NVDA", "index")
    eng._stocks["NVDAUSDT"] = {"inst": fresh, "ticker": "NVDA", "picked_at": NOW, "owned": True, "ready": False}
    asked = []

    async def history_(i, since5, since1, now):
        asked.append(since5)
    eng._binance_history = history_
    asyncio.run(eng._start_stock("NVDAUSDT"))
    assert asked == [hole - 600]


def test_the_seed_back_off_starts_over_after_a_success(monkeypatch):
    sc, calls = _scanner(monkeypatch, fail_first=2)
    start = ny_ts(2026, 11, 25, 9, 2) + 10
    _polls(sc, start, 4)                                     # fail, fail, then it works
    assert sc._seed_backoff == sc.interval
