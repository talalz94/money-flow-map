"""The 1M window: the last 30 days. Its move comes from the 5-minute grid (35 days), its σ from about a
year of daily closes (series.Daily), downloaded once and extended once a day."""
import asyncio
import json
import math
import random
import types

from conftest import ny_ts
from moneyflow import clock, history, narrative, stats
from moneyflow.series import G5_STEP, Daily, Instrument

DAY = 86400


def _walk(seed: int, n: int, sigma: float, start: float) -> list[float]:
    rnd, p, out = random.Random(seed), start, []
    for _ in range(n):
        p *= math.exp(rnd.gauss(0.0, sigma))
        out.append(p)
    return out


def _fill_g5(inst, now: float, price_of) -> None:
    """35 days of 5-minute closes: price_of(the step's end), live while its market is open."""
    k1 = int(now // G5_STEP)
    for k in range(k1 - 10079, k1):
        t = (k + 1) * G5_STEP
        inst.g5.put(k, price_of(t), clock.market_open(inst.hours, t - 1))


def _daily(inst, now: float, closes: list[float], vols=None) -> None:
    d0 = int(now // DAY) - len(closes)
    inst.daily.merge([((d0 + j) * DAY, c, (vols[j] if vols else float("nan"))) for j, c in enumerate(closes)], now)


def test_a_30_day_move_of_a_24_7_asset_against_its_oracle():
    """Bitcoin: the change from the price 30 days ago to now, and σ = the robust RMS of a year of daily
    moves × √30 (it trades every day), about 16% for 3% a day."""
    now = ny_ts(2026, 11, 27, 12, 0)
    btc = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT", "24/7")

    def price(t):
        return 90000.0 * math.exp(1e-8 * (t - now))                     # +2.6% over the 30 days
    _fill_g5(btc, now, price)
    btc.price = price(now)
    closes = _walk(1, 366, 0.03, 60000.0)
    _daily(btc, now, closes)
    w = stats.window_for("1m", now)
    r, pct, p0 = stats.window_return(btc, w, now)
    assert w.start == now - 30 * DAY and w.label == "last 30 days"
    assert abs(p0 / price(now - 30 * DAY) - 1) < 1e-12 and abs(pct - (price(now) / p0 - 1) * 100) < 1e-9
    sig, n, method = stats.sigma_for("1m", btc, now)
    rs = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    assert n == 365 and method.startswith("daily RMS, 365 days × √30")
    assert abs(sig - stats.robust_rms(rs) * math.sqrt(30)) < 1e-12
    assert 0.85 * 0.03 * math.sqrt(30) < sig < 1.15 * 0.03 * math.sqrt(30)


def test_a_30_day_move_of_an_etf_across_weekends_and_a_holiday():
    """An ETF read on Monday 30 Nov at 10:00: 30 days back is Saturday 31 Oct, so the reference is
    Friday's close carried over the weekend; the window holds 19.7 NYSE sessions (19 whole ones,
    Thanksgiving left out, and Monday's first 17 of 24 hours), so σ = daily RMS × √19.7."""
    now = ny_ts(2026, 11, 30, 10, 0)
    etf = Instrument("alpaca:HYG", "alpaca", "HYG", "us_regular")
    friday_close = ny_ts(2026, 10, 30, 16, 0)

    def price(t):                                                        # each session at its own level
        d = clock.session_date(t - 1)
        return 80.0 + 0.1 * (d.toordinal() % 7) + 0.001 * ((t // 300) % 5) if clock.market_open("us_regular", t - 1) else None
    last = [80.0]

    def carried(t):
        p = price(t)
        if p is None:
            return last[0]
        last[0] = p
        return p
    _fill_g5(etf, now, carried)
    etf.price = etf.g5.asof(now)
    closes = _walk(2, 250, 0.006, 80.0)
    _daily(etf, now, closes)
    w = stats.window_for("1m", now)
    p0 = stats.price_at(etf, w.start, now)
    assert p0 == etf.g5.asof(friday_close + 300) == price(friday_close)  # Friday's close, carried
    assert stats.month_k("us_equity", now) == 19.7 and stats.month_k("weekdays", now) == 20.7
    sig, n, method = stats.sigma_for("1m", etf, now)
    rs = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    assert n == 249 and abs(sig - stats.robust_rms(rs) * math.sqrt(19.7)) < 1e-12
    assert stats.month_k("us_equity", ny_ts(2026, 11, 13, 12, 0)) == 22.0    # a month without a holiday


def test_the_monthly_yardstick_matches_known_monthly_volatility():
    """σ sanity on random walks of known daily volatility: an index-like ETF at 1% a day (16% a year)
    reads about 4.6% a month (√21 sessions), Bitcoin-like 3% a day about 16% (√30); a short history
    (a perpetual listed weeks ago) falls back to the last 30 days of its 5-minute grid, as 1W does."""
    now = ny_ts(2026, 11, 13, 12, 0)
    for cal, hours, sd, k in (("us_equity", "us_regular", 0.01, 22.0), ("all", "24/7", 0.03, 30.0)):
        errs = []
        for seed in range(20):
            inst = Instrument("x:" + cal, "x", "X", hours)
            _daily(inst, now, _walk(seed, 366 if cal == "all" else 252, sd, 100.0))
            sig = stats.sigma_for("1m", inst, now)[0]
            errs.append(sig / (sd * math.sqrt(k)) - 1)
        assert abs(sum(errs) / len(errs)) < 0.03 and max(abs(e) for e in errs) < 0.12, (cal, errs)
    young = Instrument("binance_fut:NEWUSDT", "binance_fut", "NEWUSDT", "24/7")
    _daily(young, now, _walk(7, 20, 0.02, 10.0))                       # 19 daily moves: too few
    path = _walk(8, 10080, 0.02 / math.sqrt(288), 10.0)
    k1 = int(now // G5_STEP)
    for j, p in enumerate(path):
        young.g5.put(k1 - 10080 + j, p, True)
    sig, n, method = stats.sigma_for("1m", young, now)
    assert method.startswith("daily RMS × √30") and n >= 14 and 0.5 * 0.02 * math.sqrt(30) < sig < 1.6 * 0.02 * math.sqrt(30)


def test_daily_closes_keep_only_complete_days_and_their_volume_reads_a_month():
    """A daily bar whose day has not ended is left out (its close is not final); rows older than the
    year (and a margin) are dropped. 1M's volume ratio: the last 30 days against the median of the
    year's earlier 30-day stretches, from the daily bars (the 5-minute volume holds 35 days)."""
    now = ny_ts(2026, 11, 27, 12, 0)
    d = Daily()
    today = int(now // DAY)
    d.merge([(today * DAY, 100.0, 5.0), ((today - 1) * DAY, 99.0, 5.0), ((today - 500) * DAY, 50.0, 5.0)], now)
    assert sorted(d.rows) == [today - 1]
    inst = Instrument("binance_spot:ETHUSDT", "binance_spot", "ETHUSDT", "24/7", volume=True)
    vols = [1e9] * 336 + [2e9] * 30                                     # this month twice the usual
    vols[36:66] = [6e9] * 30                                            # ...and one earlier month busy
    _daily(inst, now, [3000.0] * 366, vols)
    v = stats.volume_read(inst, stats.window_for("1m", now), now)
    assert v["basis"] == "daily bars" and v["baseline_days"] == 11 and abs(v["rvol"] - 2.0) < 1e-12
    assert abs(v["quote_volume"] - 30 * 2e9) < 1
    inst.daily.own = False                                              # another symbol's bars (SPY's)
    assert stats.volume_read(inst, stats.window_for("1m", now), now) is None


def test_the_narrative_says_this_month_and_quotes_the_30_day_supply_change():
    nodes = {"btc": {"label": "Bitcoin", "z": -2.0, "change_pct": -9.0, "status": "live"},
             "eth": {"label": "Ether", "z": -2.0, "change_pct": -12.0, "status": "live"},
             "stables": {"label": "Stablecoin share", "z": 2.0, "change_pct": 8.0, "status": "live"}}
    out = narrative.build("1m", nodes, [], {}, {"stable_supply": {"d1": 1e8, "w1": 2e8, "m1": 3e9}})
    texts = " ".join(b["text"] for b in out)
    assert "this month" in texts and "+$3.00B in 30 days" in texts


def test_the_daily_closes_are_downloaded_once_then_extended_about_daily_and_cached(cfg, keys, monkeypatch, tmp_path):
    """At the start a year of daily closes per instrument (all ETFs in one Alpaca request; the
    perpetuals listed in 2026 take their ETF's or futures' bars, which reach back a year), saved in
    daily.json; within the day nothing more; a day later only the days since; the next start loads
    them from the file."""
    from moneyflow.connectors import alpaca as alp, binance as bn, pollers
    from moneyflow.engine import Engine
    now = ny_ts(2026, 11, 27, 18, 0)
    clk = clock.Clock(fixed=now)
    calls = []

    async def klines(session, market, symbol, interval, start_ms, end_ms, price="last"):
        calls.append(("binance", symbol, interval, round((end_ms / 1000 - start_ms / 1000) / DAY)))
        return [(t, 100.0 + (t // DAY) % 3, 1e9, 5e8) for t in range(int(-(-start_ms // 1000 // DAY) * DAY), int(end_ms / 1000), DAY)]

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        calls.append(("alpaca", tuple(symbols), tf, round((clk.now() - since) / DAY)))
        return {s: [(t, 50.0 + (t // DAY) % 2, 1e8) for t in range(int(-(-since // DAY) * DAY), int(clk.now()), DAY)
                    if clock.trading_day("us_equity", t + 12 * 3600)] for s in symbols}

    async def chart(session, symbol, interval="5m", rng="60d", now=None):
        calls.append(("yahoo", symbol, interval, rng))
        days = 366 if rng == "1y" else 31
        return [(t, 20.0 + (t // DAY) % 4) for t in range(int(now // DAY - days) * DAY, int(now), DAY)]
    monkeypatch.setattr(bn, "fetch_klines", klines)
    monkeypatch.setattr(alp, "fetch_bars", bars)
    monkeypatch.setattr(pollers, "yahoo_chart", chart)

    def engine():
        e = Engine(cfg, keys, clk)
        e.cache_dir = tmp_path
        e.connectors = {"alpaca": types.SimpleNamespace(available=lambda: (True, ""), cfg={})}
        return e
    eng = engine()
    assert asyncio.run(eng.seed_daily())
    alpaca_calls = [c for c in calls if c[0] == "alpaca"]
    assert len(alpaca_calls) == 1 and set(alpaca_calls[0][1]) >= {"HYG", "IEF", "TLT", "SPY", "QQQ", "UVXY"}
    assert alpaca_calls[0][2] == "1Day" and alpaca_calls[0][3] == 366
    assert ("binance", "BTCUSDT", "1d", 366) in calls and ("yahoo", "GC=F", "1d", "1y") in calls
    assert ("yahoo", "DX-Y.NYB", "1d", "1y") in calls
    spx, bonds, usd = eng.series["spx"].inst, eng.series["bonds"].inst, eng.series["usd"].inst
    assert len(spx.daily.rows) > 240 and not spx.daily.own and bonds.daily is bonds.primary.daily
    assert len(usd.daily.rows) >= 365 and stats.sigma_for("1m", usd, now)[2].startswith("daily RMS")
    assert not eng.series["stables"].inst.daily.rows                    # none: its own 5-minute grid
    saved = json.loads((tmp_path / history.DAILY_FILE).read_text())["instruments"]
    assert "alpaca:SPY" not in saved and "binance_fut:SPYUSDT" in saved and "alpaca:TLT" in saved
    calls.clear()
    clk.set_fixed(now + 6 * 3600)
    assert asyncio.run(eng.seed_daily()) and calls == []                # within the day: nothing
    clk.set_fixed(now + 21 * 3600)
    assert asyncio.run(eng.seed_daily())
    btc_days = [c[3] for c in calls if c[:3] == ("binance", "BTCUSDT", "1d")]
    assert btc_days and btc_days[0] <= 7 and ("yahoo", "GC=F", "1d", "1mo") in calls      # the days since only
    calls.clear()
    eng2 = engine()                                                      # the next start: from the file
    asyncio.run(eng2.seed_history())                                     # (its start-up: no daily request)
    assert not [c for c in calls if c[2] in ("1d", "1Day")]
    assert len(eng2.series["spx"].inst.daily.rows) == len(spx.daily.rows) > 240
    assert len(eng2.series["btc"].inst.daily.rows) == len(eng.series["btc"].inst.daily.rows)
    calls.clear()
    assert asyncio.run(eng2.seed_daily()) and calls == []               # fresh: nothing downloaded


def test_the_computed_dollars_30_day_move_and_its_yardstick_from_ices_daily_closes(cfg, keys):
    """The dollar, computed live from six FX legs (its history ICE's DXY), read for 1M on Friday 27 Nov
    at 12:00: from its own 5-minute grid 30 days back to the price the legs give now; σ = the robust RMS
    of a year of ICE's daily moves × √(the weekdays in the 30 days: 22, Thanksgiving is one for FX)."""
    from conftest import filled_engine_at
    from moneyflow.series import DXY
    now = ny_ts(2026, 11, 27, 12, 0)
    eng = filled_engine_at(cfg, keys, now)
    s = eng.series["usd"]
    usd = s.inst
    assert isinstance(usd, DXY) and usd.calendar == "weekdays"
    closes = _walk(3, 260, 0.004, 99.0)
    _daily(usd, now, closes)
    w = stats.window_for("1m", now)
    rd = eng.read(s, w, now)
    p0 = usd.g5.asof(w.start)
    assert usd.price == DXY.formula({n: i.price for n, i in usd.pairs.items()})
    assert rd["ref_price"] == p0 and rd["change_pct"] == round((usd.price / p0 - 1) * 100, 4)
    k = stats.month_k("weekdays", now)
    rs = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    sig = stats.robust_rms(rs) * math.sqrt(k)
    assert k == 22.0 and rd["sigma_method"] == f"daily RMS, {len(rs)} days × √22"
    assert rd["z"] == round(math.log(usd.price / p0) / sig, 3) and rd["status"] == "live"


def test_one_extraordinary_day_in_the_year_does_not_set_the_months_yardstick():
    """A year of ±1% days and one -18% day (a crash, an earnings gap): counted as it is, it alone
    raises σ_1M by over 40%; capped at the next-largest day, as everywhere (robust_rms)."""
    now = ny_ts(2026, 11, 27, 12, 0)
    inst = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT", "24/7")
    rnd = random.Random(5)
    rs = [rnd.choice((-1, 1)) * 0.01 for _ in range(365)]
    rs[200] = math.log(0.82)
    closes, p = [], 100.0
    for r in [0.0] + rs:
        p *= math.exp(r)
        closes.append(p)
    _daily(inst, now, closes)
    sig = stats.sigma_for("1m", inst, now)[0]
    plain = math.sqrt(sum(r * r for r in rs) / len(rs)) * math.sqrt(30)
    assert abs(sig - 0.01 * math.sqrt(30)) < 1e-9 and plain > 1.4 * sig


def test_the_months_moves_are_read_in_two_hour_steps():
    """The flows' correlations read a window's moves step by step: 2-hour steps for 1M (360 over 30
    days for crypto), as hourly ones for 1W (5-minute steps would be 8,640 for a month)."""
    now = ny_ts(2026, 11, 27, 12, 0)
    btc = Instrument("binance_spot:BTCUSDT", "binance_spot", "BTCUSDT", "24/7")
    _fill_g5(btc, now, lambda t: 100.0 + math.sin(t / 7000.0))
    rs = stats.step_returns(btc, stats.window_for("1m", now), now)
    ks = sorted(rs)
    assert 355 <= len(rs) <= 361 and all(b - a == 24 for a, b in zip(ks, ks[1:]))


def test_a_picked_stock_gets_its_year_of_daily_closes_when_picked(cfg, keys, monkeypatch, tmp_path):
    """A stock picked for the slot reads 1M on a year of daily closes from the start: they are loaded
    from daily.json (a pick's are saved there too), and the days since downloaded at once. Left to the
    half-hourly check, a 1M pick was judged on the 30-day fallback for up to half an hour after each
    pick, its saved year never read back at a start; when the year came the pick dropped, and it was
    picked again 2 hours later."""
    from moneyflow.connectors import alpaca as alp
    from moneyflow.engine import Engine
    now = ny_ts(2026, 11, 30, 10, 0)
    calls = []

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        calls.append((tuple(symbols), tf))
        return {s: [(t, 100.0 + (t // DAY) % 5, 1e8) for t in range(int(-(-since // DAY) * DAY), int(now) - DAY, DAY)
                    if clock.trading_day("us_equity", t + 12 * 3600)] for s in symbols}
    monkeypatch.setattr(alp, "fetch_bars", bars)

    def engine():
        e = Engine(cfg, keys, clock.Clock(fixed=now))
        e.cache_dir = tmp_path
        e.connectors = {"alpaca": types.SimpleNamespace(available=lambda: (True, ""), cfg={})}

        async def loaded(sym, ent):
            return True
        e._load_stock = loaded
        return e

    async def pick(e):
        e._want_stock("NVDAUSDT", "NVDA", now)
        for _ in range(20):
            await asyncio.sleep(0)
            pending = [t for t in e._tasks if not t.done()]
            if pending:
                await asyncio.gather(*pending)
        return e._stocks["NVDAUSDT"]["inst"]
    eng = engine()
    inst = asyncio.run(pick(eng))
    assert calls == [(("NVDA",), "1Day")] and len(inst.daily.rows) > 240
    assert stats.sigma_for("1m", inst, now)[2].startswith("daily RMS, ")
    eng._stocks.clear()                                    # dropped 2 h later: the screen still ranks on it
    ys = eng._year_sigmas(now)
    assert list(ys) == ["NVDAUSDT"] and abs(ys["NVDAUSDT"] - stats.sigma_for("1m", inst, now)[0]) < 1e-12
    calls.clear()
    inst2 = asyncio.run(pick(engine()))                    # a later start: from the file, nothing downloaded
    assert calls == [] and len(inst2.daily.rows) == len(inst.daily.rows)
    assert stats.sigma_for("1m", inst2, now)[2].startswith("daily RMS, ")


def test_yahoo_switched_off_is_asked_nothing_for_daily_closes(cfg, keys, monkeypatch, tmp_path):
    """[connectors.yahoo] enabled = false turns Yahoo off: the daily closes of the dollar, the futures
    and, without a key, the ETFs are not asked for (they were, 20 s after each start and about daily,
    again every 30 minutes when refused). Those nodes take 1M's σ from their 5-minute grid."""
    from moneyflow.connectors import alpaca as alp, binance as bn, pollers
    from moneyflow.engine import Engine
    now = ny_ts(2026, 11, 27, 18, 0)
    calls = []

    async def klines(session, market, symbol, interval, start_ms, end_ms, price="last"):
        calls.append(("binance", symbol))
        return [(t, 100.0, 1e9, 5e8) for t in range(int(-(-start_ms // 1000 // DAY) * DAY), int(end_ms / 1000), DAY)]

    refused = {"on": False}

    async def bars(conn, symbols, tf, since, end=None, feed="iex"):
        calls.append(("alpaca", tuple(symbols)))
        if refused["on"]:
            from moneyflow.connectors.base import Unauthorized
            raise Unauthorized("HTTP 401")
        return {s: [] for s in symbols}

    async def chart(session, symbol, interval="5m", rng="60d", now=None):
        calls.append(("yahoo", symbol))
        return []
    monkeypatch.setattr(bn, "fetch_klines", klines)
    monkeypatch.setattr(alp, "fetch_bars", bars)
    monkeypatch.setattr(pollers, "yahoo_chart", chart)
    off = dict(cfg, connectors=dict(cfg["connectors"], yahoo={"enabled": False}))
    for with_key in (True, False):
        e = Engine(off, keys, clock.Clock(fixed=now))
        e.cache_dir = tmp_path / str(with_key)
        e.connectors = {"alpaca": types.SimpleNamespace(available=lambda k=with_key: (k, ""), cfg={})}
        calls.clear()
        assert asyncio.run(e.seed_daily())
        assert not [c for c in calls if c[0] == "yahoo"] and ("binance", "BTCUSDT") in calls
        assert bool([c for c in calls if c[0] == "alpaca"]) is with_key
        assert not e.series["gold"].inst.daily.rows and not e.series["usd"].inst.daily.rows
    refused["on"] = True                                     # the keys refused: not Yahoo instead either
    e = Engine(off, keys, clock.Clock(fixed=now))
    e.cache_dir = tmp_path / "refused"
    e.connectors = {"alpaca": types.SimpleNamespace(available=lambda: (True, ""), cfg={}, keys_refused=lambda why: None)}
    calls.clear()
    asyncio.run(e.seed_daily())
    assert [c for c in calls if c[0] == "alpaca"] and not [c for c in calls if c[0] == "yahoo"]
