"""The engine: builds instruments and series from the config, runs the connectors, seeds history,
rolls the grids once a second and assembles snapshots (the JSON every client receives)."""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from . import SCHEMA_ID, __version__, clock, config as cfgmod, fed as fedmod, flow, history, narrative, stats
from .news import NewsBook
from .series import DXY, Derived, Instrument, StablecoinShare, Stitched, isnum

log = logging.getLogger("moneyflow.engine")
NaN = float("nan")

SOURCE_NAMES = {
    "binance_spot": "Binance spot", "binance_fut": "Binance USD-M perpetual", "alpaca": "Alpaca IEX",
    "finnhub": "Finnhub (OANDA FX)", "coinbase": "Coinbase", "hyperliquid": "Hyperliquid", "yahoo": "Yahoo",
    "computed": "computed",
}
SOURCE_SHORT = {"binance_spot": "BNC", "binance_fut": "BNC-F", "alpaca": "IEX", "finnhub": "FX", "coinbase": "CB",
                "hyperliquid": "HL", "yahoo": "YH", "computed": "CALC"}
FLOW_STATUSES = ("live", "delayed", "closed")
# How long a source may stay silent (while its market is open) before it is shown as STALE.
# IEX carries ~2-3% of US volume, so thin ETFs (IEF, HYG, USO ...) can go minutes between IEX
# trades: measured 24 Nov 2026, IEF went 150 s without one at 11:30 New York. Their price is
# still the latest real trade, and its true age is always shown.
SOURCE_STALE_S = {"alpaca": 600.0}
DISCLAIMER = ("A heuristic read of public prices, not a fact and not advice. Flows between assets are inferred from "
              "simultaneous opposite moves; nobody can see where each dollar actually goes.")


@dataclass
class SeriesDef:
    id: str
    label: str
    short: str
    group: str
    node: bool
    kind: str
    inst: Instrument
    cfg: dict
    refs: list = field(default_factory=list)          # [(ref_cfg, Instrument)]
    note: str = ""


class Context:
    def __init__(self, engine, cfg, keys, clk, data_dir: Path):
        self.engine, self.cfg, self.keys, self.clock, self.data_dir = engine, cfg, keys, clk, data_dir
        self.session: aiohttp.ClientSession | None = None


class Engine:
    def __init__(self, cfg: dict, keys, clk: clock.Clock | None = None, data_dir: Path | None = None):
        self.cfg = cfg
        self.keys = keys
        self.clock = clk or clock.Clock()
        self.data_dir = data_dir or cfgmod.data_dir(cfg)
        self.cache_dir = self.data_dir / "cache"
        self.instruments: dict[str, Instrument] = {}
        self.derived: list[Derived] = []
        self.series: dict[str, SeriesDef] = {}
        self.connectors: dict = {}
        self.ctx = Context(self, cfg, keys, self.clock, self.data_dir)
        self.news = NewsBook()
        self.vix: dict | None = None
        self.fred: dict = {}
        self.fomc: list = []
        self.calendar: list = []
        self.etf: dict = {}
        self.stable_supply: dict | None = None
        self.crypto_global: dict | None = None
        self.oi: dict[str, list] = {}
        self._cache: dict = {}
        self._snap_cache: tuple[float, dict] | None = None
        self.listeners: list = []
        self.started_at = time.time()
        self.seed_state = {"state": "pending", "done": 0, "total": 0, "errors": []}
        self.resumes = 0
        self._last_wall = time.time()
        self._tasks: list[asyncio.Task] = []
        self.local_tz = cfg.get("general", {}).get("local_timezone", "") or ""
        self.flow_cfg = cfg.get("flow", {})
        self.stale_after = float(self.flow_cfg.get("stale_after_s", 120))
        rc = cfg.get("risk", {})
        self.risk_components = [flow.RiskComponent(c["id"], c["kind"], list(c["inputs"]), float(c["weight"]),
                                                   c.get("label", "")) for c in rc.get("components", [])] or None
        self.risk_scale = float(rc.get("scale", 1.5))
        self.risk_clip = float(rc.get("clip", 4.0))
        self.build()

    # ── construction ────────────────────────────────────────────────────────────────────────
    def instrument(self, source: str, symbol: str, hours: str | None = None, volume: bool = False,
                   label: str | None = None, field_: str = "last") -> Instrument:
        key = f"{source}:{symbol}"
        inst = self.instruments.get(key)
        if inst is None:
            h = hours or cfgmod.DEFAULT_HOURS.get(source, "24/7")
            inst = Instrument(key, source, symbol, h, volume=volume, label=label or symbol)
            inst.extra["field"] = field_
            self.instruments[key] = inst
        else:
            if volume and inst.v5 is None:
                from .series import G5_SIZE, G5_STEP, VolumeRing
                inst.v5 = VolumeRing(G5_STEP, G5_SIZE)
            if field_ != "last":
                inst.extra["field"] = field_
        return inst

    def _has_volume(self, source: str) -> bool:
        return source in ("binance_spot", "binance_fut", "alpaca", "hyperliquid")

    def build(self) -> None:
        for s in cfgmod.enabled_series(self.cfg):
            kind = s.get("kind", "price")
            sid = s["id"]
            if kind == "price":
                src = s["source"]
                inst = self.instrument(src, s["symbol"], s.get("hours"), self._has_volume(src), s.get("label"),
                                       s.get("field", "last"))
            elif kind == "stitched":
                p, x = s["primary"], s["proxy"]
                prim = self.instrument(p["source"], p["symbol"], p.get("hours"), self._has_volume(p["source"]),
                                       p.get("label", p["symbol"]), p.get("field", "last"))
                prox = self.instrument(x["source"], x["symbol"], x.get("hours"), self._has_volume(x["source"]),
                                       x.get("label", x["symbol"]), x.get("field", "last"))
                inst = Stitched(f"stitched:{sid}", prim, prox, beta=float(x.get("beta", 1.0)), label=s.get("label"))
                self.derived.append(inst)
            elif kind == "dxy":
                pairs = {name: self.instrument(s.get("source", "finnhub"), sym, "fx", False, name)
                         for name, sym in s["pairs"].items()}
                inst = DXY(f"dxy:{sid}", pairs, label=s.get("label", "DXY"))
                self.derived.append(inst)
            elif kind == "stable_share":
                basket = {name: self.instrument("binance_spot", sym, "24/7", True, name)
                          for name, sym in s["basket"].items()}
                inst = StablecoinShare(f"share:{sid}", basket, label=s.get("label"))
                self.derived.append(inst)
            else:  # validated in config
                continue
            refs = []
            for r in s.get("reference", []) or []:
                ri = self.instrument(r["source"], r["symbol"], r.get("hours"), self._has_volume(r["source"]),
                                     r.get("label", r["symbol"]), r.get("field", "last"))
                refs.append((r, ri))
            self.series[sid] = SeriesDef(sid, s.get("label", sid), s.get("short", sid.upper()), s.get("group", ""),
                                         bool(s.get("node", True)), kind, inst, s, refs, s.get("note", ""))
        # perpetuals whose funding / open interest we read (no price series of their own)
        bf = cfgmod.connector_cfg(self.cfg, "binance_fut")
        for sym in set(bf.get("funding_symbols", [])) | set(bf.get("oi_symbols", [])):
            self.instrument("binance_fut", sym, "24/7", False, sym, "mark")

    def make_connectors(self) -> None:
        from .connectors import alpaca, binance, coinbase, finnhub, hyperliquid, pollers
        cfg = self.cfg
        c = {}
        by_src: dict[str, list[Instrument]] = {}
        for inst in self.instruments.values():
            by_src.setdefault(inst.source, []).append(inst)
        if cfgmod.connector_enabled(cfg, "binance_spot") and by_src.get("binance_spot"):
            bscfg = cfgmod.connector_cfg(cfg, "binance_spot")
            bs = binance.BinanceSpot(self.ctx, bscfg)
            fast_syms = {s.cfg.get("symbol") for s in self.series.values()
                         if s.kind == "price" and s.cfg.get("source") == "binance_spot" and s.node}
            if not bscfg.get("trade_stream", True):
                fast_syms = set()          # 1-minute klines only: ~1 update/s per coin instead of 15-50
            for inst in by_src["binance_spot"]:
                bs.add(inst.symbol, inst, inst.extra.get("field", "last"), fast=inst.symbol in fast_syms)
            c[bs.name] = bs
        if cfgmod.connector_enabled(cfg, "binance_fut") and by_src.get("binance_fut"):
            bcfg = cfgmod.connector_cfg(cfg, "binance_fut")
            bf = binance.BinanceFutures(self.ctx, bcfg)
            for inst in by_src["binance_fut"]:
                bf.add(inst.symbol, inst, inst.extra.get("field", "last"), volume=inst.v5 is not None)
            c[bf.name] = bf
            if bcfg.get("oi_symbols"):
                c["binance_oi"] = binance.BinanceOpenInterest(self.ctx, bcfg)
        if cfgmod.connector_enabled(cfg, "alpaca") and by_src.get("alpaca"):
            acfg = cfgmod.connector_cfg(cfg, "alpaca")
            al = alpaca.AlpacaStream(self.ctx, acfg)
            for inst in by_src["alpaca"]:
                al.add(inst.symbol, inst)
            c[al.name] = al
            c["alpaca_rest"] = alpaca.AlpacaFallback(self.ctx, acfg, al)
        if cfgmod.connector_enabled(cfg, "alpaca") and cfgmod.connector_cfg(cfg, "alpaca").get("news", True):
            c["alpaca_news"] = alpaca.AlpacaNews(self.ctx, cfgmod.connector_cfg(cfg, "alpaca"))
        if cfgmod.connector_enabled(cfg, "finnhub"):
            fcfg = cfgmod.connector_cfg(cfg, "finnhub")
            if by_src.get("finnhub"):
                fh = finnhub.FinnhubStream(self.ctx, fcfg)
                for inst in by_src["finnhub"]:
                    fh.add(inst.symbol, inst)
                c[fh.name] = fh
            if fcfg.get("news", True):
                c["finnhub_news"] = finnhub.FinnhubNews(self.ctx, fcfg)
        if cfgmod.connector_enabled(cfg, "coinbase") and by_src.get("coinbase"):
            cb = coinbase.CoinbaseStream(self.ctx, cfgmod.connector_cfg(cfg, "coinbase"))
            for inst in by_src["coinbase"]:
                cb.add(inst.symbol, inst)
            c[cb.name] = cb
        if cfgmod.connector_enabled(cfg, "hyperliquid") and by_src.get("hyperliquid"):
            hl = hyperliquid.HyperliquidStream(self.ctx, cfgmod.connector_cfg(cfg, "hyperliquid"))
            for inst in by_src["hyperliquid"]:
                hl.add(inst.symbol, inst, inst.extra.get("field", "oracle"))
            c[hl.name] = hl
        for name, cls in (("cboe", pollers.CboeVix), ("coingecko", pollers.CoinGeckoGlobal),
                          ("defillama", pollers.DefiLlamaStables), ("fred", pollers.Fred), ("fed", pollers.FedCalendar),
                          ("ffcal", pollers.FFCalendar), ("farside", pollers.FarsideEtf), ("rss", pollers.RssFeeds)):
            if cfgmod.connector_enabled(cfg, name):
                c[name] = cls(self.ctx, cfgmod.connector_cfg(cfg, name))
        ycfg = cfgmod.connector_cfg(cfg, "yahoo")
        if ycfg.get("enabled") and ycfg.get("vix_quote", True):
            c["yahoo_vix"] = pollers.YahooVix(self.ctx, ycfg)
        self.connectors = c

    # ── callbacks from connectors ───────────────────────────────────────────────────────────
    def on_news(self, items: list[dict]) -> None:
        self.news.add(items, self.clock.now())

    def watch_symbols(self) -> set[str]:
        """Tickers on the map (for filtering company-news feeds down to what matters here)."""
        out = {"SPY", "QQQ", "TLT", "IEF", "HYG", "GLD", "SLV", "USO", "UUP", "VIXY", "ANT", "BTCUSD", "ETHUSD",
               "IBIT", "ETHA", "NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "TSLA", "COIN", "MSTR"}
        for i in self.instruments.values():
            out.add(i.symbol.replace("USDT", "").upper())
        return out

    def on_vix(self, d: dict) -> None:
        """Keep the freshest VIX quote among the sources (all are 15-minute delayed)."""
        cur = self.vix
        if cur is None or (d.get("quote_ts") or 0) >= (cur.get("quote_ts") or 0):
            if cur is not None and d.get("z_today") is None and cur.get("sigma_daily") and d.get("prev_close"):
                d["z_today"] = math.log(d["level"] / d["prev_close"]) / cur["sigma_daily"]
            if cur is not None and not d.get("sigma_daily"):
                d["sigma_daily"] = cur.get("sigma_daily")
            self.vix = d

    def on_fred(self, d: dict) -> None:
        self.fred.update(d)

    def on_fomc(self, meetings: list) -> None:
        self.fomc = meetings

    def on_calendar(self, events: list) -> None:
        self.calendar = events

    def on_etf_flows(self, d: dict) -> None:
        self.etf.update(d)

    def on_open_interest(self, d: dict) -> None:
        self.oi.update(d)

    def on_crypto_global(self, total: float, pct: dict, updated_at: float, chg24=None) -> None:
        self.crypto_global = {"total_usd": total, "pct": pct, "updated_at": updated_at, "change_24h_pct": chg24}
        now = self.clock.now()
        for d in self.derived:
            if isinstance(d, StablecoinShare):
                # Re-anchor rarely: every re-anchor shifts the level a little (CoinGecko's total covers
                # all coins, the live basket ~80% of them), so it is done at start and every 6 hours,
                # and the history is rebuilt on the new anchor so the series stays consistent.
                if d.anchor_ts is None or now - d.anchor_ts > 6 * 3600:
                    d.set_weights({n: pct.get(n.lower()) for n in d.basket})
                    d.set_anchor(total, min(updated_at, now))
                    if d.anchor_px and self.seed_state.get("state") == "done":
                        d.rebuild(now)
                        self._cache.clear()

    def on_stable_supply(self, d: dict, hist: list) -> None:
        self.stable_supply = d
        for s in self.derived:
            if isinstance(s, StablecoinShare):
                s.set_supply(d["total"], d.get("observed_at", self.clock.now()), hist)
                if self.crypto_global and not s.anchor_px:
                    g = self.crypto_global
                    s.set_weights({n: g["pct"].get(n.lower()) for n in s.basket})
                    s.set_anchor(g["total_usd"], min(g["updated_at"], self.clock.now()))
                    if s.anchor_px and self.seed_state.get("state") == "done":
                        s.rebuild(self.clock.now())
                        self._cache.clear()

    # ── history ─────────────────────────────────────────────────────────────────────────────
    async def seed_history(self, incremental: bool = False, since: float | None = None) -> None:
        """Fill the grids: disk cache first, then REST for whatever is missing. `since` (after a sleep)
        forces a re-download from that time: the gap was filled with the last pre-sleep price by the
        clock tick, so "the newest cached step" would wrongly say there is nothing to fetch."""
        from .connectors import alpaca as alp, binance as bn, hyperliquid as hl, pollers
        now = self.clock.now()
        days = int(self.cfg.get("general", {}).get("history_days", 35))
        horizon = now - days * 86400

        def start_of(inst):
            t = history.last_good_ts(inst) or horizon
            if since is not None:
                t = min(t, since)
            return max(horizon, t - 600)
        st = self.seed_state
        st.update(state="running", done=0, errors=[], started=clock.iso(now))
        insts = [i for i in self.instruments.values() if i.source in ("binance_spot", "binance_fut", "alpaca", "hyperliquid")]
        st["total"] = len(insts) + 2
        if not incremental:
            for i in insts:
                history.load(self.cache_dir, i, now)
        sem = asyncio.Semaphore(3)

        async def one(inst: Instrument):
            async with sem:
                try:
                    since5 = start_of(inst)
                    since1 = max(now - 86400 + 60, since5 - 600 if incremental else now - 86400 + 60)
                    field_ = inst.extra.get("field", "last")
                    if inst.source in ("binance_spot", "binance_fut"):
                        mk = "spot" if inst.source == "binance_spot" else "fut"
                        if mk == "fut" and inst.symbol in self._funding_only():
                            return
                        pr = "index" if (mk == "fut" and field_ == "index") else "last"
                        k5 = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "5m", int(since5 * 1000), int(now * 1000), pr)
                        vol = k5
                        if pr == "index":
                            vol = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "5m", int(since5 * 1000), int(now * 1000), "last")
                        k1 = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "1m", int(since1 * 1000), int(now * 1000), pr)
                        inst.seed("g5", [(t, c) for t, c, _, _ in k5], now, [(t, q, tb) for t, _, q, tb in vol])
                        inst.seed("g1", [(t, c) for t, c, _, _ in k1], now)
                    elif inst.source == "hyperliquid":
                        k5 = await hl.fetch_candles(self.connectors["hyperliquid"], inst.symbol, "5m", since5, now)
                        k1 = await hl.fetch_candles(self.connectors["hyperliquid"], inst.symbol, "1m", since1, now)
                        inst.seed("g5", [(t, c) for t, c, _, _ in k5], now, [(t, q, tb) for t, _, q, tb in k5])
                        inst.seed("g1", [(t, c) for t, c, _, _ in k1], now)
                except Exception as e:  # noqa: BLE001
                    st["errors"].append(self.keys.redact(f"{inst.key}: {e}")[:200])
                    log.warning("history %s: %s", inst.key, self.keys.redact(e))
                finally:
                    st["done"] += 1

        alp_insts = [i for i in insts if i.source == "alpaca"]
        tasks = [one(i) for i in insts if i.source != "alpaca"]

        async def alpaca_all():
            conn = self.connectors.get("alpaca")
            if conn is None or not alp_insts or not conn.available()[0]:
                st["done"] += len(alp_insts)
                return
            try:
                since5 = min(start_of(i) for i in alp_insts)
                b5 = await alp.fetch_bars(conn, [i.symbol for i in alp_insts], "5Min", since5, None, conn.cfg.get("feed", "iex"))
                b1 = await alp.fetch_bars(conn, [i.symbol for i in alp_insts], "1Min", now - 86400 + 60, None, conn.cfg.get("feed", "iex"))
                for i in alp_insts:
                    rows = b5.get(i.symbol, [])
                    if rows:
                        i.seed("g5", [(t, c) for t, c, _ in rows], now, [(t, q, math.nan) for t, _, q in rows])
                    rows1 = b1.get(i.symbol, [])
                    if rows1:
                        i.seed("g1", [(t, c) for t, c, _ in rows1], now)
            except Exception as e:  # noqa: BLE001
                st["errors"].append(self.keys.redact(f"alpaca: {e}")[:200])
                log.warning("history alpaca: %s", self.keys.redact(e))
            finally:
                st["done"] += len(alp_insts)

        async def yahoo_dxy():
            if not cfgmod.connector_enabled(self.cfg, "yahoo"):
                return
            for s in self.series.values():
                if s.kind != "dxy":
                    continue
                h = s.cfg.get("history") or {}
                if h.get("source") != "yahoo":
                    continue
                try:
                    rows = await pollers.yahoo_chart(self.ctx.session, h["symbol"], "5m", "60d")
                    rows1 = await pollers.yahoo_chart(self.ctx.session, h["symbol"], "1m", "1d")
                    d = s.inst
                    # ICE's DXY arrives 10 minutes late: seed only up to its last bar, never carry it
                    # forward over the minutes the live computed DXY already covers
                    if rows:
                        d.seed("g5", rows, rows[-1][0] + 300)
                    if rows1:
                        d.seed("g1", rows1, rows1[-1][0] + 60)
                    d.extra["history_source"] = f"ICE DXY via Yahoo ({h['symbol']}), 5-minute"
                except Exception as e:  # noqa: BLE001
                    st["errors"].append(f"yahoo {h.get('symbol')}: {e}"[:200])
            st["done"] += 1

        await asyncio.gather(*tasks, alpaca_all(), yahoo_dxy())
        oi = self.connectors.get("binance_oi")
        if oi is not None and not incremental:
            try:
                await oi.seed()
                self.on_open_interest({s: list(oi.hist[s]) for s in oi.symbols})
            except Exception as e:  # noqa: BLE001
                st["errors"].append(f"open interest: {e}"[:200])
        st["done"] += 1
        now = self.clock.now()
        for inst in self.instruments.values():
            inst.roll(now)
        for d in self.derived:
            if isinstance(d, DXY):
                continue   # its history comes straight from ICE's DXY (Finnhub has no free FX history)
            d.rebuild(now)
        self._cache.clear()
        st.update(state="done", finished=clock.iso(now))
        self.save_cache()
        log.info("history ready (%d instruments, %d errors)", len(insts), len(st["errors"]))

    def _funding_only(self) -> set:
        used = set()
        for s in self.series.values():
            for i in [s.inst] + [r[1] for r in s.refs] + list(getattr(s.inst, "inputs", [])):
                if i.source == "binance_fut":
                    used.add(i.symbol)
        return {i.symbol for i in self.instruments.values() if i.source == "binance_fut"} - used

    def save_cache(self) -> None:
        n = 0
        for i in self.instruments.values():
            try:
                n += history.save(self.cache_dir, i)
            except OSError as e:
                log.warning("cache %s: %s", i.key, e)
        log.debug("cached %d instruments", n)

    # ── the clock tick ──────────────────────────────────────────────────────────────────────
    def tick(self, now: float | None = None) -> None:
        now = now or self.clock.now()
        for inst in self.instruments.values():
            inst.roll(now)
        self._ensure_share_anchor(now)
        for d in self.derived:
            d.refresh(now)

    def _ensure_share_anchor(self, now: float) -> None:
        """Anchor the stablecoin share as soon as everything it needs is in: the total market cap,
        the supply AND the basket's price history (cached values can arrive before any price)."""
        g, sup = self.crypto_global, self.stable_supply
        if not g or not sup or self.seed_state.get("state") != "done":
            return
        for d in self.derived:
            if isinstance(d, StablecoinShare) and not d.anchor_px:
                d.set_supply(sup["total"], sup.get("observed_at", now), d.supply_hist or None)
                d.set_weights({n: g["pct"].get(n.lower()) for n in d.basket})
                d.set_anchor(g["total_usd"], min(g["updated_at"], now))
                if d.anchor_px:
                    d.rebuild(now)
                    self._cache.clear()

    def _cached(self, key, ttl: float, fn, now: float):
        hit = self._cache.get(key)
        if hit is not None and now - hit[0] < ttl:
            return hit[1]
        v = fn()
        self._cache[key] = (now, v)
        return v

    # ── reading one series ──────────────────────────────────────────────────────────────────
    def stale_limit(self, inst: Instrument, s: SeriesDef | None = None) -> float:
        if s is not None and s.cfg.get("stale_after_s"):
            return float(s.cfg["stale_after_s"])
        return SOURCE_STALE_S.get(inst.source, self.stale_after)

    def status_of(self, s: SeriesDef, now: float) -> str:
        inst = s.inst
        if not isnum(inst.price):
            return "no_data"
        if inst.delayed_s >= 60:
            return "delayed"
        if isinstance(inst, Stitched):
            src = inst.active_source(now)
            if not isnum(src.price):
                return "no_data"
            if not src.live_seen:                      # only cached / downloaded history so far
                return "stale"
            age = src.age(now)
            return "live" if age is not None and age <= self.stale_limit(src, s) else "stale"
        if not clock.market_open(inst.hours, now):
            return "closed"
        if not inst.live_seen:                         # history from the cache is not live data
            return "stale"
        age = inst.age(now)
        if age is None or age > self.stale_limit(inst, s):
            return "stale"
        return "live"

    def source_info(self, s: SeriesDef, now: float) -> dict:
        inst = s.inst
        if isinstance(inst, Stitched):
            a = inst.active_source(now)
            proxy = a is inst.proxy
            p = inst.primary
            why = (f"{p.symbol} market closed" if not clock.market_open(p.hours, now)
                   else f"no live {p.symbol} data: Alpaca key missing or stream down")
            return {"id": a.source, "name": SOURCE_NAMES.get(a.source, a.source), "short": SOURCE_SHORT.get(a.source, ""),
                    "symbol": a.symbol, "field": a.extra.get("field", "last"), "proxy": proxy,
                    "description": (f"{inst.proxy.symbol} {inst.proxy.extra.get('field', '')} ÷ {inst.beta:g} "
                                    f"(24/7 proxy; {why})") if proxy else f"{p.symbol} on IEX (real-time)"}
        if isinstance(inst, DXY):
            return {"id": "finnhub", "name": "computed from OANDA FX via Finnhub", "short": "FX", "symbol": "DXY",
                    "field": "ICE formula", "proxy": False,
                    "description": "ICE DXY formula on EUR, JPY, GBP, CAD, SEK, CHF ticks (measured within 0.002 of ICE)"}
        if isinstance(inst, StablecoinShare):
            return {"id": "computed", "name": "DefiLlama supply ÷ CoinGecko market cap, moved by live Binance prices",
                    "short": "CALC", "symbol": "STABLE.D", "field": "share %", "proxy": False,
                    "description": "USD stablecoins / total crypto market cap"}
        fld = inst.extra.get("field", "last")
        desc = {"index": "Binance index of the underlying (1 s)", "last": "last trade", "mark": "mark price"}.get(fld, fld)
        return {"id": inst.source, "name": SOURCE_NAMES.get(inst.source, inst.source),
                "short": SOURCE_SHORT.get(inst.source, ""), "symbol": inst.symbol, "field": fld,
                "proxy": inst.source == "binance_fut", "description": desc}

    def read(self, s: SeriesDef, w: stats.Window, now: float) -> dict:
        inst = s.inst
        tf = w.tf
        r, pct, p0 = stats.window_return(inst, w, now)
        sig, n, method = self._cached(("sigma", s.id, tf), 60 if tf != "1h" else 120,
                                      lambda: stats.sigma_for(tf, inst, now), now)
        z = r / sig if isnum(r) and isnum(sig) and sig > 0 else None
        traded = self._cached(("traded", s.id, tf), 30, lambda: stats.traded_in_window(inst, w, now), now)
        if not traded:
            z = None                    # market shut for the whole window: no information, not "0"
        status = self.status_of(s, now)
        if status in ("live", "stale", "closed") and z is None and isnum(inst.price):
            status = "warming_up" if status != "closed" else status
        age = inst.age(now)
        if isinstance(inst, Stitched):
            age = inst.active_source(now).age(now)
        out = {
            "id": s.id, "label": s.label, "short": s.short, "group": s.group, "node": s.node,
            "x": s.cfg.get("x"), "y": s.cfg.get("y"),
            "price": inst.price if isnum(inst.price) else None, "ref_price": p0 if isnum(p0) else None,
            "change_pct": round(pct, 4) if isnum(pct) else None, "log_return": r if isnum(r) else None,
            "z": round(z, 3) if z is not None else None,
            "sigma_pct": round((math.exp(sig) - 1) * 100, 4) if isnum(sig) else None,
            "sigma_method": method, "sigma_samples": n,
            "direction": None if z is None else ("inflow" if z >= self.flow_cfg.get("theta", 0.5)
                                                else "outflow" if z <= -self.flow_cfg.get("theta", 0.5) else "neutral"),
            "status": status, "age_s": None if age is None else round(age, 2),
            "latency_ms": None if (lm := inst.latency_ms()) is None else round(lm, 1),
            "updated_at": clock.iso(inst.ts) if inst.ts else None,
            "source": self.source_info(s, now), "note": s.note,
        }
        if not isnum(p0):                       # e.g. a new listing: no price yet at the window start
            first = self._cached(("first", s.id), 600, lambda: stats.first_data_ts(inst), now)
            if first is not None and first > w.start:
                out["history_from"] = clock.iso(first)
        vol_src = inst.primary if isinstance(inst, Stitched) and clock.market_open(inst.primary.hours, now) else (
            inst.proxy if isinstance(inst, Stitched) else inst)
        out["volume"] = self._cached(("vol", s.id, tf), 30 if tf != "1w" else 120,
                                     lambda: stats.volume_read(vol_src, w, now), now) if vol_src.v5 is not None else None
        out["spark"] = self._cached(("spark", s.id, tf), 5 if tf == "1h" else 20,
                                    lambda: stats.sparkline(inst, w, now), now)
        if out["spark"] and isnum(inst.price) and isnum(p0) and p0 > 0:
            out["spark"] = out["spark"][:-1] + [round((inst.price / p0 - 1) * 100, 3)]
        out["derivatives"] = self._derivs(s, w, now)
        out["reference"] = [self._ref(rc, ri, s, w, now) for rc, ri in s.refs]
        if isinstance(inst, StablecoinShare):
            out["extra"] = self._stable_extra(inst, now)
        if isinstance(inst, DXY):
            out["extra"] = {"pairs": {n: (i.price if isnum(i.price) else None) for n, i in inst.pairs.items()},
                            "history": inst.extra.get("history_source")}
        return out

    def _derivs(self, s: SeriesDef, w, now) -> dict | None:
        inst = s.inst
        sym = None
        if s.id in ("btc", "eth"):
            sym = f"{s.id.upper()}USDT"
            perp = self.instruments.get(f"binance_fut:{sym}")
        elif inst.source == "binance_fut":
            sym, perp = inst.symbol, inst
        else:
            return None
        if perp is None:
            return None
        fr = perp.extra.get("funding_rate")
        out = {"symbol": sym, "funding_rate": fr,
               "funding_apr_pct": None if fr is None else round(fr * 3 * 365 * 100, 2),
               "mark": perp.extra.get("mark"), "index": perp.extra.get("index")}
        oi = self.connectors.get("binance_oi")
        if oi is not None and sym in getattr(oi, "symbols", []):
            ch = oi.change_pct(sym, w.start)
            out["oi_change_pct"] = None if ch is None else round(ch, 2)
            h = oi.hist.get(sym)
            out["open_interest"] = h[-1][1] if h else None
        return out

    def _ref(self, rc: dict, ri: Instrument, s: SeriesDef, w, now) -> dict:
        _, pct, _ = stats.window_return(ri, w, now)
        basis = None
        main = s.inst.price
        if not isnum(ri.price):
            st = "no_data"
        elif not clock.market_open(ri.hours, now):
            st = "closed"
        elif not ri.live_seen or (ri.age(now) is not None and ri.age(now) > self.stale_limit(ri)):
            st = "stale"
        else:
            st = "live"
        # basis only between two LIVE prices of the same thing (same units, both trading now)
        if st == "live" and isnum(main) and ri.price > 0 and abs(main / ri.price - 1) < 0.05:
            basis = round((main / ri.price - 1) * 100, 3)
        return {"source": ri.source, "source_name": SOURCE_NAMES.get(ri.source, ri.source), "symbol": ri.symbol,
                "price": ri.price if isnum(ri.price) else None, "change_pct": round(pct, 3) if isnum(pct) else None,
                "basis_pct": basis, "age_s": None if ri.age(now) is None else round(ri.age(now), 1), "status": st,
                "latency_ms": None if (lm := ri.latency_ms()) is None else round(lm, 1)}

    def _stable_extra(self, inst: StablecoinShare, now: float) -> dict:
        sup = self.stable_supply or {}
        g = self.crypto_global or {}
        return {"supply_usd": sup.get("total"), "supply_change_24h_usd": sup.get("d1"),
                "supply_change_7d_usd": sup.get("w1"), "supply_change_30d_usd": sup.get("m1"),
                "supply_top": sup.get("top"), "supply_age_s": None if not sup else round(now - sup.get("observed_at", now)),
                "total_crypto_cap_usd": g.get("total_usd"),
                "anchor_age_s": None if inst.anchor_ts is None else round(now - inst.anchor_ts),
                "note": "Rises mechanically when crypto prices fall; supply change = real new cash."}

    # ── snapshots ───────────────────────────────────────────────────────────────────────────
    def _returns(self, tf: str, w, now):
        return self._cached(("rets", tf), 10 if tf == "1h" else 30,
                            lambda: {sid: stats.step_returns(s.inst, w, now) for sid, s in self.series.items()}, now)

    def build_tf(self, tf: str, now: float) -> dict:
        w = stats.window_for(tf, now)
        reads = {sid: self.read(s, w, now) for sid, s in self.series.items()}
        z_all = {sid: r["z"] for sid, r in reads.items() if r["z"] is not None and r["status"] in FLOW_STATUSES}
        z_nodes = {sid: v for sid, v in z_all.items() if self.series[sid].node}
        rets = self._returns(tf, w, now)
        memo = {}

        def corr_fn(a, b):
            k = (a, b)
            if k not in memo:
                memo[k] = self._cached(("corr", tf, a, b), 30, lambda: stats.corr(rets.get(a, {}), rets.get(b, {})), now)
            return memo[k]

        fc = self.flow_cfg
        edges = flow.infer_edges(z_nodes, corr_fn, theta=fc.get("theta", 0.5), max_edges=fc.get("max_edges", 16),
                                 per_node=fc.get("per_node", 5), min_weight=fc.get("min_weight", 0.3))
        risk = flow.risk_composite(z_all, self.risk_components, self.risk_scale, self.risk_clip)
        edges_d = [e.as_dict() for e in edges]
        ctx = self._narrative_ctx(tf, w, reads, now)
        bullets = narrative.build(tf, reads, edges_d, risk, ctx)
        nodes = [r for r in reads.values() if r["node"]]
        aux = [r for r in reads.values() if not r["node"]]
        for n in nodes:
            n["inflow_share"] = round(sum(e.share for e in edges if e.dst == n["id"]), 3)
            n["outflow_share"] = round(sum(e.share for e in edges if e.src == n["id"]), 3)
        return {"timeframe": tf, "label": stats.TF_LABELS[tf],
                "window": {"start": clock.iso(w.start), "end": clock.iso(w.end), "label": w.label},
                "nodes": nodes, "aux": aux, "edges": edges_d, "risk": risk, "narrative": bullets,
                "tiles": self._tiles(tf, w, reads, now)}

    def _narrative_ctx(self, tf, w, reads, now) -> dict:
        ctx = {"stable_supply": self.stable_supply or {}, "etf": self.etf, "fed": self.fed_info(now) or {},
               "events": self.upcoming_events(now, hours=36, limit=2)}
        btc = reads.get("btc", {}).get("derivatives") or {}
        ctx["btc_deriv"] = {"funding_apr_pct": btc.get("funding_apr_pct"), "oi_change_pct": btc.get("oi_change_pct")}
        cav = []
        sess = clock.us_equity_session(now)
        eq = [r["label"] for r in reads.values() if r["node"] and r["source"]["id"] == "binance_fut"
              and r["group"] == "equities"]
        if sess != "regular" and eq:
            cav.append(f"US stock market {'closed' if sess == 'closed' else sess + '-market'}: "
                       f"{', '.join(eq)} are read from Binance's 24/7 perpetuals.")
        b = reads.get("bonds")
        if b and b["source"].get("proxy"):
            cav.append("Bonds are read from the TMF perpetual (3× TLT, thin) until US trading resumes."
                       if sess != "regular" else
                       "Bonds are read from the TMF perpetual (3× TLT, thin): no live TLT data "
                       "(Alpaca key missing or its stream is down).")
        if not clock.fx_open(now) and "usd" in reads:
            cav.append("FX market closed for the weekend: the dollar index holds Friday's close.")
        ctx["caveats"] = cav
        return ctx

    def _tiles(self, tf, w, reads, now) -> dict:
        t = {}
        v = self.vix
        vp = reads.get("vix_proxy")
        t["vix"] = {"label": "FEAR · VIX", "level": v.get("level") if v else None,
                    "change_pct": v.get("change_pct") if v else None,
                    "quote_time": v.get("quote_time") if v else None,
                    "age_s": round(now - v["quote_ts"]) if v and v.get("quote_ts") else None,
                    "status": "delayed" if v else "no_data", "z_today": None if not v or v.get("z_today") is None else round(v["z_today"], 2),
                    "source": "Cboe delayed quotes (15 min)" if v else None,
                    "proxy": None if not vp else {"label": "UVXY perp (live)", "change_pct": vp.get("change_pct"),
                                                   "z": vp.get("z"), "status": vp.get("status"), "age_s": vp.get("age_s")}}
        u = reads.get("usd")
        if u:
            t["dxy"] = {"label": "DOLLAR · DXY", "level": u.get("price"), "change_pct": u.get("change_pct"),
                        "z": u.get("z"), "status": u.get("status"), "age_s": u.get("age_s"),
                        "source": "computed from OANDA FX (Finnhub)"}
        f10 = self.fred.get("DGS10")
        if f10:
            ch = None if f10.get("prev_value") is None else round((f10["value"] - f10["prev_value"]) * 100)
            wk = None if f10.get("week_ago_value") is None else round((f10["value"] - f10["week_ago_value"]) * 100)
            t["us10y"] = {"label": "US 10Y YIELD", "value": f10["value"], "date": f10["date"], "change_bp": ch,
                          "change_5d_bp": wk, "status": "daily", "source": "FRED DGS10 (daily, ~1 day lag)",
                          "live_hint": None}
            b = reads.get("bonds")
            if b and b.get("change_pct") is not None:
                t["us10y"]["live_hint"] = {"label": "TLT " + stats.TF_LABELS[tf], "change_pct": b["change_pct"],
                                           "meaning": "bonds up = yields down"}
        f2 = self.fred.get("DGS2")
        if f10 and f2:
            t["curve_2s10s_bp"] = round((f10["value"] - f2["value"]) * 100)
        hy = self.fred.get("BAMLH0A0HYM2")
        if hy:
            t["hy_spread"] = {"value": hy["value"], "date": hy["date"],
                              "change_bp": None if hy.get("prev_value") is None else round((hy["value"] - hy["prev_value"]) * 100)}
        btc = reads.get("btc", {}).get("derivatives")
        if btc:
            t["crypto_leverage"] = {"label": "BTC LEVERAGE", "funding_apr_pct": btc.get("funding_apr_pct"),
                                    "oi_change_pct": btc.get("oi_change_pct"), "open_interest_btc": btc.get("open_interest")}
        if self.etf:
            t["etf_flows"] = {k: {kk: v.get(kk) for kk in ("last_date", "last_total_musd", "streak", "week_total_musd")}
                              for k, v in self.etf.items()}
        return t

    def fed_info(self, now: float) -> dict | None:
        return fedmod.next_decision(self.fomc, now, self.local_tz) if self.fomc else None

    MAJOR_CCY = ("USD", "EUR", "JPY", "GBP", "CNY")

    def upcoming_events(self, now: float, hours: float = 48, limit: int = 6) -> list[dict]:
        """High-impact releases soon: US first (they move every node on this map), then other majors."""
        out = []
        evs = [e for e in self.calendar if e.get("impact") == "High" and e.get("country") in self.MAJOR_CCY
               and now - 1800 <= e["ts"] <= now + hours * 3600]
        evs.sort(key=lambda e: (e.get("country") != "USD", e["ts"]))
        for e in evs:
            ny = dt.datetime.fromtimestamp(e["ts"], clock.NY)
            loc = clock.to_local(e["ts"], self.local_tz)
            out.append({"title": e["title"], "country": e["country"], "impact": e["impact"], "at": clock.iso(e["ts"]),
                        "when_text": f"{ny:%a %H:%M} New York / {loc:%a %H:%M} {clock.local_label(self.local_tz)}",
                        "in_s": round(e["ts"] - now), "forecast": e.get("forecast"), "previous": e.get("previous")})
            if len(out) >= limit:
                break
        return out

    def sources(self, now: float) -> list[dict]:
        out = []
        for name, c in self.connectors.items():
            d = c.health.as_dict(now)
            insts = [i for i in self.instruments.values() if i.source == name]
            lats = sorted(x for i in insts if (x := i.latency_ms()) is not None)
            if lats:
                d["latency_ms_median"] = round(lats[len(lats) // 2], 1)
                d["instruments"] = len(insts)
            out.append(d)
        return out

    def process_info(self) -> dict:
        def get():
            info = {"pid": os.getpid(), "uptime_s": round(time.time() - self.started_at)}
            try:
                import psutil
                p = psutil.Process()
                if not hasattr(self, "_ps"):
                    self._ps = p
                    p.cpu_percent(None)
                info["cpu_percent"] = round(self._ps.cpu_percent(None), 1)
                info["rss_mb"] = round(p.memory_info().rss / 1e6, 1)
                info["cpu_count"] = psutil.cpu_count()
            except Exception:  # noqa: BLE001 - psutil optional
                pass
            return info
        return self._cached(("proc",), 5, get, time.time())

    def snapshot(self, now: float | None = None, tfs=stats.TIMEFRAMES) -> dict:
        now = now or self.clock.now()
        if self._snap_cache and now - self._snap_cache[0] < 0.5 and tuple(tfs) == stats.TIMEFRAMES:
            return self._snap_cache[1]
        fed = self.fed_info(now)
        snap = {
            "schema": SCHEMA_ID, "version": __version__, "generated_at": clock.iso(now), "generated_ts": round(now, 3),
            "default_timeframe": self.cfg.get("general", {}).get("default_timeframe", "today"),
            "timeframes": {tf: self.build_tf(tf, now) for tf in tfs},
            "fed": fed, "events": self.upcoming_events(now),
            "news": self.news.latest(25, now), "news_version": self.news.version,
            "market": clock.market_status(now),
            "sources": self.sources(now),
            "meta": {"heuristic": True, "disclaimer": DISCLAIMER, "clock": self.clock.info(),
                     "local_timezone": self.local_tz, "history": dict(self.seed_state),
                     "process": self.process_info(), "resumes": self.resumes,
                     "keys": self.keys.describe(), "config_files": self.cfg.get("_files", [])},
        }
        if tuple(tfs) == stats.TIMEFRAMES:
            self._snap_cache = (now, snap)
        return snap

    # ── lifecycle ───────────────────────────────────────────────────────────────────────────
    async def start(self) -> None:
        conn = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(), ttl_dns_cache=900, limit=40,
                                    enable_cleanup_closed=True)
        self.ctx.session = aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=45, connect=20),
                                                 headers={"User-Agent": "moneyflow/1.0"})
        await self.clock.sync()
        self.make_connectors()
        fedc = self.connectors.get("fed")
        if fedc is not None:
            fedc.load_cache()
        for name, c in self.connectors.items():
            self._tasks.append(asyncio.create_task(c.run(), name=f"conn:{name}"))
        self._tasks.append(asyncio.create_task(self._seed_then_track(), name="seed"))
        self._tasks.append(asyncio.create_task(self._loop(), name="tick"))
        self._tasks.append(asyncio.create_task(self._housekeeping(), name="housekeeping"))

    async def _seed_then_track(self):
        """Load history; if anything failed (e.g. the PC started before the network was up), retry
        every few minutes until every instrument has its history."""
        delay = 60.0
        first = True
        while True:
            try:
                await self.seed_history(incremental=not first)
            except Exception as e:  # noqa: BLE001
                self.seed_state.update(state="failed", errors=[self.keys.redact(e)])
                log.exception("history seeding failed")
            first = False
            missing = [i.key for i in self.instruments.values()
                       if i.source in ("binance_spot", "binance_fut", "alpaca", "hyperliquid")
                       and i.g5.last_k is None and i.symbol not in self._funding_only()
                       and not (i.source == "alpaca" and not self.keys.has("ALPACA_KEY_ID", "ALPACA_SECRET"))]
            if self.seed_state.get("state") == "done" and not self.seed_state.get("errors") and not missing:
                return
            self.seed_state["retry_in_s"] = delay
            log.info("history incomplete (%d errors, %d instruments without history): retrying in %.0f s",
                     len(self.seed_state.get("errors") or []), len(missing), delay)
            await asyncio.sleep(delay)
            delay = min(900.0, delay * 2)

    async def _loop(self):
        while True:
            t0 = time.monotonic()
            try:
                self.tick()
                for fn in list(self.listeners):
                    try:
                        await fn(self)
                    except Exception:  # noqa: BLE001
                        log.exception("listener failed")
            except Exception:  # noqa: BLE001
                log.exception("tick failed")
            await asyncio.sleep(max(0.05, 1.0 - (time.monotonic() - t0)))

    async def _housekeeping(self):
        """Sleep/resume detection (wall clock jumps), NTP re-sync, periodic cache saves."""
        last_sync = time.time()
        last_save = time.time()
        while True:
            before = time.time()
            await asyncio.sleep(5)
            gap = time.time() - before
            if gap > 25:
                self.resumes += 1
                log.warning("wall clock jumped %.0fs (sleep/hibernate?): resyncing and reconnecting", gap)
                await self.on_resume(gap)
                last_sync = time.time()
            if time.time() - last_sync > 1800:
                await self.clock.sync()
                last_sync = time.time()
            if time.time() - last_save > 1800:
                await asyncio.get_running_loop().run_in_executor(None, self.save_cache)
                last_save = time.time()

    async def on_resume(self, gap: float) -> None:
        conn = self.ctx.session.connector if self.ctx.session else None
        if conn is not None and hasattr(conn, "clear_dns_cache"):
            conn.clear_dns_cache()
        gap_start = self.clock.now() - gap
        await self.clock.sync()
        for c in self.connectors.values():
            c.kick()
        if gap > 120:
            self._refill_task = asyncio.create_task(self._refill(gap_start))

    async def _refill(self, gap_start: float):
        await asyncio.sleep(10)     # let the network come back first
        try:
            await self.seed_history(incremental=True, since=gap_start)
            log.info("history gap since %s refilled", clock.iso(gap_start))
        except Exception:  # noqa: BLE001
            log.exception("gap refill failed")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        try:
            self.save_cache()
        except Exception:  # noqa: BLE001
            log.exception("final cache save failed")
        if self.ctx.session is not None:
            await self.ctx.session.close()
