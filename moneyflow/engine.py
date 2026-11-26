"""The engine: builds instruments and series from the config, runs the connectors, seeds history,
rolls the grids once a second and assembles snapshots (the JSON every client receives)."""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import os
import threading
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
# the sources whose history is downloaded and cached (the cache is loaded for these at the start)
HISTORY_SOURCES = ("binance_spot", "binance_fut", "alpaca", "hyperliquid")
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
        # filling what a sleep or a network outage left empty (shown under /api/health "backfill")
        self.backfill_state: dict = {"state": "idle"}
        self._backfill_task: asyncio.Task | None = None
        self._backfill_wake: asyncio.Event | None = None
        self._feed_waiters: set[asyncio.Event] = set()   # retries that a recovered feed cuts short
        self._save_lock = threading.Lock()  # one cache save at a time (they run in worker threads)
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
        # the notable-stock slot(s), see stocks.py
        self.stock_cfg = cfg.get("stocks", {}) or {}
        self.stock_screen: dict = {}             # tf -> {"picks", "top", "screened"} of the latest screen
        self.stock_meta: dict = {"at": None, "universe": 0, "missing": []}
        self._stocks: dict[str, dict] = {}       # perpetual -> {"inst", "ticker", "picked_at", "ready", "owned"}
        self._eased: dict[tuple, float] = {}     # (window, perpetual) -> the screen run at which its move eased
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
            if s.get("calendar"):          # the days its σ is measured on (clock.CALENDARS)
                inst.calendar = s["calendar"]
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
        if self.stock_slots() and "binance_fut" in c:
            from . import stocks
            c["stocks"] = stocks.StockScanner(self.ctx, self.stock_cfg)
        self.connectors = c

    # ── the notable-stock slot(s) ───────────────────────────────────────────────────────────
    def stock_slots(self) -> int:
        sc = self.stock_cfg
        return max(0, int(sc.get("slots", 1))) if sc.get("enabled", True) and sc.get("universe") else 0

    def on_stock_screen(self, book, now: float, missing=()) -> None:
        """A fresh screen of the universe: rank each window, start live data for new picks."""
        from . import stocks
        sc, slots = self.stock_cfg, self.stock_slots()
        z_min, score_min = float(sc.get("z_min", 2.0)), float(sc.get("score_min", 2.5))
        prev = self.stock_screen
        res = {}
        for tf in stats.TIMEFRAMES:
            exact = self._node_reads(tf, now)
            r = stocks.screen(book, tf, now, z_min, score_min, slots, exact=exact)
            # stocks the map still reads (picked lately) among the best: on its numbers too, or a
            # borderline one went in and out of the slot at every screen with unchanged prices
            near = [s["symbol"] for s in r["top"] if s["symbol"] in self._stocks and s["symbol"] not in exact]
            more = self._node_reads(tf, now, syms=near) if near else {}
            if more:
                r = stocks.screen(book, tf, now, z_min, score_min, slots, exact={**exact, **more})
            # a shown pick whose history still has a sleep's gap is held, not dropped on hourly bars
            # alone (it was, for good: once off the slot it was never re-read on the map)
            held = [dict(p, held=True) for p in (prev.get(tf) or {}).get("picks", [])
                    if p["symbol"] not in {q["symbol"] for q in r["picks"]} and self._has_holes(p["symbol"])]
            if held and len(r["picks"]) < slots:
                r = dict(r, picks=r["picks"] + held[:slots - len(r["picks"])])
            res[tf] = r
        if clock.day_roll(now) < clock.day_roll(self.clock.now()):
            # a scan begun before 17:00 ended after it: its TODAY is the old session's; screen again
            res["today"] = dict(res["today"], picks=[], top=[], rolled=True)
            self._rescreen_soon()
        before = {tf: [p["symbol"] for p in (prev.get(tf) or {}).get("picks", [])] for tf in res}
        self.stock_screen = res
        self.stock_meta = {"at": now, "universe": len(book.hourly), "missing": list(missing)}
        self._eased.clear()                        # a new screen: whatever eased is judged afresh
        for r in res.values():
            for p in r["picks"]:
                self._want_stock(p["symbol"], p["ticker"], now)
        self._prune_stocks(now)
        if any(before[tf] != [p["symbol"] for p in res[tf]["picks"]] for tf in res):
            self._cache.clear()

    def _has_holes(self, sym: str) -> bool:
        ent = self._stocks.get(sym)
        return ent is not None and any(h is not None for h in ent["inst"].holes.values())

    def _rescreen_soon(self) -> None:
        sc = self.connectors.get("stocks")
        if sc is not None:
            sc.soon = True                     # if it is mid-scan, its next sleep is short...
            sc.kick()                          # ...and if it is asleep, it wakes now

    def _node_reads(self, tf: str, now: float, syms=None) -> dict:
        """The map's own numbers for the stocks it shows now (the current picks, loaded, with no hole
        left by a sleep or an outage: until the back-fill is done its grid lacks those steps):
        {perpetual: (log return, σ, rvol, live price)}, for the screen to rank them on exactly those.
        Only the current picks: every stock kept for 2 h cost the loop ~25 ms per screen."""
        out, w = {}, stats.window_for(tf, now)
        shown = set(syms) if syms is not None else {p["symbol"] for r in self.stock_screen.values()
                                                     for p in (r or {}).get("picks", [])}
        for sym, ent in self._stocks.items():
            inst = ent["inst"]
            if (sym not in shown or not ent.get("ready") or not isnum(inst.price)
                    or any(h is not None for h in inst.holes.values())):
                continue
            try:
                r = stats.window_return(inst, w, now)[0]
                sig = stats.sigma_for(tf, inst, now)[0]
                vol = stats.volume_read(inst, w, now) if inst.v5 is not None else None
            except Exception:  # noqa: BLE001 - the hourly screen still covers it
                continue
            if isnum(r) and isnum(sig) and sig > 0:
                out[sym] = (r, sig, (vol or {}).get("rvol"), inst.price)
        return out

    def _want_stock(self, sym: str, ticker: str, now: float) -> None:
        ent = self._stocks.get(sym)
        if ent is None:
            key = f"binance_fut:{sym}"
            owned = key not in self.instruments            # a configured node may already read it
            inst = self.instrument("binance_fut", sym, "24/7", True, ticker, "index")
            if owned:
                inst.calendar = "us_equity"     # a US stock: σ from NYSE trading days, like the screen
            ent = self._stocks[sym] = {"inst": inst, "ticker": ticker, "picked_at": now, "owned": owned,
                                       "ready": not owned}
            if owned:
                self._spawn(self._start_stock(sym), f"stock:{sym}")
        ent["picked_at"] = now

    def _spawn(self, coro, name: str) -> asyncio.Task | None:
        """Run a background task, keeping a reference to it (an unreferenced task can be collected);
        stop() cancels it."""
        try:
            task = asyncio.get_running_loop().create_task(coro, name=name)
        except RuntimeError:                                 # no event loop (offline use, tests)
            coro.close()
            return None
        self._tasks = [t for t in self._tasks if not t.done()] + [task]
        return task

    STOCK_RETRY_FIRST = 30.0                 # a picked stock's history that failed: again after 30 s, to 15 min

    async def _start_stock(self, sym: str) -> None:
        """History (cache first, then the missing tail) and the live index price of a picked stock."""
        ent = self._stocks.get(sym)
        if ent is None:
            return
        inst, now = ent["inst"], self.clock.now()
        history.load(self.cache_dir, inst, now)
        delay = self.STOCK_RETRY_FIRST
        while self._stocks.get(sym) is ent:         # dropped (or picked anew, by another task): stop
            if await self._load_stock(sym, ent):
                return
            # e.g. no network: retry while it is picked (or kept), not "loading" for good; at once
            # when a feed recovers (it waited up to 15 min after the network came back)
            ent["retry_in_s"] = delay
            await self._sleep_or_feeds_back(delay)
            delay = min(900.0, delay * 2)

    async def _load_stock(self, sym: str, ent: dict) -> bool:
        inst, now = ent["inst"], self.clock.now()
        try:
            last = history.last_good_ts(inst)
            for h in inst.holes.values():                 # a gap in the cache (or an old cache): from there
                if h is not None:
                    last = min(last or h, h)
            days = int(self.cfg.get("general", {}).get("history_days", 35))
            since5 = max(now - days * 86400, (last or 0) - 600)
            await self._binance_history(inst, since5, max(now - 86400 + 60, since5 - 600), now)
            inst.roll(self.clock.now())
            bf = self.connectors.get("binance_fut")
            if bf is not None and sym in self._stocks:
                await bf.subscribe(sym, inst, "index", volume=True)
            ent["ready"] = True
            ent.pop("retry_in_s", None)
            self._cache.clear()
            log.info("notable stock %s: history loaded, live feed on", ent["ticker"])
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("notable stock %s: %s (retrying)", ent["ticker"], self.keys.redact(e))
            return False

    def _prune_stocks(self, now: float) -> None:
        """Drop the live feed of a stock no longer picked anywhere for `keep_s` (default 2 h)."""
        picked = {p["symbol"] for r in self.stock_screen.values() for p in r["picks"]}
        keep = float(self.stock_cfg.get("keep_s", 7200))
        for sym, ent in list(self._stocks.items()):
            if sym in picked or now - ent["picked_at"] < keep:
                continue
            del self._stocks[sym]
            if ent["owned"]:
                self.instruments.pop(ent["inst"].key, None)
                bf = self.connectors.get("binance_fut")
                if bf is not None and self._running():
                    self._spawn(bf.unsubscribe(sym), f"unsub:{sym}")
                elif bf is not None:
                    bf.subs.pop(sym, None)

    def slot_series(self, tf: str) -> list[tuple[SeriesDef, dict, dict]]:
        """The window's picked stock(s) as series: [(SeriesDef with id stock1.., pick, entry)]. A stock just
        picked is shown once its history and live feed are in (a few seconds), never as "no data"."""
        out = []
        pos = self.stock_cfg.get("positions") or [[0.92, 0.29]]
        for p in (self.stock_screen.get(tf) or {}).get("picks", []):
            ent = self._stocks.get(p["symbol"])
            if ent is None or not ent["ready"]:
                continue
            i = len(out)
            x, y = pos[min(i, len(pos) - 1)]
            sdef = SeriesDef(f"stock{i + 1}", f"{p['ticker']} (notable stock)", p["ticker"], "equities", True, "price",
                             ent["inst"], {"x": x, "y": y, "source": "binance_fut", "symbol": p["symbol"],
                                           "field": "index"}, [], p["why"])
            out.append((sdef, p, ent))
        return out

    @staticmethod
    def _running() -> bool:
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    def _slot_reads(self, tf: str, w, now: float) -> list[tuple[dict, Instrument]]:
        """The window's picked stock(s) as map nodes (read like every other node)."""
        from . import stocks
        out = []
        screened = (self.stock_screen.get(tf) or {}).get("screened", 0)
        z_min, at = float(self.stock_cfg.get("z_min", 2.0)), self.stock_meta.get("at")
        for i, (sdef, p, ent) in enumerate(self.slot_series(tf)):
            r = self.read(sdef, w, now)
            why, eased, rank = p["why"], False, p.get("rank", i + 1)
            if r["z"] is not None and r["change_pct"] is not None:
                # the node's own numbers, so the map, the hover and the panel say the same thing (the
                # screen's numbers, from the same estimator on hourly bars, stay under slot.screen).
                # The screen runs every few minutes and a 1H move can fade in between: then say so.
                node = {"ticker": p["ticker"], "z": r["z"], "change_pct": r["change_pct"],
                        "rvol": (r.get("volume") or {}).get("rvol"), "rank": rank}
                eased = self._eased_since_screen(tf, p, r, z_min, at)
                why = (stocks.eased_text(node, p, tf, screened, None if at is None else now - at) if eased
                       else stocks.why_text(node, tf, screened))
            r["note"] = why
            r["slot"] = {"index": i + 1, "symbol": p["ticker"], "perpetual": p["symbol"], "why": why, "eased": eased,
                         "rank": rank, "screened": screened,
                         "universe": self.stock_meta.get("universe", 0), "ready": ent["ready"],
                         "screen": {**{k: None if p.get(k) is None else round(p[k], 3)
                                       for k in ("z", "change_pct", "sigma_pct", "rvol", "score")},
                                    "from": "map" if p.get("exact") else "hourly bars"}}
            out.append((r, ent["inst"]))
        return out

    EASED_MOVE = 0.75     # "eased": the map's move is under 3/4 of the move the screen picked (or turned)

    def _eased_since_screen(self, tf: str, p: dict, r: dict, z_min: float, at: float | None) -> bool:
        """Has the picked stock's move faded since the screen picked it? Only if the MOVE shrank (the
        node's log return under 3/4 of the screen's, or turned around) and its z no longer clears the
        rule. The screen's σ (hourly bars) and the node's (5-minute data) differ by up to ~25% at some
        clock times, which alone must not call an unchanged move "eased" (it did, in 6-7 of 10 cases
        at 10:45 New York). Once eased it stays so (no flicker at the threshold) until the next screen,
        or until the move is fully back: as big as the screen's, the same way, and |z| over the rule."""
        key = (tf, p["symbol"])
        rs, rn = math.log1p(p["change_pct"] / 100.0), r.get("log_return")
        if at is not None and self._eased.get(key) == at:
            if rn is not None and rn * rs > 0 and abs(rn) >= abs(rs) and abs(r["z"]) >= z_min:
                del self._eased[key]                 # it is back: the usual text, and the narrative again
                return False
            return True
        shrank = rn is None or rn * rs <= 0 or abs(rn) < self.EASED_MOVE * abs(rs)
        eased = shrank and abs(r["z"]) < z_min
        if eased and at is not None:
            self._eased[key] = at
        return eased

    def _stocks_info(self, tf: str, node_why: dict | None = None) -> dict:
        sc, res, meta = self.stock_cfg, self.stock_screen.get(tf) or {}, self.stock_meta
        node_why = node_why or {}
        slots = self.stock_slots()
        rule = {"z_min": float(sc.get("z_min", 2.0)), "score_min": float(sc.get("score_min", 2.5))}
        # the screen's own numbers sit under "screen" (as on node.slot): a pick's "why" quotes the map's node
        rnd = lambda p: {"symbol": p["ticker"], "screen": {  # noqa: E731
            **{k: None if p.get(k) is None else round(p[k], 3) for k in ("z", "change_pct", "sigma_pct", "rvol", "score")},
            "from": "map" if p.get("exact") else "hourly bars"}}
        if not slots:
            note = "The notable-stock slot is switched off ([stocks] slots = 0 or no universe)."
        elif meta.get("at") is None:
            note = "The large-cap screen has not run yet (it starts once the map's history is loaded)."
        elif res.get("rolled"):
            note = "A new session began at 17:00 New York: the large-cap screen is running again."
        elif not res.get("picks"):
            note = (f"No large-cap stock stands out: none of {res.get('screened', 0)} moved |z| ≥ {rule['z_min']:g} "
                    f"with a score ≥ {rule['score_min']:g}.")
        else:
            note = node_why.get(res["picks"][0]["ticker"], res["picks"][0]["why"])
        loading = [p["ticker"] for p in res.get("picks", [])
                   if not (self._stocks.get(p["symbol"]) or {}).get("ready")]
        return {"slots": slots, "universe": meta.get("universe", 0), "screened": res.get("screened", 0),
                "scanned_at": clock.iso(meta["at"]) if meta.get("at") else None, "rule": rule,
                "picks": [dict(rnd(p), slot=i + 1, why=node_why.get(p["ticker"], p["why"]))
                          for i, p in enumerate(res.get("picks", []))],
                "candidates": [rnd(p) for p in res.get("top", [])], "not_listed": meta.get("missing", []),
                "positions": (sc.get("positions") or [[0.92, 0.29]])[:max(1, slots)], "note": note,
                "loading": loading}

    # ── callbacks from connectors ───────────────────────────────────────────────────────────
    def on_news(self, items: list[dict]) -> None:
        self.news.add(items, self.clock.now())

    def watch_symbols(self) -> set[str]:
        """Tickers on the map (for filtering company-news feeds down to what matters here)."""
        out = {"SPY", "QQQ", "TLT", "IEF", "HYG", "GLD", "SLV", "USO", "UUP", "VIXY", "BTCUSD", "ETHUSD",
               "IBIT", "ETHA", "NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "TSLA", "COIN", "MSTR"}
        for i in self.instruments.values():
            out.add(i.symbol.replace("USDT", "").upper())
        out |= {e["ticker"] for e in self._stocks.values()}       # the notable stock(s) of the moment
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
    async def _binance_history(self, inst: Instrument, since5: float, since1: float, now: float) -> None:
        """5-minute (with volume) and 1-minute history of one Binance instrument, from `since`."""
        from .connectors import binance as bn
        mk = "spot" if inst.source == "binance_spot" else "fut"
        pr = "index" if (mk == "fut" and inst.extra.get("field", "last") == "index") else "last"
        k5 = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "5m", int(since5 * 1000), int(now * 1000), pr)
        vol = k5
        if pr == "index":
            vol = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "5m", int(since5 * 1000), int(now * 1000), "last")
        k1 = await bn.fetch_klines(self.ctx.session, mk, inst.symbol, "1m", int(since1 * 1000), int(now * 1000), pr)
        inst.seed("g5", [(t, c) for t, c, _, _ in k5], now, [(t, q, tb) for t, _, q, tb in vol])
        inst.seed("g1", [(t, c) for t, c, _, _ in k1], now)

    async def seed_history(self, incremental: bool = False, since: float | None = None,
                           state: dict | None = None, only: set[str] | None = None) -> None:
        """Fill the grids: disk cache first, then REST for whatever is missing: from the newest good
        step, or from the first step a live feed left empty (a sleep, a network outage: `holes`),
        or from `since`. `only`: just these instruments (a back-fill). Instruments whose download
        failed are listed in state["failed"]."""
        from .connectors import alpaca as alp, hyperliquid as hl, pollers
        now = self.clock.now()
        days = int(self.cfg.get("general", {}).get("history_days", 35))
        horizon = now - days * 86400

        def start_of(inst):
            # the newest value ANYWHERE in the grid: after a sleep of over 4 hours the last 50 steps are
            # all empty, and "nothing found" downloaded 35 days of every instrument with a hole
            t = history.last_good_ts(inst, max_back=inst.g5.size) or horizon
            if since is not None:
                t = min(t, since)
            for h in inst.holes.values():
                if h is not None:
                    t = min(t, h)
            return max(horizon, t - 600)
        startup = state is None
        st = self.seed_state if startup else state
        st.update(state="running", done=0, errors=[], failed=[], ok=[], started=clock.iso(now))
        insts = [i for i in self.instruments.values() if i.source in HISTORY_SOURCES
                 and (only is None or i.key in only)]
        st["total"] = len(insts) + 2
        if not incremental:
            if startup:
                history.clean_tmp(self.cache_dir)     # a save killed midway (or refused) left it
            for i in insts:
                history.load(self.cache_dir, i, now)
        # where each download starts, taken now: once it is in, the holes are gone and the newest
        # value is recent, so the computed series' rebuild has to start from these (not from then)
        starts = {i.key: start_of(i) for i in insts}
        sem = asyncio.Semaphore(3)

        async def one(inst: Instrument):
            async with sem:
                try:
                    since5 = start_of(inst)
                    since1 = max(now - 86400 + 60, since5 - 600 if incremental else now - 86400 + 60)
                    if inst.source in ("binance_spot", "binance_fut"):
                        if inst.source == "binance_fut" and inst.symbol in self._funding_only():
                            return
                        await self._binance_history(inst, since5, since1, now)
                    elif inst.source == "hyperliquid":
                        k5 = await hl.fetch_candles(self.connectors["hyperliquid"], inst.symbol, "5m", since5, now)
                        k1 = await hl.fetch_candles(self.connectors["hyperliquid"], inst.symbol, "1m", since1, now)
                        inst.seed("g5", [(t, c) for t, c, _, _ in k5], now, [(t, q, tb) for t, _, q, tb in k5])
                        inst.seed("g1", [(t, c) for t, c, _, _ in k1], now)
                    st["ok"].append(inst.key)
                except Exception as e:  # noqa: BLE001
                    st["errors"].append(self.keys.redact(f"{inst.key}: {e}")[:200])
                    st["failed"].append(inst.key)
                    log.warning("history %s: %s", inst.key, self.keys.redact(e))
                finally:
                    st["done"] += 1

        alp_insts = [i for i in insts if i.source == "alpaca"]
        tasks = [one(i) for i in insts if i.source != "alpaca"]

        async def alpaca_all():
            if not alp_insts:
                return
            conn = self.connectors.get("alpaca")
            if conn is None or not alp_insts or not conn.available()[0]:
                # No Alpaca (no key, or switched off): take the same ETFs' history from Yahoo's chart
                # API instead, so the bonds node can anchor on TLT's real last close and stay live
                # through its 24/7 proxy. These prices are history only and never shown as live.
                if alp_insts and cfgmod.connector_enabled(self.cfg, "yahoo"):
                    for i in alp_insts:
                        # Always ask (one small request per ETF): the cache's newest step can be a
                        # carried price, which says nothing about the sessions since. Five days of
                        # bars cover a recent cache; otherwise take the full 60.
                        last = history.last_good_ts(i)
                        for h in i.holes.values():
                            if h is not None:
                                last = min(last or h, h)
                        rng = "5d" if last and now - last < 4 * 86400 else "60d"
                        try:
                            rows = await pollers.yahoo_chart(self.ctx.session, i.symbol, "5m", rng)
                            if rows:
                                i.seed("g5", rows, self._through_closed(i, rows[-1][0] + 300, now))
                                i.extra["history_source"] = "Yahoo chart (no Alpaca key)"
                            # 1-minute bars too (two sessions cover the 1-day grid): without them the
                            # 1-minute bonds series fills US hours from the proxy, 0.03% off the real TLT
                            rows1 = await pollers.yahoo_chart(self.ctx.session, i.symbol, "1m", "2d")
                            if rows1:
                                i.seed("g1", rows1, self._through_closed(i, rows1[-1][0] + 60, now))
                            st["ok"].append(i.key)
                        except Exception as e:  # noqa: BLE001
                            st["errors"].append(f"yahoo {i.symbol}: {e}"[:200])
                            st["failed"].append(i.key)
                st["done"] += len(alp_insts)
                return
            try:
                since5, since1 = min(start_of(i) for i in alp_insts), now - 86400 + 60
                b5 = await alp.fetch_bars(conn, [i.symbol for i in alp_insts], "5Min", since5, None, conn.cfg.get("feed", "iex"))
                b1 = await alp.fetch_bars(conn, [i.symbol for i in alp_insts], "1Min", since1, None, conn.cfg.get("feed", "iex"))
                for i in alp_insts:
                    # IEX has bars only for minutes that traded (`since`: none from there to the first
                    # bar is no gap), so an answer without any for a symbol is seeded too: no trade
                    # since (a weekend, a holiday, before its first print), the last close carried.
                    # Skipped, the time off stayed empty and TODAY unknown all day.
                    rows = b5.get(i.symbol, [])
                    i.seed("g5", [(t, c) for t, c, _ in rows], now, [(t, q, math.nan) for t, _, q in rows], since=since5)
                    rows1 = b1.get(i.symbol, [])
                    i.seed("g1", [(t, c) for t, c, _ in rows1], now, since=since1)
                    st["ok"].append(i.key)
            except Exception as e:  # noqa: BLE001
                st["errors"].append(self.keys.redact(f"alpaca: {e}")[:200])
                st["failed"].extend(i.key for i in alp_insts)
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
                legs = [i.key for i in s.inst.inputs]
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
                    # the FX legs have no history of their own: ICE's DXY over their gap fills the
                    # dollar, so their holes are done once it covers them
                    span = {"g5": (rows[0][0], rows[-1][0] + 300) if rows else None,
                            "g1": (rows1[0][0], rows1[-1][0] + 60) if rows1 else None}
                    for leg in d.inputs:
                        for g, hole in list(leg.holes.items()):
                            if hole is not None and span[g] and span[g][0] <= hole < span[g][1]:
                                leg.holes[g] = None
                    st["ok"].extend(legs)
                except Exception as e:  # noqa: BLE001
                    st["errors"].append(f"yahoo {h.get('symbol')}: {e}"[:200])
                    st["failed"].extend(legs)
            st["done"] += 1

        # the dollar's own history (ICE via Yahoo): at the start, and in a back-fill whenever one of
        # its FX legs has a hole (without it a sleep over 17:00 left the dollar without TODAY till
        # the next roll, ~20 h)
        dxy = only is None or any(self._dxy_of(self.instruments[k]) is not None for k in only if k in self.instruments)
        await asyncio.gather(*tasks, alpaca_all(), *([yahoo_dxy()] if dxy else []))
        oi = self.connectors.get("binance_oi")
        if oi is not None and not incremental:
            try:
                await oi.seed()
                self.on_open_interest({s: list(oi.hist[s]) for s in oi.symbols})
            except Exception as e:  # noqa: BLE001
                st["errors"].append(f"open interest: {e}"[:200])
        st["done"] += 1
        now = self.clock.now()
        ok = set(st["ok"])
        if not startup and not ok:
            st.update(state="done", finished=clock.iso(now))   # nothing came: nothing to redo or save
            return
        for inst in self.instruments.values():
            inst.roll(now)
        for d in self.derived:
            if isinstance(d, DXY):
                continue   # its history comes straight from ICE's DXY (Finnhub has no free FX history)
            # a back-fill: only what its own inputs brought, from where their downloads began
            got = [starts[i.key] for i in d.inputs if i.key in ok and i.key in starts]
            if startup or got:
                d.rebuild(now, since=None if startup else min(got))
        self._cache.clear()
        st.update(state="done", finished=clock.iso(now))
        await self._save_cache_off_loop()
        if startup:
            log.info("history ready (%d instruments, %d errors)", len(insts), len(st["errors"]))

    def _funding_only(self) -> set:
        used = set()
        for s in self.series.values():
            for i in [s.inst] + [r[1] for r in s.refs] + list(getattr(s.inst, "inputs", [])):
                if i.source == "binance_fut":
                    used.add(i.symbol)
        used |= set(self._stocks)                 # picked stocks: refilled after a sleep like any node
        return {i.symbol for i in self.instruments.values() if i.source == "binance_fut"} - used

    def save_cache(self, insts: list | None = None) -> None:
        """Write the history cache. In a worker thread (see _save_cache_off_loop) `insts` is the list
        taken on the event loop: the loop adds and drops picked stocks meanwhile, and iterating the
        live dict from the thread raised "dictionary changed size during iteration". One save at a
        time: two at once (housekeeping and a back-fill) wrote the same temporary files."""
        if insts is None:
            # (the final save) what the next start loads first: a window close gives the save about
            # 4.5 s, and on a busy PC it takes longer; the FX legs' and Coinbase's files are read only
            # by tools/replay_flows.py
            insts = sorted(self.instruments.values(), key=lambda i: i.source not in HISTORY_SOURCES)
        n = 0
        with self._save_lock:
            for i in insts:
                try:
                    n += history.save(self.cache_dir, i)
                except OSError as e:
                    log.warning("cache %s: %s", i.key, e)
        log.debug("cached %d instruments", n)

    async def _save_cache_off_loop(self) -> None:
        """save_cache in a worker thread (writing every file on the loop stalled ticks, streams and
        pushes for seconds), on the instruments as they are now."""
        insts = list(self.instruments.values())
        await asyncio.get_running_loop().run_in_executor(None, self.save_cache, insts)

    # ── the clock tick ──────────────────────────────────────────────────────────────────────
    def tick(self, now: float | None = None) -> None:
        now = now or self.clock.now()
        roll = clock.day_roll(now)
        if roll != getattr(self, "_roll_seen", roll):
            # 17:00 New York: TODAY starts a new window. Nothing computed for the old one may be served
            # after it (σ was, for up to a minute: z-scores ~6x too small, seen live on 24 Nov).
            self._cache.clear()
            # ...nor the old session's TODAY pick (its move is gone: it read as "eased"): screen again
            today = self.stock_screen.get("today")
            if today and today.get("picks"):
                self.stock_screen["today"] = dict(today, picks=[], top=[], rolled=True)
            self._eased = {k: v for k, v in self._eased.items() if k[0] != "today"}
            self._rescreen_soon()
        self._roll_seen = roll
        for inst in self.instruments.values():
            inst.feed_alive(self._feed_seen(inst.source))   # a quiet market on a live feed is covered
            inst.roll(now)
        self._ensure_share_anchor(now)
        for d in self.derived:
            d.refresh(now)

    FEEDS = {"alpaca": ("alpaca", "alpaca_rest")}    # the connectors that deliver a source's prices

    def _feed_seen(self, source: str) -> float | None:
        """The newest message or successful poll of the connector(s) feeding `source`."""
        best = None
        for name in self.FEEDS.get(source, (source,)):
            h = getattr(self.connectors.get(name), "health", None)
            for t in (getattr(h, "last_msg", None), getattr(h, "last_ok", None)):
                if t is not None and (best is None or t > best):
                    best = t
        return best

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
            # the days σ was measured on (a fallback for a recent listing uses whatever history exists)
            "sigma_days": clock.CALENDAR_LABELS.get(getattr(inst, "calendar", "all"))
            if str(method).startswith(("same-clock", "daily RMS")) else None,
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
    def _linked_pairs(self) -> set:
        """Series pairs where one is computed from the other's price: the stablecoin share is moved
        by the BTC and ETH it is set against, so it correlates with them at about -1 by arithmetic.
        Flow inference treats such a correlation as unknown instead of as proof of a rotation."""
        if getattr(self, "_linked", None) is None:
            by_inst = {id(s.inst): sid for sid, s in self.series.items()}
            pairs = set()
            for a, s in self.series.items():
                for i in getattr(s.inst, "inputs", None) or []:
                    b = by_inst.get(id(i))
                    if b is not None and b != a:
                        pairs |= {(a, b), (b, a)}
            self._linked = pairs
        return self._linked

    def _returns(self, tf: str, w, now):
        return self._cached(("rets", tf), 10 if tf == "1h" else 30,
                            lambda: {sid: stats.step_returns(s.inst, w, now) for sid, s in self.series.items()}, now)

    def build_tf(self, tf: str, now: float) -> dict:
        w = stats.window_for(tf, now)
        reads = {sid: self.read(s, w, now) for sid, s in self.series.items()}
        slots = self._slot_reads(tf, w, now)
        for r, _ in slots:
            reads[r["id"]] = r
        z_all = {sid: r["z"] for sid, r in reads.items() if r["z"] is not None and r["status"] in FLOW_STATUSES}
        z_nodes = {sid: v for sid, v in z_all.items() if reads[sid]["node"]}
        rets = self._returns(tf, w, now)
        if slots:
            rets = dict(rets)
            for r, inst in slots:
                rets[r["id"]] = self._cached(("rets", tf, r["id"], inst.key), 10 if tf == "1h" else 30,
                                             lambda inst=inst: stats.step_returns(inst, w, now), now)
        memo = {}
        linked = self._linked_pairs()

        def corr_fn(a, b):
            if (a, b) in linked:
                return None          # one is computed from the other: arithmetic, not a rotation
            k = (a, b)
            if k not in memo:
                memo[k] = self._cached(("corr", tf, a, b), 30, lambda: stats.corr(rets.get(a, {}), rets.get(b, {})), now)
            return memo[k]

        fc = self.flow_cfg
        why: dict = {}
        edges = flow.infer_edges(z_nodes, corr_fn, theta=fc.get("theta", 0.5), max_edges=fc.get("max_edges", 16),
                                 per_node=fc.get("per_node", 5), min_weight=fc.get("min_weight", 0.3), why=why)
        risk = flow.risk_composite(z_all, self.risk_components, self.risk_scale, self.risk_clip)
        edges_d = [e.as_dict() for e in edges]
        ctx = self._narrative_ctx(tf, w, reads, now)
        bullets = narrative.build(tf, reads, edges_d, risk, ctx)
        nodes = [r for r in reads.values() if r["node"]]
        aux = [r for r in reads.values() if not r["node"]]
        names = {sid: r["label"] for sid, r in reads.items()}
        for n in nodes:
            n["inflow_share"] = round(sum(e.share for e in edges if e.dst == n["id"]), 3)
            n["outflow_share"] = round(sum(e.share for e in edges if e.src == n["id"]), 3)
            n["no_edge"] = self._no_edge_info(n, why.get(n["id"]), names, any(n["id"] in (e.src, e.dst) for e in edges))
        return {"timeframe": tf, "label": stats.TF_LABELS[tf],
                "window": {"start": clock.iso(w.start), "end": clock.iso(w.end), "label": w.label},
                "nodes": nodes, "aux": aux, "edges": edges_d, "risk": risk, "narrative": bullets,
                "tiles": self._tiles(tf, w, reads, now),
                "stocks": self._stocks_info(tf, {r["slot"]["symbol"]: r["slot"]["why"] for r, _ in slots})}

    @staticmethod
    def _no_edge_info(n: dict, r: dict | None, names: dict, has_edge: bool) -> dict | None:
        """Why a node has no arrow in this window (None if it has one)."""
        if has_edge:
            return None
        if r is not None:
            return {"code": r["code"], "text": flow.no_edge_text(r, names)}
        st = n["status"]
        if n["z"] is None:
            if st == "closed":
                return {"code": "closed", "text": "No arrow: its market was closed for the whole window, so there "
                                                  "is no move to read."}
            if st == "no_data":
                return {"code": "no_data", "text": "No arrow: no data yet."}
            return {"code": "warming_up", "text": "No arrow yet: not enough history to judge a move in this window."}
        return {"code": "not_live", "text": f"No arrow while its data is {st.replace('_', ' ')}: only live prices "
                                            f"(or a closed market's last price) take part in the flows."}

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
                     "idle_shutdown_s": float(self.cfg.get("server", {}).get("idle_shutdown_minutes", 5)) * 60,
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
            self._tasks.append(asyncio.create_task(self._supervise(name, c), name=f"conn:{name}"))
        self._tasks.append(asyncio.create_task(self._seed_then_track(), name="seed"))
        self._tasks.append(asyncio.create_task(self._loop(), name="tick"))
        self._tasks.append(asyncio.create_task(self._housekeeping(), name="housekeeping"))

    RESTART_FIRST, RESTART_MAX = 5.0, 300.0

    async def _supervise(self, name: str, c) -> None:
        """Run one connector and restart it if it crashes. A bug in one feed must not end that feed
        silently for the rest of the session: on 25 Nov Finnhub's reconnect raised a TypeError, its
        task died and the dollar stayed stale until a restart. The crash is logged with its traceback
        (keys redacted), shown on the source, and the connector restarts after 5 s, doubling to 5 min."""
        delay = self.RESTART_FIRST
        while True:
            started = time.monotonic()
            try:
                await c.run()
                return                               # e.g. a disabled connector returns at once
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - anything else is a bug in that connector
                if time.monotonic() - started > 600:
                    delay = self.RESTART_FIRST         # it had been running fine: start the back-off again
                c.error(e, "crashed")
                log.error("connector %s crashed: restarting it in %.0f s", name, delay, exc_info=True)
                c.health.state = "restarting"
                c.health.detail = f"crashed ({type(e).__name__}): restarting in {delay:.0f}s"
                await asyncio.sleep(delay)
                delay = min(self.RESTART_MAX, delay * 2)

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
                       if i.source in HISTORY_SOURCES
                       and i.g5.last_k is None and i.symbol not in self._funding_only()
                       and i.symbol not in self._stocks         # picked stocks load their own history
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

    HOUSEKEEP_EVERY = 5.0
    # the cache saved every 10 minutes (was 30): a final save cut short (a window close on a busy PC:
    # Windows allows it about 4.5 s) leaves the files it did not reach at most this old
    SAVE_EVERY = 600.0

    async def _housekeeping(self):
        """Sleep/resume detection (wall clock jumps), NTP re-sync, periodic cache saves. An error in
        one round is logged and the next round runs: nothing supervises this task, and one failed
        save once ended wake detection, back-fills, hole checks, NTP and saves for the whole run."""
        last_sync = time.time()
        last_save = time.time()
        last_error = 0.0
        while True:
            before = time.time()
            await asyncio.sleep(self.HOUSEKEEP_EVERY)
            try:
                if await self._housekeep_once(time.time() - before):
                    last_sync = time.time()
                if time.time() - last_sync > 1800:
                    last_sync = time.time()
                    await self.clock.sync()
                if time.time() - last_save > self.SAVE_EVERY:
                    last_save = time.time()
                    await self._save_cache_off_loop()
            except Exception:  # noqa: BLE001 - whatever it was, the next round still runs
                if time.time() - last_error > 600:          # (a fault every 5 s logs once per 10 min)
                    last_error = time.time()
                    log.exception("housekeeping round failed (continuing)")

    async def _housekeep_once(self, gap: float) -> bool:
        """One round of housekeeping after `gap` seconds of wall time (5 when awake): a jump means the
        PC slept (resync, reconnect, back-fill); otherwise look for holes. True after a resume."""
        if gap > 25:
            self.resumes += 1
            log.warning("wall clock jumped %.0fs (sleep/hibernate?): resyncing and reconnecting", gap)
            await self.on_resume(gap)
            return True
        if self.seed_state.get("state") == "done":
            self.check_holes()
        return False

    async def on_resume(self, gap: float) -> None:
        conn = self.ctx.session.connector if self.ctx.session else None
        if conn is not None and hasattr(conn, "clear_dns_cache"):
            conn.clear_dns_cache()
        await self.clock.sync()
        for c in self.connectors.values():
            c.kick()
        self.want_backfill("wake")          # the steps that passed asleep are holes since the next tick

    # ── back-fill: what a sleep or a network outage left empty ──────────────────────────────
    BACKFILL_SETTLE = 10.0                  # let the network come back first
    BACKFILL_FIRST, BACKFILL_MAX = 30.0, 900.0

    @staticmethod
    def _through_closed(inst: Instrument, end: float, now: float) -> float:
        """How far a download ending at `end` holds: on to `now` through hours its market is shut
        (nothing trades: its last close is the price; a Saturday start left Friday 16:00 onwards
        empty, so TODAY had no start price), never over open ones (a delayed feed's missing minutes
        are unknown). Sessions open and close on 5-minute boundaries: a minute at a time until `end`
        is on one, then five."""
        step = 300 if end % 300 == 0 else 60
        while end + step <= now and not clock.market_open(inst.hours, end + step - 1):
            end += step
            if end % 300 == 0:
                step = 300
        return end

    def _dxy_of(self, inst: Instrument):
        """The dollar index that `inst` is an FX leg of, if that index has its own history (ICE's)."""
        if inst.source != "finnhub" or not cfgmod.connector_enabled(self.cfg, "yahoo"):
            return None
        for s in self.series.values():
            if s.kind == "dxy" and (s.cfg.get("history") or {}).get("source") == "yahoo" and inst in s.inst.inputs:
                return s
        return None

    def _fillable(self, inst: Instrument) -> bool:
        """Can a REST history fill this instrument's holes?"""
        if self._dxy_of(inst) is not None:
            return True                         # an FX leg: ICE's DXY fills the dollar over its gap
        if inst.source in ("binance_spot", "binance_fut"):
            return not (inst.source == "binance_fut" and inst.symbol in self._funding_only())
        if inst.source == "hyperliquid":
            return "hyperliquid" in self.connectors
        if inst.source == "alpaca":
            c = self.connectors.get("alpaca")
            return bool(c is not None and c.available()[0]) or cfgmod.connector_enabled(self.cfg, "yahoo")
        return False

    def holes(self) -> dict[str, float]:
        """{instrument: its first step a live feed left empty}, for those the exchange can fill."""
        out = {}
        for i in self.instruments.values():
            hs = [h for h in i.holes.values() if h is not None]
            if hs and self._fillable(i):
                out[i.key] = min(hs)
        return out

    def check_holes(self) -> None:
        """A network outage while awake leaves holes too (every 5 s): start the back-fill for them, and
        when a feed that left a hole delivers again, cut the back-off short, once per recovery."""
        back = [i for i in self.instruments.values() if i.recovered]
        for i in back:
            i.recovered = False
        if back:
            self._feeds_back()
        if self.holes() and (back or self._backfill_task is None or self._backfill_task.done()):
            self.want_backfill("the feeds are back" if back else "network outage")

    def _feeds_back(self) -> None:
        for ev in list(self._feed_waiters):
            ev.set()

    async def _sleep_or_feeds_back(self, delay: float) -> bool:
        """Wait `delay` seconds, or less if a feed recovers meanwhile (True then)."""
        ev = asyncio.Event()
        self._feed_waiters.add(ev)
        try:
            await asyncio.wait_for(ev.wait(), timeout=delay)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            self._feed_waiters.discard(ev)

    def want_backfill(self, reason: str) -> None:
        """Start the back-fill (or wake it from its back-off: the feeds are back)."""
        if self._backfill_task is not None and not self._backfill_task.done():
            if self._backfill_wake is not None:
                self._backfill_wake.set()
            return
        self._backfill_task = self._spawn(self._backfill(reason), "backfill")

    async def _backfill(self, reason: str) -> None:
        """Download what a sleep or a network outage left empty, until nothing is missing. After a
        failure (on 25 Nov the PC woke with DNS down, every request failed and the old one-shot refill
        never tried again: the gap stayed flat in the history for good) it retries after 30 s,
        doubling to 15 min, sooner when the feeds come back, and gives up only on a gap older than
        the history or one the exchange itself has no data for. A task: it never blocks the event
        loop or the auto-stop, and stop() cancels it."""
        self._backfill_wake = asyncio.Event()
        delay = self.BACKFILL_FIRST
        try:
            await asyncio.sleep(self.BACKFILL_SETTLE)
            attempt = 0
            while True:
                now = self.clock.now()
                horizon = now - int(self.cfg.get("general", {}).get("history_days", 35)) * 86400
                for i in self.instruments.values():         # a gap older than the history is moot
                    for g, h in list(i.holes.items()):
                        if h is not None and h < horizon:
                            i.holes[g] = None
                todo = self.holes()
                if not todo:
                    if attempt:
                        self.backfill_state = {"state": "done", "reason": reason, "attempts": attempt,
                                               "finished": clock.iso(now)}
                    return
                attempt += 1
                since = min(todo.values())
                st = {"reason": reason, "attempt": attempt, "since": clock.iso(since), "missing": sorted(todo)}
                self.backfill_state = st
                self._backfill_wake.clear()         # a "feeds are back" from now on counts (also mid-download)
                try:
                    # only what has holes, each from its own first hole: one stock's 35-day-old gap
                    # once re-downloaded 35 days of every instrument (Binance's half budget, twice)
                    await self.seed_history(incremental=True, state=st, only=set(todo))
                except Exception as e:  # noqa: BLE001 - whatever it was, try again later
                    st.setdefault("errors", []).append(self.keys.redact(e)[:200])
                    st["failed"] = sorted(todo)
                left = self.holes()
                if not left:
                    self.backfill_state = {"state": "done", "reason": reason, "attempts": attempt,
                                           "since": clock.iso(since), "finished": clock.iso(self.clock.now())}
                    log.info("history gap since %s back-filled (%s, attempt %d)", clock.iso(since), reason, attempt)
                    return
                failed = set(st.get("failed") or [])
                for key in [k for k in left if k in todo and k not in failed and left[k] <= todo[k]]:
                    # asked, answered, and the same gap still empty: the exchange has no data there.
                    # (A hole that appeared during the download was not asked for: next attempt.)
                    log.info("history %s: no exchange data for the gap since %s; left empty", key, clock.iso(left[key]))
                    inst = self.instruments[key]
                    inst.holes = {g: None for g in inst.holes}
                if not self.holes():
                    self.backfill_state = {"state": "done", "reason": reason, "attempts": attempt,
                                           "finished": clock.iso(self.clock.now())}
                    return
                st.update(state="retrying", retry_in_s=delay)
                log.warning("history gap since %s not back-filled yet (%d instruments, e.g. %s): retrying in %.0f s",
                            clock.iso(since), len(left), (st.get("errors") or ["?"])[0][:120], delay)
                try:
                    await asyncio.wait_for(self._backfill_wake.wait(), timeout=delay)
                    await asyncio.sleep(self.BACKFILL_SETTLE)   # the feeds just came back: let them settle
                except asyncio.TimeoutError:
                    pass
                delay = min(self.BACKFILL_MAX, delay * 2)
        finally:
            self._backfill_wake = None

    async def stop_feeds(self) -> None:
        """Cancel every task: the streams close and downloads stop (the first half of stop())."""
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def stop(self, save: bool = True) -> None:
        """Stop the feeds, then save the history cache (unless `save` is False: done already). The save
        takes the time (1 s, several on a busy PC): the CLI frees the port before it."""
        await self.stop_feeds()
        if save:
            self.save_cache_logged()
        if self.ctx.session is not None:
            await self.ctx.session.close()

    def save_cache_logged(self, done=None) -> None:
        """The final save, then `done` set (a threading.Event: a closing console window's handler
        waits for it) and only then its time logged (a log line can wait on the console)."""
        t = time.monotonic()
        try:
            self.save_cache()
        except Exception:  # noqa: BLE001
            if done is not None:
                done.set()
            log.exception("final cache save failed")
            return
        if done is not None:
            done.set()
        log.info("history cache saved in %.1f s", time.monotonic() - t)
