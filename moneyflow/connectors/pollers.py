"""Low-frequency REST sources. Each polls rarely (minutes to hours) and caches what it can.

Source                       Interval   Gives                                  Freshness (measured 2026-11-24)
Cboe delayed quotes (VIX)    60 s       VIX level and change                   15 minutes delayed by licence
CoinGecko /global            5 min      total crypto market cap, coin shares   CoinGecko refreshes every ~5-10 min
DefiLlama stablecoins        60 min     USD-stablecoin supply now, 1d/7d/30d   hourly-ish; history is daily
FRED                         6 h        DGS10, DGS2, HY spread                 daily, posted with ~1 day lag
federalreserve.gov           24 h       FOMC meeting calendar                  official schedule
Forex Factory weekly feed    2 h        this week's scheduled releases         exact times; no actuals
Farside Investors            2 h        US spot BTC / ETH ETF daily flows      published after each US session
RSS feeds                    3 min      headlines                              publisher-dependent (minutes)
Yahoo chart API              start-up   5-minute DXY history (unofficial)      history only
"""
from __future__ import annotations

import asyncio
import datetime as dt
import html as htmlmod
import json
import math
import re
import time

import aiohttp

from .. import clock, fed as fedmod, jsonutil
from ..news import parse_rss
from .base import PollConnector

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/130.0 Safari/537.36")
POLITE_UA = "moneyflow/1.0 (personal market dashboard; low-rate polling)"


class CboeVix(PollConnector):
    name = "cboe"
    title = "Cboe VIX (15-min delayed)"
    interval = 60.0
    URL = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/_VIX.json"
    HIST = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    HDR = {"User-Agent": BROWSER_UA, "Referer": "https://www.cboe.com/", "Accept": "application/json,text/csv,*/*"}

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.hist_at = 0.0
        self.sigma_daily = None

    def next_interval(self):
        return self.interval if clock.vix_calc_hours(self.now()) else 1800.0

    async def poll(self):
        if time.time() - self.hist_at > 86400:
            try:
                await self._history()
            except Exception as e:  # noqa: BLE001
                self.error(e, "history")
        j = await self.get_json(self.URL, headers=self.HDR)
        d = j.get("data") or {}
        price = d.get("current_price")
        if price is None:
            raise RuntimeError("no current_price")
        lt = d.get("last_trade_time")
        ts = None
        if lt:
            try:
                ts = dt.datetime.fromisoformat(lt).replace(tzinfo=clock.NY).timestamp()
            except ValueError:
                ts = None
        prev = d.get("prev_day_close")
        z = None
        if prev and self.sigma_daily:
            z = math.log(float(price) / float(prev)) / self.sigma_daily
        self.health.mark_msg(self.now())
        self.engine.on_vix({"level": float(price), "change": d.get("price_change"),
                            "change_pct": d.get("price_change_percent"), "prev_close": prev,
                            "quote_time": clock.iso(ts) if ts else None, "quote_ts": ts, "z_today": z,
                            "sigma_daily": self.sigma_daily,
                            "source": "Cboe delayed quotes", "delay_note": "15 minutes delayed"})

    async def _history(self):
        async with self.session.get(self.HIST, headers=self.HDR, timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            text = await r.text()
        closes = []
        for line in text.strip().splitlines()[1:][-90:]:
            parts = line.split(",")
            try:
                closes.append(float(parts[4]))
            except (IndexError, ValueError):
                continue
        rs = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0][-60:]
        if len(rs) >= 20:
            self.sigma_daily = math.sqrt(sum(x * x for x in rs) / len(rs))
        self.hist_at = time.time()


class CoinGeckoGlobal(PollConnector):
    name = "coingecko"
    title = "CoinGecko global market cap"
    interval = 300.0
    cache_ttl = 600.0
    URL = "https://api.coingecko.com/api/v3/global"

    async def fetch(self):
        j = await self.get_json(self.URL, headers={"User-Agent": POLITE_UA, "Accept": "application/json"})
        d = j.get("data") or {}
        total = (d.get("total_market_cap") or {}).get("usd")
        if not total:
            raise RuntimeError("no total market cap")
        return {"total": float(total), "pct": d.get("market_cap_percentage") or {},
                "updated_at": float(d.get("updated_at") or self.now()),
                "chg24": d.get("market_cap_change_percentage_24h_usd")}

    def apply(self, data):
        self.health.mark_msg(self.now())
        self.engine.on_crypto_global(data["total"], data["pct"], data["updated_at"], data.get("chg24"))


class DefiLlamaStables(PollConnector):
    name = "defillama"
    title = "DefiLlama stablecoin supply"
    interval = 3600.0
    cache_ttl = 3600.0
    NOW_URL = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
    HIST_URL = "https://stablecoins.llama.fi/stablecoincharts/all"

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.interval = float(cfg.get("interval_s", 3600))
        self.hist_at = 0.0
        self.hist: list[tuple[float, float]] = []

    def apply(self, data):
        self.hist = [tuple(h) for h in data["hist"]]
        self.hist_at = data.get("hist_at", self.hist_at)
        self.health.mark_msg(self.now())
        self.engine.on_stable_supply(data["now"], self.hist)

    async def fetch(self):
        if time.time() - self.hist_at > 12 * 3600 or not self.hist:
            j = await self.get_json(self.HIST_URL, headers={"User-Agent": POLITE_UA}, timeout=60)
            cut = time.time() - 60 * 86400
            self.hist = [(float(x["date"]), float((x.get("totalCirculatingUSD") or {}).get("peggedUSD") or 0))
                         for x in j if float(x["date"]) >= cut]
            self.hist = [h for h in self.hist if h[1] > 0]
            self.hist_at = time.time()
        j = await self.get_json(self.NOW_URL, headers={"User-Agent": POLITE_UA}, timeout=60)
        tot = d1 = w1 = m1 = 0.0
        top = []
        for a in j.get("peggedAssets", []):
            c = (a.get("circulating") or {}).get("peggedUSD") or 0
            tot += c
            d1 += (a.get("circulatingPrevDay") or {}).get("peggedUSD") or 0
            w1 += (a.get("circulatingPrevWeek") or {}).get("peggedUSD") or 0
            m1 += (a.get("circulatingPrevMonth") or {}).get("peggedUSD") or 0
            if c > 1e9:
                top.append((a.get("symbol"), c, c - ((a.get("circulatingPrevDay") or {}).get("peggedUSD") or 0)))
        top.sort(key=lambda x: -x[1])
        now = {"total": tot, "d1": tot - d1, "w1": tot - w1, "m1": tot - m1,
               "top": [{"symbol": s, "supply": c, "d1": dd} for s, c, dd in top[:6]], "observed_at": self.now()}
        return {"now": now, "hist": self.hist, "hist_at": self.hist_at}


class Fred(PollConnector):
    name = "fred"
    title = "FRED daily rates"
    interval = 21600.0
    cache_ttl = 21600.0
    URL = "https://api.stlouisfed.org/fred/series/observations"

    def available(self):
        return (self.keys.has("FRED_API_KEY"), "no FRED_API_KEY")

    def apply(self, data):
        self.health.mark_msg(self.now(), len(data))
        self.engine.on_fred(data)

    async def fetch(self):
        out = {}
        for sid in self.cfg.get("series", ["DGS10", "DGS2", "BAMLH0A0HYM2"]):
            j = await self.get_json(self.URL, params={"series_id": sid, "api_key": self.keys.get("FRED_API_KEY"),
                                                      "file_type": "json", "sort_order": "desc", "limit": 10})
            obs = [(o["date"], float(o["value"])) for o in j.get("observations", []) if o.get("value") not in (".", "")]
            if obs:
                out[sid] = {"date": obs[0][0], "value": obs[0][1],
                            "prev_date": obs[1][0] if len(obs) > 1 else None,
                            "prev_value": obs[1][1] if len(obs) > 1 else None,
                            "week_ago_value": obs[min(5, len(obs) - 1)][1]}
            await asyncio.sleep(0.6)
        if not out:
            raise RuntimeError("FRED returned no observations")
        return out


class FedCalendar(PollConnector):
    name = "fed"
    title = "Federal Reserve FOMC calendar"
    interval = 86400.0

    async def poll(self):
        cache = self.ctx.data_dir / "fomc.json"
        async with self.session.get(fedmod.FOMC_URL, headers={"User-Agent": POLITE_UA},
                                    timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            text = await r.text()
        meetings = fedmod.parse_fomc(text)
        if len(meetings) < 8:
            raise RuntimeError(f"parsed only {len(meetings)} meetings: page layout changed?")
        cache.write_text(json.dumps({"fetched_at": time.time(), "meetings": meetings}), encoding="utf-8")
        self.health.mark_msg(self.now())
        self.engine.on_fomc(meetings)

    def load_cache(self) -> None:
        p = self.ctx.data_dir / "fomc.json"
        try:
            meetings = json.loads(p.read_text(encoding="utf-8"))["meetings"]
            self.engine.on_fomc(meetings)
        except (OSError, ValueError, KeyError):
            pass


class FFCalendar(PollConnector):
    """Forex Factory's own weekly JSON export (personal use; polled every 2 h)."""
    name = "ffcal"
    title = "Forex Factory weekly calendar"
    interval = 7200.0
    cache_ttl = 7200.0
    URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

    def apply(self, data):
        self.health.mark_msg(self.now(), len(data))
        self.engine.on_calendar(data)

    async def fetch(self):
        j = await self.get_json(self.URL, headers={"User-Agent": POLITE_UA})
        events = []
        for e in j:
            try:
                ts = dt.datetime.fromisoformat(e["date"]).timestamp()
            except (KeyError, ValueError):
                continue
            events.append({"title": e.get("title", ""), "country": e.get("country", ""), "impact": e.get("impact", ""),
                           "ts": ts, "forecast": e.get("forecast") or None, "previous": e.get("previous") or None})
        return events


def parse_farside(page: str) -> list[dict]:
    """Farside's flow table -> [{date, total_musd}] for days that have a total (newest last)."""
    tables = re.findall(r"<table.*?</table>", page, re.S)
    if not tables:
        return []
    tb = max(tables, key=len)
    out = []
    for row in re.findall(r"<tr.*?</tr>", tb, re.S):
        cells = [htmlmod.unescape(re.sub(r"<[^>]+>", "", c)).strip()
                 for c in re.findall(r"<t[dh].*?</t[dh]>", row, re.S)]
        if len(cells) < 3:
            continue
        try:
            d = dt.datetime.strptime(cells[0], "%d %b %Y").date()
        except ValueError:
            continue
        tot = cells[-1].replace(",", "")
        if tot in ("-", ""):
            continue
        neg = tot.startswith("(") and tot.endswith(")")
        try:
            v = float(tot.strip("()"))
        except ValueError:
            continue
        filled = sum(1 for c in cells[1:-1] if c not in ("-", ""))
        if filled == 0 and v == 0:
            continue                      # today's placeholder row before any fund reported
        out.append({"date": d.isoformat(), "total_musd": -v if neg else v})
    return out


class FarsideEtf(PollConnector):
    name = "farside"
    title = "Farside spot ETF flows (daily)"
    interval = 7200.0
    cache_ttl = 7200.0
    PAGES = {"btc": "https://farside.co.uk/btc/", "eth": "https://farside.co.uk/eth/"}

    def apply(self, data):
        self.health.mark_msg(self.now(), len(data))
        self.engine.on_etf_flows(data)

    async def fetch(self):
        res = {}
        for k, url in self.PAGES.items():
            async with self.session.get(url, headers={"User-Agent": BROWSER_UA},
                                        timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    raise RuntimeError(f"{k}: HTTP {r.status}")
                page = await r.text()
            rows = parse_farside(page)
            if rows:
                last = rows[-1]
                streak = 0
                for row in reversed(rows):
                    if (row["total_musd"] < 0) == (last["total_musd"] < 0) and row["total_musd"] != 0:
                        streak += 1
                    else:
                        break
                res[k] = {"last_date": last["date"], "last_total_musd": last["total_musd"],
                          "streak": streak if last["total_musd"] >= 0 else -streak,
                          "week_total_musd": round(sum(r["total_musd"] for r in rows[-5:]), 1),
                          "recent": rows[-7:], "source": url}
            await asyncio.sleep(1.0)
        return res


class RssFeeds(PollConnector):
    name = "rss"
    title = "RSS headlines"
    interval = 180.0

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.feeds = cfg.get("feeds", [])
        self.validators: dict[str, dict] = {}
        self.feed_state: dict[str, dict] = {}

    def available(self):
        return (bool(self.feeds), "no feeds")

    async def poll(self):
        got = 0
        for f in self.feeds:
            url, name = f["url"], f.get("name", f["url"])
            hdr = {"User-Agent": BROWSER_UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"}
            v = self.validators.get(url, {})
            if v.get("etag"):
                hdr["If-None-Match"] = v["etag"]
            if v.get("lm"):
                hdr["If-Modified-Since"] = v["lm"]
            try:
                async with self.session.get(url, headers=hdr, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status == 304:
                        self.feed_state[name] = {"status": "not modified", "at": self.now()}
                        continue
                    if r.status != 200:
                        self.feed_state[name] = {"status": f"HTTP {r.status}", "at": self.now()}
                        continue
                    body = await r.read()
                    self.validators[url] = {"etag": r.headers.get("ETag"), "lm": r.headers.get("Last-Modified")}
            except Exception as e:  # noqa: BLE001 - one dead feed must not stop the others
                self.feed_state[name] = {"status": f"error {type(e).__name__}", "at": self.now()}
                continue
            items = parse_rss(body, name)
            self.feed_state[name] = {"status": f"ok ({len(items)} items)", "at": self.now()}
            if items:
                got += len(items)
                self.engine.on_news(items)
            await asyncio.sleep(0.5)
        self.health.mark_msg(self.now(), got)
        self.health.detail = ", ".join(f"{k}: {v['status']}" for k, v in self.feed_state.items())[:300]


class YahooVix(PollConnector):
    """Second VIX source: Yahoo's (unofficial) chart quote, also 15 minutes delayed, but it follows
    Cboe's overnight session (03:15-09:15 New York) where Cboe's own delayed JSON still shows the
    previous close. The engine keeps whichever quote is newer."""
    name = "yahoo_vix"
    title = "Yahoo VIX quote (15-min delayed, unofficial)"
    interval = 60.0

    def next_interval(self):
        return self.interval if clock.vix_calc_hours(self.now()) else 1800.0

    async def poll(self):
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"
        async with self.session.get(url, params={"interval": "1m", "range": "1d"}, headers={"User-Agent": BROWSER_UA},
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            j = jsonutil.loads(await r.read())
        meta = ((j.get("chart") or {}).get("result") or [{}])[0].get("meta") or {}
        px, ts, prev = meta.get("regularMarketPrice"), meta.get("regularMarketTime"), meta.get("chartPreviousClose")
        if not px or not ts:
            raise RuntimeError("no quote")
        self.health.mark_msg(self.now())
        self.engine.on_vix({"level": float(px), "prev_close": prev,
                            "change": None if not prev else round(float(px) - float(prev), 2),
                            "change_pct": None if not prev else round((float(px) / float(prev) - 1) * 100, 3),
                            "quote_time": clock.iso(float(ts)), "quote_ts": float(ts), "z_today": None,
                            "source": "Yahoo (unofficial)", "delay_note": "15 minutes delayed"})


async def yahoo_chart(session, symbol: str, interval: str = "5m", rng: str = "60d") -> list[tuple[float, float]]:
    """Unofficial Yahoo chart API (personal use) -> [(bar_start_ts, close)] for CLOSED bars."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    async with session.get(url, params={"interval": interval, "range": rng},
                           headers={"User-Agent": BROWSER_UA}, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            raise RuntimeError(f"Yahoo chart {symbol}: HTTP {r.status}")
        j = jsonutil.loads(await r.read())
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        return []
    ts = res[0].get("timestamp") or []
    closes = ((res[0].get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    step = {"1m": 60, "5m": 300, "1h": 3600}.get(interval, 300)
    now = time.time()
    return [(float(t), float(c)) for t, c in zip(ts, closes) if c is not None and t + step <= now]
