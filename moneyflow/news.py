"""Headlines for the news wire: merged from several sources, de-duplicated, tagged with assets.

Each item keeps its source, link and publication time; its age is shown, so a late headline is
never presented as breaking. The dashboard ticker shows the newest items first.
"""
from __future__ import annotations

import email.utils
import html
import re
import time
import xml.etree.ElementTree as ET
from collections import deque

from . import clock

TAGS = [  # (tag shown on the ticker, regex over the headline)
    ("FED", r"\bfed\b|fomc|powell|federal reserve|rate (?:cut|hike|decision)|warsh"),
    ("BTC", r"\bbitcoin\b|\bbtc\b"),
    ("ETH", r"\bether(?:eum)?\b|\beth\b"),
    ("STABLE", r"stablecoin|\busdt\b|\busdc\b|tether"),
    ("GOLD", r"\bgold\b|bullion"),
    ("OIL", r"\boil\b|crude|opec|brent|\bwti\b"),
    ("BONDS", r"treasur|\byields?\b|\bbonds?\b|\b10-year\b"),
    ("USD", r"\bdollar\b|\bdxy\b|\bfx\b|\byen\b|\beuro\b"),
    ("STOCKS", r"\bstocks?\b|s&p|nasdaq|\bdow\b|equities|wall street"),
    ("ANT", r"anthropic|\bclaude\b"),
    ("AI", r"\bai\b|nvidia|openai|artificial intelligence"),
    ("ETF", r"\betfs?\b"),
    ("MACRO", r"\bcpi\b|inflation|payrolls|\bgdp\b|\bpce\b|jobless|unemployment|tariff"),
]
_TAG_RE = [(t, re.compile(p, re.I)) for t, p in TAGS]
_WS = re.compile(r"\s+")


def tags_for(text: str) -> list[str]:
    return [t for t, r in _TAG_RE if r.search(text)][:3]


def norm_title(t: str) -> str:
    t = html.unescape(t).lower()
    t = re.sub(r"^(financialjuice|breaking|update\s*\d*|live)\s*[:\-–]\s*", "", t)
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    return _WS.sub(" ", t).strip()[:80]


def clean_title(t: str) -> str:
    t = html.unescape(re.sub(r"<[^>]+>", "", t or ""))
    t = re.sub(r"^FinancialJuice:\s*", "", t)
    return _WS.sub(" ", t).strip()


def parse_rss(body: bytes | str, source: str) -> list[dict]:
    """RSS 2.0 or Atom -> [{title, url, ts, source}] (items without a usable date are dropped)."""
    if isinstance(body, str):
        body = body.encode("utf-8", "replace")
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    atom = "{http://www.w3.org/2005/Atom}"
    items = root.findall(".//item") or root.findall(f".//{atom}entry")
    out = []
    for it in items:
        title = it.findtext("title") or it.findtext(f"{atom}title") or ""
        link = it.findtext("link") or ""
        if not link:
            le = it.find(f"{atom}link")
            link = le.get("href", "") if le is not None else ""
        d = (it.findtext("pubDate") or it.findtext("{http://purl.org/dc/elements/1.1/}date")
             or it.findtext(f"{atom}updated") or it.findtext(f"{atom}published"))
        ts = None
        if d:
            try:
                ts = email.utils.parsedate_to_datetime(d.strip()).timestamp()
            except (TypeError, ValueError):
                try:
                    ts = clock.parse_iso(d)
                except ValueError:
                    ts = None
        title = clean_title(title)
        if title and ts:
            out.append({"title": title, "url": link.strip(), "ts": ts, "source": source})
    return out


class NewsBook:
    """The latest headlines, newest first, de-duplicated across sources by normalised title."""

    def __init__(self, maxlen: int = 80, max_age_s: float = 36 * 3600):
        self.items: deque = deque(maxlen=maxlen)
        self.seen: dict[str, float] = {}
        self.max_age_s = max_age_s
        self.version = 0

    def add(self, items: list[dict], now: float | None = None) -> list[dict]:
        now = now or time.time()
        new = []
        for it in sorted(items, key=lambda x: x["ts"]):
            if now - it["ts"] > self.max_age_s or it["ts"] > now + 600:
                continue
            key = norm_title(it["title"])
            if not key or key in self.seen:
                continue
            self.seen[key] = it["ts"]
            item = dict(it)
            item.setdefault("tags", tags_for(it["title"]))
            item["first_seen"] = now
            new.append(item)
        if new:
            merged = sorted(list(self.items) + new, key=lambda x: -x["ts"])[: self.items.maxlen]
            self.items.clear()
            self.items.extend(merged)
            self.version += 1
            if len(self.seen) > 4000:   # forget old keys
                cut = sorted(self.seen.values())[-2000]
                self.seen = {k: v for k, v in self.seen.items() if v >= cut}
        return new

    def latest(self, n: int = 30, now: float | None = None) -> list[dict]:
        now = now or time.time()
        out = []
        for it in list(self.items)[:n]:
            out.append({"title": it["title"], "source": it["source"], "url": it.get("url", ""),
                        "published_at": clock.iso(it["ts"]), "age_s": round(max(0.0, now - it["ts"])),
                        "tags": it.get("tags", []), "kind": it.get("kind", "news")})
        return out
