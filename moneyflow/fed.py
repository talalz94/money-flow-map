"""The Fed: FOMC meeting dates from federalreserve.gov, and the countdown to the next rate decision.

The decision (statement) is released at 14:00 New York on the LAST day of a scheduled meeting;
meetings marked * also publish the Summary of Economic Projections. Notation votes and cancelled
meetings are not decision days.
"""
from __future__ import annotations

import datetime as dt
import re

from . import clock

FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
_MONTHS = {m: i for i, m in enumerate(("january", "february", "march", "april", "may", "june", "july", "august",
                                       "september", "october", "november", "december"), start=1)}


def _month(name: str) -> int | None:
    n = name.strip().lower()
    return _MONTHS.get(n) or next((v for k, v in _MONTHS.items() if k.startswith(n[:3])), None)


def parse_fomc(html: str) -> list[dict]:
    """-> [{"decision_ts", "date", "sep", "unscheduled"}] sorted by time."""
    out = []
    parts = re.split(r"(\d{4}) FOMC Meetings", html)
    for i in range(1, len(parts) - 1, 2):
        year, body = int(parts[i]), parts[i + 1]
        months = re.findall(r'fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>', body)
        dates = re.findall(r'fomc-meeting__date[^>]*>([^<]+)</div>', body)
        for mon, day in zip(months, dates):
            txt = " ".join(day.split())
            if re.search(r"notation|cancel", txt, re.I):
                continue
            m = re.match(r"(\d{1,2})(?:\s*-\s*(\d{1,2}))?\s*(\*)?", txt)
            if not m:
                continue
            last_day = int(m.group(2) or m.group(1))
            month = _month(mon.split("/")[-1])            # "Apr/May 30-1" -> May 1
            if month is None:
                continue
            first = _month(mon.split("/")[0])
            yr = year
            if first == 12 and month == 1:                # a meeting spanning the new year
                yr += 1
            d = dt.date(yr, month, last_day)
            ts = dt.datetime(d.year, d.month, d.day, 14, 0, tzinfo=clock.NY).timestamp()
            out.append({"decision_ts": ts, "date": d.isoformat(), "sep": bool(m.group(3)),
                        "unscheduled": "unscheduled" in txt.lower()})
    out.sort(key=lambda r: r["decision_ts"])
    dedup, seen = [], set()
    for r in out:
        if r["date"] not in seen:
            seen.add(r["date"])
            dedup.append(r)
    return dedup


def countdown_text(seconds: float) -> str:
    s = max(0, int(seconds))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m = s // 60
    return f"{d}d {h:02d}h {m:02d}m" if d else f"{h}h {m:02d}m"


def next_decision(meetings: list[dict], now: float, local_tz: str = "") -> dict | None:
    """The next decision at or after `now` (a decision stays "next" for 30 minutes after 14:00).
    `local_tz`: an IANA zone for the second clock, or "" for this PC's own zone."""
    for r in meetings:
        if r["decision_ts"] + 1800 >= now:
            ts = r["decision_ts"]
            ny = clock.at(ts, clock.NY)
            loc = clock.to_local(ts, local_tz)
            secs = ts - now
            label = "FOMC rate decision + projections" if r["sep"] else "FOMC rate decision"
            return {
                "decision_at": clock.iso(ts), "decision_ts": ts,
                "seconds": round(secs), "days": round(secs / 86400, 2), "countdown": countdown_text(secs),
                "when_ny": f"{ny:%a, %b} {ny.day}, {ny:%I:%M %p} New York".replace(" 0", " "),
                "when_local": f"{loc:%a, %b} {loc.day}, {loc:%I:%M %p}".replace(" 0", " "),
                "local_tz": local_tz or "system", "local_label": clock.local_label(local_tz), "sep": r["sep"], "label": label,
                "press_conference_ny": "2:30 PM", "source": FOMC_URL, "in_progress": secs < 0,
            }
    return None
