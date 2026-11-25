"""Time: an NTP-corrected clock, the New York trading day, and market calendars.

Why NTP: on the development PC the Windows clock ran 0.95 s FAST. Data age and latency are
"now minus the exchange's own timestamp", so an uncorrected clock would add ~1 s of fake lag to
every source. `Clock` measures the offset with SNTP (several servers, lowest round trip wins) and
re-checks it every 30 minutes and after the PC wakes from sleep.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import socket
import struct
import time
from functools import lru_cache

try:
    from zoneinfo import ZoneInfo
    NY = ZoneInfo("America/New_York")
    UTC = dt.timezone.utc
except Exception as e:  # pragma: no cover - Windows without tzdata
    raise RuntimeError("Time-zone data missing: run `pip install tzdata`") from e

log = logging.getLogger("moneyflow.clock")

NTP_SERVERS = ("time.cloudflare.com", "time.google.com", "pool.ntp.org", "time.windows.com")
_NTP_DELTA = 2208988800


def sntp_query(server: str, timeout: float = 2.0) -> tuple[float, float]:
    """One SNTP request. Returns (offset_s, round_trip_s); offset = true time - local time."""
    pkt = b"\x1b" + 47 * b"\0"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        t0 = time.time()
        s.sendto(pkt, (server, 123))
        data, _ = s.recvfrom(512)
        t3 = time.time()
    w = struct.unpack("!12I", data[:48])
    t1 = w[8] + w[9] / 2**32 - _NTP_DELTA
    t2 = w[10] + w[11] / 2**32 - _NTP_DELTA
    return ((t1 - t0) + (t2 - t3)) / 2, (t3 - t0) - (t2 - t1)


class Clock:
    """Epoch seconds corrected by the measured NTP offset. `fixed` freezes time (tests/replay)."""

    def __init__(self, servers=NTP_SERVERS, fixed: float | None = None):
        self.servers = tuple(servers)
        self.offset = 0.0
        self.rtt = None
        self.synced_at = None
        self.server = None
        self._fixed = fixed

    def now(self) -> float:
        if self._fixed is not None:
            return self._fixed
        return time.time() + self.offset

    def set_fixed(self, ts: float | None) -> None:
        self._fixed = ts

    def advance(self, seconds: float) -> None:
        if self._fixed is not None:
            self._fixed += seconds

    def sync_blocking(self) -> bool:
        best = None
        for srv in self.servers:
            try:
                off, rtt = sntp_query(srv)
            except Exception:
                continue
            if rtt >= 0 and (best is None or rtt < best[2]):
                best = (srv, off, rtt)
        if best is None:
            log.warning("NTP sync failed on all servers; using the PC clock as is")
            return False
        self.server, self.offset, self.rtt = best
        self.synced_at = time.time()
        log.info("clock offset %+.0f ms (NTP %s, rtt %.0f ms)", self.offset * 1000, self.server, self.rtt * 1000)
        return True

    async def sync(self) -> bool:
        if self._fixed is not None:
            return True
        return await asyncio.get_running_loop().run_in_executor(None, self.sync_blocking)

    def info(self) -> dict:
        return {"offset_ms": round(self.offset * 1000, 1), "ntp_server": self.server,
                "ntp_rtt_ms": None if self.rtt is None else round(self.rtt * 1000, 1),
                "synced_at": iso(self.synced_at) if self.synced_at else None}


# ── formatting ──────────────────────────────────────────────────────────────────────────────────
def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@lru_cache(maxsize=64)
def _utc_day_epoch(day: str) -> float:
    return dt.datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]), tzinfo=UTC).timestamp()


def parse_iso(s: str) -> float:
    """RFC 3339 (up to nanoseconds, 'Z' or offset) -> epoch seconds. A fast path handles the common
    'YYYY-MM-DDTHH:MM:SS[.fffffffff]Z' (every Alpaca message carries one)."""
    if len(s) >= 20 and s[-1] == "Z" and s[10] == "T" and s[13] == ":" and s[16] == ":":
        try:
            return _utc_day_epoch(s[:10]) + int(s[11:13]) * 3600 + int(s[14:16]) * 60 + float(s[17:-1])
        except ValueError:
            pass
    s = s.strip().replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        i = 0
        while i < len(rest) and rest[i].isdigit():
            i += 1
        frac, tz = rest[:i], rest[i:]
        s = f"{head}.{(frac + '000000')[:6]}{tz}"
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.timestamp()


def tz(name: str):
    return ZoneInfo(name)


FRIENDLY_TZ = {"Asia/Karachi": "Pakistan", "America/New_York": "New York", "Europe/London": "London",
               "Asia/Dubai": "Dubai", "Asia/Kolkata": "India", "Asia/Singapore": "Singapore", "Asia/Tokyo": "Tokyo"}


def to_local(ts: float, name: str = "") -> dt.datetime:
    """`ts` in the named IANA zone, or in this PC's own zone when `name` is empty."""
    if name:
        return dt.datetime.fromtimestamp(ts, ZoneInfo(name))
    return dt.datetime.fromtimestamp(ts).astimezone()


def local_label(name: str = "") -> str:
    if name:
        return FRIENDLY_TZ.get(name, name.split("/")[-1].replace("_", " "))
    return time.tzname[0] if time.tzname and time.tzname[0] else "local"


# ── the trading day ─────────────────────────────────────────────────────────────────────────────
ROLL_HOUR_NY = 17   # the global FX/futures day roll: 17:00 New York


def day_roll(ts: float) -> float:
    """Epoch of the latest 17:00 New York at or before `ts` (start of the current trading day)."""
    d = dt.datetime.fromtimestamp(ts, NY)
    day = d.date() if d.hour >= ROLL_HOUR_NY else d.date() - dt.timedelta(days=1)
    return dt.datetime(day.year, day.month, day.day, ROLL_HOUR_NY, tzinfo=NY).timestamp()  # DST-safe


def roll_n_days_before(roll_ts: float, n: int) -> float:
    d = dt.datetime.fromtimestamp(roll_ts, NY).date() - dt.timedelta(days=n)
    return dt.datetime(d.year, d.month, d.day, ROLL_HOUR_NY, tzinfo=NY).timestamp()


# ── US equity calendar (rules-based, NYSE) ──────────────────────────────────────────────────────
def _easter(y: int) -> dt.date:
    a = y % 19
    b, c = divmod(y, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return dt.date(y, month, day)


def _nth_weekday(y, m, wd, n):
    first = dt.date(y, m, 1)
    return first + dt.timedelta(days=(wd - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(y, m, wd):
    nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
    last = nxt - dt.timedelta(days=1)
    return last - dt.timedelta(days=(last.weekday() - wd) % 7)


def _observed(d: dt.date) -> dt.date:
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d


EXTRA_CLOSURES = {dt.date(2025, 1, 9)}          # national day of mourning (unscheduled)


@lru_cache(maxsize=None)
def nyse_holidays(y: int) -> frozenset:
    hs = set()
    nyd = dt.date(y, 1, 1)
    if nyd.weekday() == 6:
        hs.add(nyd + dt.timedelta(days=1))
    elif nyd.weekday() < 5:
        hs.add(nyd)                               # a Saturday New Year is not observed
    hs.add(_nth_weekday(y, 1, 0, 3))              # Martin Luther King Jr. Day
    hs.add(_nth_weekday(y, 2, 0, 3))              # Washington's Birthday
    hs.add(_easter(y) - dt.timedelta(days=2))     # Good Friday
    hs.add(_last_weekday(y, 5, 0))                # Memorial Day
    if y >= 2022:
        hs.add(_observed(dt.date(y, 6, 19)))      # Juneteenth
    hs.add(_observed(dt.date(y, 7, 4)))           # Independence Day
    hs.add(_nth_weekday(y, 9, 0, 1))              # Labor Day
    hs.add(_nth_weekday(y, 11, 3, 4))             # Thanksgiving
    hs.add(_observed(dt.date(y, 12, 25)))         # Christmas
    hs |= {d for d in EXTRA_CLOSURES if d.year == y}
    return frozenset(hs)


@lru_cache(maxsize=None)
def nyse_early_closes(y: int) -> frozenset:
    ec = set()
    j3 = dt.date(y, 7, 3)
    if j3.weekday() <= 3:
        ec.add(j3)
    ec.add(_nth_weekday(y, 11, 3, 4) + dt.timedelta(days=1))   # day after Thanksgiving
    d24 = dt.date(y, 12, 24)
    if d24.weekday() <= 3:
        ec.add(d24)
    return frozenset(ec - nyse_holidays(y))


def is_us_trading_day(d: dt.date) -> bool:
    return d.weekday() < 5 and d not in nyse_holidays(d.year)


# ── trading calendars for volatility samples ────────────────────────────────────────────────────
# Liveness is one thing (a Binance perpetual on the S&P 500 trades all weekend), comparable history
# another: the S&P's typical move must be measured on days the S&P trades, or the quiet weekend
# samples drag σ down and every weekday move looks bigger than it is.
#   all        every day (crypto)
#   weekdays   Monday to Friday sessions (FX, CME futures: gold, silver, oil, copper)
#   us_equity  NYSE trading days (US stocks and ETFs, and perpetuals on them)
CALENDARS = ("all", "weekdays", "us_equity")
CALENDAR_LABELS = {"all": "all days", "weekdays": "weekdays", "us_equity": "NYSE trading days"}


def session_date(ts: float) -> dt.date:
    """The trading day a moment belongs to: days roll at 17:00 New York, so Sunday 20:00 is Monday."""
    return (dt.datetime.fromtimestamp(ts, NY) + dt.timedelta(hours=24 - ROLL_HOUR_NY)).date()


def trading_day(calendar: str, ts: float) -> bool:
    if calendar == "all":
        return True
    d = session_date(ts)
    if calendar == "weekdays":
        return d.weekday() < 5
    if calendar == "us_equity":
        return is_us_trading_day(d)
    raise ValueError(f"unknown calendar {calendar!r}")


def default_calendar(hours: str) -> str:
    return {"fx": "weekdays", "vix": "weekdays", "us_regular": "us_equity", "us_extended": "us_equity"}.get(hours, "all")


def same_clock_before(ts: float, n: int) -> float:
    """The same New York wall-clock time `n` calendar days earlier (DST-safe)."""
    return (dt.datetime.fromtimestamp(ts, NY) - dt.timedelta(days=n)).timestamp()


def us_equity_session(ts: float) -> str:
    """'regular' | 'pre' | 'post' | 'closed' for US-listed stocks/ETFs at epoch `ts`."""
    d = dt.datetime.fromtimestamp(ts, NY)
    if not is_us_trading_day(d.date()):
        return "closed"
    hm = d.hour * 60 + d.minute
    close = 13 * 60 if d.date() in nyse_early_closes(d.year) else 16 * 60
    if 9 * 60 + 30 <= hm < close:
        return "regular"
    if 4 * 60 <= hm < 9 * 60 + 30:
        return "pre"
    if close <= hm < 20 * 60:
        return "post"
    return "closed"


def fx_open(ts: float) -> bool:
    """Spot FX trades from Sunday 17:00 to Friday 17:00 New York (holidays ignored: thin, not shut)."""
    d = dt.datetime.fromtimestamp(ts, NY)
    wd, hm = d.weekday(), d.hour * 60 + d.minute
    if wd == 5:
        return False
    if wd == 6:
        return hm >= 17 * 60
    if wd == 4:
        return hm < 17 * 60
    return True


def vix_calc_hours(ts: float) -> bool:
    """Cboe computes VIX 03:15-09:15 and 09:30-16:15 New York on US trading days."""
    d = dt.datetime.fromtimestamp(ts, NY)
    if not is_us_trading_day(d.date()):
        return False
    hm = d.hour * 60 + d.minute
    return (3 * 60 + 15 <= hm < 9 * 60 + 15) or (9 * 60 + 30 <= hm < 16 * 60 + 15)


def market_open(profile: str, ts: float) -> bool:
    """Is a market with this hours profile trading at `ts`? (Resolved per minute and cached: it is
    asked on every tick of a session-based instrument.)"""
    if profile in ("24/7", "crypto", "always", "daily"):
        return True
    return _market_open_minute(profile, int(ts // 60))


@lru_cache(maxsize=8192)
def _market_open_minute(profile: str, minute: int) -> bool:
    return _market_open(profile, minute * 60 + 30)


def _market_open(profile: str, ts: float) -> bool:
    if profile in ("24/7", "crypto", "always"):
        return True
    if profile == "fx":
        return fx_open(ts)
    if profile == "us_regular":
        return us_equity_session(ts) == "regular"
    if profile == "us_extended":
        return us_equity_session(ts) != "closed"
    if profile == "vix":
        return vix_calc_hours(ts)
    if profile == "daily":
        return True
    raise ValueError(f"unknown hours profile {profile!r}")


def market_status(ts: float) -> dict:
    return {"us_equities": us_equity_session(ts), "fx": "open" if fx_open(ts) else "closed",
            "crypto": "open", "vix": "calculating" if vix_calc_hours(ts) else "not calculated"}
