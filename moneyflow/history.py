"""On-disk cache of each instrument's grids, so a restart only downloads the missing tail
(this keeps start-up fast and far below Binance's REST weight limits). Plain JSON, git-ignored."""
from __future__ import annotations

import math
import os
import re
import time
from pathlib import Path

from . import clock, jsonutil
from .series import Instrument, NaN

# 2: a step its feed did not cover is never flagged live (it is empty until back-filled). A version-1
# cache can hold a sleep's or an outage's flat stretch flagged live (25 Nov: Bitcoin flat for 26
# steps), so its whole range is downloaded again once.
# 3: an ETF's steps outside its regular session hold the session's last close. Older caches can hold
# pre- and post-market bars there (carried over nights and weekends): they are rewritten on loading.
# 4: ... and none of them is empty: a download with no bar for an ETF (a weekend, a holiday, before its
# first print) carried nothing, so older caches can hold an empty night or weekend: filled on loading
# (any version: a start-up download that failed until after the first roll left them empty too).
# 5: ... and an ETF's sessions that a sleep or an outage from before its first trade of a run passed over
# are a gap, not its last close carried (not live, no gap, never fetched): an ETF's cache of format 4 or
# older can hold such sessions, so its whole range is downloaded again once.
VERSION = 5


def _file(dirpath: Path, inst: Instrument) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", inst.key)
    return dirpath / f"{safe}.json"


def _pack(vals) -> list:
    """NaN as null (JSON has no NaN); the floats as they are (rounding each one was half of a save's
    time, for files no smaller)."""
    return [None if (v != v) else v for v in vals]


def _oldest_first(ring, last_k: int, size: int) -> list:
    """A ring buffer's steps last_k - size + 1 .. last_k, oldest first (slices, not a get per step:
    the final save at a stop took seconds on a busy PC)."""
    i = (last_k + 1) % size
    return list(ring[i:] + ring[:i])


def save(dirpath: Path, inst: Instrument) -> bool:
    if inst.g5.last_k is None:
        return False
    dirpath.mkdir(parents=True, exist_ok=True)
    out = {"v": VERSION, "key": inst.key}
    for name in ("g1", "g5"):
        g = getattr(inst, name)
        if g.last_k is None:
            continue
        k0 = g.last_k - g.size + 1
        vals = _oldest_first(g.vals, g.last_k, g.size)
        live = _oldest_first(g.live, g.last_k, g.size)
        # trim leading unknowns
        i = next((n for n, v in enumerate(vals) if v == v), len(vals))
        out[name] = {"k0": k0 + i, "c": _pack(vals[i:]), "l": live[i:]}
    if inst.v5 is not None and inst.v5.last_k is not None:
        v = inst.v5
        k0 = v.last_k - v.size + 1
        qs, tbs = _oldest_first(v.q, v.last_k, v.size), _oldest_first(v.tb, v.last_k, v.size)
        i = next((n for n, x in enumerate(qs) if x == x), len(qs))
        out["v5"] = {"k0": k0 + i, "q": _pack(qs[i:]), "tb": _pack(tbs[i:])}
    # the gaps a download has yet to fill: an old cache's one-time whole download (mark_holes) is a gap
    # at its first value that nothing in the data shows, so a stop before it succeeded lost it for good
    holes = {name: h for name, h in inst.holes.items() if h is not None}
    if holes:
        out["holes"] = holes
    # how far back real downloads reach (None: never; a cache of only live minutes)
    out["from"] = inst.dl_from
    # this process's own temporary file: a copy started while this one still saves (the port is
    # free by then) must not write into the same one
    tmp = _file(dirpath, inst).with_suffix(f".{os.getpid()}.tmp")
    tmp.write_bytes(jsonutil.dumps_compact_bytes(out))
    for wait in (0.05, 0.1, 0.2, 0.4, None):
        try:
            tmp.replace(_file(dirpath, inst))
            return True
        except PermissionError:
            # Windows refuses while another process has the file open (a copy loading it at its
            # start): try again shortly, and leave no temporary file behind if it stays refused
            if wait is None:
                tmp.unlink(missing_ok=True)
                raise
            time.sleep(wait)
    return True


DAILY_FILE = "daily.json"


def save_daily(dirpath: Path, insts) -> int:
    """Every instrument's daily closes (series.Daily, the 1M window's σ) in one small file, written
    after each daily download (they change once a day: the final save at a close has none of them to
    write). Returns the instruments written."""
    out = {}
    for i in insts:
        d = i.daily
        if not d.rows:
            continue
        days = sorted(d.rows)
        out[i.key] = {"d": days, "c": [d.rows[k][0] for k in days], "q": _pack([d.rows[k][1] for k in days]),
                      "at": d.fetched_at, "src": d.source, "own": d.own}
    dirpath.mkdir(parents=True, exist_ok=True)
    p = dirpath / DAILY_FILE
    tmp = p.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(jsonutil.dumps_compact({"v": 1, "instruments": out}), encoding="utf-8")
    tmp.replace(p)
    return len(out)


def load_daily(dirpath: Path, insts, now: float) -> int:
    """The daily closes saved by save_daily, for the instruments given (by key). Returns how many."""
    try:
        d = jsonutil.loads((dirpath / DAILY_FILE).read_bytes())
    except (OSError, ValueError):
        return 0
    saved = (d.get("instruments") or {}) if d.get("v") == 1 else {}
    n = 0
    for i in insts:
        e = saved.get(i.key)
        if not e:
            continue
        rows = [(k * 86400, c, NaN if q is None else q) for k, c, q in zip(e.get("d", []), e.get("c", []), e.get("q", []))]
        i.daily.merge(rows, now)
        i.daily.fetched_at, i.daily.source, i.daily.own = e.get("at"), e.get("src"), bool(e.get("own", True))
        n += 1
    return n


TMP_MAX_AGE = 600.0


def clean_tmp(dirpath: Path, max_age: float = TMP_MAX_AGE) -> int:
    """Remove temporary files no save is writing any more (a save killed midway, or one whose replace
    stayed refused): those of a process that has ended (the id in `<name>.<pid>.tmp`), and any ten
    minutes old (a save writes and replaces its file within seconds; a reused process id waits that long)."""
    n = 0
    now = time.time()
    try:
        files = list(dirpath.glob("*.tmp"))
    except OSError:
        return 0
    for f in files:
        parts = f.name.rsplit(".", 2)
        pid = int(parts[1]) if len(parts) == 3 and parts[1].isdigit() else None
        try:
            if now - f.stat().st_mtime > max_age or (pid is not None and not _alive(pid)):
                f.unlink()
                n += 1
        except OSError:
            pass
    return n


def _alive(pid: int) -> bool:
    """Is a process with this id running? (Unsure counts as yes.)"""
    if pid == os.getpid():
        return True
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.OpenProcess.restype = wintypes.HANDLE
        h = k32.OpenProcess(0x1000, False, pid)            # PROCESS_QUERY_LIMITED_INFORMATION
        if not h:
            return ctypes.get_last_error() != 87             # ERROR_INVALID_PARAMETER: no such process
        try:
            code = wintypes.DWORD()
            return not k32.GetExitCodeProcess(h, ctypes.byref(code)) or code.value == 259   # STILL_ACTIVE
        finally:
            k32.CloseHandle(h)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def load(dirpath: Path, inst: Instrument, now: float) -> int | None:
    """Load cached grids; returns the newest cached 5-minute step (or None)."""
    p = _file(dirpath, inst)
    try:
        d = jsonutil.loads(p.read_bytes())
    except (OSError, ValueError):
        return None
    version = d.get("v")
    if version not in (1, 2, 3, 4, VERSION) or d.get("key") != inst.key:
        return None
    for name in ("g1", "g5"):
        part = d.get(name)
        if not part:
            continue
        g = getattr(inst, name)
        k_now = int(now // g.step)
        for n, (v, lv) in enumerate(zip(part["c"], part["l"])):
            k = part["k0"] + n
            if k >= k_now or k <= k_now - g.size:
                continue
            g.put(k, NaN if v is None else float(v), bool(lv))
        if inst.hours == "us_regular":
            # any version: a start-up download that failed until after the first roll left shut hours
            # empty in format 4 too (and later starts download only from the newest value)
            _session_closes(g, inst.hours, rewrite=version < 3)
    if inst.v5 is not None and d.get("v5"):
        part = d["v5"]
        k_now = int(now // inst.v5.step)
        for n, (q, tb) in enumerate(zip(part["q"], part["tb"])):
            k = part["k0"] + n
            if k >= k_now or k <= k_now - inst.v5.size:
                continue
            inst.v5.put(k, NaN if q is None else q, NaN if tb is None else tb)
    g = inst.g5
    if g.last_k is not None and not (inst.price == inst.price):
        # the newest step that held data (its live flag), not a stretch carried after it: a cache saved
        # in the evening ends in hours of the close carried over shut hours (or, written by an older
        # version, over a start whose download had not landed: a stale price), and dated at the save,
        # that carried value outranked a newer download
        ks = range(g.last_k, g.first_k() - 1, -1)
        k = next((k for k in ks if g.is_live(k) and g.get(k) == g.get(k)), None)
        if k is None:
            k = next((k for k in ks if g.get(k) == g.get(k)), None)
        if k is not None:
            inst.price = g.get(k)
            inst.ts = (k + 1) * g.step
    if "from" in d:
        f = d["from"]
        inst.dl_from = float(f) if isinstance(f, (int, float)) and math.isfinite(f) else None
    elif inst.g5.last_k is not None:
        # an older cache: its first value is as far as it reaches (a first download cut short left one
        # of only live minutes: downloaded again from the history window's start), with the shut hours
        # just before it (nothing traded there: an ETF's first bar is the window's first session)
        g = inst.g5
        k = next((k for k in range(g.first_k(), g.last_k + 1) if g.get(k) == g.get(k)), None)
        if k is not None:
            j = k
            while j > k - 1440 and not clock.market_open(inst.hours, j * g.step - 1):
                j -= 1
            k = j
        inst.dl_from = None if k is None else k * g.step
    mark_holes(inst, untrusted=version == 1 or (inst.hours == "us_regular" and version < 5), now=now)
    for name, h in (d.get("holes") or {}).items():   # (saved gaps: at the oldest step its grid holds)
        g = getattr(inst, name, None) if name in inst.holes else None
        if g is not None and isinstance(h, (int, float)) and math.isfinite(h):
            inst._hole(name, max(float(h), (int(now // g.step) - g.size + 1) * g.step))
    return inst.g5.last_k


def _session_closes(g, hours: str, rewrite: bool = True) -> None:
    """Steps outside the session take the session's last close, as the live series and the download
    write them: an empty one is filled, and with `rewrite` (format 2 and older) one holding a pre- or
    post-market print is replaced (unknown before the grid's first session step)."""
    if g.last_k is None:
        return
    if not rewrite:
        # the steps outside the session hold that close already: an empty one takes the value before
        # it (market hours looked up for empty steps only: this runs at every start, for every ETF),
        # never across an empty session step: the close after it is unknown (carried over one, a night
        # after a gap over the close took the price from before the gap)
        carry = NaN
        for k in range(g.first_k(), g.last_k + 1):
            v = g.get(k)
            if v == v:
                carry = v
            elif clock.market_open(hours, (k + 1) * g.step - 1):
                carry = NaN
            elif carry == carry:
                g.put(k, carry, False)
        return
    carry = NaN
    for k in range(g.first_k(), g.last_k + 1):
        v = g.get(k)
        if clock.market_open(hours, (k + 1) * g.step - 1):
            if v == v:
                carry = v
        elif v != v:
            if carry == carry:
                g.put(k, carry, False)
        elif rewrite and v != carry:
            g.put(k, carry, False)


def mark_holes(inst: Instrument, untrusted: bool = False, now: float | None = None) -> None:
    """Register what a loaded cache lacks, so the history download fills it: steps of an open market
    with no value (a past sleep or outage), the time from the cache's last value to now (the app was
    off: should the first download fail, a retry must still fetch it, not just the minutes since the
    start), and, from a version-1 cache (an ETF's: format 4 or older), its whole range."""
    for name in ("g1", "g5"):
        g = getattr(inst, name)
        if g.last_k is None:
            continue
        first = next((k for k in range(g.first_k(), g.last_k + 1) if g.get(k) == g.get(k)), None)
        if first is None:
            continue
        if untrusted:
            inst._hole(name, first * g.step)
            continue
        for k in range(first, g.last_k + 1):
            if g.get(k) != g.get(k) and clock.market_open(inst.hours, (k + 1) * g.step - 1):
                inst._hole(name, k * g.step)
                break
        last = next((k for k in range(g.last_k, first - 1, -1) if g.get(k) == g.get(k)), None)
        if now is not None and last is not None:
            # from the first closed step its market was open in (none if it stayed shut, e.g. an ETF
            # over a weekend: no download, no "no exchange data" give-up)
            k = _first_open(inst, g, last + 1, min(int(now // g.step), last + 1 + g.size))
            if k is not None:
                inst._hole(name, k * g.step)
    # a cache older than the 1-day grid leaves the 1-minute grid empty: its share of the time off is
    # a hole too, from the 5-minute one or from the grid's first minute (its first open one: an ETF's
    # Sunday or 2:00 never fills, and the back-fill fetched every ETF again for nothing). Without it the
    # minutes until the first live price carried the cache's 30-48 h old close into TODAY and 1H.
    g1, h5 = inst.g1, inst.holes.get("g5")
    if now is not None and h5 is not None and _empty(g1):
        k_now = int(now // g1.step)
        k = _first_open(inst, g1, max(int(h5 // g1.step), k_now - g1.size + 1), k_now)
        if k is not None:
            inst._hole("g1", k * g1.step)


def _first_open(inst: Instrument, g, k0: int, k1: int) -> int | None:
    """The first step from k0 (to before k1) the instrument's market was open in."""
    return next((k for k in range(k0, k1) if clock.market_open(inst.hours, (k + 1) * g.step - 1)), None)


def _empty(g) -> bool:
    return g.last_k is None or all(g.get(k) != g.get(k) for k in range(g.first_k(), g.last_k + 1))


def last_good_ts(inst: Instrument, max_back: int = 50) -> float | None:
    """End time of the newest step with a value, in the 5-minute grid, looking back `max_back` steps."""
    g = inst.g5
    if g.last_k is None:
        return None
    for k in range(g.last_k, max(g.last_k - max_back, g.last_k - g.size), -1):
        v = g.get(k)
        if v == v and not math.isinf(v):
            return (k + 1) * g.step
    return None
