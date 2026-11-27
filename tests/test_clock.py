import datetime as dt

from conftest import ny_ts
from moneyflow import clock


def test_day_roll_is_17h_new_york_and_dst_safe():
    # 10:00 New York on Mon 2 Nov 2026 (EST) -> roll = Sun 1 Nov 17:00 EST = 22:00 UTC
    r = clock.day_roll(ny_ts(2026, 11, 2, 10))
    assert dt.datetime.fromtimestamp(r, dt.timezone.utc) == dt.datetime(2026, 11, 1, 22, 0, tzinfo=dt.timezone.utc)
    # Sat 31 Oct 2026 18:00 (EDT) -> roll = 31 Oct 17:00 EDT = 21:00 UTC
    r = clock.day_roll(ny_ts(2026, 10, 31, 18))
    assert dt.datetime.fromtimestamp(r, dt.timezone.utc) == dt.datetime(2026, 10, 31, 21, 0, tzinfo=dt.timezone.utc)
    # exactly at the roll
    assert clock.day_roll(ny_ts(2026, 11, 24, 17)) == ny_ts(2026, 11, 24, 17)
    # n days before, across the DST change
    assert clock.roll_n_days_before(ny_ts(2026, 11, 2, 17), 2) == ny_ts(2026, 10, 31, 17)


def test_us_equity_sessions_holidays_and_early_close():
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 10)) == "regular"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 8)) == "pre"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 17)) == "post"
    assert clock.us_equity_session(ny_ts(2026, 11, 24, 21)) == "closed"
    assert clock.us_equity_session(ny_ts(2026, 11, 26, 11)) == "closed"      # Thanksgiving
    assert clock.us_equity_session(ny_ts(2026, 11, 27, 12, 59)) == "regular"  # early close day
    assert clock.us_equity_session(ny_ts(2026, 11, 27, 13, 30)) == "post"
    assert clock.us_equity_session(ny_ts(2026, 11, 28, 11)) == "closed"      # Saturday
    assert dt.date(2026, 7, 3) in clock.nyse_holidays(2026)                    # 4 July on a Saturday
    assert dt.date(2027, 12, 24) in clock.nyse_holidays(2027)                  # Christmas on a Saturday
    assert dt.date(2025, 1, 9) in clock.nyse_holidays(2025)                    # unscheduled closure
    assert dt.date(2026, 4, 3) in clock.nyse_holidays(2026)                    # Good Friday


def test_fx_and_vix_hours():
    assert not clock.fx_open(ny_ts(2026, 11, 28, 12))     # Saturday
    assert not clock.fx_open(ny_ts(2026, 11, 29, 16))     # Sunday before 17:00
    assert clock.fx_open(ny_ts(2026, 11, 29, 17, 5))
    assert not clock.fx_open(ny_ts(2026, 11, 27, 17, 1))  # Friday after 17:00
    assert clock.vix_calc_hours(ny_ts(2026, 11, 24, 4))
    assert not clock.vix_calc_hours(ny_ts(2026, 11, 24, 9, 20))
    assert clock.vix_calc_hours(ny_ts(2026, 11, 24, 16, 10))
    assert clock.market_open("24/7", ny_ts(2026, 11, 28, 3))


def test_parse_iso_nanoseconds_and_offsets():
    assert abs(clock.parse_iso("2026-11-24T13:19:06.823262736Z") - clock.parse_iso("2026-11-24T13:19:06.823262Z")) < 1e-6
    assert abs(clock.parse_iso("2026-11-24T08:19:06-05:00") - clock.parse_iso("2026-11-24T13:19:06Z")) < 1e-6
    # the fast path and the general parser agree
    fast = clock.parse_iso("2026-11-24T13:19:06.5Z")
    slow = clock.parse_iso("2026-11-24T13:19:06.500+00:00")
    assert abs(fast - slow) < 1e-6 and fast == 1795526346.5


def test_fixed_clock_for_replay():
    c = clock.Clock(fixed=1000.0)
    assert c.now() == 1000.0
    c.advance(5)
    assert c.now() == 1005.0


def _reply(sent, t0, offset=0.25, rtt=0.03, li=0, version=4, mode=4, stratum=2, ref=b"GPS\0", zero=False, hold=0.0):
    """A server's reply to a request sent at t0 (its transmit stamp `sent`), `rtt` round trip; `hold`:
    how long the server says it held the request."""
    import struct
    t1 = clock._ntp_stamp(t0 + rtt / 2 + offset)               # received...
    t2 = clock._ntp_stamp(t0 + rtt / 2 + offset + hold)        # ...and answered
    if zero:
        t1 = t2 = (0, 0)
    return (bytes([li << 6 | version << 3 | mode, stratum, 6, 0xEC]) + bytes(8) + ref + bytes(8)
            + struct.pack("!II", *sent) + struct.pack("!II", *t1) + struct.pack("!II", *t2))


def test_an_ntp_reply_is_checked_before_its_offset_is_used():
    """A reply from pool.ntp.org with zero timestamps (a kiss-of-death when many copies start within
    minutes) set the clock 127 years back: every tick failed until the next sync. A reply is used only
    from a synchronised server (stratum 1-15, no alarm), in server mode, answering this request, with
    its timestamps set and a round trip of 0-5 s (a correction over a day: see the offset's choice)."""
    import pytest
    t0 = 1795528800.0
    sent = clock._ntp_stamp(t0)
    t3 = t0 + 0.03
    off, rtt = clock.parse_sntp_reply(_reply(sent, t0), sent, t0, t3)
    assert abs(off - 0.25) < 1e-6 and abs(rtt - 0.03) < 1e-6
    bad = {"kiss-of-death 'RATE'": _reply(sent, t0, stratum=0, ref=b"RATE", zero=True),
           "zero timestamps": _reply(sent, t0, zero=True),
           "not synchronised": _reply(sent, t0, li=3),               # the alarm
           "not synchronised ": _reply(sent, t0, stratum=16),
           "not a server reply": _reply(sent, t0, mode=3),
           "another request": _reply(clock._ntp_stamp(t0 - 9), t0),
           "short reply": _reply(sent, t0)[:40]}
    for why, data in bad.items():                              # each for its own reason
        with pytest.raises(clock.NTPError, match=why.strip()):
            clock.parse_sntp_reply(data, sent, t0, t3)
    with pytest.raises(clock.NTPError, match="round trip"):   # a negative one (held 1 s of 0.1)
        clock.parse_sntp_reply(_reply(sent, t0, hold=1.0), sent, t0, t0 + 0.1)


def test_the_request_carries_its_send_time_and_the_reply_must_echo_it(monkeypatch):
    """The request's transmit stamp comes back as the reply's originate stamp: a stray or stale datagram
    is not taken for the answer."""
    import socket
    import struct
    import pytest
    got = {}

    class FakeSocket:
        def __init__(self, *a):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def settimeout(self, t):
            pass

        def sendto(self, pkt, addr):
            got["pkt"], got["addr"] = pkt, addr

        def recvfrom(self, n):
            sent = struct.unpack("!II", got["pkt"][40:48]) if got.get("echo", True) else (1, 2)
            return _reply(sent, clock.time.time(), offset=1.5), got["addr"]
    monkeypatch.setattr(socket, "socket", FakeSocket)
    off, rtt = clock.sntp_query("ntp.example")
    assert len(got["pkt"]) == 48 and got["pkt"][0] == 0x1B and got["addr"] == ("ntp.example", 123)
    assert abs(off - 1.5) < 0.05 and 0 <= rtt < 1
    got["echo"] = False
    with pytest.raises(clock.NTPError):
        clock.sntp_query("ntp.example")


def test_the_clock_takes_the_servers_that_agree_and_keeps_its_offset_otherwise(monkeypatch):
    """Of the usable replies, those within half a second of their median, the shortest round trip
    first: one server far off is not taken, however short its round trip. A sync with no usable reply,
    or with replies that do not agree, leaves the offset in use."""
    answers = {}

    def query(srv, timeout=2.0):
        a = answers[srv]
        if isinstance(a, Exception):
            raise a
        return a
    monkeypatch.setattr(clock, "sntp_query", query)
    c = clock.Clock(servers=("a", "b", "c", "d"))
    answers.update(a=(0.200, 0.030), b=(0.210, 0.020), c=(37.0, 0.005), d=clock.NTPError("kiss-of-death 'RATE'"))
    assert c.sync_blocking() and c.server == "b" and abs(c.offset - 0.210) < 1e-9
    answers.update(a=OSError("timed out"), b=clock.NTPError("zero timestamps"), c=OSError("timed out"), d=OSError("x"))
    assert not c.sync_blocking() and c.server == "b" and abs(c.offset - 0.210) < 1e-9     # kept
    answers.update(a=(0.2, 0.03), b=(5.0, 0.02), c=OSError("x"), d=OSError("x"))         # two that disagree
    assert not c.sync_blocking() and abs(c.offset - 0.210) < 1e-9
    answers.update(a=(-0.4, 0.03), b=OSError("x"), c=OSError("x"), d=OSError("x"))       # one usable reply
    assert c.sync_blocking() and c.server == "a" and abs(c.offset + 0.4) < 1e-9


def test_a_correction_over_a_day_is_taken_only_where_another_server_agrees(monkeypatch):
    """A PC clock 30 hours or 2 days behind (a wrong date after a BIOS reset; Windows' own time service
    corrects at most 15 hours by default) is corrected when the servers agree: a cap on every reply
    rejected them all, and market hours, the 17:00 roll and data ages all ran 30 hours off. A lone
    well-formed reply 1024 weeks off (19.6 years) is still not taken, alone or beside a good one."""
    t0 = 1795528800.0
    sent = clock._ntp_stamp(t0)
    off, _ = clock.parse_sntp_reply(_reply(sent, t0, offset=-90000.0), sent, t0, t0 + 0.03)
    assert abs(off + 90000.0) < 1e-3                                   # (the reply itself is fine)
    answers = {}

    def query(srv, timeout=2.0):
        a = answers[srv]
        if isinstance(a, Exception):
            raise a
        return a
    monkeypatch.setattr(clock, "sntp_query", query)
    c = clock.Clock(servers=("a", "b", "c", "d"))
    week1024 = -1024 * 7 * 86400.0
    answers.update(a=(108000.2, 0.03), b=(108000.1, 0.02), c=(108000.3, 0.04), d=OSError("x"))
    assert c.sync_blocking() and c.server == "b" and abs(c.offset - 108000.1) < 1e-9   # 30 hours behind
    answers.update(a=(172800.0, 0.03), b=(172800.2, 0.05), c=OSError("x"), d=OSError("x"))
    assert c.sync_blocking() and c.server == "a" and abs(c.offset - 172800.0) < 1e-9   # two days
    answers.update(a=(0.2, 0.03), b=OSError("x"), c=OSError("x"), d=OSError("x"))
    assert c.sync_blocking() and abs(c.offset - 0.2) < 1e-9
    answers.update(a=(week1024, 0.01), b=OSError("x"), c=OSError("x"), d=OSError("x"))
    assert not c.sync_blocking() and abs(c.offset - 0.2) < 1e-9                        # alone: not taken
    answers.update(a=(week1024, 0.01), b=(0.25, 0.04), c=OSError("x"), d=OSError("x"))
    assert c.sync_blocking() and c.server == "b" and abs(c.offset - 0.25) < 1e-9       # beside a good one
    answers.update(a=(week1024, 0.01), b=(0.25, 0.04), c=(0.3, 0.03), d=(0.28, 0.05))
    assert c.sync_blocking() and c.server == "c" and abs(c.offset - 0.3) < 1e-9
    # two that agree over a day lose to a reply within a day: one good (it was -619,315,200 s) or two
    answers.update(a=(week1024, 0.01), b=(week1024 + 0.01, 0.01), c=(0.02, 0.04), d=OSError("x"))
    assert c.sync_blocking() and c.server == "c" and abs(c.offset - 0.02) < 1e-9
    answers.update(a=(week1024, 0.01), b=(week1024 + 0.01, 0.01), c=(0.02, 0.04), d=(0.0, 0.03))
    assert c.sync_blocking() and c.server == "d" and abs(c.offset) < 1e-9
    answers.update(a=(108000.2, 0.03), b=(108000.1, 0.02), c=(108000.3, 0.04), d=(108000.2, 0.05))
    assert c.sync_blocking() and c.server == "b" and abs(c.offset - 108000.1) < 1e-9   # all agree: taken


def test_clock_helpers_never_raise_on_a_time_out_of_range():
    """A bad NTP reply once set the clock 127 years back (offset -4.0e9 s): datetime raised OSError 22
    for every time before 1970 on Windows, and every tick failed until the next sync. The offset is
    bounded since (a lone correction over a day is refused); behind that, every helper that formats or
    converts a time takes one out of 1970-3000, or no number at all, without raising."""
    import math
    from moneyflow import stats
    for ts in (-4.0e9, -1.0, 0.0, 1.0e13, math.nan, math.inf, -math.inf):
        assert isinstance(clock.iso(ts), str) and clock.at(ts).year in (1970, 3000)
        clock.day_roll(ts), clock.session_date(ts), clock.same_clock_before(ts, 3), clock.to_local(ts)
        clock.roll_n_days_before(clock.day_roll(ts), 2), clock.us_equity_session(ts), clock.market_status(ts)
        for hours in ("24/7", "fx", "us_regular", "us_extended", "vix", "daily"):
            clock.market_open(hours, ts)
        for cal in ("all", "weekdays", "us_equity"):
            clock.trading_day(cal, ts)
        for tf in stats.TIMEFRAMES:
            stats.window_for(tf, ts)
    assert clock.iso(1795528800.0) == "2026-11-24T14:00:00.000Z"          # in range: as before


def test_downloads_keep_the_bars_closed_by_the_corrected_clock_not_the_pcs(monkeypatch):
    """The PC's clock two days behind (the NTP correction: +2 days). The downloads left out a bar still
    open by the PC's own clock, so the last two days of closed bars looked open and were dropped (a
    clock ahead kept an open one). They go by the caller's now, the corrected clock: Binance's
    klines, Hyperliquid's candles, Yahoo's chart."""
    import asyncio
    import json
    import time
    import types
    from moneyflow.connectors import binance as bn, hyperliquid as hl, pollers
    now = time.time() + 2 * 86400
    d0 = int(now // 86400) - 10
    opens = [(d0 + j) * 86400 for j in range(11)]            # ten closed daily bars and today's, open
    closed = [t for t in opens if t + 86400 <= now]
    assert len(closed) == 10

    class Resp:
        status = 200

        def __init__(self, body):
            self.body = body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def read(self):
            return json.dumps(self.body).encode()

    class Session:
        def get(self, url, params=None, headers=None, timeout=None):
            quote = [{"close": [1.0] * len(opens)}]
            return Resp({"chart": {"result": [{"timestamp": opens, "indicators": {"quote": quote}}]}})

        def post(self, url, json=None):
            return Resp([{"t": t * 1000, "c": "1.0", "v": "2"} for t in opens])

    async def rest_get(session, base, path, p, gate):
        return [[t * 1000, "1", "1", "1", "1.0", "1", t * 1000 + 86399999, "5", 1, "1", "2"]
                for t in opens if p["startTime"] <= t * 1000 <= p["endTime"]]
    monkeypatch.setattr(bn, "rest_get", rest_get)

    async def go():
        k = await bn.fetch_klines(Session(), "spot", "BTCUSDT", "1d", opens[0] * 1000, int(now * 1000))
        c = await hl.fetch_candles(types.SimpleNamespace(cfg={}, session=Session()), "xyz:GOLD", "1d", opens[0], now)
        y = await pollers.yahoo_chart(Session(), "SPY", "1d", "1mo", now=now)
        return k, c, y
    for rows in asyncio.run(go()):
        assert [r[0] for r in rows] == closed
