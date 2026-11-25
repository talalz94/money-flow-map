import pytest

from conftest import DATA, ny_ts
from moneyflow import config as cfgmod
from moneyflow import fed, news
from moneyflow.connectors.pollers import parse_farside


def test_parse_fomc_fixture_and_next_decision():
    meetings = fed.parse_fomc((DATA / "fomc_excerpt.html").read_text(encoding="utf-8"))
    dates = [m["date"] for m in meetings]
    assert "2026-12-09" in dates and "2027-01-27" in dates and "2021-12-15" in dates
    dec = next(m for m in meetings if m["date"] == "2026-12-09")
    assert dec["sep"] is True and dec["decision_ts"] == ny_ts(2026, 12, 9, 14, 0)
    nxt = fed.next_decision(meetings, ny_ts(2026, 11, 24, 9, 0), "Asia/Karachi")
    assert nxt["decision_at"] == "2026-12-09T19:00:00.000Z"
    assert nxt["when_ny"] == "Wed, Dec 9, 2:00 PM New York"
    assert nxt["when_local"] == "Thu, Dec 10, 12:00 AM" and nxt["local_label"] == "Pakistan"
    assert fed.next_decision(meetings, ny_ts(2026, 11, 24, 9, 0))["local_tz"] == "system"   # PC's own zone
    assert nxt["label"] == "FOMC rate decision + projections"
    assert nxt["countdown"].startswith("15d ")
    # just after a decision it stays "next" for 30 minutes, then rolls to January
    assert fed.next_decision(meetings, ny_ts(2026, 12, 9, 14, 10))["decision_at"].startswith("2026-12-09")
    assert fed.next_decision(meetings, ny_ts(2026, 12, 9, 15, 0))["decision_at"].startswith("2027-01-27")


def test_parse_fomc_edge_cases():
    html = """<h4>2019 FOMC Meetings</h4>
    <div class="fomc-meeting__month"><strong>Apr/May</strong></div><div class="fomc-meeting__date">30-1</div>
    <div class="fomc-meeting__month"><strong>October</strong></div><div class="fomc-meeting__date">4 (notation vote)</div>
    <div class="fomc-meeting__month"><strong>October</strong></div><div class="fomc-meeting__date">29-30</div>"""
    ms = fed.parse_fomc(html)
    assert [m["date"] for m in ms] == ["2019-05-01", "2019-10-30"]
    assert fed.countdown_text(90061) == "1d 01h 01m"


def test_rss_and_atom_parsing_and_dedupe():
    rss = b"""<?xml version="1.0"?><rss><channel>
      <item><title>FinancialJuice: Fed&apos;s Powell says rates on hold</title><link>https://x/1</link>
      <pubDate>Tue, 24 Nov 2026 13:00:00 GMT</pubDate></item>
      <item><title>No date item</title></item></channel></rss>"""
    atom = b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Bitcoin slides as ETF outflows grow</title>
      <link href="https://y/2"/><updated>2026-11-24T13:05:00Z</updated></entry></feed>"""
    a = news.parse_rss(rss, "FJ")
    b = news.parse_rss(atom, "CD")
    assert a[0]["title"] == "Fed's Powell says rates on hold" and len(a) == 1
    assert b[0]["url"] == "https://y/2"
    book = news.NewsBook()
    now = a[0]["ts"] + 600
    assert len(book.add(a + b, now)) == 2
    assert book.add([dict(a[0], source="other")], now) == []        # same headline from another source
    latest = book.latest(10, now)
    assert latest[0]["title"].startswith("Bitcoin") and "BTC" in latest[0]["tags"] and "ETF" in latest[0]["tags"]
    assert "FED" in latest[1]["tags"] and latest[1]["age_s"] == 600


def test_farside_table_parser():
    page = """<table><tr><th></th><th>IBIT</th><th>Total</th></tr>
      <tr><td>20 Nov 2026</td><td>(544.7)</td><td>(669.2)</td></tr>
      <tr><td>23 Nov 2026</td><td>(509.3)</td><td>(569.7)</td></tr>
      <tr><td>24 Nov 2026</td><td>-</td><td>0.0</td></tr>
      <tr><td>Total</td><td>62,316</td><td>52,341</td></tr></table>"""
    rows = parse_farside(page)
    assert rows == [{"date": "2026-11-20", "total_musd": -669.2}, {"date": "2026-11-23", "total_musd": -569.7}]


def test_config_merge_by_id_and_validation(tmp_path):
    user = tmp_path / "moneyflow.toml"
    user.write_text('[server]\nport = 9001\n[[series]]\nid = "copper"\nenabled = true\n'
                    '[[series]]\nid = "silver"\nenabled = false\n[stocks]\nslots = 2\n', encoding="utf-8")
    c = cfgmod.load(paths=[user])
    ids = [s["id"] for s in cfgmod.enabled_series(c)]
    assert c["server"]["port"] == 9001 and "copper" in ids and "silver" not in ids
    assert c["stocks"]["slots"] == 2 and len(c["stocks"]["universe"]) > 100       # merged: defaults kept
    for wrong in ('[[series]]\nid = "stock1"\nsource = "binance_fut"\nsymbol = "X"\n', '[stocks]\nslots = 9\n',
                  '[stocks]\nscan_interval_s = 5\n', '[stocks]\npositions = [[1.5, 0.2]]\n'):
        f = tmp_path / "wrong.toml"
        f.write_text(wrong, encoding="utf-8")
        with pytest.raises(cfgmod.ConfigError):
            cfgmod.load(paths=[f])
    bad = tmp_path / "bad.toml"
    bad.write_text('[[risk.components]]\nid = "x"\nkind = "neg"\ninputs = ["nope"]\nweight = 1.0\n', encoding="utf-8")
    with pytest.raises(cfgmod.ConfigError):
        cfgmod.load(paths=[bad])
    lan = tmp_path / "lan.toml"
    lan.write_text('[server]\nhost = "0.0.0.0"\n', encoding="utf-8")
    with pytest.raises(cfgmod.ConfigError, match="token"):
        cfgmod.load(paths=[lan])


def test_default_risk_weights_sum_to_one(cfg):
    assert sum(c["weight"] for c in cfg["risk"]["components"]) == pytest.approx(1.0)
