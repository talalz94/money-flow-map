import logging

from moneyflow.keys import KeyStore, RedactingFilter, parse_env_text


def test_parse_env_text_handles_quotes_comments_export():
    t = """
# comment
export ALPACA_KEY_ID = "abc123"
ALPACA_SECRET='s3cr3t'
FINNHUB_KEY=tok # inline comment
EMPTY=
BROKEN LINE
"""
    d = parse_env_text(t)
    assert d["ALPACA_KEY_ID"] == "abc123"
    assert d["ALPACA_SECRET"] == "s3cr3t"
    assert d["FINNHUB_KEY"] == "tok"
    assert d["EMPTY"] == ""
    assert "BROKEN" not in d


def test_only_whitelisted_names_are_loaded(tmp_path):
    f = tmp_path / ".env"
    f.write_text("ALERT_SMTP_PASSWORD=hunter2\nBINANCE_API_SECRET=zzz\nFRED_API_KEY=fredkey123\n", encoding="utf-8")
    ks = KeyStore().load([f], use_environ=False)
    assert ks.get("FRED_API_KEY") == "fredkey123"
    assert ks.get("ALERT_SMTP_PASSWORD") is None       # never read into memory
    assert ks.get("BINANCE_API_SECRET") is None
    assert "fredkey123" not in repr(ks) and "fredkey123" not in str(ks)
    assert ks.describe()["FRED_API_KEY"].startswith("set (file:.env")


def test_first_file_wins_and_missing_files_are_ignored(tmp_path):
    a, b = tmp_path / "a.env", tmp_path / "b.env"
    a.write_text("FINNHUB_KEY=first\n", encoding="utf-8")
    b.write_text("FINNHUB_KEY=second\nFRED_API_KEY=fromb1\n", encoding="utf-8")
    ks = KeyStore().load([tmp_path / "nope.env", a, b], use_environ=False)
    assert ks.get("FINNHUB_KEY") == "first"
    assert ks.get("FRED_API_KEY") == "fromb1"


def test_redact_values_and_url_tokens(keys):
    s = "wss://ws.finnhub.io?token=finnhubtesttoken failed; key TESTKEYID12345 and api_key=whatever&x=1"
    r = keys.redact(s)
    assert "finnhubtesttoken" not in r and "TESTKEYID12345" not in r and "whatever" not in r
    assert "token=***" in r and "api_key=***" in r


def test_logging_filter_scrubs_messages_args_and_tracebacks(keys, caplog):
    log = logging.getLogger("test.redact")
    log.addFilter(RedactingFilter(keys))
    with caplog.at_level(logging.INFO, logger="test.redact"):
        log.info("connecting with %s", "TESTSECRET67890")
        try:
            raise RuntimeError("handshake failed for wss://ws.finnhub.io?token=finnhubtesttoken")
        except RuntimeError:
            log.exception("stream error")
    text = caplog.text
    assert "TESTSECRET67890" not in text
    assert "finnhubtesttoken" not in text
    assert "***" in text
