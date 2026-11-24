"""API keys: read at runtime from .env files (or the environment), never printed, logged or served.

Only the key NAMES this program needs are read (a whitelist), so unrelated secrets that live in the
same .env files (e-mail passwords, exchange trading keys) are never loaded into memory.
Every error message that leaves the process (logs, /api/sources) goes through `redact()`.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

# name -> what it unlocks (shown by `moneyflow keys`, values never shown)
KNOWN_KEYS = {
    "ALPACA_KEY_ID": "Alpaca market data (IEX real-time ETFs + news stream)",
    "ALPACA_SECRET": "Alpaca market data (secret half)",
    "FINNHUB_KEY": "Finnhub (real-time OANDA FX for the DXY, general news)",
    "FRED_API_KEY": "FRED (daily Treasury yields and credit spreads)",
    "JBLANKED_API_KEY": "JBlanked (only used by the source check; its calendar needs paid credits)",
}

_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")


def parse_env_text(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line)
        if not m:
            continue
        k, v = m.group(1), m.group(2)
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            v = v[1:-1]
        elif " #" in v:                      # inline comment after an unquoted value
            v = v.split(" #", 1)[0].rstrip()
        out[k] = v
    return out


class KeyStore:
    """Holds the whitelisted secrets. `str()`/`repr()` never reveal values."""

    def __init__(self, names=None):
        self._names = set(names or KNOWN_KEYS)
        self._vals: dict[str, str] = {}
        self._origin: dict[str, str] = {}

    def load(self, files=(), use_environ: bool = True, base: Path | None = None) -> "KeyStore":
        if use_environ:
            for n in self._names:
                v = os.environ.get(n, "").strip()
                if v:
                    self._vals[n], self._origin[n] = v, "environment"
        for f in files:
            p = Path(f)
            if base is not None and not p.is_absolute():
                p = (base / p).resolve()
            try:
                text = p.read_text(encoding="utf-8-sig")
            except OSError:
                continue
            for k, v in parse_env_text(text).items():
                if k in self._names and v and k not in self._vals:
                    self._vals[k], self._origin[k] = v, f"file:{p.name}"
        return self

    def set(self, name: str, value: str, origin: str = "code") -> None:   # tests
        self._names.add(name)
        self._vals[name], self._origin[name] = value, origin

    def get(self, name: str) -> str | None:
        return self._vals.get(name)

    def has(self, *names: str) -> bool:
        return all(self._vals.get(n) for n in names)

    def describe(self) -> dict[str, str]:
        """Which keys are present and where they came from -- never the values."""
        return {n: (f"set ({self._origin[n]})" if n in self._vals else "missing") for n in sorted(self._names)}

    def redact(self, text) -> str:
        s = str(text)
        for v in self._vals.values():
            if v and len(v) >= 6 and v in s:
                s = s.replace(v, "***")
        # belt and braces: token=..., apiKey=..., key=... in URLs
        return re.sub(r"(?i)((?:token|api_key|apikey|key|secret)=)[^&\s'\"]+", r"\1***", s)

    def __repr__(self) -> str:  # never leak values through debugging output
        return f"KeyStore({self.describe()})"

    __str__ = __repr__


class RedactingFilter(logging.Filter):
    """Logging filter that scrubs secret values from every record (message, args and exceptions)."""

    def __init__(self, keys: KeyStore):
        super().__init__()
        self.keys = keys

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(record.msg)
        if record.exc_info:
            import traceback
            msg = msg + "\n" + "".join(traceback.format_exception(*record.exc_info))
            record.exc_info = None
            record.exc_text = None
        record.msg = self.keys.redact(msg)
        record.args = ()
        return True
