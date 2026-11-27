"""JSON helpers: orjson when installed (faster, lower CPU), the standard library otherwise."""
from __future__ import annotations

import json
import math

try:  # pragma: no cover - depends on the environment
    import orjson as _orjson
except Exception:  # pragma: no cover
    _orjson = None


def loads(data):
    if _orjson is not None:
        return _orjson.loads(data)
    return json.loads(data)


def _clean(o):
    """Replace NaN/inf (not valid JSON) with None, recursively."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    return o


def dumps(obj) -> str:
    obj = _clean(obj)
    if _orjson is not None:
        return _orjson.dumps(obj).decode()
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def dumps_compact(obj) -> str:
    """Compact JSON of data that holds no NaN or inf (not checked: no pass over every value)."""
    if _orjson is not None:
        return _orjson.dumps(obj).decode()
    return json.dumps(obj, separators=(",", ":"))


def dumps_compact_bytes(obj) -> bytes:
    """dumps_compact as UTF-8 bytes (what a file gets: no decoding and encoding again)."""
    if _orjson is not None:
        return _orjson.dumps(obj)
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def dumps_pretty(obj) -> str:
    return json.dumps(_clean(obj), indent=2, ensure_ascii=False)
