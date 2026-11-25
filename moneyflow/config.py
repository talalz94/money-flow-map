"""Configuration: the packaged default.toml, merged with the user's moneyflow.toml and the git-ignored
moneyflow.local.toml (in that order), then command-line overrides. [[series]] merge by `id`."""
from __future__ import annotations

import copy
import tomllib
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent
PROJECT_DIR = PKG_DIR.parent
DEFAULT_FILE = PKG_DIR / "default.toml"
USER_FILES = ("moneyflow.toml", "moneyflow.local.toml")

SERIES_KINDS = {"price", "stitched", "dxy", "stable_share"}
SOURCES = {"binance_spot", "binance_fut", "alpaca", "finnhub", "coinbase", "hyperliquid", "yahoo", "computed"}
HOURS = {"24/7", "fx", "us_regular", "us_extended", "vix", "daily"}
DEFAULT_HOURS = {"binance_spot": "24/7", "binance_fut": "24/7", "coinbase": "24/7", "hyperliquid": "24/7",
                 "alpaca": "us_regular", "finnhub": "fx", "yahoo": "24/7"}


class ConfigError(ValueError):
    pass


def deep_merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if k == "series":
            continue
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    if "series" in over:
        out["series"] = merge_series(out.get("series", []), over["series"])
    return out


def merge_series(base: list[dict], over: list[dict]) -> list[dict]:
    by_id = {s["id"]: copy.deepcopy(s) for s in base}
    order = [s["id"] for s in base]
    for s in over:
        sid = s.get("id")
        if not sid:
            raise ConfigError("every [[series]] needs an id")
        if sid in by_id:
            by_id[sid].update(copy.deepcopy(s))
        else:
            by_id[sid] = copy.deepcopy(s)
            order.append(sid)
    return [by_id[i] for i in order]


def load(paths: list[Path] | None = None, overrides: dict | None = None, project_dir: Path | None = None) -> dict:
    """Load and validate. `paths` replaces the default search (moneyflow.toml, moneyflow.local.toml)."""
    project_dir = project_dir or PROJECT_DIR
    with open(DEFAULT_FILE, "rb") as f:
        cfg = tomllib.load(f)
    files = paths if paths is not None else [project_dir / n for n in USER_FILES]
    loaded = []
    for p in files:
        p = Path(p)
        if p.exists():
            with open(p, "rb") as f:
                try:
                    cfg = deep_merge(cfg, tomllib.load(f))
                except tomllib.TOMLDecodeError as e:
                    raise ConfigError(f"{p.name}: {e}") from e
            loaded.append(str(p.name))
    if overrides:
        cfg = deep_merge(cfg, overrides)
    cfg["_files"] = loaded
    cfg["_project_dir"] = str(project_dir)
    validate(cfg)
    return cfg


def validate(cfg: dict) -> None:
    ids = set()
    for s in cfg.get("series", []):
        sid = s.get("id")
        if not sid or not isinstance(sid, str):
            raise ConfigError("series without an id")
        if sid in ids:
            raise ConfigError(f"duplicate series id {sid!r}")
        ids.add(sid)
        if not s.get("enabled", True):
            continue
        kind = s.get("kind", "price")
        if kind not in SERIES_KINDS:
            raise ConfigError(f"series {sid}: unknown kind {kind!r} (use one of {sorted(SERIES_KINDS)})")
        if kind == "price":
            if s.get("source") not in SOURCES:
                raise ConfigError(f"series {sid}: unknown source {s.get('source')!r}")
            if not s.get("symbol"):
                raise ConfigError(f"series {sid}: needs a symbol")
        if kind == "stitched":
            for part in ("primary", "proxy"):
                p = s.get(part) or {}
                if p.get("source") not in SOURCES or not p.get("symbol"):
                    raise ConfigError(f"series {sid}: {part} needs a known source and a symbol")
        if kind == "dxy":
            need = {"EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDSEK", "USDCHF"}
            if set((s.get("pairs") or {})) != need:
                raise ConfigError(f"series {sid}: dxy needs pairs {sorted(need)}")
        if kind == "stable_share" and not s.get("basket"):
            raise ConfigError(f"series {sid}: stable_share needs a basket")
        h = s.get("hours")
        if h is not None and h not in HOURS:
            raise ConfigError(f"series {sid}: unknown hours {h!r}")
        c = s.get("calendar")
        if c is not None and c not in ("all", "weekdays", "us_equity"):
            raise ConfigError(f"series {sid}: unknown calendar {c!r} (use all, weekdays or us_equity)")
        for coord in ("x", "y"):
            v = s.get(coord)
            if v is not None and not (0 <= float(v) <= 1):
                raise ConfigError(f"series {sid}: {coord} must be within 0..1")
    for sid in ids:
        if sid.startswith("stock") and sid[5:].isdigit():
            raise ConfigError(f"series id {sid!r} is reserved for the notable-stock slots (stock1, stock2, ...)")
    st = cfg.get("stocks", {}) or {}
    if not (0 <= int(st.get("slots", 1)) <= 5):
        raise ConfigError("stocks.slots must be 0..5")
    if float(st.get("z_min", 2.0)) <= 0 or float(st.get("score_min", 2.5)) <= 0:
        raise ConfigError("stocks.z_min and stocks.score_min must be positive")
    if float(st.get("scan_interval_s", 300)) < 60:
        raise ConfigError("stocks.scan_interval_s must be at least 60 (Binance's request budget is shared)")
    for p in st.get("positions", []) or []:
        if len(p) != 2 or not all(0 <= float(v) <= 1 for v in p):
            raise ConfigError("stocks.positions: each slot needs [x, y] within 0..1")
    tf = cfg.get("general", {}).get("default_timeframe", "today")
    if tf not in ("1h", "today", "1w"):
        raise ConfigError(f"general.default_timeframe {tf!r} must be 1h, today or 1w")
    tot = sum(float(c.get("weight", 0)) for c in cfg.get("risk", {}).get("components", []))
    if cfg.get("risk", {}).get("components") and not (0.99 <= tot <= 1.01):
        raise ConfigError(f"risk component weights must sum to 1.0 (they sum to {tot:.3f})")
    for c in cfg.get("risk", {}).get("components", []):
        for i in c.get("inputs", []):
            if i not in ids:
                raise ConfigError(f"risk component {c.get('id')}: unknown input series {i!r}")
    idle = cfg.get("server", {}).get("idle_shutdown_minutes", 5)
    if not isinstance(idle, (int, float)) or idle < 0:
        raise ConfigError("server.idle_shutdown_minutes must be 0 (keep running) or a positive number of minutes")
    port = cfg.get("server", {}).get("port", 8765)
    if not (0 < int(port) < 65536):
        raise ConfigError("server.port out of range")
    host = cfg.get("server", {}).get("host", "127.0.0.1")
    if host not in ("127.0.0.1", "localhost", "::1") and not cfg.get("server", {}).get("token"):
        raise ConfigError("server.host exposes the API beyond this PC: set server.token as well")


def enabled_series(cfg: dict) -> list[dict]:
    return [s for s in cfg.get("series", []) if s.get("enabled", True)]


def connector_cfg(cfg: dict, name: str) -> dict:
    return cfg.get("connectors", {}).get(name, {})


def connector_enabled(cfg: dict, name: str) -> bool:
    return bool(connector_cfg(cfg, name).get("enabled", False))


def data_dir(cfg: dict) -> Path:
    p = Path(cfg.get("general", {}).get("data_dir", "data"))
    if not p.is_absolute():
        p = Path(cfg.get("_project_dir", PROJECT_DIR)) / p
    p.mkdir(parents=True, exist_ok=True)
    return p
