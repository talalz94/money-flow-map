"""Command line.

  python -m moneyflow run [--no-dashboard] [--open] [--port 8765]   start the service (default command)
  python -m moneyflow read [--tf today|1h|1w] [--json]               print the current read
  python -m moneyflow keys                                           which API keys were found (never values)
  python -m moneyflow check [--seconds 60]                           measure every source's latency / freshness
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import sys
import webbrowser

from . import __version__, config as cfgmod
from .keys import KeyStore, RedactingFilter


def load_keys(cfg: dict) -> KeyStore:
    files = cfg.get("keys", {}).get("env_files", [".env"])
    from pathlib import Path
    return KeyStore().load(files, base=Path(cfg["_project_dir"]))


def setup_logging(cfg: dict, keys: KeyStore, level: str | None = None) -> None:
    lvl = getattr(logging, (level or cfg.get("general", {}).get("log_level", "INFO")).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(lvl)
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")
    red = RedactingFilter(keys)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    sh.addFilter(red)
    root.addHandler(sh)
    try:
        fh = logging.handlers.RotatingFileHandler(cfgmod.data_dir(cfg) / "moneyflow.log", maxBytes=2_000_000,
                                                  backupCount=2, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
        fh.addFilter(red)
        root.addHandler(fh)
    except OSError:
        pass
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


async def serve(cfg: dict, keys: KeyStore, dashboard: bool, open_browser: bool) -> None:
    from aiohttp import web

    from .engine import Engine
    from .server import make_app
    log = logging.getLogger("moneyflow")
    engine = Engine(cfg, keys)
    app = make_app(engine, cfg, dashboard)
    runner = web.AppRunner(app, access_log=None, handle_signals=False)
    await runner.setup()
    host, port = cfg["server"]["host"], int(cfg["server"]["port"])
    site = web.TCPSite(runner, host, port, reuse_address=False)
    try:
        await site.start()
    except OSError as e:
        await runner.cleanup()
        raise SystemExit(f"Port {port} on {host} is busy ({e.strerror}). Is moneyflow already running? "
                         f"Open http://127.0.0.1:{port}/ or start with --port.")
    await engine.start()
    url = f"http://{'127.0.0.1' if host in ('0.0.0.0', '::') else host}:{port}/"
    log.info("moneyflow %s: API %sapi%s", __version__, url, f", dashboard {url}" if dashboard else " (dashboard off)")
    print(f"\n  Money Flow Map is running.\n  API:        {url}api\n"
          f"  Dashboard:  {url if dashboard else '(off: started with --no-dashboard)'}\n  Stop:       Ctrl+C\n", flush=True)
    if open_browser and dashboard:
        webbrowser.open(url)
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        log.info("stopping...")
        await engine.stop()
        await runner.cleanup()


def fmt_read(snap: dict) -> str:
    """Plain-text rendering of a one-timeframe snapshot (used by `read`)."""
    lines = []
    w = snap.get("window", {})
    r = snap.get("risk", {})
    lines.append(f"MONEY FLOW  {snap.get('label')} ({w.get('label')})   generated {snap.get('generated_at')}")
    if r.get("score") is not None:
        lines.append(f"Risk meter: {r['label']} {r['score']:+d}  (coverage {r.get('coverage', 0) * 100:.0f}%)")
    lines.append("")
    lines.append(f"{'asset':30s} {'σ':>6s} {'change':>9s}  {'status':10s} {'age':>7s}  source")
    nodes = sorted(snap.get("nodes", []), key=lambda n: -(n.get("z") or -99))
    for n in nodes:
        z = "   n/a" if n.get("z") is None else f"{n['z']:+6.1f}"
        ch = "      n/a" if n.get("change_pct") is None else f"{n['change_pct']:+8.2f}%"
        age = "" if n.get("age_s") is None else (f"{n['age_s']:.1f}s" if n["age_s"] < 120 else f"{n['age_s'] / 60:.0f}m")
        src = n.get("source", {})
        lines.append(f"{n['label'][:30]:30s} {z} {ch}  {n.get('status', ''):10s} {age:>7s}  {src.get('name', '')} {src.get('symbol', '')}")
    if snap.get("edges"):
        lines.append("")
        lines.append("Flows (inferred):")
        names = {n["id"]: n["label"] for n in snap.get("nodes", [])}
        for e in snap["edges"][:6]:
            corr = "" if e.get("corr") is None else "  corr %+.2f" % e["corr"]
            lines.append(f"  {names.get(e['from'], e['from'])} -> {names.get(e['to'], e['to'])}  weight {e['weight']:.2f}{corr}")
    if snap.get("narrative"):
        lines.append("")
        lines.append("What the flow says:")
        for b in snap["narrative"]:
            lines.append(f"  - {b['text']}")
    fed = snap.get("fed")
    if fed:
        lines.append("")
        lines.append(f"Next Fed decision: {fed['countdown']}  ({fed['when_ny']}; {fed['when_local']} {fed.get('local_label', '')})")
    lines.append("")
    lines.append("Heuristic read of public prices, not advice.")
    return "\n".join(lines)


def cmd_read(args, cfg) -> int:
    from .client import MoneyFlowClient
    import json
    url = args.url or f"http://127.0.0.1:{cfg['server']['port']}"
    c = MoneyFlowClient(url, token=cfg["server"].get("token") or None)
    try:
        snap = c.snapshot(args.tf)
    except OSError as e:
        print(f"moneyflow is not running at {url} ({e}). Start it with:  python -m moneyflow run", file=sys.stderr)
        return 2
    print(json.dumps(snap, indent=2) if args.json else fmt_read(snap))
    return 0


def cmd_keys(cfg) -> int:
    from .keys import KNOWN_KEYS
    ks = load_keys(cfg)
    print("API keys (values are never shown):")
    for name, state in ks.describe().items():
        print(f"  {name:18s} {state:28s} {KNOWN_KEYS.get(name, '')}")
    print("\nKey files searched:", ", ".join(cfg.get("keys", {}).get("env_files", [])))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="moneyflow", description="Real-time cross-market money-flow read (heuristic).")
    p.add_argument("--config", action="append", help="extra TOML file(s) merged over the defaults")
    p.add_argument("--version", action="version", version=f"moneyflow {__version__}")
    sub = p.add_subparsers(dest="cmd")
    r = sub.add_parser("run", help="start the service (default)")
    r.add_argument("--no-dashboard", action="store_true", help="API only, no web page")
    r.add_argument("--open", action="store_true", help="open the dashboard in the browser")
    r.add_argument("--port", type=int)
    r.add_argument("--host")
    r.add_argument("--log-level")
    rd = sub.add_parser("read", help="print the current read from the running service")
    rd.add_argument("--tf", default="today", choices=["1h", "today", "1w"])
    rd.add_argument("--json", action="store_true")
    rd.add_argument("--url")
    sub.add_parser("keys", help="show which API keys were found (never their values)")
    ck = sub.add_parser("check", help="measure latency and freshness of every source")
    ck.add_argument("--seconds", type=float, default=60)
    ck.add_argument("--out", help="write the JSON report here")
    args = p.parse_args(argv)
    cmd = args.cmd or "run"
    for stream in (sys.stdout, sys.stderr):      # σ, →, − print on any Windows console or pipe
        try:
            if not stream.isatty():
                stream.reconfigure(encoding="utf-8", errors="replace")
            else:
                stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass

    overrides = {}
    if cmd == "run":
        srv = {}
        if getattr(args, "port", None):
            srv["port"] = args.port
        if getattr(args, "host", None):
            srv["host"] = args.host
        if getattr(args, "no_dashboard", False):
            srv["dashboard"] = False
        if srv:
            overrides["server"] = srv
    try:
        paths = None
        if args.config:
            from pathlib import Path
            paths = [cfgmod.PROJECT_DIR / n for n in cfgmod.USER_FILES] + [Path(c) for c in args.config]
        cfg = cfgmod.load(paths, overrides)
    except cfgmod.ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2

    if cmd == "read":
        return cmd_read(args, cfg)
    if cmd == "keys":
        return cmd_keys(cfg)
    keys = load_keys(cfg)
    setup_logging(cfg, keys, getattr(args, "log_level", None))
    if cmd == "check":
        from . import latency
        return asyncio.run(latency.main(cfg, keys, args.seconds, args.out))
    dashboard = bool(cfg["server"].get("dashboard", True))
    open_browser = bool(getattr(args, "open", False) or cfg["server"].get("open_browser", False))
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(serve(cfg, keys, dashboard, open_browser))
    except KeyboardInterrupt:
        print("stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
