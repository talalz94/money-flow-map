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
    from .server import IDLE_KEY, make_app
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
    idle = app[IDLE_KEY]
    log.info("moneyflow %s: API %sapi%s", __version__, url, f", dashboard {url}" if dashboard else " (dashboard off)")
    auto = (f"stops by itself {idle.grace / 60:g} min after the last dashboard tab or app disconnects "
            f"(or if never opened)" if idle.grace else "keeps running until stopped (auto-stop off)")
    log.info("auto-stop: %s", auto)
    print(f"\n  Money Flow Map is running.\n  API:        {url}api\n"
          f"  Dashboard:  {url if dashboard else '(off: started with --no-dashboard)'}\n"
          f"  Stop:       Ctrl+C, or it {auto}\n", flush=True)
    if open_browser and dashboard:
        webbrowser.open(url)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _graceful(*_):
        loop.call_soon_threadsafe(stop.set)

    import signal
    import threading
    previous = {}
    for name in ("SIGBREAK", "SIGTERM"):          # Ctrl+Break; kill
        if hasattr(signal, name):
            try:
                previous[name] = signal.signal(getattr(signal, name), _graceful)
            except (ValueError, OSError):
                pass
    closing, saved = threading.Event(), threading.Event()
    console = _on_console_close(loop, stop, closing, saved) if sys.platform == "win32" else None

    async def watch_idle():
        """Not in use (no dashboard tab or app) for the grace period: stop, as if Ctrl+C was pressed."""
        step = max(0.2, min(5.0, idle.grace / 6))
        while not stop.is_set():
            await asyncio.sleep(step)
            if idle.should_stop():
                why = idle.reason()
                log.info("auto-stop: %s", why)
                print(f"\n  Money Flow Map: {why}\n", flush=True)
                stop.set()

    watcher = asyncio.create_task(watch_idle(), name="idle") if idle.grace else None
    try:
        await stop.wait()
    finally:
        if watcher is not None:
            watcher.cancel()
        log.info("stopping (saving the history cache)...")
        if closing.is_set():
            # the console window is closing (or Windows logs off or shuts down): the process ends about
            # 5 s later whatever runs, so the cache is saved first
            engine.save_cache_logged()
            saved.set()
        await engine.stop_feeds()
        await site.stop()               # the port is free: start.bat can open a new copy at once
        await engine.stop(save=not closing.is_set())   # the save, not held up by open streams
        saved.set()
        await runner.cleanup()          # closes the dashboards' streams and waits for their handlers
        if console is not None:
            _console_handler_off(console)
        for name, handler in previous.items():
            try:
                signal.signal(getattr(signal, name), handler)
            except (ValueError, OSError, TypeError):
                pass
        log.info("stopped")


def _on_console_close(loop, stop: asyncio.Event, closing, saved):
    """Windows ends a console process about 5 s after its window is closed (and at logoff or shutdown)
    whatever its Python handlers do: the C runtime's handler for that event returns at once, so the
    process was gone within 60 ms, its cache unsaved. This handler starts the graceful stop instead and
    holds the process until the cache is saved (first, on this path). Called from a thread of the
    system's; returned to keep a reference to it (None if it could not be set)."""
    try:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        proto = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)
    except (ImportError, OSError, AttributeError):
        return None

    def handler(event):
        if event not in (2, 5, 6):          # CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT
            return False                    # Ctrl+C, Ctrl+Break: the signal handlers
        closing.set()
        try:
            loop.call_soon_threadsafe(stop.set)
        except RuntimeError:                # the loop has ended: nothing left to save
            return False
        saved.wait(4.5)
        return True

    h = proto(handler)
    return h if kernel32.SetConsoleCtrlHandler(h, True) else None


def _console_handler_off(h) -> None:
    import ctypes
    ctypes.WinDLL("kernel32").SetConsoleCtrlHandler(h, False)


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
    r.add_argument("--stay-running", action="store_true",
                   help="do not stop when unused (by default it stops [server] idle_shutdown_minutes, 5, "
                        "after the last dashboard tab or app disconnects)")
    r.add_argument("--idle-minutes", type=float, help="stop after this many unused minutes (0 = never)")
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
        if getattr(args, "idle_minutes", None) is not None:
            srv["idle_shutdown_minutes"] = args.idle_minutes
        if getattr(args, "stay_running", False):
            srv["idle_shutdown_minutes"] = 0
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
        from .client import is_running
        if is_running(f"http://127.0.0.1:{cfg['server']['port']}"):
            print("moneyflow is running: stop it first (Alpaca's free plan allows one stream connection,\n"
                  "so a second one would be refused and the check would measure the REST fallback).\n"
                  "While it runs, the dashboard and /api/sources already show live latency per source.",
                  file=sys.stderr)
            return 2
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
