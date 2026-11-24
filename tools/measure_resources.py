"""Measure the service's CPU and memory while it runs with live data.

    python tools/measure_resources.py --minutes 10                 # headless (API only)
    python tools/measure_resources.py --minutes 10 --clients 1     # + one WebSocket client (= a dashboard tab)

Starts `python -m moneyflow run --no-dashboard` on a spare port, waits for the history to load,
then samples the process every second with psutil. CPU is reported two ways:
  * % of ONE logical core (what Task Manager's per-process "Details" view shows x logical cores)
  * % of the WHOLE machine (what Task Manager's "Processes" tab shows)
Memory: working set (RSS) and private bytes. Writes a JSON report to data/.
Stop any running moneyflow first: Alpaca's free plan allows one stream connection.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from moneyflow.client import is_running  # noqa: E402


def wait_ready(port: int, timeout: float = 180) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            h = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=3))
            if h.get("history", {}).get("state") == "done":
                return h
        except OSError:
            pass
        time.sleep(1)
    raise SystemExit("service did not become ready")


async def ws_clients(port: int, n: int, stop: asyncio.Event, counter: list):
    import aiohttp

    async def one():
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(f"http://127.0.0.1:{port}/ws?tf=all") as ws:
                while not stop.is_set():
                    try:
                        m = await ws.receive(timeout=2)
                    except asyncio.TimeoutError:
                        continue
                    if m.type == aiohttp.WSMsgType.TEXT:
                        counter[0] += 1
                        counter[1] += len(m.data)
                        json.loads(m.data)          # parse like a real client would
                    else:
                        break
    await asyncio.gather(*(one() for _ in range(n)))


def measure(port: int, minutes: float, clients: int, warmup: float) -> dict:
    cmd = [sys.executable, "-m", "moneyflow", "run", "--no-dashboard", "--port", str(port)]
    log = open(ROOT / "data" / f"measure_{port}.log", "w", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    try:
        t_start = time.time()
        wait_ready(port)
        ready_s = time.time() - t_start
        time.sleep(warmup)
        p = psutil.Process(proc.pid)
        ncpu = psutil.cpu_count()
        stop = asyncio.Event()
        counter = [0, 0]
        loop = asyncio.new_event_loop()
        import threading
        th = None
        if clients:
            th = threading.Thread(target=lambda: loop.run_until_complete(ws_clients(port, clients, stop, counter)), daemon=True)
            th.start()
            time.sleep(3)
        p.cpu_percent(None)
        cpu, rss, priv, threads = [], [], [], []
        t_end = time.time() + minutes * 60
        while time.time() < t_end:
            time.sleep(1.0)
            cpu.append(p.cpu_percent(None))
            mi = p.memory_info()
            rss.append(mi.rss / 1e6)
            priv.append(getattr(mi, "private", mi.rss) / 1e6)
            threads.append(p.num_threads())
        if clients:
            loop.call_soon_threadsafe(stop.set)
            th.join(10)
        total_cpu_s = sum(p.cpu_times()[:2])
        health = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=5))
    finally:
        proc.terminate()
        try:
            proc.wait(15)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
    q = lambda xs, f: round(sorted(xs)[min(len(xs) - 1, int(f * (len(xs) - 1)))], 2)
    return {
        "measured_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "machine": {"logical_cpus": ncpu, "cpu": _cpu_name(), "ram_gb": round(psutil.virtual_memory().total / 1e9, 1)},
        "mode": f"headless API + {clients} WebSocket client(s)" if clients else "headless API, no clients",
        "seconds": len(cpu), "startup_to_ready_s": round(ready_s, 1),
        "cpu_pct_of_one_core": {"mean": round(statistics.mean(cpu), 2), "median": q(cpu, 0.5), "p95": q(cpu, 0.95), "max": round(max(cpu), 1)},
        "cpu_pct_of_machine": {"mean": round(statistics.mean(cpu) / ncpu, 3), "p95": round(q(cpu, 0.95) / ncpu, 3)},
        "rss_mb": {"start": round(rss[0], 1), "end": round(rss[-1], 1), "max": round(max(rss), 1)},
        "private_mb": {"end": round(priv[-1], 1), "max": round(max(priv), 1)},
        "threads": max(threads), "process_cpu_seconds_total": round(total_cpu_s, 1),
        "ws_messages": counter[0], "ws_mb_received": round(counter[1] / 1e6, 1),
        "connectors": health.get("connectors"),
    }


def _cpu_name() -> str:
    try:
        import platform
        return platform.processor()
    except Exception:  # noqa: BLE001
        return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--clients", type=int, default=0)
    ap.add_argument("--port", type=int, default=8790)
    ap.add_argument("--warmup", type=float, default=20)
    a = ap.parse_args()
    if is_running():
        sys.exit("moneyflow is running on :8765 - stop it first (Alpaca allows one stream connection).")
    (ROOT / "data").mkdir(exist_ok=True)
    rep = measure(a.port, a.minutes, a.clients, a.warmup)
    out = ROOT / "data" / f"resources_{'c' + str(a.clients) if a.clients else 'headless'}_{dt.datetime.now():%Y%m%d_%H%M}.json"
    out.write_text(json.dumps(rep, indent=1), encoding="utf-8")
    print(json.dumps(rep, indent=1))
    print(f"\nreport: {out}")


if __name__ == "__main__":
    main()
