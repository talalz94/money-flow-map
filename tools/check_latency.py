"""Latency / freshness check of every enabled source (same as `python -m moneyflow check`).

    python tools/check_latency.py --seconds 120

Runs the real connectors without the history download and prints, per instrument, the message
rate and the NTP-corrected latency (receive time minus the exchange's timestamp), plus the age of
what each REST poller returned. Stop the running service first (Alpaca allows one stream).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moneyflow.cli import main  # noqa: E402

if __name__ == "__main__":
    args = sys.argv[1:]
    raise SystemExit(main(["check", *args]))
