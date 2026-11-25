"""Stop the service when nobody uses it: it must not keep running in the background.

In use = at least one WebSocket or SSE stream is open (a dashboard tab, or an app streaming), or
there was an HTTP request (the dashboard page, the REST API) or a webhook delivery within the grace
period. When neither holds for `grace_s` (default 5 minutes, long enough for a page refresh), the
service stops cleanly: feeds and background tasks stop, the history cache is saved, the port is
freed and the process exits. Being started and never opened counts the same. grace_s = 0 keeps it
running (config `[server] idle_shutdown_minutes = 0`, or `moneyflow run --stay-running`).

Times are monotonic seconds (time.monotonic()), so a wall-clock change cannot trigger a stop.
"""
from __future__ import annotations

import time


class IdleGuard:
    def __init__(self, grace_s: float, now: float | None = None):
        self.grace = max(0.0, float(grace_s))
        now = time.monotonic() if now is None else now
        self.started = now
        self.last = now                  # the start counts as activity: never opened -> stops after the grace
        self.streams = 0
        self.last_what = "start"

    def touch(self, now: float | None = None, what: str = "request") -> None:
        now = time.monotonic() if now is None else now
        if now >= self.last:
            self.last, self.last_what = now, what

    def stream_opened(self, now: float | None = None) -> None:
        self.streams += 1
        self.touch(now, "stream opened")

    def stream_closed(self, now: float | None = None) -> None:
        self.streams = max(0, self.streams - 1)
        self.touch(now, "stream closed")        # the grace counts from the last viewer leaving

    def idle_s(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return 0.0 if self.streams else max(0.0, now - self.last)

    def should_stop(self, now: float | None = None) -> bool:
        return self.grace > 0 and self.streams == 0 and self.idle_s(now) >= self.grace

    def info(self, now: float | None = None) -> dict:
        idle = self.idle_s(now)
        return {"idle_shutdown_s": self.grace, "streams": self.streams, "idle_s": round(idle, 1),
                "last_activity": self.last_what,
                "stops_in_s": None if not self.grace or self.streams else round(max(0.0, self.grace - idle), 1)}

    def reason(self, now: float | None = None) -> str:
        mins = self.grace / 60
        never = self.last_what == "start"
        return (f"{'not opened' if never else 'no dashboard tab or app connected'} for "
                f"{mins:g} minute{'s' if mins != 1 else ''}: stopping. (Keep it running with "
                f"[server] idle_shutdown_minutes = 0 or `moneyflow run --stay-running`.)")
