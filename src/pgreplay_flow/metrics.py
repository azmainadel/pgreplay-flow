"""In-process metrics with Prometheus text exposition. No third-party deps.

Worker metrics are process-local counters and summaries. Database-wide gauges
(run counts per status, queue lag) come from Flow.stats(). serve_metrics()
exposes both on an HTTP endpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .flow import Flow
    from .worker import Worker

log = logging.getLogger("pgreplay_flow")

PREFIX = "pgreplay_flow_"

_HELP = {
    "runs_claimed_total": ("counter", "Runs claimed by this worker (including recoveries)."),
    "runs_recovered_total": ("counter", "Claims that took over a run whose lease had expired."),
    "runs_finished_total": ("counter", "Runs this worker drove to a terminal status."),
    "runs_suspended_total": ("counter", "Times a run was parked (sleep, backoff, signal, child)."),
    "runs_abandoned_total": ("counter", "Runs given up mid-execution (lease lost, DB down, shutdown)."),
    "runs_in_flight": ("gauge", "Runs currently executing in this worker."),
    "run_execution_seconds": ("summary", "Wall time of one execution slice of a run."),
    "step_attempts_total": ("counter", "Live step attempts, by outcome."),
    "step_attempt_seconds": ("summary", "Wall time of one step attempt."),
    "runs_purged_total": ("counter", "Runs deleted by retention cleanup."),
    "runs": ("gauge", "Runs in the database, by queue, workflow and status."),
    "queue_lag_seconds": ("gauge", "Age of the oldest runnable run that is not yet claimed."),
    "expired_leases": ("gauge", "Running runs whose lease expired (awaiting recovery)."),
}

Labels = tuple[tuple[str, str], ...]


def _labels(kw: dict[str, Any]) -> Labels:
    return tuple(sorted((k, str(v)) for k, v in kw.items()))


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _fmt_labels(labels: Labels) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in labels) + "}"


def _fmt_value(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else repr(float(v))


class Metrics:
    """Thread-safe counters, gauges and summaries (sum + count)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[Labels, float]] = defaultdict(dict)
        self._gauges: dict[str, dict[Labels, float]] = defaultdict(dict)
        self._summaries: dict[str, dict[Labels, list[float]]] = defaultdict(dict)

    def inc(self, name: str, value: float = 1.0, /, **labels: Any) -> None:
        key = _labels(labels)
        with self._lock:
            series = self._counters[name]
            series[key] = series.get(key, 0.0) + value

    def set(self, name: str, value: float, /, **labels: Any) -> None:
        with self._lock:
            self._gauges[name][_labels(labels)] = value

    def observe(self, name: str, value: float, /, **labels: Any) -> None:
        key = _labels(labels)
        with self._lock:
            s = self._summaries[name].setdefault(key, [0.0, 0.0])
            s[0] += value
            s[1] += 1

    def get(self, name: str, **labels: Any) -> float:
        """Current value of a counter or gauge series (0 if absent)."""
        key = _labels(labels)
        with self._lock:
            if name in self._counters:
                return self._counters[name].get(key, 0.0)
            return self._gauges.get(name, {}).get(key, 0.0)

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            for name, series in sorted(self._counters.items()):
                _header(lines, name)
                for labels, v in sorted(series.items()):
                    lines.append(f"{PREFIX}{name}{_fmt_labels(labels)} {_fmt_value(v)}")
            for name, series in sorted(self._gauges.items()):
                _header(lines, name)
                for labels, v in sorted(series.items()):
                    lines.append(f"{PREFIX}{name}{_fmt_labels(labels)} {_fmt_value(v)}")
            for name, series in sorted(self._summaries.items()):
                _header(lines, name)
                for labels, (total, count) in sorted(series.items()):
                    lab = _fmt_labels(labels)
                    lines.append(f"{PREFIX}{name}_sum{lab} {_fmt_value(total)}")
                    lines.append(f"{PREFIX}{name}_count{lab} {_fmt_value(count)}")
        return "\n".join(lines) + ("\n" if lines else "")


def _header(lines: list[str], name: str) -> None:
    kind, help_text = _HELP.get(name, ("untyped", name))
    lines.append(f"# HELP {PREFIX}{name} {help_text}")
    lines.append(f"# TYPE {PREFIX}{name} {kind}")


def render_stats(stats: dict[str, Any]) -> str:
    """Prometheus text for Flow.stats() output."""
    m = Metrics()
    for row in stats["runs"]:
        m.set("runs", row["count"], queue=row["queue"], workflow=row["workflow"],
              status=row["status"])
    for queue, lag in stats["queue_lag_seconds"].items():
        m.set("queue_lag_seconds", lag, queue=queue)
    m.set("expired_leases", stats["expired_leases"])
    return m.render()


async def collect(flow: Flow, worker: Worker | None = None) -> str:
    """Full exposition: database gauges plus this worker's counters."""
    parts = [render_stats(await flow.stats())]
    if worker is not None:
        parts.append(worker.metrics.render())
    return "".join(parts)


async def serve_metrics(
    flow: Flow, worker: Worker | None = None, *, host: str = "127.0.0.1", port: int = 9464
) -> asyncio.Server:
    """Serve GET /metrics in Prometheus text format.

    The endpoint is unauthenticated. It binds to localhost by default; only
    expose it on a private network or behind an authenticating proxy.
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), 5)
            # Drain headers; we don't use them.
            while await asyncio.wait_for(reader.readline(), 5) not in (b"\r\n", b"\n", b""):
                pass
            parts = request_line.decode("latin-1").split()
            if len(parts) < 2 or parts[0] != "GET" or parts[1].split("?")[0] != "/metrics":
                status, body, ctype = "404 Not Found", b"not found\n", "text/plain"
            else:
                try:
                    body = (await collect(flow, worker)).encode()
                    status, ctype = "200 OK", "text/plain; version=0.0.4; charset=utf-8"
                except Exception:  # noqa: BLE001 - report, don't crash the server
                    log.exception("metrics collection failed")
                    status, body, ctype = "503 Service Unavailable", b"collection failed\n", "text/plain"
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
            )
            await writer.drain()
        except (TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    return await asyncio.start_server(handle, host, port)
