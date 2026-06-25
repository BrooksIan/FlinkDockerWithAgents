"""Prometheus metrics and structured logging for the platform."""

from __future__ import annotations

import json
import logging
import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

API_REQUESTS = Counter(
    "apemosyne_api_requests_total",
    "Total HTTP requests to the control API",
    ["method", "path", "status"],
)
FLINK_REACHABLE = Gauge(
    "apemosyne_flink_reachable",
    "1 when Flink REST responds, else 0",
)
FLINK_JOBS_RUNNING = Gauge(
    "apemosyne_flink_jobs_running",
    "Number of RUNNING Flink jobs",
)
FLINK_SLOTS_FREE = Gauge(
    "apemosyne_flink_slots_free",
    "Free TaskManager slots",
)
AGENTS_REGISTERED = Gauge(
    "apemosyne_agents_registered",
    "Registered agents in the manifest",
)
REQUEST_LATENCY = Histogram(
    "apemosyne_api_request_duration_seconds",
    "HTTP request latency",
    ["method", "path"],
)


def configure_logging(*, json_logs: bool = False) -> None:
    if not json_logs:
        return
    handler = logging.StreamHandler()

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "time": self.formatTime(record, self.datefmt),
            }
            if hasattr(record, "extra_fields"):
                payload.update(record.extra_fields)  # type: ignore[attr-defined]
            return json.dumps(payload)

    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("apemosyne")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


def refresh_flink_gauges(
    *,
    reachable: bool,
    jobs_running: int = 0,
    slots_free: int = 0,
    agents_registered: int = 0,
) -> None:
    FLINK_REACHABLE.set(1 if reachable else 0)
    FLINK_JOBS_RUNNING.set(jobs_running)
    FLINK_SLOTS_FREE.set(slots_free)
    AGENTS_REGISTERED.set(agents_registered)


def track_request(method: str, path: str, status: int, duration_sec: float) -> None:
    API_REQUESTS.labels(method=method, path=path, status=str(status)).inc()
    REQUEST_LATENCY.labels(method=method, path=path).observe(duration_sec)
