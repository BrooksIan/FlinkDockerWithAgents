"""CM timeseries metric checks for monitor severities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from ratatoskr.cm.env import cm_metric_thresholds


def _latest_value(timeseries_payload: dict[str, Any]) -> float | None:
    items = timeseries_payload.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        series = item.get("timeSeries")
        if not isinstance(series, list):
            continue
        for ts in series:
            if not isinstance(ts, dict):
                continue
            data = ts.get("data")
            if not isinstance(data, list):
                continue
            for point in reversed(data):
                if not isinstance(point, dict):
                    continue
                raw = point.get("value")
                if raw is None:
                    continue
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    continue
    return None


def _hdfs_service_name(services: list[dict[str, Any]]) -> str | None:
    for svc in services:
        if str(svc.get("type") or "").upper() == "HDFS":
            name = str(svc.get("name") or "").strip()
            if name:
                return name
    return None


def _kafka_service_name(services: list[dict[str, Any]]) -> str | None:
    for svc in services:
        if str(svc.get("type") or "").upper() == "KAFKA":
            name = str(svc.get("name") or "").strip()
            if name:
                return name
    return None


MetricCheck = dict[str, Any]


def metric_checks_for_cluster(
    *,
    cluster: str,
    services: list[dict[str, Any]],
) -> list[MetricCheck]:
    """Build timeseries queries for the current cluster layout."""
    checks: list[MetricCheck] = []
    hdfs = _hdfs_service_name(services)
    if hdfs:
        checks.append(
            {
                "id": "hdfs_capacity_ratio",
                "severity": "HDFS_CAPACITY_HIGH",
                "service": hdfs,
                "metric": "hdfs_capacity_used_ratio",
                "query": (
                    f"SELECT dfs_capacity_used / dfs_capacity "
                    f"WHERE category = SERVICE AND serviceName = '{hdfs}'"
                ),
                "threshold_key": "hdfs_capacity_pct",
                "scale": 100.0,
                "comparison": "gte",
            }
        )
    kafka = _kafka_service_name(services)
    if kafka:
        checks.append(
            {
                "id": "kafka_under_replicated",
                "severity": "KAFKA_UNDER_REPLICATED",
                "service": kafka,
                "metric": "under_replicated_partitions",
                "query": (
                    f"SELECT under_replicated_partitions "
                    f"WHERE category = SERVICE AND serviceName = '{kafka}'"
                ),
                "threshold_key": "kafka_under_replicated_min",
                "scale": 1.0,
                "comparison": "gte",
            }
        )
    return checks


def evaluate_metric_checks(
    checks: list[MetricCheck],
    *,
    fetch_timeseries: Callable[[str, datetime, datetime], dict[str, Any]],
    thresholds: dict[str, float] | None = None,
    lookback_minutes: int = 15,
) -> dict[str, Any]:
    """
    Run metric checks and return breaches + raw samples.

    ``fetch_timeseries(query, from_time, to_time)`` returns CM API JSON body.
    """
    limits = thresholds if thresholds is not None else cm_metric_thresholds()
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=max(1, lookback_minutes))
    breaches: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []

    for check in checks:
        query = str(check.get("query") or "").strip()
        if not query:
            continue
        payload = fetch_timeseries(query, start, now)
        value = _latest_value(payload if isinstance(payload, dict) else {})
        threshold_key = str(check.get("threshold_key") or "")
        threshold = limits.get(threshold_key)
        scale = float(check.get("scale") or 1.0)
        scaled = value * scale if value is not None else None

        sample = {
            "id": check.get("id"),
            "metric": check.get("metric"),
            "service": check.get("service"),
            "query": query,
            "value": value,
            "scaled_value": scaled,
            "threshold_key": threshold_key,
            "threshold": threshold,
            "severity": check.get("severity"),
        }
        samples.append(sample)

        if value is None or threshold is None:
            continue
        cmp = str(check.get("comparison") or "gte")
        breached = scaled >= threshold if cmp == "gte" else scaled <= threshold
        if breached:
            breaches.append(
                {
                    **sample,
                    "breach": True,
                }
            )

    severities: list[str] = []
    for breach in breaches:
        sev = str(breach.get("severity") or "METRIC_BREACH")
        if sev not in severities:
            severities.append(sev)

    return {
        "breaches": breaches,
        "samples": samples,
        "severities": severities,
        "checked_at": now.isoformat(),
    }
