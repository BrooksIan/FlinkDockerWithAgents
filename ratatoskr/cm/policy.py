"""Deterministic policy for Cloudera Manager monitoring (recommend-only)."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from ratatoskr.cm.client import CMClient
from ratatoskr.cm.recommendations import RECOMMEND_RULES, build_recommendations

_SEVERITY_PENALTIES: dict[str, int] = {
    "CM_UNREACHABLE": 100,
    "CLUSTER_BAD": 40,
    "SERVICE_DOWN": 35,
    "SERVICE_BAD": 30,
    "ROLE_DOWN": 30,
    "HEALTH_CHECK_FAIL": 25,
    "HOST_BAD": 25,
    "COMMAND_FAILED": 25,
    "PARCEL_ERROR": 20,
    "EVENT_CRITICAL": 15,
    "EVENT_WARN": 8,
    "CONFIG_STALE": 15,
    "HOST_DECOMMISSIONED": 10,
    "MGMT_UNHEALTHY": 35,
    "HDFS_CAPACITY_HIGH": 20,
    "KAFKA_UNDER_REPLICATED": 20,
    "METRIC_BREACH": 15,
    "CM_SLOW": 5,
}

_HIGH = frozenset(
    {
        "CM_UNREACHABLE",
        "CLUSTER_BAD",
        "SERVICE_DOWN",
        "SERVICE_BAD",
        "ROLE_DOWN",
        "HEALTH_CHECK_FAIL",
        "HOST_BAD",
        "COMMAND_FAILED",
        "EVENT_CRITICAL",
        "MGMT_UNHEALTHY",
    }
)
_MEDIUM = frozenset(
    {
        "PARCEL_ERROR",
        "EVENT_WARN",
        "CONFIG_STALE",
        "HOST_DECOMMISSIONED",
        "HDFS_CAPACITY_HIGH",
        "KAFKA_UNDER_REPLICATED",
        "METRIC_BREACH",
        "CM_SLOW",
    }
)

_TRACKED_KEYS = (
    "bad_services",
    "stopped_services",
    "stopped_roles",
    "failed_health_checks",
    "bad_hosts",
    "stale_services",
    "critical_events",
    "event_warnings",
    "parcel_errors",
    "failed_commands",
    "metric_breaches",
)


def classify_health(health: dict[str, Any]) -> dict[str, Any]:
    severities = list(health.get("severities") or [])
    level = "OK"
    if any(s in _HIGH for s in severities):
        level = "HIGH"
    elif any(s in _MEDIUM for s in severities):
        level = "MEDIUM"
    elif severities:
        level = "LOW"

    penalty = 0
    seen: set[str] = set()
    for severity in severities:
        if severity in seen:
            continue
        seen.add(severity)
        penalty += _SEVERITY_PENALTIES.get(severity, 5)
    score = max(0, 100 - penalty)

    findings: list[dict[str, Any]] = []
    for key in _TRACKED_KEYS:
        for item in health.get(key) or []:
            if not isinstance(item, dict):
                continue
            findings.append({**item, "finding_type": key})

    return {
        "healthy": bool(health.get("healthy")) and not severities,
        "level": level,
        "score": score,
        "severities": severities,
        "summary": "healthy" if not severities else ", ".join(severities),
        "findings": findings,
    }


def _entity_key(category: str, item: dict[str, Any]) -> str:
    parts = [
        category,
        str(item.get("fingerprint") or ""),
        str(item.get("event_kind") or ""),
        str(item.get("service") or ""),
        str(item.get("role") or ""),
        str(item.get("host_id") or ""),
        str(item.get("id") or ""),
        str(item.get("name") or ""),
        str(item.get("hostname") or ""),
    ]
    return "|".join(parts)


def diff_health(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any]:
    prev = previous or {}
    new_findings: list[str] = []
    resolved_findings: list[str] = []
    persistent_findings: list[str] = []

    prev_keys: set[str] = set()
    curr_keys: set[str] = set()
    for key in _TRACKED_KEYS:
        for item in prev.get(key) or []:
            if isinstance(item, dict):
                prev_keys.add(_entity_key(key, item))
        for item in current.get(key) or []:
            if isinstance(item, dict):
                curr_keys.add(_entity_key(key, item))

    new_findings = sorted(curr_keys - prev_keys)
    resolved_findings = sorted(prev_keys - curr_keys)
    persistent_findings = sorted(curr_keys & prev_keys)

    prev_sev = set(prev.get("severities") or [])
    curr_sev = set(current.get("severities") or [])
    prev_score = classify_health(prev).get("score") if prev else None
    curr_score = classify_health(current).get("score")

    return {
        "new_findings": new_findings,
        "resolved_findings": resolved_findings,
        "persistent_findings": persistent_findings,
        "severities_new": sorted(curr_sev - prev_sev),
        "severities_resolved": sorted(prev_sev - curr_sev),
        "score_change": (
            int(curr_score) - int(prev_score)
            if prev_score is not None and curr_score is not None
            else None
        ),
    }


def run_monitor_cycle(
    client: CMClient,
    cluster: str | None = None,
    *,
    previous_health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One poll → classify → recommend cycle (no mutations)."""
    poll_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    target_cluster = cluster or client.cluster or None

    try:
        health = client.get_cluster_health_snapshot(target_cluster)
    except Exception as exc:  # noqa: BLE001
        health = {
            "cluster": target_cluster or "",
            "healthy": False,
            "severities": ["CM_UNREACHABLE"],
            "probe": {"ok": False, "error": str(exc)},
            "counts": {},
        }

    classification = classify_health(health)
    recommendations = build_recommendations(health, classification)
    delta = diff_health(previous_health, health) if previous_health is not None else None

    health_out = {
        "cluster": health.get("cluster"),
        "healthy": health.get("healthy"),
        "severities": health.get("severities"),
        "counts": health.get("counts"),
        "probe": health.get("probe"),
        "cluster_info": health.get("cluster_info"),
        "bad_services": health.get("bad_services"),
        "stopped_services": health.get("stopped_services"),
        "stopped_roles": health.get("stopped_roles"),
        "failed_health_checks": health.get("failed_health_checks"),
        "bad_hosts": health.get("bad_hosts"),
        "stale_services": health.get("stale_services"),
        "critical_events": health.get("critical_events"),
        "event_warnings": health.get("event_warnings"),
        "suppressed_events": health.get("suppressed_events"),
        "parcel_errors": health.get("parcel_errors"),
        "failed_commands": health.get("failed_commands"),
        "metric_breaches": health.get("metric_breaches"),
        "metrics": health.get("metrics"),
        "mgmt": health.get("mgmt"),
    }

    return {
        "agent": "workflow_cm_monitor",
        "poll_id": poll_id,
        "ts": ts,
        "classification": classification,
        "delta": delta,
        "health": health_out,
        "recommendations": recommendations,
        "audit": {
            "poll_id": poll_id,
            "mode": "recommend_only",
            "recommendation_count": len(recommendations),
            "severity_level": classification.get("level"),
            "health_score": classification.get("score"),
        },
    }


__all__ = [
    "RECOMMEND_RULES",
    "build_recommendations",
    "classify_health",
    "diff_health",
    "run_monitor_cycle",
]
