"""Correlate NiFi and Kafka monitor OutputEvents into incidents (deterministic)."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from ratatoskr.correlation.rules import CORRELATION_RULES, level_max


def _classification(event: dict[str, Any] | None) -> dict[str, Any]:
    if not event:
        return {"healthy": True, "level": "OK", "score": 100, "severities": []}
    c = event.get("classification")
    if isinstance(c, dict):
        return c
    health = event.get("health") if isinstance(event.get("health"), dict) else {}
    sevs = list(health.get("severities") or event.get("severities") or [])
    return {
        "healthy": not sevs,
        "level": "OK" if not sevs else "MEDIUM",
        "score": 100 if not sevs else 80,
        "severities": sevs,
    }


def _severities(event: dict[str, Any] | None) -> set[str]:
    c = _classification(event)
    return {str(s) for s in (c.get("severities") or [])}


def _score(event: dict[str, Any] | None) -> int:
    c = _classification(event)
    try:
        return int(c.get("score", 100))
    except (TypeError, ValueError):
        return 100


def _fingerprint(rule_id: str, nifi_sevs: set[str], kafka_sevs: set[str]) -> str:
    raw = f"{rule_id}|{','.join(sorted(nifi_sevs))}|{','.join(sorted(kafka_sevs))}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _evidence(nifi: dict[str, Any] | None, kafka: dict[str, Any] | None) -> dict[str, Any]:
    n_health = (nifi or {}).get("health") if isinstance((nifi or {}).get("health"), dict) else {}
    k_health = (kafka or {}).get("health") if isinstance((kafka or {}).get("health"), dict) else {}
    return {
        "nifi": {
            "poll_id": (nifi or {}).get("poll_id"),
            "level": _classification(nifi).get("level"),
            "score": _score(nifi),
            "severities": sorted(_severities(nifi)),
            "queued": len(n_health.get("queued_connections") or []),
            "stopped": len(n_health.get("stopped_processors") or []),
        },
        "kafka": {
            "poll_id": (kafka or {}).get("poll_id"),
            "level": _classification(kafka).get("level"),
            "score": _score(kafka),
            "severities": sorted(_severities(kafka)),
            "missing_topics": [
                t.get("name") for t in (k_health.get("missing_topics") or []) if t.get("name")
            ],
            "lag_groups": [
                g.get("group_id")
                for g in (
                    list(k_health.get("lag_crit_groups") or [])
                    + list(k_health.get("lag_warn_groups") or [])
                )
                if g.get("group_id")
            ],
        },
    }


def correlate_signals(
    nifi_event: dict[str, Any] | None,
    kafka_event: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Match CORRELATION_RULES against a pair of monitor OutputEvents.

    Does not mutate NiFi or Kafka — observe-only.
    """
    nifi_sevs = _severities(nifi_event)
    kafka_sevs = _severities(kafka_event)
    evidence = _evidence(nifi_event, kafka_event)

    incidents: list[dict[str, Any]] = []
    matched_ids: list[str] = []
    specific_hit = False

    for rule in CORRELATION_RULES:
        if rule.get("fallback") and specific_hit:
            continue
        nifi_need = rule["nifi_any"]
        kafka_need = rule["kafka_any"]
        if not (nifi_sevs & nifi_need):
            continue
        if not (kafka_sevs & kafka_need):
            continue
        if not rule.get("fallback"):
            specific_hit = True
        rid = str(rule["id"])
        matched_ids.append(rid)
        fp = _fingerprint(rid, nifi_sevs & nifi_need, kafka_sevs & kafka_need)
        incidents.append(
            {
                "id": str(uuid.uuid4()),
                "fingerprint": fp,
                "rule": rid,
                "level": rule["level"],
                "title": rule["title"],
                "hint": rule.get("hint"),
                "nifi_matched": sorted(nifi_sevs & nifi_need),
                "kafka_matched": sorted(kafka_sevs & kafka_need),
                "evidence": evidence,
            }
        )

    combined_level = level_max(
        str(_classification(nifi_event).get("level") or "OK"),
        str(_classification(kafka_event).get("level") or "OK"),
        *[str(i["level"]) for i in incidents],
    )
    # Combined score: min of sides, minus 10 per correlated incident (floor 0)
    combined_score = max(
        0,
        min(_score(nifi_event), _score(kafka_event)) - 10 * len(incidents),
    )

    return {
        "agent": "workflow_signal_correlate",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "classification": {
            "healthy": not incidents
            and _classification(nifi_event).get("healthy", True)
            and _classification(kafka_event).get("healthy", True),
            "level": combined_level if incidents else level_max(
                str(_classification(nifi_event).get("level") or "OK"),
                str(_classification(kafka_event).get("level") or "OK"),
            ),
            "score": combined_score
            if incidents
            else min(_score(nifi_event), _score(kafka_event)),
            "summary": (
                "healthy"
                if not incidents
                and _classification(nifi_event).get("healthy")
                and _classification(kafka_event).get("healthy")
                else ", ".join(matched_ids) or "uncorrelated_degradation"
            ),
            "incident_count": len(incidents),
        },
        "incidents": incidents,
        "matched_rules": matched_ids,
        "signals": {
            "nifi": {
                "agent": (nifi_event or {}).get("agent"),
                "poll_id": (nifi_event or {}).get("poll_id"),
                "classification": _classification(nifi_event),
            },
            "kafka": {
                "agent": (kafka_event or {}).get("agent"),
                "poll_id": (kafka_event or {}).get("poll_id"),
                "classification": _classification(kafka_event),
            },
        },
        "evidence": evidence,
    }


def run_correlate_cycle(
    *,
    nifi_event: dict[str, Any] | None = None,
    kafka_event: dict[str, Any] | None = None,
    poll_live: bool = False,
) -> dict[str, Any]:
    """Correlate provided events, or optionally live-poll both monitors."""
    if poll_live:
        if nifi_event is None:
            from ratatoskr.nifi.client import NiFiClient
            from ratatoskr.nifi.policy import run_monitor_cycle

            client = NiFiClient()
            try:
                nifi_event = run_monitor_cycle(client, phase="monitor")
            except Exception as exc:  # noqa: BLE001
                nifi_event = {
                    "agent": "workflow_nifi_monitor",
                    "classification": {
                        "healthy": False,
                        "level": "HIGH",
                        "score": 0,
                        "severities": ["NIFI_UNREACHABLE"],
                        "summary": str(exc),
                    },
                    "health": {"severities": ["NIFI_UNREACHABLE"]},
                }
            finally:
                pass
        if kafka_event is None:
            from ratatoskr.kafka import KafkaClient, run_monitor_cycle as kafka_cycle

            client = KafkaClient()
            try:
                kafka_event = kafka_cycle(client, phase="monitor")
            except Exception as exc:  # noqa: BLE001
                kafka_event = {
                    "agent": "workflow_kafka_monitor",
                    "classification": {
                        "healthy": False,
                        "level": "HIGH",
                        "score": 0,
                        "severities": ["BROKER_UNREACHABLE"],
                        "summary": str(exc),
                    },
                    "health": {"severities": ["BROKER_UNREACHABLE"]},
                }
            finally:
                client.close()

    return correlate_signals(nifi_event, kafka_event)
