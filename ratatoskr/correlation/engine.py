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


def _solo_summary(
    nifi_event: dict[str, Any] | None,
    kafka_event: dict[str, Any] | None,
) -> str:
    """Human summary when no cross-signal rule matched."""
    n_c = _classification(nifi_event)
    k_c = _classification(kafka_event)
    n_ok = bool(n_c.get("healthy"))
    k_ok = bool(k_c.get("healthy"))
    if n_ok and k_ok:
        return "healthy"
    parts: list[str] = []
    if not n_ok:
        sevs = ",".join(n_c.get("severities") or []) or "unhealthy"
        parts.append(f"nifi_only:{sevs}")
    if not k_ok:
        sevs = ",".join(k_c.get("severities") or []) or "unhealthy"
        parts.append(f"kafka_only:{sevs}")
    return "+".join(parts) if parts else "uncorrelated_degradation"


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
    combined_score = max(
        0,
        min(_score(nifi_event), _score(kafka_event)) - 10 * len(incidents),
    )
    summary = (
        ", ".join(matched_ids)
        if matched_ids
        else _solo_summary(nifi_event, kafka_event)
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
            "summary": summary,
            "incident_count": len(incidents),
            "cross_signal": bool(incidents),
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


def run_cross_stack_cycle(
    *,
    nifi_event: dict[str, Any] | None = None,
    kafka_event: dict[str, Any] | None = None,
    poll_live: bool = False,
    phase: str | None = None,
    dry_run: bool | None = None,
    nifi_pg_id: str = "root",
    nifi_client: Any | None = None,
    kafka_client: Any | None = None,
) -> dict[str, Any]:
    """
    Correlate NiFi + Kafka, then optionally run cross-stack heal playbooks.

    ``phase``: ``monitor`` (default) = observe + plan; ``lab`` = execute playbooks.
    """
    from ratatoskr.correlation.env import cross_heal_phase
    from ratatoskr.correlation.heal import apply_cross_heal_policy, plan_cross_heals

    correlated = run_correlate_cycle(
        nifi_event=nifi_event,
        kafka_event=kafka_event,
        poll_live=poll_live,
    )
    effective = (phase or cross_heal_phase()).strip().lower()
    heal = apply_cross_heal_policy(
        correlated,
        nifi_client=nifi_client,
        kafka_client=kafka_client,
        nifi_pg_id=nifi_pg_id,
        dry_run=dry_run,
        phase=effective,
    )
    # Always surface the planned steps even in monitor
    if not heal.get("cross_heal_plan"):
        heal["cross_heal_plan"] = [
            {
                "id": s["id"],
                "side": s["side"],
                "phase": s["phase"],
                "rule": s["rule"],
            }
            for s in plan_cross_heals(correlated)
        ]

    out = dict(correlated)
    out["agent"] = "workflow_cross_stack_heal"
    out["cross_heal_phase"] = heal.get("cross_heal_phase", effective)
    out["cross_heal_dry_run"] = heal.get("cross_heal_dry_run")
    out["cross_heal_plan"] = heal.get("cross_heal_plan") or []
    out["heal_actions"] = heal.get("heal_actions") or []
    out["step_results"] = heal.get("step_results") or []
    return out
