"""Correlate NiFi and Kafka monitor OutputEvents into incidents (deterministic)."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from ratatoskr.correlation.rules import (
    CM_CORRELATION_RULES,
    CORRELATION_RULES,
    DATAPLANE_CORRELATION_RULES,
    level_max,
)


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


def _evidence(
    nifi: dict[str, Any] | None,
    kafka: dict[str, Any] | None,
    cm: dict[str, Any] | None = None,
) -> dict[str, Any]:
    n_health = (nifi or {}).get("health") if isinstance((nifi or {}).get("health"), dict) else {}
    k_health = (kafka or {}).get("health") if isinstance((kafka or {}).get("health"), dict) else {}
    c_health = (cm or {}).get("health") if isinstance((cm or {}).get("health"), dict) else {}
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
        "cm": {
            "poll_id": (cm or {}).get("poll_id"),
            "level": _classification(cm).get("level"),
            "score": _score(cm),
            "severities": sorted(_severities(cm)),
            "cluster": c_health.get("cluster"),
            "critical_events": len(c_health.get("critical_events") or []),
            "metric_breaches": len(c_health.get("metric_breaches") or []),
            "suppressed_events": c_health.get("suppressed_events"),
        },
    }


def _solo_summary(
    nifi_event: dict[str, Any] | None,
    kafka_event: dict[str, Any] | None,
    cm_event: dict[str, Any] | None = None,
) -> str:
    """Human summary when no cross-signal rule matched."""
    n_c = _classification(nifi_event)
    k_c = _classification(kafka_event)
    c_c = _classification(cm_event)
    n_ok = bool(n_c.get("healthy"))
    k_ok = bool(k_c.get("healthy"))
    c_ok = bool(c_c.get("healthy"))
    if n_ok and k_ok and c_ok:
        return "healthy"
    parts: list[str] = []
    if not n_ok:
        sevs = ",".join(n_c.get("severities") or []) or "unhealthy"
        parts.append(f"nifi_only:{sevs}")
    if not k_ok:
        sevs = ",".join(k_c.get("severities") or []) or "unhealthy"
        parts.append(f"kafka_only:{sevs}")
    if not c_ok:
        sevs = ",".join(c_c.get("severities") or []) or "unhealthy"
        parts.append(f"cm_only:{sevs}")
    return "+".join(parts) if parts else "uncorrelated_degradation"


def _fingerprint_multi(rule_id: str, *sev_sets: set[str]) -> str:
    parts = [rule_id] + [",".join(sorted(s)) for s in sev_sets]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _match_route_sevs(route_sevs: set[str], rule: dict[str, Any]) -> set[str]:
    need = set(rule.get("route_any") or ())
    prefix = rule.get("route_prefix")
    matched = route_sevs & need
    if prefix:
        matched |= {s for s in route_sevs if str(s).startswith(str(prefix))}
    return matched


def correlate_signals(
    nifi_event: dict[str, Any] | None,
    kafka_event: dict[str, Any] | None,
    *,
    cm_event: dict[str, Any] | None = None,
    schema_event: dict[str, Any] | None = None,
    route_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Match correlation rules against monitor OutputEvents.

    Does not mutate NiFi, Kafka, or CM — observe-only.
    """
    nifi_sevs = _severities(nifi_event)
    kafka_sevs = _severities(kafka_event)
    cm_sevs = _severities(cm_event)
    schema_sevs = _severities(schema_event)
    route_sevs = _severities(route_event)
    evidence = _evidence(nifi_event, kafka_event, cm_event)
    evidence["schema"] = {
        "poll_id": (schema_event or {}).get("poll_id"),
        "level": _classification(schema_event).get("level"),
        "score": _score(schema_event),
        "severities": sorted(schema_sevs),
        "violation_count": (_classification(schema_event) or {}).get("violation_count")
        or ((schema_event or {}).get("classification") or {}).get("violation_count"),
    }
    evidence["route"] = {
        "poll_id": (route_event or {}).get("poll_id"),
        "level": _classification(route_event).get("level"),
        "score": _score(route_event),
        "severities": sorted(route_sevs),
    }

    incidents: list[dict[str, Any]] = []
    matched_ids: list[str] = []
    specific_hit = False
    cm_specific_hit = False

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

    for rule in CM_CORRELATION_RULES:
        if rule.get("fallback") and (specific_hit or cm_specific_hit):
            continue
        cm_need = set(rule.get("cm_any") or ())
        nifi_need = set(rule.get("nifi_any") or ())
        kafka_need = set(rule.get("kafka_any") or ())
        cm_matched = cm_sevs & cm_need
        if not cm_matched:
            continue
        nifi_matched = nifi_sevs & nifi_need if nifi_need else set()
        kafka_matched = kafka_sevs & kafka_need if kafka_need else set()
        if nifi_need and not nifi_matched:
            continue
        if kafka_need and not kafka_matched:
            continue
        if not nifi_matched and not kafka_matched:
            continue
        if not rule.get("fallback"):
            cm_specific_hit = True
        rid = str(rule["id"])
        matched_ids.append(rid)
        fp = _fingerprint_multi(rid, cm_matched, nifi_matched, kafka_matched)
        incidents.append(
            {
                "id": str(uuid.uuid4()),
                "fingerprint": fp,
                "rule": rid,
                "level": rule["level"],
                "title": rule["title"],
                "hint": rule.get("hint"),
                "cm_matched": sorted(cm_matched),
                "nifi_matched": sorted(nifi_matched),
                "kafka_matched": sorted(kafka_matched),
                "evidence": evidence,
            }
        )

    for rule in DATAPLANE_CORRELATION_RULES:
        schema_need = set(rule.get("schema_any") or ())
        kafka_need = set(rule.get("kafka_any") or ())
        route_matched = _match_route_sevs(route_sevs, rule) if (
            rule.get("route_any") or rule.get("route_prefix")
        ) else set()
        schema_matched = schema_sevs & schema_need if schema_need else set()

        if schema_need and not schema_matched:
            continue
        if kafka_need and not (kafka_sevs & kafka_need):
            continue
        if (rule.get("route_any") or rule.get("route_prefix")) and not route_matched:
            continue
        # Solo schema / solo route rules must have their side
        if not schema_need and not kafka_need and not route_matched:
            continue

        rid = str(rule["id"])
        matched_ids.append(rid)
        fp = _fingerprint_multi(rid, schema_matched, route_matched, kafka_sevs & kafka_need)
        incidents.append(
            {
                "id": str(uuid.uuid4()),
                "fingerprint": fp,
                "rule": rid,
                "level": rule["level"],
                "title": rule["title"],
                "hint": rule.get("hint"),
                "schema_matched": sorted(schema_matched),
                "route_matched": sorted(route_matched),
                "kafka_matched": sorted(kafka_sevs & kafka_need) if kafka_need else [],
                "evidence": evidence,
            }
        )

    combined_level = level_max(
        str(_classification(nifi_event).get("level") or "OK"),
        str(_classification(kafka_event).get("level") or "OK"),
        str(_classification(cm_event).get("level") or "OK"),
        str(_classification(schema_event).get("level") or "OK"),
        str(_classification(route_event).get("level") or "OK"),
        *[str(i["level"]) for i in incidents],
    )
    base_score = min(
        _score(nifi_event),
        _score(kafka_event),
        _score(cm_event) if cm_event else 100,
        _score(schema_event) if schema_event else 100,
        _score(route_event) if route_event else 100,
    )
    combined_score = max(0, base_score - 10 * len(incidents))
    summary = (
        ", ".join(matched_ids)
        if matched_ids
        else _solo_summary(nifi_event, kafka_event, cm_event)
    )

    healthy = (
        not incidents
        and _classification(nifi_event).get("healthy", True)
        and _classification(kafka_event).get("healthy", True)
        and _classification(cm_event).get("healthy", True)
        and _classification(schema_event).get("healthy", True)
        and _classification(route_event).get("healthy", True)
    )

    return {
        "agent": "workflow_signal_correlate",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "classification": {
            "healthy": healthy,
            "level": combined_level if incidents else level_max(
                str(_classification(nifi_event).get("level") or "OK"),
                str(_classification(kafka_event).get("level") or "OK"),
                str(_classification(cm_event).get("level") or "OK"),
                str(_classification(schema_event).get("level") or "OK"),
                str(_classification(route_event).get("level") or "OK"),
            ),
            "score": combined_score if incidents else base_score,
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
            "cm": {
                "agent": (cm_event or {}).get("agent"),
                "poll_id": (cm_event or {}).get("poll_id"),
                "classification": _classification(cm_event),
            },
            "schema": {
                "agent": (schema_event or {}).get("agent"),
                "poll_id": (schema_event or {}).get("poll_id"),
                "classification": _classification(schema_event),
            },
            "route": {
                "agent": (route_event or {}).get("agent"),
                "poll_id": (route_event or {}).get("poll_id"),
                "classification": _classification(route_event),
            },
        },
        "evidence": evidence,
    }


def _should_poll_cm() -> bool:
    import os

    from ratatoskr.cm.env import cm_cluster, knox_token

    if knox_token() or cm_cluster():
        return True
    return bool((os.environ.get("CM_API_BASE") or "").strip())


def run_correlate_cycle(
    *,
    nifi_event: dict[str, Any] | None = None,
    kafka_event: dict[str, Any] | None = None,
    cm_event: dict[str, Any] | None = None,
    schema_event: dict[str, Any] | None = None,
    route_event: dict[str, Any] | None = None,
    poll_live: bool = False,
    poll_dataplane: bool = False,
    poll_cm: bool | None = None,
) -> dict[str, Any]:
    """Correlate provided events, or optionally live-poll monitors (+ data-plane)."""
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

        effective_poll_cm = poll_cm if poll_cm is not None else _should_poll_cm()
        if cm_event is None and effective_poll_cm:
            from ratatoskr.cm import CMClient, run_monitor_cycle as cm_cycle

            client = CMClient()
            try:
                cm_event = cm_cycle(client)
            except Exception as exc:  # noqa: BLE001
                cm_event = {
                    "agent": "workflow_cm_monitor",
                    "classification": {
                        "healthy": False,
                        "level": "HIGH",
                        "score": 0,
                        "severities": ["CM_UNREACHABLE"],
                        "summary": str(exc),
                    },
                    "health": {"severities": ["CM_UNREACHABLE"]},
                }

    if poll_live or poll_dataplane:
        if schema_event is None:
            try:
                from ratatoskr.schema import run_schema_gate_cycle

                schema_event = run_schema_gate_cycle(phase="monitor")
            except Exception as exc:  # noqa: BLE001
                schema_event = {
                    "agent": "workflow_schema_gate",
                    "classification": {
                        "healthy": False,
                        "level": "MEDIUM",
                        "score": 50,
                        "severities": ["SCHEMA_NO_THROUGHPUT"],
                        "summary": str(exc),
                    },
                }
        if route_event is None:
            try:
                from ratatoskr.routing import run_route_enrich_cycle

                route_event = run_route_enrich_cycle(phase="monitor")
            except Exception as exc:  # noqa: BLE001
                route_event = {
                    "agent": "workflow_route_enrich",
                    "classification": {
                        "healthy": False,
                        "level": "MEDIUM",
                        "score": 50,
                        "severities": ["DATAPLANE_FLOW_MISSING"],
                        "summary": str(exc),
                    },
                }

    return correlate_signals(
        nifi_event,
        kafka_event,
        cm_event=cm_event,
        schema_event=schema_event,
        route_event=route_event,
    )


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
