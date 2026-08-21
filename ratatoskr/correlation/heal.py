"""Coordinated NiFi ↔ Kafka heal playbooks (lab / gated).

Runs after ``correlate_signals`` when ``CROSS_HEAL_PHASE=lab``. Each matched
rule maps to ordered side-specific heals that reuse NiFi/Kafka
``build_heal_plan`` / ``apply_heal_policy`` on a narrowed health snapshot.
"""

from __future__ import annotations

import os
from typing import Any

from ratatoskr.correlation.env import (
    cross_heal_allow_empty_queue,
    cross_heal_dry_run,
    cross_heal_phase,
    demo_consume_names,
    demo_kafka_topic,
)

# rule_id → ordered steps. phase is the side monitor heal phase.
CROSS_HEAL_PLAYBOOKS: dict[str, list[dict[str, Any]]] = {
    "kafka_topic_nifi_consumer": [
        {
            "id": "kafka_create_missing",
            "side": "kafka",
            "phase": "safe",
            "ops": frozenset({"create_topic"}),
            "topic_prefer": True,
        },
        {
            "id": "nifi_start_consumer_path",
            "side": "nifi",
            "phase": "safe",
            "ops": frozenset({"enable_controller_service", "start_processor"}),
            "name_prefer": True,
        },
    ],
    "nifi_invalid_kafka_missing": [
        {
            "id": "kafka_create_missing",
            "side": "kafka",
            "phase": "safe",
            "ops": frozenset({"create_topic"}),
            "topic_prefer": True,
        },
        {
            "id": "nifi_fix_and_start",
            "side": "nifi",
            "phase": "lab",
            "ops": frozenset(
                {
                    "enable_controller_service",
                    "fix_processor_config",
                    "start_processor",
                }
            ),
        },
    ],
    "pipeline_backpressure_lag": [
        {
            "id": "nifi_queue_relief",
            "side": "nifi",
            "phase": "lab",
            "ops": frozenset(
                {"stop_processor", "empty_connection_queue", "start_processor"}
            ),
            "require_empty_queue": True,
        },
    ],
    "nifi_stopped_kafka_lag": [
        {
            "id": "nifi_start_stopped",
            "side": "nifi",
            "phase": "safe",
            "ops": frozenset({"enable_controller_service", "start_processor"}),
            "name_prefer": True,
        },
    ],
}


def plan_cross_heals(correlated: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand matched incidents into ordered cross-stack heal steps."""
    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for incident in correlated.get("incidents") or []:
        rule = str(incident.get("rule") or "")
        playbook = CROSS_HEAL_PLAYBOOKS.get(rule)
        if not playbook:
            continue
        for raw in playbook:
            key = f"{rule}:{raw['id']}"
            if key in seen:
                continue
            seen.add(key)
            steps.append(
                {
                    **raw,
                    "rule": rule,
                    "incident_id": incident.get("id"),
                    "fingerprint": incident.get("fingerprint"),
                }
            )
    return steps


def _name_matches(name: str, prefer: frozenset[str]) -> bool:
    if not name or not prefer:
        return True
    return any(p in name for p in prefer)


def _narrow_kafka_health(
    health: dict[str, Any],
    *,
    ops: frozenset[str],
    topic_prefer: bool,
) -> dict[str, Any]:
    h = dict(health)
    demo = demo_kafka_topic()
    if "create_topic" in ops:
        missing = list(h.get("missing_topics") or [])
        if topic_prefer:
            prefer = [t for t in missing if t.get("name") == demo]
            h["missing_topics"] = prefer or missing
    else:
        h["missing_topics"] = []
    # Drop sources for ops we are not running this step
    if "increase_partitions" not in ops:
        h["undersized_topics"] = []
    if "recreate_topic" not in ops:
        h["oversized_topics"] = []
    if "reset_offsets" not in ops:
        h["lag_crit_groups"] = []
    if "delete_group" not in ops:
        h["empty_lagging_groups"] = []
    return h


def _narrow_nifi_health(
    health: dict[str, Any],
    *,
    ops: frozenset[str],
    name_prefer: bool,
) -> dict[str, Any]:
    h = dict(health)
    prefer = demo_consume_names() if name_prefer else frozenset()

    def _filter_ents(key: str) -> list[dict[str, Any]]:
        ents = list(h.get(key) or [])
        if not prefer:
            return ents
        matched = [e for e in ents if _name_matches(str(e.get("name") or ""), prefer)]
        return matched or ents

    if "enable_controller_service" in ops:
        h["disabled_controller_services"] = list(
            h.get("disabled_controller_services") or []
        )
    else:
        h["disabled_controller_services"] = []

    if "start_processor" in ops:
        h["stopped_processors"] = _filter_ents("stopped_processors")
    else:
        h["stopped_processors"] = []

    if "fix_processor_config" in ops or "terminate_processor" in ops:
        h["invalid_processors"] = list(h.get("invalid_processors") or [])
    else:
        h["invalid_processors"] = []

    if "stop_processor" in ops:
        h["backpressure_sources"] = list(h.get("backpressure_sources") or [])
    else:
        h["backpressure_sources"] = []

    if "restart_processor" in ops:
        pass  # derived from bulletins in build_heal_plan
    else:
        # Clear bulletins so restart rule cannot fire
        if "restart_processor" not in ops:
            h["bulletins"] = []

    if "empty_connection_queue" in ops:
        h["queued_connections"] = list(h.get("queued_connections") or [])
    else:
        h["queued_connections"] = []

    return h


def _annotate(
    actions: list[dict[str, Any]],
    *,
    step: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            **action,
            "cross_step": step.get("id"),
            "cross_rule": step.get("rule"),
            "cross_side": step.get("side"),
        }
        for action in actions
    ]


def apply_cross_heal_policy(
    correlated: dict[str, Any],
    *,
    nifi_client: Any | None = None,
    kafka_client: Any | None = None,
    nifi_pg_id: str = "root",
    dry_run: bool | None = None,
    phase: str | None = None,
) -> dict[str, Any]:
    """
    Execute playbooks for matched correlation rules.

    Returns ``heal_actions`` (annotated) and per-step side results.
    When phase is ``monitor``, returns the plan only (no mutations).
    """
    effective_phase = (phase or cross_heal_phase()).strip().lower()
    effective_dry = cross_heal_dry_run() if dry_run is None else bool(dry_run)
    steps = plan_cross_heals(correlated)

    result: dict[str, Any] = {
        "cross_heal_phase": effective_phase,
        "cross_heal_dry_run": effective_dry,
        "cross_heal_plan": [
            {
                "id": s["id"],
                "side": s["side"],
                "phase": s["phase"],
                "rule": s["rule"],
            }
            for s in steps
        ],
        "heal_actions": [],
        "step_results": [],
    }

    if effective_phase != "lab" or not steps:
        return result

    if cross_heal_allow_empty_queue():
        os.environ.setdefault("NIFI_HEAL_ALLOW_EMPTY_QUEUE", "1")

    all_actions: list[dict[str, Any]] = []
    own_kafka = kafka_client is None

    try:
        if any(s["side"] == "kafka" for s in steps) and kafka_client is None:
            from ratatoskr.kafka.client import KafkaClient

            kafka_client = KafkaClient()
        if any(s["side"] == "nifi" for s in steps) and nifi_client is None:
            from ratatoskr.nifi.client import NiFiClient

            nifi_client = NiFiClient()

        for step in steps:
            if step.get("require_empty_queue") and not (
                cross_heal_allow_empty_queue()
                or (os.environ.get("NIFI_HEAL_ALLOW_EMPTY_QUEUE") or "").strip()
                in ("1", "true", "yes", "on")
            ):
                result["step_results"].append(
                    {
                        "step": step["id"],
                        "skipped": True,
                        "reason": "empty_queue_not_allowed",
                    }
                )
                continue

            side = step["side"]
            side_phase = str(step["phase"])
            ops = step.get("ops") or frozenset()
            if not isinstance(ops, frozenset):
                ops = frozenset(ops)

            if side == "kafka":
                from ratatoskr.kafka.policy import (
                    apply_heal_policy,
                    classify_health,
                    reset_heal_cooldown,
                )

                reset_heal_cooldown()
                raw = kafka_client.get_cluster_health_status()
                narrowed = _narrow_kafka_health(
                    raw,
                    ops=ops,
                    topic_prefer=bool(step.get("topic_prefer")),
                )
                actions = apply_heal_policy(
                    kafka_client,
                    narrowed,
                    phase=side_phase,
                    dry_run=effective_dry,
                )
                clf = classify_health(narrowed)
            else:
                from ratatoskr.nifi.policy import (
                    apply_heal_policy,
                    classify_health,
                    reset_heal_cooldown,
                )

                reset_heal_cooldown()
                raw = nifi_client.get_flow_health_status(str(nifi_pg_id))
                narrowed = _narrow_nifi_health(
                    raw,
                    ops=ops,
                    name_prefer=bool(step.get("name_prefer")),
                )
                actions = apply_heal_policy(
                    nifi_client,
                    narrowed,
                    phase=side_phase,
                    dry_run=effective_dry,
                    process_group_id=str(nifi_pg_id),
                )
                clf = classify_health(narrowed)

            # Drop actions outside the playbook ops (defense in depth)
            actions = [a for a in actions if str(a.get("op") or "") in ops]
            annotated = _annotate(actions, step=step)
            all_actions.extend(annotated)
            result["step_results"].append(
                {
                    "step": step["id"],
                    "side": side,
                    "phase": side_phase,
                    "rule": step["rule"],
                    "skipped": False,
                    "heal_actions": annotated,
                    "side_healthy": clf.get("healthy"),
                    "side_severities": clf.get("severities"),
                }
            )

        result["heal_actions"] = all_actions
        return result
    finally:
        if own_kafka and kafka_client is not None:
            try:
                kafka_client.close()
            except Exception:  # noqa: BLE001
                pass
