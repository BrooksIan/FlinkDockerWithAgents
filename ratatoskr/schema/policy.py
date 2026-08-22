"""Schema gate classify / propose / apply / verify cycle."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ratatoskr.dataplane.flow import (
    DEFAULT_JSON_SCHEMA,
    LAB_JSON_SCHEMA,
    PG_NAME,
    ensure_dataplane_flow,
    ensure_dataplane_topics,
    find_dataplane_pg_id,
    get_schema_text,
    update_schema_text,
)
from ratatoskr.dataplane.topics import TOPIC_RAW, TOPIC_VALID, TOPIC_VIOLATIONS
from ratatoskr.schema.env import (
    ALLOWED_OPS,
    HEAL_LIKE_OPS,
    lab_schema_text,
    phase_at_least,
    schema_dry_run,
    schema_max_mutations,
    schema_phase,
    schema_verify,
)

_last_apply_mono: float = 0.0


def reset_schema_cooldown() -> None:
    global _last_apply_mono
    _last_apply_mono = 0.0


def sample_topic_messages(
    topic: str,
    *,
    max_messages: int = 20,
    timeout_ms: int = 2500,
) -> list[dict[str, Any]]:
    """Read a short sample from a topic (latest, dedicated ephemeral group)."""
    from kafka import KafkaConsumer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    group = f"ratatoskr-schema-sample-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: b.decode("utf-8", errors="replace"),
    )
    out: list[dict[str, Any]] = []
    try:
        for msg in consumer:
            out.append(
                {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "value": msg.value,
                }
            )
            if len(out) >= max_messages:
                break
    finally:
        consumer.close()
    return out


def topic_approx_count(topic: str) -> dict[str, Any]:
    """Approximate message count via beginning/end offsets."""
    from kafka import KafkaConsumer, TopicPartition

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_bootstrap_servers(),
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            return {"topic": topic, "count": 0, "missing": True}
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        total = sum(end[tp] - begin[tp] for tp in tps)
        return {
            "topic": topic,
            "count": int(total),
            "missing": False,
            "partitions": {
                str(tp.partition): {"begin": begin[tp], "end": end[tp]} for tp in tps
            },
        }
    finally:
        consumer.close()


def classify_schema_health(snapshot: dict[str, Any]) -> dict[str, Any]:
    severities: list[str] = []
    viol = int((snapshot.get("violations") or {}).get("count") or 0)
    raw = int((snapshot.get("raw") or {}).get("count") or 0)
    valid = int((snapshot.get("valid") or {}).get("count") or 0)
    if snapshot.get("flow_missing"):
        severities.append("DATAPLANE_FLOW_MISSING")
    if viol > 0:
        severities.append("SCHEMA_VIOLATIONS")
    if raw > 0 and valid == 0 and viol == 0:
        severities.append("SCHEMA_NO_THROUGHPUT")
    level = "OK"
    if "DATAPLANE_FLOW_MISSING" in severities:
        level = "HIGH"
    elif "SCHEMA_VIOLATIONS" in severities:
        level = "MEDIUM"
    elif severities:
        level = "LOW"
    return {
        "healthy": level == "OK",
        "level": level,
        "score": 100 if level == "OK" else (70 if level == "MEDIUM" else 40),
        "severities": severities,
        "summary": ", ".join(severities) if severities else "ok",
        "violation_count": viol,
        "valid_count": valid,
        "raw_count": raw,
    }


def poll_schema_snapshot(client: Optional[Any] = None) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient

    nifi = client or NiFiClient()
    pg_id = find_dataplane_pg_id(nifi)
    schema_text = get_schema_text(nifi, pg_id) if pg_id else None
    return {
        "process_group_id": pg_id,
        "process_group_name": PG_NAME if pg_id else None,
        "flow_missing": pg_id is None,
        "schema_text": schema_text,
        "raw": topic_approx_count(TOPIC_RAW),
        "valid": topic_approx_count(TOPIC_VALID),
        "violations": topic_approx_count(TOPIC_VIOLATIONS),
        "violation_sample": sample_topic_messages(TOPIC_VIOLATIONS, max_messages=5),
    }


def build_schema_plan(
    snapshot: dict[str, Any],
    *,
    phase: str | None = None,
    desired_schema: str | None = None,
) -> list[dict[str, Any]]:
    active = (phase or schema_phase()).strip().lower()
    plan: list[dict[str, Any]] = []
    if phase_at_least(active, "safe") and snapshot.get("flow_missing"):
        plan.append(
            {
                "op": "ensure_flow",
                "reason": "dataplane_pg_missing",
                "min_phase": "safe",
            }
        )
        plan.append(
            {
                "op": "ensure_topics",
                "reason": "ensure_dataplane_topics",
                "min_phase": "safe",
            }
        )
    elif phase_at_least(active, "safe"):
        plan.append(
            {
                "op": "ensure_topics",
                "reason": "idempotent_topic_ensure",
                "min_phase": "safe",
            }
        )

    if phase_at_least(active, "lab"):
        target = desired_schema or lab_schema_text() or LAB_JSON_SCHEMA
        current = snapshot.get("schema_text") or ""
        if target and target.strip() != (current or "").strip():
            plan.append(
                {
                    "op": "update_schema_text",
                    "reason": "lab_schema_swap",
                    "min_phase": "lab",
                    "schema_text": target,
                }
            )
    return plan


def _reject_heal_like(op: str) -> None:
    if op in HEAL_LIKE_OPS or op not in ALLOWED_OPS:
        raise RuntimeError(
            f"schema gate rejects op {op!r}; allowed={sorted(ALLOWED_OPS)}"
        )


def apply_schema_plan(
    client: Any,
    plan: list[dict[str, Any]],
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
) -> list[dict[str, Any]]:
    from ratatoskr.schema.env import schema_cooldown_sec

    global _last_apply_mono
    active = (phase or schema_phase()).strip().lower()
    is_dry = schema_dry_run() if dry_run is None else bool(dry_run)
    max_m = schema_max_mutations()
    cooldown = schema_cooldown_sec()
    actions: list[dict[str, Any]] = []

    if cooldown > 0 and _last_apply_mono and (time.monotonic() - _last_apply_mono) < cooldown:
        return [
            {
                "op": "cooldown",
                "ok": False,
                "skipped": True,
                "reason": f"cooldown {cooldown}s",
            }
        ]

    applied = 0
    for proposed in plan:
        op = str(proposed.get("op") or "")
        try:
            _reject_heal_like(op)
        except RuntimeError as exc:
            actions.append({**proposed, "ok": False, "error": str(exc)})
            continue
        min_phase = str(proposed.get("min_phase") or "safe")
        if not phase_at_least(active, min_phase):
            actions.append({**proposed, "ok": False, "skipped": True, "reason": "phase"})
            continue
        if max_m and applied >= max_m:
            actions.append({**proposed, "ok": False, "skipped": True, "reason": "max_mutations"})
            continue
        if is_dry:
            actions.append({**proposed, "ok": True, "dry_run": True})
            applied += 1
            continue
        try:
            if op == "ensure_topics":
                result = ensure_dataplane_topics()
                actions.append({**proposed, "ok": True, "result": result})
            elif op == "ensure_flow":
                result = ensure_dataplane_flow(client, ensure_topics=True)
                actions.append({**proposed, "ok": True, "result": result})
            elif op == "update_schema_text":
                pg = find_dataplane_pg_id(client)
                if not pg:
                    raise RuntimeError("dataplane process group missing")
                result = update_schema_text(
                    client, pg, str(proposed.get("schema_text") or DEFAULT_JSON_SCHEMA)
                )
                actions.append({**proposed, "ok": True, "result": result})
            else:
                actions.append({**proposed, "ok": False, "error": f"unknown op {op}"})
                continue
            applied += 1
            _last_apply_mono = time.monotonic()
        except Exception as exc:  # noqa: BLE001
            actions.append({**proposed, "ok": False, "error": str(exc)})
    return actions


def run_schema_gate_cycle(
    client: Optional[Any] = None,
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    desired_schema: str | None = None,
) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient

    nifi = client or NiFiClient()
    active = (phase or schema_phase()).strip().lower()
    snapshot = poll_schema_snapshot(nifi)
    classification = classify_schema_health(snapshot)
    plan = build_schema_plan(snapshot, phase=active, desired_schema=desired_schema)
    actions: list[dict[str, Any]] = []
    if active != "monitor":
        actions = apply_schema_plan(nifi, plan, phase=active, dry_run=dry_run)

    verify: dict[str, Any] | None = None
    if schema_verify() and actions and any(a.get("ok") and not a.get("dry_run") for a in actions):
        time.sleep(0.5)
        after = poll_schema_snapshot(nifi)
        verify = {
            "classification": classify_schema_health(after),
            "snapshot": {
                "violations": after.get("violations"),
                "valid": after.get("valid"),
                "schema_text_changed": after.get("schema_text") != snapshot.get("schema_text"),
            },
        }

    return {
        "agent": "workflow_schema_gate",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "phase": active,
        "classification": classification,
        "health": snapshot,
        "actions": actions,
        "plan": plan,
        "mutations": [
            a for a in actions if a.get("ok") and not a.get("dry_run") and not a.get("skipped")
        ],
        "verify": verify,
    }
