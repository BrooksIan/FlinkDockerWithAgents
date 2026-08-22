"""Phase 3 NiFi runbook demo helpers (offline + live orchestration)."""

from __future__ import annotations

import json
from typing import Any

RUNBOOK_BRIEF_TOPIC = "nifi.runbook.brief"

# Scenario id → (fixture_id, fault flag for nifi_fault_inject, heal phase, blurb)
SCENARIOS: dict[str, dict[str, Any]] = {
    "stop-generate": {
        "fixture": "stop-generate",
        "fault": "--stop-generate",
        "target": "sample",
        "heal_phase": "safe",
        "title": "Stop GenerateFlowFile → safe start_processor",
    },
    "invalid-log": {
        "fixture": "invalid-log",
        "fault": "--invalid-log",
        "target": "sample",
        "heal_phase": "lab",
        "title": "INVALID LogAttribute → lab fix_processor_config",
    },
    "queue-backlog": {
        "fixture": "queue-backlog",
        "fault": "--queue-backlog",
        "target": "sample",
        "heal_phase": "lab",
        "title": "Queue backlog → lab empty/start (gated)",
    },
    "stop-consume": {
        "fixture": "stop-consume",
        "fault": "--stop-consume",
        "target": "kafka",
        "heal_phase": "safe",
        "title": "Stop ConsumeKafka → safe start_processor",
    },
}


def list_scenarios() -> list[dict[str, str]]:
    return [
        {
            "id": sid,
            "title": str(meta["title"]),
            "heal_phase": str(meta["heal_phase"]),
            "fixture": str(meta["fixture"]),
        }
        for sid, meta in SCENARIOS.items()
    ]


def summarize_monitor(event: dict[str, Any]) -> dict[str, Any]:
    cls = event.get("classification") or {}
    health = event.get("health") or {}
    return {
        "phase": event.get("phase"),
        "poll_id": event.get("poll_id"),
        "healthy": cls.get("healthy"),
        "level": cls.get("level"),
        "score": cls.get("score"),
        "severities": cls.get("severities") or health.get("severities"),
        "stopped": [p.get("name") for p in (health.get("stopped_processors") or [])],
        "invalid": [p.get("name") for p in (health.get("invalid_processors") or [])],
        "disabled_services": [
            s.get("name") for s in (health.get("disabled_controller_services") or [])
        ],
        "heal_plan_ops": [
            f"{a.get('op')}:{a.get('name') or a.get('id')}"
            for a in (event.get("heal_plan") or [])
            if a.get("op")
        ],
    }


def summarize_runbook(event: dict[str, Any]) -> dict[str, Any]:
    rb = event.get("runbook") or {}
    rem = rb.get("remediation") or {}
    out = {
        "mode": rb.get("mode"),
        "headline": rb.get("headline"),
        "situation": rb.get("situation"),
        "safe_options": rem.get("safe_options") or [],
        "lab_options": rem.get("lab_options") or [],
        "do_not": rem.get("do_not") or [],
        "verify": rb.get("verify") or [],
        "heal_plan_source": (event.get("source") or {}).get("heal_plan_source"),
        "mutations": event.get("mutations"),
    }
    if event.get("hitl"):
        out["hitl"] = event["hitl"]
    return out


def operator_talking_points(runbook_event: dict[str, Any], *, heal_phase: str) -> list[str]:
    """POC narration after the runbook prints."""
    rb = runbook_event.get("runbook") or {}
    rem = rb.get("remediation") or {}
    mode = rb.get("mode") or "fallback"
    return [
        f"Inference mode={mode} — ReAct explained; it did not mutate NiFi "
        f"(mutations={runbook_event.get('mutations')}).",
        "Read diagnostic_steps, then remediation.safe_options / lab_options.",
        "Phase 4 HITL: approve before heal "
        f"(demo: --heal prompts, or --heal --approve / --heal --reject).",
        f"Approved heals use NIFI_HEAL_PHASE={heal_phase} via workflow_nifi_monitor.",
        f"Suggested safe ops: {rem.get('safe_options') or []}",
        f"Suggested lab ops: {rem.get('lab_options') or []}",
    ]


def run_offline_scenario(scenario_id: str) -> dict[str, Any]:
    """Fixture monitor → runbook (no live NiFi)."""
    if scenario_id not in SCENARIOS:
        raise KeyError(f"unknown scenario: {scenario_id}")
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import load_fixture

    meta = SCENARIOS[scenario_id]
    monitor = load_fixture(str(meta["fixture"]))
    runbook = build_runbook(monitor)
    return {
        "scenario": scenario_id,
        "meta": meta,
        "monitor": monitor,
        "runbook": runbook,
        "monitor_summary": summarize_monitor(monitor),
        "runbook_summary": summarize_runbook(runbook),
        "talking_points": operator_talking_points(
            runbook, heal_phase=str(meta["heal_phase"])
        ),
    }


def publish_runbook_brief(
    runbook_event: dict[str, Any],
    *,
    topic: str = RUNBOOK_BRIEF_TOPIC,
) -> dict[str, Any]:
    """Publish runbook OutputEvent to Studio Kafka (optional demo sink)."""
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    meta = producer.send(topic, runbook_event).get(timeout=15)
    producer.flush()
    producer.close()
    return {
        "ok": True,
        "topic": topic,
        "bootstrap": bootstrap,
        "partition": getattr(meta, "partition", None),
        "offset": getattr(meta, "offset", None),
    }
