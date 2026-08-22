"""Phase 3 NiFi runbook demo helpers (offline + live orchestration)."""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from typing import Any, Iterator

RUNBOOK_BRIEF_TOPIC = "nifi.runbook.brief"

# Scenario id → fault + heal scope (names keep live POC focused on the injected story).
SCENARIOS: dict[str, dict[str, Any]] = {
    "stop-generate": {
        "fixture": "stop-generate",
        "fault": "--stop-generate",
        "target": "sample",
        "heal_phase": "safe",
        "title": "Stop GenerateFlowFile → safe start_processor",
        "watch_names": ["GenerateFlowFile", "UpdateAttribute", "LogAttribute"],
        "heal_names": ["GenerateFlowFile"],
    },
    "invalid-log": {
        "fixture": "invalid-log",
        "fault": "--invalid-log",
        "target": "sample",
        "heal_phase": "lab",
        "title": "INVALID LogAttribute → lab fix_processor_config",
        "watch_names": ["GenerateFlowFile", "UpdateAttribute", "LogAttribute"],
        "heal_names": ["LogAttribute"],
    },
    "queue-backlog": {
        "fixture": "queue-backlog",
        "fault": "--queue-backlog",
        "target": "sample",
        "heal_phase": "lab",
        "title": "Queue backlog → lab empty/start (gated)",
        "watch_names": ["GenerateFlowFile", "UpdateAttribute", "LogAttribute"],
        "heal_names": ["GenerateFlowFile", "UpdateAttribute", "LogAttribute"],
        "heal_name_regex": r"GenerateFlowFile|UpdateAttribute|LogAttribute",
    },
    "stop-consume": {
        "fixture": "stop-consume",
        "fault": "--stop-consume",
        "target": "kafka",
        "heal_phase": "safe",
        "title": "Stop ConsumeKafka → safe start_processor",
        "watch_names": ["ConsumeKafka", "UpdateAttribute", "LogAttribute"],
        "heal_names": ["ConsumeKafka"],
    },
}


def list_scenarios() -> list[dict[str, str]]:
    return [
        {
            "id": sid,
            "title": str(meta["title"]),
            "heal_phase": str(meta["heal_phase"]),
            "fixture": str(meta["fixture"]),
            "heal_names": ",".join(meta.get("heal_names") or []),
        }
        for sid, meta in SCENARIOS.items()
    ]


def names_to_regex(names: list[str]) -> str:
    """Exact-name alternation suitable for NIFI_WATCH_* / NIFI_HEAL_ALLOW_NAME_REGEX."""
    parts = [re.escape(n) for n in names if n]
    if not parts:
        return ""
    return r"^(?:%s)$" % "|".join(parts)


def scenario_watch_regex(scenario: dict[str, Any]) -> str | None:
    if scenario.get("watch_name_regex"):
        return str(scenario["watch_name_regex"])
    names = list(scenario.get("watch_names") or [])
    return names_to_regex(names) or None


def scenario_heal_regex(scenario: dict[str, Any]) -> str | None:
    if scenario.get("heal_name_regex"):
        return str(scenario["heal_name_regex"])
    names = list(scenario.get("heal_names") or [])
    return names_to_regex(names) or None


def filter_ops_by_scenario(ops: list[str], scenario: dict[str, Any]) -> list[str]:
    """Keep remediation refs whose component name matches scenario heal scope."""
    pattern = scenario_heal_regex(scenario)
    if not pattern:
        return list(ops)
    cre = re.compile(pattern)
    out: list[str] = []
    for ref in ops:
        name = ref.split(":", 1)[-1] if ":" in ref else ref
        if cre.search(name):
            out.append(ref)
    return out


@contextmanager
def scoped_nifi_env(
    *,
    watch_regex: str | None = None,
    heal_regex: str | None = None,
) -> Iterator[None]:
    """Temporarily set watch / heal allowlist env for a focused POC poll."""
    keys = ("NIFI_WATCH_NAME_REGEX", "NIFI_HEAL_ALLOW_NAME_REGEX")
    prev = {k: os.environ.get(k) for k in keys}
    try:
        if watch_regex:
            os.environ["NIFI_WATCH_NAME_REGEX"] = watch_regex
        if heal_regex:
            os.environ["NIFI_HEAL_ALLOW_NAME_REGEX"] = heal_regex
        yield
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def format_apply_status(applied: dict[str, Any]) -> str:
    """One-line heal outcome for demos."""
    audit = applied.get("audit") or {}
    dry = applied.get("dry_run")
    if dry is None:
        dry = audit.get("dry_run")
    actions = list(applied.get("heal_actions") or [])
    ok_t = sum(1 for a in actions if a.get("ok") is True)
    ok_f = sum(1 for a in actions if a.get("ok") is False)
    ok_n = sum(1 for a in actions if a.get("ok") is None)
    skipped = [a.get("skipped") for a in actions if a.get("skipped")]
    parts = [
        f"dry_run={bool(dry)}",
        f"phase={applied.get('phase') or audit.get('phase')}",
        f"actions={len(actions)}",
        f"executed_ok={applied.get('executed_ok', ok_t)}",
        f"failed={ok_f}",
        f"planned_only={ok_n}",
    ]
    if skipped:
        parts.append(f"skipped={skipped[:3]}")
    if applied.get("skipped"):
        parts.append(f"gate={applied['skipped']}")
    return "heal status: " + " ".join(parts)


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
        f"Approved heals use NIFI_HEAL_PHASE={heal_phase} via workflow_nifi_monitor "
        "(scoped to scenario heal_names).",
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
