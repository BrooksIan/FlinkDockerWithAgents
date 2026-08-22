"""Deterministic NiFi runbook when Cloudera Inference is unavailable (Phase 0)."""

from __future__ import annotations

from typing import Any

from ratatoskr.nifi.runbook.schema import empty_runbook, wrap_runbook_event


def _severities(event: dict[str, Any]) -> list[str]:
    c = event.get("classification") or {}
    sevs = list(c.get("severities") or [])
    if sevs:
        return [str(s) for s in sevs]
    health = event.get("health") or {}
    return [str(s) for s in (health.get("severities") or [])]


def _names(items: list[dict[str, Any]] | None, *, limit: int = 5) -> list[str]:
    out: list[str] = []
    for it in items or []:
        name = it.get("name") or it.get("id")
        if name:
            out.append(str(name))
        if len(out) >= limit:
            break
    return out


def _heal_refs(plan: list[dict[str, Any]] | None, *, phase_hint: str) -> list[str]:
    """Format heal_plan entries as remediation option strings (ids from plan only)."""
    refs: list[str] = []
    for a in plan or []:
        op = a.get("op")
        name = a.get("name") or a.get("id")
        if not op or not name:
            continue
        refs.append(f"{op}:{name}")
    return refs


def _split_heal_by_phase(plan: list[dict[str, Any]] | None) -> tuple[list[str], list[str]]:
    """Best-effort split: start/enable → safe; terminate/stop/empty/fix → lab."""
    safe_ops = {"start_processor", "enable_controller_service"}
    lab_ops = {
        "terminate_processor",
        "stop_processor",
        "empty_connection_queue",
        "fix_processor_config",
        "restart_processor",
    }
    safe: list[str] = []
    lab: list[str] = []
    for a in plan or []:
        op = str(a.get("op") or "")
        name = a.get("name") or a.get("id")
        if not op or not name:
            continue
        ref = f"{op}:{name}"
        if op in safe_ops:
            safe.append(ref)
        elif op in lab_ops:
            lab.append(ref)
        else:
            lab.append(ref)
    return safe, lab


def fallback_runbook(monitor_event: dict[str, Any]) -> dict[str, Any]:
    """
    Build a valid runbook from a ``workflow_nifi_monitor`` OutputEvent.

    Never mutates NiFi — explanation + checklist only.
    """
    event = monitor_event
    if isinstance(monitor_event.get("value"), dict):
        event = monitor_event["value"]

    classification = event.get("classification") or {}
    health = event.get("health") or {}
    heal_plan = list(event.get("heal_plan") or [])
    sevs = _severities(event)
    level = classification.get("level") or ("OK" if not sevs else "MEDIUM")
    score = classification.get("score")
    healthy = bool(classification.get("healthy")) and not sevs

    stopped = _names(health.get("stopped_processors"))
    invalid = _names(health.get("invalid_processors"))
    queued = _names(health.get("queued_connections"))
    disabled = _names(health.get("disabled_controller_services"))
    safe_opts, lab_opts = _split_heal_by_phase(heal_plan)

    rb = empty_runbook(mode="fallback")

    if healthy:
        rb["headline"] = "NiFi flow looks healthy"
        rb["situation"] = (
            f"Monitor classification level={level}, score={score}. "
            "No severities reported — no remediation required."
        )
        rb["likely_causes"] = [
            {
                "cause": "No active NiFi fault pattern",
                "confidence": "high",
                "evidence": ["severities:[]"],
            }
        ]
        rb["diagnostic_steps"] = [
            {
                "step": "Continue periodic workflow_nifi_monitor polls",
                "where": "CLI",
                "expect": "classification.healthy true",
            }
        ]
        rb["verify"] = ["classification.healthy is true", "severities empty"]
        rb["remediation"]["do_not"] = [
            "Do not set NIFI_HEAL_PHASE=lab without a confirmed fault",
        ]
        return wrap_runbook_event(
            rb,
            source={
                "poll_id": event.get("poll_id"),
                "severities": sevs,
                "level": level,
                "score": score,
            },
        )

    # Headline from primary severity
    primary = sevs[0] if sevs else "UNHEALTHY"
    rb["headline"] = f"NiFi runbook: {primary}" + (f" (+{len(sevs) - 1} more)" if len(sevs) > 1 else "")
    rb["situation"] = (
        f"Monitor level={level}, score={score}, severities={', '.join(sevs)}. "
        f"Stopped={stopped or '[]'}; invalid={invalid or '[]'}; "
        f"queued={queued or '[]'}; disabled_services={disabled or '[]'}."
    )

    causes: list[dict[str, Any]] = []
    if "STOPPED" in sevs:
        causes.append(
            {
                "cause": "One or more processors are STOPPED",
                "confidence": "high",
                "evidence": [f"stopped:{n}" for n in stopped] or ["severity:STOPPED"],
            }
        )
    if "INVALID" in sevs:
        causes.append(
            {
                "cause": "Processor validationStatus INVALID (config / relationships)",
                "confidence": "high",
                "evidence": [f"invalid:{n}" for n in invalid] or ["severity:INVALID"],
            }
        )
    if any(s.startswith("BACKPRESSURE") for s in sevs):
        causes.append(
            {
                "cause": "Connection queue backlog (downstream slow or stopped)",
                "confidence": "high",
                "evidence": [f"queued:{n}" for n in queued] or ["severity:BACKPRESSURE"],
            }
        )
    if "DISABLED_SERVICE" in sevs:
        causes.append(
            {
                "cause": "Required controller service is DISABLED",
                "confidence": "high",
                "evidence": [f"service:{n}" for n in disabled] or ["severity:DISABLED_SERVICE"],
            }
        )
    if "BULLETIN_ERROR" in sevs:
        causes.append(
            {
                "cause": "Active ERROR/WARNING bulletins on problem components",
                "confidence": "medium",
                "evidence": ["severity:BULLETIN_ERROR"],
            }
        )
    if "NIFI_UNREACHABLE" in sevs:
        causes.append(
            {
                "cause": "NiFi API unreachable (stack / credentials / network)",
                "confidence": "high",
                "evidence": ["severity:NIFI_UNREACHABLE"],
            }
        )
    if not causes:
        causes.append(
            {
                "cause": f"Unclassified degradation: {', '.join(sevs)}",
                "confidence": "low",
                "evidence": sevs,
            }
        )
    rb["likely_causes"] = causes

    diag: list[dict[str, Any]] = [
        {
            "step": "Open NiFi UI canvas and inspect red/yellow processors",
            "where": "UI",
            "expect": "Match names in monitor health lists",
        },
        {
            "step": "Re-run workflow_nifi_monitor with NIFI_HEAL_PHASE=monitor",
            "where": "CLI",
            "expect": "Same severities until remediated",
        },
    ]
    if invalid:
        diag.append(
            {
                "step": f"Inspect processor config / relationships for {', '.join(invalid)}",
                "where": "UI",
                "expect": "validationStatus VALID after fix",
            }
        )
    if queued:
        diag.append(
            {
                "step": f"Inspect queue depths on {', '.join(queued)}",
                "where": "UI",
                "expect": "Identify stopped downstream or slow consumer",
            }
        )
    if "NIFI_UNREACHABLE" in sevs:
        diag.insert(
            0,
            {
                "step": "Verify ratatoskr up --profile nifi and NIFI_USERNAME/PASSWORD",
                "where": "CLI",
                "expect": "https://localhost:8443/nifi reachable",
            },
        )
    rb["diagnostic_steps"] = diag

    # Remediation: prefer heal_plan refs; else severity templates without inventing ids
    if not safe_opts and "STOPPED" in sevs and stopped:
        safe_opts = [f"start_processor:{n}" for n in stopped]
    if not safe_opts and "DISABLED_SERVICE" in sevs and disabled:
        safe_opts = [f"enable_controller_service:{n}" for n in disabled]
    if not lab_opts and "INVALID" in sevs and invalid:
        lab_opts = [f"fix_processor_config:{n}" for n in invalid] + [
            f"terminate_processor:{n}" for n in invalid
        ]
    if not lab_opts and any(s.startswith("BACKPRESSURE") for s in sevs) and queued:
        lab_opts = [f"empty_connection_queue:{n}" for n in queued]

    rb["remediation"] = {
        "safe_options": safe_opts,
        "lab_options": lab_opts,
        "do_not": [
            "Do not empty queues without NIFI_HEAL_ALLOW_EMPTY_QUEUE=1",
            "Do not run lab heals without reviewing diagnostic_steps",
            "ReAct runbook must not mutate NiFi — use workflow_nifi_monitor heal phases",
        ],
    }

    rb["verify"] = [
        "Re-poll workflow_nifi_monitor (phase=monitor)",
        "classification.healthy true or severities cleared",
        f"score improved from {score}" if score is not None else "score improved",
    ]

    return wrap_runbook_event(
        rb,
        source={
            "poll_id": event.get("poll_id"),
            "phase": event.get("phase"),
            "severities": sevs,
            "level": level,
            "score": score,
            "heal_plan_ops": _heal_refs(heal_plan, phase_hint="any"),
        },
    )
