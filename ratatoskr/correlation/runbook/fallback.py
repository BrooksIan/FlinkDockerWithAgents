"""Deterministic cross-signal runbook from correlation OutputEvents."""

from __future__ import annotations

from typing import Any

from ratatoskr.correlation.heal import plan_cross_heals
from ratatoskr.correlation.runbook.context import (
    _normalize_correlation,
    allowed_cross_remediation,
)
from ratatoskr.nifi.runbook.schema import empty_runbook, wrap_runbook_event


def fallback_cross_runbook(correlation: dict[str, Any]) -> dict[str, Any]:
    """Explain-only cross-stack checklist; never mutates."""
    data = _normalize_correlation(correlation)
    classification = data.get("classification") or {}
    incidents = list(data.get("incidents") or [])
    signals = data.get("signals") or {}
    allowed = allowed_cross_remediation(data)
    plan = plan_cross_heals(data)

    rb = empty_runbook(mode="fallback")
    nifi_cls = ((signals.get("nifi") or {}).get("classification") or {})
    kafka_cls = ((signals.get("kafka") or {}).get("classification") or {})

    if not incidents:
        summary = str(classification.get("summary") or "")
        cross = classification.get("cross_signal")
        if classification.get("healthy") and nifi_cls.get("healthy", True) and kafka_cls.get(
            "healthy", True
        ):
            rb["headline"] = "No correlated incidents — stack looks healthy"
            rb["situation"] = (
                "NiFi and Kafka monitors do not form a cross-signal incident. "
                f"summary={summary or 'healthy'}."
            )
            rb["likely_causes"] = [
                {
                    "cause": "No active correlated failure pattern",
                    "confidence": "high",
                    "evidence": ["incidents:[]"],
                }
            ]
            rb["diagnostic_steps"] = [
                {
                    "step": "Continue workflow_signal_correlate polls",
                    "where": "CLI",
                    "expect": "cross_signal false and healthy",
                }
            ]
            rb["verify"] = ["classification.healthy true", "incidents empty"]
            rb["remediation"]["do_not"] = [
                "Do not set CROSS_HEAL_PHASE=lab without a matched incident",
            ]
        else:
            rb["headline"] = "Degradation without a matched cross-signal rule"
            rb["situation"] = (
                f"summary={summary}, cross_signal={cross}, "
                f"nifi_level={nifi_cls.get('level')}, kafka_level={kafka_cls.get('level')}."
            )
            rb["likely_causes"] = [
                {
                    "cause": "Independent single-side faults (no CORRELATION_RULES hit)",
                    "confidence": "medium",
                    "evidence": [summary] if summary else ["cross_signal:false"],
                }
            ]
            rb["diagnostic_steps"] = [
                {
                    "step": "Inspect NiFi severities / react_nifi_runbook for NiFi-only faults",
                    "where": "CLI",
                    "expect": "NiFi-side checklist",
                },
                {
                    "step": "Inspect Kafka severities via workflow_kafka_monitor",
                    "where": "CLI",
                    "expect": "Kafka-side checklist",
                },
            ]
            rb["verify"] = ["Re-run workflow_signal_correlate after side fixes"]
            rb["remediation"]["do_not"] = [
                "Do not run CROSS_HEAL_PHASE=lab without a matched rule playbook",
            ]
        return wrap_runbook_event(
            rb,
            agent="react_cross_runbook",
            source={
                "matched_rules": data.get("matched_rules") or [],
                "incident_count": 0,
                "summary": summary,
                "cross_signal": bool(cross),
            },
        )

    top = incidents[0]
    titles = [str(i.get("title") or i.get("rule")) for i in incidents]
    rb["headline"] = str(top.get("title") or "Cross-signal incident runbook")
    rb["situation"] = (
        f"{len(incidents)} correlated incident(s): {'; '.join(titles)}. "
        f"Combined level={classification.get('level')}, score={classification.get('score')}, "
        f"cross_signal={classification.get('cross_signal')}."
    )
    causes = []
    for i in incidents:
        causes.append(
            {
                "cause": str(i.get("hint") or i.get("title") or i.get("rule")),
                "confidence": "high" if i.get("hint") else "medium",
                "evidence": [
                    f"rule:{i.get('rule')}",
                    *[f"nifi:{s}" for s in (i.get("nifi_matched") or [])],
                    *[f"kafka:{s}" for s in (i.get("kafka_matched") or [])],
                ],
            }
        )
    rb["likely_causes"] = causes

    diag = [
        {
            "step": "Confirm evidence.nifi / evidence.kafka in the correlation event",
            "where": "CLI",
            "expect": "Matched severities align with canvas / consumer lag",
        },
        {
            "step": "Review CROSS_HEAL_PLAYBOOKS steps for matched rules (plan only)",
            "where": "CLI",
            "expect": f"{len(plan)} playbook step(s) proposed",
        },
    ]
    for step in plan[:4]:
        diag.append(
            {
                "step": (
                    f"Playbook {step.get('id')}: {step.get('side')} "
                    f"phase={step.get('phase')} ops={sorted(step.get('ops') or [])}"
                ),
                "where": "CLI",
                "expect": "Matches allowed_remediation refs",
            }
        )
    rb["diagnostic_steps"] = diag

    rb["remediation"] = {
        "safe_options": list(allowed["safe_options"]),
        "lab_options": list(allowed["lab_options"]),
        "do_not": [
            "Do not run CROSS_HEAL_PHASE=lab without reviewing diagnostic_steps",
            "Do not empty NiFi queues without CROSS_HEAL_ALLOW_EMPTY_QUEUE / NIFI_HEAL_ALLOW_EMPTY_QUEUE",
            "ReAct cross runbook must not mutate — use workflow_cross_stack_heal",
        ],
    }
    rb["verify"] = [
        "Re-run workflow_signal_correlate",
        "incidents empty or cross_signal false",
        "Side monitors healthy or severities cleared",
    ]

    return wrap_runbook_event(
        rb,
        agent="react_cross_runbook",
        source={
            "matched_rules": data.get("matched_rules") or [],
            "incident_count": len(incidents),
            "cross_signal": bool(classification.get("cross_signal")),
            "level": classification.get("level"),
            "score": classification.get("score"),
            "cross_heal_steps": [s.get("id") for s in plan],
            "heal_plan_ops": list(allowed["safe_options"]) + list(allowed["lab_options"]),
        },
    )
