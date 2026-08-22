"""Cross-signal runbook context from correlation OutputEvents."""

from __future__ import annotations

from typing import Any

from ratatoskr.correlation.heal import plan_cross_heals


def _normalize_correlation(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("value"), dict):
        payload = payload["value"]
    if payload.get("agent") == "workflow_signal_correlate" or "incidents" in payload:
        return payload
    if isinstance(payload.get("correlation"), dict):
        return _normalize_correlation(payload["correlation"])
    return payload


def slim_correlation(event: dict[str, Any]) -> dict[str, Any]:
    """Compact correlation payload for LLM prompts."""
    data = _normalize_correlation(event)
    classification = data.get("classification") or {}
    signals = data.get("signals") or {}
    incidents = []
    for i in data.get("incidents") or []:
        if not isinstance(i, dict):
            continue
        incidents.append(
            {
                k: i.get(k)
                for k in (
                    "rule",
                    "level",
                    "title",
                    "hint",
                    "nifi_matched",
                    "kafka_matched",
                    "schema_matched",
                    "route_matched",
                    "fingerprint",
                )
                if i.get(k) is not None
            }
        )

    def _side_brief(side: str) -> dict[str, Any]:
        sig = signals.get(side) or {}
        if not isinstance(sig, dict):
            return {}
        cls = sig.get("classification") or {}
        return {
            "healthy": cls.get("healthy"),
            "level": cls.get("level"),
            "score": cls.get("score"),
            "severities": cls.get("severities") or [],
            "summary": cls.get("summary"),
        }

    plan = plan_cross_heals(data)
    allowed = allowed_cross_remediation(data)
    return {
        "classification": {
            "healthy": classification.get("healthy"),
            "level": classification.get("level"),
            "score": classification.get("score"),
            "summary": classification.get("summary"),
            "cross_signal": classification.get("cross_signal"),
            "incident_count": classification.get("incident_count"),
            "matched_rules": data.get("matched_rules") or [],
        },
        "incidents": incidents,
        "signals": {
            "nifi": _side_brief("nifi"),
            "kafka": _side_brief("kafka"),
        },
        "cross_heal_plan": [
            {
                k: (
                    sorted(v) if isinstance(v, frozenset) else v
                )
                for k, v in step.items()
                if k in ("id", "side", "phase", "ops", "rule", "require_empty_queue")
            }
            for step in plan
        ],
        "allowed_remediation": allowed,
        "remediation_rules": [
            "Cite remediation ONLY from allowed_remediation (exact strings).",
            "Prefix side as nifi:op or kafka:op (optional :ComponentName).",
            "Prefer diagnostic_steps before remediation.",
            "You explain only — mutations via workflow_cross_stack_heal / side monitors.",
        ],
    }


def allowed_cross_remediation(event: dict[str, Any]) -> dict[str, list[str]]:
    """Build safe/lab option catalogs from CROSS_HEAL_PLAYBOOKS for matched incidents."""
    data = _normalize_correlation(event)
    safe: list[str] = []
    lab: list[str] = []
    seen: set[str] = set()
    for step in plan_cross_heals(data):
        side = str(step.get("side") or "nifi")
        phase = str(step.get("phase") or "lab").lower()
        ops = step.get("ops") or ()
        if isinstance(ops, (set, frozenset)):
            op_list = sorted(ops)
        elif isinstance(ops, (list, tuple)):
            op_list = list(ops)
        else:
            op_list = []
        for op in op_list:
            ref = f"{side}:{op}"
            if ref in seen:
                continue
            seen.add(ref)
            if phase == "safe":
                safe.append(ref)
            else:
                lab.append(ref)
    return {"safe_options": safe, "lab_options": lab}
