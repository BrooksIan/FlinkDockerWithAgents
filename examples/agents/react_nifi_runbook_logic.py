"""NiFi runbook logic (no flink_agents — testable; explain-only, no mutations)."""

from __future__ import annotations

import json
from typing import Any

from ratatoskr.nifi.runbook.context import (
    allowed_remediation,
    constrain_remediation,
    enrich_monitor_context,
)
from ratatoskr.nifi.runbook.fallback import fallback_runbook
from ratatoskr.nifi.runbook.schema import (
    ALLOWED_CONFIDENCE,
    empty_runbook,
    is_valid_runbook,
    wrap_runbook_event,
)


def _normalize_monitor_event(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept monitor OutputEvent, Kafka wrapper, or {monitor|nifi: {...}}."""
    if isinstance(payload.get("value"), dict):
        payload = payload["value"]
    if payload.get("agent") == "workflow_nifi_monitor" or "classification" in payload:
        return payload
    for key in ("monitor", "nifi", "nifi_event", "event"):
        inner = payload.get(key)
        if isinstance(inner, dict):
            return _normalize_monitor_event(inner)
    return payload


def slim_monitor_event(event: dict[str, Any]) -> dict[str, Any]:
    """Phase 2 prompt payload (queues, bulletins, proposed heal refs, guidance)."""
    return enrich_monitor_context(event)


def parse_llm_runbook(
    content: str | dict[str, Any],
    *,
    monitor_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Coerce LLM JSON into a schema-valid runbook body (mode=llm)."""
    if isinstance(content, dict):
        payload = content
    else:
        text = content.strip()
        import re

        match = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
        if match:
            text = match.group(1).strip()
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("runbook response must be a JSON object")

    rb = empty_runbook(mode="llm")
    rb["headline"] = str(payload.get("headline") or "NiFi runbook")
    rb["situation"] = str(payload.get("situation") or "")

    causes_in = payload.get("likely_causes") or []
    causes: list[dict[str, Any]] = []
    if isinstance(causes_in, list):
        for c in causes_in:
            if isinstance(c, str):
                causes.append({"cause": c, "confidence": "medium", "evidence": []})
                continue
            if not isinstance(c, dict):
                continue
            conf = c.get("confidence") or "medium"
            if conf not in ALLOWED_CONFIDENCE:
                conf = "medium"
            ev = c.get("evidence") or []
            if isinstance(ev, str):
                ev = [ev]
            causes.append(
                {
                    "cause": str(c.get("cause") or c.get("hypothesis") or "Unspecified"),
                    "confidence": conf,
                    "evidence": [str(x) for x in ev],
                }
            )
    rb["likely_causes"] = causes

    steps_in = payload.get("diagnostic_steps") or []
    steps: list[dict[str, Any]] = []
    if isinstance(steps_in, list):
        for s in steps_in:
            if isinstance(s, str):
                steps.append({"step": s, "where": "UI", "expect": ""})
                continue
            if not isinstance(s, dict):
                continue
            steps.append(
                {
                    "step": str(s.get("step") or s.get("action") or ""),
                    "where": str(s.get("where") or "UI"),
                    "expect": str(s.get("expect") or ""),
                }
            )
    rb["diagnostic_steps"] = [s for s in steps if s["step"]]

    rem_in = payload.get("remediation") if isinstance(payload.get("remediation"), dict) else {}
    allowed = allowed_remediation(monitor_event or {})
    rb["remediation"] = constrain_remediation(
        rem_in,
        allowed_safe=allowed["safe_options"],
        allowed_lab=allowed["lab_options"],
    )

    verify = payload.get("verify") or []
    if isinstance(verify, str):
        verify = [verify]
    rb["verify"] = [str(v) for v in verify] if isinstance(verify, list) else []

    if not is_valid_runbook(rb):
        raise ValueError("LLM runbook failed schema validation")
    return rb


def llm_runbook(monitor_event: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.llm_client import chat_completion_json

    from examples.agents.react_nifi_runbook_prompt import RUNBOOK_SYSTEM, RUNBOOK_USER

    slim = slim_monitor_event(monitor_event)
    raw = chat_completion_json(
        system=RUNBOOK_SYSTEM,
        user=RUNBOOK_USER.format(payload=json.dumps(slim, default=str)),
    )
    body = parse_llm_runbook(raw, monitor_event=monitor_event)
    body["mode"] = "llm"
    allowed = slim.get("allowed_remediation") or {}
    return wrap_runbook_event(
        body,
        source={
            "poll_id": monitor_event.get("poll_id"),
            "phase": monitor_event.get("phase"),
            "severities": list((monitor_event.get("classification") or {}).get("severities") or []),
            "level": (monitor_event.get("classification") or {}).get("level"),
            "score": (monitor_event.get("classification") or {}).get("score"),
            "heal_plan_source": slim.get("heal_plan_source"),
            "heal_plan_ops": list(allowed.get("safe_options") or [])
            + list(allowed.get("lab_options") or []),
        },
    )


def build_runbook(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a runbook OutputEvent; LLM when configured, else deterministic fallback."""
    from ratatoskr.designer.llm_settings import get_react_llm_settings

    event = _normalize_monitor_event(payload)
    settings = get_react_llm_settings()
    if settings.is_complete():
        try:
            out = llm_runbook(event)
        except Exception as exc:  # noqa: BLE001 — demo-friendly fallback
            out = fallback_runbook(event)
            out["runbook"]["mode"] = "fallback"
            out["llm_error"] = str(exc)
    else:
        out = fallback_runbook(event)

    # Hard guarantee: explain-only
    out["agent"] = "react_nifi_runbook"
    out["mutations"] = []
    return out
