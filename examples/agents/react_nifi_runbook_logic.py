"""NiFi runbook logic (no flink_agents — testable; explain-only, no mutations)."""

from __future__ import annotations

import json
from typing import Any

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
    """Keep prompt small — drop bulky nested lists beyond names/ids."""
    classification = event.get("classification") or {}
    health = event.get("health") or {}

    def _brief(items: list[Any] | None, *, limit: int = 8) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            out.append(
                {
                    k: it.get(k)
                    for k in ("id", "name", "state", "validationStatus", "queuedCount", "runStatus")
                    if it.get(k) is not None
                }
            )
            if len(out) >= limit:
                break
        return out

    heal_plan = []
    for a in event.get("heal_plan") or []:
        if not isinstance(a, dict):
            continue
        heal_plan.append(
            {k: a.get(k) for k in ("op", "id", "name", "reason") if a.get(k) is not None}
        )

    return {
        "poll_id": event.get("poll_id"),
        "phase": event.get("phase"),
        "classification": {
            "healthy": classification.get("healthy"),
            "level": classification.get("level"),
            "score": classification.get("score"),
            "severities": classification.get("severities") or [],
            "summary": classification.get("summary"),
        },
        "health": {
            "severities": health.get("severities") or [],
            "stopped_processors": _brief(health.get("stopped_processors")),
            "invalid_processors": _brief(health.get("invalid_processors")),
            "disabled_controller_services": _brief(health.get("disabled_controller_services")),
            "queued_connections": _brief(health.get("queued_connections")),
            "bulletins": _brief(health.get("bulletins"), limit=5),
            "probe": health.get("probe"),
        },
        "heal_plan": heal_plan,
    }


def parse_llm_runbook(content: str | dict[str, Any]) -> dict[str, Any]:
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
    def _str_list(key: str) -> list[str]:
        raw = rem_in.get(key) or []
        if isinstance(raw, str):
            return [raw]
        if not isinstance(raw, list):
            return []
        return [str(x) for x in raw]

    rb["remediation"] = {
        "safe_options": _str_list("safe_options"),
        "lab_options": _str_list("lab_options"),
        "do_not": _str_list("do_not")
        or [
            "Do not empty queues without NIFI_HEAL_ALLOW_EMPTY_QUEUE=1",
            "ReAct runbook must not mutate NiFi — use workflow_nifi_monitor heal phases",
        ],
    }

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
    body = parse_llm_runbook(raw)
    body["mode"] = "llm"
    return wrap_runbook_event(
        body,
        source={
            "poll_id": monitor_event.get("poll_id"),
            "phase": monitor_event.get("phase"),
            "severities": list((monitor_event.get("classification") or {}).get("severities") or []),
            "level": (monitor_event.get("classification") or {}).get("level"),
            "score": (monitor_event.get("classification") or {}).get("score"),
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
