"""Cross-signal runbook logic (no flink_agents — testable; explain-only)."""

from __future__ import annotations

import json
from typing import Any

from ratatoskr.correlation.runbook import (
    allowed_cross_remediation,
    fallback_cross_runbook,
    slim_correlation,
)
from ratatoskr.correlation.runbook.context import _normalize_correlation
from ratatoskr.nifi.runbook.context import constrain_remediation
from ratatoskr.nifi.runbook.schema import (
    ALLOWED_CONFIDENCE,
    empty_runbook,
    is_valid_runbook,
    wrap_runbook_event,
)


def parse_cross_runbook(
    content: str | dict[str, Any],
    *,
    correlation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(content, dict):
        payload = content
    else:
        import re

        text = content.strip()
        match = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
        if match:
            text = match.group(1).strip()
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("runbook response must be a JSON object")

    rb = empty_runbook(mode="llm")
    rb["headline"] = str(payload.get("headline") or "Cross-signal runbook")
    rb["situation"] = str(payload.get("situation") or "")

    causes: list[dict[str, Any]] = []
    for c in payload.get("likely_causes") or []:
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
                "cause": str(c.get("cause") or "Unspecified"),
                "confidence": conf,
                "evidence": [str(x) for x in ev],
            }
        )
    rb["likely_causes"] = causes

    steps: list[dict[str, Any]] = []
    for s in payload.get("diagnostic_steps") or []:
        if isinstance(s, str):
            steps.append({"step": s, "where": "CLI", "expect": ""})
            continue
        if not isinstance(s, dict):
            continue
        steps.append(
            {
                "step": str(s.get("step") or ""),
                "where": str(s.get("where") or "CLI"),
                "expect": str(s.get("expect") or ""),
            }
        )
    rb["diagnostic_steps"] = [s for s in steps if s["step"]]

    rem_in = payload.get("remediation") if isinstance(payload.get("remediation"), dict) else {}
    allowed = allowed_cross_remediation(correlation or {})
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
        raise ValueError("LLM cross runbook failed schema validation")
    return rb


def llm_cross_runbook(correlation: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.llm_client import chat_completion_json

    from examples.agents.react_cross_runbook_prompt import (
        CROSS_RUNBOOK_SYSTEM,
        CROSS_RUNBOOK_USER,
    )

    slim = slim_correlation(correlation)
    raw = chat_completion_json(
        system=CROSS_RUNBOOK_SYSTEM,
        user=CROSS_RUNBOOK_USER.format(payload=json.dumps(slim, default=str)),
    )
    body = parse_cross_runbook(raw, correlation=correlation)
    body["mode"] = "llm"
    allowed = slim.get("allowed_remediation") or {}
    return wrap_runbook_event(
        body,
        agent="react_cross_runbook",
        source={
            "matched_rules": (correlation.get("matched_rules") or []),
            "incident_count": len(correlation.get("incidents") or []),
            "cross_signal": (correlation.get("classification") or {}).get("cross_signal"),
            "heal_plan_ops": list(allowed.get("safe_options") or [])
            + list(allowed.get("lab_options") or []),
        },
    )


def build_cross_runbook(payload: dict[str, Any]) -> dict[str, Any]:
    """Build cross-signal runbook; LLM when configured, else fallback."""
    from ratatoskr.designer.llm_settings import get_react_llm_settings

    data = _normalize_correlation(payload)
    settings = get_react_llm_settings()
    if settings.is_complete():
        try:
            out = llm_cross_runbook(data)
        except Exception as exc:  # noqa: BLE001
            out = fallback_cross_runbook(data)
            out["runbook"]["mode"] = "fallback"
            out["llm_error"] = str(exc)
    else:
        out = fallback_cross_runbook(data)

    out["agent"] = "react_cross_runbook"
    out["mutations"] = []
    return out
