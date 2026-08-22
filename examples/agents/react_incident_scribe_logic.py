"""Incident scribe logic (no flink_agents — testable; explain-only, no mutations)."""

from __future__ import annotations

import json
from typing import Any


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept correlation OutputEvent or a thin wrapper."""
    if "incidents" in payload or payload.get("agent") == "workflow_signal_correlate":
        return payload
    if isinstance(payload.get("correlation"), dict):
        return payload["correlation"]
    return payload


def fallback_scribe(payload: dict[str, Any]) -> dict[str, Any]:
    data = _normalize_payload(payload)
    incidents = list(data.get("incidents") or [])
    classification = data.get("classification") or {}
    signals = data.get("signals") or {}

    if not incidents:
        nifi_ok = ((signals.get("nifi") or {}).get("classification") or {}).get(
            "healthy", True
        )
        kafka_ok = ((signals.get("kafka") or {}).get("classification") or {}).get(
            "healthy", True
        )
        if nifi_ok and kafka_ok and classification.get("healthy", True):
            return {
                "headline": "No correlated incidents — stack looks healthy",
                "summary": (
                    "NiFi and Kafka monitor signals do not form a cross-signal incident. "
                    "Individual sides report healthy or uncorrelated noise only."
                ),
                "likely_cause": "No active correlated failure pattern.",
                "suggested_next_steps": [
                    "Continue periodic monitor polls",
                    "Review single-side severities if score < 100",
                ],
                "mode": "fallback",
            }
        return {
            "headline": "Degradation without a matched correlation rule",
            "summary": (
                f"Combined level={classification.get('level')}, "
                f"score={classification.get('score')}. "
                "Sides are unhealthy but no specific NiFi↔Kafka rule matched."
            ),
            "likely_cause": "Independent faults on one or both sides.",
            "suggested_next_steps": [
                "Inspect NiFi classification.severities",
                "Inspect Kafka classification.severities",
                "Re-run workflow_signal_correlate after fixes",
            ],
            "mode": "fallback",
        }

    top = incidents[0]
    titles = [str(i.get("title") or i.get("rule")) for i in incidents]
    hints = [str(i.get("hint")) for i in incidents if i.get("hint")]
    return {
        "headline": str(top.get("title") or "Correlated incident"),
        "summary": (
            f"{len(incidents)} correlated incident(s): {'; '.join(titles)}. "
            f"Combined level={classification.get('level')}, score={classification.get('score')}."
        ),
        "likely_cause": hints[0] if hints else "See matched rule hints in the correlation payload.",
        "suggested_next_steps": [
            "Confirm evidence.nifi / evidence.kafka / evidence.schema in the correlation event",
            "For schema/route drift: propose on dataplane.propose then ack before apply",
            "Heal infra with NIFI_HEAL_PHASE / KAFKA_HEAL_PHASE=safe only after dry-run",
            "Re-poll monitors and re-correlate to verify resolution",
        ],
        "mode": "fallback",
        "incidents_referenced": [i.get("fingerprint") for i in incidents],
    }


def parse_scribe_payload(content: str | dict[str, Any]) -> dict[str, Any]:
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
        raise ValueError("scribe response must be a JSON object")
    steps = payload.get("suggested_next_steps") or []
    if isinstance(steps, str):
        steps = [steps]
    return {
        "headline": str(payload.get("headline") or "Incident brief"),
        "summary": str(payload.get("summary") or ""),
        "likely_cause": str(payload.get("likely_cause") or ""),
        "suggested_next_steps": [str(s) for s in steps],
    }


def llm_scribe(payload: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.designer.llm_client import chat_completion_json

    from examples.agents.react_incident_scribe_prompt import SCRIBE_SYSTEM, SCRIBE_USER

    data = _normalize_payload(payload)
    # Keep prompt small — drop bulky health lists
    slim = {
        "classification": data.get("classification"),
        "matched_rules": data.get("matched_rules"),
        "incidents": [
            {
                "rule": i.get("rule"),
                "level": i.get("level"),
                "title": i.get("title"),
                "hint": i.get("hint"),
                "nifi_matched": i.get("nifi_matched"),
                "kafka_matched": i.get("kafka_matched"),
            }
            for i in (data.get("incidents") or [])
        ],
        "signals": data.get("signals"),
        "evidence": data.get("evidence"),
    }
    raw = chat_completion_json(
        system=SCRIBE_SYSTEM,
        user=SCRIBE_USER.format(payload=json.dumps(slim, default=str)),
    )
    result = parse_scribe_payload(raw)
    result["mode"] = "llm"
    return result


def scribe_incident(payload: dict[str, Any]) -> dict[str, Any]:
    """Write an operator brief; LLM when configured, else deterministic fallback."""
    from ratatoskr.designer.llm_settings import get_react_llm_settings

    data = _normalize_payload(payload)
    settings = get_react_llm_settings()
    if settings.is_complete():
        try:
            out = llm_scribe(data)
        except Exception as exc:  # noqa: BLE001 — demo-friendly fallback
            out = fallback_scribe(data)
            out["mode"] = "fallback"
            out["llm_error"] = str(exc)
    else:
        out = fallback_scribe(data)

    return {
        "agent": "react_incident_scribe",
        "mutations": [],  # hard guarantee: explain-only
        "brief": out,
        "source": {
            "matched_rules": data.get("matched_rules") or [],
            "incident_count": len(data.get("incidents") or []),
            "classification": data.get("classification"),
        },
    }
