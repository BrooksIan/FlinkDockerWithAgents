"""ReAct double-value logic (no flink_agents import — testable on host)."""

from __future__ import annotations

import json
import re
from typing import Any


def parse_int_from_text(text: str) -> int | None:
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def fallback_double(message: str, value_hint: int | None) -> dict[str, Any]:
    source = value_hint if value_hint is not None else parse_int_from_text(message)
    if source is None:
        source = 0
    doubled = source * 2
    return {
        "input": source,
        "doubled": doubled,
        "reasoning": "Deterministic fallback (LLM unavailable or not configured).",
        "mode": "fallback",
    }


def parse_llm_double_payload(content: str, *, value_hint: int | None = None) -> dict[str, Any]:
    """Parse JSON LLM content into input/doubled/reasoning."""
    import re

    text = content.strip()
    match = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
    if match:
        text = match.group(1).strip()
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("LLM response must be a JSON object")
    input_val = int(payload.get("input", value_hint or 0))
    doubled = int(payload.get("doubled", input_val * 2))
    return {
        "input": input_val,
        "doubled": doubled,
        "reasoning": str(payload.get("reasoning") or "LLM doubled the input value."),
    }


def llm_double(message: str, value_hint: int | None) -> dict[str, Any]:
    """Direct OpenAI call — host/testing fallback when Flink chat model is unavailable."""
    from apemosyne.designer.llm_client import chat_completion_json

    from examples.agents.react_double_value_prompt import DOUBLE_VALUE_SYSTEM, DOUBLE_VALUE_USER

    hint = "" if value_hint is None else str(value_hint)
    payload = chat_completion_json(
        system=DOUBLE_VALUE_SYSTEM,
        user=DOUBLE_VALUE_USER.format(message=json.dumps(message), value=hint),
    )
    result = parse_llm_double_payload(json.dumps(payload), value_hint=value_hint)
    result["mode"] = "llm"
    return result


def double_value_from_message(message: str, *, value_hint: int | None = None) -> dict[str, Any]:
    """Extract numeric input and return input/doubled (LLM when configured, else fallback)."""
    from apemosyne.designer.llm_settings import get_react_llm_settings

    settings = get_react_llm_settings()
    if settings.is_complete():
        try:
            return llm_double(message, value_hint)
        except Exception as exc:
            raise RuntimeError(f"LLM call failed: {exc}") from exc
    return fallback_double(message, value_hint)


def payload_from_input(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    message = getattr(raw, "message", None)
    value = getattr(raw, "value", None)
    payload: dict[str, Any] = {}
    if message is not None:
        payload["message"] = message
    if value is not None:
        payload["value"] = value
    if not payload:
        payload["message"] = str(raw)
    return payload


def message_from_payload(payload: dict[str, Any]) -> str:
    if "message" in payload:
        return str(payload["message"])
    if "value" in payload:
        return str(payload["value"])
    if "input" in payload:
        return str(payload["input"])
    if "doubled" in payload:
        return str(payload["doubled"])
    return json.dumps(payload, default=str)


def hint_value(payload: dict[str, Any]) -> int | None:
    raw = payload.get("value")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
