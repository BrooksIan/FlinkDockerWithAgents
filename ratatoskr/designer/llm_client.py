"""OpenAI-compatible LLM calls using designer ReAct defaults."""

from __future__ import annotations

import json
import re
from typing import Any

from ratatoskr.designer.llm_settings import get_react_llm_settings
from ratatoskr.designer.models import LlmSettings


class LlmNotConfiguredError(RuntimeError):
    """ReAct LLM settings are incomplete."""


def react_llm_settings() -> LlmSettings:
    settings = get_react_llm_settings()
    if not settings.is_complete():
        raise LlmNotConfiguredError(
            "ReAct LLM not configured. Set endpoint, model ID, and API key in Designer "
            "(/designer) or via RATATOSKR_LLM_* / CLOUDERA_* environment variables."
        )
    return settings


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    match = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text


def chat_completion_json(
    *,
    system: str,
    user: str,
    settings: LlmSettings | None = None,
) -> dict[str, Any]:
    """Call chat completions and parse a JSON object from the assistant message."""
    resolved = settings or react_llm_settings()
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("openai package is required for ReAct LLM agents") from exc

    client = OpenAI(
        base_url=resolved.endpoint_url.rstrip("/"),
        api_key=resolved.api_key,
    )
    response = client.chat.completions.create(
        model=resolved.model_id,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0,
    )
    content = (response.choices[0].message.content or "").strip()
    if not content:
        raise RuntimeError("LLM returned an empty response")
    payload = json.loads(_strip_json_fence(content))
    if not isinstance(payload, dict):
        raise RuntimeError("LLM response must be a JSON object")
    return payload
