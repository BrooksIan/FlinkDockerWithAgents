"""Default LLM settings for ReAct agents (designer + runtime)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apemosyne.designer.models import LlmSettings
from apemosyne.designer.store import REACT_LLM_KEY, DesignerStore, designer_db_path

_default_store: DesignerStore | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_designer_store(root: Path | None = None) -> DesignerStore:
    global _default_store
    if _default_store is None or root is not None:
        _default_store = DesignerStore(designer_db_path(root))
    return _default_store


def reset_designer_store_for_tests() -> None:
    global _default_store
    _default_store = None


def _env_llm_settings() -> LlmSettings:
    endpoint = (
        os.environ.get("APEMOSYNE_LLM_ENDPOINT_URL")
        or os.environ.get("CLOUDERA_AI_BASE_URL")
        or os.environ.get("OPENAI_BASE_URL")
        or ""
    ).strip().rstrip("/")
    model_id = (
        os.environ.get("APEMOSYNE_LLM_MODEL_ID")
        or os.environ.get("CLOUDERA_MODEL_ID")
        or os.environ.get("CLOUDERA_MODEL_NAME")
        or os.environ.get("OPENAI_MODEL")
        or ""
    ).strip()
    api_key = (
        os.environ.get("APEMOSYNE_LLM_API_KEY")
        or os.environ.get("CLOUDERA_JWT_TOKEN")
        or os.environ.get("CLOUDERA_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or ""
    ).strip()
    return LlmSettings(endpoint_url=endpoint, model_id=model_id, api_key=api_key)


def _stored_llm_settings(store: DesignerStore) -> LlmSettings | None:
    raw = store.get_json(REACT_LLM_KEY)
    if not raw:
        return None
    return LlmSettings(
        endpoint_url=str(raw.get("endpoint_url") or "").strip().rstrip("/"),
        model_id=str(raw.get("model_id") or "").strip(),
        api_key=str(raw.get("api_key") or "").strip(),
    )


def get_react_llm_settings(*, root: Path | None = None) -> LlmSettings:
    """Resolved settings: designer store overrides, then environment variables."""
    store = default_designer_store(root)
    stored = _stored_llm_settings(store)
    if stored is not None:
        env = _env_llm_settings()
        return LlmSettings(
            endpoint_url=stored.endpoint_url or env.endpoint_url,
            model_id=stored.model_id or env.model_id,
            api_key=stored.api_key or env.api_key,
        )
    return _env_llm_settings()


def _api_key_hint(api_key: str) -> str | None:
    if not api_key:
        return None
    if len(api_key) <= 4:
        return "****"
    return f"{'*' * (len(api_key) - 4)}{api_key[-4:]}"


def llm_settings_for_api(*, root: Path | None = None) -> dict[str, Any]:
    """Public API view — never returns the full API key."""
    store = default_designer_store(root)
    stored = _stored_llm_settings(store)
    env = _env_llm_settings()
    effective = get_react_llm_settings(root=root)
    source = "designer" if stored is not None else ("environment" if env.is_complete() else "unset")
    return {
        "scope": "react",
        "endpoint_url": effective.endpoint_url,
        "model_id": effective.model_id,
        "api_key_set": bool(effective.api_key),
        "api_key_hint": _api_key_hint(effective.api_key),
        "configured": effective.is_complete(),
        "source": source,
        "env_fallback": {
            "endpoint_url": env.endpoint_url or None,
            "model_id": env.model_id or None,
            "api_key_set": bool(env.api_key),
        },
    }


def update_react_llm_settings(
    *,
    endpoint_url: str,
    model_id: str,
    api_key: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Persist ReAct LLM defaults. Empty api_key keeps the existing stored key."""
    store = default_designer_store(root)
    current = _stored_llm_settings(store) or _env_llm_settings()
    resolved_key = (api_key if api_key is not None else current.api_key).strip()
    payload = {
        "endpoint_url": endpoint_url.strip().rstrip("/"),
        "model_id": model_id.strip(),
        "api_key": resolved_key,
    }
    store.set_json(REACT_LLM_KEY, payload, updated_at=_utc_now())
    return llm_settings_for_api(root=root)


def resolve_react_llm_settings_from_body(
    body: dict[str, Any] | None,
    *,
    root: Path | None = None,
) -> LlmSettings:
    """Build effective settings from a save/test payload (keeps stored key when omitted)."""
    current = get_react_llm_settings(root=root)
    raw = body or {}
    endpoint_url = str(raw.get("endpoint_url") or current.endpoint_url or "").strip().rstrip("/")
    model_id = str(raw.get("model_id") or current.model_id or "").strip()
    api_key_raw = raw.get("api_key")
    if api_key_raw is None:
        api_key = current.api_key
    else:
        api_key = str(api_key_raw).strip()
    return LlmSettings(endpoint_url=endpoint_url, model_id=model_id, api_key=api_key)


def test_react_llm_settings(
    body: dict[str, Any] | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Validate LLM endpoint, credentials, and ReAct double-value prompt path."""
    import json
    import time

    from apemosyne.designer.llm_client import LlmNotConfiguredError, chat_completion_json
    from examples.agents.react_double_value_logic import parse_llm_double_payload
    from examples.agents.react_double_value_prompt import DOUBLE_VALUE_SYSTEM, DOUBLE_VALUE_USER

    settings = resolve_react_llm_settings_from_body(body, root=root)
    if not settings.is_complete():
        missing = []
        if not settings.endpoint_url.strip():
            missing.append("endpoint URL")
        if not settings.model_id.strip():
            missing.append("model ID")
        if not settings.api_key.strip():
            missing.append("API key")
        raise LlmNotConfiguredError(
            "ReAct LLM not configured. Provide "
            + ", ".join(missing)
            + " in Designer or via environment variables."
        )

    started = time.perf_counter()
    payload = chat_completion_json(
        system=DOUBLE_VALUE_SYSTEM,
        user=DOUBLE_VALUE_USER.format(message=json.dumps("Please double input value 3"), value="3"),
        settings=settings,
    )
    result = parse_llm_double_payload(json.dumps(payload), value_hint=3)
    duration_ms = int((time.perf_counter() - started) * 1000)
    ok = int(result["input"]) == 3 and int(result["doubled"]) == 6
    return {
        "ok": ok,
        "duration_ms": duration_ms,
        "model_id": settings.model_id,
        "endpoint_url": settings.endpoint_url,
        "result": {
            "input": int(result["input"]),
            "doubled": int(result["doubled"]),
            "reasoning": str(result.get("reasoning") or ""),
        },
        "message": (
            "LLM connection verified — double-value prompt returned 3 → 6."
            if ok
            else f"LLM responded but doubling check failed: input={result['input']}, doubled={result['doubled']}"
        ),
    }
