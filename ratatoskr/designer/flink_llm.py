"""Flink Agents chat model resources backed by Designer ReAct LLM settings."""

from __future__ import annotations

from typing import Any

from ratatoskr.designer.llm_client import LlmNotConfiguredError
from ratatoskr.designer.llm_settings import get_react_llm_settings
from ratatoskr.designer.models import LlmSettings


def require_react_llm_settings() -> LlmSettings:
    settings = get_react_llm_settings()
    if not settings.is_complete():
        raise LlmNotConfiguredError(
            "ReAct LLM not configured. Set endpoint, model ID, and API key in Designer "
            "Settings or via RATATOSKR_LLM_* / CLOUDERA_* / OPENAI_* environment variables."
        )
    return settings


def react_llm_connection_descriptor() -> Any:
    """OpenAI-compatible connection from Designer LLM settings."""
    from flink_agents.api.resource import ResourceDescriptor, ResourceName

    settings = require_react_llm_settings()
    return ResourceDescriptor(
        clazz=ResourceName.ChatModel.OPENAI_COMPLETIONS_CONNECTION,
        api_key=settings.api_key,
        api_base_url=settings.endpoint_url.rstrip("/"),
        max_retries=3,
        timeout=120.0,
    )


def react_skills_chat_model_descriptor(
    *,
    connection: str,
    prompt: str,
    skills: list[str],
    allowed_commands: list[str],
    temperature: float = 0.0,
) -> Any:
    """Chat model setup with skills enabled, using the configured model ID."""
    from flink_agents.api.resource import ResourceDescriptor, ResourceName

    settings = require_react_llm_settings()
    return ResourceDescriptor(
        clazz=ResourceName.ChatModel.OPENAI_COMPLETIONS_SETUP,
        connection=connection,
        model=settings.model_id,
        prompt=prompt,
        skills=skills,
        allowed_commands=allowed_commands,
        temperature=temperature,
    )
