"""Agent designer data models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LlmSettings:
    """OpenAI-compatible LLM connection settings for ReAct agents."""

    endpoint_url: str
    model_id: str
    api_key: str = ""

    def is_complete(self) -> bool:
        return bool(self.endpoint_url.strip() and self.model_id.strip() and self.api_key.strip())


@dataclass(frozen=True)
class ApiFetchSettings:
    """HTTP API settings for workflow_api_fetch and similar agents."""

    endpoint_url: str
    http_method: str = "GET"
    api_key: str = ""
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer"
    timeout_seconds: int = 15

    def is_complete(self) -> bool:
        return bool(self.endpoint_url.strip())

    def normalized_method(self) -> str:
        method = self.http_method.strip().upper()
        return method if method in ("GET", "POST") else "GET"
