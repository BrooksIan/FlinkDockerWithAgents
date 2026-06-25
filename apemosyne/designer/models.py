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
