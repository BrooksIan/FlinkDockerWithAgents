"""API configuration from environment."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiSettings:
    host: str = "127.0.0.1"
    port: int = 8090
    api_key: str | None = None
    flink_rest_host: str = "localhost"
    flink_rest_port: int = 8081
    default_profile: str = "minimal"
    log_json: bool = False

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def flink_rest_url(self) -> str:
        return f"http://{self.flink_rest_host}:{self.flink_rest_port}"


def load_settings() -> ApiSettings:
    key = os.environ.get("APEMOSYNE_API_KEY", "").strip() or None
    return ApiSettings(
        host=os.environ.get("APEMOSYNE_API_HOST", "127.0.0.1").strip(),
        port=int(os.environ.get("APEMOSYNE_API_PORT", "8090").strip()),
        api_key=key,
        flink_rest_host=os.environ.get("FLINK_REST_ADDRESS", "localhost").strip(),
        flink_rest_port=int(os.environ.get("FLINK_REST_PORT", "8081").strip()),
        default_profile=os.environ.get("APEMOSYNE_PROFILE", "minimal").strip(),
        log_json=os.environ.get("APEMOSYNE_LOG_JSON", "0").strip().lower()
        in ("1", "true", "yes"),
    )
