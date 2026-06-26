"""Flink REST host port defaults (minimal vs honeypot stacks)."""

from __future__ import annotations

import os

from ratatoskr.constants import DEFAULT_PROFILE, FULL_PROFILE, normalize_profile

MINIMAL_FLINK_REST_PORT = 8082
FULL_FLINK_REST_PORT = 8081


def default_flink_rest_port(profile: str | None = None) -> int:
    """Host port for Flink REST/Web UI. Override with ``FLINK_REST_PORT``."""
    env = os.environ.get("FLINK_REST_PORT", "").strip()
    if env:
        return int(env)
    active = normalize_profile(profile or os.environ.get("RATATOSKR_PROFILE", DEFAULT_PROFILE))
    return FULL_FLINK_REST_PORT if active == FULL_PROFILE else MINIMAL_FLINK_REST_PORT


def studio_flink_rest_port() -> int:
    """Host REST port for the minimal Studio Flink stack (not honeypot)."""
    return MINIMAL_FLINK_REST_PORT


def flink_web_ui_url(profile: str | None = None, *, host: str = "localhost") -> str:
    return f"http://{host}:{default_flink_rest_port(profile)}"
