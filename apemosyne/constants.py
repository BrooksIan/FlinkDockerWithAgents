"""Shared constants for CLI defaults and environment loading."""

from __future__ import annotations

from typing import Final, FrozenSet, Tuple

DEFAULT_PROFILE: Final[str] = "minimal"
FULL_PROFILE: Final[str] = "full"
VALID_PROFILES: Final[FrozenSet[str]] = frozenset({DEFAULT_PROFILE, FULL_PROFILE})

VERIFY_PROFILE_HONEYPOT: Final[str] = "honeypot"
VALID_VERIFY_PROFILES: Final[FrozenSet[str]] = frozenset(
    {VERIFY_PROFILE_HONEYPOT, FULL_PROFILE, "cowrie"}
)

VERIFY_TIERS: Final[Tuple[str, ...]] = ("quick", "standard", "full", "nightly")
DEFAULT_STARTUP_MODE: Final[str] = "flink"

PROFILE_HELP: Final[str] = (
    "Stack profile: 'minimal' (Flink JM/TM only) or 'full' (Cowrie honeypot + Kafka + dashboard)"
)
STARTUP_MODE_HELP: Final[str] = (
    "Startup preset: flink (default), honeypot. Run: apemosyne modes"
)

GENERIC_DEMOS: Final[FrozenSet[str]] = frozenset({"datastream", "table", "workflow", "react"})

ENV_FILE_NAMES: Final[Tuple[str, ...]] = (".env", ".env.flink", ".env.cowrie", ".env.llm")
ENV_OVERLAY_FILES: Final[Tuple[str, ...]] = (".env.flink", ".env.cowrie", ".env.llm")
CONFIG_SECTIONS: Final[Tuple[str, ...]] = ("flink", "cowrie", "llm", "all")


def normalize_profile(profile: str) -> str:
    value = profile.strip().lower()
    if value in ("minimal", "min", "flink"):
        return DEFAULT_PROFILE
    if value in ("full", "honeypot", "cowrie"):
        return FULL_PROFILE
    return value


def normalize_verify_profile(profile: str | None) -> str | None:
    if profile is None:
        return None
    value = profile.strip().lower()
    if value in VALID_VERIFY_PROFILES:
        return VERIFY_PROFILE_HONEYPOT if value in ("full", "cowrie") else value
    return value


def default_demo_profile(demo_name: str) -> str:
    return DEFAULT_PROFILE if demo_name in GENERIC_DEMOS else FULL_PROFILE
