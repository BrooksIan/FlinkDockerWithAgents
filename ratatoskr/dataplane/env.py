"""Shared data-plane phase / dry-run / allowlist gates."""

from __future__ import annotations

import os
import re
from typing import Pattern

DATAPLANE_PHASES = frozenset({"monitor", "safe", "lab"})

_PHASE_RANK = {"monitor": 0, "safe": 1, "lab": 2}


def _truthy(name: str, default: str = "") -> bool:
    return (os.environ.get(name) or default).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def dataplane_phase(env_name: str = "DATAPLANE_PHASE") -> str:
    raw = (os.environ.get(env_name) or "monitor").strip().lower()
    return raw if raw in DATAPLANE_PHASES else "monitor"


def phase_at_least(active: str, minimum: str) -> bool:
    return _PHASE_RANK.get(active, 0) >= _PHASE_RANK.get(minimum, 0)


def dataplane_dry_run(env_name: str = "DATAPLANE_DRY_RUN") -> bool:
    return _truthy(env_name)


def dataplane_verify(env_name: str = "DATAPLANE_VERIFY") -> bool:
    raw = (os.environ.get(env_name) or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def dataplane_max_mutations(env_name: str = "DATAPLANE_MAX_MUTATIONS") -> int:
    return max(0, _int_env(env_name, 0))


def dataplane_cooldown_sec(env_name: str = "DATAPLANE_COOLDOWN_SEC") -> float:
    return max(0.0, _float_env(env_name, 0.0))


def dataplane_allow_name_regex(
    env_name: str = "DATAPLANE_ALLOW_NAME_REGEX",
) -> Pattern[str] | None:
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return None
    try:
        return re.compile(raw)
    except re.error:
        return None


def default_kafka_bootstrap_for_nifi() -> str:
    return (
        os.environ.get("NIFI_KAFKA_BOOTSTRAP")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or "kafka:9092"
    ).strip()
