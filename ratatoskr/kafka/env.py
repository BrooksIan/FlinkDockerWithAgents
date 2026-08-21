"""Kafka monitor / heal env helpers."""

from __future__ import annotations

import os
import re
from typing import Pattern

HEAL_PHASES = frozenset({"monitor", "safe", "lab"})
CATALOG_MODES = frozenset({"studio", "full"})

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


def heal_phase() -> str:
    raw = (os.environ.get("KAFKA_HEAL_PHASE") or "monitor").strip().lower()
    return raw if raw in HEAL_PHASES else "monitor"


def catalog_mode() -> str:
    """
    Topic catalog scope for TOPIC_MISSING.

    - studio (default): Studio Kafka init topics only (no cowrie.*)
    - full: all known pipeline + static topics (honeypot + studio)
    """
    raw = (os.environ.get("KAFKA_CATALOG") or "studio").strip().lower()
    if raw in ("honeypot", "all"):
        return "full"
    return raw if raw in CATALOG_MODES else "studio"


def phase_at_least(active: str, minimum: str) -> bool:
    return _PHASE_RANK.get(active, 0) >= _PHASE_RANK.get(minimum, 0)


def heal_dry_run() -> bool:
    return _truthy("KAFKA_HEAL_DRY_RUN")


def heal_verify() -> bool:
    raw = (os.environ.get("KAFKA_HEAL_VERIFY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def heal_cooldown_sec() -> float:
    return max(0.0, _float_env("KAFKA_HEAL_COOLDOWN_SEC", 0.0))


def heal_max_mutations() -> int:
    """0 = unlimited."""
    return max(0, _int_env("KAFKA_HEAL_MAX_MUTATIONS", 0))


def lag_warn_threshold() -> int:
    return max(1, _int_env("KAFKA_LAG_WARN", 1000))


def lag_crit_threshold() -> int:
    warn = lag_warn_threshold()
    return max(warn, _int_env("KAFKA_LAG_CRIT", 10000))


def probe_slow_ms() -> float:
    return max(1.0, _float_env("KAFKA_PROBE_SLOW_MS", 3000.0))


def default_partitions() -> int:
    return max(1, _int_env("KAFKA_TOPIC_PARTITIONS", 1))


def default_replication_factor() -> int:
    return max(1, _int_env("KAFKA_TOPIC_RF", 1))


def flag_unexpected_topics() -> bool:
    return _truthy("KAFKA_FLAG_UNEXPECTED", "0")


def watch_prefixes() -> tuple[str, ...]:
    """Comma-separated topic/group prefixes; empty = no prefix filter."""
    raw = (os.environ.get("KAFKA_WATCH_PREFIXES") or "").strip()
    if not raw:
        return ()
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def heal_allow_topics() -> frozenset[str] | None:
    raw = (os.environ.get("KAFKA_HEAL_ALLOW_TOPICS") or "").strip()
    if not raw:
        return None
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def heal_allow_groups() -> frozenset[str] | None:
    """Required for lab offset-reset / delete-group; None = deny all group mutates."""
    raw = (os.environ.get("KAFKA_HEAL_ALLOW_GROUPS") or "").strip()
    if not raw:
        return None
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def heal_allow_name_regex() -> Pattern[str] | None:
    raw = (os.environ.get("KAFKA_HEAL_ALLOW_NAME_REGEX") or "").strip()
    if not raw:
        return None
    try:
        return re.compile(raw)
    except re.error:
        return None


def heal_allow_group_prefixes() -> tuple[str, ...]:
    """Comma-separated group id prefixes allowed for lab group mutates (in addition to exact allowlist)."""
    raw = (os.environ.get("KAFKA_HEAL_ALLOW_GROUP_PREFIXES") or "").strip()
    if not raw:
        return ()
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def offset_reset_strategy() -> str:
    """Lab reset_offsets target: latest (skip backlog, default) or earliest (replay)."""
    raw = (os.environ.get("KAFKA_HEAL_OFFSET_STRATEGY") or "latest").strip().lower()
    return raw if raw in ("latest", "earliest") else "latest"


def allow_increase_partitions() -> bool:
    """Lab increase_partitions toward catalog (default on). Set KAFKA_HEAL_ALLOW_INCREASE_PARTITIONS=0."""
    raw = (os.environ.get("KAFKA_HEAL_ALLOW_INCREASE_PARTITIONS") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def allow_recreate_topic() -> bool:
    """Lab recreate_topic for oversized catalog topics (default off — destructive)."""
    return _truthy("KAFKA_HEAL_ALLOW_RECREATE")


def matches_watch(name: str) -> bool:
    prefixes = watch_prefixes()
    if not prefixes:
        return True
    return any(name.startswith(p) for p in prefixes)
