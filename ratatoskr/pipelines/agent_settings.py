"""Per-agent node configuration for Studio pipelines."""

from __future__ import annotations

from typing import Any

# Agents that fetch their own input (e.g. poll a REST API) and therefore do not
# require an upstream source node in a Studio pipeline.
SELF_SOURCING_AGENTS: frozenset[str] = frozenset(
    {
        "workflow_api_fetch",
        "readapi_reactthoughts_writekafka",
    }
)


def is_self_sourcing(agent: str | None) -> bool:
    return bool(agent) and agent in SELF_SOURCING_AGENTS

# Required node.config keys per agent manifest (Studio inspector validation).
AGENT_REQUIRED_SETTINGS: dict[str, tuple[str, ...]] = {
    "workflow_api_fetch": (),
    "readapi_reactthoughts_writekafka": ("endpoint_url", "kafka_topic"),
}

AGENT_OPTIONAL_SETTINGS: dict[str, tuple[str, ...]] = {
    "workflow_api_fetch": (
        "endpoint_url",
        "http_method",
        "api_key",
        "path",
        "path_suffix",
        "expand_records",
    ),
    "readapi_reactthoughts_writekafka": (
        "endpoint_url",
        "http_method",
        "api_key",
        "path",
        "path_suffix",
        "kafka_topic",
        "kafka_bootstrap",
    ),
}


def agents_with_settings() -> list[str]:
    keys = set(AGENT_REQUIRED_SETTINGS) | set(AGENT_OPTIONAL_SETTINGS)
    return sorted(keys)


def agent_settings_keys(agent: str) -> tuple[str, ...]:
    required = AGENT_REQUIRED_SETTINGS.get(agent, ())
    optional = AGENT_OPTIONAL_SETTINGS.get(agent, ())
    return required + optional


def _record_key(record: dict[str, Any]) -> str:
    return str(record.get("key") or record.get("k") or "1")


def _record_payload(record: Any) -> dict[str, Any]:
    """Extract the inner payload dict from a Flink pipeline record."""
    if not isinstance(record, dict):
        return {"value": record}
    if isinstance(record.get("value"), dict):
        return dict(record["value"])
    if isinstance(record.get("v"), dict):
        return dict(record["v"])
    if "value" in record or "v" in record:
        return {"value": record.get("value", record.get("v"))}
    return {k: v for k, v in record.items() if k not in ("key", "k")}


def apply_agent_node_config(
    records: list[dict[str, Any]],
    config: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Merge agent node settings into each input record.

    Records keep the Flink ``{"key", "value"}`` shape (flink_agents requires a
    ``value``/``v`` field). Settings are merged into the inner ``value`` payload
    so the agent can read them after unwrapping. Record fields win on conflict.
    """
    if not config:
        return records

    defaults = {k: v for k, v in config.items() if v is not None and str(v).strip() != ""}
    if not defaults:
        return records

    merged: list[dict[str, Any]] = []
    for record in records:
        payload = _record_payload(record)
        combined = {**defaults, **payload}
        key = _record_key(record) if isinstance(record, dict) else "1"
        merged.append({"key": key, "value": combined})
    return merged


def missing_required_settings(agent: str, config: dict[str, Any] | None) -> list[str]:
    required = AGENT_REQUIRED_SETTINGS.get(agent, ())
    if not required:
        return []
    raw = config or {}
    missing: list[str] = []
    for key in required:
        value = raw.get(key)
        if value is None or str(value).strip() == "":
            missing.append(key)
    return missing


__all__ = [
    "AGENT_OPTIONAL_SETTINGS",
    "AGENT_REQUIRED_SETTINGS",
    "SELF_SOURCING_AGENTS",
    "agent_settings_keys",
    "agents_with_settings",
    "apply_agent_node_config",
    "is_self_sourcing",
    "missing_required_settings",
]
