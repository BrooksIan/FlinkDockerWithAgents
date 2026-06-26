"""Local (non-Flink) dynamic session window simulation for Studio preview runs."""

from __future__ import annotations

from typing import Any

from ratatoskr.pipelines.window_config import parse_window_config
from ratatoskr.pipelines.window_policies import (
    resolve_gap_policy,
    resolve_summarize_policy,
    session_summary_to_agent_record,
)


def _event_timestamp(event: dict[str, Any], *, fallback: int) -> int:
    for name in ("timestamp", "time", "ts"):
        value = event.get(name)
        if value is None and isinstance(event.get("value"), dict):
            value = event["value"].get(name)
        if value is not None:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
    return fallback


def apply_dynamic_session_windows(
    events: list[dict[str, Any]],
    *,
    key_field: str,
    gap_policy: str,
    gap_ms: int,
) -> list[dict[str, Any]]:
    """Simulate processing-time dynamic session windows on an ordered event list."""
    if not events:
        return []

    gap_fn = resolve_gap_policy(gap_policy, gap_ms=gap_ms)
    summarize_fn = resolve_summarize_policy(gap_policy, key_field=key_field)

    summaries: list[dict[str, Any]] = []
    buckets: dict[str, list[dict[str, Any]]] = {}
    last_ts: dict[str, int] = {}

    for index, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        key = str(event.get(key_field) or event.get("key") or "unknown")
        ts = _event_timestamp(event, fallback=index)
        gap = gap_fn(event)

        if key in buckets and key in last_ts and ts - last_ts[key] > gap:
            summaries.append(summarize_fn(key, buckets[key]))
            buckets[key] = []

        buckets.setdefault(key, []).append(event)
        last_ts[key] = ts

    for key in sorted(buckets):
        batch = buckets[key]
        if batch:
            summaries.append(summarize_fn(key, batch))

    return summaries


def apply_window_node(
    events: list[dict[str, Any]],
    config: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Apply a Studio window node config to source records."""
    parsed = parse_window_config(config)
    if parsed.window_type != "dynamic_session":
        raise ValueError(f"Unsupported window_type {parsed.window_type!r}")
    return apply_dynamic_session_windows(
        events,
        key_field=parsed.key_field,
        gap_policy=parsed.gap_policy,
        gap_ms=parsed.gap_ms,
    )


__all__ = ["apply_dynamic_session_windows", "apply_window_node", "session_summary_to_agent_record"]
