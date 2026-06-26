"""Domain-neutral dynamic session window policies for Studio and cluster codegen."""

from __future__ import annotations

from typing import Any, Callable

GAP_POLICY_DEFAULT = "default"
GAP_POLICY_SESSION_DETECT = "session_detect"

KNOWN_GAP_POLICIES = frozenset({GAP_POLICY_DEFAULT, GAP_POLICY_SESSION_DETECT})

DEFAULT_GAP_MS = 1_000


def default_gap_ms(_event: dict[str, Any], *, gap_ms: int = DEFAULT_GAP_MS) -> int:
    return gap_ms


def default_summarize(
    key: str,
    events: list[dict[str, Any]],
    *,
    key_field: str = "key",
) -> dict[str, Any]:
    """Generic session batch — no domain-specific fields."""
    timestamps: list[int] = []
    for event in events:
        for name in ("timestamp", "time", "ts"):
            value = event.get(name)
            if value is None and isinstance(event.get("value"), dict):
                value = event["value"].get(name)
            if value is not None:
                try:
                    timestamps.append(int(float(value)))
                except (TypeError, ValueError):
                    pass
                break

    summary: dict[str, Any] = {
        "key": key,
        "events": events,
        "event_count": len(events),
        "first_ts": min(timestamps) if timestamps else 0,
        "last_ts": max(timestamps) if timestamps else 0,
    }
    if key_field != "key":
        summary[key_field] = key
    return summary


def _session_detect_gap_ms(event: dict[str, Any]) -> int:
    from examples.agents.session_window_policy import session_gap_ms

    return session_gap_ms(event)


def _session_detect_summarize(key: str, events: list[dict[str, Any]], *, key_field: str) -> dict[str, Any]:
    from examples.agents.session_window_policy import summarize_session

    summary = summarize_session(key, events)
    if key_field != "src_ip" and key_field not in summary:
        summary[key_field] = key
    return summary


def resolve_gap_policy(
    policy: str,
    *,
    gap_ms: int = DEFAULT_GAP_MS,
) -> Callable[[dict[str, Any]], int]:
    if policy == GAP_POLICY_SESSION_DETECT:
        return _session_detect_gap_ms
    return lambda event: default_gap_ms(event, gap_ms=gap_ms)


def resolve_summarize_policy(
    policy: str,
    *,
    key_field: str = "key",
) -> Callable[[str, list[dict[str, Any]]], dict[str, Any]]:
    if policy == GAP_POLICY_SESSION_DETECT:
        return lambda key, events: _session_detect_summarize(key, events, key_field=key_field)
    return lambda key, events: default_summarize(key, events, key_field=key_field)


def gap_ms_from_config(config: dict[str, Any] | None) -> int:
    raw = (config or {}).get("gap_ms", DEFAULT_GAP_MS)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_GAP_MS


def session_summary_to_agent_record(
    summary: dict[str, Any],
    *,
    key_field: str = "key",
) -> dict[str, Any]:
    key = str(summary.get("key") or summary.get(key_field) or "1")
    return {"key": key, "value": summary, "output": summary}


def prepare_agent_input(
    summary: dict[str, Any],
    *,
    agent: str | None,
    key_field: str = "key",
) -> dict[str, Any]:
    """Shape a closed session summary for a downstream agent step."""
    if agent == "session_detect":
        return session_summary_to_agent_record(summary, key_field=key_field)
    if agent == "workflow_counter":
        count = summary.get("event_count", 0)
        key = str(summary.get("key") or summary.get(key_field) or "1")
        return {"key": key, "value": count}
    return session_summary_to_agent_record(summary, key_field=key_field)
