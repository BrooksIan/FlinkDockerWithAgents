"""CM event normalization, suppression, and grouping for IT ops signal quality."""

from __future__ import annotations

import hashlib
import re
from typing import Any

# Built-in CDP event patterns → stable kind for recommendations.
EVENT_KIND_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("impala_spnego", re.compile(r"Must authenticate with SPNEGO", re.I)),
    ("impala_state_fetcher", re.compile(r"impala_IMPALA_SERVICE_STATE_FETCHER", re.I)),
    (
        "zookeeper_ssl_keystore",
        re.compile(r"zookeeper\.ssl\.keyStore\.location not specified", re.I),
    ),
    ("metrics_missing", re.compile(r"No metrics in EntityTypeUpdateEntry", re.I)),
]

DEFAULT_SUPPRESS_PATTERNS: list[str] = [
    r"zookeeper\.ssl\.keyStore\.location not specified",
]

_HIGH_IMPACT_KINDS = frozenset({"impala_spnego", "impala_state_fetcher"})


def cm_event_suppress_patterns() -> list[re.Pattern[str]]:
    import os

    raw = (os.environ.get("CM_EVENT_SUPPRESS_PATTERNS") or "").strip()
    patterns = list(DEFAULT_SUPPRESS_PATTERNS)
    if raw:
        if raw.lower() in ("none", "off", "0"):
            patterns = []
        else:
            patterns.extend(p.strip() for p in raw.split(",") if p.strip())
    out: list[re.Pattern[str]] = []
    for pat in patterns:
        try:
            out.append(re.compile(pat, re.I))
        except re.error:
            continue
    return out


def normalize_event_content(content: Any) -> str:
    text = str(content or "").strip()
    text = re.sub(r"^\(\d+\s+skipped\)\s*", "", text, flags=re.I)
    text = re.sub(r"https?://\S+", "<url>", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def classify_event_kind(content: str) -> str | None:
    for kind, pattern in EVENT_KIND_PATTERNS:
        if pattern.search(content):
            return kind
    return None


def infer_service_hint(content: str, *, category: str | None = None) -> str | None:
    kind = classify_event_kind(content)
    if kind:
        if kind.startswith("impala"):
            return "impala"
        if kind.startswith("zookeeper"):
            return "zookeeper"
    lowered = content.lower()
    for svc in ("impala", "hdfs", "yarn", "kafka", "zookeeper", "hive", "hbase"):
        if svc in lowered:
            return svc
    if category and str(category).upper() != "LOG_EVENT":
        return str(category).lower()
    return None


def event_fingerprint(content: str, *, category: str | None = None) -> str:
    kind = classify_event_kind(content)
    if kind:
        return kind
    norm = normalize_event_content(content)
    if not norm:
        return "empty"
    digest = hashlib.sha256(norm.encode("utf-8", errors="replace")).hexdigest()[:16]
    cat = (category or "event").lower()
    return f"{cat}:{digest}"


def _is_suppressed(content: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(p.search(content) for p in patterns)


def process_cm_events(
    events: list[dict[str, Any]],
    *,
    suppress_patterns: list[re.Pattern[str]] | None = None,
) -> dict[str, Any]:
    """
    Filter, suppress, and group raw CM events.

    Returns grouped ``critical_events`` and ``event_warnings`` plus audit counts.
    """
    patterns = suppress_patterns if suppress_patterns is not None else cm_event_suppress_patterns()
    groups: dict[str, dict[str, Any]] = {}
    suppressed = 0

    for event in events:
        if not isinstance(event, dict):
            continue
        severity = str(event.get("severity") or "").upper()
        if severity not in ("CRITICAL", "IMPORTANT", "ERROR"):
            continue
        raw_content = event.get("content") or ""
        if isinstance(event.get("attributes"), dict):
            raw_content = raw_content or event["attributes"].get("content")
        content = normalize_event_content(raw_content)
        if not content:
            continue
        if _is_suppressed(content, patterns):
            suppressed += 1
            continue

        fp = event_fingerprint(content, category=str(event.get("category") or ""))
        kind = classify_event_kind(content)
        service_hint = infer_service_hint(content, category=str(event.get("category") or ""))
        occurred = event.get("timeOccurred") or event.get("time_occurred")

        if fp not in groups:
            groups[fp] = {
                "fingerprint": fp,
                "event_kind": kind,
                "content": content,
                "severity": severity,
                "category": event.get("category"),
                "service_hint": service_hint,
                "count": 0,
                "first_seen": occurred,
                "last_seen": occurred,
                "sample_ids": [],
            }
        group = groups[fp]
        group["count"] = int(group.get("count") or 0) + 1
        if occurred and (not group.get("first_seen") or str(occurred) < str(group["first_seen"])):
            group["first_seen"] = occurred
        if occurred and (not group.get("last_seen") or str(occurred) > str(group["last_seen"])):
            group["last_seen"] = occurred
        if severity in ("CRITICAL", "ERROR"):
            group["severity"] = severity
        eid = event.get("id")
        if eid and len(group["sample_ids"]) < 3:
            group["sample_ids"].append(eid)

    critical_events: list[dict[str, Any]] = []
    event_warnings: list[dict[str, Any]] = []
    for group in sorted(groups.values(), key=lambda g: (-int(g.get("count") or 0), str(g.get("content") or ""))):
        sev = str(group.get("severity") or "")
        kind = group.get("event_kind")
        if sev in ("CRITICAL", "ERROR") or kind in _HIGH_IMPACT_KINDS:
            critical_events.append(group)
        elif sev == "IMPORTANT":
            event_warnings.append(group)

    return {
        "critical_events": critical_events,
        "event_warnings": event_warnings,
        "suppressed_events": suppressed,
        "raw_event_count": len(events),
    }


__all__ = [
    "EVENT_KIND_PATTERNS",
    "classify_event_kind",
    "event_fingerprint",
    "normalize_event_content",
    "process_cm_events",
]
