"""Deterministic CM runbook from ``workflow_cm_monitor`` OutputEvents."""

from __future__ import annotations

from typing import Any

from ratatoskr.nifi.runbook.schema import empty_runbook, wrap_runbook_event


def _severities(event: dict[str, Any]) -> list[str]:
    classification = event.get("classification") or {}
    sevs = list(classification.get("severities") or [])
    if sevs:
        return [str(s) for s in sevs]
    health = event.get("health") or {}
    return [str(s) for s in (health.get("severities") or [])]


def _recommendation_summaries(
    recommendations: list[dict[str, Any]] | None,
    *,
    limit: int = 5,
) -> list[str]:
    out: list[str] = []
    for rec in recommendations or []:
        if not isinstance(rec, dict):
            continue
        summary = str(rec.get("summary") or rec.get("rule_id") or "").strip()
        if summary:
            out.append(summary)
        if len(out) >= limit:
            break
    return out


def _diagnostic_steps_from_recommendations(
    recommendations: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for rec in recommendations or []:
        if not isinstance(rec, dict):
            continue
        manual = rec.get("manual_steps") or []
        console_url = rec.get("console_url")
        where = "CM UI"
        if console_url:
            where = str(console_url)
        for step in manual:
            text = str(step).strip()
            if not text:
                continue
            steps.append({"step": text, "where": where, "expect": "Actionable signal"})
        if len(steps) >= 12:
            break
    if not steps:
        steps.append(
            {
                "step": "Open Cloudera Manager → cluster home and review health summary",
                "where": "CM UI",
                "expect": "Severity badges match monitor classification",
            }
        )
    return steps


def fallback_runbook(monitor_event: dict[str, Any]) -> dict[str, Any]:
    """Build a valid runbook from a ``workflow_cm_monitor`` OutputEvent."""
    event = monitor_event
    if isinstance(monitor_event.get("value"), dict):
        event = monitor_event["value"]

    classification = event.get("classification") or {}
    health = event.get("health") or {}
    recommendations = list(event.get("recommendations") or [])
    sevs = _severities(event)
    level = classification.get("level") or ("OK" if not sevs else "MEDIUM")
    score = classification.get("score")
    cluster = str(health.get("cluster") or event.get("cluster") or "")
    healthy = bool(classification.get("healthy")) and not sevs
    suppressed = int(health.get("suppressed_events") or 0)

    rb = empty_runbook(mode="fallback")

    if healthy:
        rb["headline"] = f"CM cluster {cluster or 'unknown'} looks healthy"
        rb["situation"] = (
            f"Monitor classification level={level}, score={score}. "
            "No severities reported — no remediation required."
        )
        if suppressed:
            rb["situation"] += f" ({suppressed} noisy events suppressed.)"
        rb["likely_causes"] = [
            {
                "cause": "No active CM fault pattern",
                "confidence": "high",
                "evidence": ["severities:[]"],
            }
        ]
        rb["diagnostic_steps"] = [
            {
                "step": "Continue periodic workflow_cm_monitor polls",
                "where": "CLI",
                "expect": "classification.healthy true",
            }
        ]
        rb["verify"] = ["classification.healthy is true", "severities empty"]
        rb["remediation"]["do_not"] = [
            "Do not run CM service commands without a confirmed fault",
        ]
        return wrap_runbook_event(
            rb,
            source={
                "poll_id": event.get("poll_id"),
                "cluster": cluster,
                "severities": sevs,
                "level": level,
                "score": score,
                "recommendation_count": len(recommendations),
                "suppressed_events": suppressed,
            },
            agent="react_cm_runbook",
        )

    rec_summaries = _recommendation_summaries(recommendations)
    rb["headline"] = f"CM cluster {cluster or 'unknown'} — {level} ({', '.join(sevs[:3])})"
    rb["situation"] = (
        f"Monitor reports level={level}, score={score}, severities={sevs}. "
        f"{len(recommendations)} structured recommendation(s) emitted."
    )
    if suppressed:
        rb["situation"] += f" Suppressed {suppressed} duplicate/noisy event(s)."

    causes: list[dict[str, Any]] = []
    for sev in sevs[:6]:
        causes.append(
            {
                "cause": f"Active severity: {sev}",
                "confidence": "high" if sev in {"CM_UNREACHABLE", "ROLE_DOWN", "SERVICE_DOWN"} else "medium",
                "evidence": [f"severity:{sev}"],
            }
        )
    for summary in rec_summaries[:3]:
        causes.append(
            {
                "cause": summary,
                "confidence": "medium",
                "evidence": ["recommendation"],
            }
        )
    rb["likely_causes"] = causes or [
        {
            "cause": "CM health degradation detected",
            "confidence": "medium",
            "evidence": sevs,
        }
    ]

    rb["diagnostic_steps"] = _diagnostic_steps_from_recommendations(recommendations)

    safe_options: list[str] = []
    for rec in recommendations:
        if str(rec.get("priority") or "").lower() != "high":
            continue
        rule_id = rec.get("rule_id")
        if rule_id:
            safe_options.append(f"Follow runbook for rule {rule_id}: {rec.get('summary', '')}")
    rb["remediation"]["safe_options"] = safe_options[:8] or [
        "Review CM recommendations and execute manual steps in change window",
    ]
    rb["remediation"]["lab_options"] = []
    rb["remediation"]["do_not"] = [
        "Do not restart services or run CM commands from this agent (recommend-only)",
        "Do not suppress production alerts without documenting the underlying issue",
    ]

    rb["verify"] = [
        "Re-run workflow_cm_monitor and confirm severities decrease",
        "classification.score improves or stabilizes",
        "Grouped critical_events count does not grow",
    ]

    return wrap_runbook_event(
        rb,
        source={
            "poll_id": event.get("poll_id"),
            "cluster": cluster,
            "severities": sevs,
            "level": level,
            "score": score,
            "recommendation_count": len(recommendations),
            "suppressed_events": suppressed,
        },
        agent="react_cm_runbook",
    )
