"""Lab-gated Kafka↔NiFi backfill / replay job (not heal)."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ratatoskr.dataplane.flow import (
    find_dataplane_pg_id,
    start_replay_path,
    stop_replay_path,
)
from ratatoskr.dataplane.topics import TOPIC_REPLAY_OUT, TOPIC_VALID
from ratatoskr.replay.env import (
    default_replay_dest,
    default_replay_group,
    default_replay_hours,
    default_replay_source,
    phase_at_least,
    replay_catchup_timeout_sec,
    replay_dry_run,
    replay_phase,
    replay_verify,
)


def _topic_end_offsets(topic: str) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_servers(),
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "topic": topic,
            "missing": True,
            "end": {},
            "total_end": 0,
            "warning": str(exc),
        }
    try:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            return {"topic": topic, "missing": True, "end": {}, "total_end": 0}
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        end = consumer.end_offsets(tps)
        return {
            "topic": topic,
            "missing": False,
            "end": {f"{tp.partition}": end[tp] for tp in tps},
            "total_end": sum(end.values()),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "topic": topic,
            "missing": True,
            "end": {},
            "total_end": 0,
            "warning": str(exc),
        }
    finally:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            pass


def build_replay_plan(
    *,
    source: str | None = None,
    dest: str | None = None,
    hours: float | None = None,
    group: str | None = None,
) -> dict[str, Any]:
    src = source or default_replay_source()
    dst = dest or default_replay_dest()
    hrs = default_replay_hours() if hours is None else float(hours)
    grp = group or default_replay_group()
    now_ms = int(time.time() * 1000)
    ts_ms = now_ms - int(hrs * 3600 * 1000)
    source_meta = _topic_end_offsets(src)
    dest_meta = _topic_end_offsets(dst)
    steps = [
        {
            "op": "stop_replay_path",
            "reason": "pause_dedicated_replay_processors",
            "allowlisted": ["ReplayConsume", "ReplayMark", "ReplayPublish"],
        },
        {
            "op": "reset_offsets_by_timestamp",
            "group_id": grp,
            "topic": src,
            "timestamp_ms": ts_ms,
            "hours": hrs,
        },
        {
            "op": "start_replay_path",
            "reason": "consume_window_publish_dest",
            "dest_topic": dst,
        },
        {
            "op": "wait_catchup",
            "timeout_sec": replay_catchup_timeout_sec(),
        },
        {
            "op": "stop_replay_path",
            "reason": "idle_replay_path",
        },
    ]
    return {
        "source": src,
        "dest": dst,
        "group_id": grp,
        "hours": hrs,
        "timestamp_ms": ts_ms,
        "source_meta": source_meta,
        "dest_meta_before": dest_meta,
        "job_steps": steps,
        "live_groups_untouched": True,
        "note": "Only ratatoskr-dataplane-replay group + Replay* processors are mutated",
    }


def _wait_dest_growth(
    dest: str,
    before_total: int,
    *,
    timeout_sec: float,
    poll_sec: float = 1.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    last = before_total
    while time.monotonic() < deadline:
        meta = _topic_end_offsets(dest)
        last = int(meta.get("total_end") or 0)
        if last > before_total:
            return {
                "ok": True,
                "before": before_total,
                "after": last,
                "grew": last - before_total,
            }
        time.sleep(poll_sec)
    return {
        "ok": last > before_total,
        "before": before_total,
        "after": last,
        "grew": max(0, last - before_total),
        "timeout": True,
    }


def apply_replay_plan(
    plan: dict[str, Any],
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    nifi_client: Optional[Any] = None,
    kafka_client: Optional[Any] = None,
) -> list[dict[str, Any]]:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.nifi.client import NiFiClient

    active = (phase or replay_phase()).strip().lower()
    is_dry = replay_dry_run() if dry_run is None else bool(dry_run)
    actions: list[dict[str, Any]] = []

    if not phase_at_least(active, "lab"):
        for step in plan.get("job_steps") or []:
            actions.append({**step, "ok": False, "skipped": True, "reason": "phase_not_lab"})
        return actions

    nifi = nifi_client or NiFiClient()
    kafka = kafka_client or KafkaClient()
    own_kafka = kafka_client is None
    try:
        pg_id = find_dataplane_pg_id(nifi)
        if not pg_id:
            return [{"op": "replay", "ok": False, "error": "dataplane pg missing"}]

        dest_before = int((plan.get("dest_meta_before") or {}).get("total_end") or 0)

        for step in plan.get("job_steps") or []:
            op = str(step.get("op") or "")
            if is_dry:
                actions.append({**step, "ok": True, "dry_run": True})
                continue
            try:
                if op == "stop_replay_path":
                    names = stop_replay_path(nifi, pg_id)
                    actions.append({**step, "ok": True, "stopped": names})
                elif op == "start_replay_path":
                    names = start_replay_path(nifi, pg_id)
                    actions.append({**step, "ok": True, "started": names})
                elif op == "reset_offsets_by_timestamp":
                    result = kafka.reset_offsets_by_timestamp(
                        str(step["group_id"]),
                        str(step["topic"]),
                        int(step["timestamp_ms"]),
                    )
                    actions.append({**step, "ok": True, "result": result})
                elif op == "wait_catchup":
                    result = _wait_dest_growth(
                        str(plan.get("dest") or TOPIC_REPLAY_OUT),
                        dest_before,
                        timeout_sec=float(step.get("timeout_sec") or 30),
                    )
                    actions.append({**step, "ok": bool(result.get("ok")), "result": result})
                else:
                    actions.append({**step, "ok": False, "error": f"unknown op {op}"})
            except Exception as exc:  # noqa: BLE001
                actions.append({**step, "ok": False, "error": str(exc)})
                break
    finally:
        if own_kafka:
            kafka.close()
    return actions


def run_replay_cycle(
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    source: str | None = None,
    dest: str | None = None,
    hours: float | None = None,
    group: str | None = None,
) -> dict[str, Any]:
    active = (phase or replay_phase()).strip().lower()
    plan = build_replay_plan(
        source=source, dest=dest, hours=hours, group=group
    )
    is_dry = replay_dry_run() if dry_run is None else bool(dry_run)
    actions: list[dict[str, Any]] = []

    if active == "monitor" and not is_dry:
        actions = [{**s, "ok": True, "planned": True} for s in plan.get("job_steps") or []]
    elif active == "lab" or is_dry:
        # dry_run simulates lab steps without mutations
        actions = apply_replay_plan(
            plan,
            phase="lab",
            dry_run=True if is_dry else False,
        )

    verify: dict[str, Any] | None = None
    if (
        replay_verify()
        and active == "lab"
        and not is_dry
        and actions
        and any(a.get("ok") and not a.get("dry_run") for a in actions)
    ):
        dest_topic = str(plan.get("dest") or TOPIC_REPLAY_OUT)
        after = _topic_end_offsets(dest_topic)
        verify = {
            "dest": after,
            "source_group": plan.get("group_id"),
            "live_path_note": "schema/route consumer groups were not reset",
        }

    classification = {
        "healthy": True,
        "level": "OK",
        "score": 100,
        "severities": [],
        "summary": "replay_plan" if active == "monitor" else "replay_job",
    }
    if any(a.get("ok") is False and not a.get("skipped") for a in actions):
        classification = {
            "healthy": False,
            "level": "HIGH",
            "score": 40,
            "severities": ["REPLAY_FAILED"],
            "summary": "REPLAY_FAILED",
        }

    return {
        "agent": "workflow_replay",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "phase": active,
        "classification": classification,
        "job": plan,
        "job_steps": actions or plan.get("job_steps"),
        "actions": actions,
        "mutations": [
            a
            for a in actions
            if a.get("ok") and not a.get("dry_run") and not a.get("planned")
        ],
        "verify": verify,
        "source_default": TOPIC_VALID,
        "dest_default": TOPIC_REPLAY_OUT,
    }
