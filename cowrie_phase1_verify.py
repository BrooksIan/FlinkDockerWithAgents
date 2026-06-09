"""
Phase 1.5 verifier for Kafka ingestion + normalization.

Reads a small sample from:
- raw topic: cowrie.events
- normalized topic: cowrie.normalized

Validates that normalized messages conform to expected schema and that the sessionization key is present.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Tuple


def _env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


REQUIRED_NORMALIZED_KEYS = [
    "ts",
    "ingested_at",
    "source",
    "event_id",
    "event_type",
    "src_ip",
    "key",
    "raw",
]


def _safe_load(s: str) -> Dict[str, Any]:
    try:
        o = json.loads(s)
        return o if isinstance(o, dict) else {"_non_dict": o}
    except Exception as e:
        return {"_parse_error": str(e), "_raw": s}


def _validate_normalized(obj: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    for k in REQUIRED_NORMALIZED_KEYS:
        if k not in obj:
            errs.append(f"missing:{k}")
    if obj.get("source") != "cowrie":
        errs.append("source!=cowrie")
    if not isinstance(obj.get("raw"), dict):
        errs.append("raw_not_dict")
    if not obj.get("key"):
        errs.append("empty_key")
    if not obj.get("src_ip"):
        errs.append("empty_src_ip")
    return errs


def _sample_topic(consumer, n: int) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    for _ in range(n):
        msg = next(consumer)
        s = (msg.value or "").strip()
        out.append((s, _safe_load(s)))
    return out


def main() -> int:
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("ERROR: kafka-python not installed. Install: python3 -m pip install kafka-python")
        return 2

    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic = _env("KAFKA_COWRIE_TOPIC", "cowrie.events")
    norm_topic = _env("KAFKA_NORMALIZED_TOPIC", "cowrie.normalized")
    n = int(_env("PHASE1_VERIFY_N", "5"))

    def _consumer(topic: str, group: str):
        return KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group,
            auto_offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", "latest"),
            enable_auto_commit=False,
            consumer_timeout_ms=8000,
            value_deserializer=lambda v: v.decode("utf-8", errors="ignore") if v else "",
        )

    print(f"Phase 1.5 verify (bootstrap={bootstrap})")
    print(f"- raw:        {raw_topic}")
    print(f"- normalized: {norm_topic}")
    print("")

    raw_c = _consumer(raw_topic, "phase1-verify-raw")
    norm_c = _consumer(norm_topic, "phase1-verify-norm")

    try:
        raw_msgs = _sample_topic(raw_c, n)
    except StopIteration:
        raw_msgs = []
    try:
        norm_msgs = _sample_topic(norm_c, n)
    except StopIteration:
        norm_msgs = []

    print(f"Raw messages read: {len(raw_msgs)}")
    if raw_msgs:
        ex = raw_msgs[0][1]
        print(f"Raw example keys: {sorted(list(ex.keys()))[:12]}{' ...' if len(ex.keys()) > 12 else ''}")
    print("")

    print(f"Normalized messages read: {len(norm_msgs)}")
    bad = 0
    for i, (_, obj) in enumerate(norm_msgs, 1):
        errs = _validate_normalized(obj)
        if errs:
            bad += 1
            print(f"- normalized[{i}] INVALID: {', '.join(errs)}")
        else:
            # Compact “shape” print
            print(
                f"- normalized[{i}] OK: event_type={obj.get('event_type')} src_ip={obj.get('src_ip')} "
                f"session_id={obj.get('session_id')} key={obj.get('key')}"
            )

    print("")
    if not raw_msgs:
        print("NOTE: no raw messages seen. Generate an event (e.g. ssh to Cowrie) and re-run.")
    if not norm_msgs:
        print("NOTE: no normalized messages seen. Ensure flink-pipeline-supervisor is running and raw events exist.")

    if bad:
        print(f"FAIL: {bad}/{len(norm_msgs)} normalized messages invalid")
        return 1
    print("PASS: normalized messages match expected schema (Phase 1.5)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

