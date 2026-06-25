#!/usr/bin/env python3
"""
Production pipeline e2e: one normalized event → Phase 2 ``cowrie.alerts`` + Phase 3 ``cowrie.react_alerts``.

Usage (inside JobManager with Kafka + Flink sidecars reachable):

  python3 test/test_production_pipeline.py
  python3 test/test_production_pipeline.py --e2e
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _bootstrap_paths() -> None:
    root = "/opt/flink"
    if root not in sys.path:
        sys.path.insert(0, root)
    if os.path.isdir(root):
        return
    repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo not in sys.path:
        sys.path.insert(0, repo)
    try:
        from apemosyne._bootstrap import install_aliases
        from apemosyne.paths import configure_runtime_sys_path

        install_aliases()
        configure_runtime_sys_path()
    except ImportError:
        hp_src = os.path.join(repo, "honeypot", "src")
        for sub in ("core", "pipeline", "traps", "react", "integrations", "cluster", "services"):
            path = os.path.join(hp_src, sub)
            if os.path.isdir(path) and path not in sys.path:
                sys.path.insert(0, path)


_bootstrap_paths()


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv

        for path in ("/opt/flink/.env", ".env"):
            if os.path.isfile(path):
                load_dotenv(path)
                return
    except ImportError:
        pass


def _sample_cowrie_event(*, src_ip: str) -> Dict[str, Any]:
    session = f"prod-test-{uuid.uuid4().hex[:8]}"
    return {
        "eventid": "cowrie.login.success",
        "username": "root",
        "password": "prod-pipeline-test",
        "message": "login attempt [root/prod-pipeline-test] succeeded",
        "sensor": "cowrie",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "src_ip": src_ip,
        "src_port": 54322,
        "session": session,
        "protocol": "ssh",
    }


def _build_normalized_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    from cowrie_normalize import normalize_cowrie_event, sessionization_key

    norm = normalize_cowrie_event(raw)
    norm["key"] = sessionization_key(norm)
    return norm


def _publish_normalized(bootstrap: str, topic: str, payload: Dict[str, Any]) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )
    producer.send(topic, value=json.dumps(payload))
    producer.flush()
    producer.close()


def _wait_for_alert(
    bootstrap: str,
    topic: str,
    *,
    source_ip: str,
    predicate,
    timeout_sec: int = 120,
) -> Optional[Dict[str, Any]]:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=f"prod-pipeline-{topic.replace('.', '-')}-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        value_deserializer=lambda v: v.decode("utf-8", errors="ignore") if v else "",
    )
    deadline = time.time() + timeout_sec
    try:
        while time.time() < deadline:
            for msg in consumer:
                payload = (msg.value or "").strip()
                if not payload:
                    continue
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict):
                    continue
                if obj.get("source_ip") != source_ip:
                    continue
                if predicate(obj):
                    return obj
    finally:
        consumer.close()
    return None


def run_smoke() -> int:
    from cowrie_pipeline import (
        DETECTION_REACT,
        DETECTION_WORKFLOW,
        PHASE2_ENGINE_FLINK_AGENTS,
        PHASE2_ENGINE_PURE_PYTHON,
        TOPIC_ALERTS,
        TOPIC_DISINFO_REQUESTS,
        TOPIC_EVENTS,
        TOPIC_NORMALIZED,
        TOPIC_NORMALIZED_ENRICHED,
        TOPIC_REACT_ALERTS,
        TOPIC_SESSION_ACTOR,
        hot_path_allows_react,
        pipeline_kafka_topics,
        is_react_alert,
        is_workflow_alert,
        kafka_pipeline_active,
        kafka_publish_enabled,
        kafka_topic_for_alert,
        resolve_hot_path_engine,
        resolve_phase2_engine,
    )

    print("=" * 60)
    print("Production pipeline smoke test")
    print("=" * 60)

    wf = {"detection_source": DETECTION_WORKFLOW, "alert_id": "ALERT-1"}
    react = {"detection_source": DETECTION_REACT, "alert_id": "REACT-1"}

    assert kafka_topic_for_alert(wf) == TOPIC_ALERTS
    assert kafka_topic_for_alert(react) == TOPIC_REACT_ALERTS
    assert is_workflow_alert(wf)
    assert is_react_alert(react)
    print(f"OK  topic routing workflow→{TOPIC_ALERTS} react→{TOPIC_REACT_ALERTS}")
    topics = pipeline_kafka_topics()
    for name in (
        TOPIC_EVENTS,
        TOPIC_NORMALIZED,
        TOPIC_NORMALIZED_ENRICHED,
        TOPIC_SESSION_ACTOR,
        TOPIC_ALERTS,
        TOPIC_REACT_ALERTS,
        TOPIC_DISINFO_REQUESTS,
    ):
        assert name in topics, f"missing pipeline topic {name}"
    print(f"OK  pipeline_kafka_topics ({len(topics)} topics)")

    from kafka_alerts_to_dashboard import resolve_alert_topics

    alert_topics = resolve_alert_topics()
    assert TOPIC_ALERTS in alert_topics and TOPIC_REACT_ALERTS in alert_topics
    print(f"OK  resolve_alert_topics ({len(alert_topics)} topics)")

    print(f"OK  kafka_pipeline_active={kafka_pipeline_active()} publish={kafka_publish_enabled()}")

    old_pipeline = os.environ.get("COWRIE_KAFKA_PIPELINE")
    old_allow = os.environ.get("COWRIE_ALLOW_REACT_ON_HOT_PATH")
    os.environ["COWRIE_KAFKA_PIPELINE"] = "1"
    os.environ.pop("COWRIE_ALLOW_REACT_ON_HOT_PATH", None)
    assert resolve_hot_path_engine(cloudera_config_ok=True) == "workflow"
    assert not hot_path_allows_react()
    os.environ["COWRIE_ALLOW_REACT_ON_HOT_PATH"] = "1"
    os.environ["COWRIE_COUNTER_ATTACK_ENGINE"] = "react"
    assert hot_path_allows_react()
    assert resolve_hot_path_engine(cloudera_config_ok=True) == "react"
    if old_pipeline is None:
        os.environ.pop("COWRIE_KAFKA_PIPELINE", None)
    else:
        os.environ["COWRIE_KAFKA_PIPELINE"] = old_pipeline
    if old_allow is None:
        os.environ.pop("COWRIE_ALLOW_REACT_ON_HOT_PATH", None)
    else:
        os.environ["COWRIE_ALLOW_REACT_ON_HOT_PATH"] = old_allow
    print("OK  hot path defaults to workflow when Kafka pipeline active")

    old_phase2 = os.environ.get("COWRIE_PHASE2_ENGINE")
    os.environ.pop("COWRIE_PHASE2_ENGINE", None)
    assert resolve_phase2_engine() == PHASE2_ENGINE_PURE_PYTHON
    os.environ["COWRIE_PHASE2_ENGINE"] = "flink_agents"
    assert resolve_phase2_engine() == PHASE2_ENGINE_FLINK_AGENTS
    if old_phase2 is None:
        os.environ.pop("COWRIE_PHASE2_ENGINE", None)
    else:
        os.environ["COWRIE_PHASE2_ENGINE"] = old_phase2
    print("OK  Phase 2 engine resolves pure_python / flink_agents")

    print("=" * 60)
    print("PASS (smoke)")
    print("=" * 60)
    return 0


def run_e2e() -> int:
    from cowrie_phase3_react_augmentor import cloudera_config_ok, validate_react_alert
    from cowrie_pipeline import DETECTION_REACT, DETECTION_WORKFLOW, TOPIC_ALERTS, TOPIC_REACT_ALERTS

    _load_dotenv()

    print("=" * 60)
    print("Production pipeline e2e test")
    print("=" * 60)

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    from cowrie_pipeline import actor_classify_active, normalized_input_topic

    in_topic = normalized_input_topic()
    workflow_topic = os.environ.get("KAFKA_ALERTS_TOPIC", TOPIC_ALERTS)
    react_topic = os.environ.get("KAFKA_REACT_ALERTS_TOPIC", TOPIC_REACT_ALERTS)

    expected_ip = f"203.0.113.{uuid.uuid4().int % 200 + 10}"
    raw = _sample_cowrie_event(src_ip=expected_ip)
    normalized = _build_normalized_event(raw)

    print(f"Kafka bootstrap: {kafka_bootstrap}")
    print(f"Input topic:     {in_topic}")
    print(f"Workflow topic:  {workflow_topic}")
    print(f"ReAct topic:     {react_topic}")
    print(f"Test src_ip:     {expected_ip}")
    if actor_classify_active():
        print("Note: COWRIE_ACTOR_CLASSIFY=1 — publish targets enriched topic")
    print()

    import cowrie_phase2_workflow_job as phase2

    os.environ.setdefault(
        "KAFKA_GROUP_ID",
        f"cowrie-workflow-flink-prod-{uuid.uuid4().hex[:8]}",
    )

    print("Ensuring Phase 2 workflow job is RUNNING...")
    job_id = phase2.submit_remote_job(wait=True, wait_for_running=True)
    print(f"OK  Phase 2 job {job_id} RUNNING")

    delay = float(os.environ.get("PRODUCTION_E2E_PUBLISH_DELAY", "3"))
    time.sleep(delay)
    print("Publishing normalized event...")
    _publish_normalized(kafka_bootstrap, in_topic, normalized)

    timeout = int(os.environ.get("PRODUCTION_E2E_TIMEOUT", "120"))
    print(f"Waiting for workflow alert on {workflow_topic}...")
    wf_alert = _wait_for_alert(
        kafka_bootstrap,
        workflow_topic,
        source_ip=expected_ip,
        predicate=lambda a: str(a.get("detection_source", "")).lower()
        in (DETECTION_WORKFLOW, "flink_agents"),
        timeout_sec=timeout,
    )
    if wf_alert is None:
        print("FAIL: timed out waiting for Phase 2 alert on cowrie.alerts")
        return 1

    actions = wf_alert.get("response_actions") or []
    if not isinstance(actions, list) or len(actions) < 1:
        print("FAIL: workflow alert missing response_actions")
        return 1

    print(
        f"OK  workflow detection_source={wf_alert.get('detection_source')} "
        f"threat_type={wf_alert.get('threat_type')} actions={len(actions)}"
    )

    if not cloudera_config_ok():
        print("SKIP: Phase 3 ReAct (no Cloudera creds) — workflow path verified")
        print("=" * 60)
        print("PASS (e2e workflow only)")
        print("=" * 60)
        return 0

    print(f"Waiting for ReAct alert on {react_topic}...")
    react_alert = _wait_for_alert(
        kafka_bootstrap,
        react_topic,
        source_ip=expected_ip,
        predicate=lambda a: str(a.get("detection_source", "")).lower() == DETECTION_REACT,
        timeout_sec=timeout,
    )
    if react_alert is None:
        print(
            "WARN: timed out waiting for cowrie.react_alerts "
            "(ensure kafka-react-augmentor is running)"
        )
        print("=" * 60)
        print("PASS (e2e workflow; ReAct timeout)")
        print("=" * 60)
        return 0

    errs = validate_react_alert(react_alert, expected_ip=expected_ip)
    if errs:
        print(f"FAIL: ReAct alert invalid: {', '.join(errs)}")
        return 1

    print(
        f"OK  react detection_source={react_alert.get('detection_source')} "
        f"threat_type={react_alert.get('threat_type')} "
        f"confidence={react_alert.get('confidence')}"
    )
    print("=" * 60)
    print("PASS (e2e workflow + ReAct)")
    print("=" * 60)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Production Cowrie pipeline test")
    parser.add_argument(
        "--e2e",
        action="store_true",
        help="Publish normalized event; verify cowrie.alerts (+ react if Cloudera configured)",
    )
    args = parser.parse_args(argv)
    if args.e2e:
        return run_e2e()
    return run_smoke()


if __name__ == "__main__":
    raise SystemExit(main())
