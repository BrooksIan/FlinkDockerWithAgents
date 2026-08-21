#!/usr/bin/env python3
"""Heal demo catalog for the shared Kafka→NiFi lab.

Scenarios (``--list`` for the table):

  NiFi safe
    stop-consume   STOPPED ConsumeKafka → start_processor
    disable-cs     DISABLED Studio Kafka CS → enable + start

  NiFi lab
    invalid-log    INVALID LogAttribute → fix_processor_config (template)
    queue-backlog  queued update-to-log → empty_queue + start

  Kafka
    delete-topic         TOPIC_MISSING → create_topic
    increase-partitions  TOPIC_PARTITIONS_LOW → increase_partitions
    lag-group            empty/stalled group → delete_group (prefix allow)
    lag-earliest         LAG_CRIT → reset_offsets strategy=earliest

  Cross-stack (workflow_cross_stack_heal)
    cross-topic    TOPIC_MISSING + STOPPED → create_topic then start ConsumeKafka
    cross-lag      BACKPRESSURE + LAG → NiFi queue relief playbook

Prereqs:
  ratatoskr kafka up && ratatoskr up --profile nifi
  ./scripts/nifi_load_kafka_flow.sh

Usage:
  python3 scripts/demo_nifi_kafka_heal.py --list
  python3 scripts/demo_nifi_kafka_heal.py --scenario invalid-log
  python3 scripts/demo_nifi_kafka_heal.py --scenario cross-topic
  python3 scripts/demo_nifi_kafka_heal.py --all
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]

# Fault group dedicated to lag demos (not the live NiFi consumer group).
FAULT_LAG_GROUP = "ratatoskr-kafka-fault-lab"
DEMO_TOPIC = "nifi.kafka.demo"


def _bootstrap() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _load(name: str, rel: str):
    path = ROOT / rel
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _banner(title: str) -> None:
    print()
    print("=" * 64)
    print(title)
    print("=" * 64)


def _health(result: dict) -> dict:
    return result.get("health") or {}


def _summarize_nifi(result: dict) -> None:
    cls = result.get("classification") or {}
    health = _health(result)
    audit = result.get("audit") or {}
    queued = [
        {
            "name": q.get("name"),
            "flowFilesQueued": q.get("flowFilesQueued"),
            "backpressure_level": q.get("backpressure_level"),
        }
        for q in (health.get("queued_connections") or [])
    ]
    print(
        json.dumps(
            {
                "phase": result.get("phase"),
                "healthy": health.get("healthy"),
                "level": cls.get("level"),
                "severities": health.get("severities") or cls.get("severities"),
                "stopped": [p.get("name") for p in (health.get("stopped_processors") or [])],
                "invalid": [p.get("name") for p in (health.get("invalid_processors") or [])],
                "disabled_services": [
                    s.get("name")
                    for s in (health.get("disabled_controller_services") or [])
                ],
                "queued": queued,
                "heal_actions": result.get("heal_actions") or [],
                "dry_run": audit.get("dry_run"),
            },
            indent=2,
            default=str,
        )
    )


def _summarize_kafka(result: dict) -> None:
    cls = result.get("classification") or {}
    health = _health(result)
    audit = result.get("audit") or {}
    print(
        json.dumps(
            {
                "phase": result.get("phase"),
                "healthy": health.get("healthy"),
                "level": cls.get("level"),
                "severities": health.get("severities") or cls.get("severities"),
                "missing_topics": [
                    t.get("name") if isinstance(t, dict) else t
                    for t in (health.get("missing_topics") or [])
                ],
                "undersized": [
                    t.get("name") for t in (health.get("undersized_topics") or [])
                ],
                "oversized": [
                    t.get("name") for t in (health.get("oversized_topics") or [])
                ],
                "lag_warn": [
                    g.get("group_id") for g in (health.get("lag_warn_groups") or [])
                ],
                "lag_crit": [
                    g.get("group_id") for g in (health.get("lag_crit_groups") or [])
                ],
                "empty_lagging": [
                    g.get("group_id") for g in (health.get("empty_lagging_groups") or [])
                ],
                "heal_actions": result.get("heal_actions") or [],
                "dry_run": audit.get("dry_run"),
            },
            indent=2,
            default=str,
        )
    )


def _ops(actions: list[dict]) -> set[str]:
    return {str(a.get("op")) for a in actions if a.get("op")}


def _clear_nifi_env() -> None:
    for key in (
        "NIFI_HEAL_ALLOW_EMPTY_QUEUE",
        "NIFI_HEAL_ALLOW_IDS",
        "NIFI_HEAL_ALLOW_NAME_REGEX",
        "NIFI_WATCH_NAME_REGEX",
        "NIFI_WATCH_ID_REGEX",
        "NIFI_HEAL_PHASE",
    ):
        os.environ.pop(key, None)


def _clear_kafka_env() -> None:
    for key in (
        "KAFKA_HEAL_PHASE",
        "KAFKA_HEAL_ALLOW_GROUPS",
        "KAFKA_HEAL_ALLOW_TOPICS",
        "KAFKA_HEAL_ALLOW_NAME_REGEX",
        "KAFKA_HEAL_DRY_RUN",
        "KAFKA_FLAG_UNEXPECTED",
    ):
        os.environ.pop(key, None)


def _ensure_nifi_baseline(fault, flow, sample) -> tuple[Any, str]:
    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    sample.wait_ready(client, attempts=12, delay=2.0)
    ensure = flow.ensure_kafka_flow(client, repair=False)
    pg_id = ensure["process_group_id"]
    baseline = client.get_flow_health_status(pg_id)
    if not baseline.get("healthy"):
        print("Baseline unhealthy — repairing Kafka demo flow")
        flow.repair_kafka_flow(client, pg_id, bootstrap=flow.default_bootstrap())
        time.sleep(1.0)
        baseline = client.get_flow_health_status(pg_id)
    print(
        f"Baseline PG={pg_id} topic={flow.DEMO_TOPIC} healthy={baseline.get('healthy')}"
    )
    if not baseline.get("healthy"):
        raise RuntimeError(f"unhealthy baseline: {baseline.get('severities')}")
    return client, pg_id


def _nifi_detect_ok(health: dict, spec: dict) -> bool:
    stopped = {p.get("name") for p in (health.get("stopped_processors") or [])}
    invalid = {p.get("name") for p in (health.get("invalid_processors") or [])}
    disabled = {
        s.get("name") for s in (health.get("disabled_controller_services") or [])
    }
    sevs = set(health.get("severities") or [])
    queued = health.get("queued_connections") or []

    if spec.get("expect_stopped") and not set(spec["expect_stopped"]) & stopped:
        return False
    if spec.get("expect_invalid") and not set(spec["expect_invalid"]) & invalid:
        return False
    if spec.get("expect_disabled") and not set(spec["expect_disabled"]) & disabled:
        return False
    if spec.get("expect_severities") and not set(spec["expect_severities"]) & sevs:
        return False
    if spec.get("expect_queued") and not queued:
        # soft: severity BACKPRESSURE may also count
        if not ({"BACKPRESSURE", "BACKPRESSURE_WARN", "BACKPRESSURE_CRIT"} & sevs):
            return False
    return True


def run_nifi_scenario(
    *,
    name: str,
    spec: dict,
    dry_run: bool,
    publish_after: int,
) -> int:
    from ratatoskr.kafka_sources import kafka_reachable
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import reset_heal_cooldown, run_monitor_cycle

    fault = _load("nifi_fault_inject", "scripts/nifi_fault_inject.py")
    flow = _load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    sample = _load("nifi_load_sample_flow", "scripts/nifi_load_sample_flow.py")

    if not kafka_reachable():
        print("FAIL  Studio Kafka not up — run: ratatoskr kafka up")
        return 1

    _clear_nifi_env()
    for k, v in (spec.get("env") or {}).items():
        os.environ[k] = str(v)
    os.environ["NIFI_HEAL_DRY_RUN"] = "1" if dry_run else "0"
    os.environ["NIFI_HEAL_VERIFY"] = "1"
    reset_heal_cooldown()

    client, pg_id = _ensure_nifi_baseline(fault, flow, sample)
    heal_phase = spec["heal_phase"]

    _banner(f"1) Inject fault — {name}")
    print(spec.get("blurb", ""))
    injected = spec["inject"](fault, client, pg_id)
    print(json.dumps(injected, indent=2, default=str))
    time.sleep(float(spec.get("settle_sec", 1.0)))

    _banner(f"2) Monitor phase (detect only)")
    reset_heal_cooldown()
    detect = run_monitor_cycle(client, pg_id, phase="monitor")
    _summarize_nifi(detect)
    if detect.get("heal_actions"):
        print("FAIL  monitor phase must not mutate")
        return 1
    if not _nifi_detect_ok(_health(detect), spec):
        print("FAIL  expected fault not visible in monitor snapshot")
        return 1
    print("OK    fault visible; no heal_actions")

    _banner(f"3) Heal phase={heal_phase}")
    reset_heal_cooldown()
    actions: list[dict] = []
    heal = run_monitor_cycle(NiFiClient(), pg_id, phase=heal_phase, dry_run=dry_run)
    _summarize_nifi(heal)
    actions.extend(heal.get("heal_actions") or [])

    max_passes = int(spec.get("heal_passes", 2))
    for i in range(2, max_passes + 1):
        if dry_run or _health(heal).get("healthy"):
            break
        # INVALID may remain until config restore; queue may need extra pass
        if spec.get("skip_extra_if_ops") and _ops(actions) >= set(spec["skip_extra_if_ops"]):
            break
        time.sleep(1.5)
        reset_heal_cooldown()
        heal = run_monitor_cycle(NiFiClient(), pg_id, phase=heal_phase, dry_run=False)
        print(f"--- heal pass {i} ---")
        _summarize_nifi(heal)
        actions.extend(heal.get("heal_actions") or [])

    if not actions:
        print("FAIL  no heal_actions proposed/applied")
        return 1
    want_ops = set(spec.get("expect_ops") or [])
    got = _ops(actions)
    if want_ops and not (want_ops & got):
        print(f"FAIL  expected ops intersecting {want_ops}, got {got}")
        return 1
    print(f"OK    heal_actions={len(actions)} ops={sorted(got)} dry_run={dry_run}")

    # INVALID containment leave config broken — restore relationships for a clean end state.
    post = spec.get("post_heal")
    if post == "restore_log_config" and not dry_run:
        _banner("3b) Config restore (INVALID needs property fix beyond terminate)")
        print(json.dumps(fault.restore_log_attribute_config(NiFiClient(), pg_id), indent=2))
        # Ensure rest of flow running
        flow.repair_kafka_flow(NiFiClient(), pg_id, bootstrap=flow.default_bootstrap())

    if dry_run:
        _banner("4) Dry-run — restoring flow")
        flow.repair_kafka_flow(NiFiClient(), pg_id, bootstrap=flow.default_bootstrap())
        print("OK    restored")
        return 0

    _banner("4) Verify")
    time.sleep(1.0)
    reset_heal_cooldown()
    # After invalid-log restore, expect healthy; after queue/lab, allow one safe start pass
    verify = run_monitor_cycle(NiFiClient(), pg_id, phase="monitor")
    _summarize_nifi(verify)
    if not _health(verify).get("healthy"):
        # One opportunistic safe pass for leftover STOPPED after lab queue relief
        reset_heal_cooldown()
        fix = run_monitor_cycle(NiFiClient(), pg_id, phase="safe", dry_run=False)
        print("--- leftover safe start ---")
        _summarize_nifi(fix)
        verify = run_monitor_cycle(NiFiClient(), pg_id, phase="monitor")
        _summarize_nifi(verify)
    if not _health(verify).get("healthy"):
        print("FAIL  still unhealthy after heal")
        return 1
    print("OK    healthy after heal")

    if publish_after > 0:
        _banner("5) Publish smoke")
        from kafka import KafkaProducer

        marker = f"healed-{name}-{int(time.time())}"
        p = KafkaProducer(bootstrap_servers="localhost:9094")
        for i in range(publish_after):
            p.send(
                flow.DEMO_TOPIC,
                json.dumps({"hello": "healed", "scenario": name, "marker": marker, "i": i}).encode(),
            )
        p.flush()
        p.close()
        time.sleep(2.0)
        if not NiFiClient().get_flow_health_status(pg_id).get("healthy"):
            print("FAIL  unhealthy after publish")
            return 1
        print(f"OK    published {publish_after} marker={marker}")

    _banner(f"PASS — {name}")
    return 0


def run_kafka_scenario(
    *,
    name: str,
    spec: dict,
    dry_run: bool,
    publish_after: int,
) -> int:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import reset_heal_cooldown, run_monitor_cycle
    from ratatoskr.kafka_sources import kafka_reachable
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import (
        reset_heal_cooldown as reset_nifi_cooldown,
        run_monitor_cycle as run_nifi_cycle,
    )

    kfault = _load("kafka_fault_inject", "scripts/kafka_fault_inject.py")
    nfault = _load("nifi_fault_inject", "scripts/nifi_fault_inject.py")
    flow = _load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    sample = _load("nifi_load_sample_flow", "scripts/nifi_load_sample_flow.py")

    if not kafka_reachable():
        print("FAIL  Studio Kafka not up — run: ratatoskr kafka up")
        return 1

    _clear_kafka_env()
    for k, v in (spec.get("env") or {}).items():
        os.environ[k] = str(v)
    os.environ["KAFKA_HEAL_DRY_RUN"] = "1" if dry_run else "0"
    os.environ["KAFKA_HEAL_VERIFY"] = "1"
    os.environ.setdefault("KAFKA_CATALOG", "studio")
    reset_heal_cooldown()

    flow.ensure_demo_topic()
    heal_phase = spec["heal_phase"]
    nifi_client = None
    pg_id = None
    if spec.get("stop_nifi_consumer_first"):
        # Prevent ConsumeKafka + auto.create from recreating the topic mid-demo.
        sample.wait_ready(NiFiClient(), attempts=8, delay=2.0)
        nifi_client = NiFiClient()
        ensure = flow.ensure_kafka_flow(nifi_client, repair=False)
        pg_id = ensure["process_group_id"]
        print(json.dumps(nfault.inject_stop_consume(nifi_client, pg_id), indent=2))
        time.sleep(1.0)

    _banner(f"1) Inject fault — {name}")
    print(spec.get("blurb", ""))
    kc = KafkaClient()
    try:
        injected = spec["inject"](kfault, kc)
        print(json.dumps(injected, indent=2, default=str))
        # Wait until metadata drops the topic (auto-create races otherwise).
        if name == "delete-topic":
            for _ in range(20):
                if DEMO_TOPIC not in kc.list_topics():
                    break
                time.sleep(0.25)
            if DEMO_TOPIC in kc.list_topics():
                print("FAIL  topic still present after delete (likely auto-recreated)")
                return 1
            observe = float(spec.get("observe_sec", 10.0))
            print(
                f"… pausing {observe:.0f}s so you can watch NiFi bulletins / "
                f"Kafka TOPIC_MISSING before heal"
            )
            time.sleep(observe)
        time.sleep(float(spec.get("settle_sec", 1.0)))

        _banner("2) Monitor phase (detect only)")
        reset_heal_cooldown()
        detect = run_monitor_cycle(kc, phase="monitor")
        _summarize_kafka(detect)
        if detect.get("heal_actions"):
            print("FAIL  monitor phase must not mutate")
            return 1
        health = _health(detect)
        sevs = set(health.get("severities") or [])
        if not (set(spec.get("expect_severities") or []) & sevs):
            print(f"FAIL  expected severities {spec.get('expect_severities')}, got {sevs}")
            return 1
        print("OK    fault visible; no heal_actions")

        _banner(f"3) Heal phase={heal_phase}")
        reset_heal_cooldown()
        heal = run_monitor_cycle(kc, phase=heal_phase, dry_run=dry_run)
        _summarize_kafka(heal)
        actions = list(heal.get("heal_actions") or [])
        if not dry_run and not _health(heal).get("healthy") and int(spec.get("heal_passes", 1)) > 1:
            time.sleep(1.0)
            reset_heal_cooldown()
            heal2 = run_monitor_cycle(kc, phase=heal_phase, dry_run=False)
            print("--- heal pass 2 ---")
            _summarize_kafka(heal2)
            actions.extend(heal2.get("heal_actions") or [])
            heal = heal2
        if not actions:
            print("FAIL  no heal_actions")
            return 1
        want = set(spec.get("expect_ops") or [])
        got = _ops(actions)
        if want and not (want & got):
            print(f"FAIL  expected ops intersecting {want}, got {got}")
            return 1
        print(f"OK    heal_actions={len(actions)} ops={sorted(got)}")

        if dry_run:
            _banner("4) Dry-run — restore catalog")
            print(json.dumps(kfault.restore_catalog(kc), indent=2))
            if pg_id is not None:
                flow.repair_kafka_flow(
                    NiFiClient(), pg_id, bootstrap=flow.default_bootstrap()
                )
            return 0

        _banner("4) Verify")
        time.sleep(1.0)
        reset_heal_cooldown()
        verify = run_monitor_cycle(kc, phase="monitor")
        _summarize_kafka(verify)
        sevs2 = set(_health(verify).get("severities") or [])
        banned = set(spec.get("cleared_severities") or [])
        if banned & sevs2:
            print(f"FAIL  severities still present: {banned & sevs2}")
            return 1
        still_under = {
            t.get("name") for t in (_health(verify).get("undersized_topics") or [])
        }
        for name_ok in spec.get("verify_not_undersized") or []:
            if name_ok in still_under:
                print(f"FAIL  topic still undersized: {name_ok}")
                return 1
        if DEMO_TOPIC not in kc.list_topics() and name == "delete-topic":
            print("FAIL  topic still missing")
            return 1
        print("OK    kafka heal verified")

        if pg_id is not None and not dry_run:
            _banner("4b) Restart NiFi ConsumeKafka after topic recreate")
            reset_nifi_cooldown()
            nifi_heal = run_nifi_cycle(NiFiClient(), pg_id, phase="safe", dry_run=False)
            _summarize_nifi(nifi_heal)
            if not _health(nifi_heal).get("healthy"):
                time.sleep(1.0)
                reset_nifi_cooldown()
                nifi_heal = run_nifi_cycle(NiFiClient(), pg_id, phase="safe", dry_run=False)
                _summarize_nifi(nifi_heal)

        if publish_after > 0 and name in ("delete-topic", "increase-partitions"):
            _banner("5) Publish smoke (topic ready)")
            from kafka import KafkaProducer

            p = KafkaProducer(bootstrap_servers="localhost:9094")
            for i in range(publish_after):
                p.send(DEMO_TOPIC, json.dumps({"hello": "healed", "scenario": name, "i": i}).encode())
            p.flush()
            p.close()
            print(f"OK    published {publish_after} to {DEMO_TOPIC}")

        cleanup_parts = spec.get("cleanup_partitions")
        if cleanup_parts is not None and not dry_run:
            _banner("6) Cleanup — restore demo topic partition count")
            os.environ.pop("KAFKA_TOPIC_PARTITIONS", None)
            os.environ.pop("KAFKA_HEAL_ALLOW_TOPICS", None)
            try:
                kc.recreate_topic(DEMO_TOPIC, partitions=int(cleanup_parts))
                print(json.dumps({"recreated": DEMO_TOPIC, "partitions": cleanup_parts}, indent=2))
            except Exception as exc:  # noqa: BLE001
                print(f"cleanup warning: {exc}")
            if pg_id is not None:
                try:
                    from ratatoskr.nifi.policy import reset_heal_cooldown as reset_nifi_cd
                    from ratatoskr.nifi.policy import run_monitor_cycle as run_nifi

                    reset_nifi_cd()
                    run_nifi(NiFiClient(), pg_id, phase="safe", dry_run=False)
                except Exception as exc:  # noqa: BLE001
                    print(f"nifi restart warning: {exc}")

        _banner(f"PASS — {name}")
        return 0
    finally:
        kc.close()


# --- Inject helpers + cross runner -------------------------------------------

def _sc_stop_consume(fault, client, pg_id):
    return fault.inject_stop_consume(client, pg_id)


def _sc_disable_cs(fault, client, pg_id):
    return fault.inject_disable_kafka_cs(client, pg_id)


def _sc_invalid_log(fault, client, pg_id):
    return fault.inject_kafka_invalid_log(client, pg_id)


def _sc_queue_backlog(fault, client, pg_id):
    return fault.inject_kafka_stop_log(client, pg_id, publish=8, settle_sec=4.0)


def _sc_delete_topic(kfault, kc):
    return kfault.inject_delete_topic(kc, DEMO_TOPIC)


def _sc_lag_group(kfault, kc):
    return kfault.inject_lag_group(
        kc, topic=DEMO_TOPIC, group_id=FAULT_LAG_GROUP, messages=40
    )


def _sc_undersize(kfault, kc):
    return kfault.inject_undersize_topic(kc, DEMO_TOPIC, partitions=1)


def _sc_cross_topic(fault, flow, kfault, nifi_client, kc, pg_id):
    """Stop ConsumeKafka then delete demo topic (avoid auto-create race)."""
    stop = fault.inject_stop_consume(nifi_client, pg_id)
    time.sleep(1.0)
    deleted = kfault.inject_delete_topic(kc, DEMO_TOPIC)
    return {"nifi": stop, "kafka": deleted}


def _sc_cross_lag(fault, flow, kfault, nifi_client, kc, pg_id):
    """Queue backlog on NiFi + lagging empty group on Kafka."""
    _ = flow
    nifi = fault.inject_kafka_stop_log(nifi_client, pg_id, publish=8, settle_sec=4.0)
    kafka = kfault.inject_lag_group(
        kc, topic=DEMO_TOPIC, group_id=FAULT_LAG_GROUP, messages=40
    )
    return {"nifi": nifi, "kafka": kafka}


def run_cross_scenario(
    *,
    name: str,
    spec: dict,
    dry_run: bool,
    publish_after: int,
) -> int:
    from ratatoskr.correlation import run_cross_stack_cycle
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import reset_heal_cooldown as reset_kafka_cd
    from ratatoskr.kafka.policy import run_monitor_cycle as run_kafka
    from ratatoskr.kafka_sources import kafka_reachable
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import reset_heal_cooldown as reset_nifi_cd
    from ratatoskr.nifi.policy import run_monitor_cycle as run_nifi

    fault = _load("nifi_fault_inject", "scripts/nifi_fault_inject.py")
    flow = _load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    sample = _load("nifi_load_sample_flow", "scripts/nifi_load_sample_flow.py")
    kfault = _load("kafka_fault_inject", "scripts/kafka_fault_inject.py")

    if not kafka_reachable():
        print("FAIL  Studio Kafka not up — run: ratatoskr kafka up")
        return 1

    _clear_nifi_env()
    _clear_kafka_env()
    for k, v in (spec.get("env") or {}).items():
        os.environ[k] = str(v)
    os.environ["CROSS_HEAL_DRY_RUN"] = "1" if dry_run else "0"
    os.environ["CROSS_HEAL_PHASE"] = "lab"

    nifi_client, pg_id = _ensure_nifi_baseline(fault, flow, sample)
    kc = KafkaClient()
    try:
        _banner(f"1) Inject cross-stack fault — {name}")
        print(spec.get("blurb", ""))
        injected = spec["inject"](fault, flow, kfault, nifi_client, kc, pg_id)
        print(json.dumps(injected, indent=2, default=str))
        observe = float(spec.get("observe_sec", 2.0))
        if observe:
            print(f"Waiting {observe}s for monitors to see fault…")
            time.sleep(observe)

        _banner("2) Side monitors (detect)")
        reset_nifi_cd()
        reset_kafka_cd()
        nifi_ev = run_nifi(NiFiClient(), pg_id, phase="monitor")
        kafka_ev = run_kafka(kc, phase="monitor")
        _summarize_nifi(nifi_ev)
        _summarize_kafka(kafka_ev)

        want_rules = set(spec.get("expect_rules") or [])
        _banner("3) Cross-stack heal (lab)")
        reset_nifi_cd()
        reset_kafka_cd()
        result = run_cross_stack_cycle(
            nifi_event=nifi_ev,
            kafka_event=kafka_ev,
            phase="lab",
            dry_run=dry_run,
            nifi_pg_id=pg_id,
            nifi_client=NiFiClient(),
            kafka_client=kc,
        )
        print(
            json.dumps(
                {
                    "matched_rules": result.get("matched_rules"),
                    "cross_heal_plan": result.get("cross_heal_plan"),
                    "heal_actions": result.get("heal_actions"),
                    "step_results": [
                        {
                            "step": s.get("step"),
                            "side": s.get("side"),
                            "skipped": s.get("skipped"),
                            "reason": s.get("reason"),
                            "ops": sorted(_ops(s.get("heal_actions") or [])),
                        }
                        for s in (result.get("step_results") or [])
                    ],
                },
                indent=2,
                default=str,
            )
        )
        matched = set(result.get("matched_rules") or [])
        if want_rules and not (want_rules & matched):
            print(f"FAIL  expected rules intersecting {want_rules}, got {matched}")
            return 1
        actions = result.get("heal_actions") or []
        if not actions and not dry_run:
            # dry_run still expects proposals
            pass
        if not actions:
            print("FAIL  no cross heal_actions")
            return 1
        want_ops = set(spec.get("expect_ops") or [])
        got = _ops(actions)
        if want_ops and not (want_ops & got):
            print(f"FAIL  expected ops intersecting {want_ops}, got {got}")
            return 1
        print(f"OK    cross heal ops={sorted(got)}")

        if dry_run:
            _banner("4) Dry-run — restore")
            print(json.dumps(kfault.restore_catalog(kc), indent=2))
            flow.ensure_demo_topic()
            flow.repair_kafka_flow(
                NiFiClient(), pg_id, bootstrap=flow.default_bootstrap()
            )
            return 0

        _banner("4) Verify")
        time.sleep(1.5)
        reset_nifi_cd()
        reset_kafka_cd()
        nifi_v = run_nifi(NiFiClient(), pg_id, phase="monitor")
        kafka_v = run_kafka(kc, phase="monitor")
        _summarize_nifi(nifi_v)
        _summarize_kafka(kafka_v)
        banned = set(spec.get("cleared_severities") or [])
        k_sevs = set(_health(kafka_v).get("severities") or [])
        if banned & k_sevs:
            print(f"FAIL  kafka severities still present: {banned & k_sevs}")
            return 1
        if name == "cross-topic" and DEMO_TOPIC not in kc.list_topics():
            print("FAIL  topic still missing")
            return 1
        # Opportunistic safe pass if Consume still stopped
        if not _health(nifi_v).get("healthy"):
            reset_nifi_cd()
            run_nifi(NiFiClient(), pg_id, phase="safe", dry_run=False)

        if publish_after > 0 and name == "cross-topic":
            _banner("5) Publish smoke")
            from kafka import KafkaProducer

            p = KafkaProducer(bootstrap_servers="localhost:9094")
            for i in range(publish_after):
                p.send(
                    DEMO_TOPIC,
                    json.dumps(
                        {"hello": "cross-healed", "scenario": name, "i": i}
                    ).encode(),
                )
            p.flush()
            p.close()
            print(f"OK    published {publish_after} to {DEMO_TOPIC}")

        _banner(f"PASS — {name}")
        return 0
    finally:
        kc.close()


# --- Scenario registry -------------------------------------------------------

SCENARIOS: dict[str, dict[str, Any]] = {
    "stop-consume": {
        "stack": "nifi",
        "heal_phase": "safe",
        "title": "STOPPED ConsumeKafka → start_processor",
        "blurb": "Operator stops the Kafka consumer processor.",
        "inject": _sc_stop_consume,
        "expect_stopped": ["ConsumeKafka"],
        "expect_severities": ["STOPPED"],
        "expect_ops": ["start_processor"],
        "heal_passes": 2,
    },
    "disable-cs": {
        "stack": "nifi",
        "heal_phase": "safe",
        "title": "DISABLED Studio Kafka CS → enable_controller_service",
        "blurb": "Kafka connection service disabled; consumers go INVALID/STOPPED.",
        "inject": _sc_disable_cs,
        "expect_disabled": ["Studio Kafka"],
        "expect_severities": ["DISABLED_SERVICE"],
        "expect_ops": ["enable_controller_service"],
        "heal_passes": 2,
    },
    "invalid-log": {
        "stack": "nifi",
        "heal_phase": "lab",
        "title": "INVALID LogAttribute → fix_processor_config (templated)",
        "blurb": "Clear auto-terminated relationships → validation INVALID; lab applies template fix.",
        "inject": _sc_invalid_log,
        "expect_invalid": ["LogAttribute"],
        "expect_severities": ["INVALID"],
        "expect_ops": ["fix_processor_config"],
        "heal_passes": 2,
        "skip_extra_if_ops": ["fix_processor_config"],
    },
    "queue-backlog": {
        "stack": "nifi",
        "heal_phase": "lab",
        "title": "Queue backlog → empty_connection_queue + starts",
        "blurb": "Stop LogAttribute and publish so update-to-log backs up.",
        "inject": _sc_queue_backlog,
        "env": {"NIFI_HEAL_ALLOW_EMPTY_QUEUE": "1"},
        "expect_stopped": ["LogAttribute"],
        "expect_queued": True,
        "expect_severities": ["STOPPED", "BACKPRESSURE", "BACKPRESSURE_WARN", "BACKPRESSURE_CRIT"],
        "expect_ops": ["empty_connection_queue", "start_processor", "stop_processor"],
        "heal_passes": 2,
        "settle_sec": 1.0,
    },
    "delete-topic": {
        "stack": "kafka",
        "heal_phase": "safe",
        "title": "TOPIC_MISSING nifi.kafka.demo → create_topic",
        "blurb": "Stop NiFi consumer (avoid auto-create), then delete the shared demo topic.",
        "inject": _sc_delete_topic,
        "stop_nifi_consumer_first": True,
        "expect_severities": ["TOPIC_MISSING"],
        "expect_ops": ["create_topic"],
        "cleared_severities": ["TOPIC_MISSING"],
        "heal_passes": 1,
        "settle_sec": 0.5,
        "observe_sec": 10.0,
    },
    "increase-partitions": {
        "stack": "kafka",
        "heal_phase": "lab",
        "title": "TOPIC_PARTITIONS_LOW → increase_partitions",
        "blurb": "Recreate demo topic with 1 partition while catalog wants 3 (allowlisted).",
        "inject": _sc_undersize,
        "stop_nifi_consumer_first": True,
        "env": {
            "KAFKA_TOPIC_PARTITIONS": "3",
            "KAFKA_HEAL_ALLOW_TOPICS": DEMO_TOPIC,
        },
        "expect_severities": ["TOPIC_PARTITIONS_LOW"],
        "expect_ops": ["increase_partitions"],
        "cleared_severities": [],
        "verify_not_undersized": [DEMO_TOPIC],
        "heal_passes": 1,
        "settle_sec": 1.0,
        "cleanup_partitions": 1,
    },
    "lag-group": {
        "stack": "kafka",
        "heal_phase": "lab",
        "title": "Empty lagging group → delete_group (prefix allow)",
        "blurb": f"Produce + commit earliest on group {FAULT_LAG_GROUP}, then close consumer.",
        "inject": _sc_lag_group,
        "env": {
            "KAFKA_HEAL_ALLOW_GROUP_PREFIXES": "ratatoskr-",
            "KAFKA_LAG_WARN": "10",
            "KAFKA_LAG_CRIT": "100",
        },
        "expect_severities": [
            "LAG_WARN",
            "LAG_CRIT",
            "GROUP_EMPTY",
            "CONSUMER_STALLED",
        ],
        "expect_ops": ["reset_offsets", "delete_group"],
        "cleared_severities": [],
        "heal_passes": 1,
        "settle_sec": 1.0,
    },
    "lag-earliest": {
        "stack": "kafka",
        "heal_phase": "lab",
        "title": "LAG_CRIT → reset_offsets strategy=earliest",
        "blurb": "Build critical lag with a member still attached (no delete_group).",
        "inject": _sc_lag_group,
        "env": {
            "KAFKA_HEAL_ALLOW_GROUPS": FAULT_LAG_GROUP,
            "KAFKA_HEAL_OFFSET_STRATEGY": "earliest",
            "KAFKA_LAG_WARN": "10",
            "KAFKA_LAG_CRIT": "20",
        },
        "expect_severities": [
            "LAG_WARN",
            "LAG_CRIT",
            "GROUP_EMPTY",
            "CONSUMER_STALLED",
        ],
        # empty group prefers delete_group; still accept reset if planned
        "expect_ops": ["reset_offsets", "delete_group"],
        "cleared_severities": [],
        "heal_passes": 1,
        "settle_sec": 1.0,
    },
    "cross-topic": {
        "stack": "cross",
        "heal_phase": "lab",
        "title": "TOPIC_MISSING + STOPPED → create_topic then start ConsumeKafka",
        "blurb": "Stop ConsumeKafka, delete nifi.kafka.demo; cross playbook recreates topic then starts consumer.",
        "inject": _sc_cross_topic,
        "expect_rules": ["kafka_topic_nifi_consumer"],
        "expect_ops": ["create_topic", "start_processor"],
        "cleared_severities": ["TOPIC_MISSING"],
        "observe_sec": 10.0,
    },
    "cross-lag": {
        "stack": "cross",
        "heal_phase": "lab",
        "title": "BACKPRESSURE + LAG → NiFi queue relief (cross playbook)",
        "blurb": "Stop LogAttribute + publish backlog; inject lagging fault group; cross heal drains queue / starts.",
        "inject": _sc_cross_lag,
        "env": {
            "CROSS_HEAL_ALLOW_EMPTY_QUEUE": "1",
            "NIFI_HEAL_ALLOW_EMPTY_QUEUE": "1",
            "KAFKA_LAG_WARN": "10",
            "KAFKA_LAG_CRIT": "100",
        },
        "expect_rules": ["pipeline_backpressure_lag", "nifi_stopped_kafka_lag"],
        "expect_ops": [
            "empty_connection_queue",
            "start_processor",
            "stop_processor",
        ],
        "observe_sec": 2.0,
    },
}


def list_scenarios() -> None:
    print(f"{'scenario':<16} {'stack':<6} {'phase':<6}  title")
    print("-" * 72)
    for key, spec in SCENARIOS.items():
        print(
            f"{key:<16} {spec['stack']:<6} {spec['heal_phase']:<6}  {spec['title']}"
        )


def run_one(name: str, *, dry_run: bool, publish_after: int) -> int:
    spec = SCENARIOS[name]
    _banner(f"Demo scenario: {name}")
    print(spec["title"])
    if spec["stack"] == "nifi":
        return run_nifi_scenario(
            name=name, spec=spec, dry_run=dry_run, publish_after=publish_after
        )
    if spec["stack"] == "cross":
        return run_cross_scenario(
            name=name, spec=spec, dry_run=dry_run, publish_after=publish_after
        )
    return run_kafka_scenario(
        name=name, spec=spec, dry_run=dry_run, publish_after=publish_after
    )


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=tuple(SCENARIOS.keys()),
        default="stop-consume",
        help="Which heal story to run",
    )
    parser.add_argument("--list", action="store_true", help="List scenarios and exit")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run every scenario (restores between)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Propose heals without applying (then restore)",
    )
    parser.add_argument(
        "--publish-after",
        type=int,
        default=2,
        help="Messages to publish after successful NiFi/topic heal (0=skip)",
    )
    args = parser.parse_args()

    if args.list:
        list_scenarios()
        return 0

    names = list(SCENARIOS.keys()) if args.all else [args.scenario]
    failed = 0
    for name in names:
        rc = run_one(name, dry_run=args.dry_run, publish_after=args.publish_after)
        if rc != 0:
            failed += 1
            if not args.all:
                return rc
            print(f"CONTINUE after failure in {name}")
            # Best-effort restore between --all runs
            try:
                flow = _load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
                kfault = _load("kafka_fault_inject", "scripts/kafka_fault_inject.py")
                from ratatoskr.kafka.client import KafkaClient
                from ratatoskr.nifi.client import NiFiClient

                kc = KafkaClient()
                try:
                    kfault.restore_catalog(kc)
                    flow.ensure_demo_topic()
                finally:
                    kc.close()
                client = NiFiClient()
                ensure = flow.ensure_kafka_flow(client, repair=False)
                flow.repair_kafka_flow(
                    client,
                    ensure["process_group_id"],
                    bootstrap=flow.default_bootstrap(),
                )
            except Exception as exc:  # noqa: BLE001
                print(f"restore warning: {exc}")
    if failed:
        print(f"FAILED {failed}/{len(names)} scenarios")
        return 1
    if args.all:
        _banner(f"PASS — all {len(names)} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
