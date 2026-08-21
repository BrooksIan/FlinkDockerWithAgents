#!/usr/bin/env python3
"""End-to-end demo: break the Kafka→NiFi flow, monitor, then heal.

Story (default scenario ``stop-consume``):
  1. Ensure Ratatoskr Kafka Demo is healthy
  2. Stop ConsumeKafka (something goes wrong)
  3. Phase monitor — detect STOPPED, no mutations
  4. Phase safe — start_processor heal
  5. Verify RUNNING + optional publish

Prereqs:
  ratatoskr kafka up
  ratatoskr up --profile nifi
  ./scripts/nifi_load_kafka_flow.sh   # once

Usage:
  python3 scripts/demo_nifi_kafka_heal.py
  python3 scripts/demo_nifi_kafka_heal.py --scenario disable-cs
  python3 scripts/demo_nifi_kafka_heal.py --dry-run   # propose heals only
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


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


def _summarize(result: dict) -> None:
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
                "stopped": [
                    p.get("name") for p in (health.get("stopped_processors") or [])
                ],
                "invalid": [
                    p.get("name") for p in (health.get("invalid_processors") or [])
                ],
                "disabled_services": [
                    s.get("name")
                    for s in (health.get("disabled_controller_services") or [])
                ],
                "heal_actions": result.get("heal_actions") or [],
                "dry_run": audit.get("dry_run"),
            },
            indent=2,
            default=str,
        )
    )


def run(*, scenario: str, dry_run: bool, publish_after: int) -> int:
    _bootstrap()
    from ratatoskr.kafka_sources import kafka_reachable
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import reset_heal_cooldown, run_monitor_cycle

    fault = _load("nifi_fault_inject", "scripts/nifi_fault_inject.py")
    flow = _load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    sample = _load("nifi_load_sample_flow", "scripts/nifi_load_sample_flow.py")

    _banner("Demo: NiFi ← Kafka monitor + heal")
    if not kafka_reachable():
        print("FAIL  Studio Kafka not up — run: ratatoskr kafka up")
        return 1

    client = NiFiClient()
    sample.wait_ready(client, attempts=12, delay=2.0)

    # Clear noisy env for a clean demo; scope heal to this PG via process_group_id.
    for key in (
        "NIFI_HEAL_ALLOW_EMPTY_QUEUE",
        "NIFI_HEAL_ALLOW_IDS",
        "NIFI_HEAL_ALLOW_NAME_REGEX",
        "NIFI_WATCH_NAME_REGEX",
        "NIFI_WATCH_ID_REGEX",
    ):
        os.environ.pop(key, None)
    os.environ["NIFI_HEAL_DRY_RUN"] = "1" if dry_run else "0"
    os.environ["NIFI_HEAL_VERIFY"] = "1"
    reset_heal_cooldown()

    ensure = flow.ensure_kafka_flow(client, repair=False)
    pg_id = ensure["process_group_id"]
    baseline = client.get_flow_health_status(pg_id)
    if not baseline.get("healthy"):
        print("Baseline unhealthy — repairing Kafka demo flow")
        ensure = flow.repair_kafka_flow(
            client, pg_id, bootstrap=flow.default_bootstrap()
        )
        time.sleep(1.0)
        baseline = client.get_flow_health_status(pg_id)
    print(f"Baseline PG={pg_id} topic={flow.DEMO_TOPIC} healthy={baseline.get('healthy')}")
    if not baseline.get("healthy"):
        print("FAIL  could not establish healthy baseline")
        print(json.dumps(baseline.get("severities"), indent=2))
        return 1
    print(json.dumps({"ensure_created": ensure.get("created"), "bootstrap": ensure.get("bootstrap") or flow.default_bootstrap()}, indent=2))

    _banner("1) Inject fault")
    if scenario == "stop-consume":
        injected = fault.inject_stop_consume(client, pg_id)
        expect_sev = "STOPPED"
        expect_name = "ConsumeKafka"
    elif scenario == "disable-cs":
        injected = fault.inject_disable_kafka_cs(client, pg_id)
        expect_sev = "DISABLED_SERVICE"
        expect_name = "Studio Kafka"
    else:
        raise SystemExit(f"unknown scenario: {scenario}")
    print(json.dumps(injected, indent=2))
    time.sleep(1.0)

    _banner("2) Monitor phase (detect only — no mutations)")
    reset_heal_cooldown()
    detect = run_monitor_cycle(client, pg_id, phase="monitor")
    _summarize(detect)
    health = _health(detect)
    stopped_names = {p.get("name") for p in (health.get("stopped_processors") or [])}
    disabled_names = {
        s.get("name") for s in (health.get("disabled_controller_services") or [])
    }
    sevs = set(health.get("severities") or [])
    fault_visible = expect_name in stopped_names or expect_name in disabled_names
    if not fault_visible and expect_sev not in sevs:
        print(f"FAIL  expected {expect_name} fault not visible in monitor snapshot")
        return 1
    if detect.get("heal_actions"):
        print("FAIL  monitor phase must not mutate")
        return 1
    print(f"OK    detected fault on {expect_name} (phase=monitor, no heal)")

    _banner("3) Safe phase (heal)")
    reset_heal_cooldown()
    # Fresh client so mutations list is clean
    client2 = NiFiClient()
    heal = run_monitor_cycle(client2, pg_id, phase="safe", dry_run=dry_run)
    _summarize(heal)
    actions = list(heal.get("heal_actions") or [])
    # enable_controller_service then start_processor often needs a second pass
    # (ConsumeKafka stays INVALID until the CS is ENABLED).
    if not dry_run and not _health(heal).get("healthy"):
        time.sleep(1.5)
        reset_heal_cooldown()
        heal2 = run_monitor_cycle(NiFiClient(), pg_id, phase="safe", dry_run=False)
        print("--- second safe pass (enable→start ordering) ---")
        _summarize(heal2)
        actions.extend(heal2.get("heal_actions") or [])
        heal = heal2
    if not actions:
        print("FAIL  safe phase proposed/applied no heal_actions")
        return 1
    print(f"OK    heal_actions={len(actions)} dry_run={dry_run}")

    if dry_run:
        _banner("4) Dry-run only — restoring flow")
        flow.repair_kafka_flow(client2, pg_id, bootstrap=flow.default_bootstrap())
        print("OK    restored (dry-run demo does not leave fault in place)")
        return 0

    _banner("4) Verify healed")
    time.sleep(1.0)
    reset_heal_cooldown()
    verify = run_monitor_cycle(NiFiClient(), pg_id, phase="monitor")
    _summarize(verify)
    vhealth = _health(verify)
    if not vhealth.get("healthy"):
        print("FAIL  process group still unhealthy after safe heal")
        return 1
    print("OK    process group healthy after heal")

    if publish_after > 0:
        _banner("5) Publish smoke after heal")
        from kafka import KafkaProducer

        marker = f"healed-{int(time.time())}"
        p = KafkaProducer(bootstrap_servers="localhost:9094")
        for i in range(publish_after):
            p.send(
                flow.DEMO_TOPIC,
                json.dumps({"hello": "healed", "marker": marker, "i": i}).encode(),
            )
        p.flush()
        p.close()
        time.sleep(2.0)
        snap_health = NiFiClient().get_flow_health_status(pg_id)
        if not snap_health.get("healthy"):
            print("FAIL  unhealthy after publish")
            return 1
        print(f"OK    published {publish_after} msgs marker={marker}; still healthy")

    _banner("PASS — monitor detected fault, safe phase healed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("stop-consume", "disable-cs"),
        default="stop-consume",
        help="Fault to inject (default: stop-consume)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Propose safe heals without applying (then restore)",
    )
    parser.add_argument(
        "--publish-after",
        type=int,
        default=2,
        help="Messages to publish after successful heal (0=skip)",
    )
    args = parser.parse_args()
    return run(
        scenario=args.scenario,
        dry_run=args.dry_run,
        publish_after=args.publish_after,
    )


if __name__ == "__main__":
    raise SystemExit(main())
