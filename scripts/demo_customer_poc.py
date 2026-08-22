#!/usr/bin/env python3
"""Customer POC demo (10–15 min): break → monitor → propose → ack → apply → verify.

Narrates the data-plane agent loop on the Ratatoskr Data Plane spine
(schema gate + route/enrich + approval bus). Does not run heal-lab ops.

Prereqs:
  ratatoskr kafka up
  ratatoskr up --profile nifi
  ./scripts/nifi_load_dataplane_flow.sh

Usage:
  python3 scripts/demo_customer_poc.py
  python3 scripts/demo_customer_poc.py --pause          # wait for Enter between steps
  python3 scripts/demo_customer_poc.py --dry-run-apply  # ack+apply without writing NiFi
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _banner(title: str) -> None:
    print()
    print("=" * 64)
    print(title)
    print("=" * 64)


def _say(msg: str) -> None:
    print(msg, flush=True)


def _pause(enabled: bool, hint: str) -> None:
    if not enabled:
        return
    input(f"\n[pause] {hint} — press Enter to continue… ")


def _pp(label: str, obj: Any) -> None:
    print(f"\n--- {label} ---")
    if not isinstance(obj, dict):
        print(json.dumps(obj, indent=2, default=str))
        return
    slim: dict[str, Any] = {}
    for k in (
        "phase",
        "action",
        "target",
        "classification",
        "matched_rules",
        "ok",
        "headline",
        "summary",
        "likely_cause",
        "suggested_next_steps",
        "mode",
        "valid",
        "violations",
    ):
        if k in obj:
            slim[k] = obj.get(k)
    if "proposal" in obj and isinstance(obj["proposal"], dict):
        slim["proposal_id"] = obj["proposal"].get("proposal_id")
        slim["plan"] = obj["proposal"].get("plan")
    if "apply" in obj:
        slim["apply"] = {
            "ok": (obj["apply"] or {}).get("ok"),
            "dry_run": (obj["apply"] or {}).get("dry_run"),
            "actions": (obj["apply"] or {}).get("actions"),
        }
    if "incidents" in obj:
        slim["incidents"] = [
            {"rule": i.get("rule"), "level": i.get("level"), "title": i.get("title")}
            for i in (obj.get("incidents") or [])
        ]
    print(json.dumps(slim if slim else obj, indent=2, default=str))


def _publish(topic: str, payloads: list[dict]) -> None:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for p in payloads:
            producer.send(topic, p)
        producer.flush()
    finally:
        producer.close()


def _break_route_env(client: Any, pg_id: str) -> dict[str, Any]:
    """Force EnrichUpdate ratatoskr.env to a bad value (creates ROUTE_DRIFT)."""
    from ratatoskr.dataplane.flow import processors_by_name

    procs = processors_by_name(client, pg_id)
    enrich = procs.get("EnrichUpdate")
    if not enrich:
        raise RuntimeError("EnrichUpdate not found — load dataplane flow first")
    if enrich.get("state") not in ("STOPPED", "DISABLED"):
        client.stop_processor(enrich["id"])
        time.sleep(0.4)
    client.update_processor_config(
        enrich["id"],
        properties={"ratatoskr.env": "broken-poc"},
    )
    time.sleep(0.3)
    client.start_processor(enrich["id"])
    return {"op": "break_route_env", "ratatoskr.env": "broken-poc", "ok": True}


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pause",
        action="store_true",
        help="Wait for Enter between steps (live narration)",
    )
    parser.add_argument(
        "--dry-run-apply",
        action="store_true",
        help="Propose/ack/apply with dry_run (no NiFi property write)",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Do not ensure dataplane flow/topics",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=8.0,
        help="Seconds to wait for NiFi after publishing events",
    )
    args = parser.parse_args()

    from examples.agents.react_incident_scribe_logic import fallback_scribe
    from ratatoskr.correlation import run_correlate_cycle
    from ratatoskr.dataplane.bus import run_approval_cycle
    from ratatoskr.dataplane.flow import ensure_dataplane_flow, find_dataplane_pg_id
    from ratatoskr.dataplane.topics import TOPIC_RAW, TOPIC_VALID, TOPIC_VIOLATIONS
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.routing import run_route_enrich_cycle
    from ratatoskr.schema import run_schema_gate_cycle
    from ratatoskr.schema.policy import topic_approx_count

    desired_rule = {
        "match": {"type": "order"},
        "set": {"env": "customer-poc", "pipeline": "dataplane"},
        "route": "enriched",
    }

    _banner("Customer POC — Ratatoskr data-plane agents")
    _say(
        "Story: bad events + drifted enrich config → agents monitor → "
        "propose fix on Kafka → human ack → apply → verify."
    )
    _say("Heal (start/stop processors) is intentionally out of this path.")

    if not args.skip_load:
        _banner("0. Ensure lab spine")
        _say("Loading Ratatoskr Data Plane (topics + ValidateJson path)…")
        print(ensure_dataplane_flow(NiFiClient()))
    else:
        from ratatoskr.dataplane.flow import ensure_dataplane_topics

        ensure_dataplane_topics()

    nifi = NiFiClient()
    pg_id = find_dataplane_pg_id(nifi)
    if not pg_id:
        _say("ERROR: dataplane process group missing — run ./scripts/nifi_load_dataplane_flow.sh")
        return 1

    # --- BREAK ---
    _banner("1. BREAK")
    _say("Inject route drift (ratatoskr.env=broken-poc) and publish valid + invalid events.")
    print(_break_route_env(nifi, pg_id))
    events = [
        {"id": "poc-1", "type": "order", "payload": {"sku": "DEMO", "qty": 1}},
        {"id": "poc-2", "type": "order", "payload": {"sku": "DEMO", "qty": 2}},
        {"id": 99, "payload": "not-an-object"},  # schema violation
    ]
    _publish(TOPIC_RAW, events)
    _say(f"Published {len(events)} messages to {TOPIC_RAW}; waiting {args.wait}s for NiFi…")
    time.sleep(args.wait)
    counts = {
        "valid": topic_approx_count(TOPIC_VALID),
        "violations": topic_approx_count(TOPIC_VIOLATIONS),
    }
    _pp("topic counts after break", counts)
    _pause(args.pause, "Show NiFi UI / topics if desired")

    # --- MONITOR ---
    _banner("2. MONITOR (no mutations)")
    schema_mon = run_schema_gate_cycle(phase="monitor")
    route_mon = run_route_enrich_cycle(phase="monitor", rule=desired_rule)
    _pp("schema gate", schema_mon)
    _pp("route enrich", route_mon)

    corr = run_correlate_cycle(
        nifi_event={
            "agent": "workflow_nifi_monitor",
            "classification": {
                "healthy": True,
                "level": "OK",
                "score": 100,
                "severities": [],
            },
        },
        kafka_event={
            "agent": "workflow_kafka_monitor",
            "classification": {
                "healthy": True,
                "level": "OK",
                "score": 100,
                "severities": [],
            },
        },
        schema_event=schema_mon,
        route_event=route_mon,
    )
    _pp("correlate", corr)
    brief = fallback_scribe(corr)
    _pp("incident scribe (explain-only)", brief)
    _pause(args.pause, "Call out SCHEMA_VIOLATIONS / ROUTE_DRIFT — still zero writes")

    # --- PROPOSE → ACK → APPLY ---
    _banner("3. PROPOSE → ACK → APPLY (approval bus)")
    _say(
        "Publishing plan to dataplane.propose, ack on dataplane.ack, "
        f"then apply ({'dry-run' if args.dry_run_apply else 'live config_apply'})."
    )
    approval = run_approval_cycle(
        action="propose_ack_apply",
        target="route",
        dry_run=args.dry_run_apply,
        phase_on_apply="safe",
        rule=desired_rule,
    )
    _pp("approval cycle", approval)
    if approval.get("error"):
        _say(f"ERROR: {approval['error']}")
        return 1
    _pause(args.pause, "Show Kafka topics dataplane.propose / dataplane.ack if useful")

    # --- VERIFY ---
    _banner("4. VERIFY")
    route_after = run_route_enrich_cycle(phase="monitor", rule=desired_rule)
    schema_after = run_schema_gate_cycle(phase="monitor")
    _pp("route after", route_after)
    _pp(
        "schema after",
        {
            "classification": schema_after.get("classification"),
            "violations": (schema_after.get("health") or {}).get("violations"),
            "valid": (schema_after.get("health") or {}).get("valid"),
        },
    )

    route_ok = bool((route_after.get("classification") or {}).get("healthy"))
    if args.dry_run_apply:
        _say(
            "VERIFY note: dry-run apply — route may still show drift "
            "(expected). Re-run without --dry-run-apply for a clean verify."
        )
        route_ok = True  # do not fail the scripted dry-run path
    elif not route_ok:
        _say("WARN: route still drifted after apply")
        return 1

    viol = int(
        ((schema_after.get("health") or {}).get("violations") or {}).get("count") or 0
    )
    valid = int(((schema_after.get("health") or {}).get("valid") or {}).get("count") or 0)
    if valid < 1 or viol < 1:
        _say(
            "WARN: expected both valid and violation traffic "
            f"(valid={valid}, violations={viol})"
        )

    _banner("POC complete")
    _say("Talking points:")
    _say("  • monitor never mutates")
    _say("  • schema gate quarantines bad events (no processor heal)")
    _say("  • desired-state changes go propose → ack → apply")
    _say("  • correlate + scribe explain; heal is a separate demo if needed")
    _say("")
    _say("Optional follow-ups:")
    _say("  python3 scripts/demo_nifi_kafka_heal.py --list")
    _say("  docs/CUSTOMER_POC.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
