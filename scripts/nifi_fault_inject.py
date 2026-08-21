#!/usr/bin/env python3
"""Inject faults into Ratatoskr NiFi demo flows for heal demos.

Targets:
  sample  — Ratatoskr Sample (Generate→Update→Log)  [default]
  kafka   — Ratatoskr Kafka Demo (ConsumeKafka→Update→Log)

Examples:
  # Sample flow (1B / 1C)
  python scripts/nifi_fault_inject.py --stop-generate
  python scripts/nifi_fault_inject.py --invalid-log
  python scripts/nifi_fault_inject.py --queue-backlog
  python scripts/nifi_fault_inject.py --restore

  # Kafka→NiFi demo flow
  python scripts/nifi_fault_inject.py --target kafka --stop-consume
  python scripts/nifi_fault_inject.py --target kafka --disable-cs
  python scripts/nifi_fault_inject.py --target kafka --stop-log
  python scripts/nifi_fault_inject.py --target kafka --restore
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import time
from pathlib import Path

SAMPLE_PG = "Ratatoskr Sample"
KAFKA_PG = "Ratatoskr Kafka Demo"


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _load_sibling(name: str):
    path = Path(__file__).resolve().parent / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _find_pg(client, name: str) -> str:
    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    pgs = client._request("GET", f"/process-groups/{root_id}/process-groups")
    for ent in (pgs or {}).get("processGroups") or []:
        if (ent.get("component") or {}).get("name") == name:
            return (ent.get("component") or {}).get("id")
    hint = (
        "scripts/nifi_load_sample_flow.sh"
        if name == SAMPLE_PG
        else "scripts/nifi_load_kafka_flow.sh"
    )
    raise RuntimeError(f"{name!r} process group not found — run {hint}")


def _processors_by_name(client, pg_id: str) -> dict[str, dict]:
    out = {}
    for ent in client.list_processors(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
        }
    return out


def _services_by_name(client, pg_id: str) -> dict[str, dict]:
    out = {}
    for ent in client.get_controller_services(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
        }
    return out


def _stop(client, proc: dict, name: str) -> dict:
    client.stop_processor(proc["id"], (proc.get("revision") or {}).get("version"))
    return {"stopped": proc["id"], "name": name}


def _ensure_generate_running(client, procs: dict) -> None:
    gen = procs.get("GenerateFlowFile")
    if not gen:
        return
    if gen.get("state") != "RUNNING":
        client.start_processor(gen["id"], (gen.get("revision") or {}).get("version"))


def inject_invalid_log(client, pg_id: str) -> dict:
    """Clear LogAttribute auto-terminated relationships → VALIDATION INVALID."""
    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    if not log:
        raise SystemExit("LogAttribute not found")
    if log.get("state") != "STOPPED":
        _stop(client, log, "LogAttribute")
        procs = _processors_by_name(client, pg_id)
        log = procs["LogAttribute"]
    client.update_processor_config(log["id"], auto_terminated_relationships=[])
    try:
        procs = _processors_by_name(client, pg_id)
        log = procs["LogAttribute"]
        client.start_processor(log["id"], (log.get("revision") or {}).get("version"))
    except Exception as exc:  # noqa: BLE001
        return {
            "invalid_log": log["id"],
            "name": "LogAttribute",
            "auto_terminated": [],
            "start_error": str(exc),
        }
    return {"invalid_log": log["id"], "name": "LogAttribute", "auto_terminated": []}


def inject_queue_backlog(client, pg_id: str, *, settle_sec: float = 3.0) -> dict:
    """Stop LogAttribute while Generate keeps running → queued connection."""
    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    gen = procs.get("GenerateFlowFile")
    if not log:
        raise SystemExit("LogAttribute not found")
    if gen and gen.get("state") != "STOPPED":
        _stop(client, gen, "GenerateFlowFile")
        procs = _processors_by_name(client, pg_id)
        gen = procs.get("GenerateFlowFile")
    if gen:
        client.update_processor_config(gen["id"], scheduling_period="1 sec")
    if log.get("state") != "STOPPED":
        _stop(client, log, "LogAttribute")
    procs = _processors_by_name(client, pg_id)
    log = procs["LogAttribute"]
    client.update_processor_config(log["id"], auto_terminated_relationships=["success"])
    _ensure_generate_running(client, _processors_by_name(client, pg_id))
    upd = _processors_by_name(client, pg_id).get("UpdateAttribute")
    if upd and upd.get("state") != "RUNNING":
        client.start_processor(upd["id"], (upd.get("revision") or {}).get("version"))
    time.sleep(max(0.0, settle_sec))
    return {
        "queue_backlog": True,
        "stopped": log["id"],
        "name": "LogAttribute",
        "settle_sec": settle_sec,
        "generate_schedule": "1 sec",
    }


def inject_lab_demo(client, pg_id: str) -> dict:
    """Combine queue backlog + INVALID LogAttribute for Phase 1C demos."""
    backlog = inject_queue_backlog(client, pg_id, settle_sec=5.0)
    invalid = inject_invalid_log(client, pg_id)
    time.sleep(1.0)
    return {"lab_demo": True, "backlog": backlog, "invalid": invalid}


def inject_kafka_invalid_log(client, pg_id: str) -> dict:
    """Make Kafka-demo LogAttribute INVALID (lab → terminate_processor)."""
    return inject_invalid_log(client, pg_id)


def restore_log_attribute_config(client, pg_id: str) -> dict:
    """After terminate: restore success auto-terminate + start LogAttribute."""
    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    if not log:
        raise SystemExit("LogAttribute not found")
    if log.get("state") not in ("STOPPED", "DISABLED"):
        try:
            _stop(client, log, "LogAttribute")
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.75)
        procs = _processors_by_name(client, pg_id)
        log = procs["LogAttribute"]
    client.update_processor_config(log["id"], auto_terminated_relationships=["success"])
    # Config PUT bumps revision; terminate/start often 409 if we race.
    time.sleep(0.75)
    last_err: Exception | None = None
    for _ in range(8):
        procs = _processors_by_name(client, pg_id)
        log = procs.get("LogAttribute") or log
        if log.get("state") == "RUNNING":
            return {
                "restored_log": log["id"],
                "auto_terminated": ["success"],
                "state": "RUNNING",
            }
        try:
            client.start_processor(
                log["id"], (log.get("revision") or {}).get("version")
            )
            last_err = None
            break
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(0.75)
    if last_err is not None:
        procs = _processors_by_name(client, pg_id)
        log = procs.get("LogAttribute") or log
        if log.get("state") == "RUNNING":
            return {
                "restored_log": log["id"],
                "auto_terminated": ["success"],
                "state": "RUNNING",
                "note": "start raced but processor is RUNNING",
            }
        raise RuntimeError(
            f"failed to start LogAttribute after config restore: {last_err}"
        ) from last_err
    return {"restored_log": log["id"], "auto_terminated": ["success"]}


def inject_stop_consume(client, pg_id: str) -> dict:
    """Stop ConsumeKafka — Phase 1B safe heal (start_processor)."""
    procs = _processors_by_name(client, pg_id)
    consume = procs.get("ConsumeKafka")
    if not consume:
        raise SystemExit("ConsumeKafka not found — is this the Kafka demo PG?")
    return _stop(client, consume, "ConsumeKafka")


def inject_disable_kafka_cs(client, pg_id: str) -> dict:
    """Disable Studio Kafka CS — Phase 1B safe heal (enable_controller_service)."""
    procs = _processors_by_name(client, pg_id)
    # Stop consumers first so disable is clean.
    for name in ("ConsumeKafka", "UpdateAttribute", "LogAttribute"):
        proc = procs.get(name)
        if proc and proc.get("state") != "STOPPED":
            _stop(client, proc, name)
    # NiFi needs a beat before CS run-status accepts DISABLED.
    for _ in range(10):
        time.sleep(0.5)
        procs = _processors_by_name(client, pg_id)
        if all(
            (procs.get(n) or {}).get("state") == "STOPPED"
            for n in ("ConsumeKafka", "UpdateAttribute", "LogAttribute")
            if n in procs
        ):
            break
    services = _services_by_name(client, pg_id)
    cs = services.get("Studio Kafka")
    if not cs:
        raise SystemExit("Studio Kafka controller service not found")
    if cs.get("state") == "DISABLED":
        return {"disabled": cs["id"], "name": "Studio Kafka", "already": True}
    last_err = None
    for _ in range(6):
        services = _services_by_name(client, pg_id)
        cs = services.get("Studio Kafka") or cs
        try:
            client.disable_controller_service(
                cs["id"], (cs.get("revision") or {}).get("version")
            )
            last_err = None
            break
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(0.75)
    if last_err is not None:
        raise RuntimeError(f"failed to disable Studio Kafka: {last_err}") from last_err
    # Confirm
    for _ in range(10):
        services = _services_by_name(client, pg_id)
        cs = services.get("Studio Kafka") or cs
        if cs.get("state") == "DISABLED":
            return {"disabled": cs["id"], "name": "Studio Kafka"}
        time.sleep(0.5)
    return {
        "disabled": cs["id"],
        "name": "Studio Kafka",
        "state": cs.get("state"),
        "warning": "disable requested but state not yet DISABLED",
    }


def inject_kafka_stop_log(
    client, pg_id: str, *, publish: int = 5, settle_sec: float = 3.0
) -> dict:
    """Stop LogAttribute, publish to nifi.kafka.demo → queue backlog (lab)."""
    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    if not log:
        raise SystemExit("LogAttribute not found")
    if log.get("state") != "STOPPED":
        _stop(client, log, "LogAttribute")
    # Keep Consume + Update running so flowfiles pile up on update-to-log.
    for name in ("ConsumeKafka", "UpdateAttribute"):
        proc = _processors_by_name(client, pg_id).get(name)
        if proc and proc.get("state") != "RUNNING":
            client.start_processor(proc["id"], (proc.get("revision") or {}).get("version"))
    published = 0
    if publish > 0:
        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(bootstrap_servers="localhost:9094")
            for i in range(publish):
                producer.send(
                    "nifi.kafka.demo",
                    f'{{"fault":"queue","i":{i}}}'.encode(),
                )
            producer.flush()
            producer.close()
            published = publish
        except Exception as exc:  # noqa: BLE001
            return {
                "queue_backlog": True,
                "stopped": log["id"],
                "name": "LogAttribute",
                "publish_error": str(exc),
            }
    time.sleep(max(0.0, settle_sec))
    return {
        "queue_backlog": True,
        "stopped": log["id"],
        "name": "LogAttribute",
        "published": published,
        "settle_sec": settle_sec,
    }


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=("sample", "kafka"),
        default="sample",
        help="Which process group to fault (default: sample)",
    )
    parser.add_argument("--stop-generate", action="store_true", help="Stop GenerateFlowFile (sample 1B)")
    parser.add_argument(
        "--invalid-log",
        action="store_true",
        help="Make LogAttribute INVALID (clear auto-terminate success)",
    )
    parser.add_argument(
        "--queue-backlog",
        action="store_true",
        help="Sample: stop LogAttribute while Generate runs",
    )
    parser.add_argument(
        "--lab-demo",
        action="store_true",
        help="Sample: INVALID + queue backlog for Phase 1C",
    )
    parser.add_argument(
        "--stop-consume",
        action="store_true",
        help="Kafka target: stop ConsumeKafka (1B safe heal)",
    )
    parser.add_argument(
        "--disable-cs",
        action="store_true",
        help="Kafka target: disable Studio Kafka controller service (1B)",
    )
    parser.add_argument(
        "--stop-log",
        action="store_true",
        help="Kafka target: stop LogAttribute + publish (queue backlog)",
    )
    parser.add_argument(
        "--kafka-invalid-log",
        action="store_true",
        help="Kafka target: make LogAttribute INVALID (lab terminate)",
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=3.0,
        help="Seconds to wait for queue buildup",
    )
    parser.add_argument(
        "--publish",
        type=int,
        default=5,
        help="Messages to publish for kafka --stop-log",
    )
    parser.add_argument("--restore", action="store_true", help="Repair + start target flow")
    args = parser.parse_args()

    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    pg_name = KAFKA_PG if args.target == "kafka" else SAMPLE_PG
    pg_id = _find_pg(client, pg_name)
    procs = _processors_by_name(client, pg_id)

    if args.target == "kafka":
        if args.stop_consume:
            print(inject_stop_consume(client, pg_id))
            return 0
        if args.disable_cs:
            print(inject_disable_kafka_cs(client, pg_id))
            return 0
        if args.stop_log:
            print(
                inject_kafka_stop_log(
                    client, pg_id, publish=args.publish, settle_sec=args.settle_sec
                )
            )
            return 0
        if args.kafka_invalid_log or args.invalid_log:
            print(inject_kafka_invalid_log(client, pg_id))
            return 0
        if args.restore:
            flow = _load_sibling("nifi_load_kafka_flow")
            print(
                flow.repair_kafka_flow(
                    client, pg_id, bootstrap=flow.default_bootstrap()
                )
            )
            return 0
        if args.stop_generate or args.lab_demo or args.queue_backlog:
            raise SystemExit(
                "sample-only flags used with --target kafka — try "
                "--stop-consume / --disable-cs / --stop-log / --kafka-invalid-log"
            )
        parser.print_help()
        return 1

    # sample target
    if args.stop_generate:
        gen = procs.get("GenerateFlowFile")
        if not gen:
            raise SystemExit("GenerateFlowFile not found in sample flow")
        print(_stop(client, gen, "GenerateFlowFile"))
        return 0

    if args.lab_demo:
        print(inject_lab_demo(client, pg_id))
        return 0

    if args.invalid_log:
        print(inject_invalid_log(client, pg_id))
        return 0

    if args.queue_backlog:
        print(inject_queue_backlog(client, pg_id, settle_sec=args.settle_sec))
        return 0

    if args.restore:
        mod = _load_sibling("nifi_load_sample_flow")
        print(mod.repair_sample_flow(client, pg_id))
        return 0

    if args.stop_consume or args.disable_cs or args.stop_log:
        raise SystemExit("kafka-only flags — re-run with --target kafka")

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
