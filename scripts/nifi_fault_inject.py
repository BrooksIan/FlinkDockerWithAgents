#!/usr/bin/env python3
"""Inject faults into the Ratatoskr sample NiFi flow for heal demos.

Examples:
  python scripts/nifi_fault_inject.py --stop-generate          # 1B safe heal
  python scripts/nifi_fault_inject.py --invalid-log            # INVALID LogAttribute
  python scripts/nifi_fault_inject.py --queue-backlog          # stop LogAttribute, leave Generate running
  python scripts/nifi_fault_inject.py --lab-demo               # INVALID + backlog for 1C
  python scripts/nifi_fault_inject.py --restore
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _find_sample_pg(client):
    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    pgs = client._request("GET", f"/process-groups/{root_id}/process-groups")
    for ent in (pgs or {}).get("processGroups") or []:
        name = (ent.get("component") or {}).get("name")
        if name == "Ratatoskr Sample":
            return (ent.get("component") or {}).get("id")
    raise RuntimeError(
        "Ratatoskr Sample process group not found — run scripts/nifi_load_sample_flow.sh"
    )


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
    # Try start so NiFi emits INVALID / bulletin (may fail — that's the point)
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
    # Speed Generate (often defaults to 1 min) so demos build a queue quickly.
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
    # Keep success auto-terminated so Generate→Update still run; LogAttribute stopped = backlog
    client.update_processor_config(log["id"], auto_terminated_relationships=["success"])
    _ensure_generate_running(client, _processors_by_name(client, pg_id))
    upd = _processors_by_name(client, pg_id).get("UpdateAttribute")
    if upd and upd.get("state") != "RUNNING":
        client.start_processor(upd["id"], (upd.get("revision") or {}).get("version"))
    # Leave LogAttribute STOPPED
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


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop-generate", action="store_true", help="Stop GenerateFlowFile (1B)")
    parser.add_argument(
        "--invalid-log",
        action="store_true",
        help="Make LogAttribute INVALID (clear auto-terminate success)",
    )
    parser.add_argument(
        "--queue-backlog",
        action="store_true",
        help="Stop LogAttribute while Generate runs to build a queue",
    )
    parser.add_argument(
        "--lab-demo",
        action="store_true",
        help="INVALID + queue backlog for Phase 1C (lab heal) demos",
    )
    parser.add_argument(
        "--settle-sec",
        type=float,
        default=3.0,
        help="Seconds to wait for queue buildup (--queue-backlog)",
    )
    parser.add_argument("--restore", action="store_true", help="Repair + start all sample processors")
    args = parser.parse_args()

    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    pg_id = _find_sample_pg(client)
    procs = _processors_by_name(client, pg_id)

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
        # Reuse repair from sample flow loader (same directory sibling).
        import importlib.util

        repair_path = Path(__file__).resolve().parent / "nifi_load_sample_flow.py"
        spec = importlib.util.spec_from_file_location("nifi_load_sample_flow", repair_path)
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)
        print(mod.repair_sample_flow(client, pg_id))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
