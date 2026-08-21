#!/usr/bin/env python3
"""Inject faults into the Ratatoskr sample NiFi flow for heal demos."""

from __future__ import annotations

import argparse
import sys
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
    raise RuntimeError("Ratatoskr Sample process group not found — run scripts/nifi_load_sample_flow.sh")


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


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop-generate", action="store_true", help="Stop GenerateFlowFile")
    parser.add_argument("--restore", action="store_true", help="Start all sample processors")
    args = parser.parse_args()

    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    pg_id = _find_sample_pg(client)
    procs = _processors_by_name(client, pg_id)

    if args.stop_generate:
        gen = procs.get("GenerateFlowFile")
        if not gen:
            raise SystemExit("GenerateFlowFile not found in sample flow")
        client.stop_processor(gen["id"], (gen.get("revision") or {}).get("version"))
        print({"stopped": gen["id"], "name": "GenerateFlowFile"})
        return 0

    if args.restore:
        for name, proc in procs.items():
            try:
                client.start_processor(proc["id"], (proc.get("revision") or {}).get("version"))
                print({"started": proc["id"], "name": name})
            except Exception as exc:  # noqa: BLE001
                print({"error": str(exc), "name": name})
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
