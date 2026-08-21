#!/usr/bin/env python3
"""Create / repair the Ratatoskr sample NiFi flow (GenerateFlowFile → UpdateAttribute → LogAttribute)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def wait_ready(client, attempts: int = 40, delay: float = 5.0) -> None:
    last = None
    for i in range(attempts):
        try:
            client.get_nifi_version()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            msg = str(exc)
            if "401" in msg or "Unauthorized" in msg:
                raise RuntimeError(
                    "NiFi API returned 401 Unauthorized. Credentials do not match "
                    "the running container.\n\n"
                    "Defaults are admin / RatatoskrNiFi1! from nifi/docker-compose.yml "
                    "(or NIFI_USERNAME / NIFI_PASSWORD in .env).\n"
                    "Single-user password is set only when NiFi volumes are first created.\n\n"
                    "Fix:\n"
                    "  1. Check docker logs for a generated password:\n"
                    "       docker logs deploy-nifi-1 2>&1 | grep -i 'Generated Password'\n"
                    "  2. Or reset volumes and recreate with known defaults:\n"
                    "       docker compose -f deploy/docker-compose.yml -f nifi/docker-compose.yml down -v\n"
                    "       ratatoskr up --profile nifi\n"
                    "       ./scripts/nifi_load_sample_flow.sh\n"
                    f"\nOriginal error: {exc}"
                ) from exc
            if i == 0 or (i + 1) % 6 == 0:
                print(f"  still waiting ({i + 1}/{attempts}): {exc}")
            time.sleep(delay)
    raise RuntimeError(f"NiFi API not ready after {attempts} attempts: {last}")


def _find_sample_pg(client, root_id: str) -> str | None:
    pgs = client._request("GET", f"/process-groups/{root_id}/process-groups")
    for ent in (pgs or {}).get("processGroups") or []:
        name = (ent.get("component") or {}).get("name")
        if name == "Ratatoskr Sample":
            return (ent.get("component") or {}).get("id")
    return None


def _processors_by_name(client, pg_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for ent in client.list_processors(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
            "entity": ent,
        }
    return out


def repair_sample_flow(client, pg_id: str) -> dict:
    """Auto-terminate LogAttribute success relationship and restart processors."""
    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    if not log or not log.get("id"):
        raise RuntimeError("LogAttribute not found in Ratatoskr Sample")

    # Must be stopped to update config
    if log.get("state") != "STOPPED":
        client.stop_processor(log["id"], (log.get("revision") or {}).get("version"))

    client.update_processor_config(
        log["id"],
        auto_terminated_relationships=["success"],
    )

    # Restart all sample processors
    procs = _processors_by_name(client, pg_id)
    started = []
    for name, proc in procs.items():
        try:
            client.start_processor(proc["id"], (proc.get("revision") or {}).get("version"))
            started.append(name)
        except Exception as exc:  # noqa: BLE001
            started.append(f"{name}:ERROR:{exc}")

    return {"process_group_id": pg_id, "repaired": True, "started": started}


def ensure_sample_flow(client, *, repair: bool = False) -> dict:
    from ratatoskr.nifi.client import NiFiClient

    assert isinstance(client, NiFiClient)
    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    if not root_id:
        raise RuntimeError("Could not resolve root process group id")

    existing = _find_sample_pg(client, root_id)
    if existing:
        if repair:
            return repair_sample_flow(client, existing)
        return {"process_group_id": existing, "created": False}

    pg = client.create_process_group(root_id, "Ratatoskr Sample", x=0, y=0)
    pg_id = (pg.get("component") or {}).get("id") or pg.get("id")
    if not pg_id:
        raise RuntimeError(f"Failed to create process group: {pg}")

    gen = client.create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        "GenerateFlowFile",
        x=100,
        y=100,
        properties={"File Size": "1B", "Batch Size": "1"},
    )
    upd = client.create_processor(
        pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "UpdateAttribute",
        x=400,
        y=100,
        properties={"ratatoskr.demo": "true"},
    )
    # Terminal processor: auto-terminate success or NiFi marks it INVALID.
    log = client.create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.LogAttribute",
        "LogAttribute",
        x=700,
        y=100,
        properties={},
        auto_terminated_relationships=["success"],
    )

    gen_id = (gen.get("component") or {}).get("id")
    upd_id = (upd.get("component") or {}).get("id")
    log_id = (log.get("component") or {}).get("id")

    client.create_connection(
        pg_id, gen_id, "PROCESSOR", upd_id, "PROCESSOR", ["success"], name="gen-to-update"
    )
    client.create_connection(
        pg_id, upd_id, "PROCESSOR", log_id, "PROCESSOR", ["success"], name="update-to-log"
    )

    for pid in (gen_id, upd_id, log_id):
        try:
            client.start_processor(pid)
        except Exception:
            pass

    return {
        "process_group_id": pg_id,
        "created": True,
        "processors": {"generate": gen_id, "update": upd_id, "log": log_id},
    }


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", action="store_true", help="Wait until NiFi API is ready")
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Fix existing Ratatoskr Sample (auto-terminate LogAttribute success + restart)",
    )
    args = parser.parse_args()

    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    if args.wait:
        print("Waiting for NiFi API...")
        wait_ready(client)
    result = ensure_sample_flow(client, repair=args.repair)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
