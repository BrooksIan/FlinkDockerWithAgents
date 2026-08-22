#!/usr/bin/env python3
"""Create / repair the Ratatoskr Data Plane NiFi flow (schema / route / replay)."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", action="store_true", help="Wait until NiFi API is ready")
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Reconfigure existing Ratatoskr Data Plane + restart live path",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap (default NIFI_KAFKA_BOOTSTRAP or kafka:9092)",
    )
    args = parser.parse_args()

    from ratatoskr.dataplane.flow import ensure_dataplane_flow
    from ratatoskr.nifi.client import NiFiClient

    sample_path = Path(__file__).resolve().parent / "nifi_load_sample_flow.py"
    spec = importlib.util.spec_from_file_location("nifi_load_sample_flow", sample_path)
    sample = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(sample)

    client = NiFiClient()
    if args.wait:
        print("Waiting for NiFi API...")
        sample.wait_ready(client)
    result = ensure_dataplane_flow(
        client, repair=args.repair, bootstrap=args.bootstrap
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
