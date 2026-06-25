#!/usr/bin/env python3
"""Flink Agents launch smoke test (Docker image or Flink cluster TaskManager)."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _bootstrap_paths() -> None:
    root = Path("/opt/flink")
    if root.is_dir():
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        return

    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    try:
        from apemosyne._bootstrap import install_aliases
        from apemosyne.paths import configure_runtime_sys_path

        install_aliases()
        configure_runtime_sys_path(repo)
    except ImportError:
        hp_src = repo / "honeypot" / "src"
        for sub in (
            "cluster",
            "core",
            "pipeline",
            "react",
            "integrations",
            "traps",
            "services",
        ):
            path = hp_src / sub
            if path.is_dir() and str(path) not in sys.path:
                sys.path.insert(0, str(path))


def _smoke_import() -> None:
    import flink_agents  # noqa: F401

    print("OK  flink_agents import")


def _smoke_pyflink() -> None:
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    print("OK  pyflink StreamExecutionEnvironment")


def _run_cluster() -> int:
    import cluster_launch_test

    return cluster_launch_test.run_cluster_launch()


def main() -> int:
    _bootstrap_paths()
    parser = argparse.ArgumentParser(description="Flink Agents launch smoke test")
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="Submit a minimal PyFlink job to the Flink cluster",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Flink Agents launch smoke test")
    print("=" * 60)
    try:
        _smoke_import()
        if args.cluster:
            rc = _run_cluster()
            if rc == 0:
                print("=" * 60)
                print("PASS (cluster launch)")
                print("=" * 60)
            return rc
        _smoke_pyflink()
        print("=" * 60)
        print("PASS (launch smoke)")
        print("=" * 60)
        return 0
    except Exception as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
