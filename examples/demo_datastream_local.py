#!/usr/bin/env python3
"""Host-only Flink Agents import smoke (no Docker required)."""

from __future__ import annotations

import sys


def main() -> int:
    print("Apemosyne local demo smoke")
    try:
        import flink_agents  # noqa: F401
    except ImportError:
        print("SKIP flink_agents not installed on host (expected outside container)")
        return 0
    print("OK flink_agents import")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
