#!/usr/bin/env python3
"""Verify helper: run ``ratatoskr doctor`` as a subprocess (for verify tiers)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    cmd = [sys.executable, "-m", "ratatoskr.main", "doctor", *sys.argv[1:]]
    return subprocess.run(cmd, cwd=root).returncode


if __name__ == "__main__":
    raise SystemExit(main())
