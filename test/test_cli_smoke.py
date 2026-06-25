#!/usr/bin/env python3
"""Generic workspace smoke test — no Docker, no honeypot modules required."""

from __future__ import annotations

import sys
from pathlib import Path


def _check_paths(root: Path) -> list[str]:
    required = [
        (root / "docker-compose.yml", "minimal Flink compose"),
        (root / "apemosyne/manifests/startup-modes.yaml", "startup modes"),
        (root / "apemosyne/manifests/verify-tiers.yaml", "verify tiers"),
        (root / "apemosyne/manifests/demo-files.yaml", "demo catalog"),
        (root / "examples/demo_datastream.py", "datastream demo"),
    ]
    missing = [label for path, label in required if not path.is_file()]
    return missing


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    print("=" * 60)
    print("Apemosyne workspace smoke test")
    print("=" * 60)

    missing = _check_paths(root)
    if missing:
        for label in missing:
            print(f"FAIL missing {label}")
        return 1
    print("OK  workspace layout")

    from apemosyne._bootstrap import install_aliases

    install_aliases()
    from apemosyne import paths
    from apemosyne.manifests import load_demo_catalog, load_verify_tiers
    from apemosyne.startup_modes import load_startup_modes

    assert paths.manifests_dir().is_dir()
    catalog = load_demo_catalog(root=root)
    assert "datastream" in catalog.choices
    print(f"OK  demo catalog ({len(catalog.choices)} demos)")

    modes = load_startup_modes(root=root)
    assert modes.default in modes.modes
    print(f"OK  startup modes (default={modes.default})")

    tiers = load_verify_tiers(root=root)
    assert "quick" in tiers
    print(f"OK  verify tiers ({', '.join(sorted(tiers))})")

    print("=" * 60)
    print("PASS (smoke)")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
