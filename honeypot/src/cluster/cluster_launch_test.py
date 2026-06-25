"""Bootstrap Flink cluster runtime and run the launch smoke job (honeypot shim)."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_repo_on_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "apemosyne" / "runtime").is_dir():
            root = str(parent)
            if root not in sys.path:
                sys.path.insert(0, root)
            return


_ensure_repo_on_path()

from apemosyne.runtime.cluster_launch_test import bootstrap_runtime, run_cluster_launch

__all__ = ["bootstrap_runtime", "run_cluster_launch"]

if __name__ == "__main__":
    raise SystemExit(run_cluster_launch())
