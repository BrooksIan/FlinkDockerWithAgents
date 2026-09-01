#!/usr/bin/env python3
"""One-shot live probe against a real Cloudera Manager API (requires CM_* env)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _reexec_with_project_venv() -> None:
    """System python often lacks project deps — prefer repo .venv when present."""
    try:
        import requests  # noqa: F401
    except ImportError:
        import subprocess

        if os.environ.get("RATATOSKR_VENV_REEXEC") == "1":
            print(
                "Missing dependency 'requests' in project venv. Install deps:\n"
                "  .venv/bin/pip install -e .",
                file=sys.stderr,
            )
            raise SystemExit(1)

        venv_py = _repo_root() / ".venv" / "bin" / "python"
        if venv_py.is_file():
            env = os.environ.copy()
            env["RATATOSKR_VENV_REEXEC"] = "1"
            raise SystemExit(subprocess.call([str(venv_py), *sys.argv], env=env))
        print(
            "Missing dependency 'requests'. Install project deps, then retry:\n"
            "  python3 -m venv .venv\n"
            "  .venv/bin/pip install -e .\n"
            "  .venv/bin/python scripts/cm_monitor_live_probe.py",
            file=sys.stderr,
        )
        raise SystemExit(1)


def _bootstrap() -> None:
    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> int:
    _bootstrap()
    try:
        from dotenv import load_dotenv

        load_dotenv(_repo_root() / ".env")
    except ImportError:
        pass

    from ratatoskr.cm import CMClient, run_monitor_cycle
    from ratatoskr.cm.env import cm_api_base, cm_auth_mode, cm_cluster, cm_user, knox_token

    missing = []
    if not cm_api_base():
        missing.append("CM_API_BASE")
    if cm_auth_mode() == "knox":
        if not knox_token():
            missing.append("KNOX_TOKEN")
    else:
        if not cm_user():
            missing.append("CM_USER")
        if not os.environ.get("CM_PASSWORD"):
            missing.append("CM_PASSWORD")
    if missing:
        print(
            "Missing required env for live CM probe: "
            + ", ".join(missing),
            file=sys.stderr,
        )
        print(
            "\nDirect CM example:\n"
            "  export CM_API_BASE=https://cm.example.com:7183\n"
            "  export CM_USER=admin\n"
            "  export CM_PASSWORD=...\n"
            "  export CM_CLUSTER=my-cluster   # optional if only one cluster\n"
            "\nCDP Knox example:\n"
            "  export CM_API_BASE=https://<gateway>/<env>/cdp-proxy-token/cm-api\n"
            "  export KNOX_TOKEN=<jwt>\n"
            "  export CM_CLUSTER=my-cluster   # optional\n"
            "  python scripts/cm_monitor_live_probe.py",
            file=sys.stderr,
        )
        return 2

    cluster = os.environ.get("CM_CLUSTER") or cm_cluster() or None
    client = CMClient(cluster=cluster or "")
    result = run_monitor_cycle(client, cluster=cluster)
    print(json.dumps(result, indent=2, default=str))
    probe = (result.get("health") or {}).get("probe") or {}
    if not probe.get("ok"):
        return 1
    print(
        f"\nOK cluster={result.get('health', {}).get('cluster')} "
        f"score={result.get('classification', {}).get('score')} "
        f"recommendations={len(result.get('recommendations') or [])}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    _reexec_with_project_venv()
    raise SystemExit(main())
