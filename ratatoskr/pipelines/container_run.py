#!/usr/bin/env python3
"""Run a pipeline inside the Flink JobManager container (JSON payload path as argv[1])."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path


def _bootstrap() -> None:
    repo = Path("/opt/flink")
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> int:
    _bootstrap()
    from ratatoskr.pipelines.executor import execute_pipeline_agents
    from ratatoskr.pipelines.models import pipeline_from_dict

    payload_path = Path(sys.argv[1])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    pipeline = pipeline_from_dict(payload["pipeline"])
    output, steps = execute_pipeline_agents(
        pipeline,
        input_override=payload.get("input_override"),
        deliver_sinks=False,
    )
    print(
        json.dumps(
            {
                "output": output,
                "steps": [asdict(step) for step in steps],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
