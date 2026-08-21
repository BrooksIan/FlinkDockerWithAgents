#!/usr/bin/env python3
"""Local runner for ``react_incident_scribe`` (explain-only).

Chains demo correlation → scribe by default.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> int:
    _bootstrap()
    from examples.agents.react_incident_scribe_logic import scribe_incident
    from examples.agents.run_workflow_signal_correlate_local import _demo_events
    from ratatoskr.correlation import run_correlate_cycle

    nifi, kafka = _demo_events()
    correlation = run_correlate_cycle(nifi_event=nifi, kafka_event=kafka)
    brief = scribe_incident(correlation)
    print("Incident scribe results:")
    print(json.dumps(brief, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
