"""Published designer agent — ReadAPI-ReactThoughts-WriteKafka. Auto-generated; do not edit."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_DEFINITION_ID = "def_722f593495b5"
_MODULE_NAME = f"ratatoskr_published_{_DEFINITION_ID}"


def _load_class():
    repo = Path(__file__).resolve().parents[3]
    module_path = repo / ".ratatoskr" / "agents" / _DEFINITION_ID / "agent.py"
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load published agent from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return getattr(module, "ReadapiReactthoughtsWritekafkaAgent")


ReadapiReactthoughtsWritekafkaAgent = _load_class()
