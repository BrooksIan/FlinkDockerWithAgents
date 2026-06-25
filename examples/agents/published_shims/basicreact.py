"""Published designer agent — BasicReAct. Auto-generated; do not edit."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_DEFINITION_ID = "def_a8888ce93ad3"
_MODULE_NAME = f"apemosyne_published_{_DEFINITION_ID}"


def _load_class():
    repo = Path(__file__).resolve().parents[3]
    module_path = repo / ".apemosyne" / "agents" / _DEFINITION_ID / "agent.py"
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load published agent from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return getattr(module, "BasicreactAgent")


BasicreactAgent = _load_class()
