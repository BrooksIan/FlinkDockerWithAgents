"""Bytecode-backed module (source pending recovery)."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _boot() -> None:
    shim_paths = (
        Path("/opt/flink/_pyc_shim.py"),
        Path(__file__).resolve().parents[1] / "_pyc_shim.py",
    )
    for shim_path in shim_paths:
        if not shim_path.is_file():
            continue
        spec = importlib.util.spec_from_file_location("_pyc_shim", shim_path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.load_sibling_pyc(globals())
        return
    raise ImportError("Cannot load _pyc_shim.py for bytecode-backed module")


_boot()
