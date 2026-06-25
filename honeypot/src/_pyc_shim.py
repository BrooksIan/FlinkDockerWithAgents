"""Load a sibling ``__pycache__/*.cpython-312.pyc`` when source is not yet recovered."""

from __future__ import annotations

import sys
from importlib.machinery import SourcelessFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

_PYC_SUBDIRS = ("react", "core", "pipeline", "integrations", "services", "traps", "demo")


def resolve_sibling_pyc(module_file: str | Path) -> Path:
    """Find bytecode for a shim module (repo tree or flat ``/opt/flink`` mounts)."""
    path = Path(module_file).resolve()
    stem = path.stem
    name = f"{stem}.cpython-312.pyc"
    candidates = [
        path.parent / "__pycache__" / name,
        Path("/opt/flink/__pycache__") / name,
    ]
    for sub in _PYC_SUBDIRS:
        candidates.append(Path(f"/opt/flink/pyc/{sub}") / name)
    for pyc in candidates:
        if pyc.is_file():
            return pyc
    tried = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Bytecode not found for {stem} (tried: {tried})")


def load_sibling_pyc(module_globals: dict) -> None:
    """Execute bytecode for the calling shim and merge public names into ``module_globals``."""
    module_file = module_globals["__file__"]
    path = Path(module_file).resolve()
    stem = path.stem
    pyc = resolve_sibling_pyc(module_file)
    loader = SourcelessFileLoader(stem, str(pyc))
    spec = spec_from_loader(stem, loader)
    if spec is None:
        raise ImportError(f"Cannot load {pyc}")
    mod = module_from_spec(spec)
    sys.modules[stem] = mod
    loader.exec_module(mod)
    for key, value in mod.__dict__.items():
        if key.startswith("__"):
            continue
        module_globals[key] = value


def run_sibling_pyc_main(module_file: str) -> None:
    """Run ``if __name__ == '__main__'`` block from sibling bytecode."""
    pyc = resolve_sibling_pyc(module_file)
    loader = SourcelessFileLoader("__main__", str(pyc))
    spec = spec_from_loader("__main__", loader)
    if spec is None:
        raise ImportError(f"Cannot load {pyc}")
    mod = module_from_spec(spec)
    loader.exec_module(mod)
