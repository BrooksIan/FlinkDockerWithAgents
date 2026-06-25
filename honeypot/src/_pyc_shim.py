"""Load a sibling ``__pycache__/*.cpython-312.pyc`` when source is not yet recovered."""

from __future__ import annotations

import sys
from importlib.machinery import SourcelessFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path


def load_sibling_pyc(module_globals: dict) -> None:
    """Execute bytecode next to the calling shim and merge public names into ``module_globals``."""
    here = Path(module_globals["__file__"]).resolve().parent
    stem = Path(module_globals["__file__"]).stem
    pyc = here / "__pycache__" / f"{stem}.cpython-312.pyc"
    if not pyc.is_file():
        raise FileNotFoundError(f"Bytecode not found for {stem}: {pyc}")
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
    here = Path(module_file).resolve().parent
    stem = Path(module_file).stem
    pyc = here / "__pycache__" / f"{stem}.cpython-312.pyc"
    if not pyc.is_file():
        raise FileNotFoundError(pyc)
    loader = SourcelessFileLoader("__main__", str(pyc))
    spec = spec_from_loader("__main__", loader)
    if spec is None:
        raise ImportError(f"Cannot load {pyc}")
    mod = module_from_spec(spec)
    loader.exec_module(mod)
