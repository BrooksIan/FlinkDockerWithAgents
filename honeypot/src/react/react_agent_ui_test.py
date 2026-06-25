\
"""Bytecode-backed module (source pending recovery)."""
from __future__ import annotations

import sys
from importlib.machinery import SourcelessFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path


def _load() -> None:
    here = Path(__file__).resolve().parent
    stem = Path(__file__).stem
    pyc = here / "__pycache__" / f"{stem}.cpython-312.pyc"
    loader = SourcelessFileLoader(stem, str(pyc))
    spec = spec_from_loader(stem, loader)
    mod = module_from_spec(spec)
    sys.modules[stem] = mod
    loader.exec_module(mod)
    for key, value in mod.__dict__.items():
        if not key.startswith("__"):
            globals()[key] = value


_load()

if __name__ == "__main__":
    pyc = Path(__file__).resolve().parent / "__pycache__" / f"{Path(__file__).stem}.cpython-312.pyc"
    loader = SourcelessFileLoader("__main__", str(pyc))
    spec = spec_from_loader("__main__", loader)
    mod = module_from_spec(spec)
    loader.exec_module(mod)
