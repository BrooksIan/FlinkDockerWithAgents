"""Load a sibling ``__pycache__/*.cpython-312.pyc`` when source is not yet recovered."""

from __future__ import annotations

import marshal
from pathlib import Path

_PYC_SUBDIRS = ("react", "core", "pipeline", "integrations", "services", "traps", "demo")
_PYC_HEADER_SIZE = 16


def resolve_sibling_pyc(module_file: str | Path) -> Path:
    """Find bytecode for a shim module (repo tree or flat ``/opt/flink`` mounts)."""
    path = Path(module_file).resolve()
    stem = path.stem
    if stem.endswith(".cpython-312"):
        stem = stem.removesuffix(".cpython-312")
    name = f"{stem}.cpython-312.pyc"
    candidates: list[Path] = []
    flat_mount = path.parent == Path("/opt/flink")
    if not flat_mount:
        candidates.append(path.parent / "__pycache__" / name)
    for sub in _PYC_SUBDIRS:
        candidates.append(Path(f"/opt/flink/pyc/{sub}") / name)
    for pyc in candidates:
        if pyc.is_file():
            return pyc
    tried = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Bytecode not found for {stem} (tried: {tried})")


def _load_code(pyc: Path):
    """Unmarshal ``.cpython-312.pyc`` body; fail clearly on Python version skew."""
    import sys

    raw = pyc.read_bytes()
    if len(raw) < _PYC_HEADER_SIZE:
        raise ImportError(f"Truncated bytecode: {pyc}")
    try:
        return marshal.loads(raw[_PYC_HEADER_SIZE:])
    except Exception as exc:
        raise ImportError(
            f"Cannot load {pyc.name} on Python {sys.version_info.major}."
            f"{sys.version_info.minor} (need 3.12+): {exc}"
        ) from exc


def load_sibling_pyc(module_globals: dict) -> None:
    """Execute bytecode for the calling shim into ``module_globals``."""
    module_file = module_globals["__file__"]
    pyc = resolve_sibling_pyc(module_file)
    code = _load_code(pyc)
    namespace = {
        "__name__": module_globals.get("__name__"),
        "__file__": str(Path(module_file).resolve()),
        "__package__": module_globals.get("__package__"),
        "__loader__": module_globals.get("__loader__"),
        "__spec__": module_globals.get("__spec__"),
    }
    exec(code, namespace)
    module_globals.clear()
    module_globals.update(namespace)


def run_sibling_pyc_main(module_file: str) -> None:
    """Run ``if __name__ == '__main__'`` block from sibling bytecode."""
    pyc = resolve_sibling_pyc(module_file)
    code = _load_code(pyc)
    namespace = {
        "__name__": "__main__",
        "__file__": str(Path(module_file).resolve()),
        "__package__": None,
    }
    exec(code, namespace)
