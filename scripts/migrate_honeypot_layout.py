#!/usr/bin/env python3
"""One-shot layout migration: root cowrie modules → honeypot/src, compose paths, pyc shims."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HONEYPOT = REPO / "honeypot"
SRC = HONEYPOT / "src"

SHIM_TEMPLATE = '''\
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
'''

# Modules referenced by compose / CLI but not yet placed under honeypot/src.
EXTRA_MODULE_DIRS: dict[str, str] = {
    "cowrie_log_processor.py": "services",
    "cowrie_kafka_shipper.py": "pipeline",
    "cowrie_kafka_normalizer.py": "pipeline",
    "cowrie_kafka_flink_job.py": "pipeline",
    "cowrie_kafka_actor_classifier.py": "pipeline",
    "cowrie_actor_classify_job.py": "traps",
    "cowrie_phase2_workflow_processor.py": "pipeline",
    "cloudera_misinformation_llm.py": "integrations",
    "cowrie_phase1_verify.py": "pipeline",
    "cowrie_response_tools.py": "core",
    "demo_cowrie_response.py": "demo",
    "demo_cloudera_react_agent.py": "demo",
}


def build_module_map() -> dict[str, str]:
    mapping: dict[str, str] = dict(EXTRA_MODULE_DIRS)
    for subdir in SRC.iterdir():
        if not subdir.is_dir() or subdir.name.startswith("_"):
            continue
        cache = subdir / "__pycache__"
        if not cache.is_dir():
            continue
        for pyc in cache.glob("*.cpython-312.pyc"):
            name = pyc.name.split(".cpython-")[0] + ".py"
            mapping.setdefault(name, subdir.name)
    # Tracked root sources
    for name in (
        "cowrie_pipeline.py",
        "cowrie_flink_jobs_startup.py",
        "cowrie_flink_pipeline_supervisor.py",
        "cowrie_kafka_topics_startup.py",
        "cowrie_phase1_verify.py",
        "flink_cluster_submit.py",
        "kafka_alerts_to_dashboard.py",
    ):
        if name.startswith("cowrie_") or name.startswith("flink_") or name.startswith("kafka_"):
            stem = name.replace(".py", "")
            if stem.startswith("flink_"):
                mapping.setdefault(name, "cluster")
            elif stem.startswith("kafka_"):
                mapping.setdefault(name, "services")
            else:
                mapping.setdefault(name, "pipeline")
    return mapping


def dest_dir(filename: str, module_map: dict[str, str]) -> Path:
    sub = module_map.get(filename)
    if sub == "demo":
        return HONEYPOT / "demo"
    if not sub:
        raise KeyError(f"No destination for {filename}")
    return SRC / sub


def copy_pyc_from_root(name: str, dest: Path) -> None:
    stem = name.replace(".py", "")
    src_pyc = REPO / "__pycache__" / f"{stem}.cpython-312.pyc"
    if not src_pyc.is_file():
        return
    cache = dest / "__pycache__"
    cache.mkdir(parents=True, exist_ok=True)
    dst = cache / src_pyc.name
    if not dst.exists():
        shutil.copy2(src_pyc, dst)


def ensure_shim(dest: Path, name: str) -> None:
    py = dest / name
    if py.is_file():
        text = py.read_text(encoding="utf-8")
        if "_pyc_shim" in text:
            return
        if len(text.strip()) > 50:
            return
    pyc = dest / "__pycache__" / f"{name.replace('.py', '')}.cpython-312.pyc"
    if not pyc.is_file():
        return
    py.write_text(SHIM_TEMPLATE, encoding="utf-8")


def move_root_modules(module_map: dict[str, str]) -> None:
    for name in sorted(set(module_map) | {p.name for p in REPO.glob("cowrie_*.py")} | {p.name for p in REPO.glob("flink_*.py")} | {p.name for p in REPO.glob("kafka_*.py")}):
        if name not in module_map and not (REPO / name).is_file():
            continue
        try:
            dest = dest_dir(name, module_map)
        except KeyError:
            continue
        dest.mkdir(parents=True, exist_ok=True)
        copy_pyc_from_root(name, dest)
        root_py = REPO / name
        dst_py = dest / name
        if root_py.is_file():
            if dst_py.exists() and dst_py.read_bytes() == root_py.read_bytes():
                root_py.unlink()
            elif not dst_py.exists():
                shutil.move(str(root_py), str(dst_py))
            else:
                root_py.unlink()
        ensure_shim(dest, name)


def move_dashboard() -> None:
    src = REPO / "dashboard" / "dashboard_cowrie.py"
    dst_dir = HONEYPOT / "dashboard"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "dashboard_cowrie.py"
    if src.is_file():
        if not dst.exists():
            shutil.move(str(src), str(dst))
        else:
            src.unlink()


def merge_cowrie_data() -> None:
    root_data = REPO / "cowrie-data"
    hp_data = HONEYPOT / "cowrie-data"
    if not root_data.is_dir():
        return
    hp_data.mkdir(parents=True, exist_ok=True)
    for item in root_data.iterdir():
        target = hp_data / item.name
        if item.is_dir():
            if target.exists():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.move(str(item), str(target))
        elif not target.exists():
            shutil.move(str(item), str(target))
        else:
            item.unlink()
    try:
        root_data.rmdir()
    except OSError:
        pass


def rewrite_compose(module_map: dict[str, str]) -> None:
    src_compose = HONEYPOT / "docker-compose.yml"
    if not src_compose.is_file():
        src_compose = REPO / "docker-compose-cowrie.yml"
    text = src_compose.read_text(encoding="utf-8")

    def repl_py(match: re.Match[str]) -> str:
        name = match.group(1)
        if name.startswith("demo/"):
            inner = name.split("/", 1)[1]
            return f"./demo/{inner}"
        if name not in module_map:
            return match.group(0)
        sub = module_map[name]
        if sub == "demo":
            return f"./demo/{name}"
        return f"./src/{sub}/{name}"

    text = re.sub(r"\./((?:demo/)?[a-z0-9_]+\.py)", repl_py, text)
    replacements = {
        "./cowrie-config/": "./cowrie-config/",
        "./cowrie-data": "./cowrie-data",
        "./cowrie-logs": "./cowrie-logs",
        "./dashboard/": "./dashboard/",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    (HONEYPOT / "docker-compose.yml").write_text(text, encoding="utf-8")


def main() -> None:
    module_map = build_module_map()
    move_root_modules(module_map)
    move_dashboard()
    merge_cowrie_data()
    rewrite_compose(module_map)
    print(f"Migrated {len(module_map)} modules under {HONEYPOT}")
    print(f"Wrote {HONEYPOT / 'docker-compose.yml'}")


if __name__ == "__main__":
    main()
