"""Startup mode presets for ``apemosyne up --mode``."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import yaml

from apemosyne.manifests import STARTUP_MODES_FILE, ManifestError, manifests_dir
from apemosyne.paths import honeypot_manifests_dir, project_root


@dataclass(frozen=True)
class StartupMode:
    name: str
    description: str
    profile: str
    wait: int = 10
    run_doctor: bool = True
    build_image_if_missing: bool = False
    ensure_kafka_topics: bool = False
    ensure_flink_jobs: bool = False
    endpoints: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class StartupModeCatalog:
    default: str
    modes: Dict[str, StartupMode]


@dataclass(frozen=True)
class UpOptions:
    profile: str
    build_image: bool
    wait: int
    skip_doctor: bool
    ensure_kafka: bool
    ensure_flink: bool
    mode_name: str


def _startup_modes_path(root: Path) -> Path:
    for base in (manifests_dir(root), honeypot_manifests_dir(root)):
        path = base / STARTUP_MODES_FILE
        if path.is_file():
            return path
    return manifests_dir(root) / STARTUP_MODES_FILE


def _parse_mode(name: str, raw: Mapping[str, object]) -> StartupMode:
    endpoints = raw.get("endpoints", [])
    if not isinstance(endpoints, list):
        endpoints = []
    return StartupMode(
        name=name,
        description=str(raw.get("description", "")),
        profile=str(raw.get("profile", "minimal")),
        wait=int(raw.get("wait", 10)),
        run_doctor=bool(raw.get("run_doctor", True)),
        build_image_if_missing=bool(raw.get("build_image_if_missing", False)),
        ensure_kafka_topics=bool(raw.get("ensure_kafka_topics", False)),
        ensure_flink_jobs=bool(raw.get("ensure_flink_jobs", False)),
        endpoints=[str(item) for item in endpoints],
    )


def load_startup_modes(*, root: Optional[Path] = None) -> StartupModeCatalog:
    repo = root or project_root()
    path = _startup_modes_path(repo)
    if not path.is_file():
        raise ManifestError(f"Startup modes file not found: {path}")

    try:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ManifestError(f"Invalid YAML in {path}: {exc}") from exc

    if not isinstance(data, Mapping):
        raise ManifestError(f"Startup modes root must be a mapping: {path}")

    default = str(data.get("default", "flink"))
    raw_modes = data.get("modes")
    if not isinstance(raw_modes, Mapping):
        raise ManifestError(f"{path} must define a 'modes' mapping")

    modes: Dict[str, StartupMode] = {}
    for name, raw in raw_modes.items():
        if not isinstance(raw, Mapping):
            raise ManifestError(f"Mode '{name}' must be a mapping")
        modes[str(name)] = _parse_mode(str(name), raw)

    if default not in modes:
        raise ManifestError(f"Default startup mode '{default}' is not defined in {path}")

    return StartupModeCatalog(default=default, modes=modes)


def get_startup_mode(name: str, *, root: Optional[Path] = None) -> StartupMode:
    catalog = load_startup_modes(root=root)
    if name not in catalog.modes:
        known = ", ".join(sorted(catalog.modes))
        raise ManifestError(f"Unknown startup mode '{name}'. Known modes: {known}")
    return catalog.modes[name]


def list_startup_mode_names(*, root: Optional[Path] = None) -> List[str]:
    return sorted(load_startup_modes(root=root).modes)


def normalize_profile(profile: str) -> str:
    value = profile.strip().lower()
    if value in ("minimal", "min", "flink"):
        return "minimal"
    if value in ("full", "honeypot", "cowrie"):
        return "full"
    return value


def resolve_mode_name(
    *,
    mode: Optional[str] = None,
    profile: Optional[str] = None,
    root: Optional[Path] = None,
) -> str:
    catalog = load_startup_modes(root=root)
    if mode:
        if mode not in catalog.modes:
            raise ManifestError(f"Unknown startup mode '{mode}'")
        return mode
    if profile:
        norm = normalize_profile(profile)
        for name, spec in catalog.modes.items():
            if spec.profile == norm:
                return name
    return catalog.default


def resolve_up_options(
    *,
    mode: Optional[str] = None,
    profile: Optional[str] = None,
    build_image: bool = False,
    wait: Optional[int] = None,
    skip_doctor: bool = False,
    ensure_kafka: Optional[bool] = None,
    ensure_flink: Optional[bool] = None,
    root: Optional[Path] = None,
) -> UpOptions:
    mode_name = resolve_mode_name(mode=mode, profile=profile, root=root)
    spec = get_startup_mode(mode_name, root=root)
    return UpOptions(
        profile=spec.profile,
        build_image=build_image or spec.build_image_if_missing,
        wait=spec.wait if wait is None else wait,
        skip_doctor=skip_doctor,
        ensure_kafka=spec.ensure_kafka_topics if ensure_kafka is None else ensure_kafka,
        ensure_flink=spec.ensure_flink_jobs if ensure_flink is None else ensure_flink,
        mode_name=mode_name,
    )
