"""Load and validate YAML manifests for demos, file copy, and verify tiers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml

from ratatoskr.paths import (
    examples_dir,
    honeypot_manifests_dir,
    manifests_dir,
    project_root,
)

from ratatoskr.constants import VERIFY_PROFILE_HONEYPOT

DEMO_CATALOG_FILE = "demo-files.yaml"
VERIFY_TIERS_FILE = "verify-tiers.yaml"
STARTUP_MODES_FILE = "startup-modes.yaml"


class ManifestError(Exception):
    """Invalid or missing manifest YAML."""


@dataclass(frozen=True)
class ManifestFile:
    local: Path
    remote: str
    optional: bool = False
    local_rel: str = ""

    def __post_init__(self) -> None:
        if not self.local_rel:
            object.__setattr__(self, "local_rel", str(self.local))


@dataclass(frozen=True)
class FileManifest:
    name: str
    profile: Optional[str]
    files: List[ManifestFile]
    source: Path


@dataclass(frozen=True)
class DemoSpec:
    name: str
    script: str
    files: List[ManifestFile]


@dataclass(frozen=True)
class DemoCatalog:
    choices: List[str]
    demos: Dict[str, DemoSpec]


@dataclass(frozen=True)
class VerifyStep:
    type: str
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VerifyTier:
    name: str
    steps: List[VerifyStep]


def _load_yaml(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _parse_file_entry(entry: Mapping[str, Any], repo_root: Path) -> ManifestFile:
    local_raw = entry.get("local")
    remote = entry.get("remote")
    if not local_raw or not remote:
        raise ManifestError("Each files entry requires 'local' and 'remote'")
    local = Path(str(local_raw))
    if not local.is_absolute():
        local = repo_root / local
    return ManifestFile(
        local=local,
        remote=str(remote),
        optional=bool(entry.get("optional", False)),
        local_rel=str(local_raw),
    )


def _demo_catalog_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    for base in (manifests_dir(root), honeypot_manifests_dir(root), examples_dir(root)):
        candidate = base / DEMO_CATALOG_FILE
        if candidate.is_file():
            paths.append(candidate)
    return paths


def _parse_demo_catalog_file(
    path: Path, repo_root: Path, *, validate: bool = True
) -> DemoCatalog:
    if not path.is_file():
        raise ManifestError(f"Demo catalog not found: {path}")

    data = _load_yaml(path)
    if not isinstance(data, Mapping):
        raise ManifestError(f"Demo catalog root must be a mapping: {path}")

    raw_demos = data.get("demos")
    if not isinstance(raw_demos, Mapping):
        raise ManifestError(f"{path} must define a 'demos' mapping")

    choices = data.get("choices")
    if isinstance(choices, list):
        demo_names = [str(name) for name in choices]
    else:
        demo_names = sorted(str(name) for name in raw_demos)

    demos: Dict[str, DemoSpec] = {}
    for name in demo_names:
        raw = raw_demos.get(name)
        if not isinstance(raw, Mapping):
            raise ManifestError(f"Demo '{name}' missing from demos mapping")
        script = raw.get("script")
        raw_files = raw.get("files")
        if not script or not isinstance(raw_files, list):
            raise ManifestError(f"Demo '{name}' requires 'script' and 'files'")

        files = [
            _parse_file_entry(entry, repo_root)
            for entry in raw_files
            if isinstance(entry, Mapping)
        ]
        if validate:
            missing = [
                item.local_rel
                for item in files
                if not item.optional and not item.local.is_file()
            ]
            if missing:
                raise ManifestError(
                    f"Demo '{name}' references missing files: {', '.join(missing)}"
                )
        demos[name] = DemoSpec(name=name, script=str(script), files=files)

    return DemoCatalog(choices=demo_names, demos=demos)


def load_demo_catalog(*, root: Optional[Path] = None, validate: bool = True) -> DemoCatalog:
    """Merge demo catalogs from workspace, honeypot, and examples."""
    repo_root = root or project_root()
    merged_choices: list[str] = []
    merged_demos: Dict[str, DemoSpec] = {}

    for path in _demo_catalog_paths(repo_root):
        catalog = _parse_demo_catalog_file(path, repo_root, validate=validate)
        for name in catalog.choices:
            if name not in merged_choices:
                merged_choices.append(name)
        for name, spec in catalog.demos.items():
            if name in merged_demos and name not in catalog.choices:
                continue
            merged_demos[name] = spec

    if not merged_demos:
        raise ManifestError(
            "No demo catalogs found (expected ratatoskr/manifests/demo-files.yaml "
            "or examples/demo-files.yaml)"
        )
    return DemoCatalog(choices=merged_choices, demos=merged_demos)


def get_demo_spec(
    name: str, *, root: Optional[Path] = None, validate: bool = True
) -> DemoSpec:
    catalog = load_demo_catalog(root=root, validate=validate)
    if name not in catalog.demos:
        known = ", ".join(catalog.choices)
        raise ManifestError(f"Unknown demo '{name}'. Known demos: {known}")
    return catalog.demos[name]


def _manifest_search_dirs(root: Path) -> list[Path]:
    dirs: list[Path] = []
    for base in (manifests_dir(root), honeypot_manifests_dir(root)):
        if base.is_dir():
            dirs.append(base)
    return dirs


def list_manifests(*, root: Optional[Path] = None) -> List[str]:
    """List file-copy manifest stem names (excludes verify/demo/startup catalogs)."""
    repo = root or project_root()
    skip = {DEMO_CATALOG_FILE, VERIFY_TIERS_FILE, STARTUP_MODES_FILE}
    names: list[str] = []
    for base in _manifest_search_dirs(repo):
        for path in sorted(base.glob("*.yaml")):
            if path.name in skip:
                continue
            stem = path.stem
            if stem not in names:
                names.append(stem)
    return names


def load_manifest(
    name: str, *, root: Optional[Path] = None, validate: bool = True
) -> FileManifest:
    repo = root or project_root()
    stem = name.removesuffix(".yaml")
    for base in _manifest_search_dirs(repo):
        path = base / f"{stem}.yaml"
        if not path.is_file():
            continue
        data = _load_yaml(path)
        if not isinstance(data, Mapping):
            raise ManifestError(f"Manifest root must be a mapping: {path}")
        raw_files = data.get("files")
        if not isinstance(raw_files, list):
            raise ManifestError(f"{path} must define 'files'")
        files = [
            _parse_file_entry(entry, repo)
            for entry in raw_files
            if isinstance(entry, Mapping)
        ]
        if validate:
            validate_manifest_paths(
                FileManifest(
                    name=stem,
                    profile=data.get("profile"),
                    files=files,
                    source=path,
                ),
                root=repo,
            )
        return FileManifest(
            name=stem,
            profile=str(data["profile"]) if data.get("profile") else None,
            files=files,
            source=path,
        )
    raise ManifestError(f"Manifest not found: {name}")


def validate_manifest_paths(
    manifest: FileManifest,
    *,
    required_only: bool = True,
    root: Optional[Path] = None,
) -> None:
    repo = root or project_root()
    missing = [
        item.local_rel
        for item in manifest.files
        if (not required_only or not item.optional) and not item.local.is_file()
    ]
    if missing:
        raise ManifestError(
            f"Manifest '{manifest.name}' references missing files: {', '.join(missing)}"
        )



def _normalize_step_options(options: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(options)
    if "path" in normalized and "paths" not in normalized:
        path_val = normalized.pop("path")
        if isinstance(path_val, str):
            normalized["paths"] = [path_val]
        elif isinstance(path_val, list):
            normalized["paths"] = path_val
    return normalized


def _parse_step_entries(raw_steps: list) -> List[VerifyStep]:
    steps: list[VerifyStep] = []
    for entry in raw_steps:
        if not isinstance(entry, Mapping):
            continue
        step_type = str(entry.get("type", "python"))
        options = _normalize_step_options(
            {k: v for k, v in entry.items() if k != "type"}
        )
        steps.append(VerifyStep(type=step_type, options=options))
    return steps


def _resolve_verify_tier(name: str, raw_tiers: Mapping[str, Any]) -> List[VerifyStep]:
    raw = raw_tiers.get(name)
    if not isinstance(raw, Mapping):
        raise ManifestError(f"Verify tier '{name}' must be a mapping")

    steps: list[VerifyStep] = []
    extends = raw.get("extends")
    if extends:
        parent = str(extends)
        if parent not in raw_tiers:
            raise ManifestError(f"Verify tier '{name}' extends unknown tier '{parent}'")
        steps.extend(_resolve_verify_tier(parent, raw_tiers))

    raw_steps = raw.get("steps")
    if isinstance(raw_steps, list):
        steps.extend(_parse_step_entries(raw_steps))
    return steps


def _overlay_tier_steps(name: str, raw_tiers: Mapping[str, Any]) -> List[VerifyStep]:
    """Return direct steps from an overlay manifest (no ``extends``)."""
    raw = raw_tiers.get(name)
    if not isinstance(raw, Mapping):
        return []
    raw_steps = raw.get("steps")
    if not isinstance(raw_steps, list):
        return []
    return _parse_step_entries(raw_steps)


def _honeypot_verify_overlay_active(profile: str | None) -> bool:
    if profile is None:
        return False
    return profile.strip().lower() in (VERIFY_PROFILE_HONEYPOT, "full", "cowrie")


def _load_verify_tiers_file(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        raise ManifestError(f"Verify tiers file not found: {path}")
    data = _load_yaml(path)
    if not isinstance(data, Mapping):
        raise ManifestError(f"Verify tiers root must be a mapping: {path}")
    raw_tiers = data.get("tiers")
    if not isinstance(raw_tiers, Mapping):
        raise ManifestError(f"{path} must define a 'tiers' mapping")
    return raw_tiers


def load_verify_tiers(
    *, root: Optional[Path] = None, profile: Optional[str] = None
) -> Dict[str, VerifyTier]:
    repo = root or project_root()
    base_path = manifests_dir(repo) / VERIFY_TIERS_FILE
    raw_tiers = _load_verify_tiers_file(base_path)

    resolved: Dict[str, VerifyTier] = {}
    for tier_name in raw_tiers:
        name = str(tier_name)
        resolved[name] = VerifyTier(
            name=name,
            steps=_resolve_verify_tier(name, raw_tiers),
        )

    if _honeypot_verify_overlay_active(profile):
        overlay_path = honeypot_manifests_dir(repo) / VERIFY_TIERS_FILE
        if overlay_path.is_file():
            overlay_tiers = _load_verify_tiers_file(overlay_path)
            for tier_name, tier in list(resolved.items()):
                overlay_steps = _overlay_tier_steps(tier_name, overlay_tiers)
                if overlay_steps:
                    resolved[tier_name] = VerifyTier(
                        name=tier_name,
                        steps=tier.steps + overlay_steps,
                    )

    return resolved


def get_verify_tier(
    name: str, *, root: Optional[Path] = None, profile: Optional[str] = None
) -> VerifyTier:
    tiers = load_verify_tiers(root=root, profile=profile)
    if name not in tiers:
        known = ", ".join(sorted(tiers))
        raise ManifestError(f"Unknown verify tier '{name}'. Known tiers: {known}")
    return tiers[name]
