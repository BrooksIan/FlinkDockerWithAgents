"""Copy files into containers using YAML manifests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import typer

from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.docker_utils import container_id, docker_cp, project_root
from apemosyne.manifests import FileManifest, load_manifest


@dataclass(frozen=True)
class CopyStats:
    copied: int = 0
    skipped: int = 0
    failed: int = 0


def manifest_path_pairs(manifest: FileManifest) -> List[Tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in manifest.files:
        pairs.append((str(item.local), item.remote))
    return pairs


def copy_file_to_container(local: str, container: str, remote: str) -> bool:
    return docker_cp(project_root() / local if not str(local).startswith("/") else local, container, remote)


def copy_pairs_to_container(
    container: str,
    pairs: Sequence[Tuple[str, str]],
) -> CopyStats:
    copied = skipped = failed = 0
    root = project_root()
    for local_rel, remote in pairs:
        local_path = Path(local_rel)
        if not local_path.is_absolute():
            local_path = root / local_rel
        if not local_path.is_file():
            typer.echo(f"  [skip] missing {local_rel}", err=True)
            skipped += 1
            continue
        if docker_cp(local_path, container, remote):
            copied += 1
        else:
            typer.echo(f"  [fail] {local_rel} -> {remote}", err=True)
            failed += 1
    return CopyStats(copied=copied, skipped=skipped, failed=failed)


def copy_pairs_to_cluster(
    pairs: Sequence[Tuple[str, str]],
    *,
    profile: str = DEFAULT_PROFILE,
    services: Iterable[str] = ("jobmanager", "taskmanager"),
) -> CopyStats:
    totals = CopyStats()
    for service in services:
        cid = container_id(service, profile=profile)
        if not cid:
            continue
        stats = copy_pairs_to_container(cid, pairs)
        totals = CopyStats(
            copied=totals.copied + stats.copied,
            skipped=totals.skipped + stats.skipped,
            failed=totals.failed + stats.failed,
        )
    return totals


def copy_manifest_to_container(
    name: str,
    container: str,
    *,
    profile: Optional[str] = None,
) -> CopyStats:
    manifest = load_manifest(name)
    return copy_pairs_to_container(container, manifest_path_pairs(manifest))


def copy_manifest_to_cluster(
    name: str,
    profile: str = DEFAULT_PROFILE,
) -> CopyStats:
    manifest = load_manifest(name)
    pairs = manifest_path_pairs(manifest)
    typer.echo(f"Copying manifest '{name}' ({len(pairs)} file(s)) to cluster...")
    stats = copy_pairs_to_cluster(pairs, profile=profile)
    typer.echo(
        f"  copied={stats.copied} skipped={stats.skipped} failed={stats.failed}"
    )
    if stats.failed:
        raise typer.Exit(1)
    return stats
