"""Sync Designer LLM settings into Flink cluster containers."""

from __future__ import annotations

import shlex
from pathlib import Path

from apemosyne.constants import DEFAULT_PROFILE
from apemosyne.docker_utils import container_id, docker_cp, project_root

REMOTE_DESIGNER_DB = "/tmp/apemosyne_designer.db"


def designer_copy_pairs(root: Path | None = None) -> list[tuple[str, str]]:
    repo = root or project_root()
    pairs: list[tuple[str, str]] = []
    for rel in (
        "apemosyne/designer/__init__.py",
        "apemosyne/designer/models.py",
        "apemosyne/designer/store.py",
        "apemosyne/designer/llm_settings.py",
        "apemosyne/designer/llm_client.py",
        "apemosyne/designer/runtime_env.py",
        "examples/agents/react_double_value_logic.py",
        "examples/agents/react_double_value_prompt.py",
    ):
        local = repo / rel
        if local.is_file():
            pairs.append((str(local), f"/opt/flink/{rel}"))
    return pairs


def sync_designer_db_to_cluster(
    *,
    root: Path | None = None,
    profile: str = DEFAULT_PROFILE,
) -> str | None:
    from apemosyne.designer.store import designer_db_path

    local_db = designer_db_path(root)
    if not local_db.is_file():
        return None
    synced = False
    for service in ("jobmanager", "taskmanager"):
        cid = container_id(service, profile=profile)
        if cid and docker_cp(local_db, cid, REMOTE_DESIGNER_DB):
            synced = True
    return REMOTE_DESIGNER_DB if synced else None


def react_llm_shell_prefix(
    *,
    root: Path | None = None,
    remote_designer_db: str | None = REMOTE_DESIGNER_DB,
) -> str:
    """Shell exports for ReAct LLM settings (Designer store + env fallback)."""
    exports: list[str] = []
    if remote_designer_db:
        exports.append(f"export APEMOSYNE_DESIGNER_DB={shlex.quote(remote_designer_db)}")

    try:
        from apemosyne.designer.llm_settings import get_react_llm_settings

        settings = get_react_llm_settings(root=root)
        if settings.endpoint_url:
            exports.append(
                f"export APEMOSYNE_LLM_ENDPOINT_URL={shlex.quote(settings.endpoint_url)}"
            )
        if settings.model_id:
            exports.append(f"export APEMOSYNE_LLM_MODEL_ID={shlex.quote(settings.model_id)}")
        if settings.api_key:
            exports.append(f"export APEMOSYNE_LLM_API_KEY={shlex.quote(settings.api_key)}")
    except Exception:
        pass

    if not exports:
        return ""
    return " && ".join(exports) + " && "
