"""Bootstrap Flink Agents on the Flink Docker image (bundled PyFlink, no apache-flink pip)."""

from __future__ import annotations


def patch_flink_agents_version() -> None:
    """Register the cluster Flink version when apache-flink pip metadata is absent."""
    try:
        from importlib.metadata import version as pkg_version

        pkg_version("apache-flink")
        return
    except Exception:
        pass

    try:
        from flink_agents.api import version_compatibility
        from apemosyne.runtime.flink_cluster_submit import flink_major_version

        major = flink_major_version()
        version_compatibility.flink_version_manager._flink_version = f"{major}.0"
        version_compatibility.flink_version_manager._initialized = True
    except Exception:
        return
