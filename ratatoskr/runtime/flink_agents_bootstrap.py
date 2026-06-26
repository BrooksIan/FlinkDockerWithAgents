"""Bootstrap Flink Agents on the Flink Docker image (bundled PyFlink, no apache-flink pip)."""

from __future__ import annotations


def patch_flink_agents_version() -> None:
    """Register the cluster Flink version when apache-flink pip metadata is absent."""
    try:
        from importlib.metadata import version as pkg_version

        pkg_version("apache-flink")
    except Exception:
        try:
            from flink_agents.api import version_compatibility
            from ratatoskr.runtime.flink_cluster_submit import flink_major_version

            major = flink_major_version()
            version_compatibility.flink_version_manager._flink_version = f"{major}.0"
            version_compatibility.flink_version_manager._initialized = True
        except Exception:
            pass

    patch_flink_agents_jar_loading()


def patch_flink_agents_jar_loading() -> None:
    """Skip duplicate per-jar ``add_jars`` calls from Flink Agents (Pemja classloaders)."""
    try:
        from flink_agents.api import execution_environment as ee
    except ImportError:
        return
    if getattr(ee, "_RATATOSKR_JAR_PATCH", False):
        return

    import importlib

    _original_get = ee.AgentsExecutionEnvironment.get_execution_environment

    def _patched_get_execution_environment(env=None, t_env=None, **kwargs):
        if env is None:
            return _original_get(env=env, t_env=t_env, **kwargs)

        try:
            from flink_agents.api import version_compatibility
            from ratatoskr.runtime.flink_cluster_submit import (
                attach_flink_agents_jars,
                flink_major_version,
            )

            if not version_compatibility.flink_version_manager._initialized:
                major = flink_major_version()
                version_compatibility.flink_version_manager._flink_version = f"{major}.0"
                version_compatibility.flink_version_manager._initialized = True
            major_version = version_compatibility.flink_version_manager.major_version
        except Exception as exc:
            raise ModuleNotFoundError("Apache Flink is not installed.") from exc

        if not major_version:
            raise ModuleNotFoundError("Apache Flink is not installed.")

        attach_flink_agents_jars(env)

        return importlib.import_module(
            "flink_agents.runtime.remote_execution_environment"
        ).create_instance(env=env, t_env=t_env, **kwargs)

    ee.AgentsExecutionEnvironment.get_execution_environment = staticmethod(
        _patched_get_execution_environment
    )
    ee._RATATOSKR_JAR_PATCH = True
