"""Load CLI implementation modules from bytecode; alias legacy ``flink_cowrie`` imports."""

from __future__ import annotations

import sys
from importlib.machinery import SourcelessFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

_PKG = Path(__file__).resolve().parent
_INSTALLED = False

# Dependency order for modules that exist only as .pyc today.
_PYC_MODULES = [
    "env_sync",
    "config",
    "checks.doctor",
    "checks.demo_ready",
    "checks",
    "commands.config_cmd",
    "commands.dashboard",
    "commands.demo",
    "commands.demo_ready_cmd",
    "commands.doctor_cmd",
    "commands.modes_cmd",
    "commands.sync",
    "commands.sync_env_cmd",
    "commands.utils",
]


def _pyc_path(relative: str) -> Path:
    parts = relative.split(".")
    if len(parts) == 1:
        pkg_init = _PKG / parts[0] / "__pycache__" / "__init__.cpython-312.pyc"
        if pkg_init.is_file():
            return pkg_init
        return _PKG / "__pycache__" / f"{parts[0]}.cpython-312.pyc"
    parent = _PKG.joinpath(*parts[:-1])
    return parent / "__pycache__" / f"{parts[-1]}.cpython-312.pyc"


def _source_py_path(relative: str) -> Path | None:
    parts = relative.split(".")
    if len(parts) == 1:
        path = _PKG / f"{parts[0]}.py"
    else:
        path = _PKG.joinpath(*parts[:-1], f"{parts[-1]}.py")
    return path if path.is_file() else None


def _load_pyc(qualified: str) -> None:
    if qualified in sys.modules:
        return
    relative = qualified.removeprefix("apemosyne.")
    if _source_py_path(relative) is not None:
        import importlib

        importlib.import_module(qualified)
        legacy = qualified.replace("apemosyne", "flink_cowrie", 1)
        if qualified in sys.modules:
            sys.modules[legacy] = sys.modules[qualified]
        return
    pyc = _pyc_path(relative)
    if not pyc.is_file():
        return
    loader = SourcelessFileLoader(qualified, str(pyc))
    spec = spec_from_loader(qualified, loader)
    if spec is None:
        return
    mod = module_from_spec(spec)
    sys.modules[qualified] = mod
    loader.exec_module(mod)
    legacy = qualified.replace("apemosyne", "flink_cowrie", 1)
    sys.modules[legacy] = mod
    # Register parent namespace packages for legacy imports (e.g. flink_cowrie.checks).
    if "." in qualified:
        legacy_parent = legacy.rsplit(".", 1)[0]
        ape_parent = qualified.rsplit(".", 1)[0]
        if legacy_parent not in sys.modules and ape_parent in sys.modules:
            sys.modules[legacy_parent] = sys.modules[ape_parent]


def install_aliases() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    pkg = sys.modules["apemosyne"]
    sys.modules["flink_cowrie"] = pkg

    class _LegacyAliasFinder:
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "flink_cowrie" or fullname.startswith("flink_cowrie."):
                alias = fullname.replace("flink_cowrie", "apemosyne", 1)
                return importlib.util.find_spec(alias)
            return None

    import importlib
    import importlib.util

    if not any(type(f).__name__ == "_LegacyAliasFinder" for f in sys.meta_path):
        sys.meta_path.insert(0, _LegacyAliasFinder())

    for mod in (
        "apemosyne.constants",
        "apemosyne.docker_utils",
        "apemosyne.paths",
        "apemosyne.manifests",
        "apemosyne.startup_modes",
        "apemosyne.commands",
    ):
        importlib.import_module(mod)
    sys.modules["flink_cowrie.commands"] = sys.modules["apemosyne.commands"]

    for name in _PYC_MODULES:
        _load_pyc(f"apemosyne.{name}")

    if "apemosyne.checks" in sys.modules:
        sys.modules["flink_cowrie.checks"] = sys.modules["apemosyne.checks"]
    sys.modules["flink_cowrie.commands"] = sys.modules["apemosyne.commands"]

    from apemosyne.commands import test_cmd

    if hasattr(test_cmd, "register_legacy_commands"):
        test_cmd.register_legacy_commands()


def _rebrand_typer_app(mod) -> None:
    """Patch Click group name/version from legacy flink-cowrie bytecode."""
    import typer
    import typer.main
    from apemosyne import __version__

    def _version_callback(ctx, param, value) -> None:
        if value:
            typer.echo(f"apemosyne {__version__}")
            raise typer.Exit()

    mod._version_callback = _version_callback
    click_cmd = typer.main.get_command(mod.app)
    click_cmd.name = "apemosyne"
    for param in click_cmd.params:
        if param.name == "version":
            param.callback = _version_callback
            break


def get_app():
    """Return the Typer application (prefers ``apemosyne.cli`` source)."""
    install_aliases()
    from apemosyne.cli import app as source_app

    return source_app


def _legacy_get_app_from_bytecode():
    """Fallback if source CLI is unavailable."""
    impl = "apemosyne._cli_impl"
    if impl not in sys.modules:
        pyc = _PKG / "__pycache__" / "cli.cpython-312.pyc"
        loader = SourcelessFileLoader(impl, str(pyc))
        spec = spec_from_loader(impl, loader)
        mod = module_from_spec(spec)
        sys.modules[impl] = mod
        loader.exec_module(mod)
        sys.modules["flink_cowrie._cli_impl"] = mod
    _rebrand_typer_app(sys.modules[impl])
    return sys.modules[impl].app
