"""Built-in workflow tool definitions for agent codegen."""

from __future__ import annotations

from typing import Any

_BUILTIN_TOOLS: dict[str, dict[str, Any]] = {
    "double": {
        "description": "Return twice the input value.",
        "param_type": "int",
        "body": "return value * 2",
    },
    "scale": {
        "description": "Multiply input by a configured factor.",
        "param_type": "int",
        "body": "return value * factor",
        "extra_params": [{"name": "factor", "type": "int", "default": 2}],
    },
    "identity": {
        "description": "Return the input unchanged.",
        "param_type": "int",
        "body": "return value",
    },
}


def list_builtin_tools() -> list[dict[str, Any]]:
    return [{"name": name, **meta} for name, meta in _BUILTIN_TOOLS.items()]


def get_builtin_tool(name: str) -> dict[str, Any]:
    tool = _BUILTIN_TOOLS.get(name)
    if tool is None:
        raise KeyError(f"Unknown builtin tool {name!r}")
    return tool
