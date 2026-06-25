"""Seed payloads for reference agent definitions."""

from __future__ import annotations

from typing import Any

DOUBLE_VALUE_ID = "def_double_value_v1"


def double_value_definition_payload() -> dict[str, Any]:
    """Graph matching workflow_counter (Double Value)."""
    return {
        "id": DOUBLE_VALUE_ID,
        "name": "Double Value",
        "type": "workflow",
        "version": 1,
        "description": "Doubles numeric input values using a @tool. Reference template from workflow_counter.",
        "status": "draft",
        "manifest_name": "workflow_counter",
        "catalog_category_id": "workflow",
        "catalog_subcategory_id": "transform",
        "catalog_tags": ["demo", "transform", "numeric"],
        "input_schema": {
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": {
                    "type": "integer",
                    "description": "Number to double",
                }
            },
        },
        "output_schema": {
            "type": "object",
            "properties": {
                "input": {"type": "integer"},
                "doubled": {"type": "integer"},
                "agent": {"type": "string"},
            },
        },
        "nodes": [
            {
                "id": "in1",
                "kind": "input_event",
                "name": "InputEvent",
                "config": {"event_type": "_input_event"},
            },
            {
                "id": "act1",
                "kind": "action",
                "name": "process",
                "config": {"listens_to": ["_input_event"]},
            },
            {
                "id": "tool1",
                "kind": "tool",
                "name": "double",
                "config": {"tool_ref": "double", "expression": "value * 2"},
            },
            {
                "id": "out1",
                "kind": "output_event",
                "name": "OutputEvent",
                "config": {"event_type": "_output_event"},
            },
        ],
        "edges": [
            {"id": "e1", "source": "in1", "target": "act1", "kind": "listens_to"},
            {"id": "e2", "source": "act1", "target": "tool1", "kind": "calls"},
            {"id": "e3", "source": "act1", "target": "out1", "kind": "emits"},
        ],
        "layout": {
            "in1": {"x": 80, "y": 200},
            "act1": {"x": 320, "y": 200},
            "tool1": {"x": 560, "y": 120},
            "out1": {"x": 560, "y": 280},
        },
    }
