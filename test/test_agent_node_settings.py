#!/usr/bin/env python3
"""Tests for per-agent Studio node settings."""

from __future__ import annotations


def test_apply_agent_node_config_merges_defaults() -> None:
    from ratatoskr.pipelines.agent_settings import apply_agent_node_config

    records = [{"key": "1", "value": {"query": {"limit": 5}}}]
    merged = apply_agent_node_config(
        records,
        {"endpoint_url": "https://api.example.com", "kafka_topic": "workflow.test.output"},
    )
    assert merged[0]["value"]["endpoint_url"] == "https://api.example.com"
    assert merged[0]["value"]["kafka_topic"] == "workflow.test.output"
    assert merged[0]["value"]["query"] == {"limit": 5}
    assert merged[0]["key"] == "1"


def test_apply_agent_node_config_record_fields_win() -> None:
    from ratatoskr.pipelines.agent_settings import apply_agent_node_config

    records = [{"key": "1", "value": {"endpoint_url": "https://override.example.com"}}]
    merged = apply_agent_node_config(records, {"endpoint_url": "https://default.example.com"})
    assert merged[0]["value"]["endpoint_url"] == "https://override.example.com"


def test_apply_agent_node_config_seed_trigger() -> None:
    from ratatoskr.pipelines.agent_settings import apply_agent_node_config

    merged = apply_agent_node_config([{"key": "1", "value": {}}], {"endpoint_url": "https://api.example.com"})
    assert merged[0]["value"]["endpoint_url"] == "https://api.example.com"
    assert "value" in merged[0]


def test_missing_required_settings_readapi() -> None:
    from ratatoskr.pipelines.agent_settings import missing_required_settings

    assert missing_required_settings("readapi_reactthoughts_writekafka", {}) == [
        "endpoint_url",
        "kafka_topic",
    ]
    assert (
        missing_required_settings(
            "readapi_reactthoughts_writekafka",
            {"endpoint_url": "https://api.example.com", "kafka_topic": "out"},
        )
        == []
    )


def test_self_sourcing_agent_pipeline_valid_without_source() -> None:
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from ratatoskr.pipelines.validate import validate_pipeline

    pipeline = Pipeline(
        id="p1",
        name="API poll",
        nodes=[
            PipelineNode(
                id="a1",
                kind="agent",
                agent="readapi_reactthoughts_writekafka",
                config={
                    "endpoint_url": "https://api.example.com/posts",
                    "kafka_topic": "workflow.test.output",
                },
            ),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "capture"}),
        ],
        edges=[PipelineEdge(id="e1", source="a1", target="sink1")],
        layout={},
    )
    result = validate_pipeline(pipeline)
    assert result["valid"] is True, result["errors"]


def test_validate_pipeline_requires_readapi_settings() -> None:
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from ratatoskr.pipelines.validate import validate_pipeline

    pipeline = Pipeline(
        id="p1",
        name="Read API",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": {}}]},
            ),
            PipelineNode(id="a1", kind="agent", agent="readapi_reactthoughts_writekafka"),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "capture"}),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="a1"),
            PipelineEdge(id="e2", source="a1", target="sink1"),
        ],
        layout={},
    )
    result = validate_pipeline(pipeline)
    assert result["valid"] is False
    assert any("missing required settings" in err for err in result["errors"])
