#!/usr/bin/env python3
"""Studio window node tests."""

from __future__ import annotations


def _generic_window_pipeline():
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    return Pipeline(
        id="pipe_window_generic",
        name="Session window",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={
                    "records": [
                        {"key": "user-a", "value": 1, "timestamp": 100},
                        {"key": "user-a", "value": 2, "timestamp": 101},
                        {"key": "user-a", "value": 3, "timestamp": 102},
                        {"key": "user-b", "value": 10, "timestamp": 200},
                    ]
                },
            ),
            PipelineNode(
                id="win1",
                kind="window",
                config={
                    "window_type": "dynamic_session",
                    "key_field": "key",
                    "gap_policy": "default",
                    "gap_ms": 1000,
                    "execution_mode": "logic",
                },
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "capture"}),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="win1"),
            PipelineEdge(id="e2", source="win1", target="agent_wc"),
            PipelineEdge(id="e3", source="agent_wc", target="sink1"),
        ],
    )


def _session_detect_pipeline():
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    return Pipeline(
        id="pipe_window_test",
        name="Session detect",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={
                    "records": [
                        {"eventid": "cowrie.login.failed", "src_ip": "10.0.0.42", "timestamp": 100},
                        {"eventid": "cowrie.login.failed", "src_ip": "10.0.0.42", "timestamp": 101},
                        {"eventid": "cowrie.login.failed", "src_ip": "10.0.0.42", "timestamp": 102},
                        {"eventid": "cowrie.login.failed", "src_ip": "10.0.0.42", "timestamp": 103},
                        {"eventid": "cowrie.login.failed", "src_ip": "10.0.0.42", "timestamp": 104},
                        {"eventid": "cowrie.command.input", "src_ip": "10.0.0.99", "timestamp": 200},
                    ]
                },
            ),
            PipelineNode(
                id="win1",
                kind="window",
                config={
                    "window_type": "dynamic_session",
                    "key_field": "src_ip",
                    "gap_policy": "session_detect",
                    "execution_mode": "logic",
                },
            ),
            PipelineNode(id="agent_sd", kind="agent", agent="session_detect"),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "capture"}),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="win1"),
            PipelineEdge(id="e2", source="win1", target="agent_sd"),
            PipelineEdge(id="e3", source="agent_sd", target="sink1"),
        ],
    )


def test_validate_pipeline_accepts_generic_window_topology() -> None:
    from ratatoskr.pipelines.validate import validate_pipeline

    result = validate_pipeline(_generic_window_pipeline())
    assert result["valid"] is True
    assert not any("session_detect" in w for w in result.get("warnings", []))


def test_validate_pipeline_accepts_window_topology() -> None:
    from ratatoskr.pipelines.validate import validate_pipeline

    result = validate_pipeline(_session_detect_pipeline())
    assert result["valid"] is True


def test_validate_pipeline_rejects_window_after_agent() -> None:
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from ratatoskr.pipelines.validate import validate_pipeline

    pipeline = Pipeline(
        id="bad",
        name="bad",
        nodes=[
            PipelineNode(id="src1", kind="source", config={"records": [{"key": "1"}]}),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="win1", kind="window", config={"key_field": "key"}),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(id="e2", source="agent_wc", target="win1"),
            PipelineEdge(id="e3", source="win1", target="sink1"),
        ],
    )
    result = validate_pipeline(pipeline)
    assert result["valid"] is False


def test_window_local_emits_generic_summaries() -> None:
    from ratatoskr.pipelines.window_local import apply_window_node

    events = _generic_window_pipeline().nodes[0].config["records"]
    summaries = apply_window_node(events, _generic_window_pipeline().nodes[1].config)
    assert len(summaries) == 2
    user_a = next(s for s in summaries if s["key"] == "user-a")
    assert user_a["event_count"] == 3


def test_window_local_emits_session_detect_summaries() -> None:
    from ratatoskr.pipelines.window_local import apply_window_node
    from examples.agents.session_window_policy import SEVERITY_CRITICAL, classify_session

    events = _session_detect_pipeline().nodes[0].config["records"]
    summaries = apply_window_node(events, _session_detect_pipeline().nodes[1].config)
    assert len(summaries) == 2
    brute = next(s for s in summaries if s["src_ip"] == "10.0.0.42")
    assert brute["event_count"] == 5
    assert classify_session(brute) == SEVERITY_CRITICAL


def test_generate_cluster_runner_generic_window_compiles() -> None:
    import ast

    from ratatoskr.pipelines.cluster_codegen import generate_cluster_runner

    script = generate_cluster_runner(_generic_window_pipeline())
    ast.parse(script)
    assert "FixedGapExtractor" in script
    assert "prepare_agent_input" in script
    assert "process_session_summary" not in script


def test_generate_cluster_runner_window_logic_compiles() -> None:
    import ast

    from ratatoskr.pipelines.cluster_codegen import generate_cluster_runner

    script = generate_cluster_runner(_session_detect_pipeline())
    ast.parse(script)
    assert "DynamicProcessingTimeSessionWindows" in script
    assert "process_session_summary" in script
    assert "stream = env.from_collection(RECORDS)" in script
    assert "        stream = (" not in script


def test_validate_pipeline_warns_mismatched_session_detect_policy() -> None:
    from ratatoskr.pipelines.validate import validate_pipeline

    pipeline = _generic_window_pipeline()
    pipeline.nodes[1].config["gap_policy"] = "session_detect"
    result = validate_pipeline(pipeline)
    assert result["valid"] is True
    assert any("session_detect" in w for w in result.get("warnings", []))


def test_validate_pipeline_cluster_window_records() -> None:
    from ratatoskr.pipelines.validate_cluster import validate_pipeline_cluster

    result = validate_pipeline_cluster(_session_detect_pipeline())
    assert result["valid"] is True
    assert result["mode"] == "streaming_window"


def test_validate_pipeline_cluster_kafka_requires_window() -> None:
    from ratatoskr.pipelines.models import Pipeline, PipelineNode
    from ratatoskr.pipelines.validate_cluster import validate_pipeline_cluster

    pipeline = Pipeline(
        id="pipe_kafka",
        name="kafka",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"source_type": "kafka", "topic": "workflow.test.input"},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[],
    )
    result = validate_pipeline_cluster(pipeline)
    assert result["valid"] is False


def test_validate_pipeline_reports_disconnected_window() -> None:
    from ratatoskr.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from ratatoskr.pipelines.validate import validate_pipeline

    pipeline = Pipeline(
        id="pipe_disconnected",
        name="disconnected",
        nodes=[
            PipelineNode(id="src1", kind="source", config={"records": [{"src_ip": "1"}]}),
            PipelineNode(
                id="win1",
                kind="window",
                config={"key_field": "src_ip", "gap_policy": "session_detect"},
            ),
            PipelineNode(id="agent_sd", kind="agent", agent="session_detect"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_sd"),
            PipelineEdge(id="e2", source="agent_sd", target="sink1"),
        ],
    )
    result = validate_pipeline(pipeline)
    assert result["valid"] is False
    assert any("window" in err.lower() or "connect" in err.lower() for err in result["errors"])


if __name__ == "__main__":
    test_validate_pipeline_accepts_generic_window_topology()
    test_validate_pipeline_accepts_window_topology()
    test_validate_pipeline_rejects_window_after_agent()
    test_window_local_emits_generic_summaries()
    test_window_local_emits_session_detect_summaries()
    test_generate_cluster_runner_generic_window_compiles()
    test_generate_cluster_runner_window_logic_compiles()
    test_validate_pipeline_warns_mismatched_session_detect_policy()
    test_validate_pipeline_cluster_window_records()
    test_validate_pipeline_cluster_kafka_requires_window()
    test_validate_pipeline_reports_disconnected_window()
    print("OK  pipeline window tests")
