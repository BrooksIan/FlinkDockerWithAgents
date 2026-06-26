#!/usr/bin/env python3
"""Pipeline cluster submit (PR1) tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
import pytest


def _batch_pipeline():
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    return Pipeline(
        id="pipe_cluster_test",
        name="Counter batch",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "capture"}),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(id="e2", source="agent_wc", target="sink1"),
        ],
    )


def test_validate_pipeline_cluster_rejects_kafka_source() -> None:
    from apemosyne.pipelines.models import Pipeline, PipelineNode
    from apemosyne.pipelines.validate_cluster import validate_pipeline_cluster

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
    assert any("Kafka" in err and "source" in err.lower() or "streaming" in err.lower() for err in result["errors"])


def test_validate_pipeline_cluster_allows_kafka_sink() -> None:
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from apemosyne.pipelines.validate_cluster import validate_pipeline_cluster

    pipeline = Pipeline(
        id="pipe_kafka_sink",
        name="kafka sink",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(
                id="sink1",
                kind="sink",
                config={"sink_type": "kafka", "topic": "workflow.test.output"},
            ),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(id="e2", source="agent_wc", target="sink1"),
        ],
    )
    result = validate_pipeline_cluster(pipeline)
    assert result["valid"] is True
    assert result["mode"] == "batch_kafka_sink"


def test_validate_pipeline_cluster_warns_published_react() -> None:
    from pathlib import Path

    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode
    from apemosyne.pipelines.validate_cluster import (
        PUBLISHED_REACT_CLUSTER_WARNING,
        validate_pipeline_cluster,
    )

    root = Path(__file__).resolve().parents[1]
    if not (root / ".apemosyne/agents/def_a8888ce93ad3/agent.py").is_file():
        return

    pipeline = Pipeline(
        id="pipe_pub",
        name="Published",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent1", kind="agent", agent="basicreact"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent1"),
            PipelineEdge(id="e2", source="agent1", target="sink1"),
        ],
    )
    result = validate_pipeline_cluster(pipeline)
    assert result["valid"] is True
    assert any(PUBLISHED_REACT_CLUSTER_WARNING in w for w in result["warnings"])
    assert any("basicreact" in w for w in result["warnings"])


def test_validate_pipeline_includes_cluster_section() -> None:
    import tempfile
    from pathlib import Path

    from apemosyne.pipelines.service import PipelineService, reset_pipeline_service_for_tests
    from apemosyne.pipelines.store import PipelineStore

    reset_pipeline_service_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        service = PipelineService(PipelineStore(Path(tmp) / "pipelines.db"))
        created = service.create(
            name="Counter batch",
            nodes=[
                {
                    "id": "src1",
                    "kind": "source",
                    "config": {"records": [{"key": "1", "value": 1}]},
                },
                {"id": "agent_wc", "kind": "agent", "agent": "workflow_counter"},
                {"id": "sink1", "kind": "sink", "config": {"sink_type": "capture"}},
            ],
            edges=[
                {"id": "e1", "source": "src1", "target": "agent_wc"},
                {"id": "e2", "source": "agent_wc", "target": "sink1"},
            ],
        )
        body = service.validate(created["id"])
        assert "cluster" in body
        assert isinstance(body["cluster"]["warnings"], list)
        assert body["cluster"]["valid"] is True
    reset_pipeline_service_for_tests()


def test_generate_cluster_runner_workflow_counter() -> None:
    from apemosyne.pipelines.cluster_codegen import cluster_job_name, generate_cluster_runner

    pipeline = _batch_pipeline()
    script = generate_cluster_runner(pipeline)
    assert "from examples.agents.workflow_counter import CounterAgent" in script
    assert "env.from_collection(RECORDS)" in script
    assert "stream.print()" in script
    assert cluster_job_name(pipeline) in script


def test_generate_cluster_runner_published_agent_import() -> None:
    from pathlib import Path

    from apemosyne.pipelines.cluster_codegen import generate_cluster_runner
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    root = Path(__file__).resolve().parents[1]
    if not (root / ".apemosyne/agents/def_a8888ce93ad3/agent.py").is_file():
        return

    pipeline = Pipeline(
        id="pipe_pub",
        name="Published",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent1", kind="agent", agent="basicreact"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent1"),
            PipelineEdge(id="e2", source="agent1", target="sink1"),
        ],
    )
    script = generate_cluster_runner(pipeline, root=root)
    assert "from apemosyne_published_def_a8888ce93ad3 import BasicreactAgent" in script
    assert "published_shims" not in script


def test_generate_cluster_runner_kafka_sink_default_topic() -> None:
    from apemosyne.pipelines.cluster_codegen import generate_cluster_runner
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    pipeline = Pipeline(
        id="pipe_ks_default",
        name="Kafka sink default",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="sink1", kind="sink", config={"sink_type": "kafka"}),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(id="e2", source="agent_wc", target="sink1"),
        ],
    )
    script = generate_cluster_runner(pipeline)
    assert "deliver_batch_kafka_sink" not in script
    assert "KafkaSinkAgent" not in script
    assert "stream.print()" in script


def test_generate_cluster_runner_kafka_sink() -> None:
    from apemosyne.pipelines.cluster_codegen import generate_cluster_runner
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    pipeline = Pipeline(
        id="pipe_ks",
        name="Kafka sink",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(
                id="sink1",
                kind="sink",
                config={"sink_type": "kafka", "topic": "workflow.test.output"},
            ),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(id="e2", source="agent_wc", target="sink1"),
        ],
    )
    script = generate_cluster_runner(pipeline)
    assert "deliver_batch_kafka_sink" not in script
    assert "PublishToKafka" not in script
    assert "MapFunction" not in script
    assert "stream.print()" in script


def test_generate_cluster_runner_counter_echo_mapping() -> None:
    from apemosyne.pipelines.cluster_codegen import generate_cluster_runner
    from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

    pipeline = Pipeline(
        id="pipe_ce",
        name="Counter then Echo",
        nodes=[
            PipelineNode(
                id="src1",
                kind="source",
                config={"records": [{"key": "1", "value": 3}]},
            ),
            PipelineNode(id="agent_wc", kind="agent", agent="workflow_counter"),
            PipelineNode(id="agent_re", kind="agent", agent="react_echo"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[
            PipelineEdge(id="e1", source="src1", target="agent_wc"),
            PipelineEdge(
                id="e2",
                source="agent_wc",
                target="agent_re",
                mapping={"message": "$.doubled"},
            ),
            PipelineEdge(id="e3", source="agent_re", target="sink1"),
        ],
    )
    script = generate_cluster_runner(pipeline)
    assert "ReactEchoAgent" in script
    assert "apply_edge_mapping" in script
    assert "$.doubled" in script


def test_pipeline_cluster_submit_api_mocked() -> None:
    from dataclasses import asdict
    from unittest.mock import patch

    os.environ.pop("APEMOSYNE_API_KEY", None)
    from fastapi.testclient import TestClient

    from apemosyne.api.app import create_app
    from apemosyne.api.config import ApiSettings
    from apemosyne.pipelines.cluster_submit import PipelineClusterSubmitResult
    from apemosyne.pipelines.service import PipelineService, reset_pipeline_service_for_tests
    from apemosyne.pipelines.store import PipelineStore

    reset_pipeline_service_for_tests()
    pipeline = _batch_pipeline()
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "pipelines.db"
        os.environ["APEMOSYNE_PIPELINES_DB"] = str(db)
        service = PipelineService(PipelineStore(db))
        created = service.create(
            name=pipeline.name,
            nodes=[asdict(n) for n in pipeline.nodes],
            edges=[asdict(e) for e in pipeline.edges],
        )

        client = TestClient(
            create_app(ApiSettings(api_key=None, flink_rest_host="127.0.0.1", flink_rest_port=1))
        )

        with patch(
            "apemosyne.pipelines.service.submit_pipeline_cluster",
            return_value=PipelineClusterSubmitResult(
                run_id="run_test123",
                return_code=0,
                flink_job_id="job_abc",
                validation={"valid": True, "errors": [], "warnings": [], "mode": "batch"},
            ),
        ):
            response = client.post(f"/v1/pipelines/{created['id']}/submit")

        assert response.status_code == 200
        body = response.json()
        assert body["run_id"] == "run_test123"
        assert body["flink_job_id"] == "job_abc"
        assert body["status"] == "submitted"

        del os.environ["APEMOSYNE_PIPELINES_DB"]
        reset_pipeline_service_for_tests()


if __name__ == "__main__":
    test_validate_pipeline_cluster_rejects_kafka_source()
    test_validate_pipeline_cluster_allows_kafka_sink()
    test_generate_cluster_runner_workflow_counter()
    test_generate_cluster_runner_counter_echo_mapping()
    print("OK  pipeline cluster submit tests")
    print("PASS")
