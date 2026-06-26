#!/usr/bin/env python3
"""Published agent container copy tests."""

from __future__ import annotations

from pathlib import Path


def test_published_agent_artifact_pairs_includes_generated_files() -> None:
    from apemosyne.agents.published_copy import published_agent_artifact_pairs
    from apemosyne.agents.registry import AgentSpec

    root = Path(__file__).resolve().parents[1]
    spec = AgentSpec(
        name="basicreact",
        type="react",
        entry="examples.agents.published_shims.basicreact:BasicreactAgent",
        module="examples.agents.published_shims.basicreact",
        class_name="BasicreactAgent",
        runner=".apemosyne/agents/def_a8888ce93ad3/run_local.py",
        cluster_script="",
        description="test",
    )
    pairs = published_agent_artifact_pairs(root, spec)
    if not (root / spec.runner).is_file():
        return

    remotes = {remote for _, remote in pairs}
    assert "/opt/flink/.apemosyne/agents/def_a8888ce93ad3/agent.py" in remotes
    assert "/opt/flink/.apemosyne/agents/def_a8888ce93ad3/agent_logic.py" in remotes
    assert "/opt/flink/examples/agents/published_shims/basicreact.py" in remotes
    assert (
        "/opt/flink/pythonpath/agent-site-packages/apemosyne_published_def_a8888ce93ad3.py"
        in remotes
    )


def test_pipeline_copy_pairs_includes_published_react_designer_files() -> None:
    from apemosyne.pipelines.docker_runner import _pipeline_copy_pairs
    from apemosyne.pipelines.models import Pipeline, PipelineNode

    root = Path(__file__).resolve().parents[1]
    runner = root / ".apemosyne/agents/def_a8888ce93ad3/run_local.py"
    if not runner.is_file():
        return

    pipeline = Pipeline(
        id="pipe_test",
        name="test",
        nodes=[
            PipelineNode(id="src1", kind="source", config={"records": [{"key": "1", "value": 1}]}),
            PipelineNode(id="agent1", kind="agent", agent="basicreact"),
            PipelineNode(id="sink1", kind="sink"),
        ],
        edges=[],
    )
    pairs = _pipeline_copy_pairs(root, pipeline)
    remotes = {remote for _, remote in pairs}
    assert "/opt/flink/apemosyne/flink_rest.py" in remotes
    assert "/opt/flink/apemosyne/agents/published_copy.py" in remotes
    assert "/opt/flink/apemosyne/designer/llm_client.py" in remotes
    assert "/opt/flink/.apemosyne/agents/def_a8888ce93ad3/agent.py" in remotes


def test_published_shim_resolves_repo_root() -> None:
    root = Path(__file__).resolve().parents[1]
    shim = root / "examples/agents/published_shims/basicreact.py"
    agent_py = root / ".apemosyne/agents/def_a8888ce93ad3/agent.py"
    if not shim.is_file() or not agent_py.is_file():
        return

    repo_from_shim = shim.resolve().parents[3]
    assert repo_from_shim == root
    assert (repo_from_shim / ".apemosyne/agents/def_a8888ce93ad3/agent.py").is_file()


if __name__ == "__main__":
    test_published_agent_artifact_pairs_includes_generated_files()
    test_pipeline_copy_pairs_includes_published_react_designer_files()
    test_published_shim_resolves_repo_root()
    print("OK  published artifact pairs")
    print("PASS")
