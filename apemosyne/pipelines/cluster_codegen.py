"""Generate PyFlink cluster runner scripts for linear Studio pipelines."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from apemosyne.agents.published_copy import published_cluster_import_line
from apemosyne.agents.registry import get_agent_spec
from apemosyne.pipelines.executor import linear_execution_order
from apemosyne.pipelines.models import Pipeline
from apemosyne.paths import project_root


def cluster_job_name(pipeline: Pipeline) -> str:
    return f"Apemosyne Pipeline: {pipeline.name} [{pipeline.id}]"


def cluster_runner_relpath(pipeline_id: str) -> str:
    return f".apemosyne/pipelines/{pipeline_id}/run_cluster.py"


def compiled_pipeline_dir(pipeline_id: str, *, root: Path | None = None) -> Path:
    repo = root or project_root()
    directory = repo / ".apemosyne" / "pipelines" / pipeline_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_cluster_runner(
    pipeline: Pipeline,
    *,
    root: Path | None = None,
) -> Path:
    """Generate and persist ``run_cluster.py`` for a pipeline."""
    repo = root or project_root()
    content = generate_cluster_runner(pipeline, root=repo)
    target = compiled_pipeline_dir(pipeline.id, root=repo) / "run_cluster.py"
    target.write_text(content, encoding="utf-8")
    return target


def generate_cluster_runner(pipeline: Pipeline, *, root: Path | None = None) -> str:
    repo = root or project_root()
    by_id = {n.id: n for n in pipeline.nodes}
    edge_by_target = {e.target: e for e in pipeline.edges}
    order = linear_execution_order(pipeline)

    source = next(n for n in pipeline.nodes if n.kind == "source")
    records = list(source.config.get("records") or [])
    if not records:
        raise ValueError("Source node has no input records")

    agent_nodes = [by_id[nid] for nid in order if by_id[nid].kind == "agent"]
    imports: list[str] = []
    for node in agent_nodes:
        if not node.agent:
            raise ValueError(f"Agent node {node.id!r} missing agent name")
        spec = get_agent_spec(node.agent, root=repo)
        imports.append(published_cluster_import_line(spec))

    sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)

    map_functions: list[str] = []
    apply_blocks: list[str] = []
    stream_var = "stream"
    prev_kind = "source"

    for node_id in order:
        node = by_id[node_id]
        if node.kind == "source":
            continue
        if node.kind == "agent":
            if not node.agent:
                continue
            spec = get_agent_spec(node.agent, root=repo)
            if prev_kind == "agent":
                edge = edge_by_target.get(node_id)
                if edge and edge.mapping:
                    fn_name = _safe_fn_name(f"map_{edge.id}")
                    mapping_literal = repr(edge.mapping)
                    map_functions.append(
                        f"def {fn_name}(row):\n"
                        f"    from apemosyne.pipelines.executor import apply_edge_mapping\n"
                        f"    return apply_edge_mapping([row], {mapping_literal})[0]\n"
                    )
                    apply_blocks.append(f"    {stream_var} = {stream_var}.map({fn_name})")
                else:
                    fn_name = _safe_fn_name(f"passthrough_{node_id}")
                    map_functions.append(
                        f"def {fn_name}(row):\n"
                        f"    key = str(row.get('key') or row.get('k') or '1')\n"
                        f"    if 'output' in row:\n"
                        f"        return {{'key': key, 'value': row['output']}}\n"
                        f"    if 'value' in row:\n"
                        f"        return {{'key': key, 'value': row['value']}}\n"
                        f"    return row\n"
                    )
                    apply_blocks.append(f"    {stream_var} = {stream_var}.map({fn_name})")

            apply_blocks.append(
                f"    {stream_var} = agents_env.from_datastream(\n"
                f"        input={stream_var},\n"
                f"        key_selector=lambda row: str(row.get('key') or row.get('k') or '1'),\n"
                f"    ).apply({spec.class_name}()).to_datastream()"
            )
            prev_kind = "agent"
            continue

        if node.kind == "sink":
            apply_blocks.append(f"    {stream_var}.print()")
            prev_kind = "sink"

    job_name = cluster_job_name(pipeline)
    lines = [
        '#!/usr/bin/env python3',
        f'"""Cluster runner for pipeline {pipeline.name!r} ({pipeline.id}). Auto-generated."""',
        "",
        "from __future__ import annotations",
        "",
        "import sys",
        "from pathlib import Path",
        "",
        "",
        "def _bootstrap() -> None:",
        '    root = Path("/opt/flink")',
        "    if root.is_dir():",
        "        if str(root) not in sys.path:",
        "            sys.path.insert(0, str(root))",
        "        return",
        "    repo = Path(__file__).resolve().parents[3]",
        "    if str(repo) not in sys.path:",
        "        sys.path.insert(0, str(repo))",
        "",
        "",
        f"RECORDS = {json.dumps(records, indent=4)}",
        "",
    ]
    if map_functions:
        lines.extend(map_functions)
        lines.append("")

    lines.extend(
        [
            "def main() -> None:",
            "    _bootstrap()",
            "    from apemosyne.runtime.flink_agents_bootstrap import patch_flink_agents_version",
            "",
            "    patch_flink_agents_version()",
            "    from pyflink.datastream import StreamExecutionEnvironment",
            "    from flink_agents.api.execution_environment import AgentsExecutionEnvironment",
            "",
        ]
    )
    for imp in sorted(set(imports)):
        lines.append(f"    {imp}")
    lines.extend(
        [
            "",
            "    env = StreamExecutionEnvironment.get_execution_environment()",
            "    env.set_parallelism(1)",
            "    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)",
            "",
            "    stream = env.from_collection(RECORDS)",
        ]
    )
    lines.extend(apply_blocks)
    lines.append(f'    agents_env.execute({job_name!r})')
    lines.extend(
        [
            "",
            "",
            'if __name__ == "__main__":',
            "    main()",
            "",
        ]
    )
    return "\n".join(lines)


def _safe_fn_name(raw: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"fn_{cleaned}"
    return cleaned


def pipeline_execution_plan(pipeline: Pipeline) -> list[dict[str, Any]]:
    """Declarative steps for pipeline run detail UI."""
    by_id = {n.id: n for n in pipeline.nodes}
    steps: list[dict[str, Any]] = []
    for node_id in linear_execution_order(pipeline):
        node = by_id[node_id]
        if node.kind == "source":
            source_type = str(node.config.get("source_type") or "records").strip().lower()
            steps.append(
                {
                    "kind": "source",
                    "name": "source",
                    "description": f"Source ({source_type})",
                }
            )
        elif node.kind == "agent":
            steps.append(
                {
                    "kind": "agent",
                    "name": node.agent or node.id,
                    "description": f"Agent {node.agent}",
                }
            )
        elif node.kind == "sink":
            sink_type = str(node.config.get("sink_type") or "capture").strip().lower()
            steps.append(
                {
                    "kind": "sink",
                    "name": "capture" if sink_type != "kafka" else str(node.config.get("topic") or "kafka"),
                    "description": f"Sink ({sink_type})",
                }
            )
    return steps
