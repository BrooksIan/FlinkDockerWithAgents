"""Generate PyFlink cluster runner scripts for linear Studio pipelines."""

from __future__ import annotations

import json
import re
import textwrap
from pathlib import Path
from typing import Any

from apemosyne.agents.published_copy import published_cluster_import_line
from apemosyne.agents.registry import get_agent_spec
from apemosyne.pipelines.executor import linear_execution_order
from apemosyne.pipelines.models import Pipeline
from apemosyne.pipelines.window_config import (
    EXECUTION_AGENT_BRIDGE,
    parse_window_config,
    pipeline_window_node,
)
from apemosyne.pipelines.window_codegen import (
    bridge_topic_for_pipeline,
    cluster_agent_logic_block,
    cluster_kafka_sink_block,
    cluster_source_block,
    cluster_window_block,
    window_uses_logic_agent,
)
from apemosyne.paths import project_root


def _indent_code_block(block: str, *, spaces: int = 4) -> list[str]:
    """Normalize and indent a codegen block for insertion inside ``main()``."""
    prefix = " " * spaces
    normalized = textwrap.dedent(block).strip("\n")
    return [f"{prefix}{line}" if line else "" for line in normalized.splitlines()]


def cluster_job_name(pipeline: Pipeline) -> str:
    return f"Apemosyne Pipeline: {pipeline.name} [{pipeline.id}]"


def cluster_runner_relpath(pipeline_id: str) -> str:
    return f".apemosyne/pipelines/{pipeline_id}/run_cluster.py"


def cluster_bridge_window_relpath(pipeline_id: str) -> str:
    return f".apemosyne/pipelines/{pipeline_id}/run_cluster_window.py"


def cluster_bridge_agent_relpath(pipeline_id: str) -> str:
    return f".apemosyne/pipelines/{pipeline_id}/run_cluster_agent.py"


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
    window_node = pipeline_window_node(pipeline)
    window_config = parse_window_config(window_node.config if window_node else None)
    if window_node and window_config.execution_mode == EXECUTION_AGENT_BRIDGE:
        window_path = compiled_pipeline_dir(pipeline.id, root=repo) / "run_cluster_window.py"
        agent_path = compiled_pipeline_dir(pipeline.id, root=repo) / "run_cluster_agent.py"
        window_path.write_text(generate_cluster_bridge_window_runner(pipeline, root=repo), encoding="utf-8")
        agent_path.write_text(generate_cluster_bridge_agent_runner(pipeline, root=repo), encoding="utf-8")
    content = generate_cluster_runner(pipeline, root=repo)
    target = compiled_pipeline_dir(pipeline.id, root=repo) / "run_cluster.py"
    target.write_text(content, encoding="utf-8")
    return target


def _bootstrap_lines() -> list[str]:
    return [
        '#!/usr/bin/env python3',
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
    ]


def generate_cluster_runner(pipeline: Pipeline, *, root: Path | None = None) -> str:
    repo = root or project_root()
    window_node = pipeline_window_node(pipeline)
    if window_node is not None:
        window_config = parse_window_config(window_node.config)
        if window_config.execution_mode == EXECUTION_AGENT_BRIDGE:
            return _generate_cluster_bridge_orchestrator(pipeline, root=repo)
        return _generate_cluster_window_runner(pipeline, root=repo, bridge_sink=False)

    return _generate_cluster_agents_runner(pipeline, root=repo)


def _generate_cluster_agents_runner(pipeline: Pipeline, *, root: Path | None = None) -> str:
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
    lines = _bootstrap_lines()
    lines.append(f'"""Cluster runner for pipeline {pipeline.name!r} ({pipeline.id}). Auto-generated."""')
    lines.append("")
    lines.append(f"RECORDS = {json.dumps(records, indent=4)}")
    lines.append("")
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
    lines.extend(["", "", 'if __name__ == "__main__":', "    main()", ""])
    return "\n".join(lines)


def _generate_cluster_window_runner(
    pipeline: Pipeline,
    *,
    root: Path | None = None,
    bridge_sink: bool,
) -> str:
    repo = root or project_root()
    by_id = {n.id: n for n in pipeline.nodes}
    edge_by_target = {e.target: e for e in pipeline.edges}
    order = linear_execution_order(pipeline)

    source = next(n for n in pipeline.nodes if n.kind == "source")
    source_type = str(source.config.get("source_type") or "records").strip().lower()
    records = list(source.config.get("records") or [])
    if source_type != "kafka" and not records:
        raise ValueError("Source node has no input records")

    window_node = pipeline_window_node(pipeline)
    if window_node is None:
        raise ValueError("Pipeline has no window node")
    window_config = parse_window_config(window_node.config)

    extra_imports: set[str] = set()
    source_imports, source_block = cluster_source_block(
        source_type=source_type,
        records_literal=json.dumps(records, indent=4),
        kafka_topic=str(source.config.get("topic") or "").strip() or None,
        key_field=window_config.key_field,
    )
    extra_imports.update(source_imports)

    window_imports, window_block = cluster_window_block(window_config)
    extra_imports.update(window_imports)

    apply_blocks: list[str] = [source_block, window_block]
    uses_agents_env = False
    uses_kafka_jars = source_type == "kafka" or bridge_sink
    agent_imports: list[str] = []

    for node_id in order:
        node = by_id[node_id]
        if node.kind != "agent" or not node.agent:
            continue
        logic = cluster_agent_logic_block(node.agent, window_config)
        if logic and window_uses_logic_agent(pipeline, window_config):
            imports, block = logic
            extra_imports.update(imports)
            apply_blocks.append(block)
            continue

        spec = get_agent_spec(node.agent, root=repo)
        agent_imports.append(published_cluster_import_line(spec))
        uses_agents_env = True
        apply_blocks.append(
            "stream = agents_env.from_datastream(\n"
            "    input=stream,\n"
            "    key_selector=lambda row: str(row.get('key') or '1'),\n"
            f").apply({spec.class_name}()).to_datastream()"
        )

    sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
    if bridge_sink:
        topic = bridge_topic_for_pipeline(pipeline.id, window_config)
        sink_imports, sink_block = cluster_kafka_sink_block(topic)
        extra_imports.update(sink_imports)
        apply_blocks.append(sink_block)
        uses_kafka_jars = True
    elif sink_node is not None:
        sink_type = str(sink_node.config.get("sink_type") or "capture").strip().lower()
        if sink_type == "kafka":
            topic = str(sink_node.config.get("topic") or "").strip() or "workflow.test.output"
            sink_imports, sink_block = cluster_kafka_sink_block(topic)
            extra_imports.update(sink_imports)
            apply_blocks.append(sink_block)
            uses_kafka_jars = True
        else:
            apply_blocks.append("stream.print()")

    job_name = cluster_job_name(pipeline)
    lines = _bootstrap_lines()
    lines.append(f'"""Cluster runner for pipeline {pipeline.name!r} ({pipeline.id}). Auto-generated."""')
    lines.append("")
    if source_type != "kafka":
        lines.append(f"RECORDS = {json.dumps(records, indent=4)}")
        lines.append("")

    lines.append("def main() -> None:")
    lines.append("    _bootstrap()")
    if uses_agents_env:
        lines.append("    from apemosyne.runtime.flink_agents_bootstrap import patch_flink_agents_version")
        lines.append("    patch_flink_agents_version()")
    lines.append("    from pyflink.datastream import StreamExecutionEnvironment")
    if uses_agents_env:
        lines.append("    from flink_agents.api.execution_environment import AgentsExecutionEnvironment")
    for imp in sorted(extra_imports):
        lines.append(f"    {imp}")
    if uses_kafka_jars:
        lines.append("    from apemosyne.runtime.kafka_jars import attach_kafka_jars")
    for imp in sorted(set(agent_imports)):
        lines.append(f"    {imp}")
    lines.extend(
        [
            "",
            "    env = StreamExecutionEnvironment.get_execution_environment()",
            "    env.set_parallelism(1)",
        ]
    )
    if uses_kafka_jars:
        lines.append("    attach_kafka_jars(env)")
    if uses_agents_env:
        lines.append("    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)")
    lines.append("")
    for block in apply_blocks:
        lines.extend(_indent_code_block(block))
    if uses_agents_env:
        lines.append(f"    agents_env.execute({job_name!r})")
    else:
        lines.append(f"    env.execute({job_name!r})")
    lines.extend(["", "", 'if __name__ == "__main__":', "    main()", ""])
    return "\n".join(lines)


def generate_cluster_bridge_window_runner(pipeline: Pipeline, *, root: Path | None = None) -> str:
    return _generate_cluster_window_runner(pipeline, root=root, bridge_sink=True)


def generate_cluster_bridge_agent_runner(pipeline: Pipeline, *, root: Path | None = None) -> str:
    repo = root or project_root()
    window_node = pipeline_window_node(pipeline)
    if window_node is None:
        raise ValueError("Pipeline has no window node")
    window_config = parse_window_config(window_node.config)
    topic = bridge_topic_for_pipeline(pipeline.id, window_config)

    order = linear_execution_order(pipeline)
    by_id = {n.id: n for n in pipeline.nodes}
    agent_nodes = [by_id[nid] for nid in order if by_id[nid].kind == "agent"]
    if not agent_nodes:
        raise ValueError("Bridge agent runner requires an agent node")

    imports: list[str] = []
    apply_blocks: list[str] = []
    for node in agent_nodes:
        if not node.agent:
            continue
        spec = get_agent_spec(node.agent, root=repo)
        imports.append(published_cluster_import_line(spec))
        apply_blocks.append(
            "stream = agents_env.from_datastream(\n"
            "    input=stream,\n"
            "    key_selector=lambda row: str(row.get('key') or '1'),\n"
            f").apply({spec.class_name}()).to_datastream()"
        )

    sink_node = next((n for n in pipeline.nodes if n.kind == "sink"), None)
    if sink_node is not None:
        apply_blocks.append("stream.print()")

    job_name = f"{cluster_job_name(pipeline)} — agent bridge"
    lines = _bootstrap_lines()
    lines.append(f'"""Agent bridge runner for {pipeline.name!r} ({pipeline.id}). Auto-generated."""')
    lines.append("")
    lines.append("def main() -> None:")
    lines.append("    _bootstrap()")
    lines.append("    from apemosyne.runtime.flink_agents_bootstrap import patch_flink_agents_version")
    lines.append("    patch_flink_agents_version()")
    lines.append("    from pyflink.common.serialization import SimpleStringSchema")
    lines.append("    from pyflink.datastream import StreamExecutionEnvironment")
    lines.append("    from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer")
    lines.append("    from flink_agents.api.execution_environment import AgentsExecutionEnvironment")
    lines.append("    from apemosyne.runtime.kafka_jars import attach_kafka_jars")
    lines.append("    from apemosyne.kafka_sources import cluster_kafka_bootstrap_servers")
    lines.append("    import json")
    for imp in sorted(set(imports)):
        lines.append(f"    {imp}")
    lines.extend(
        [
            "",
            "    env = StreamExecutionEnvironment.get_execution_environment()",
            "    attach_kafka_jars(env)",
            "    env.set_parallelism(1)",
            "    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)",
            "    _props = {",
            '        "bootstrap.servers": cluster_kafka_bootstrap_servers(),',
            f'        "group.id": "apemosyne-bridge-{pipeline.id}",',
            '        "auto.offset.reset": "earliest",',
            "    }",
            "    _consumer = FlinkKafkaConsumer(",
            f'        topics="{topic}",',
            "        deserialization_schema=SimpleStringSchema(),",
            "        properties=_props,",
            "    )",
            "    def _parse_line(raw: str):",
            "        try:",
            "            return json.loads(raw)",
            "        except Exception:",
            "            return {'key': 'unknown'}",
            "    stream = env.add_source(_consumer).map(_parse_line)",
        ]
    )
    for block in apply_blocks:
        lines.extend(_indent_code_block(block))
    lines.append(f"    agents_env.execute({job_name!r})")
    lines.extend(["", "", 'if __name__ == "__main__":', "    main()", ""])
    return "\n".join(lines)


def _generate_cluster_bridge_orchestrator(pipeline: Pipeline, *, root: Path | None = None) -> str:
    job_name = cluster_job_name(pipeline)
    lines = _bootstrap_lines()
    lines.append(f'"""Orchestrator for agent-bridge pipeline {pipeline.name!r} ({pipeline.id})."""')
    lines.append("")
    lines.append("def main() -> None:")
    lines.append("    _bootstrap()")
    lines.append("    import runpy")
    lines.append("    from pathlib import Path")
    lines.append("    base = Path(__file__).resolve().parent")
    lines.append('    runpy.run_path(str(base / "run_cluster_window.py"), run_name="__main__")')
    lines.append('    runpy.run_path(str(base / "run_cluster_agent.py"), run_name="__main__")')
    lines.append(f"    print('Submitted bridge pipeline {job_name!r}')")
    lines.extend(["", "", 'if __name__ == "__main__":', "    main()", ""])
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
        elif node.kind == "window":
            from apemosyne.pipelines.window_config import parse_window_config

            parsed = parse_window_config(node.config)
            steps.append(
                {
                    "kind": "window",
                    "name": "window",
                    "description": (
                        f"Dynamic session on {parsed.key_field} ({parsed.gap_policy}, "
                        f"{parsed.execution_mode})"
                    ),
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
