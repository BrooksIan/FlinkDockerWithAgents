"""Compile agent definitions into Python, YAML, and manifest artifacts."""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apemosyne.designer.definitions.models import AgentDefinition, AgentDefinitionNode
from apemosyne.designer.definitions.validate import validate_agent_definition
from apemosyne.paths import project_root
from apemosyne.tools.builtins import get_builtin_tool


class CompileError(ValueError):
    """Agent definition cannot be compiled."""


@dataclass(frozen=True)
class CompiledArtifact:
    path: str
    content: str


@dataclass(frozen=True)
class CompileResult:
    definition_id: str
    agent_slug: str
    class_name: str
    output_dir: str
    files: tuple[CompiledArtifact, ...]
    validation: dict[str, Any]


def compiled_agents_dir(root: Path | None = None) -> Path:
    repo = root or project_root()
    directory = repo / ".apemosyne" / "agents"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def compile_agent_definition(
    definition: AgentDefinition,
    *,
    root: Path | None = None,
    write_files: bool = True,
) -> CompileResult:
    validation = validate_agent_definition(definition)
    if not validation["valid"]:
        raise CompileError(
            "Definition is invalid: " + "; ".join(validation.get("errors") or [])
        )

    if definition.type == "workflow":
        files = _compile_workflow(definition)
    elif definition.type == "react":
        raise CompileError("ReAct agent codegen is not implemented yet (Phase 5)")
    else:
        raise CompileError(f"Unsupported agent type {definition.type!r}")

    agent_slug = _agent_slug(definition)
    class_name = _class_name(definition.name)
    output_dir = compiled_agents_dir(root) / definition.id

    if write_files:
        output_dir.mkdir(parents=True, exist_ok=True)
        for artifact in files:
            target = output_dir / artifact.path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(artifact.content, encoding="utf-8")

    return CompileResult(
        definition_id=definition.id,
        agent_slug=agent_slug,
        class_name=class_name,
        output_dir=str(output_dir.relative_to(root or project_root())),
        files=tuple(files),
        validation=validation,
    )


def compile_result_to_dict(result: CompileResult) -> dict[str, Any]:
    return {
        "definition_id": result.definition_id,
        "agent_slug": result.agent_slug,
        "class_name": result.class_name,
        "output_dir": result.output_dir,
        "status": "compiled",
        "validation": result.validation,
        "files": [
            {"path": artifact.path, "content": artifact.content}
            for artifact in result.files
        ],
    }


def _compile_workflow(definition: AgentDefinition) -> list[CompiledArtifact]:
    action = _single_node(definition, "action")
    tools = _tools_for_action(definition, action)
    input_field = _primary_input_field(definition)
    agent_slug = _agent_slug(definition)
    class_name = _class_name(definition.name)
    module_name = f"generated_{definition.id}"

    agent_py = _render_agent_module(
        definition=definition,
        class_name=class_name,
        action=action,
        tools=tools,
        input_field=input_field,
        agent_slug=agent_slug,
    )
    actions_py = _render_actions_module(
        definition=definition,
        action=action,
        tools=tools,
        input_field=input_field,
        agent_slug=agent_slug,
    )
    agent_yaml = _render_flink_yaml(definition, action, tools, module_name)
    manifest_snippet = _render_manifest_snippet(
        definition, class_name, module_name, agent_slug
    )
    run_local = _render_run_local(definition.id, class_name)

    return [
        CompiledArtifact("agent.py", agent_py),
        CompiledArtifact("agent_actions.py", actions_py),
        CompiledArtifact("agent.yaml", agent_yaml),
        CompiledArtifact("manifest_snippet.yaml", manifest_snippet),
        CompiledArtifact("run_local.py", run_local),
    ]


def _single_node(definition: AgentDefinition, kind: str) -> AgentDefinitionNode:
    matches = [n for n in definition.nodes if n.kind == kind]
    if len(matches) != 1:
        raise CompileError(f"Expected exactly one {kind} node")
    return matches[0]


def _tools_for_action(
    definition: AgentDefinition, action: AgentDefinitionNode
) -> list[AgentDefinitionNode]:
    tool_ids = {
        edge.target
        for edge in definition.edges
        if edge.kind == "calls" and edge.source == action.id
    }
    by_id = {node.id: node for node in definition.nodes}
    tools = [by_id[tid] for tid in tool_ids if tid in by_id]
    if not tools:
        raise CompileError(f"Action {action.name!r} must call at least one tool")
    return tools


def _primary_input_field(definition: AgentDefinition) -> str:
    required = definition.input_schema.get("required") or []
    if required:
        return str(required[0])
    properties = definition.input_schema.get("properties") or {}
    if properties:
        return next(iter(properties))
    return "value"


def _agent_slug(definition: AgentDefinition) -> str:
    if definition.manifest_name:
        return definition.manifest_name
    slug = re.sub(r"[^a-z0-9_]+", "_", definition.id.lower()).strip("_")
    return slug or definition.id


def _class_name(name: str) -> str:
    parts = re.sub(r"[^a-zA-Z0-9]+", " ", name).title().split()
    base = "".join(parts) or "Generated"
    return f"{base}Agent"


def _tool_body(tool: AgentDefinitionNode) -> str:
    config = tool.config or {}
    expression = str(config.get("expression") or "").strip()
    if expression:
        return f"return {expression}"
    tool_ref = str(config.get("tool_ref") or tool.name).strip()
    builtin = get_builtin_tool(tool_ref)
    if tool_ref == "scale":
        factor = int(config.get("factor") or 2)
        return f"return value * {factor}"
    return str(builtin["body"])


def _tool_description(tool: AgentDefinitionNode) -> str:
    config = tool.config or {}
    tool_ref = str(config.get("tool_ref") or tool.name).strip()
    try:
        return str(get_builtin_tool(tool_ref).get("description") or f"{tool.name} tool")
    except KeyError:
        return f"{tool.name} tool"


def _output_mapping(
    definition: AgentDefinition,
    *,
    tools: list[AgentDefinitionNode],
    input_var: str,
    result_var: str,
    agent_slug: str,
) -> list[tuple[str, str]]:
    properties = definition.output_schema.get("properties") or {}
    if not properties:
        return [("output", result_var), ("agent", f'"{agent_slug}"')]

    mapping: list[tuple[str, str]] = []
    input_field = _primary_input_field(definition)
    for key in properties:
        if key == "agent":
            mapping.append((key, f'"{agent_slug}"'))
        elif key in ("input", input_field):
            mapping.append((key, input_var))
        elif len(tools) == 1 and key in {tools[0].name, "doubled", "output", "result"}:
            mapping.append((key, result_var))
        else:
            mapping.append((key, result_var))
    return mapping


def _render_agent_module(
    *,
    definition: AgentDefinition,
    class_name: str,
    action: AgentDefinitionNode,
    tools: list[AgentDefinitionNode],
    input_field: str,
    agent_slug: str,
) -> str:
    tool_methods: list[str] = []
    for tool in tools:
        body = _tool_body(tool)
        doc = _tool_description(tool)
        tool_methods.append(
            textwrap.dedent(
                f"""
                @tool
                @staticmethod
                def {tool.name}(value: int) -> int:
                    \"\"\"{doc}\"\"\"
                    {body}
                """
            ).strip()
        )

    primary_tool = tools[0].name
    output_lines = _output_mapping(
        definition,
        tools=tools,
        input_var="n",
        result_var="result",
        agent_slug=agent_slug,
    )
    output_dict = ", ".join(f'"{k}": {v}' for k, v in output_lines)

    return textwrap.dedent(
        f'''\
        """Generated workflow agent — {definition.name}."""

        from __future__ import annotations

        from flink_agents.api.agents.agent import Agent
        from flink_agents.api.decorators import action, tool
        from flink_agents.api.events.event import Event, InputEvent, OutputEvent
        from flink_agents.api.runner_context import RunnerContext

        _INPUT_EVENT = InputEvent.EVENT_TYPE


        def _int_from_input(event: Event, *, field: str = "{input_field}") -> int:
            payload = InputEvent.from_event(event).input
            if isinstance(payload, dict):
                raw = payload.get(field, 0)
            else:
                raw = getattr(payload, field, payload)
            return int(raw)


        class {class_name}(Agent):
            """{definition.description or definition.name}"""

            {"\n\n    ".join(tool_methods)}

            @action(_INPUT_EVENT)
            @staticmethod
            def {action.name}(event: Event, ctx: RunnerContext) -> None:
                n = _int_from_input(event)
                result = {class_name}.{primary_tool}(n)
                ctx.send_event(
                    OutputEvent(
                        output={{{output_dict}}}
                    )
                )
        '''
    ).strip() + "\n"


def _render_actions_module(
    *,
    definition: AgentDefinition,
    action: AgentDefinitionNode,
    tools: list[AgentDefinitionNode],
    input_field: str,
    agent_slug: str,
) -> str:
    tool_functions: list[str] = []
    for tool in tools:
        body = _tool_body(tool)
        doc = _tool_description(tool)
        tool_functions.append(
            textwrap.dedent(
                f"""
                def {tool.name}(value: int) -> int:
                    \"\"\"{doc}\"\"\"
                    {body}
                """
            ).strip()
        )

    primary_tool = tools[0].name
    output_lines = _output_mapping(
        definition,
        tools=tools,
        input_var="n",
        result_var="result",
        agent_slug=agent_slug,
    )
    output_dict = ", ".join(f'"{k}": {v}' for k, v in output_lines)

    return textwrap.dedent(
        f'''\
        """Generated module-level actions for Flink YAML."""

        from __future__ import annotations

        from flink_agents.api.events.event import Event, InputEvent, OutputEvent
        from flink_agents.api.runner_context import RunnerContext

        {"\n\n".join(tool_functions)}


        def _int_from_input(event: Event, *, field: str = "{input_field}") -> int:
            payload = InputEvent.from_event(event).input
            if isinstance(payload, dict):
                raw = payload.get(field, 0)
            else:
                raw = getattr(payload, field, payload)
            return int(raw)


        def {action.name}(event: Event, ctx: RunnerContext) -> None:
            n = _int_from_input(event)
            result = {primary_tool}(n)
            ctx.send_event(
                OutputEvent(
                    output={{{output_dict}}}
                )
            )
        '''
    ).strip() + "\n"


def _render_flink_yaml(
    definition: AgentDefinition,
    action: AgentDefinitionNode,
    tools: list[AgentDefinitionNode],
    module_name: str,
) -> str:
    listens = action.config.get("listens_to") or ["input"]
    listen_yaml = ", ".join(str(item) for item in listens)
    tool_entries = "\n".join(
        textwrap.dedent(
            f"""
              - name: {tool.name}
                function: .apemosyne.agents.{definition.id}.agent_actions:{tool.name}
                type: python"""
        ).rstrip()
        for tool in tools
    )
    return textwrap.dedent(
        f"""\
        # Generated Flink Agents YAML for {definition.name}
        agents:
          - name: { _agent_slug(definition) }
            description: {definition.description or definition.name}

            actions:
              - name: {action.name}
                function: .apemosyne.agents.{definition.id}.agent_actions:{action.name}
                listen_to: [{listen_yaml}]
                type: python

            tools:
        {tool_entries}
        """
    ).strip() + "\n"


def _render_manifest_snippet(
    definition: AgentDefinition,
    class_name: str,
    module_name: str,
    agent_slug: str,
) -> str:
    return textwrap.dedent(
        f"""\
        # Manifest snippet — merge into examples/agents/agent-manifest.yaml on publish
        agents:
          {agent_slug}:
            type: workflow
            description: {definition.description or definition.name}
            entry: generated.{definition.id}.agent:{class_name}
            runner: .apemosyne/agents/{definition.id}/run_local.py
            flink_yaml: .apemosyne/agents/{definition.id}/agent.yaml
        """
    ).strip() + "\n"


def _render_run_local(definition_id: str, class_name: str) -> str:
    return textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        """Local runner for generated agent `{definition_id}`."""

        from __future__ import annotations

        import importlib.util
        import sys
        from pathlib import Path


        def _bootstrap() -> Path:
            repo = Path(__file__).resolve().parents[3]
            if str(repo) not in sys.path:
                sys.path.insert(0, str(repo))
            return repo


        def _load_agent_class():
            repo = _bootstrap()
            module_path = repo / ".apemosyne" / "agents" / "{definition_id}" / "agent.py"
            spec = importlib.util.spec_from_file_location("generated_agent", module_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Cannot load generated agent from {{module_path}}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, "{class_name}")


        def main() -> int:
            from flink_agents.api.execution_environment import AgentsExecutionEnvironment

            agent_cls = _load_agent_class()
            env = AgentsExecutionEnvironment.get_execution_environment()
            input_data = [{{"key": "1", "value": 3}}, {{"key": "2", "value": 10}}]
            output_data = env.from_list(input_data).apply(agent_cls()).to_list()
            env.execute()
            print("Generated agent results:")
            for record in output_data:
                print(record)
            return 0


        if __name__ == "__main__":
            raise SystemExit(main())
        '''
    ).strip() + "\n"
