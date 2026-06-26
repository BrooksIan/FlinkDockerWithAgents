"""Compile agent definitions into Python, YAML, and manifest artifacts."""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ratatoskr.designer.definitions.models import AgentDefinition, AgentDefinitionNode
from ratatoskr.designer.definitions.validate import validate_agent_definition
from ratatoskr.designer.skills_catalog import react_llm_config
from ratatoskr.paths import project_root
from ratatoskr.tools.builtins import get_builtin_tool


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
    directory = repo / ".ratatoskr" / "agents"
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
        files = _compile_react(definition)
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


def _tools_for_action_optional(
    definition: AgentDefinition, action: AgentDefinitionNode
) -> list[AgentDefinitionNode]:
    tool_ids = {
        edge.target
        for edge in definition.edges
        if edge.kind == "calls" and edge.source == action.id
    }
    by_id = {node.id: node for node in definition.nodes}
    return [
        by_id[tid]
        for tid in tool_ids
        if tid in by_id and by_id[tid].kind in ("tool", "mcp_tool")
    ]


def _react_prompts(prompt_node: AgentDefinitionNode | None) -> tuple[str, str]:
    default_system = (
        "Extract the numeric input value from the user message and compute doubled = input * 2.\n\n"
        "Ensure your response can be parsed by Python json, using this format as an example:\n"
        '{"input": 7, "doubled": 14, "reasoning": "Identified 7 as the input and doubled it."}\n\n'
        "Respond with JSON only."
    )
    default_user = '"message": {message},\n"value": {value}'
    if prompt_node is None:
        return default_system, default_user
    config = prompt_node.config or {}
    system = str(config.get("system") or "").strip()
    user = str(config.get("user") or "").strip()
    system = system or default_system
    user = user or default_user
    return _ensure_json_system_prompt(system), user


def _react_skills_system_prompt(prompt_node: AgentDefinitionNode | None) -> str:
    default_system = (
        "You are a helpful assistant with access to Flink Agent skills. "
        "When a skill applies, you must load it first with load_skill and strictly "
        "follow its instructions."
    )
    if prompt_node is None:
        return default_system
    config = prompt_node.config or {}
    system = str(config.get("system") or "").strip()
    return system or default_system


def _render_react_skills_agent_module(
    *,
    definition: AgentDefinition,
    class_name: str,
    agent_slug: str,
    system_prompt: str,
    skills: list[str],
    allowed_commands: list[str],
) -> str:
    skills_literal = repr(list(skills))
    commands_literal = repr(list(allowed_commands))
    return textwrap.dedent(
        f'''\
        """Generated ReAct skills agent — {definition.name}."""

        from __future__ import annotations

        from flink_agents.api.agents.agent import Agent
        from flink_agents.api.chat_message import ChatMessage, MessageRole
        from flink_agents.api.decorators import (
            action,
            chat_model_connection,
            chat_model_setup,
            prompt,
            skills,
        )
        from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
        from flink_agents.api.events.event import Event, InputEvent, OutputEvent
        from flink_agents.api.prompts.prompt import Prompt
        from flink_agents.api.resource import ResourceDescriptor
        from flink_agents.api.runner_context import RunnerContext
        from flink_agents.api.skills import Skills

        from examples.agents.react_skills_paths import examples_skills_dir

        _INPUT_EVENT = InputEvent.EVENT_TYPE
        _SYSTEM_PROMPT = {system_prompt!r}


        class {class_name}(Agent):
            """{definition.description or definition.name}"""

            @skills
            @staticmethod
            def platform_skills() -> Skills:
                return Skills.from_local_dir(str(examples_skills_dir()))

            @prompt
            @staticmethod
            def system_prompt() -> Prompt:
                return Prompt.from_messages(
                    messages=[
                        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM_PROMPT),
                    ],
                )

            @chat_model_connection
            @staticmethod
            def designer_llm_connection() -> ResourceDescriptor:
                from ratatoskr.designer.flink_llm import react_llm_connection_descriptor

                return react_llm_connection_descriptor()

            @chat_model_setup
            @staticmethod
            def skills_model() -> ResourceDescriptor:
                from ratatoskr.designer.flink_llm import react_skills_chat_model_descriptor

                return react_skills_chat_model_descriptor(
                    connection="designer_llm_connection",
                    prompt="system_prompt",
                    skills={skills_literal},
                    allowed_commands={commands_literal},
                )

            @action(_INPUT_EVENT)
            @staticmethod
            def process_input(event: Event, ctx: RunnerContext) -> None:
                payload = InputEvent.from_event(event).input
                if isinstance(payload, dict):
                    question = str(
                        payload.get("message") or payload.get("question") or payload
                    )
                else:
                    question = str(payload)
                ctx.send_event(
                    ChatRequestEvent(
                        model="skills_model",
                        messages=[ChatMessage(role=MessageRole.USER, content=question)],
                    )
                )

            @action(ChatResponseEvent.EVENT_TYPE)
            @staticmethod
            def process_chat_response(event: Event, ctx: RunnerContext) -> None:
                chat_response = ChatResponseEvent.from_event(event)
                answer = chat_response.response.content
                ctx.send_event(
                    OutputEvent(
                        output={{
                            "answer": answer,
                            "result": answer,
                            "agent": "{agent_slug}",
                        }}
                    )
                )
        '''
    ).strip() + "\n"


_JSON_RESPONSE_SUFFIX = (
    "\n\nRespond with a single JSON object only (no markdown fences or surrounding prose). "
    'Include at least: "input" (number), "result" (string), "reasoning" (string). '
    "You may include additional JSON fields for your task."
)


def _ensure_json_system_prompt(system: str) -> str:
    lower = system.lower()
    if "json" in lower and ("{" in system or "respond with" in lower):
        return system
    return system.rstrip() + _JSON_RESPONSE_SUFFIX


def _compile_react(definition: AgentDefinition) -> list[CompiledArtifact]:
    action = _single_node(definition, "action")
    tools = _tools_for_action_optional(definition, action)
    prompt_nodes = [n for n in definition.nodes if n.kind == "prompt"]
    llm_nodes = [n for n in definition.nodes if n.kind == "llm_call"]
    if not prompt_nodes and not llm_nodes:
        raise CompileError("ReAct agent must have at least one prompt or llm_call node")

    prompt_node = prompt_nodes[0] if prompt_nodes else None
    llm_node = llm_nodes[0] if llm_nodes else None
    llm_config = react_llm_config(llm_node)
    use_platform_llm = llm_config["use_platform_llm"]
    class_name = _class_name(definition.name)
    agent_slug = _agent_slug(definition)

    if llm_config["mode"] == "flink_skills":
        system_prompt = _react_skills_system_prompt(prompt_node)
        agent_py = _render_react_skills_agent_module(
            definition=definition,
            class_name=class_name,
            agent_slug=agent_slug,
            system_prompt=system_prompt,
            skills=llm_config["skills"],
            allowed_commands=llm_config["allowed_commands"],
        )
        manifest_snippet = _render_react_manifest_snippet(
            definition, class_name, agent_slug
        )
        run_local = _render_react_run_local(
            definition.id,
            class_name,
            definition,
            skills_mode=True,
        )
        return [
            CompiledArtifact("agent.py", agent_py),
            CompiledArtifact("manifest_snippet.yaml", manifest_snippet),
            CompiledArtifact("run_local.py", run_local),
        ]

    system_prompt, user_prompt = _react_prompts(prompt_node)

    agent_logic = _render_react_logic_module(
        definition=definition,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        use_platform_llm=use_platform_llm,
    )
    agent_py = _render_react_agent_module(
        definition=definition,
        class_name=class_name,
        action=action,
        tools=tools,
        agent_slug=agent_slug,
    )
    manifest_snippet = _render_react_manifest_snippet(
        definition, class_name, agent_slug
    )
    run_local = _render_react_run_local(definition.id, class_name, definition)

    return [
        CompiledArtifact("agent.py", agent_py),
        CompiledArtifact("agent_logic.py", agent_logic),
        CompiledArtifact("manifest_snippet.yaml", manifest_snippet),
        CompiledArtifact("run_local.py", run_local),
    ]


def _render_react_logic_module(
    *,
    definition: AgentDefinition,
    system_prompt: str,
    user_prompt: str,
    use_platform_llm: bool,
) -> str:
    run_react_body = (
        textwrap.dedent(
            """
            from ratatoskr.designer.llm_client import chat_completion_json
            from ratatoskr.designer.llm_settings import get_react_llm_settings

            settings = get_react_llm_settings()
            if not settings.is_complete():
                return fallback_process(message, value_hint)
            try:
                payload = chat_completion_json(
                    system=SYSTEM_PROMPT,
                    user=USER_PROMPT.format(
                        message=json.dumps(message),
                        value="" if value_hint is None else str(value_hint),
                    ),
                    settings=settings,
                )
                result = parse_llm_payload(json.dumps(payload), value_hint=value_hint)
                result["mode"] = "llm"
                return result
            except Exception as exc:
                return fallback_process(message, value_hint, reason=str(exc))
            """
        ).strip()
        if use_platform_llm
        else "return fallback_process(message, value_hint)"
    )
    run_react_body = textwrap.indent(run_react_body, "    ")

    lines = [
        f'"""Generated ReAct logic — {definition.name} (no flink_agents import)."""',
        "",
        "from __future__ import annotations",
        "",
        "import json",
        "import re",
        "from typing import Any",
    ]
    lines.extend(
        [
            "",
            f"SYSTEM_PROMPT = {system_prompt!r}",
            "",
            f"USER_PROMPT = {user_prompt!r}",
            "",
            "",
            "def parse_int_from_text(text: str) -> int | None:",
            '    match = re.search(r"-?\\d+", text)',
            "    if not match:",
            "        return None",
            "    try:",
            "        return int(match.group(0))",
            "    except ValueError:",
            "        return None",
            "",
            "",
            "def fallback_process(message: str, value_hint: int | None, *, reason: str | None = None) -> dict[str, Any]:",
            "    source = value_hint if value_hint is not None else parse_int_from_text(message)",
            "    if source is None:",
            "        source = 0",
            "    doubled = source * 2",
            "    return {",
            '        "message": message,',
            '        "input": source,',
            '        "doubled": doubled,',
            '        "result": str(doubled),',
            '        "reasoning": reason or "Deterministic fallback (LLM unavailable or not configured).",',
            '        "mode": "fallback",',
            '        "fallback_reason": reason,',
            "    }",
            "",
            "",
            "def parse_llm_payload(content: str, *, value_hint: int | None = None) -> dict[str, Any]:",
            "    text = content.strip()",
            '    match = re.match(r"^```(?:json)?\\s*([\\s\\S]*?)\\s*```$", text, re.IGNORECASE)',
            "    if match:",
            "        text = match.group(1).strip()",
            "    payload = json.loads(text)",
            "    if not isinstance(payload, dict):",
            '        raise ValueError("LLM response must be a JSON object")',
            '    input_val = int(payload.get("input", value_hint or 0))',
            '    doubled_raw = payload.get("doubled")',
            "    doubled = int(doubled_raw) if doubled_raw is not None else input_val * 2",
            "    result = {",
            '        "input": input_val,',
            '        "doubled": doubled,',
            '        "result": str(payload.get("result") or payload.get("reasoning") or doubled),',
            '        "reasoning": str(payload.get("reasoning") or "LLM processed the input."),',
            "    }",
            "    for key, val in payload.items():",
            "        if key not in result:",
            "            result[key] = val",
            "    return result",
            "",
            "",
            "def run_react(message: str, *, value_hint: int | None = None) -> dict[str, Any]:",
            '    """Run ReAct LLM path when configured, else deterministic fallback."""',
            run_react_body,
            "",
            "",
            "def payload_from_input(raw: Any) -> dict[str, Any]:",
            "    if isinstance(raw, dict):",
            "        return raw",
            '    message = getattr(raw, "message", None)',
            '    value = getattr(raw, "value", None)',
            "    payload: dict[str, Any] = {}",
            "    if message is not None:",
            '        payload["message"] = message',
            "    if value is not None:",
            '        payload["value"] = value',
            "    if not payload:",
            '        payload["message"] = str(raw)',
            "    return payload",
            "",
            "",
            "def message_from_payload(payload: dict[str, Any]) -> str:",
            '    if "message" in payload:',
            '        return str(payload["message"])',
            '    if "value" in payload:',
            '        return str(payload["value"])',
            '    if "input" in payload:',
            '        return str(payload["input"])',
            "    return json.dumps(payload, default=str)",
            "",
            "",
            "def hint_value(payload: dict[str, Any]) -> int | None:",
            '    raw = payload.get("value")',
            "    if raw is None:",
            "        return None",
            "    try:",
            "        return int(raw)",
            "    except (TypeError, ValueError):",
            "        return None",
            "",
        ]
    )
    return "\n".join(lines)


def _render_react_agent_module(
    *,
    definition: AgentDefinition,
    class_name: str,
    action: AgentDefinitionNode,
    tools: list[AgentDefinitionNode],
    agent_slug: str,
) -> str:
    tool_methods: list[str] = []
    for tool in tools:
        body = _tool_body(tool)
        doc = _tool_description(tool)
        param_type, return_type = _tool_signature(tool)
        tool_methods.append(
            textwrap.dedent(
                f"""
                @tool
                @staticmethod
                def {tool.name}(value: {param_type}) -> {return_type}:
                    \"\"\"{doc}\"\"\"
                    {body}
                """
            ).strip()
        )

    tool_section = ("\n\n    " + "\n\n    ".join(tool_methods)) if tool_methods else ""
    primary_tool = tools[0].name if tools else None
    tool_output = ""
    if primary_tool:
        tool_output = f'\n                "tool_{primary_tool}": {class_name}.{primary_tool}(input_val),'

    return textwrap.dedent(
        f'''\
        """Generated ReAct agent — {definition.name}."""

        from __future__ import annotations

        import importlib.util
        from pathlib import Path
        from typing import Any

        from flink_agents.api.agents.agent import Agent
        from flink_agents.api.decorators import action, tool
        from flink_agents.api.events.event import Event, InputEvent, OutputEvent
        from flink_agents.api.runner_context import RunnerContext

        _INPUT_EVENT = InputEvent.EVENT_TYPE


        def _load_logic():
            logic_path = Path(__file__).resolve().parent / "agent_logic.py"
            spec = importlib.util.spec_from_file_location("generated_agent_logic", logic_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Cannot load generated logic from {{logic_path}}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module


        _logic_module = None


        def _logic():
            global _logic_module
            if _logic_module is None:
                _logic_module = _load_logic()
            return _logic_module


        class {class_name}(Agent):
            """{definition.description or definition.name}"""
            {tool_section}

            @action(_INPUT_EVENT)
            @staticmethod
            def {action.name}(event: Event, ctx: RunnerContext) -> None:
                logic = _logic()
                payload = logic.payload_from_input(InputEvent.from_event(event).input)
                message = logic.message_from_payload(payload)
                value_hint = logic.hint_value(payload)
                result = logic.run_react(message, value_hint=value_hint)
                input_val = int(result.get("input", value_hint or 0))
                ctx.send_event(
                    OutputEvent(
                        output={{
                            "message": message,
                            "input": input_val,
                            "doubled": int(result.get("doubled", input_val * 2)),
                            "result": str(result.get("result", result.get("doubled", input_val * 2))),
                            "reasoning": result.get("reasoning", ""),
                            "mode": result.get("mode", "unknown"),
                            "agent": "{agent_slug}",{tool_output}
                        }}
                    )
                )
        '''
    ).strip() + "\n"


def _render_react_manifest_snippet(
    definition: AgentDefinition,
    class_name: str,
    agent_slug: str,
) -> str:
    return textwrap.dedent(
        f"""\
        # Manifest snippet — merge into examples/agents/agent-manifest.yaml on publish
        agents:
          {agent_slug}:
            type: react
            description: {definition.description or definition.name}
            entry: generated.{definition.id}.agent:{class_name}
            runner: .ratatoskr/agents/{definition.id}/run_local.py
        """
    ).strip() + "\n"


def _render_react_run_local(
    definition_id: str, class_name: str, definition: AgentDefinition, *, skills_mode: bool = False
) -> str:
    if skills_mode:
        sample_records = (
            '[{"key": "1", "message": "What is (2 + 3) * 4?"}, '
            '{"key": "2", "message": "Compute 2 ^ 10."}]'
        )
    elif "message" in (definition.input_schema.get("required") or []) or "message" in (
        definition.input_schema.get("properties") or {}
    ):
        sample_records = (
            '[{"key": "1", "message": "Please double the input value 7"}, '
            '{"key": "2", "message": "process value 21", "value": 21}]'
        )
    else:
        sample_records = '[{"key": "1", "value": 3}, {"key": "2", "value": 10}]'

    return textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        """Local runner for generated ReAct agent `{definition_id}`."""

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
            module_path = repo / ".ratatoskr" / "agents" / "{definition_id}" / "agent.py"
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
            input_data = {sample_records}
            output_data = env.from_list(input_data).apply(agent_cls()).to_list()
            env.execute()
            print("Generated ReAct agent results:")
            for record in output_data:
                print(record)
            return 0


        if __name__ == "__main__":
            raise SystemExit(main())
        '''
    ).strip() + "\n"


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
    tools = [
        by_id[tid]
        for tid in tool_ids
        if tid in by_id and by_id[tid].kind in ("tool", "mcp_tool")
    ]
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
    if tool.kind == "mcp_tool":
        return _mcp_tool_body(tool)
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


def _mcp_tool_body(tool: AgentDefinitionNode) -> str:
    config = tool.config or {}
    server_ref = str(config.get("server_ref") or "").strip()
    tool_name = str(config.get("tool_name") or tool.name).strip()
    arg_name = str(config.get("arg_name") or "ip").strip()
    return textwrap.dedent(
        f"""
        from ratatoskr.mcp.client import invoke_tool
        return invoke_tool(
            {server_ref!r},
            {tool_name!r},
            {{{arg_name!r}: str(value)}},
        )
        """
    ).strip()


def _tool_description(tool: AgentDefinitionNode) -> str:
    if tool.kind == "mcp_tool":
        config = tool.config or {}
        server_ref = str(config.get("server_ref") or "").strip()
        tool_name = str(config.get("tool_name") or tool.name).strip()
        return f"MCP tool {tool_name} via {server_ref or 'server'}"
    config = tool.config or {}
    tool_ref = str(config.get("tool_ref") or tool.name).strip()
    try:
        return str(get_builtin_tool(tool_ref).get("description") or f"{tool.name} tool")
    except KeyError:
        return f"{tool.name} tool"


def _tool_signature(tool: AgentDefinitionNode) -> tuple[str, str]:
    if tool.kind == "mcp_tool":
        return "str", "dict"
    return "int", "int"


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
        param_type, return_type = _tool_signature(tool)
        tool_methods.append(
            textwrap.dedent(
                f"""
                @tool
                @staticmethod
                def {tool.name}(value: {param_type}) -> {return_type}:
                    \"\"\"{doc}\"\"\"
                    {body}
                """
            ).strip()
        )

    primary_tool_node = tools[0]
    primary_tool = primary_tool_node.name
    input_extract = (
        "n = _str_from_input(event)"
        if primary_tool_node.kind == "mcp_tool"
        else "n = _int_from_input(event)"
    )
    input_helpers = (
        textwrap.dedent(
            f'''
            def _str_from_input(event: Event, *, field: str = "{input_field}") -> str:
                payload = InputEvent.from_event(event).input
                if isinstance(payload, dict):
                    raw = payload.get(field, "")
                else:
                    raw = getattr(payload, field, payload)
                return str(raw)
            '''
        ).strip()
        if primary_tool_node.kind == "mcp_tool"
        else textwrap.dedent(
            f'''
            def _int_from_input(event: Event, *, field: str = "{input_field}") -> int:
                payload = InputEvent.from_event(event).input
                if isinstance(payload, dict):
                    raw = payload.get(field, 0)
                else:
                    raw = getattr(payload, field, payload)
                return int(raw)
            '''
        ).strip()
    )
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

{input_helpers}

        class {class_name}(Agent):
            """{definition.description or definition.name}"""

            {"\n\n    ".join(tool_methods)}

            @action(_INPUT_EVENT)
            @staticmethod
            def {action.name}(event: Event, ctx: RunnerContext) -> None:
                {input_extract}
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
        param_type, return_type = _tool_signature(tool)
        tool_functions.append(
            textwrap.dedent(
                f"""
                def {tool.name}(value: {param_type}) -> {return_type}:
                    \"\"\"{doc}\"\"\"
                    {body}
                """
            ).strip()
        )

    primary_tool_node = tools[0]
    primary_tool = primary_tool_node.name
    input_extract = (
        "n = _str_from_input(event)"
        if primary_tool_node.kind == "mcp_tool"
        else "n = _int_from_input(event)"
    )
    input_helper = (
        textwrap.dedent(
            f'''
            def _str_from_input(event: Event, *, field: str = "{input_field}") -> str:
                payload = InputEvent.from_event(event).input
                if isinstance(payload, dict):
                    raw = payload.get(field, "")
                else:
                    raw = getattr(payload, field, payload)
                return str(raw)
            '''
        ).strip()
        if primary_tool_node.kind == "mcp_tool"
        else textwrap.dedent(
            f'''
            def _int_from_input(event: Event, *, field: str = "{input_field}") -> int:
                payload = InputEvent.from_event(event).input
                if isinstance(payload, dict):
                    raw = payload.get(field, 0)
                else:
                    raw = getattr(payload, field, payload)
                return int(raw)
            '''
        ).strip()
    )
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


        {input_helper}


        def {action.name}(event: Event, ctx: RunnerContext) -> None:
            {input_extract}
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
                function: .ratatoskr.agents.{definition.id}.agent_actions:{tool.name}
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
                function: .ratatoskr.agents.{definition.id}.agent_actions:{action.name}
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
            runner: .ratatoskr/agents/{definition.id}/run_local.py
            flink_yaml: .ratatoskr/agents/{definition.id}/agent.yaml
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
            module_path = repo / ".ratatoskr" / "agents" / "{definition_id}" / "agent.py"
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
