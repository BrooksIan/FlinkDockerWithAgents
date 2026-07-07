"""High-level agent definition CRUD."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ratatoskr.designer.definitions.models import (
    AgentDefinition,
    AgentDefinitionEdge,
    AgentDefinitionNode,
)
from ratatoskr.designer.definitions.seed import DOUBLE_VALUE_ID, double_value_definition_payload
from ratatoskr.designer.definitions.store import (
    AgentDefinitionStore,
    agent_definitions_store,
)
from ratatoskr.designer.definitions.validate import validate_agent_definition

_default_service: "AgentDefinitionService | None" = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _definition_to_dict(definition: AgentDefinition) -> dict[str, Any]:
    return {
        "id": definition.id,
        "name": definition.name,
        "type": definition.type,
        "version": definition.version,
        "description": definition.description,
        "status": definition.status,
        "nodes": [asdict(n) for n in definition.nodes],
        "edges": [asdict(e) for e in definition.edges],
        "layout": definition.layout,
        "input_schema": definition.input_schema,
        "output_schema": definition.output_schema,
        "manifest_name": definition.manifest_name,
        "catalog_category_id": definition.catalog_category_id,
        "catalog_subcategory_id": definition.catalog_subcategory_id,
        "catalog_tags": list(definition.catalog_tags),
        "mcp_servers": list(definition.mcp_servers),
        "created_at": definition.created_at,
        "updated_at": definition.updated_at,
    }


def _parse_nodes(raw: list[dict[str, Any]]) -> list[AgentDefinitionNode]:
    return [
        AgentDefinitionNode(
            id=n["id"],
            kind=n["kind"],
            name=str(n.get("name") or ""),
            config=n.get("config") or {},
        )
        for n in raw
    ]


def _parse_edges(raw: list[dict[str, Any]]) -> list[AgentDefinitionEdge]:
    return [
        AgentDefinitionEdge(
            id=e["id"],
            source=e["source"],
            target=e["target"],
            kind=e["kind"],
        )
        for e in raw
    ]


class AgentDefinitionService:
    def __init__(self, store: AgentDefinitionStore) -> None:
        self._store = store

    def store_count(self) -> int:
        return self._store.count()

    def create(
        self,
        name: str,
        *,
        agent_type: str = "workflow",
        description: str = "",
        nodes: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
        layout: dict[str, dict[str, float]] | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        manifest_name: str | None = None,
        catalog_category_id: str | None = None,
        catalog_subcategory_id: str | None = None,
        catalog_tags: list[str] | None = None,
        mcp_servers: list[str] | None = None,
    ) -> dict[str, Any]:
        definition_id = f"def_{uuid.uuid4().hex[:12]}"
        now = _utc_now()
        self._store.insert(
            definition_id=definition_id,
            name=name,
            agent_type=agent_type,
            version=1,
            description=description,
            status="draft",
            nodes=nodes or [],
            edges=edges or [],
            layout=layout or {},
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            manifest_name=manifest_name,
            catalog={
                "category_id": catalog_category_id,
                "subcategory_id": catalog_subcategory_id,
                "tags": catalog_tags or [],
                "mcp_servers": mcp_servers or [],
            },
            created_at=now,
            updated_at=now,
        )
        return self.get(definition_id)

    def create_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        definition_id = str(payload.get("id") or f"def_{uuid.uuid4().hex[:12]}")
        now = _utc_now()
        self._store.insert(
            definition_id=definition_id,
            name=str(payload["name"]),
            agent_type=str(payload.get("type") or "workflow"),
            version=int(payload.get("version") or 1),
            description=str(payload.get("description") or ""),
            status=str(payload.get("status") or "draft"),
            nodes=payload.get("nodes") or [],
            edges=payload.get("edges") or [],
            layout=payload.get("layout") or {},
            input_schema=payload.get("input_schema") or {},
            output_schema=payload.get("output_schema") or {},
            manifest_name=payload.get("manifest_name"),
            catalog={
                "category_id": payload.get("catalog_category_id"),
                "subcategory_id": payload.get("catalog_subcategory_id"),
                "tags": list(payload.get("catalog_tags") or []),
                "mcp_servers": list(payload.get("mcp_servers") or []),
            },
            created_at=now,
            updated_at=now,
        )
        return self.get(definition_id)

    def get(self, definition_id: str) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        return _definition_to_dict(definition)

    def list_definitions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        self._ensure_seeds()
        return [
            _definition_to_dict(d) for d in self._store.list_definitions(limit=limit)
        ]

    def _ensure_seeds(self) -> None:
        if self._store.count() == 0:
            self.create_from_payload(double_value_definition_payload())

    def seed_double_value(self) -> dict[str, Any]:
        try:
            return self.get(DOUBLE_VALUE_ID)
        except KeyError:
            return self.create_from_payload(double_value_definition_payload())

    def update(self, definition_id: str, body: dict[str, Any]) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)

        nodes = body.get("nodes")
        edges = body.get("edges")
        catalog = {
            "category_id": definition.catalog_category_id,
            "subcategory_id": definition.catalog_subcategory_id,
            "tags": list(definition.catalog_tags),
            "mcp_servers": list(definition.mcp_servers),
        }
        if "catalog_category_id" in body:
            catalog["category_id"] = body["catalog_category_id"]
        if "catalog_subcategory_id" in body:
            catalog["subcategory_id"] = body["catalog_subcategory_id"]
        if "catalog_tags" in body:
            catalog["tags"] = list(body["catalog_tags"] or [])
        if "mcp_servers" in body:
            catalog["mcp_servers"] = list(body["mcp_servers"] or [])

        updated = self._store.update(
            definition_id,
            name=body.get("name"),
            agent_type=body.get("type"),
            version=body.get("version"),
            description=body.get("description"),
            status=body.get("status"),
            nodes=nodes,
            edges=edges,
            layout=body.get("layout"),
            input_schema=body.get("input_schema"),
            output_schema=body.get("output_schema"),
            manifest_name=body.get("manifest_name"),
            catalog=catalog,
            updated_at=_utc_now(),
        )
        if not updated:
            raise KeyError(definition_id)
        return self.get(definition_id)

    def delete(self, definition_id: str) -> None:
        if not self._store.delete(definition_id):
            raise KeyError(definition_id)

    def validate(self, definition_id: str) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        return validate_agent_definition(definition)

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        from ratatoskr.designer.definitions.models import agent_definition_from_dict

        definition = agent_definition_from_dict(payload)
        return validate_agent_definition(definition)

    def compile(self, definition_id: str, *, root: Path | None = None) -> dict[str, Any]:
        from ratatoskr.designer.definitions.compile import (
            CompileError,
            compile_agent_definition,
            compile_result_to_dict,
        )

        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        try:
            result = compile_agent_definition(definition, root=root, write_files=True)
        except CompileError as exc:
            raise ValueError(str(exc)) from exc

        self._store.update(
            definition_id,
            status="compiled",
            updated_at=_utc_now(),
        )
        payload = compile_result_to_dict(result)
        payload["definition"] = self.get(definition_id)
        return payload

    def publish(self, definition_id: str, *, root: Path | None = None) -> dict[str, Any]:
        from ratatoskr.designer.definitions.publish import (
            PublishError,
            publish_agent_definition,
            publish_result_to_dict,
        )

        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        try:
            result = publish_agent_definition(definition, root=root, compile_first=True)
        except PublishError as exc:
            raise ValueError(str(exc)) from exc

        self._store.update(
            definition_id,
            status="published",
            manifest_name=result.manifest_name,
            updated_at=_utc_now(),
        )
        payload = publish_result_to_dict(result)
        payload["definition"] = self.get(definition_id)
        return payload

    def run_local(
        self,
        definition_id: str,
        *,
        records: list[dict[str, Any]] | None = None,
        root: Path | None = None,
    ) -> dict[str, Any]:
        import re
        import subprocess
        import sys

        from ratatoskr.agents.submit import _run_service
        from ratatoskr.paths import project_root
        from ratatoskr.runs.service import default_run_service

        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        repo = root or project_root()

        # Preferred path: run inside the Flink JobManager container, where
        # flink_agents and PyFlink are installed. The host interpreter used by
        # the API almost never has flink_agents, so host execution fails with
        # "No module named 'flink_agents'".
        cluster = self._try_run_in_cluster(definition, records, repo, root=root)
        if cluster is not None:
            runs = _run_service(repo)
            run_label = definition.manifest_name or definition_id
            run_id = runs.create_pipeline_run(run_label, kind="local", status="running")
            if cluster["return_code"] == 0:
                runs.finish_run(run_id, status="finished")
            else:
                runs.finish_run(
                    run_id,
                    status="failed",
                    error=cluster["stderr"].strip()
                    or f"exit code {cluster['return_code']}",
                )
            return {
                "run_id": run_id,
                "return_code": cluster["return_code"],
                "definition_id": definition_id,
                "mode": "cluster",
                "stdout": cluster["stdout"],
                "stderr": cluster["stderr"],
                "output": cluster["output"],
                "records": cluster["records"],
            }

        # No custom records: prefer the published manifest runner when available.
        if records is None and definition.manifest_name:
            runner = repo / self._manifest_runner_path(definition.manifest_name, repo)
            runs = _run_service(repo)
            run_id = runs.create_run(definition.manifest_name, kind="local", status="running")
            completed = subprocess.run(
                [sys.executable, str(runner)],
                cwd=repo,
                capture_output=True,
                text=True,
            )
            if completed.returncode == 0:
                runs.finish_run(run_id, status="finished")
            else:
                runs.finish_run(
                    run_id,
                    status="failed",
                    error=completed.stderr.strip() or f"exit code {completed.returncode}",
                )
            run_detail = default_run_service(repo).get_run(run_id) or {}
            return {
                "run_id": run_id,
                "return_code": completed.returncode,
                "agent": definition.manifest_name,
                "mode": "manifest",
                "stdout": completed.stdout,
                "stderr": completed.stderr,
                "output": run_detail.get("output") or self._parse_stdout_output(completed.stdout),
            }

        # Custom records (or unpublished agent): run through the generated agent.
        runner = repo / ".ratatoskr" / "agents" / definition_id / "run_local.py"
        if not runner.is_file():
            self.compile(definition_id, root=root)
            runner = repo / ".ratatoskr" / "agents" / definition_id / "run_local.py"
        if not runner.is_file():
            raise ValueError("Compile the agent before running locally")

        runs = _run_service(repo)
        run_label = definition.manifest_name or definition_id
        run_id = runs.create_pipeline_run(run_label, kind="local", status="running")

        if records is not None:
            class_name = self._class_name(definition.name)
            rc, stdout, stderr, output = self._execute_generated_agent(
                definition_id,
                class_name,
                records,
                repo,
            )
        else:
            completed = subprocess.run(
                [sys.executable, str(runner)],
                cwd=repo,
                capture_output=True,
                text=True,
            )
            rc = completed.returncode
            stdout = completed.stdout
            stderr = completed.stderr
            output = self._parse_stdout_output(stdout)

        if rc == 0:
            runs.finish_run(run_id, status="finished")
        else:
            runs.finish_run(run_id, status="failed", error=stderr.strip() or f"exit code {rc}")

        return {
            "run_id": run_id,
            "return_code": rc,
            "definition_id": definition_id,
            "mode": "compiled",
            "stdout": stdout,
            "stderr": stderr,
            "output": output,
            "records": records,
        }

    def _try_run_in_cluster(
        self,
        definition: Any,
        records: list[dict[str, Any]] | None,
        repo: Path,
        *,
        root: Path | None = None,
    ) -> dict[str, Any] | None:
        """Execute the generated agent inside the JobManager container.

        Returns a result dict when the container path was used, or ``None`` when
        no Flink JobManager container is available (so the caller can fall back
        to host execution).
        """
        import json
        import tempfile

        try:
            from ratatoskr.docker_utils import (
                PYFLINK_PYTHONPATH,
                container_id,
                docker_cp,
                docker_exec_output,
            )
        except Exception:
            return None

        try:
            cid = container_id("jobmanager")
        except Exception:
            cid = None
        if not cid:
            return None

        definition_id = definition.id

        # Ensure the generated agent artifacts exist before copying them in.
        local_dir = repo / ".ratatoskr" / "agents" / definition_id
        if not (local_dir / "agent.py").is_file():
            try:
                self.compile(definition_id, root=root)
            except Exception:
                return None
        if not (local_dir / "agent.py").is_file():
            return None

        class_name = self._class_name(definition.name)
        remote_dir = f"/opt/flink/.ratatoskr/agents/{definition_id}"

        for name in ("agent.py", "agent_logic.py", "agent_actions.py"):
            source = local_dir / name
            if source.is_file():
                if not docker_cp(source, cid, f"{remote_dir}/{name}"):
                    return None

        # Ensure ratatoskr support modules the generated agent may import (e.g.
        # the Designer LLM connection for ReAct agents) are present in-cluster,
        # in case the runtime sync predates them.
        for rel in (
            "ratatoskr/designer/flink_llm.py",
            "ratatoskr/designer/llm_client.py",
            "ratatoskr/designer/llm_settings.py",
            "ratatoskr/designer/models.py",
        ):
            support = repo / rel
            if support.is_file():
                docker_cp(support, cid, f"/opt/flink/{rel}")

        # ReAct skills agents load bundled skills from examples/skills at
        # runtime; make sure that directory exists in the container.
        skills_dir = repo / "examples" / "skills"
        if skills_dir.is_dir():
            import subprocess

            subprocess.run(
                ["docker", "exec", "-u", "root", cid, "mkdir", "-p", "/opt/flink/examples"],
                cwd=repo,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["docker", "cp", str(skills_dir), f"{cid}:/opt/flink/examples/skills"],
                cwd=repo,
                capture_output=True,
                text=True,
            )

        # User-authored skills (pasted through the designer) are merged into the
        # same /opt/flink/examples/skills directory the agent loads from.
        user_skills_dir = repo / "data" / "skills"
        if user_skills_dir.is_dir():
            import subprocess

            subprocess.run(
                ["docker", "exec", "-u", "root", cid, "mkdir", "-p", "/opt/flink/examples/skills"],
                cwd=repo,
                capture_output=True,
                text=True,
            )
            for skill_dir in user_skills_dir.iterdir():
                if skill_dir.is_dir():
                    subprocess.run(
                        ["docker", "cp", str(skill_dir), f"{cid}:/opt/flink/examples/skills"],
                        cwd=repo,
                        capture_output=True,
                        text=True,
                    )

        sample = records if records is not None else self._default_sample_records(definition)
        runtime_records = self._normalize_records_for_runtime(sample)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            records_local = tmp_path / "_records.json"
            driver_local = tmp_path / "_run_records.py"
            records_local.write_text(json.dumps(runtime_records), encoding="utf-8")
            driver_local.write_text(
                self._cluster_driver_source(class_name), encoding="utf-8"
            )
            if not docker_cp(records_local, cid, f"{remote_dir}/_records.json"):
                return None
            if not docker_cp(driver_local, cid, f"{remote_dir}/_run_records.py"):
                return None

            exports = [
                "cd /opt/flink",
                f"export PYTHONPATH={PYFLINK_PYTHONPATH}",
                # Point the designer store at a writable path (/tmp is writable
                # by the flink user) so ReAct agents fall back to env-var LLM
                # settings instead of failing to open a DB in a root-owned dir.
                "export RATATOSKR_DESIGNER_DB=/tmp/ratatoskr_designer_runtime.db",
            ]
            exports.extend(self._llm_env_exports())
            command = " && ".join(exports) + f" && python3 {remote_dir}/_run_records.py"
            rc, stdout, stderr = docker_exec_output(cid, command, interactive=False)

        return {
            "return_code": rc,
            "stdout": stdout,
            "stderr": stderr,
            "output": self._parse_stdout_output(stdout),
            "records": sample,
        }

    @staticmethod
    def _llm_env_exports() -> list[str]:
        """Forward the host's resolved Designer LLM settings into the container.

        ReAct agents resolve the OpenAI-compatible connection at plan-build time
        from these settings; the container has no designer DB of its own.
        """
        import shlex

        try:
            from ratatoskr.designer.llm_settings import get_react_llm_settings

            settings = get_react_llm_settings()
        except Exception:
            return []

        exports: list[str] = []
        if settings.endpoint_url:
            exports.append(
                f"export RATATOSKR_LLM_ENDPOINT_URL={shlex.quote(settings.endpoint_url)}"
            )
        if settings.model_id:
            exports.append(
                f"export RATATOSKR_LLM_MODEL_ID={shlex.quote(settings.model_id)}"
            )
        if settings.api_key:
            exports.append(
                f"export RATATOSKR_LLM_API_KEY={shlex.quote(settings.api_key)}"
            )
        return exports

    @staticmethod
    def _cluster_driver_source(class_name: str) -> str:
        import textwrap

        return textwrap.dedent(
            f'''\
            #!/usr/bin/env python3
            """In-container runner for designer test runs."""

            from __future__ import annotations

            import importlib.util
            import json
            import sys
            from pathlib import Path

            HERE = Path(__file__).resolve().parent
            REPO = "/opt/flink"


            def main() -> int:
                if REPO not in sys.path:
                    sys.path.insert(0, REPO)
                records = json.loads((HERE / "_records.json").read_text())
                module_path = HERE / "agent.py"
                module_name = "generated_agent"
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                if spec is None or spec.loader is None:
                    raise RuntimeError(f"Cannot load generated agent from {{module_path}}")
                module = importlib.util.module_from_spec(spec)
                # Register before exec so flink_agents can resolve the module of
                # decorated action/tool functions via inspect.getmodule().
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                agent_cls = getattr(module, "{class_name}")

                from flink_agents.api.execution_environment import (
                    AgentsExecutionEnvironment,
                )

                env = AgentsExecutionEnvironment.get_execution_environment()
                output = env.from_list(records).apply(agent_cls()).to_list()
                env.execute()
                print("Generated agent results:")
                for record in output:
                    print(record)
                return 0


            if __name__ == "__main__":
                raise SystemExit(main())
            '''
        )

    @staticmethod
    def _normalize_records_for_runtime(
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Ensure each record has a ``value``/``v`` field the runtime requires.

        The flink_agents local runner needs every input record to carry the
        payload under ``value`` (or ``v``) plus an optional ``key``. Designer
        test records sometimes only carry domain fields (e.g. ``message``), so
        wrap any such fields into ``value``.
        """
        normalized: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                normalized.append({"value": record})
                continue
            if "value" in record or "v" in record:
                normalized.append(record)
                continue
            key = record.get("key", record.get("k"))
            payload = {k: v for k, v in record.items() if k not in ("key", "k")}
            new_record: dict[str, Any] = {}
            if key is not None:
                new_record["key"] = key
            new_record["value"] = payload
            normalized.append(new_record)
        return normalized

    @staticmethod
    def _default_sample_records(definition: Any) -> list[dict[str, Any]]:
        required = definition.input_schema.get("required") or []
        properties = definition.input_schema.get("properties") or {}
        if "message" in required or "message" in properties:
            return [
                {"key": "1", "message": "Please double the input value 7"},
                {"key": "2", "message": "process value 21", "value": 21},
            ]
        return [{"key": "1", "value": 3}, {"key": "2", "value": 10}]

    @staticmethod
    def _class_name(name: str) -> str:
        import re

        parts = re.sub(r"[^a-zA-Z0-9]+", " ", name).title().split()
        base = "".join(parts) or "Generated"
        return f"{base}Agent"

    @staticmethod
    def _manifest_runner_path(manifest_name: str, repo: Path) -> Path:
        from ratatoskr.agents.catalog import get_agent_spec

        spec = get_agent_spec(manifest_name, root=repo)
        return Path(spec.runner)

    @staticmethod
    def _parse_stdout_output(stdout: str) -> Any:
        import ast

        lines = stdout.splitlines()
        markers = ("Generated agent results:", "Generated ReAct agent results:")
        start = None
        for index, line in enumerate(lines):
            if line.strip() in markers:
                start = index + 1
                break
        if start is None:
            return None
        records: list[Any] = []
        for line in lines[start:]:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                records.append(ast.literal_eval(stripped))
            except (SyntaxError, ValueError):
                records.append(stripped)
        return records or None

    def _execute_generated_agent(
        self,
        definition_id: str,
        class_name: str,
        records: list[dict[str, Any]],
        repo: Path,
    ) -> tuple[int, str, str, Any]:
        import importlib.util
        import io
        import sys
        from contextlib import redirect_stderr, redirect_stdout

        module_path = repo / ".ratatoskr" / "agents" / definition_id / "agent.py"
        if not module_path.is_file():
            return 1, "", f"Generated agent module not found: {module_path}", None

        if str(repo) not in sys.path:
            sys.path.insert(0, str(repo))

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        output_records: Any = None
        try:
            spec = importlib.util.spec_from_file_location("generated_agent", module_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Cannot load generated agent from {module_path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            agent_cls = getattr(module, class_name)

            with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                from flink_agents.api.execution_environment import AgentsExecutionEnvironment

                env = AgentsExecutionEnvironment.get_execution_environment()
                output_records = env.from_list(records).apply(agent_cls()).to_list()
                env.execute()
                print("Generated agent results:")
                for record in output_records:
                    print(record)
            return 0, stdout_buf.getvalue(), stderr_buf.getvalue(), output_records
        except Exception as exc:
            stderr_buf.write(str(exc))
            return 1, stdout_buf.getvalue(), stderr_buf.getvalue().strip(), output_records


def reset_agent_definition_service_for_tests() -> None:
    global _default_service
    _default_service = None


def default_agent_definition_service(root: Path | None = None) -> AgentDefinitionService:
    global _default_service
    if _default_service is None or root is not None:
        _default_service = AgentDefinitionService(agent_definitions_store(root))
    return _default_service
