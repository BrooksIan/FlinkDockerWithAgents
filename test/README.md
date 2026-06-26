# Tests

Tests for the **Apemosyne Flink Agents platform** and workspace-wide CLI behavior. Honeypot-specific tests live under [`honeypot/test/`](../honeypot/test/).

## Layout

| File / directory | Scope |
|------------------|--------|
| `test_cli_smoke.py` | Workspace layout, manifests, demo catalog |
| `test_generic_platform.py` | Agent registry, validate paths, API factory |
| `test_api_platform.py` | Control API OpenAPI, health, metrics, auth |
| `test_launch_flink_agents.py` | Flink Agents import + cluster launch smoke |
| `test_pipelines_platform.py` | Studio pipelines API + Kafka validation |
| `test_pipeline_cluster_submit.py` | Cluster codegen, validate, submit |
| `honeypot/test/` | Cowrie pipeline, policy, traps, ReAct (optional) |

## Run locally (no Docker)

```bash
pip install -e .
pytest test/test_cli_smoke.py test/test_generic_platform.py test/test_api_platform.py
apemosyne verify --tier quick
```

## Verify tiers

```bash
apemosyne verify --tier quick       # smoke + agent registry + API unit tests
apemosyne verify --tier standard    # + platform doctor
apemosyne verify --tier full        # + Docker image check
apemosyne verify --tier nightly
```

Tiers: `apemosyne/manifests/verify-tiers.yaml`. Honeypot overlay: `honeypot/manifests/` with `--profile honeypot`.

## Launch smoke test

```bash
apemosyne test validate             # file layout (generic paths)
apemosyne test launch               # flink_agents import in image
apemosyne test launch --cluster     # submit job to JobManager (needs stack up)
```

For Studio pipeline cluster work, restart the minimal stack and sync runtime code first:

```bash
./scripts/restart-studio-cluster.sh --smoke
```

## Control API

API tests use FastAPI `TestClient` — no running server required:

```bash
python test/test_api_platform.py
```

Live check when API is running:

```bash
apemosyne api start    # separate terminal
apemosyne api check
curl http://127.0.0.1:8090/v1/health
```

## Honeypot tests (optional)

```bash
apemosyne up --profile full
apemosyne test phase1|phase2|phase3|production [--e2e]
pytest honeypot/test/
```

## See also

- [../docs/PLATFORM.md](../docs/PLATFORM.md) — platform architecture
- [../apemosyne/README.md](../apemosyne/README.md) — CLI commands
