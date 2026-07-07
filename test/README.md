# Tests

<p align="center">
  <img src="../docs/branding/Ratatoskr_title_image.png" alt="Ratatoskr — wood-textured wordmark and squirrel mascot" width="320" />
</p>

Tests for the **Ratatoskr Flink Agents platform** and workspace-wide CLI behavior. Honeypot-specific tests live under [`honeypot/test/`](../honeypot/test/).

## Layout

| File / directory | Scope |
|------------------|--------|
| `test_cli_smoke.py` | Workspace layout, manifests, demo catalog |
| `test_generic_platform.py` | Agent registry, validate paths, API factory |
| `test_api_platform.py` | Control API OpenAPI, health, metrics, auth |
| `test_launch_flink_agents.py` | Flink Agents import + cluster launch smoke |
| `test_pipelines_platform.py` | Studio pipelines API + Kafka validation |
| `test_pipeline_assist.py` | Pipeline assistant generate/build + agent suggestions |
| `test_api_fetch_settings.py` | API fetch settings + `workflow_api_fetch` |
| `test_pipeline_cluster_submit.py` | Cluster codegen, validate, submit |
| `honeypot/test/` | Cowrie pipeline, policy, traps, ReAct (optional) |

## Run locally (no Docker)

```bash
pip install -e .
pytest test/test_cli_smoke.py test/test_generic_platform.py test/test_api_platform.py
ratatoskr verify --tier quick
```

## Verify tiers

```bash
ratatoskr verify --tier quick       # smoke + agent registry + API unit tests
ratatoskr verify --tier standard    # + platform doctor
ratatoskr verify --tier full        # + Docker image check
ratatoskr verify --tier nightly
```

Tiers: `ratatoskr/manifests/verify-tiers.yaml`. Honeypot overlay: `honeypot/manifests/` with `--profile honeypot`.

## Launch smoke test

```bash
ratatoskr test validate             # file layout (generic paths)
ratatoskr test launch               # flink_agents import in image
ratatoskr test launch --cluster     # submit job to JobManager (needs stack up)
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
ratatoskr api start    # separate terminal
ratatoskr api check
curl http://127.0.0.1:8090/v1/health
```

## Honeypot tests (optional)

```bash
ratatoskr up --profile full
ratatoskr test phase1|phase2|phase3|production [--e2e]
pytest honeypot/test/
```

## See also

- [../docs/PLATFORM.md](../docs/PLATFORM.md) — platform architecture
- [../ratatoskr/README.md](../ratatoskr/README.md) — CLI commands
