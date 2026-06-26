# Apemosyne — Flink Agents CLI

Python package for the **`apemosyne`** command-line tool and **Control API** — build Flink Agents images, run registered agents, and expose a dashboard-ready HTTP surface.

## Install

```bash
pip install -e .
apemosyne --help
```

Entry point: `apemosyne.main:main` → `apemosyne.cli:app` (Typer).

## Commands

### Stack & build

| Command | Purpose |
|---------|---------|
| `apemosyne build [git-ref]` | Build `agent_flink_image` from `Dockerfile` |
| `apemosyne up` | Start Docker Compose (default profile: `minimal`) |
| `apemosyne up --mode platform` | Flink stack + API docs URL in startup output |
| `apemosyne up --profile full` | Optional honeypot stack |
| `apemosyne down` / `status` / `logs` | Compose lifecycle |
| `apemosyne kafka up` / `down` / `status` | Studio Kafka (`docker-compose.kafka.yml`, host `:9094`) |
| `apemosyne doctor` | Platform preflight (manifest, Flink REST, API) |

### Agents

| Command | Purpose |
|---------|---------|
| `apemosyne agent list` | Agents from `examples/agents/agent-manifest.yaml` |
| `apemosyne agent describe <name>` | Metadata and entry class |
| `apemosyne agent run <name> --local` | Local `AgentsExecutionEnvironment` runner |
| `apemosyne agent run <name> --cluster` | Submit cluster job via JobManager |
| `apemosyne agent submit <name>` | Same as `--cluster` |
| `apemosyne agent status` | Flink jobs via REST |
| `apemosyne agent cancel <job-id>` | Cancel a Flink job |

### Control API

| Command | Purpose |
|---------|---------|
| `apemosyne api start` | Run FastAPI on `:8090` (uvicorn) |
| `apemosyne api url` | Print configured base URL |
| `apemosyne api openapi [-o file]` | Dump OpenAPI JSON |
| `apemosyne api check` | Probe `/v1/health` |

See [../docs/PLATFORM.md](../docs/PLATFORM.md) for endpoints, auth, and dashboard integration.

### Demos, verify, test

| Command | Purpose |
|---------|---------|
| `apemosyne demo <name>` | Run demo in TaskManager (`datastream`, `workflow`, `react`, …) |
| `apemosyne verify --tier quick\|standard\|full` | Manifest-driven checks |
| `apemosyne test launch [--cluster]` | Flink Agents import + optional cluster submit |
| `apemosyne test validate` | Required workspace files (generic; honeypot paths with `--profile full`) |

Legacy bytecode commands (`config`, `sync`, `dashboard`, …) load when available.

## Package layout

```text
apemosyne/
├── cli.py                 # Top-level Typer app
├── api/                   # FastAPI control plane
│   ├── app.py             # Application factory
│   ├── routes.py          # /v1/* handlers
│   ├── auth.py            # Optional X-API-Key
│   ├── observability.py   # Prometheus + JSON logging
│   └── config.py          # Env-based settings
├── agents/                # Registry + submit helpers
├── runtime/               # Flink cluster submit + studio sync
│   ├── flink_cluster_submit.py
│   └── studio_cluster_sync.py   # Copy runtime into JM/TM after updates
├── docker_utils.py        # Compose helpers
├── paths.py               # Repo root, honeypot_dir(), runtime paths
├── manifests.py           # YAML catalogs
├── startup_modes.py       # up --mode presets
├── copy_manifest.py       # Copy files into containers
└── commands/
    ├── stack.py
    ├── build.py
    ├── agent_cmd.py
    ├── api_cmd.py
    ├── doctor_platform.py
    ├── test_cmd.py
    └── verify_cmd.py
```

## Compose profiles

| Profile | Compose file | Stack |
|---------|--------------|-------|
| `minimal` (default) | `docker-compose.yml` | JobManager + TaskManager |
| `kafka` | `docker-compose.kafka.yml` | Studio Zookeeper + Kafka (`apemosyne kafka up`) |
| `full` | `honeypot/docker-compose.yml` | Cowrie honeypot + Kafka + pipeline |

`configure_runtime_sys_path()` loads honeypot modules only for the `full` profile.

## Environment

| Variable | Default | Notes |
|----------|---------|-------|
| `APEMOSYNE_API_KEY` | unset | Local dev: leave unset. Set for protected routes when API is exposed. |
| `APEMOSYNE_API_HOST` | `127.0.0.1` | API bind |
| `APEMOSYNE_API_PORT` | `8090` | API port |
| `FLINK_REST_ADDRESS` | `localhost` | JobManager for API/CLI |
| `FLINK_REST_PORT` | `8082` (minimal) / `8081` (full) | Host Flink REST port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Studio Kafka (after `apemosyne kafka up`) |
| `APEMOSYNE_PROFILE` | `minimal` | Compose profile for agent/pipeline cluster submit |

See [../.env.example](../.env.example).

## Studio cluster restart

After editing `apemosyne/` runtime code, pipeline cluster codegen, or the Dockerfile:

```bash
./scripts/restart-studio-cluster.sh              # Flink + Kafka + sync + bootstrap
./scripts/restart-studio-cluster.sh --build      # rebuild agent_flink_image first
./scripts/restart-studio-cluster.sh --smoke      # + cluster launch smoke job
./scripts/restart-studio-cluster.sh --sync-only  # copy code only (containers stay up)
```

The script loads `.env`, force-recreates minimal Flink JM/TM, starts Studio Kafka, copies runtime modules into containers (`studio_cluster_sync.py`), and bootstraps Flink Agents thin JARs (avoids Pemja classloader issues on TaskManagers).

## Development

```bash
# No Docker
apemosyne verify --tier quick
pytest test/test_cli_smoke.py test/test_generic_platform.py test/test_api_platform.py

# Docker + doctor
apemosyne verify --tier standard
apemosyne build && apemosyne up && apemosyne test launch --cluster
```

## Subproject integration

1. **`apemosyne/paths.py`** — `examples_dir()`, optional `honeypot_dir()`
2. **`apemosyne/manifests/`** — verify tiers, startup modes, demo catalog
3. **`honeypot/manifests/`** — optional honeypot overlays

## See also

- [../README.md](../README.md) — workspace overview
- [../docs/PLATFORM.md](../docs/PLATFORM.md) — Control API and agents
- [../examples/README.md](../examples/README.md) — example agents
- [../honeypot/README.md](../honeypot/README.md) — optional Cowrie pipeline
