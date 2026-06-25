# Apemosyne — Flink Agents CLI

Python package that provides the **`apemosyne`** command-line tool for this workspace.

## Install

From the repository root:

```bash
pip install -e .
apemosyne --help
```

Entry point: `apemosyne.cli:app` (Typer).

## What it does

| Area | Module / command | Purpose |
|------|------------------|---------|
| Stack | `up`, `down`, `status`, `logs` | Docker Compose lifecycle |
| Build | `build` | Build `agent_flink_image` from `Dockerfile` |
| Demos | `demo` | Run Flink Agents demos in TaskManager |
| Verify | `verify` | Tiered checks (`quick` → `nightly`) |
| Tests | `test` | Launch smoke, phase1–3, production e2e |
| Config | `config`, `doctor` | Env validation and pre-flight fixes |
| Sync | `sync`, `sync-env` | Copy manifests / `.env` into containers |
| Process | `process` | Run honeypot pipeline scripts on the host |

## Package layout

```text
apemosyne/
├── cli.py                 # Top-level Typer app
├── docker_utils.py        # Compose helpers, container exec
├── paths.py               # Repo root, honeypot_dir(), PYTHONPATH
├── manifests.py           # YAML copy/sync catalogs
├── startup_modes.py       # up --mode presets
├── config.py              # Merged .env / profile config
├── checks/                # doctor, demo-ready
└── commands/
    ├── stack.py           # up / down / ensure-kafka / ensure-flink-jobs
    ├── build.py
    ├── demo.py
    ├── test_cmd.py
    ├── verify_cmd.py
    ├── process.py         # log processor + sidecar scripts
    └── …
```

## Compose profiles

| Profile | Compose file | Stack |
|---------|--------------|-------|
| `minimal` | `docker-compose.yml` | JobManager + TaskManager |
| `full` | `honeypot/docker-compose.yml` | Cowrie honeypot + Kafka + pipeline + dashboard |

## Subproject integration

Subprojects register with the CLI via:

1. **`apemosyne/paths.py`** — `honeypot_dir()`, `examples_dir()`, `configure_runtime_sys_path()`
2. **`apemosyne/manifests/`** — generic verify tiers, startup modes, demo catalog
3. **`honeypot/manifests/`** — optional honeypot file-copy manifests and extra demos

To add a new subproject, create a sibling directory (e.g. `myapp/`) with its own compose file and manifests, then extend profiles or startup modes.

## Development

```bash
# Fast local checks (no Docker)
apemosyne verify --tier quick
pytest test/

# With Docker image
apemosyne verify --tier standard
```

## See also

- [../README.md](../README.md) — workspace overview
- [../honeypot/README.md](../honeypot/README.md) — Cowrie honeypot subproject
- [../test/README.md](../test/README.md) — test layout
