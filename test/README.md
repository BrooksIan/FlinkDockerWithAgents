# Tests

Tests for the **Flink Agents CLI** and workspace-wide behavior. Honeypot-specific tests live under [`honeypot/test/`](../honeypot/test/).

## Layout

| Directory | Scope |
|-----------|--------|
| `test/` (here) | CLI, config, manifests, doctor, verify tiers, launch smoke |
| `honeypot/test/` | Cowrie pipeline, policy, traps, ReAct, cluster e2e |

## Run locally

```bash
pip install -e .
pytest test/
pytest honeypot/test/
```

`conftest.py` in each tree calls `apemosyne.paths.configure_runtime_sys_path()` so `honeypot/src/*` modules import correctly.

## Verify tiers

```bash
apemosyne verify --tier quick       # no Docker
apemosyne verify --tier standard    # + Docker smoke
apemosyne verify --tier full
apemosyne verify --tier nightly     # cluster e2e (full stack up)
```

Tiers are defined in `apemosyne/manifests/verify-tiers.yaml` (generic workspace checks). Honeypot-specific tiers may also exist under `honeypot/manifests/`.

## Launch smoke test

Confirms Flink Agents can build and execute a minimal workflow:

```bash
apemosyne test launch
apemosyne test launch --cluster --in-container   # visible in Flink Web UI
```

## Docker helper scripts

Optional shell wrappers (require running TaskManager):

- `test_launch_flink_agents_docker.sh`
- `test_cloudera_llm_docker.sh`
- `test_react_agent_docker.sh`
- `test_react_cloudera_openai_docker.sh`

Default container name: `honeypot-taskmanager-1` (override with `FLINK_CONTAINER`).

## Integration scripts

Some files under `honeypot/test/` are **scripts**, not pytest unit tests. They skip collection unless run directly or via `apemosyne test`:

- `test_cloudera_llm.py`, `test_react_simple.py` — need `flink_agents` / Cloudera JWT
- `test_phase1_cluster.py`, `test_phase2_cluster.py` — need Kafka + Flink cluster

## See also

- [../apemosyne/README.md](../apemosyne/README.md)
- [../honeypot/README.md](../honeypot/README.md)
