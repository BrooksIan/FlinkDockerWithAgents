# Honeypot tests

Unit and integration tests for the **Cowrie honeypot subproject**. CLI-level tests are in [`../../test/`](../../test/).

## Run

```bash
# From repo root
pytest honeypot/test/

# Single file
python3 honeypot/test/test_cowrie_policy.py
```

## Categories

| Area | Examples |
|------|----------|
| Policy & alerts | `test_cowrie_policy.py`, `test_cowrie_security_alert.py` |
| Pipeline | `test_phase2_engine.py`, `test_production_pipeline.py` |
| Traps / actor | `test_trap_state.py`, `test_actor_classify.py` |
| ReAct | `test_react_dashboard_bridge.py`, `test_phase3_react.py` |
| Cluster e2e | `test_phase1_cluster.py`, `test_phase2_cluster.py` |

## Cluster tests

Require a running full stack:

```bash
ratatoskr up --profile full
ratatoskr test phase1 --e2e
```

Or via verify:

```bash
ratatoskr verify --tier nightly
```

## Script-style tests

These modules run integration checks at import or via `python3 …` — pytest skips them when dependencies are missing:

- `test_cloudera_llm.py`, `test_react_simple.py` — Cloudera / `flink_agents`
- `test_alerting.py` — `flink_agents` + demo imports
- `test_clapback.py` — counter-attack demo script

## See also

- [../README.md](../README.md) — honeypot subproject
- [../../test/README.md](../../test/README.md) — workspace test overview
