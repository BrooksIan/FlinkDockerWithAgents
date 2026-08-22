# Dataplane approval bus

Kafka desired-state path for schema / route / replay: **propose → ack → apply**.

## Topics

| Topic | Role |
|-------|------|
| `dataplane.propose` | Agent publishes a plan (`proposal_id`, `target`, `plan`) |
| `dataplane.ack` | Operator/demo publishes `{proposal_id, approved}` |

Created by `ensure_dataplane_topics()` / `./scripts/nifi_load_dataplane_flow.sh`.

## Actions

| Action | Behavior |
|--------|----------|
| `propose` | Live-poll plan for `target` and publish |
| `ack` | Publish approval (or `--nack`) for a `proposal_id` |
| `apply` | Load proposal + require approved ack, then mutate |
| `propose_ack_apply` | Demo one-shot (all three) |

Targets: `schema` | `route` | `replay`.

```bash
python3 examples/agents/run_workflow_dataplane_approval_local.py --action propose --target route
python3 examples/agents/run_workflow_dataplane_approval_local.py --action ack --proposal-id <id>
python3 examples/agents/run_workflow_dataplane_approval_local.py --action apply --target route

# One-shot demo
python3 scripts/demo_dataplane_approval.py --target route
python3 scripts/demo_dataplane_approval.py --target route --dry-run
```

Agent: `workflow_dataplane_approval` · module [`ratatoskr/dataplane/bus.py`](../ratatoskr/dataplane/bus.py).

## Related

- [SCHEMA_GATE.md](SCHEMA_GATE.md) · [ROUTE_ENRICH.md](ROUTE_ENRICH.md) · [REPLAY.md](REPLAY.md)
