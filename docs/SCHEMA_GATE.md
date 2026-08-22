# Schema / contract gate

Data-plane agent that **quarantines bad events** via NiFi `ValidateJson` and reports on `schema.violations`. It never uses heal run-state ops (`start`/`stop`/`restart`/`empty_queue`).

## Flow

Prereqs: Studio Kafka + NiFi, then load the shared spine:

```bash
ratatoskr kafka up
ratatoskr up --profile nifi
./scripts/nifi_load_dataplane_flow.sh
```

Path:

`events.raw` → ConsumeRaw → ValidateJson → `events.valid` | `schema.violations`

JSON Schema lives on the `ValidateJson` processor (`SCHEMA_CONTENT_PROPERTY` — real JSON Schema, not Avro).

## Agent

| Phase | Env | Mutations |
|-------|-----|-----------|
| monitor | `SCHEMA_GATE_PHASE=monitor` | none — topic counts + violation sample |
| safe | `SCHEMA_GATE_PHASE=safe` | `ensure_topics`, `ensure_flow` |
| lab | `SCHEMA_GATE_PHASE=lab` | `update_schema_text` only |

Allowed ops: `{ensure_topics, ensure_flow, update_schema_text}`. Heal-like ops are rejected.

```bash
export SCHEMA_GATE_PHASE=monitor
python3 examples/agents/run_workflow_schema_gate_local.py

# Or via catalog
ratatoskr agent run workflow_schema_gate --local
```

## Demo

```bash
python3 scripts/demo_schema_gate.py
python3 scripts/demo_schema_gate.py --phase lab   # swap to tighter schema
```

Publishes valid + invalid JSON to `events.raw`, waits for NiFi, then asserts routing into `events.valid` / `schema.violations` and prints a monitor cycle.

## Related

- Spine: [`ratatoskr/dataplane/`](../ratatoskr/dataplane/) · [`scripts/nifi_load_dataplane_flow.py`](../scripts/nifi_load_dataplane_flow.py)
- Route/enrich: [ROUTE_ENRICH.md](ROUTE_ENRICH.md)
- Replay: [REPLAY.md](REPLAY.md)
