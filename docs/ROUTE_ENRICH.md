# Routing and enrichment

Agents **propose** declarative routing rules; NiFi **executes** them by patching `EnrichUpdate` / `RouteType` processor properties (`config_apply`, not heal).

## Flow

Downstream of the schema gate (same **Ratatoskr Data Plane** PG):

`events.valid` → ConsumeValid → EnrichUpdate → RouteType → PublishEnriched → `events.enriched`

Unmatched routes are auto-terminated. Default rule matches `event.type == order`.

```bash
./scripts/nifi_load_dataplane_flow.sh
```

## Rule shape

```json
{
  "match": { "type": "order" },
  "set": { "env": "lab", "pipeline": "dataplane" },
  "route": "enriched"
}
```

Mapped to:

- `EnrichUpdate`: `ratatoskr.env`, `ratatoskr.pipeline`, `event.type`, …
- `RouteType`: `Routing Strategy` + named property (e.g. `enriched=${event.type:equals('order')}`)

## Phases

| Phase | Env | Behavior |
|-------|-----|----------|
| monitor | `ROUTE_PHASE=monitor` | Diff proposed vs live; no writes |
| safe | `ROUTE_PHASE=safe` | Allowlisted property patches (`SAFE_*` keys) |
| lab | `ROUTE_PHASE=lab` | Broader allowlist (`LAB_*` keys: region/team, extra routes) |

Stop→patch→start is recorded as `config_apply`, never as heal.

```bash
python3 examples/agents/run_workflow_route_enrich_local.py --phase monitor
python3 examples/agents/run_workflow_route_enrich_local.py --phase safe \
  --rule-json '{"match":{"type":"order"},"set":{"env":"prod"},"route":"enriched"}'
```

## Demo

```bash
python3 scripts/demo_route_enrich.py
```

## Related

- [SCHEMA_GATE.md](SCHEMA_GATE.md) · [REPLAY.md](REPLAY.md)
