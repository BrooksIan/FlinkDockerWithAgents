# Backfill / replay job

Lab-gated **replay job** (not Kafka/NiFi heal): reprocess a time window from topic X into topic Y using a **dedicated** consumer group and Replay* processors.

## Defaults

| Knob | Default |
|------|---------|
| Source | `events.valid` |
| Dest | `events.replay.out` |
| Group | `ratatoskr-dataplane-replay` |
| Hours | `1` (`REPLAY_HOURS`) |

Live schema/route groups (`ratatoskr-dataplane-schema`, `ratatoskr-dataplane-route`) are **not** reset.

## Job steps

1. Stop ReplayConsume / ReplayMark / ReplayPublish  
2. `KafkaClient.reset_offsets_by_timestamp(group, source, now - N hours)`  
3. Start replay path  
4. Wait for dest topic growth (catch-up)  
5. Stop replay path  

## Phases

| Phase | Env | Behavior |
|-------|-----|----------|
| monitor | `REPLAY_PHASE=monitor` | Plan only (`planned: true` steps) |
| lab | `REPLAY_PHASE=lab` | Execute (respects `REPLAY_DRY_RUN`) |

```bash
./scripts/nifi_load_dataplane_flow.sh

python3 examples/agents/run_workflow_replay_local.py --phase monitor --hours 2
REPLAY_PHASE=lab python3 examples/agents/run_workflow_replay_local.py --hours 1
```

## Demo

```bash
python3 scripts/demo_replay.py
python3 scripts/demo_replay.py --dry-run
```

## API

[`KafkaClient.reset_offsets_by_timestamp`](../ratatoskr/kafka/client.py) commits each partition to the first offset at/after the given epoch ms (falls back to log end when no match).

## Related

- [SCHEMA_GATE.md](SCHEMA_GATE.md) · [ROUTE_ENRICH.md](ROUTE_ENRICH.md)
- Heal remains in [NIFI_MONITOR.md](NIFI_MONITOR.md) / [KAFKA_MONITOR.md](KAFKA_MONITOR.md)
