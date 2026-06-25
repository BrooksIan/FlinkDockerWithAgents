# Flink Agents Examples

Generic demos that run on the **minimal Flink stack** with no honeypot dependencies.

Part of the [Flink Agents CLI](../README.md) workspace.

## Prerequisites

```bash
pip install -e .
apemosyne build
apemosyne up              # or: apemosyne up --profile minimal
```

## Demos

| Demo | Command | Description |
|------|---------|-------------|
| Datastream | `apemosyne demo datastream` | Flink Agents DataStream smoke test |
| Table | `apemosyne demo table` | Table API smoke test |

Source files (when present in this directory):

- `demo_datastream.py`
- `demo_table.py`
- `demo_datastream_local.py` — host-only variant
- `demo-files.yaml` — manifest for `apemosyne sync`

## Cowrie / security demos

Honeypot-specific demos live under [`honeypot/demo/`](../honeypot/demo/) — see [honeypot/README.md](../honeypot/README.md).

Examples:

```bash
apemosyne demo cowrie
apemosyne up --profile full
```

## Adding an example

1. Add `demo_myexample.py` under `examples/`
2. Register it in `examples/demo-files.yaml` (or the merged demo catalog)
3. Ensure it only imports `flink_agents` and stdlib — no `honeypot/` imports
4. Document the `apemosyne demo myexample` command here
