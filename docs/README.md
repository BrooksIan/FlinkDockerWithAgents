# Documentation

Guides for the **Apemosyne** Flink Agents workspace. Honeypot-specific docs live under [`honeypot/docs/`](../honeypot/docs/).

## Getting started

| Doc | Description |
|-----|-------------|
| [../README.md](../README.md) | Workspace overview and quick start |
| [PLATFORM.md](PLATFORM.md) | **Control API, agents, observability, Studio cluster, dashboard integration** |
| [../apemosyne/README.md](../apemosyne/README.md) | CLI package and commands |
| [../examples/README.md](../examples/README.md) | Example agents and demos |

## Guides in this directory

| Doc | Description |
|-----|-------------|
| [PLATFORM.md](PLATFORM.md) | Platform control plane — API, agent registry, Studio cluster, verify |
| [FLINK_AGENTS.md](FLINK_AGENTS.md) | Workflow vs ReAct agents — concepts, comparison, diagrams |
| [AGENT_DESIGNER_PLAN.md](AGENT_DESIGNER_PLAN.md) | Agent Designer — visual agent authoring, codegen, roadmap |
| [../dashboard/README.md](../dashboard/README.md) | Dashboard — pages, dev setup, project structure |

## Optional honeypot (`honeypot/docs/`)

| Doc | Description |
|-----|-------------|
| [../honeypot/README.md](../honeypot/README.md) | Cowrie honeypot demo |
| [COWRIE_QUICKSTART.md](../honeypot/docs/COWRIE_QUICKSTART.md) | Cowrie setup |
| [PRODUCTION_ARCHITECTURE.md](../honeypot/docs/PRODUCTION_ARCHITECTURE.md) | Hot path vs ReAct enrichment |

## Architecture diagrams

### Flink Agents (workflow vs ReAct)

| Diagram | Source |
|---------|--------|
| [Overview stack](FLINK_AGENTS.md#what-flink-agents-adds-to-flink) | `docs/images/flink-agents-overview.mmd` |
| [Workflow sequence](FLINK_AGENTS.md#execution-model) | `docs/images/workflow-agent-flow.mmd` |
| [ReAct loop](FLINK_AGENTS.md#execution-model-1) | `docs/images/react-agent-loop.mmd` |
| [Hybrid hot path + enrichment](FLINK_AGENTS.md#recommended-hybrid-pattern) | `docs/images/workflow-vs-react-hybrid.mmd` |

### Honeypot (optional)

| Diagram | Preview |
|---------|---------|
| [Reference architecture](../honeypot/README.md) | ![VeryNiceRA](../honeypot/docs/images/VeryNiceRA.png) |
| Production topology | ![topology](../honeypot/docs/images/production-topology.png) |

PNG sources: `docs/images/` and `honeypot/docs/images/` (`scripts/render_architecture_diagrams.sh`).

## External links

- [Flink Agents 0.3 documentation](https://nightlies.apache.org/flink/flink-agents-docs-release-0.3/)
- [Apache Flink Agents GitHub](https://github.com/apache/flink-agents)
