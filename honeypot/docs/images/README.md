# Architecture images

PNG diagrams referenced from [honeypot/README.md](../../README.md) and [docs/PRODUCTION_ARCHITECTURE.md](../../../docs/PRODUCTION_ARCHITECTURE.md).

| File | Used in |
|------|---------|
| `VeryNiceRA.png` | honeypot README (primary reference architecture) |
| `PrettyRASlide.png` | honeypot README (simplified diagram), root README, docs README |
| `production-topology.png` | PRODUCTION_ARCHITECTURE.md |
| `production-pipeline-phases.png` | PRODUCTION_ARCHITECTURE.md |
| `production-hot-path-vs-enrichment.png` | PRODUCTION_ARCHITECTURE.md |
| `production-components.png` | PRODUCTION_ARCHITECTURE.md |

Optional Mermaid sources (`*.mmd`) can be rendered with:

```bash
./scripts/render_architecture_diagrams.sh
```

Dashboard screenshots can be regenerated with:

```bash
.venv/bin/python scripts/generate_dashboard_images.py
```
