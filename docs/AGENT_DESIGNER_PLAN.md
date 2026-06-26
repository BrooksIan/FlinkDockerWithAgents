# Agent Designer — implementation plan

This document outlines a phased plan to build a **visual Agent Designer** in the Ratatoskr dashboard, complementing the existing **Agent Catalog** (`examples/agents/agent-catalog.yaml`) and **Agentic Studio** (pipeline canvas).

## Goals

1. Let users compose **workflow agents** from actions, tools, and event wiring without hand-editing Python/YAML.
2. Persist designs as versioned definitions the platform can run locally, in Studio pipelines, and on the Flink cluster.
3. Reuse existing introspection (`GET /v1/agents/{name}/graph`, Flink YAML parsing) as the read path; the designer is the write path.

## Current state (baseline)

| Layer | Today |
|-------|--------|
| **Runtime registry** | `examples/agents/agent-manifest.yaml` — module entry, runners, cluster scripts |
| **Catalog** | `examples/agents/agent-catalog.yaml` — categories, display names, I/O schemas |
| **Reference agent** | **Double Value** (`workflow_counter`) — `@action process` → `@tool double` → `OutputEvent` |
| **Introspection** | `agent_graph()` from Flink YAML + execution plan |
| **Studio** | Linear pipelines: source → agent → sink; agent node references manifest name |

The designer targets **workflow agents first**; ReAct agents (multi-step tool loops + optional LLM) come later.

---

## Phase 0 — ReAct LLM defaults (implemented)

Platform-wide OpenAI-compatible settings for all ReAct agents:

- **API:** `GET/PUT /v1/designer/llm-settings` — endpoint URL, model ID, API key (masked on read)
- **Store:** `.ratatoskr/designer.db` with env fallback (`RATATOSKR_LLM_*`, `CLOUDERA_*`, `OPENAI_*`)
- **UI:** Dashboard **Settings** page → **LLM connection** (endpoint, model, test)
- **Catalog:** ReAct category marked `llm_required: true`

---

## Phase 1 — Definition model & store (backend)

**Deliverables**

- `AgentDefinition` dataclass: id, name, type (`workflow`), version, description, graph (nodes/edges), generated artifacts paths.
- Node kinds: `input_event`, `action`, `tool`, `output_event` (aligned with Flink Agents 0.3+ event types).
- Edge kinds: `listens_to`, `calls`, `emits`.
- SQLite store (mirror `ratatoskr/pipelines/store.py` pattern): CRUD + list by category.
- Validation: acyclic tool graph, exactly one primary action, required listen_to event type.

**API**

```
GET    /v1/agent-definitions
POST   /v1/agent-definitions
GET    /v1/agent-definitions/{id}
PUT    /v1/agent-definitions/{id}
DELETE /v1/agent-definitions/{id}
POST   /v1/agent-definitions/{id}/validate
POST   /v1/agent-definitions/{id}/compile   # → preview Python + YAML
```

**Seed**

- Import **Double Value** from existing `workflow_counter` as definition `def_double_value_v1` so the designer opens with a working template.

---

## Phase 2 — Code generation (compile pipeline)

**Inputs:** validated `AgentDefinition` graph.

**Outputs (per definition version):**

1. **Python module** — class with `@action` / `@tool` methods (or module-level actions like `workflow_counter_actions.py` for YAML mode).
2. **Flink YAML** — `agents[].actions[]`, `tools[]`, `listen_to` (matches `examples/agents/workflow_counter/agent.yaml`).
3. **Manifest snippet** — entry point, runner stub, cluster script stub for merge into catalog/manifest on publish.

**Compiler rules (Double Value reference)**

```text
InputEvent (value: int)
    → action: process
        → tool: double(value) → int
    → OutputEvent { input, doubled, agent }
```

**Implementation notes**

- Start with **template-based codegen** (Jinja2 or string templates), not AST manipulation.
- Tool bodies: inline Python expression or reference to a shared tool library (`ratatoskr.tools.*`).
- Keep generated files under `.ratatoskr/agents/{id}/` until user explicitly publishes to `examples/agents/`.

---

## Phase 3 — Designer UI (dashboard)

**Route:** `/designer` and `/designer/:id`

**Layout (three-pane, similar to Studio)**

| Pane | Content |
|------|---------|
| **Palette** | Event types, action blocks, tool blocks (from catalog + builtins) |
| **Canvas** | `@xyflow/react` graph — same interaction model as Studio |
| **Inspector** | Node config: function name, input field mapping, tool expression, listen_to events |

**Workflow-specific palette (MVP)**

- Input: `_input_event` / `InputEvent`
- Actions: `process` (generic), templates from catalog
- Tools: `double`, `identity`, `extract_field` (extensible list)
- Output: `_output_event` / `OutputEvent`

**UX flows**

1. **New from template** — pick catalog entry (e.g. Double Value) → opens pre-wired graph.
2. **Edit** — change tool expression `value * 2` → validate → compile preview.
3. **Test** — run with static records or `workflow.test.input` (reuse pipeline executor with single-agent mini-pipeline).
4. **Publish** — write manifest + catalog entries (admin gate in v1: export ZIP / copy-paste).

**Reuse from Studio**

- `AgentGraphPanel` read-only view → becomes editable with inspector hooks.
- Shared node components (`AgentNode` styling, drag/drop payload format).

---

## Phase 4 — Catalog & Studio integration

1. **Catalog sync** — published definitions appear in `agent-catalog.yaml` (or DB-backed catalog with YAML export).
2. **Studio palette** — load from `/v1/agents/catalog` (already grouped by Workflow / ReAct subcategories).
3. **Pipeline edge mapping** — designer exposes `input_schema` / `output_schema`; Studio suggests mappings (e.g. `workflow_counter → react_echo`: `message: $.doubled`).
4. **Run linkage** — agent runs and pipeline runs share span format; designer test runs create `kind=local` runs visible on `/runs`.

---

## Phase 5 — ReAct & advanced workflow (later)

- ReAct loop node: planner → tool selection → observation → terminate.
- LLM tool nodes (Cloudera / OpenAI) with credential profiles from `.env`.
- Conditional edges (branch on field value).
- Sub-graph agents (agent calls agent).
- Flink cluster submit from designer (reuse `submit_agent_cluster`).

---

## Data model sketch

```yaml
# AgentDefinition (stored JSON)
id: def_double_value_v1
name: Double Value
type: workflow
version: 1
description: Doubles numeric input values
graph:
  nodes:
    - { id: in1, kind: input_event, event_type: _input_event }
    - { id: act1, kind: action, name: process, listens_to: [_input_event] }
    - { id: tool1, kind: tool, name: double, expression: "value * 2" }
    - { id: out1, kind: output_event, event_type: _output_event }
  edges:
    - { source: in1, target: act1, kind: listens_to }
    - { source: act1, target: tool1, kind: calls }
    - { source: act1, target: out1, kind: emits }
io:
  input_schema: { type: object, properties: { value: { type: integer } } }
  output_schema: { type: object, properties: { input: {}, doubled: {}, agent: {} } }
catalog:
  category_id: workflow
  subcategory_id: transform
  tags: [demo, transform, numeric]
manifest: workflow_counter   # set after publish
```

---

## Testing strategy

| Layer | Tests |
|-------|--------|
| Catalog | `test/test_agent_catalog.py` (done) |
| Definition store | CRUD + validation unit tests |
| Compiler | Golden files: Double Value → match existing `workflow_counter.py` / `agent.yaml` |
| API | FastAPI TestClient for definitions routes |
| UI | Playwright smoke: create graph, validate, preview compile |
| E2E | Designer agent → Studio pipeline → `workflow.test.output` consumer |

---

## Recommended build order

1. **Phase 1** — definition model + API + seed Double Value (1–2 days)
2. **Phase 2** — compiler templates + compile preview API (2–3 days)
3. **Phase 3** — minimal designer UI (palette + canvas + inspector) (3–5 days)
4. **Phase 4** — catalog publish + Studio palette refresh (1 day)
5. **Phase 5** — ReAct / LLM (future milestone)

---

## Open decisions

| Question | Recommendation |
|----------|----------------|
| Store definitions in SQLite or YAML files? | SQLite for drafts; YAML export on publish (consistent with pipelines) |
| Inline tool code vs library-only? | Library-only for MVP; inline editor in Phase 5 |
| Overwrite `examples/agents/` on publish? | No — write to `.ratatoskr/agents/`; manual or gated promote |
| Designer route name? | `/designer` (nav item under Agents) |

---

## Success criteria for MVP

- [ ] User opens Designer, sees **Double Value** template graph.
- [ ] User changes multiplier (e.g. triple), compiles, runs against `workflow.test.input`.
- [ ] New agent appears in catalog under **Workflow → Transform**.
- [ ] Studio palette shows new display name; pipeline run writes to Kafka sink.

See also: [PLATFORM.md](./PLATFORM.md) (Studio section), [examples/README.md](../examples/README.md) (agent registry).
