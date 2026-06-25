import type { AgentSummary } from "../api/types";

const DEFAULT_RECORDS = '[\n  { "key": "1", "value": 3 },\n  { "key": "2", "value": 10 }\n]';

interface Props {
  agents: AgentSummary[];
  onAddSource: () => void;
  onAddSink: () => void;
  onAddAgent: (agent: AgentSummary) => void;
}

function dragPayload(kind: "source" | "agent" | "sink", agent?: AgentSummary) {
  return JSON.stringify({
    kind,
    agent: agent?.name,
    agentType: agent?.type,
    description: agent?.description,
  });
}

function PaletteItem({
  label,
  sub,
  payload,
  onClick,
}: {
  label: string;
  sub?: string;
  payload: string;
  onClick: () => void;
}) {
  return (
    <div
      className="studio-palette-item"
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData("application/reactflow", payload);
        e.dataTransfer.effectAllowed = "move";
      }}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <span className="studio-palette-item-label">{label}</span>
      {sub && <span className="studio-palette-item-sub muted">{sub}</span>}
    </div>
  );
}

export function NodePalette({ agents, onAddSource, onAddSink, onAddAgent }: Props) {
  return (
    <div className="studio-palette card">
      <h3 style={{ marginTop: 0 }}>Palette</h3>
      <p className="muted">Drag nodes onto the canvas. Connect by dragging between the blue dots (left → right).</p>
      <div className="studio-palette-actions">
        <PaletteItem
          label="+ Source"
          sub="Input records"
          payload={dragPayload("source")}
          onClick={onAddSource}
        />
        <PaletteItem label="+ Sink" sub="Output" payload={dragPayload("sink")} onClick={onAddSink} />
      </div>
      <h4>Agents</h4>
      <ul className="studio-agent-list">
        {agents.map((a) => (
          <li key={a.name}>
            <PaletteItem
              label={`+ ${a.name}`}
              sub={a.type}
              payload={dragPayload("agent", a)}
              onClick={() => onAddAgent(a)}
            />
          </li>
        ))}
      </ul>
      <p className="muted" style={{ fontSize: "0.8rem", marginTop: "1rem" }}>
        Default source records:
        <pre className="yaml" style={{ marginTop: "0.5rem" }}>
          {DEFAULT_RECORDS}
        </pre>
      </p>
    </div>
  );
}
