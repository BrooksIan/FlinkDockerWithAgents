import type { AgentNodeKind } from "../api/types";
import type { DesignerDroppedSpec } from "./definitionUtils";
import { kindLabel } from "./definitionUtils";

interface Props {
  agentType: string;
  onAdd: (spec: DesignerDroppedSpec) => void;
}

function dragPayload(spec: DesignerDroppedSpec) {
  return JSON.stringify(spec);
}

function PaletteItem({
  label,
  sub,
  spec,
  onClick,
}: {
  label: string;
  sub?: string;
  spec: DesignerDroppedSpec;
  onClick: () => void;
}) {
  return (
    <div
      className="studio-palette-item"
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData("application/reactflow", dragPayload(spec));
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

const WORKFLOW_BLOCKS: DesignerDroppedSpec[] = [
  { kind: "input_event", name: "InputEvent", config: { event_type: "_input_event" } },
  { kind: "action", name: "process", config: { listens_to: ["_input_event"] } },
  { kind: "tool", name: "double", config: { tool_ref: "double", expression: "value * 2" } },
  {
    kind: "tool",
    name: "scale",
    config: { tool_ref: "scale", expression: "value * 3", factor: 3 },
  },
  { kind: "output_event", name: "OutputEvent", config: { event_type: "_output_event" } },
];

function blockSpec(kind: AgentNodeKind, name: string, config: Record<string, unknown>): DesignerDroppedSpec {
  return { kind, name, config };
}

export function DesignerPalette({ agentType, onAdd }: Props) {
  const blocks =
    agentType === "react"
      ? [
          ...WORKFLOW_BLOCKS.slice(0, 2),
          blockSpec("prompt", "prompt", { template: "system" }),
          blockSpec("llm_call", "llm", { use_platform_llm: true }),
          WORKFLOW_BLOCKS[4],
        ]
      : WORKFLOW_BLOCKS;

  return (
    <div className="studio-palette card">
      <h3 style={{ marginTop: 0 }}>Palette</h3>
      <p className="muted">
        Drag blocks onto the canvas. Connect input → action → tools → output.
      </p>

      <h4>Events</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "input_event" || b.kind === "output_event")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${kindLabel(spec.kind)}`}
              sub={spec.name}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>

      <h4>Logic</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "action" || b.kind === "prompt" || b.kind === "llm_call")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${kindLabel(spec.kind)}`}
              sub={spec.name}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>

      <h4>Tools</h4>
      <div className="studio-palette-actions">
        {blocks
          .filter((b) => b.kind === "tool")
          .map((spec) => (
            <PaletteItem
              key={`${spec.kind}-${spec.name}`}
              label={`+ ${spec.name}`}
              sub={(spec.config?.expression as string) || ""}
              spec={spec}
              onClick={() => onAdd(spec)}
            />
          ))}
      </div>
    </div>
  );
}
