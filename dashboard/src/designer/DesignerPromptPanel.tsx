import type { Node } from "@xyflow/react";
import { DEFAULT_PROMPT_SYSTEM, DEFAULT_PROMPT_USER } from "./promptDefaults";

interface Props {
  nodes: Node[];
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onAddPrompt: () => void;
}

function promptNode(nodes: Node[]): Node | undefined {
  return nodes.find((n) => n.type === "prompt");
}

export function DesignerPromptPanel({ nodes, onUpdateNode, onAddPrompt }: Props) {
  const node = promptNode(nodes);

  if (!node) {
    return (
      <section className="card designer-prompt-panel">
        <div className="designer-prompt-panel-header">
          <h3 style={{ margin: 0 }}>Prompt instructions</h3>
        </div>
        <p className="muted" style={{ margin: "0.5rem 0 0" }}>
          ReAct agents need a prompt node on the canvas. Add one to define system and user
          instructions for the LLM.
        </p>
        <button type="button" style={{ marginTop: "0.75rem" }} onClick={onAddPrompt}>
          + Add prompt node
        </button>
      </section>
    );
  }

  const data = node.data as { name?: string; config?: Record<string, unknown> };
  const config = data.config || {};
  const system = String(config.system ?? "");
  const user = String(config.user ?? "");

  return (
    <section className="card designer-prompt-panel">
      <div className="designer-prompt-panel-header">
        <h3 style={{ margin: 0 }}>Prompt instructions</h3>
        <span className="muted designer-prompt-node-ref">
          Node <code>{node.id}</code>
          {data.name ? ` · ${data.name}` : ""}
        </span>
      </div>
      <p className="muted designer-prompt-hint">
        Tell the LLM what to do. System instructions set behavior; the user template receives
        each input record. Use <code>{"{message}"}</code> and <code>{"{value}"}</code>{" "}
        placeholders.
      </p>

      <label className="studio-label">System prompt</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={7}
        value={system}
        placeholder={DEFAULT_PROMPT_SYSTEM}
        onChange={(e) =>
          onUpdateNode(node.id, {
            config: { ...config, system: e.target.value },
          })
        }
      />

      <label className="studio-label">User prompt template</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={3}
        value={user}
        placeholder={DEFAULT_PROMPT_USER}
        onChange={(e) =>
          onUpdateNode(node.id, {
            config: { ...config, user: e.target.value },
          })
        }
      />
    </section>
  );
}

/** Shared fields for the inspector when a prompt node is selected. */
export function PromptInstructionFields({
  nodeId,
  config,
  onUpdate,
}: {
  nodeId: string;
  config: Record<string, unknown>;
  onUpdate: (nodeId: string, patch: { config: Record<string, unknown> }) => void;
}) {
  const system = String(config.system ?? "");
  const user = String(config.user ?? "");

  return (
    <>
      <p className="muted designer-prompt-hint" style={{ marginTop: 0 }}>
        Instructions sent to the LLM on each run. Edits also appear in the Prompt instructions
        panel above the canvas.
      </p>
      <label className="studio-label">System prompt</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={6}
        value={system}
        placeholder={DEFAULT_PROMPT_SYSTEM}
        onChange={(e) => onUpdate(nodeId, { config: { ...config, system: e.target.value } })}
      />
      <label className="studio-label">User prompt template</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={3}
        value={user}
        placeholder={DEFAULT_PROMPT_USER}
        onChange={(e) => onUpdate(nodeId, { config: { ...config, user: e.target.value } })}
      />
      <p className="muted" style={{ fontSize: "0.82rem" }}>
        Use <code>{"{message}"}</code> and <code>{"{value}"}</code> placeholders.
      </p>
    </>
  );
}
