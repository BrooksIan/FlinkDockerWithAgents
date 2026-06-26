import type { Edge, Node } from "@xyflow/react";
import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { AgentEdgeKind, AgentNodeKind, DesignerSkill, LlmCallConfig } from "../api/types";
import { PromptInstructionFields } from "./DesignerPromptPanel";
import { kindLabel } from "./definitionUtils";

interface Props {
  selectedNode: Node | null;
  selectedEdge: Edge | null;
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onUpdateEdge: (edgeId: string, kind: AgentEdgeKind) => void;
  onDeleteNode: (nodeId: string) => void;
  onDeleteEdge: (edgeId: string) => void;
}

const EDGE_KINDS: AgentEdgeKind[] = ["listens_to", "calls", "emits"];

function llmConfigFromNode(config: Record<string, unknown>): LlmCallConfig {
  const mode = config.mode === "flink_skills" ? "flink_skills" : "simple";
  const skills = Array.isArray(config.skills)
    ? config.skills.map((item) => String(item)).filter(Boolean)
    : [];
  const allowed_commands = Array.isArray(config.allowed_commands)
    ? config.allowed_commands.map((item) => String(item)).filter(Boolean)
    : [];
  return {
    use_platform_llm: config.use_platform_llm !== false,
    mode,
    skills,
    allowed_commands,
  };
}

function mergeAllowedCommands(
  selectedSkills: string[],
  catalog: DesignerSkill[],
  current: string[],
): string[] {
  const merged = [...current];
  for (const skillId of selectedSkills) {
    const entry = catalog.find((item) => item.id === skillId);
    if (!entry) continue;
    for (const command of entry.default_allowed_commands) {
      if (!merged.includes(command)) merged.push(command);
    }
  }
  return merged;
}

export function DesignerInspector({
  selectedNode,
  selectedEdge,
  onUpdateNode,
  onUpdateEdge,
  onDeleteNode,
  onDeleteEdge,
}: Props) {
  const [skillCatalog, setSkillCatalog] = useState<DesignerSkill[]>([]);
  const [skillsError, setSkillsError] = useState<string | null>(null);

  useEffect(() => {
    api
      .designerSkills()
      .then(setSkillCatalog)
      .catch((err) => setSkillsError(String(err)));
  }, []);

  if (!selectedNode && !selectedEdge) {
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Inspector</h3>
        <p className="muted">Select a node or edge to configure agent logic.</p>
      </div>
    );
  }

  if (selectedEdge) {
    const kind = ((selectedEdge.data as { kind?: AgentEdgeKind })?.kind || "listens_to") as AgentEdgeKind;
    return (
      <div className="studio-inspector card">
        <h3 style={{ marginTop: 0 }}>Edge</h3>
        <p className="muted">
          {selectedEdge.source} → {selectedEdge.target}
        </p>
        <label className="studio-label">Edge kind</label>
        <select
          className="studio-select"
          value={kind}
          onChange={(e) => onUpdateEdge(selectedEdge.id, e.target.value as AgentEdgeKind)}
        >
          {EDGE_KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
        <div className="actions" style={{ marginTop: "1rem" }}>
          <button type="button" className="secondary" onClick={() => onDeleteEdge(selectedEdge.id)}>
            Delete edge
          </button>
        </div>
      </div>
    );
  }

  if (!selectedNode) return null;

  const kind = (selectedNode.type as AgentNodeKind) || "action";
  const data = selectedNode.data as {
    name?: string;
    config?: Record<string, unknown>;
  };
  const config = data.config || {};
  const name = data.name || "";

  return (
    <div className="studio-inspector card">
      <h3 style={{ marginTop: 0 }}>{kindLabel(kind)}</h3>
      <p className="muted">
        Node <code>{selectedNode.id}</code>
      </p>

      <label className="studio-label">Name</label>
      <input
        className="studio-input"
        type="text"
        value={name}
        onChange={(e) => onUpdateNode(selectedNode.id, { name: e.target.value })}
      />

      {kind === "input_event" || kind === "output_event" ? (
        <>
          <label className="studio-label">Event type</label>
          <input
            className="studio-input"
            type="text"
            value={String(config.event_type || "")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, event_type: e.target.value },
              })
            }
          />
        </>
      ) : null}

      {kind === "action" ? (
        <>
          <label className="studio-label">Listens to (JSON array)</label>
          <textarea
            className="studio-textarea"
            rows={3}
            defaultValue={JSON.stringify(config.listens_to || ["_input_event"])}
            key={`${selectedNode.id}-listens`}
            onBlur={(e) => {
              try {
                const listens_to = JSON.parse(e.target.value) as string[];
                onUpdateNode(selectedNode.id, { config: { ...config, listens_to } });
              } catch {
                /* keep previous */
              }
            }}
          />
        </>
      ) : null}

      {kind === "tool" ? (
        <>
          <label className="studio-label">Tool ref</label>
          <select
            className="studio-select"
            value={String(config.tool_ref || "double")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, tool_ref: e.target.value },
              })
            }
          >
            <option value="double">double</option>
            <option value="scale">scale</option>
            <option value="identity">identity</option>
          </select>
          <label className="studio-label">Expression</label>
          <input
            className="studio-input"
            type="text"
            value={String(config.expression || "")}
            onChange={(e) =>
              onUpdateNode(selectedNode.id, {
                config: { ...config, expression: e.target.value },
              })
            }
          />
          {config.tool_ref === "scale" || String(config.expression || "").includes("factor") ? (
            <>
              <label className="studio-label">Scale factor</label>
              <input
                className="studio-input"
                type="number"
                min={1}
                value={Number(config.factor || 2)}
                onChange={(e) =>
                  onUpdateNode(selectedNode.id, {
                    config: { ...config, factor: parseInt(e.target.value, 10) || 2 },
                  })
                }
              />
            </>
          ) : null}
        </>
      ) : null}

      {kind === "prompt" ? (
        <PromptInstructionFields
          nodeId={selectedNode.id}
          config={config}
          onUpdate={(nodeId, patch) => onUpdateNode(nodeId, patch)}
        />
      ) : null}

      {kind === "llm_call" ? (
        <>
          <label className="studio-label">Execution mode</label>
          <select
            className="studio-select"
            value={llmConfigFromNode(config).mode || "simple"}
            onChange={(e) => {
              const mode = e.target.value === "flink_skills" ? "flink_skills" : "simple";
              const next: LlmCallConfig = {
                ...llmConfigFromNode(config),
                mode,
              };
              if (mode === "flink_skills" && (!next.skills || next.skills.length === 0)) {
                next.skills = skillCatalog.length ? [skillCatalog[0].id] : ["math-calculator"];
                next.allowed_commands = mergeAllowedCommands(
                  next.skills,
                  skillCatalog,
                  next.allowed_commands || [],
                );
              }
              onUpdateNode(selectedNode.id, {
                config: { ...config, ...next },
              });
            }}
          >
            <option value="simple">Simple (HTTP LLM)</option>
            <option value="flink_skills">Flink skills (native chat model)</option>
          </select>

          {llmConfigFromNode(config).mode === "simple" ? (
            <>
              <label className="studio-label">
                <input
                  type="checkbox"
                  checked={llmConfigFromNode(config).use_platform_llm !== false}
                  onChange={(e) =>
                    onUpdateNode(selectedNode.id, {
                      config: { ...config, use_platform_llm: e.target.checked },
                    })
                  }
                />{" "}
                Use platform LLM from Settings
              </label>
              <p className="muted" style={{ fontSize: "0.85rem" }}>
                Direct OpenAI-compatible call via Designer settings. Compile appends JSON response
                rules to the system prompt.
              </p>
            </>
          ) : (
            <>
              <p className="muted" style={{ fontSize: "0.85rem" }}>
                Uses native <code>@chat_model_setup</code> with <code>load_skill</code> and{" "}
                <code>bash</code>. Configure endpoint and model in <strong>Settings</strong>.
              </p>
              {skillsError ? (
                <p className="error-text" style={{ fontSize: "0.85rem" }}>
                  {skillsError}
                </p>
              ) : null}
              <label className="studio-label">Skills</label>
              {skillCatalog.length === 0 ? (
                <p className="muted" style={{ fontSize: "0.85rem" }}>
                  No skills found under <code>examples/skills/</code>.
                </p>
              ) : (
                <div className="designer-skill-list">
                  {skillCatalog.map((skill) => {
                    const selected = (llmConfigFromNode(config).skills || []).includes(skill.id);
                    return (
                      <label key={skill.id} className="designer-skill-option">
                        <input
                          type="checkbox"
                          checked={selected}
                          onChange={(e) => {
                            const current = llmConfigFromNode(config).skills || [];
                            const nextSkills = e.target.checked
                              ? [...current, skill.id]
                              : current.filter((item) => item !== skill.id);
                            const nextCommands = mergeAllowedCommands(
                              nextSkills,
                              skillCatalog,
                              llmConfigFromNode(config).allowed_commands || [],
                            );
                            onUpdateNode(selectedNode.id, {
                              config: {
                                ...config,
                                mode: "flink_skills",
                                skills: nextSkills,
                                allowed_commands: nextCommands,
                              },
                            });
                          }}
                        />
                        <span>
                          <strong>{skill.name}</strong>
                          {skill.description ? (
                            <span className="muted"> — {skill.description}</span>
                          ) : null}
                        </span>
                      </label>
                    );
                  })}
                </div>
              )}
              <label className="studio-label">Allowed bash commands</label>
              <input
                className="studio-input"
                type="text"
                value={(llmConfigFromNode(config).allowed_commands || []).join(", ")}
                placeholder="echo, bc"
                onChange={(e) => {
                  const allowed_commands = e.target.value
                    .split(",")
                    .map((item) => item.trim())
                    .filter(Boolean);
                  onUpdateNode(selectedNode.id, {
                    config: { ...config, mode: "flink_skills", allowed_commands },
                  });
                }}
              />
              <p className="muted" style={{ fontSize: "0.85rem" }}>
                Whitelist for the built-in <code>bash</code> tool. Auto-filled from selected skills.
              </p>
            </>
          )}
        </>
      ) : null}

      <div className="actions" style={{ marginTop: "1rem" }}>
        <button type="button" className="secondary" onClick={() => onDeleteNode(selectedNode.id)}>
          Delete node
        </button>
      </div>
    </div>
  );
}
