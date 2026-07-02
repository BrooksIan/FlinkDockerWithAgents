import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type {
  AgentAssistTypePreference,
  AgentDefinition,
  AgentDefinitionAssistResult,
  ReactLlmSettings,
} from "../api/types";
import { computeAssistDiff } from "./assistDiff";

type AssistMode = "create" | "refine";

interface Props {
  mode: AssistMode;
  definition?: AgentDefinition | null;
  llmSettings?: ReactLlmSettings | null;
  busy?: boolean;
  onResult?: (result: AgentDefinitionAssistResult) => void;
  onAccept?: (result: AgentDefinitionAssistResult) => void | Promise<void>;
  onError?: (message: string) => void;
}

function diffList(title: string, items: string[]) {
  if (!items.length) return null;
  return (
    <div className="designer-assist-diff-block">
      <strong>{title}</strong>
      <ul className="designer-assist-diff-list">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
}

export function DesignerAssistPanel({
  mode,
  definition,
  llmSettings,
  busy: externalBusy,
  onResult,
  onAccept,
  onError,
}: Props) {
  const [goal, setGoal] = useState("");
  const [instruction, setInstruction] = useState("");
  const [agentType, setAgentType] = useState<AgentAssistTypePreference>("auto");
  const [constraintsText, setConstraintsText] = useState("");
  const [constraintsError, setConstraintsError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<AgentDefinitionAssistResult | null>(null);

  const llmConfigured = llmSettings?.configured ?? true;
  const isBusy = busy || Boolean(externalBusy);

  const diff = useMemo(() => {
    if (!result || mode !== "refine") return null;
    return computeAssistDiff(definition, result.definition);
  }, [definition, mode, result]);

  function parseConstraints(): Record<string, unknown> | null | undefined {
    const trimmed = constraintsText.trim();
    if (!trimmed) return undefined;
    try {
      const parsed = JSON.parse(trimmed) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        setConstraintsError("Constraints must be a JSON object.");
        return null;
      }
      setConstraintsError(null);
      return parsed as Record<string, unknown>;
    } catch {
      setConstraintsError("Invalid JSON for constraints.");
      return null;
    }
  }

  async function handleGenerate() {
    const text = mode === "create" ? goal : instruction;
    if (!text.trim()) {
      onError?.(mode === "create" ? "Describe what the agent should do." : "Enter a refinement instruction.");
      return;
    }
    if (!llmConfigured) {
      onError?.("Configure the Designer LLM in Settings before using assist.");
      return;
    }

    const constraints = mode === "create" ? parseConstraints() : undefined;
    if (constraints === null) return;

    setBusy(true);
    onError?.("");
    try {
      const response =
        mode === "create"
          ? await api.assistGenerateAgentDefinition({
              goal: goal.trim(),
              agent_type_preference: agentType,
              constraints,
            })
          : await api.assistRefineAgentDefinition(definition!.id, {
              instruction: instruction.trim(),
              agent_type_preference: agentType === "auto" ? null : agentType,
            });
      setResult(response);
      onResult?.(response);
    } catch (err) {
      onError?.(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleAccept() {
    if (!result) return;
    try {
      await onAccept?.(result);
    } catch (err) {
      onError?.(String(err));
    }
  }

  return (
    <section className="card designer-assist-panel">
      <div className="designer-assist-header">
        <div>
          <h3 style={{ margin: 0 }}>
            {mode === "create" ? "Design with LLM" : "Ask LLM to improve this agent"}
          </h3>
          <p className="muted designer-assist-hint">
            {mode === "create"
              ? "Describe your goal. The Designer LLM proposes a draft graph, schemas, and sample test records. Nothing is saved until you accept."
              : "Describe changes you want. Review the diff preview before applying edits to this draft."}
          </p>
        </div>
        {!llmConfigured && (
          <p className="error designer-assist-llm-warning">
            Designer LLM is not configured. Open <Link to="/settings">Settings</Link> to connect an
            endpoint, model, and API key.
          </p>
        )}
      </div>

      <div className="designer-assist-form">
        <label className="studio-label">
          {mode === "create" ? "Agent goal" : "Improvement instruction"}
        </label>
        <textarea
          className="studio-textarea designer-assist-goal"
          rows={mode === "create" ? 4 : 3}
          value={mode === "create" ? goal : instruction}
          disabled={isBusy}
          placeholder={
            mode === "create"
              ? "Example: Build a workflow agent that doubles numeric input values and tags results for the transform catalog."
              : "Example: Add an MCP IP check tool and update the output schema to include the lookup result."
          }
          onChange={(e) => {
            if (mode === "create") setGoal(e.target.value);
            else setInstruction(e.target.value);
          }}
        />

        <div className="designer-assist-controls">
          <label className="create-agent-field">
            <span className="muted">Agent type</span>
            <select
              className="studio-select"
              value={agentType}
              disabled={isBusy}
              onChange={(e) => setAgentType(e.target.value as AgentAssistTypePreference)}
            >
              <option value="auto">Auto</option>
              <option value="workflow">Workflow</option>
              <option value="react">ReAct (LLM)</option>
            </select>
          </label>
          <button type="button" disabled={isBusy || !llmConfigured} onClick={handleGenerate}>
            {isBusy ? "Thinking…" : mode === "create" ? "Generate draft" : "Propose changes"}
          </button>
        </div>

        {mode === "create" && (
          <>
            <label className="studio-label">Optional constraints (JSON)</label>
            <textarea
              className="studio-textarea designer-assist-constraints"
              rows={3}
              value={constraintsText}
              disabled={isBusy}
              placeholder={'{"input_shape": {"value": "number"}, "tools_allowed": ["double"]}'}
              onChange={(e) => {
                setConstraintsText(e.target.value);
                setConstraintsError(null);
              }}
            />
            {constraintsError && <p className="error">{constraintsError}</p>}
          </>
        )}
      </div>

      {result && (
        <div className="designer-assist-preview">
          <div className="designer-assist-preview-header">
            <h4 style={{ margin: 0 }}>Draft preview</h4>
            <span
              className={`badge ${result.validation.valid ? "ok" : "warn"}`}
              title={result.validation.errors.join("; ")}
            >
              {result.validation.valid ? "Valid draft" : "Needs fixes"}
            </span>
          </div>

          {result.rationale && (
            <div className="designer-assist-block">
              <strong>Rationale</strong>
              <p className="muted" style={{ margin: "0.35rem 0 0" }}>
                {result.rationale}
              </p>
            </div>
          )}

          {result.warnings.length > 0 && (
            <div className="designer-assist-block">
              <strong>Assumptions</strong>
              <ul className="designer-assist-warnings">
                {result.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </div>
          )}

          <div className="designer-assist-block">
            <strong>Proposed agent</strong>
            <p className="muted" style={{ margin: "0.35rem 0 0" }}>
              {result.definition.name} · {result.definition.type} ·{" "}
              {(result.definition.nodes || []).length} nodes ·{" "}
              {(result.definition.edges || []).length} edges
            </p>
            {result.definition.description && (
              <p className="muted" style={{ margin: "0.35rem 0 0" }}>
                {result.definition.description}
              </p>
            )}
          </div>

          {mode === "refine" && diff && (
            <div className="designer-assist-block designer-assist-diff">
              <strong>Change preview</strong>
              {diff.fields_changed.length === 0 &&
              diff.nodes_added.length === 0 &&
              diff.nodes_removed.length === 0 &&
              diff.edges_added.length === 0 &&
              diff.edges_removed.length === 0 ? (
                <p className="muted" style={{ margin: "0.35rem 0 0" }}>
                  No structural changes detected.
                </p>
              ) : (
                <>
                  {diffList("Fields changed", diff.fields_changed)}
                  {diffList("Nodes added", diff.nodes_added)}
                  {diffList("Nodes removed", diff.nodes_removed)}
                  {diffList("Edges added", diff.edges_added)}
                  {diffList("Edges removed", diff.edges_removed)}
                </>
              )}
            </div>
          )}

          {result.test_records.length > 0 && (
            <div className="designer-assist-block">
              <strong>Suggested test records</strong>
              <pre className="designer-assist-code">{JSON.stringify(result.test_records, null, 2)}</pre>
            </div>
          )}

          {!result.validation.valid && (
            <div className="designer-assist-block">
              <strong>Validation issues</strong>
              <ul className="designer-assist-warnings">
                {result.validation.errors.map((issue) => (
                  <li key={issue}>{issue}</li>
                ))}
              </ul>
            </div>
          )}

          <div className="designer-assist-actions">
            <button type="button" disabled={isBusy} onClick={handleAccept}>
              {mode === "create" ? "Accept draft and open editor" : "Apply proposed changes"}
            </button>
            <button
              type="button"
              className="secondary"
              disabled={isBusy}
              onClick={() => setResult(null)}
            >
              Discard preview
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
