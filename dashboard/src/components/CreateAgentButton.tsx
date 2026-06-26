import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import {
  defaultAgentName,
  newAgentPayload,
  type NewAgentType,
} from "../designer/newAgentTemplates";

interface Props {
  onError?: (message: string) => void;
}

export function CreateAgentButton({ onError }: Props) {
  const navigate = useNavigate();
  const [agentType, setAgentType] = useState<NewAgentType>("workflow");
  const [agentName, setAgentName] = useState("");
  const [creating, setCreating] = useState(false);

  const placeholder = defaultAgentName(agentType);
  const trimmedName = agentName.trim();

  async function handleCreate() {
    if (!trimmedName) {
      onError?.("Agent name is required");
      return;
    }
    setCreating(true);
    onError?.("");
    try {
      const created = await api.createAgentDefinition(newAgentPayload(agentType, trimmedName));
      navigate(`/designer/${created.id}`);
    } catch (err) {
      onError?.(String(err));
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="create-agent-bar">
      <label className="create-agent-field">
        <span className="muted">Name</span>
        <input
          className="studio-input create-agent-name-input"
          type="text"
          value={agentName}
          placeholder={placeholder}
          disabled={creating}
          maxLength={120}
          onChange={(e) => setAgentName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void handleCreate();
          }}
        />
      </label>
      <label className="create-agent-field">
        <span className="muted">Agent type</span>
        <select
          className="studio-select"
          value={agentType}
          disabled={creating}
          onChange={(e) => setAgentType(e.target.value as NewAgentType)}
        >
          <option value="workflow">Workflow</option>
          <option value="react">ReAct (LLM)</option>
          <option value="react_skills">ReAct (Flink skills)</option>
        </select>
      </label>
      <button type="button" disabled={creating || !trimmedName} onClick={handleCreate}>
        {creating ? "Creating…" : "Create new agent"}
      </button>
    </div>
  );
}
