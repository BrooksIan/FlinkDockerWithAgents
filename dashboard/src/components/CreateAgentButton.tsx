import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import { newAgentPayload, type NewAgentType } from "../designer/newAgentTemplates";

interface Props {
  onError?: (message: string) => void;
}

export function CreateAgentButton({ onError }: Props) {
  const navigate = useNavigate();
  const [agentType, setAgentType] = useState<NewAgentType>("workflow");
  const [creating, setCreating] = useState(false);

  async function handleCreate() {
    setCreating(true);
    onError?.("");
    try {
      const created = await api.createAgentDefinition(newAgentPayload(agentType));
      navigate(`/designer/${created.id}`);
    } catch (err) {
      onError?.(String(err));
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="create-agent-bar">
      <label className="create-agent-type">
        <span className="muted">Agent type</span>
        <select
          className="studio-select"
          value={agentType}
          disabled={creating}
          onChange={(e) => setAgentType(e.target.value as NewAgentType)}
        >
          <option value="workflow">Workflow</option>
          <option value="react">ReAct (LLM)</option>
        </select>
      </label>
      <button type="button" disabled={creating} onClick={handleCreate}>
        {creating ? "Creating…" : "Create new agent"}
      </button>
    </div>
  );
}
