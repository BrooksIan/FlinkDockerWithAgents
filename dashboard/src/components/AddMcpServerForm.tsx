import { useState } from "react";
import { api } from "../api/client";
import type { McpCatalog, McpCatalogServerCreate, McpInstance } from "../api/types";

interface Props {
  onAdded: (catalog: McpCatalog, instances: McpInstance[]) => void;
}

export function AddMcpServerForm({ onAdded }: Props) {
  const [open, setOpen] = useState(false);
  const [displayName, setDisplayName] = useState("");
  const [description, setDescription] = useState("");
  const [transport, setTransport] = useState("http");
  const [toolName, setToolName] = useState("");
  const [toolDescription, setToolDescription] = useState("");
  const [secretName, setSecretName] = useState("");
  const [secretLabel, setSecretLabel] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function resetForm() {
    setDisplayName("");
    setDescription("");
    setTransport("http");
    setToolName("");
    setToolDescription("");
    setSecretName("");
    setSecretLabel("");
    setError(null);
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const body: McpCatalogServerCreate = {
        display_name: displayName.trim(),
        description: description.trim(),
        transport,
        tool_name: toolName.trim(),
        tool_description: toolDescription.trim() || undefined,
      };
      if (secretName.trim()) {
        body.secret_name = secretName.trim();
        body.secret_label = secretLabel.trim() || secretName.trim();
      }
      await api.addMcpCatalogServer(body);
      const [catalog, instancesResponse] = await Promise.all([
        api.mcpCatalog(),
        api.mcpInstances(),
      ]);
      resetForm();
      setOpen(false);
      onAdded(catalog, instancesResponse.instances);
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  }

  if (!open) {
    return (
      <div className="actions" style={{ marginTop: "1rem" }}>
        <button type="button" className="secondary" onClick={() => setOpen(true)}>
          Add MCP server
        </button>
      </div>
    );
  }

  return (
    <div className="card mcp-add-server-form" style={{ marginTop: "1rem" }}>
      <h4 style={{ marginTop: 0 }}>Add MCP server to catalog</h4>
      <p className="muted">
        Custom servers are stored in the platform database and appear under the Custom category.
      </p>
      <form onSubmit={handleSubmit} className="designer-form">
        <label className="designer-field">
          <span>Display name</span>
          <input
            type="text"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder="My Threat Intel API"
            required
          />
        </label>
        <label className="designer-field">
          <span>Description</span>
          <input
            type="text"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="What this MCP server provides"
          />
        </label>
        <label className="designer-field">
          <span>Transport</span>
          <select
            className="studio-select"
            value={transport}
            onChange={(e) => setTransport(e.target.value)}
          >
            <option value="http">HTTP</option>
            <option value="sse">SSE</option>
            <option value="stdio">stdio</option>
          </select>
        </label>
        <label className="designer-field">
          <span>Primary tool name</span>
          <input
            type="text"
            value={toolName}
            onChange={(e) => setToolName(e.target.value)}
            placeholder="lookup"
            required
          />
        </label>
        <label className="designer-field">
          <span>Tool description</span>
          <input
            type="text"
            value={toolDescription}
            onChange={(e) => setToolDescription(e.target.value)}
            placeholder="Describe what the tool does"
          />
        </label>
        <label className="designer-field">
          <span>Secret env var (optional)</span>
          <input
            type="text"
            value={secretName}
            onChange={(e) => setSecretName(e.target.value)}
            placeholder="MY_API_KEY"
          />
        </label>
        <label className="designer-field">
          <span>Secret label (optional)</span>
          <input
            type="text"
            value={secretLabel}
            onChange={(e) => setSecretLabel(e.target.value)}
            placeholder="API key"
          />
        </label>
        <div className="actions">
          <button type="submit" disabled={saving}>
            {saving ? "Adding…" : "Add to catalog"}
          </button>
          <button
            type="button"
            className="secondary"
            disabled={saving}
            onClick={() => {
              setOpen(false);
              resetForm();
            }}
          >
            Cancel
          </button>
        </div>
      </form>
      {error && <p className="error">{error}</p>}
    </div>
  );
}
