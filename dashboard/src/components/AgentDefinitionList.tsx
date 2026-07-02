import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { AgentDefinition } from "../api/types";

interface Props {
  onSelect?: (definition: AgentDefinition) => void;
}

export function AgentDefinitionList({ onSelect }: Props) {
  const [definitions, setDefinitions] = useState<AgentDefinition[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  function reload() {
    setLoading(true);
    api
      .agentDefinitions()
      .then(setDefinitions)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    reload();
  }, []);

  async function handleDelete(id: string, name: string) {
    if (!window.confirm(`Delete agent definition "${name}"? This cannot be undone.`)) return;
    setDeletingId(id);
    setError(null);
    try {
      await api.deleteAgentDefinition(id);
      setDefinitions((current) => current.filter((item) => item.id !== id));
    } catch (err) {
      setError(String(err));
    } finally {
      setDeletingId(null);
    }
  }

  function statusBadgeClass(status: string): string {
    if (status === "published") return "ok";
    if (status === "compiled") return "ok";
    return "warn";
  }

  return (
    <div className="card designer-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>Agent definitions</h3>
        <span className="badge">{definitions.length}</span>
      </div>
      <p className="muted">
        Visual agent logic designs stored in the platform. Open an agent in the editor to validate,
        compile, publish, and test.
      </p>

      {error && <p className="error">{error}</p>}

      {loading ? (
        <p className="muted">Loading definitions…</p>
      ) : definitions.length === 0 ? (
        <p className="muted">No agent definitions yet. Create one to get started.</p>
      ) : (
        <ul className="designer-definition-list">
          {definitions.map((def) => (
            <li key={def.id} className="designer-definition-item">
              <div className="designer-definition-main">
                <strong>{def.name}</strong>
                <span className="muted"> · {def.type}</span>
                <span className={`badge ${statusBadgeClass(def.status)}`}>
                  {def.status}
                </span>
              </div>
              <p className="muted designer-definition-desc">{def.description}</p>
              <div className="designer-definition-meta muted">
                <code>{def.id}</code>
                {def.manifest_name && (
                  <>
                    {" "}
                    · manifest: <code>{def.manifest_name}</code>
                  </>
                )}
                {" "}
                · {def.nodes?.length ?? 0} nodes, {def.edges?.length ?? 0} edges
              </div>
              <div className="actions" style={{ margin: "0.5rem 0 0" }}>
                <Link to={`/designer/${def.id}`} className="secondary-link">
                  Open editor
                </Link>
                <button type="button" className="secondary" onClick={() => onSelect?.(def)}>
                  Summary
                </button>
                {def.manifest_name && (
                  <Link to={`/agents/${def.manifest_name}`} className="secondary-link">
                    View runtime agent
                  </Link>
                )}
                <button
                  type="button"
                  className="secondary"
                  disabled={deletingId === def.id}
                  onClick={() => handleDelete(def.id, def.name)}
                >
                  {deletingId === def.id ? "Deleting…" : "Delete"}
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
