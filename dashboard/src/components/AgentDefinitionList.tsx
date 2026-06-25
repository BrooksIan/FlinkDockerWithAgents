import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type {
  AgentDefinition,
  AgentDefinitionCompileResult,
  AgentDefinitionValidation,
} from "../api/types";

interface Props {
  onSelect?: (definition: AgentDefinition) => void;
  onCompiled?: (result: AgentDefinitionCompileResult) => void;
}

export function AgentDefinitionList({ onSelect, onCompiled }: Props) {
  const [definitions, setDefinitions] = useState<AgentDefinition[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [validatingId, setValidatingId] = useState<string | null>(null);
  const [compilingId, setCompilingId] = useState<string | null>(null);
  const [publishingId, setPublishingId] = useState<string | null>(null);
  const [validation, setValidation] = useState<AgentDefinitionValidation | null>(null);

  useEffect(() => {
    api
      .agentDefinitions()
      .then(setDefinitions)
      .catch((e) => setError(String(e)));
  }, []);

  async function handleValidate(id: string) {
    setValidatingId(id);
    setValidation(null);
    setError(null);
    try {
      const result = await api.validateAgentDefinition(id);
      setValidation(result);
    } catch (err) {
      setError(String(err));
    } finally {
      setValidatingId(null);
    }
  }

  async function handleCompile(id: string) {
    setCompilingId(id);
    setError(null);
    try {
      const result = await api.compileAgentDefinition(id);
      if (result.definition) {
        setDefinitions((current) =>
          current.map((item) => (item.id === id ? result.definition! : item)),
        );
      }
      onCompiled?.(result);
    } catch (err) {
      setError(String(err));
    } finally {
      setCompilingId(null);
    }
  }

  async function handlePublish(id: string) {
    setPublishingId(id);
    setError(null);
    try {
      const result = await api.publishAgentDefinition(id);
      if (result.definition) {
        setDefinitions((current) =>
          current.map((item) => (item.id === id ? result.definition! : item)),
        );
      }
    } catch (err) {
      setError(String(err));
    } finally {
      setPublishingId(null);
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
        Visual agent logic designs stored in the platform. Templates seed automatically on first
        load.
      </p>

      {error && <p className="error">{error}</p>}

      {definitions.length === 0 ? (
        <p className="muted">Loading definitions…</p>
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
                  Edit canvas
                </Link>
                <button
                  type="button"
                  className="secondary"
                  disabled={validatingId === def.id || compilingId === def.id}
                  onClick={() => handleValidate(def.id)}
                >
                  {validatingId === def.id ? "Validating…" : "Validate"}
                </button>
                <button
                  type="button"
                  disabled={validatingId === def.id || compilingId === def.id || publishingId === def.id}
                  onClick={() => handleCompile(def.id)}
                >
                  {compilingId === def.id ? "Compiling…" : "Compile"}
                </button>
                <button
                  type="button"
                  className="secondary"
                  disabled={validatingId === def.id || compilingId === def.id || publishingId === def.id}
                  onClick={() => handlePublish(def.id)}
                >
                  {publishingId === def.id ? "Publishing…" : "Add to catalog"}
                </button>
                {def.manifest_name && (
                  <Link to={`/agents/${def.manifest_name}`} className="secondary-link">
                    View runtime agent
                  </Link>
                )}
                <button
                  type="button"
                  className="secondary"
                  onClick={() => onSelect?.(def)}
                >
                  Inspect JSON
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}

      {validation && (
        <div className={`llm-test-result ${validation.valid ? "ok" : ""}`} style={{ marginTop: "1rem" }}>
          <p className={`badge ${validation.valid ? "ok" : "warn"}`} style={{ margin: 0 }}>
            {validation.valid ? "Graph is valid" : "Validation failed"}
          </p>
          {validation.errors.length > 0 && (
            <ul className="designer-validation-list">
              {validation.errors.map((msg) => (
                <li key={msg} className="error">
                  {msg}
                </li>
              ))}
            </ul>
          )}
          {validation.warnings.length > 0 && (
            <ul className="designer-validation-list">
              {validation.warnings.map((msg) => (
                <li key={msg} className="muted">
                  {msg}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
