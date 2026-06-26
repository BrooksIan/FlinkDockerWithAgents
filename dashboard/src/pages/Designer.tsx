import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { AgentCatalog, AgentDefinition, AgentDefinitionCompileResult } from "../api/types";
import { AgentDefinitionList } from "../components/AgentDefinitionList";
import { CompilePreviewPanel } from "../components/CompilePreviewPanel";
import { CreateAgentButton } from "../components/CreateAgentButton";

export function DesignerPage() {
  const [catalog, setCatalog] = useState<AgentCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [inspected, setInspected] = useState<AgentDefinition | null>(null);
  const [compileResult, setCompileResult] = useState<AgentDefinitionCompileResult | null>(null);

  useEffect(() => {
    api
      .agentCatalog()
      .then(setCatalog)
      .catch((e) => setError(String(e)));
  }, []);

  return (
    <>
      <h2>Agent Designer</h2>
      <div className="designer-page-header">
        <p className="muted" style={{ margin: 0 }}>
          Compose workflow and ReAct agents visually. ReAct agents require an LLM — configure
          defaults in <Link to="/settings">Settings</Link>. Attach MCP servers there too; enabled
          instances appear in the canvas palette and inspector.
        </p>
        <CreateAgentButton onError={setError} />
      </div>
      {error && <p className="error">{error}</p>}

      <div className="designer-layout designer-layout-two">
        <section className="designer-section">
          <h3>Your agents</h3>
          <AgentDefinitionList onSelect={setInspected} onCompiled={setCompileResult} />
          <div style={{ marginTop: "1rem" }}>
            <CompilePreviewPanel result={compileResult} />
          </div>
        </section>

        <section className="designer-section">
          <h3>Catalog preview</h3>
          <div className="card">
            {!catalog ? (
              <p className="muted">Loading catalog…</p>
            ) : catalog.categories.length === 0 ? (
              <p className="muted">No catalog categories yet.</p>
            ) : (
              catalog.categories.map((category) => (
                <div key={category.id} className="designer-catalog-category">
                  <p style={{ margin: 0 }}>
                    <strong>{category.label}</strong>
                    {category.llm_required && (
                      <span className="badge warn" style={{ marginLeft: "0.5rem" }}>
                        LLM required
                      </span>
                    )}
                  </p>
                  <p className="muted">{category.description}</p>
                  <ul className="designer-catalog-list">
                    {category.subcategories.flatMap((sub) =>
                      sub.agents.map((agent) => (
                        <li key={agent.id}>
                          <Link to={`/agents/${agent.manifest}`}>{agent.display_name}</Link>
                          <span className="muted"> — {agent.description}</span>
                        </li>
                      )),
                    )}
                  </ul>
                </div>
              ))
            )}
          </div>
        </section>
      </div>

      {inspected && (
        <section className="designer-section" style={{ marginTop: "1.5rem" }}>
          <h3>Definition JSON — {inspected.name}</h3>
          <pre className="card designer-json-preview">{JSON.stringify(inspected, null, 2)}</pre>
        </section>
      )}
    </>
  );
}
