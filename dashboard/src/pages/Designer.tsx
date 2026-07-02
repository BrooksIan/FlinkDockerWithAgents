import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../api/client";
import type { AgentCatalog, AgentDefinition, AgentDefinitionAssistResult, ReactLlmSettings } from "../api/types";
import { AgentDefinitionList } from "../components/AgentDefinitionList";
import { CreateAgentButton } from "../components/CreateAgentButton";
import { DesignerDefinitionSummary } from "../components/DesignerDefinitionSummary";
import { DesignerAssistPanel } from "../designer/DesignerAssistPanel";

export function DesignerPage() {
  const navigate = useNavigate();
  const [catalog, setCatalog] = useState<AgentCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [inspected, setInspected] = useState<AgentDefinition | null>(null);
  const [llmSettings, setLlmSettings] = useState<ReactLlmSettings | null>(null);
  const [creatingFromAssist, setCreatingFromAssist] = useState(false);

  useEffect(() => {
    api
      .agentCatalog()
      .then(setCatalog)
      .catch((e) => setError(String(e)));
    api.reactLlmSettings().then(setLlmSettings).catch(() => setLlmSettings(null));
  }, []);

  async function handleAcceptAssistDraft(result: AgentDefinitionAssistResult) {
    setCreatingFromAssist(true);
    setError(null);
    try {
      const created = await api.createAgentDefinition(result.definition);
      navigate(`/designer/${created.id}`, {
        state: { suggestedTestRecords: result.test_records },
      });
    } catch (err) {
      setError(String(err));
      throw err;
    } finally {
      setCreatingFromAssist(false);
    }
  }

  return (
    <>
      <h2>Agent Designer</h2>
      <div className="designer-page-header">
        <p className="muted" style={{ margin: 0 }}>
          Compose workflow and ReAct agents visually. Open an agent in the editor to validate,
          compile, publish, and test. Configure LLM defaults and MCP servers in{" "}
          <Link to="/settings">Settings</Link>.
        </p>
        <CreateAgentButton onError={setError} />
      </div>
      {error && <p className="error">{error}</p>}

      <DesignerAssistPanel
        mode="create"
        llmSettings={llmSettings}
        busy={creatingFromAssist}
        onError={setError}
        onAccept={handleAcceptAssistDraft}
      />

      <div className="designer-layout designer-layout-two">
        <section className="designer-section">
          <h3>Your agents</h3>
          <AgentDefinitionList onSelect={setInspected} />
        </section>

        <section className="designer-section">
          {inspected ? (
            <DesignerDefinitionSummary definition={inspected} />
          ) : (
            <>
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
            </>
          )}
        </section>
      </div>
    </>
  );
}
