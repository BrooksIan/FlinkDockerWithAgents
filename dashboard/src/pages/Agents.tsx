import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { AgentCatalog, CatalogAgentEntry } from "../api/types";
import { TypeBadge } from "../components/StatusBadge";

function AgentCard({ agent }: { agent: CatalogAgentEntry }) {
  return (
    <div className="catalog-agent-card">
      <div className="catalog-agent-card-header">
        <Link to={`/agents/${agent.manifest}`}>{agent.display_name}</Link>
        <TypeBadge type={agent.type} />
      </div>
      <p className="muted catalog-agent-manifest">
        <code>{agent.manifest}</code>
      </p>
      <p>{agent.description}</p>
      {agent.tags.length > 0 && (
        <div className="catalog-tags">
          {agent.tags.map((tag) => (
            <span key={tag} className="catalog-tag">
              {tag}
            </span>
          ))}
        </div>
      )}
      {agent.input_schema?.properties ? (
        <p className="muted catalog-schema-hint">
          Input:{" "}
          {Object.keys(agent.input_schema.properties as Record<string, unknown>).join(", ")}
        </p>
      ) : null}
    </div>
  );
}

export function AgentsPage() {
  const [catalog, setCatalog] = useState<AgentCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .agentCatalog()
      .then(setCatalog)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  return (
    <>
      <h2>Agent Catalog</h2>
      <p className="muted">
        Browse registered agents by category. Runtime definitions live in{" "}
        <code>examples/agents/agent-manifest.yaml</code>; display metadata in{" "}
        <code>agent-catalog.yaml</code>.
      </p>
      {loading && <p className="muted">Loading…</p>}
      {error && <p className="error">{error}</p>}
      {!loading && !error && catalog && (
        <div className="catalog">
          {catalog.categories.map((category) => (
            <section key={category.id} className="catalog-category card">
              <h3>{category.label}</h3>
              {category.description && <p className="muted">{category.description}</p>}
              {category.subcategories.map((sub) => (
                <div key={sub.id} className="catalog-subcategory">
                  <h4>{sub.label}</h4>
                  {sub.description && (
                    <p className="muted catalog-subcategory-desc">{sub.description}</p>
                  )}
                  <div className="catalog-agent-grid">
                    {sub.agents.map((agent) => (
                      <AgentCard key={agent.id} agent={agent} />
                    ))}
                  </div>
                </div>
              ))}
            </section>
          ))}
        </div>
      )}
    </>
  );
}
