import { useEffect, useState } from "react";
import { api } from "../api/client";
import type {
  McpCatalog,
  McpInstance,
  McpInstanceTestResult,
  McpSecretSpec,
} from "../api/types";

interface Props {
  catalog: McpCatalog | null;
  instances: McpInstance[];
  onUpdated?: (instances: McpInstance[]) => void;
}

function secretFields(instance: McpInstance, catalog: McpCatalog | null): McpSecretSpec[] {
  for (const category of catalog?.categories || []) {
    for (const server of category.servers) {
      if (server.id === instance.catalog_id) {
        return server.required_secrets;
      }
    }
  }
  return [];
}

function McpServerCard({
  instance,
  catalog,
  onUpdated,
}: {
  instance: McpInstance;
  catalog: McpCatalog | null;
  onUpdated?: (instances: McpInstance[]) => void;
}) {
  const [enabled, setEnabled] = useState(instance.enabled);
  const [secretInputs, setSecretInputs] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [testResult, setTestResult] = useState<McpInstanceTestResult | null>(null);

  useEffect(() => {
    setEnabled(instance.enabled);
    setSecretInputs({});
  }, [instance]);

  const secrets = secretFields(instance, catalog);

  async function refreshInstances() {
    const response = await api.mcpInstances();
    onUpdated?.(response.instances);
  }

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setSaved(false);
    setTestResult(null);
    try {
      const body: { enabled: boolean; secrets?: Record<string, string> } = { enabled };
      const filled = Object.fromEntries(
        Object.entries(secretInputs).filter(([, value]) => value.trim().length > 0),
      );
      if (Object.keys(filled).length > 0) {
        body.secrets = filled;
      }
      await api.updateMcpInstance(instance.catalog_id, body);
      setSecretInputs({});
      setSaved(true);
      await refreshInstances();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setError(null);
    setTestResult(null);
    setSaved(false);
    try {
      const body: { secrets?: Record<string, string> } = {};
      const filled = Object.fromEntries(
        Object.entries(secretInputs).filter(([, value]) => value.trim().length > 0),
      );
      if (Object.keys(filled).length > 0) {
        body.secrets = filled;
      }
      const result = await api.testMcpInstance(instance.catalog_id, body);
      setTestResult(result);
      if (!result.ok) {
        setError(result.message);
      } else {
        await refreshInstances();
      }
    } catch (err) {
      setError(String(err));
    } finally {
      setTesting(false);
    }
  }

  const canTest =
    enabled &&
    secrets.every((spec) => {
      if (secretInputs[spec.name]?.trim()) return true;
      return Boolean(instance.secrets[spec.name]?.set);
    });

  return (
    <div className="card designer-tool mcp-server-card" style={{ marginTop: "1rem" }}>
      <div className="designer-tool-header">
        <h4 style={{ margin: 0 }}>{instance.display_name}</h4>
        <span className={`badge ${instance.configured ? "ok" : "warn"}`}>
          {instance.configured ? "Ready" : "Not configured"}
        </span>
      </div>
      <p className="muted">{instance.description}</p>
      <p className="muted designer-settings-meta">
        Instance <code>{instance.instance_id}</code>
      </p>

      <form onSubmit={handleSave} className="designer-form">
        <label className="designer-field designer-checkbox-field">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
          />
          <span>Enable for all agents</span>
        </label>

        {secrets.map((spec) => (
          <label key={spec.name} className="designer-field">
            <span>{spec.label}</span>
            <input
              type="password"
              value={secretInputs[spec.name] || ""}
              onChange={(e) =>
                setSecretInputs((prev) => ({ ...prev, [spec.name]: e.target.value }))
              }
              placeholder={
                instance.secrets[spec.name]?.set
                  ? `Stored (${instance.secrets[spec.name]?.hint || "set"})`
                  : spec.name
              }
              autoComplete="off"
            />
          </label>
        ))}

        <div className="actions">
          <button type="submit" disabled={saving || testing}>
            {saving ? "Saving…" : "Save"}
          </button>
          <button
            type="button"
            className="secondary"
            disabled={testing || saving || !canTest}
            onClick={handleTest}
          >
            {testing ? "Testing…" : "Test connection"}
          </button>
        </div>
      </form>

      {testResult?.ok && (
        <div className="llm-test-result ok">
          <p className="badge ok" style={{ margin: 0 }}>
            {testResult.message}
          </p>
          {testResult.tool && (
            <p className="muted" style={{ margin: "0.5rem 0 0" }}>
              Tool <code>{testResult.tool}</code>
              {typeof testResult.result.source === "string" && (
                <>
                  {" "}
                  · source <code>{String(testResult.result.source)}</code>
                </>
              )}
            </p>
          )}
        </div>
      )}
      {saved && <p className="badge ok" style={{ marginTop: "0.75rem" }}>Saved</p>}
      {error && <p className="error">{error}</p>}
    </div>
  );
}

export function McpSettingsTool({ catalog, instances, onUpdated }: Props) {
  return (
    <div className="card designer-tool mcp-settings-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>MCP servers</h3>
      </div>
      <p className="muted">
        Enable catalog MCP servers project-wide. Agents attach enabled instances in the{" "}
        <strong>Designer</strong> inspector.
      </p>

      {instances.length === 0 ? (
        <p className="muted">No MCP servers in catalog.</p>
      ) : (
        instances.map((instance) => (
          <McpServerCard
            key={instance.catalog_id}
            instance={instance}
            catalog={catalog}
            onUpdated={onUpdated}
          />
        ))
      )}

      <details className="designer-env-hint">
        <summary className="muted">Environment variable fallback</summary>
        <p className="muted">
          Secrets can also be supplied via environment variables such as{" "}
          <code>ABUSEIPDB_API_KEY</code> in <code>.env</code>.
        </p>
      </details>
    </div>
  );
}

export function McpSettingsToolLoader() {
  const [catalog, setCatalog] = useState<McpCatalog | null>(null);
  const [instances, setInstances] = useState<McpInstance[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([api.mcpCatalog(), api.mcpInstances()])
      .then(([cat, inst]) => {
        setCatalog(cat);
        setInstances(inst.instances);
      })
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <p className="error">{error}</p>;
  if (!catalog) return <p className="muted">Loading MCP settings…</p>;
  return (
    <McpSettingsTool catalog={catalog} instances={instances} onUpdated={setInstances} />
  );
}
