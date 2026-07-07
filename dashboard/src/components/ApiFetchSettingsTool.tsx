import { useEffect, useState } from "react";
import { api } from "../api/client";
import type {
  ApiFetchSettings,
  ApiFetchSettingsTestResult,
} from "../api/types";

interface Props {
  settings: ApiFetchSettings | null;
  onSaved?: (settings: ApiFetchSettings) => void;
}

export function ApiFetchSettingsTool({ settings, onSaved }: Props) {
  const [endpointUrl, setEndpointUrl] = useState("");
  const [httpMethod, setHttpMethod] = useState<"GET" | "POST">("GET");
  const [authHeader, setAuthHeader] = useState("Authorization");
  const [authPrefix, setAuthPrefix] = useState("Bearer");
  const [timeoutSeconds, setTimeoutSeconds] = useState(15);
  const [apiKey, setApiKey] = useState("");
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [testResult, setTestResult] = useState<ApiFetchSettingsTestResult | null>(null);
  const [testError, setTestError] = useState<string | null>(null);

  useEffect(() => {
    if (!settings) return;
    setEndpointUrl(settings.endpoint_url || settings.env_fallback?.endpoint_url || "");
    setHttpMethod(settings.http_method === "POST" ? "POST" : "GET");
    setAuthHeader(settings.auth_header || "Authorization");
    setAuthPrefix(settings.auth_prefix ?? "Bearer");
    setTimeoutSeconds(settings.timeout_seconds || 15);
    setApiKey("");
  }, [settings]);

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setSaved(false);
    setTestResult(null);
    setTestError(null);
    try {
      const body: {
        endpoint_url: string;
        http_method: string;
        auth_header: string;
        auth_prefix: string;
        timeout_seconds: number;
        api_key?: string;
      } = {
        endpoint_url: endpointUrl.trim(),
        http_method: httpMethod,
        auth_header: authHeader.trim() || "Authorization",
        auth_prefix: authPrefix.trim(),
        timeout_seconds: timeoutSeconds,
      };
      if (apiKey.trim()) {
        body.api_key = apiKey.trim();
      }
      const updated = await api.updateApiFetchSettings(body);
      setApiKey("");
      setSaved(true);
      onSaved?.(updated);
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setTestError(null);
    setTestResult(null);
    setSaved(false);
    try {
      const body: Record<string, string | number> = {};
      if (endpointUrl.trim()) body.endpoint_url = endpointUrl.trim();
      if (httpMethod) body.http_method = httpMethod;
      if (authHeader.trim()) body.auth_header = authHeader.trim();
      body.auth_prefix = authPrefix.trim();
      body.timeout_seconds = timeoutSeconds;
      if (apiKey.trim()) body.api_key = apiKey.trim();
      const result = await api.testApiFetchSettings(body);
      setTestResult(result);
      if (!result.ok) {
        setTestError(result.message);
      }
    } catch (err) {
      setTestError(String(err));
    } finally {
      setTesting(false);
    }
  }

  const canTest = endpointUrl.trim().length > 0;

  return (
    <div className="card designer-tool llm-settings-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>API fetch (workflow agent)</h3>
        {settings && (
          <span className={`badge ${settings.configured ? "ok" : "warn"}`}>
            {settings.configured ? "Configured" : "Incomplete"}
          </span>
        )}
      </div>
      <p className="muted">
        Default HTTP endpoint for the <strong>API Fetch</strong> workflow agent (
        <code>workflow_api_fetch</code>). Each upstream event triggers one poll; responses
        are normalized into structured <code>records</code> for downstream agents.
      </p>

      {settings && (
        <p className="muted designer-settings-meta">
          Source: <code>{settings.source}</code>
          {settings.api_key_set && settings.api_key_hint && (
            <>
              {" "}
              · API key: <code>{settings.api_key_hint}</code>
            </>
          )}
        </p>
      )}

      <form onSubmit={handleSave} className="designer-form">
        <label className="designer-field">
          <span>Endpoint URL</span>
          <input
            type="url"
            value={endpointUrl}
            onChange={(e) => setEndpointUrl(e.target.value)}
            placeholder="https://jsonplaceholder.typicode.com/"
            required
          />
        </label>

        <label className="designer-field">
          <span>HTTP method</span>
          <select value={httpMethod} onChange={(e) => setHttpMethod(e.target.value as "GET" | "POST")}>
            <option value="GET">GET</option>
            <option value="POST">POST</option>
          </select>
        </label>

        <label className="designer-field">
          <span>Auth header name</span>
          <input
            type="text"
            value={authHeader}
            onChange={(e) => setAuthHeader(e.target.value)}
            placeholder="Authorization"
          />
        </label>

        <label className="designer-field">
          <span>Auth prefix (optional)</span>
          <input
            type="text"
            value={authPrefix}
            onChange={(e) => setAuthPrefix(e.target.value)}
            placeholder="Bearer"
          />
        </label>

        <label className="designer-field">
          <span>API key</span>
          <input
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder={settings?.api_key_set ? "Leave blank to keep current key" : "Optional bearer / API key"}
            autoComplete="off"
          />
        </label>

        <label className="designer-field">
          <span>Timeout (seconds)</span>
          <input
            type="number"
            min={1}
            max={120}
            value={timeoutSeconds}
            onChange={(e) => setTimeoutSeconds(Number(e.target.value) || 15)}
          />
        </label>

        <div className="actions">
          <button type="submit" disabled={saving || testing}>
            {saving ? "Saving…" : "Save API fetch settings"}
          </button>
          <button
            type="button"
            className="secondary"
            disabled={testing || saving || !canTest}
            onClick={handleTest}
          >
            {testing ? "Testing…" : "Test fetch"}
          </button>
        </div>
      </form>

      {testResult?.ok && (
        <div className="llm-test-result ok">
          <p className="badge ok" style={{ margin: 0 }}>
            {testResult.message}
          </p>
          {testResult.preview_keys && testResult.preview_keys.length > 0 && (
            <p className="muted" style={{ margin: "0.5rem 0 0" }}>
              Keys: {testResult.preview_keys.join(", ")} · {testResult.duration_ms}ms
            </p>
          )}
        </div>
      )}
      {testError && <p className="error">{testError}</p>}

      {saved && <p className="badge ok" style={{ marginTop: "0.75rem" }}>Saved</p>}
      {error && <p className="error">{error}</p>}

      <details className="designer-env-hint">
        <summary className="muted">Environment variable fallback</summary>
        <p className="muted">
          If designer settings are unset, the platform reads{" "}
          <code>RATATOSKR_API_FETCH_ENDPOINT_URL</code>, <code>RATATOSKR_API_FETCH_HTTP_METHOD</code>, and{" "}
          <code>RATATOSKR_API_FETCH_API_KEY</code> from <code>.env</code>.
        </p>
      </details>
    </div>
  );
}

export function ApiFetchSettingsToolLoader() {
  const [settings, setSettings] = useState<ApiFetchSettings | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .apiFetchSettings()
      .then(setSettings)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <p className="error">{error}</p>;
  if (!settings) return <p className="muted">Loading API fetch settings…</p>;
  return <ApiFetchSettingsTool settings={settings} onSaved={setSettings} />;
}
