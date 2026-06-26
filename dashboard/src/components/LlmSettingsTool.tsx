import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { ReactLlmSettings, ReactLlmSettingsTestResult } from "../api/types";

interface Props {
  settings: ReactLlmSettings | null;
  onSaved?: (settings: ReactLlmSettings) => void;
}

export function LlmSettingsTool({ settings, onSaved }: Props) {
  const [endpointUrl, setEndpointUrl] = useState("");
  const [modelId, setModelId] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [testResult, setTestResult] = useState<ReactLlmSettingsTestResult | null>(null);
  const [testError, setTestError] = useState<string | null>(null);

  useEffect(() => {
    if (!settings) return;
    setEndpointUrl(settings.endpoint_url || settings.env_fallback?.endpoint_url || "");
    setModelId(settings.model_id || settings.env_fallback?.model_id || "");
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
      const body: { endpoint_url: string; model_id: string; api_key?: string } = {
        endpoint_url: endpointUrl.trim(),
        model_id: modelId.trim(),
      };
      if (apiKey.trim()) {
        body.api_key = apiKey.trim();
      }
      const updated = await api.updateReactLlmSettings(body);
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
      const body: { endpoint_url?: string; model_id?: string; api_key?: string } = {};
      if (endpointUrl.trim()) body.endpoint_url = endpointUrl.trim();
      if (modelId.trim()) body.model_id = modelId.trim();
      if (apiKey.trim()) body.api_key = apiKey.trim();
      const result = await api.testReactLlmSettings(body);
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

  const canTest =
    endpointUrl.trim().length > 0 &&
    modelId.trim().length > 0 &&
    (apiKey.trim().length > 0 || Boolean(settings?.api_key_set));

  return (
    <div className="card designer-tool llm-settings-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>LLM connection</h3>
        {settings && (
          <span className={`badge ${settings.configured ? "ok" : "warn"}`}>
            {settings.configured ? "Configured" : "Incomplete"}
          </span>
        )}
      </div>
      <p className="muted">
        Default OpenAI-compatible settings for all <strong>ReAct</strong> agents. Used by the
        agent designer and cluster runs unless overridden per agent.
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
          <span>Model endpoint URL</span>
          <input
            type="url"
            value={endpointUrl}
            onChange={(e) => setEndpointUrl(e.target.value)}
            placeholder="https://your-llm.example/v1"
            required
          />
        </label>

        <label className="designer-field">
          <span>Model ID</span>
          <input
            type="text"
            value={modelId}
            onChange={(e) => setModelId(e.target.value)}
            placeholder="NousResearch/Hermes-3-Llama-3.1-8B"
            required
          />
        </label>

        <label className="designer-field">
          <span>Model API key</span>
          <input
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder={settings?.api_key_set ? "Leave blank to keep current key" : "Bearer / API key"}
            autoComplete="off"
          />
        </label>

        <div className="actions">
          <button type="submit" disabled={saving || testing}>
            {saving ? "Saving…" : "Save LLM settings"}
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
          <p className="muted" style={{ margin: "0.5rem 0 0" }}>
            {testResult.result.input} → {testResult.result.doubled} in {testResult.duration_ms}ms
            {testResult.result.reasoning && (
              <>
                {" "}
                · <em>{testResult.result.reasoning}</em>
              </>
            )}
          </p>
        </div>
      )}
      {testError && <p className="error">{testError}</p>}

      {saved && <p className="badge ok" style={{ marginTop: "0.75rem" }}>Saved</p>}
      {error && <p className="error">{error}</p>}

      <details className="designer-env-hint">
        <summary className="muted">Environment variable fallback</summary>
        <p className="muted">
          If designer settings are unset, the platform reads{" "}
          <code>RATATOSKR_LLM_*</code>, <code>CLOUDERA_AI_BASE_URL</code>,{" "}
          <code>CLOUDERA_MODEL_ID</code>, and <code>CLOUDERA_JWT_TOKEN</code> from{" "}
          <code>.env</code>.
        </p>
      </details>
    </div>
  );
}

interface LoaderProps {
  onLoaded?: (settings: ReactLlmSettings) => void;
}

export function LlmSettingsToolLoader({ onLoaded }: LoaderProps) {
  const [settings, setSettings] = useState<ReactLlmSettings | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .reactLlmSettings()
      .then((s) => {
        setSettings(s);
        onLoaded?.(s);
      })
      .catch((e) => setError(String(e)));
  }, [onLoaded]);

  if (error) return <p className="error">{error}</p>;
  if (!settings) return <p className="muted">Loading LLM settings…</p>;
  return <LlmSettingsTool settings={settings} onSaved={setSettings} />;
}
