import { Link } from "react-router-dom";
import type { KafkaTopicSummary } from "../api/types";
import { agentSettingsDefinition, type AgentSettingField } from "./agentSettings";

interface Props {
  agent: string;
  config: Record<string, unknown>;
  kafkaTopics: KafkaTopicSummary[];
  onUpdate: (config: Record<string, unknown>) => void;
}

function fieldValue(config: Record<string, unknown>, field: AgentSettingField): string | boolean {
  const raw = config[field.key];
  if (field.type === "checkbox") {
    return Boolean(raw ?? field.defaultValue ?? false);
  }
  if (raw === undefined || raw === null) {
    return String(field.defaultValue ?? "");
  }
  return String(raw);
}

export function AgentNodeSettingsForm({ agent, config, kafkaTopics, onUpdate }: Props) {
  const definition = agentSettingsDefinition(agent);
  if (!definition) return null;

  function patch(key: string, value: unknown) {
    const next = { ...config };
    if (value === "" || value === undefined) {
      delete next[key];
    } else {
      next[key] = value;
    }
    onUpdate(next);
  }

  return (
    <div className="agent-node-settings">
      {definition.title && <h4 style={{ margin: "1rem 0 0.5rem" }}>{definition.title}</h4>}
      {definition.hint && (
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          {definition.hint}
        </p>
      )}
      {definition.settingsLink && (
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          <Link to={definition.settingsLink.path}>{definition.settingsLink.label}</Link>
        </p>
      )}

      {definition.fields.map((field) => {
        const value = fieldValue(config, field);
        const id = `${agent}-${field.key}`;

        if (field.type === "select") {
          return (
            <div key={field.key}>
              <label className="studio-label" htmlFor={id}>
                {field.label}
                {field.required ? " *" : ""}
              </label>
              <select
                id={id}
                className="studio-select"
                value={String(value || field.defaultValue || field.options?.[0] || "")}
                onChange={(e) => patch(field.key, e.target.value)}
              >
                {(field.options || []).map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
              {field.help && (
                <p className="muted" style={{ fontSize: "0.8rem" }}>
                  {field.help}
                </p>
              )}
            </div>
          );
        }

        if (field.type === "checkbox") {
          return (
            <div key={field.key} style={{ marginTop: "0.75rem" }}>
              <label className="studio-label" style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
                <input
                  id={id}
                  type="checkbox"
                  checked={Boolean(value)}
                  onChange={(e) => patch(field.key, e.target.checked)}
                />
                {field.label}
              </label>
              {field.help && (
                <p className="muted" style={{ fontSize: "0.8rem" }}>
                  {field.help}
                </p>
              )}
            </div>
          );
        }

        if (field.type === "kafka_topic") {
          const topic = String(value || "");
          return (
            <div key={field.key}>
              <label className="studio-label" htmlFor={id}>
                {field.label}
                {field.required ? " *" : ""}
              </label>
              <select
                id={id}
                className="studio-select"
                value={topic}
                onChange={(e) => patch(field.key, e.target.value)}
              >
                <option value="">Select topic…</option>
                {kafkaTopics.map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
                {topic && !kafkaTopics.some((t) => t.name === topic) && (
                  <option value={topic}>{topic}</option>
                )}
              </select>
              {field.help && (
                <p className="muted" style={{ fontSize: "0.8rem" }}>
                  {field.help}
                </p>
              )}
            </div>
          );
        }

        return (
          <div key={field.key}>
            <label className="studio-label" htmlFor={id}>
              {field.label}
              {field.required ? " *" : ""}
            </label>
            <input
              id={id}
              className="studio-input"
              type={field.type === "password" ? "password" : field.type === "url" ? "url" : "text"}
              placeholder={field.placeholder}
              defaultValue={String(value)}
              key={`${agent}-${field.key}-${String(config[field.key] ?? "")}`}
              onBlur={(e) => patch(field.key, e.target.value.trim())}
            />
            {field.help && (
              <p className="muted" style={{ fontSize: "0.8rem" }}>
                {field.help}
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}
