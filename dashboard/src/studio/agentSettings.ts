/** Per-agent Studio node settings shown in the pipeline inspector. */

export type AgentSettingFieldType = "text" | "url" | "password" | "select" | "checkbox" | "kafka_topic";

export interface AgentSettingField {
  key: string;
  label: string;
  type: AgentSettingFieldType;
  required?: boolean;
  placeholder?: string;
  defaultValue?: string | boolean;
  options?: string[];
  help?: string;
}

export interface AgentSettingsDefinition {
  title?: string;
  hint?: string;
  settingsLink?: { label: string; path: string };
  fields: AgentSettingField[];
}

export const AGENT_SETTINGS: Record<string, AgentSettingsDefinition> = {
  workflow_api_fetch: {
    title: "API fetch",
    hint: "Each upstream event triggers one HTTP request. Platform defaults from Settings apply when fields are left blank.",
    settingsLink: { label: "Platform API fetch defaults", path: "/settings" },
    fields: [
      {
        key: "endpoint_url",
        label: "API URL",
        type: "url",
        placeholder: "https://api.example.com/v1/data",
        help: "Overrides the platform default endpoint for this pipeline node.",
      },
      {
        key: "http_method",
        label: "HTTP method",
        type: "select",
        options: ["GET", "POST"],
        defaultValue: "GET",
      },
      {
        key: "api_key",
        label: "API key",
        type: "password",
        placeholder: "Optional bearer / API key",
      },
      {
        key: "path_suffix",
        label: "Path suffix",
        type: "text",
        placeholder: "items/42",
        help: "Appended to the base URL for every poll.",
      },
      {
        key: "expand_records",
        label: "Expand list responses",
        type: "checkbox",
        defaultValue: false,
        help: "Emit one output event per normalized API record.",
      },
    ],
  },
  readapi_reactthoughts_writekafka: {
    title: "Read API → ReAct → Kafka",
    hint: "Fetches JSON from the API URL, asks the configured ReAct LLM for thoughts, and publishes results to Kafka.",
    settingsLink: { label: "ReAct LLM settings", path: "/settings" },
    fields: [
      {
        key: "endpoint_url",
        label: "API URL",
        type: "url",
        required: true,
        placeholder: "https://api.example.com/v1/posts",
      },
      {
        key: "http_method",
        label: "HTTP method",
        type: "select",
        options: ["GET", "POST"],
        defaultValue: "GET",
      },
      {
        key: "api_key",
        label: "API key",
        type: "password",
        placeholder: "Optional bearer / API key",
      },
      {
        key: "path_suffix",
        label: "Path suffix",
        type: "text",
        placeholder: "posts/1",
      },
      {
        key: "kafka_topic",
        label: "Kafka output topic",
        type: "kafka_topic",
        required: true,
        defaultValue: "workflow.test.output",
      },
      {
        key: "kafka_bootstrap",
        label: "Kafka bootstrap (optional)",
        type: "text",
        placeholder: "localhost:9093",
      },
    ],
  },
};

export function agentSettingsDefinition(agent: string | undefined): AgentSettingsDefinition | null {
  if (!agent) return null;
  return AGENT_SETTINGS[agent] ?? null;
}
