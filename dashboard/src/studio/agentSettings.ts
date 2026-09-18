/** Per-agent Studio node settings shown in the pipeline inspector. */

export type AgentSettingFieldType =
  | "text"
  | "url"
  | "password"
  | "select"
  | "radio"
  | "checkbox"
  | "kafka_topic";

export interface AgentSettingField {
  key: string;
  label: string;
  type: AgentSettingFieldType;
  required?: boolean;
  placeholder?: string;
  defaultValue?: string | boolean;
  options?: string[];
  /** Optional display labels keyed by option value (radio/select). */
  optionLabels?: Record<string, string>;
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
  workflow_nifi_monitor: {
    title: "NiFi monitor / heal",
    hint: "Poll NiFi health and optionally auto-heal. Phase gates mutations (NIFI_HEAL_PHASE).",
    fields: [
      {
        key: "phase",
        label: "Heal phase",
        type: "radio",
        required: true,
        options: ["monitor", "safe", "lab"],
        defaultValue: "monitor",
        optionLabels: {
          monitor: "Monitor — observe only (no mutations)",
          safe: "Safe — start stopped processors / enable services",
          lab: "Lab — safe + config fix, terminate, optional empty queues",
        },
        help: "Maps to NIFI_HEAL_PHASE for this pipeline node.",
      },
      {
        key: "process_group_id",
        label: "Process group ID",
        type: "text",
        defaultValue: "root",
        placeholder: "root",
        help: "NiFi process group to poll (default: root).",
      },
    ],
  },
};

export function agentSettingsDefinition(agent: string | undefined): AgentSettingsDefinition | null {
  if (!agent) return null;
  return AGENT_SETTINGS[agent] ?? null;
}
