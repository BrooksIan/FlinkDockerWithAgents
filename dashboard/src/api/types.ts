/** Hand-maintained types aligned with Control API /v1 (regenerate via npm run generate-api). */

export type HealthStatus = "ok" | "degraded" | "unavailable";

export interface PipelineHealth {
  status: HealthStatus;
  flink: {
    reachable: boolean;
    url: string;
    flink_version?: string;
    taskmanagers?: number;
    slots_total?: number;
    slots_free?: number;
    jobs_running?: number;
    jobs_finished?: number;
    error?: string;
  };
  agents: { ok: boolean; registered: number };
  api_version: string;
}

export interface AgentSummary {
  name: string;
  type: string;
  description: string;
  entry: string;
  runner: string;
  cluster_script: string;
  flink_yaml?: string | null;
}

export interface AgentDetail extends AgentSummary {
  class?: string;
  members?: string[];
  import_note?: string;
  flink_yaml_path?: string | null;
  flink_yaml?: string | null;
}

export interface JobSummary {
  id: string;
  name: string;
  state: string;
  start_time?: number;
  end_time?: number;
}

export interface EventSnapshot {
  type: "snapshot";
  health: PipelineHealth;
  jobs: JobSummary[];
  jobs_error?: string;
}
