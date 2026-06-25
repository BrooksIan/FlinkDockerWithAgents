import type { HealthStatus } from "../api/types";

export function StatusBadge({ status }: { status: HealthStatus | string }) {
  const cls =
    status === "ok" ? "ok" : status === "degraded" ? "warn" : status === "unavailable" ? "bad" : "neutral";
  return <span className={`badge ${cls}`}>{status}</span>;
}

export function JobStateBadge({ state }: { state: string }) {
  const s = state.toUpperCase();
  const cls =
    s === "RUNNING" ? "ok" : s === "FINISHED" ? "neutral" : s === "FAILED" || s === "CANCELED" ? "bad" : "warn";
  return <span className={`badge ${cls}`}>{state}</span>;
}

export function TypeBadge({ type }: { type: string }) {
  return <span className="badge neutral">{type}</span>;
}
