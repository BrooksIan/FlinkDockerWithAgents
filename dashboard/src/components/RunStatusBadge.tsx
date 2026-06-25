import { JobStateBadge } from "./StatusBadge";

export function RunStatusBadge({ status }: { status: string }) {
  const s = status.toLowerCase();
  const cls =
    s === "finished" || s === "running"
      ? "ok"
      : s === "failed" || s === "canceled"
        ? "bad"
        : s === "starting"
          ? "warn"
          : "neutral";
  return <span className={`badge ${cls}`}>{status}</span>;
}

export { JobStateBadge };
