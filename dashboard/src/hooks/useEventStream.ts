import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { EventSnapshot, JobSummary, PipelineHealth } from "../api/types";

export function useEventStream(enabled = true) {
  const [health, setHealth] = useState<PipelineHealth | null>(null);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) return;

    const url = api.eventsUrl();
    const es = new EventSource(url);

    es.onopen = () => {
      setConnected(true);
      setError(null);
    };

    es.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data) as EventSnapshot;
        if (data.health) setHealth(data.health);
        if (data.jobs) setJobs(data.jobs);
        if (data.jobs_error) setError(data.jobs_error);
      } catch {
        /* ignore parse errors */
      }
    };

    es.onerror = () => {
      setConnected(false);
      setError("Event stream disconnected");
      es.close();
    };

    return () => es.close();
  }, [enabled]);

  return { health, jobs, connected, error };
}
