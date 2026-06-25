import { useEffect, useState } from "react";
import { api } from "../api/client";

/** Flink Web UI base URL from Control API health (respects FLINK_REST_PORT). */
export function useFlinkUrl(fallback = "http://localhost:8082"): string {
  const [url, setUrl] = useState(fallback);

  useEffect(() => {
    api
      .health()
      .then((h) => {
        if (h.flink?.url) setUrl(h.flink.url);
      })
      .catch(() => {});
  }, []);

  return url;
}
