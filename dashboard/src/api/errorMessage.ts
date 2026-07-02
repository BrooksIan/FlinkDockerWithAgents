/** Map raw API errors into user-facing copy for the designer UI. */

const STATUS_HINTS: Record<number, string> = {
  400: "The request was invalid.",
  401: "Authentication failed. Check your API key.",
  403: "You do not have permission to perform this action.",
  404: "The requested resource was not found.",
  422: "Validation failed.",
  503: "The service is temporarily unavailable.",
};

function parseDetail(text: string): string | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (typeof parsed === "string") return parsed;
    if (parsed && typeof parsed === "object" && "detail" in parsed) {
      const detail = (parsed as { detail: unknown }).detail;
      if (typeof detail === "string") return detail;
      if (Array.isArray(detail)) {
        return detail
          .map((item) => {
            if (typeof item === "string") return item;
            if (item && typeof item === "object" && "msg" in item) {
              return String((item as { msg: unknown }).msg);
            }
            return JSON.stringify(item);
          })
          .join("; ");
      }
    }
    return trimmed;
  } catch {
    return trimmed;
  }
}

export function friendlyApiError(error: unknown, context?: string): string {
  const raw = String(error);
  const match = raw.match(/^(\d{3})\s+(\S+):\s*(.*)$/s);
  if (!match) {
    return context ? `${context}: ${raw}` : raw;
  }

  const status = Number(match[1]);
  const detail = parseDetail(match[3]);
  const hint = STATUS_HINTS[status] || `Request failed (${status}).`;
  const message = detail || hint;

  if (context) {
    return `${context}: ${message}`;
  }
  return message;
}
