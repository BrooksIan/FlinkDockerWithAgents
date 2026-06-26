import { useEffect, useState } from "react";

export interface ToastMessage {
  id: number;
  tone: "ok" | "error" | "info";
  text: string;
}

let nextToastId = 0;

export function useToastStack(timeoutMs = 5000) {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  function push(text: string, tone: ToastMessage["tone"] = "info") {
    const id = ++nextToastId;
    setToasts((current) => [...current, { id, tone, text }]);
    return id;
  }

  function dismiss(id: number) {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }

  useEffect(() => {
    if (toasts.length === 0) return;
    const timer = window.setTimeout(() => {
      setToasts((current) => current.slice(1));
    }, timeoutMs);
    return () => window.clearTimeout(timer);
  }, [toasts, timeoutMs]);

  return { toasts, push, dismiss };
}

export function ToastStack({
  toasts,
  onDismiss,
}: {
  toasts: ToastMessage[];
  onDismiss: (id: number) => void;
}) {
  if (toasts.length === 0) return null;
  return (
    <div className="toast-stack" aria-live="polite" aria-atomic="true">
      {toasts.map((toast) => (
        <div key={toast.id} className={`toast toast-${toast.tone}`}>
          <span>{toast.text}</span>
          <button type="button" className="secondary toast-dismiss" onClick={() => onDismiss(toast.id)}>
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
