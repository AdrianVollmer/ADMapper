/**
 * Event streaming abstraction for Tauri and HTTP SSE.
 *
 * In Tauri mode, uses Tauri's event system.
 * In HTTP mode, uses Server-Sent Events (SSE).
 */

import { isRunningInTauri } from "./client";
import type { ImportProgress } from "./types";

/** Event handler callback */
export type EventHandler<T> = (data: T) => void;

/** Unsubscribe function returned by subscribe */
export type Unsubscribe = () => void;

/**
 * Subscribe to import progress events.
 * @param jobId - The import job ID
 * @param onProgress - Called when progress updates are received
 * @param onError - Called on error (optional)
 * @returns Unsubscribe function
 */
export function subscribeToImportProgress(
  jobId: string,
  onProgress: EventHandler<ImportProgress>,
  onError?: (error: Event | string) => void
): Unsubscribe {
  if (isRunningInTauri()) {
    return subscribeTauriEvent<ImportProgress>("import-progress", (event) => {
      // Filter by job ID since Tauri events are global
      if (event.job_id === jobId) {
        onProgress(event);
      }
    });
  }

  return subscribeSSE(`/api/import/progress/${jobId}`, onProgress, onError);
}

/**
 * Subscribe to query progress events.
 * @param queryId - The query ID
 * @param onProgress - Called when progress updates are received
 * @param onError - Called on error (optional)
 * @returns Unsubscribe function
 */
export function subscribeToQueryProgress(
  queryId: string,
  onProgress: EventHandler<QueryProgressEvent>,
  onError?: (error: Event | string) => void
): Unsubscribe {
  if (isRunningInTauri()) {
    return subscribeTauriEvent<QueryProgressEvent>("query-progress", (event) => {
      // Filter by query ID since Tauri events are global
      if (event.query_id === queryId) {
        onProgress(event);
      }
    });
  }

  return subscribeSSE(`/api/query/progress/${queryId}`, onProgress, onError);
}

/**
 * Subscribe to query activity events.
 * @param onActivity - Called when activity updates are received
 * @param onError - Called on error (optional)
 * @returns Unsubscribe function
 */
export function subscribeToQueryActivity(
  onActivity: EventHandler<QueryActivityEvent>,
  onError?: (error: Event | string) => void
): Unsubscribe {
  if (isRunningInTauri()) {
    return subscribeTauriEvent<QueryActivityEvent>("query-activity", onActivity);
  }

  return subscribeSSE("/api/query/activity", onActivity, onError);
}

// ============================================================================
// Internal helpers
// ============================================================================

/**
 * Subscribe to a Tauri event.
 */
function subscribeTauriEvent<T>(event: string, handler: EventHandler<T>): Unsubscribe {
  let unlistenFn: (() => void) | null = null;
  let cancelled = false;

  // Start listening (async)
  window
    .__TAURI__!.event.listen<T>(event, (e) => {
      if (!cancelled) {
        handler(e.payload);
      }
    })
    .then((unlisten) => {
      if (cancelled) {
        // Already unsubscribed before listen completed
        unlisten();
      } else {
        unlistenFn = unlisten;
      }
    });

  return () => {
    cancelled = true;
    if (unlistenFn) {
      unlistenFn();
    }
  };
}

/**
 * Subscribe to an SSE endpoint.
 */
function subscribeSSE<T>(
  url: string,
  handler: EventHandler<T>,
  onError?: (error: Event | string) => void
): Unsubscribe {
  const eventSource = new EventSource(url);

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data) as T;
      handler(data);
    } catch {
      // Ignore JSON parse errors
    }
  };

  eventSource.onerror = (event) => {
    if (onError) {
      onError(event);
    }
  };

  return () => {
    eventSource.close();
  };
}

// ============================================================================
// Event types
// ============================================================================

/** Query progress event */
export interface QueryProgressEvent {
  query_id: string;
  status: "running" | "completed" | "failed" | "aborted";
  started_at: number;
  duration_ms?: number;
  result_count?: number;
  error?: string;
  results?: unknown;
  graph?: {
    nodes: unknown[];
    relationships: unknown[];
  };
}

/** Query activity event */
export interface QueryActivityEvent {
  active: number;
}
