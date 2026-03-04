/**
 * Unified Transport Abstraction
 *
 * Provides a consistent interface for both HTTP and Tauri modes:
 * - Request/response operations
 * - Event subscriptions with guaranteed initial state delivery
 * - Automatic reconnection
 */

import { api, isRunningInTauri } from "./client";

// ============================================================================
// Types
// ============================================================================

/** Unsubscribe function returned by subscriptions */
export type Unsubscribe = () => void;

/** Options for event subscriptions */
export interface SubscribeOptions {
  /** Fetch initial state before subscribing to events (default: true) */
  fetchInitial?: boolean;
  /** Auto-reconnect on connection loss (default: true for SSE, N/A for Tauri) */
  autoReconnect?: boolean;
  /** Reconnect delay in ms (default: 5000) */
  reconnectDelay?: number;
}

/** Channel definition for event subscriptions */
export interface ChannelDefinition<T> {
  /** Event name (used for Tauri events) */
  name: string;
  /** URL for SSE subscription (HTTP mode) */
  sseUrl: string | ((params: Record<string, string>) => string);
  /**
   * URL to fetch initial state (JSON endpoint).
   * Optional when sseOnly is true since initial state comes from SSE stream.
   */
  initialStateUrl?: string | ((params: Record<string, string>) => string);
  /** Key in event payload to filter by (e.g., 'job_id', 'query_id') */
  filterKey?: keyof T;
  /**
   * When true, the endpoint is SSE-only and doesn't support JSON initial state fetch.
   * The SSE stream itself delivers initial state on connect.
   * This prevents subscribe() from attempting api.get() which would hang.
   */
  sseOnly?: boolean;
}

// ============================================================================
// Channel Definitions
// ============================================================================

/** Query activity channel - tracks number of active queries */
export interface QueryActivityEvent {
  active: number;
}

export const QUERY_ACTIVITY_CHANNEL: ChannelDefinition<QueryActivityEvent> = {
  name: "query-activity",
  sseUrl: "/api/query/activity",
  sseOnly: true, // SSE stream sends initial state on connect
};

/** Query progress channel - tracks individual query progress */
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

export const QUERY_PROGRESS_CHANNEL: ChannelDefinition<QueryProgressEvent> = {
  name: "query-progress",
  sseUrl: (params) => `/api/query/progress/${params.queryId}`,
  filterKey: "query_id",
  sseOnly: true, // SSE stream sends final state for completed queries
};

/** Import progress channel - tracks BloodHound import progress */
export interface ImportProgressEvent {
  job_id: string;
  status: "running" | "completed" | "failed";
  total_files: number;
  files_processed: number;
  current_file?: string;
  nodes_imported: number;
  edges_imported: number;
  error?: string;
}

export const IMPORT_PROGRESS_CHANNEL: ChannelDefinition<ImportProgressEvent> = {
  name: "import-progress",
  sseUrl: (params) => `/api/import/progress/${params.jobId}`,
  filterKey: "job_id",
  sseOnly: true, // SSE stream sends final state for completed imports
};

// ============================================================================
// Transport Implementation
// ============================================================================

/**
 * Subscribe to an event channel.
 *
 * This function normalizes behavior between HTTP (SSE) and Tauri modes:
 * - Always fetches initial state before subscribing (configurable)
 * - Handles reconnection for SSE
 * - Filters events by params for Tauri (which broadcasts globally)
 *
 * @param channel - Channel definition
 * @param params - Parameters for URL interpolation and event filtering
 * @param handler - Called with each event
 * @param onError - Called on error (optional)
 * @param options - Subscription options
 * @returns Unsubscribe function
 */
export function subscribe<T>(
  channel: ChannelDefinition<T>,
  params: Record<string, string>,
  handler: (data: T) => void,
  onError?: (error: string) => void,
  options: SubscribeOptions = {}
): Unsubscribe {
  // For SSE-only channels, don't attempt initial state fetch (would hang on streaming response)
  const fetchInitialDefault = !channel.sseOnly;
  const { fetchInitial = fetchInitialDefault, autoReconnect = true, reconnectDelay = 5000 } = options;

  let cancelled = false;
  let cleanup: (() => void) | null = null;

  // Start the subscription process
  const start = async () => {
    if (cancelled) return;

    // Fetch initial state if requested and endpoint is available
    if (fetchInitial && channel.initialStateUrl) {
      try {
        const url =
          typeof channel.initialStateUrl === "function" ? channel.initialStateUrl(params) : channel.initialStateUrl;
        const initial = await api.get<T>(url);
        if (!cancelled) {
          handler(initial);
        }
      } catch (err) {
        // Initial fetch failed - not fatal, continue with subscription
        console.warn(`Failed to fetch initial state for ${channel.name}:`, err);
      }
    }

    if (cancelled) return;

    // Subscribe to events
    if (isRunningInTauri()) {
      cleanup = subscribeTauri(channel, params, handler);
    } else {
      cleanup = subscribeSSE(channel, params, handler, onError, autoReconnect, reconnectDelay);
    }
  };

  // Start async without awaiting
  start();

  // Return unsubscribe function
  return () => {
    cancelled = true;
    if (cleanup) {
      cleanup();
    }
  };
}

/**
 * Subscribe to Tauri events.
 */
function subscribeTauri<T>(
  channel: ChannelDefinition<T>,
  params: Record<string, string>,
  handler: (data: T) => void
): Unsubscribe {
  let unlistenFn: (() => void) | null = null;
  let cancelled = false;

  window
    .__TAURI__!.event.listen<T>(channel.name, (e) => {
      if (cancelled) return;

      // Filter by params if filterKey is specified
      if (channel.filterKey) {
        const filterValue = params[channel.filterKey as string];
        const eventValue = e.payload[channel.filterKey];
        if (filterValue && eventValue !== filterValue) {
          return; // Skip events that don't match the filter
        }
      }

      handler(e.payload);
    })
    .then((unlisten) => {
      if (cancelled) {
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
 * Subscribe to SSE events.
 */
function subscribeSSE<T>(
  channel: ChannelDefinition<T>,
  params: Record<string, string>,
  handler: (data: T) => void,
  onError?: (error: string) => void,
  autoReconnect = true,
  reconnectDelay = 5000
): Unsubscribe {
  let cancelled = false;
  let eventSource: EventSource | null = null;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

  const connect = () => {
    if (cancelled) return;

    const url = typeof channel.sseUrl === "function" ? channel.sseUrl(params) : channel.sseUrl;

    eventSource = new EventSource(url);

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as T;
        handler(data);
      } catch {
        // Ignore JSON parse errors
      }
    };

    eventSource.onerror = () => {
      if (cancelled) return;

      eventSource?.close();
      eventSource = null;

      if (onError) {
        onError("Connection lost");
      }

      if (autoReconnect && !cancelled) {
        reconnectTimeout = setTimeout(connect, reconnectDelay);
      }
    };
  };

  connect();

  return () => {
    cancelled = true;
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
    }
    if (eventSource) {
      eventSource.close();
    }
  };
}
