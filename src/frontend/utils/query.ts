/**
 * Query Execution Utilities
 *
 * Shared logic for executing queries via the async query system.
 * Queries are started via POST, then results are streamed via SSE or Tauri events.
 *
 * Only one non-background query can run at a time. Starting a new foreground
 * query automatically aborts any previous foreground query to prevent
 * multiple graph results being rendered on top of each other.
 */

import { api, ApiClientError } from "../api/client";
import type { QueryStartResponse, QueryProgressEvent, GraphData, QueryResult } from "../api/types";
import { subscribe, QUERY_PROGRESS_CHANNEL } from "../api/transport";

/** Callback for when a foreground query starts */
let onForegroundQueryStart: (() => void) | null = null;

/** Register a callback for foreground query starts (used to reset history cursor) */
export function setForegroundQueryCallback(callback: () => void): void {
  onForegroundQueryStart = callback;
}

/**
 * Tracks the current foreground (non-background) query.
 * Only one foreground query can run at a time - starting a new one aborts the previous.
 */
let currentForegroundQuery: {
  queryId: string;
  abortController: AbortController;
  cleanup: () => void;
} | null = null;

/**
 * Abort the current foreground query if one is running.
 * Called automatically when a new foreground query starts.
 * Also sends abort request to the backend to stop query execution.
 */
export function abortCurrentForegroundQuery(): void {
  if (currentForegroundQuery) {
    const queryId = currentForegroundQuery.queryId;
    console.log(`[query] Aborting previous foreground query: ${queryId}`);

    // Abort the frontend tracking
    currentForegroundQuery.abortController.abort();
    currentForegroundQuery.cleanup();

    // Also tell the backend to abort (fire and forget)
    api.postNoContent(`/api/query/abort/${queryId}`).catch((err) => {
      // Ignore errors - query might have already completed
      console.debug(`[query] Backend abort request failed (probably already complete):`, err);
    });

    currentForegroundQuery = null;
  }
}

/**
 * Register an external query (e.g., from run-query.ts) as the current foreground query.
 * This allows external components to participate in the "one foreground query at a time" system.
 *
 * @param queryId The query ID
 * @param cleanup Cleanup function to call when the query is aborted
 * @returns AbortController that will be triggered if the query should be aborted
 */
export function registerForegroundQuery(queryId: string, cleanup: () => void): AbortController {
  // Abort any existing foreground query
  abortCurrentForegroundQuery();

  const abortController = new AbortController();

  currentForegroundQuery = {
    queryId,
    abortController,
    cleanup,
  };

  return abortController;
}

/**
 * Unregister the current foreground query (called when query completes normally).
 * @param queryId The query ID to unregister (must match current to unregister)
 */
export function unregisterForegroundQuery(queryId: string): void {
  if (currentForegroundQuery?.queryId === queryId) {
    currentForegroundQuery = null;
  }
}

/** Result of executing a query */
export interface QueryExecutionResult {
  /** Number of result rows */
  resultCount: number;
  /** Extracted graph data (if extract_graph was true) */
  graph?: GraphData;
  /** Raw query results (headers and rows) */
  results?: QueryResult;
  /** Query ID */
  queryId: string;
}

/** Options for query execution */
export interface QueryExecutionOptions {
  /** Whether to extract graph data from results (default: true) */
  extractGraph?: boolean;
  /** Mark as background query, excluded from back navigation (default: false) */
  background?: boolean;
}

/** Error thrown when a query is aborted */
export class QueryAbortedError extends Error {
  constructor(queryId: string) {
    super(`Query ${queryId} was aborted`);
    this.name = "QueryAbortedError";
  }
}

/**
 * Execute a query via the query API.
 *
 * For fast queries (<50ms), results are returned inline (sync mode).
 * For slower queries, waits for results via SSE or Tauri events (async mode).
 *
 * For foreground (non-background) queries, only one can run at a time.
 * Starting a new foreground query automatically aborts any previous one.
 *
 * @param query The query string
 * @param options Query execution options
 * @returns Query execution result
 * @throws Error on query failure or timeout
 * @throws QueryAbortedError if the query was aborted by a newer query
 */
export async function executeQuery(query: string, options: QueryExecutionOptions = {}): Promise<QueryExecutionResult> {
  const { extractGraph = true, background = false } = options;

  // For foreground queries, abort any previous foreground query
  if (!background) {
    abortCurrentForegroundQuery();
  }

  // Create abort controller for this query
  const abortController = new AbortController();

  // Start the query
  let startResponse: QueryStartResponse;
  try {
    startResponse = await api.post<QueryStartResponse>(
      "/api/graph/query",
      {
        query,
        extract_graph: extractGraph,
        background,
      },
      abortController.signal
    );
  } catch (err) {
    // If aborted during POST, throw QueryAbortedError
    if (abortController.signal.aborted) {
      throw new QueryAbortedError("unknown");
    }
    throw err;
  }

  // Notify that a foreground query started (resets history cursor)
  if (!background && onForegroundQueryStart) {
    onForegroundQueryStart();
  }

  const queryId = startResponse.query_id;

  // Handle sync mode - results are inline
  if (startResponse.mode === "sync") {
    const result: QueryExecutionResult = {
      resultCount: startResponse.result_count ?? 0,
      queryId,
    };
    if (startResponse.graph) {
      result.graph = startResponse.graph;
    }
    if (startResponse.results) {
      result.results = startResponse.results;
    }
    return result;
  }

  // Handle async mode - wait for results via events
  return new Promise((resolve, reject) => {
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let unsubscribe: (() => void) | null = null;
    let resolved = false;

    // Timeout after 5 minutes
    timeoutId = setTimeout(
      () => {
        cleanup();
        reject(new Error("Query timed out after 5 minutes"));
      },
      5 * 60 * 1000
    );

    const cleanup = () => {
      resolved = true;
      if (unsubscribe) {
        unsubscribe();
        unsubscribe = null;
      }
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      // Clear from current foreground query tracking
      if (!background && currentForegroundQuery?.queryId === queryId) {
        currentForegroundQuery = null;
      }
    };

    // Listen for abort signal
    const onAbort = () => {
      if (!resolved) {
        cleanup();
        reject(new QueryAbortedError(queryId));
      }
    };
    abortController.signal.addEventListener("abort", onAbort);

    // Track this as the current foreground query
    if (!background) {
      currentForegroundQuery = {
        queryId,
        abortController,
        cleanup: () => {
          abortController.signal.removeEventListener("abort", onAbort);
          cleanup();
        },
      };
    }

    unsubscribe = subscribe(
      QUERY_PROGRESS_CHANNEL,
      { queryId, query_id: queryId },
      (progress) => {
        // Ignore events if already resolved or aborted
        if (resolved || abortController.signal.aborted) {
          return;
        }

        const progressEvent = progress as QueryProgressEvent;

        switch (progressEvent.status) {
          case "completed": {
            cleanup();
            abortController.signal.removeEventListener("abort", onAbort);
            const result: QueryExecutionResult = {
              resultCount: progressEvent.result_count ?? 0,
              queryId,
            };
            if (progressEvent.graph) {
              result.graph = progressEvent.graph;
            }
            if (progressEvent.results) {
              result.results = progressEvent.results;
            }
            resolve(result);
            break;
          }

          case "failed":
            cleanup();
            abortController.signal.removeEventListener("abort", onAbort);
            reject(new Error(progressEvent.error ?? "Query failed"));
            break;

          case "aborted":
            cleanup();
            abortController.signal.removeEventListener("abort", onAbort);
            reject(new QueryAbortedError(queryId));
            break;

          // "running" status - just wait for next event
        }
      },
      () => {
        if (!resolved && !abortController.signal.aborted) {
          cleanup();
          abortController.signal.removeEventListener("abort", onAbort);
          reject(new Error("Lost connection to query progress stream"));
        }
      }
    );
  });
}

/**
 * Execute a query. History is managed automatically by the backend.
 *
 * @param name Display name for the query (used for logging)
 * @param query The query string
 * @param options Query execution options (extractGraph, background)
 * @returns Query execution result
 * @throws Error on query failure
 */
export async function executeQueryWithHistory(
  _name: string,
  query: string,
  options: QueryExecutionOptions | boolean = {}
): Promise<QueryExecutionResult> {
  // Support legacy boolean argument for extractGraph
  const opts: QueryExecutionOptions = typeof options === "boolean" ? { extractGraph: options } : options;
  return executeQuery(query, opts);
}

/** Format duration in human readable format */
export function formatDuration(ms: number | null): string {
  if (ms === null) return "-";
  if (ms < 1000) {
    return `${ms}ms`;
  } else if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)}s`;
  } else {
    const minutes = Math.floor(ms / 60000);
    const seconds = Math.floor((ms % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
  }
}

/**
 * Get a user-friendly error message from a query error.
 */
export function getQueryErrorMessage(err: unknown): string {
  if (err instanceof ApiClientError) {
    return err.message;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}
