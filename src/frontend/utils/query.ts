/**
 * Query Execution Utilities
 *
 * Shared logic for executing queries via the async query system.
 * Queries are started via POST, then results are streamed via SSE or Tauri events.
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

/**
 * Execute a query via the query API.
 *
 * For fast queries (<50ms), results are returned inline (sync mode).
 * For slower queries, waits for results via SSE or Tauri events (async mode).
 *
 * @param query The query string
 * @param options Query execution options
 * @returns Query execution result
 * @throws Error on query failure or timeout
 */
export async function executeQuery(query: string, options: QueryExecutionOptions = {}): Promise<QueryExecutionResult> {
  const { extractGraph = true, background = false } = options;

  // Start the query
  const startResponse = await api.post<QueryStartResponse>("/api/graph/query", {
    query,
    extract_graph: extractGraph,
    background,
  });

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

    // Timeout after 5 minutes
    timeoutId = setTimeout(
      () => {
        cleanup();
        reject(new Error("Query timed out after 5 minutes"));
      },
      5 * 60 * 1000
    );

    const cleanup = () => {
      if (unsubscribe) {
        unsubscribe();
        unsubscribe = null;
      }
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    unsubscribe = subscribe(
      QUERY_PROGRESS_CHANNEL,
      { queryId },
      (progress) => {
        const progressEvent = progress as QueryProgressEvent;

        switch (progressEvent.status) {
          case "completed": {
            cleanup();
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
            reject(new Error(progressEvent.error ?? "Query failed"));
            break;

          case "aborted":
            cleanup();
            reject(new Error("Query was aborted"));
            break;

          // "running" status - just wait for next event
        }
      },
      () => {
        cleanup();
        reject(new Error("Lost connection to query progress stream"));
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
