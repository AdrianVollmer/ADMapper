/**
 * Query Execution Utilities
 *
 * Shared logic for executing queries via the async query system.
 * Queries are started via POST, then results are streamed via SSE.
 */

import { api, ApiClientError } from "../api/client";
import type { QueryStartResponse, QueryProgressEvent, GraphData } from "../api/types";

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
 * Execute a query via the async query API.
 *
 * This starts the query and waits for results via SSE.
 *
 * @param query The query string
 * @param options Query execution options
 * @returns Query execution result
 * @throws Error on query failure or timeout
 */
export async function executeQuery(query: string, options: QueryExecutionOptions = {}): Promise<QueryExecutionResult> {
  const { extractGraph = true, background = false } = options;

  // Start the async query
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

  // Wait for results via SSE
  return new Promise((resolve, reject) => {
    const eventSource = new EventSource(`/api/query/progress/${queryId}`);
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    // Timeout after 5 minutes
    timeoutId = setTimeout(
      () => {
        eventSource.close();
        reject(new Error("Query timed out after 5 minutes"));
      },
      5 * 60 * 1000
    );

    const cleanup = () => {
      eventSource.close();
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    eventSource.onmessage = (event) => {
      try {
        const progress: QueryProgressEvent = JSON.parse(event.data);

        switch (progress.status) {
          case "completed": {
            cleanup();
            const result: QueryExecutionResult = {
              resultCount: progress.result_count ?? 0,
              queryId,
            };
            if (progress.graph) {
              result.graph = progress.graph;
            }
            resolve(result);
            break;
          }

          case "failed":
            cleanup();
            reject(new Error(progress.error ?? "Query failed"));
            break;

          case "aborted":
            cleanup();
            reject(new Error("Query was aborted"));
            break;

          // "running" status - just wait for next event
        }
      } catch (err) {
        console.error("Failed to parse query progress:", err);
        cleanup();
        reject(new Error(`Failed to parse query progress: ${err}`));
      }
    };

    eventSource.onerror = () => {
      cleanup();
      reject(new Error("Lost connection to query progress stream"));
    };
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
