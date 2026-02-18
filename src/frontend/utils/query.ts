/**
 * Query Execution Utilities
 *
 * Shared logic for executing queries via the async query system.
 * Queries are started via POST, then results are streamed via SSE.
 */

import { api, ApiClientError } from "../api/client";
import type { QueryStartResponse, QueryProgressEvent, GraphData } from "../api/types";

/** Result of executing a query */
export interface QueryExecutionResult {
  /** Number of result rows */
  resultCount: number;
  /** Extracted graph data (if extract_graph was true) */
  graph?: GraphData;
  /** Query ID */
  queryId: string;
}

/**
 * Execute a query via the async query API.
 *
 * This starts the query and waits for results via SSE.
 *
 * @param query The query string
 * @param extractGraph Whether to extract graph data from results
 * @returns Query execution result
 * @throws Error on query failure or timeout
 */
export async function executeQuery(
  query: string,
  extractGraph: boolean = true
): Promise<QueryExecutionResult> {
  // Start the async query
  const startResponse = await api.post<QueryStartResponse>("/api/graph/query", {
    query,
    extract_graph: extractGraph,
  });

  const queryId = startResponse.query_id;

  // Wait for results via SSE
  return new Promise((resolve, reject) => {
    const eventSource = new EventSource(`/api/query/progress/${queryId}`);
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    // Timeout after 5 minutes
    timeoutId = setTimeout(() => {
      eventSource.close();
      reject(new Error("Query timed out after 5 minutes"));
    }, 5 * 60 * 1000);

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
 * @param extractGraph Whether to extract graph data from results
 * @returns Query execution result
 * @throws Error on query failure
 */
export async function executeQueryWithHistory(
  _name: string,
  query: string,
  extractGraph: boolean = true
): Promise<QueryExecutionResult> {
  return executeQuery(query, extractGraph);
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
