/**
 * Query Execution Utilities
 *
 * Shared logic for executing CozoDB queries.
 */

import { api, ApiClientError } from "../api/client";
import type { QueryResponse, GraphData } from "../api/types";

/** Result of executing a query */
export interface QueryExecutionResult {
  /** Number of result rows */
  resultCount: number;
  /** Extracted graph data (if extract_graph was true) */
  graph?: GraphData;
  /** Raw query response */
  response: QueryResponse;
}

/**
 * Execute a CozoDB query via the API.
 *
 * @param query The CozoDB query string
 * @param extractGraph Whether to extract graph data from results
 * @returns Query execution result
 * @throws ApiClientError on API errors
 */
export async function executeQuery(
  query: string,
  extractGraph: boolean = true
): Promise<QueryExecutionResult> {
  const response = await api.post<QueryResponse>("/api/graph/query", {
    query,
    extract_graph: extractGraph,
  });

  return {
    resultCount: response.results?.rows?.length ?? 0,
    graph: response.graph,
    response,
  };
}

/**
 * Get a user-friendly error message from a query error.
 */
export function getQueryErrorMessage(err: unknown): string {
  if (err instanceof ApiClientError) {
    return err.message;
  }
  return String(err);
}
