/**
 * Query Execution Utilities
 *
 * Shared logic for executing CozoDB queries.
 */

import { api, ApiClientError } from "../api/client";
import type { QueryResponse, GraphData } from "../api/types";
import { addToHistory } from "../components/query-history";

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
export async function executeQuery(query: string, extractGraph: boolean = true): Promise<QueryExecutionResult> {
  const response = await api.post<QueryResponse>("/api/graph/query", {
    query,
    extract_graph: extractGraph,
  });

  const result: QueryExecutionResult = {
    resultCount: response.results?.rows?.length ?? 0,
    response,
  };
  if (response.graph !== undefined) {
    result.graph = response.graph;
  }
  return result;
}

/**
 * Execute a query and automatically add it to history.
 *
 * This is the preferred way to run queries as it ensures all queries
 * are tracked in the history for later reference.
 *
 * @param name Display name for the query in history
 * @param query The CozoDB query string
 * @param extractGraph Whether to extract graph data from results
 * @returns Query execution result
 * @throws ApiClientError on API errors
 */
export async function executeQueryWithHistory(
  name: string,
  query: string,
  extractGraph: boolean = true
): Promise<QueryExecutionResult> {
  const result = await executeQuery(query, extractGraph);
  // Add to history in background (don't await to avoid blocking)
  addToHistory(name, query, result.resultCount);
  return result;
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
