/**
 * API Response Types
 *
 * TypeScript interfaces for all backend API responses.
 * These types provide compile-time safety and documentation.
 */

// ============================================================================
// Graph Data Types
// ============================================================================

/** Node in the graph from API */
export interface GraphNode {
  id: string;
  label: string;
  type: string;
  properties?: Record<string, unknown>;
}

/** Edge in the graph from API */
export interface GraphEdge {
  source: string;
  target: string;
  type: string;
  properties?: Record<string, unknown>;
}

/** Full graph data from /api/graph/all */
export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

// ============================================================================
// Search Types
// ============================================================================

/** Search result from /api/graph/search */
export interface SearchResult {
  id: string;
  label: string;
  type: string;
}

// ============================================================================
// Path Finding Types
// ============================================================================

/** Step in a path from /api/graph/path */
export interface PathStep {
  node: GraphNode;
  edge_type?: string;
}

/** Path finding response from /api/graph/path */
export interface PathResponse {
  found: boolean;
  path: PathStep[];
  graph: GraphData;
}

// ============================================================================
// Query Types
// ============================================================================

/** Query result structure */
export interface QueryResult {
  headers: string[];
  rows: unknown[][];
}

/** Status of a running or completed query */
export type QueryStatus = "running" | "completed" | "failed" | "aborted";

/** Response when starting an async query */
export interface QueryStartResponse {
  query_id: string;
}

/** Progress event from query SSE stream */
export interface QueryProgressEvent {
  query_id: string;
  status: QueryStatus;
  started_at: number;
  duration_ms: number | null;
  result_count: number | null;
  error: string | null;
  results?: QueryResult;
  graph?: GraphData;
}

/** Query response from /api/graph/query (legacy - now returns QueryStartResponse) */
export interface QueryResponse {
  results: QueryResult;
  graph?: GraphData;
}

// ============================================================================
// Query History Types
// ============================================================================

/** Query history entry from API */
export interface QueryHistoryEntry {
  id: string;
  name: string;
  query: string;
  timestamp: number;
  result_count: number | null;
  status: QueryStatus;
  started_at: number;
  duration_ms: number | null;
  error: string | null;
}

/** Paginated response wrapper */
export interface PaginatedResponse<T> {
  entries: T[];
  total: number;
  page: number;
  per_page: number;
}

/** Query history list response */
export type QueryHistoryResponse = PaginatedResponse<QueryHistoryEntry>;

// ============================================================================
// Import Types
// ============================================================================

/** Import job creation response */
export interface ImportJobResponse {
  job_id: string;
}

/** Import progress event from SSE */
export interface ImportProgress {
  job_id: string;
  status: "running" | "completed" | "failed";
  current_file: string | null;
  files_processed: number;
  total_files: number;
  nodes_imported: number;
  edges_imported: number;
  error: string | null;
}

// ============================================================================
// API Error Types
// ============================================================================

/** Standard API error response */
export interface ApiError {
  status: number;
  message: string;
}
