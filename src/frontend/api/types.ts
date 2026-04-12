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
  /** Display name (from BloodHound's name property) */
  name: string;
  /** Cypher label (User, Computer, Group, etc.) */
  type: string;
  properties?: Record<string, unknown>;
}

/** Relationship in the graph from API */
export interface GraphEdge {
  source: string;
  target: string;
  type: string;
  properties?: Record<string, unknown>;
  exploit_likelihood?: number;
}

/** Full graph data from /api/graph/all */
export interface GraphData {
  nodes: GraphNode[];
  relationships: GraphEdge[];
}

// ============================================================================
// Search Types
// ============================================================================

/** Search result from /api/graph/search */
export interface SearchResult {
  id: string;
  /** Display name (from BloodHound's name property) */
  name: string;
  /** Cypher label (User, Computer, Group, etc.) */
  type: string;
  /** Node properties */
  properties: Record<string, unknown>;
}

// ============================================================================
// Path Finding Types
// ============================================================================

/** Step in a path from /api/graph/path */
export interface PathStep {
  node: GraphNode;
  rel_type?: string;
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

/** Response when starting a query - can be sync (inline results) or async (query_id) */
export type QueryStartResponse = QueryStartResponseSync | QueryStartResponseAsync;

/** Sync response - query completed fast, results are inline */
export interface QueryStartResponseSync {
  mode: "sync";
  query_id: string;
  duration_ms: number;
  result_count: number | null;
  results?: QueryResult;
  graph?: GraphData;
}

/** Async response - query is running, subscribe to progress events */
export interface QueryStartResponseAsync {
  mode: "async";
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
  /** Whether this is a background query (auto-fired, not user-initiated) */
  background: boolean;
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
// Database Types
// ============================================================================

/** Database connection status response */
export interface DatabaseStatusResponse {
  connected: boolean;
  database_type: string | null;
}

/** Database type identifiers */
export type DatabaseType = "crustdb" | "neo4j" | "falkordb";

/** Supported database info */
export interface SupportedDatabaseInfo {
  id: DatabaseType;
  name: string;
  connection_type: "file" | "network";
}

// ============================================================================
// API Error Types
// ============================================================================

/** Standard API error response */
export interface ApiError {
  status: number;
  message: string;
}

// ============================================================================
// Settings Types
// ============================================================================

/** Theme options */
export type Theme = "dark" | "light";

/** Graph layout options */
export type GraphLayout = "force" | "hierarchical" | "circular" | "grid" | "lattice";

/** Layout settings (visgraph) */
export interface LayoutSettings {
  /** Iterations for force-directed layout (1-5000, default 1000) */
  iterations: number;
  /** Initial temperature for force-directed layout (0.01-1.0, default 0.1) */
  temperature: number;
  /** Direction for hierarchical layout */
  direction: LayoutDirection;
}

/** Application settings */
export interface Settings {
  theme: Theme;
  defaultGraphLayout: GraphLayout;
  layout?: LayoutSettings;
  /** If true, nodes and relationships stay same visual size regardless of zoom level */
  fixedNodeSizes?: boolean;
  /** Nodes with more than this many incoming connections are auto-collapsed on load (0 = disabled) */
  autoCollapseThreshold?: number;
}

// ============================================================================
// Server-Side Layout Types
// ============================================================================

/** Available server-side layout algorithms */
export type ServerLayoutAlgorithm = "force_directed" | "hierarchical" | "circular" | "grid" | "lattice";

/** Direction for hierarchical layout */
export type LayoutDirection = "top_to_bottom" | "bottom_to_top" | "left_to_right" | "right_to_left";

/** Request body for POST /api/graph/layout */
export interface LayoutRequest {
  nodes: string[];
  edges: [number, number][];
  algorithm: ServerLayoutAlgorithm;
  direction?: LayoutDirection;
  iterations?: number;
  temperature?: number;
  /** Node type labels for tiebreaking in hierarchical layout ordering. */
  node_labels?: string[];
}

/** Single node position from layout response */
export interface NodePosition {
  id: string;
  x: number;
  y: number;
}

/** Response from POST /api/graph/layout */
export interface LayoutResponse {
  positions: NodePosition[];
}

// ============================================================================
// Cache Types
// ============================================================================

/** Query cache statistics */
export interface CacheStats {
  /** Whether the connected database supports caching */
  supported: boolean;
  /** Number of cached entries (if supported) */
  entry_count: number | null;
  /** Total size of cached data in bytes (if supported) */
  size_bytes: number | null;
}
