/**
 * API Response Validation
 *
 * Runtime type guards for validating API responses.
 * These catch API contract mismatches before they cause runtime errors.
 */

import type {
  GraphNode,
  GraphEdge,
  GraphData,
  SearchResult,
  PathStep,
  PathResponse,
  QueryResponse,
  QueryHistoryEntry,
  PaginatedResponse,
} from "./types";

// ============================================================================
// Basic type guards
// ============================================================================

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNumber(value: unknown): value is number {
  return typeof value === "number";
}

function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value);
}

// ============================================================================
// Graph type guards
// ============================================================================

/** Validate a GraphNode structure */
export function isGraphNode(value: unknown): value is GraphNode {
  if (!isObject(value)) return false;
  return (
    isString(value.id) &&
    isString(value.label) &&
    isString(value.type)
  );
}

/** Validate a GraphEdge structure */
export function isGraphEdge(value: unknown): value is GraphEdge {
  if (!isObject(value)) return false;
  return (
    isString(value.source) &&
    isString(value.target) &&
    isString(value.type)
  );
}

/** Validate a GraphData structure */
export function isGraphData(value: unknown): value is GraphData {
  if (!isObject(value)) return false;
  if (!isArray(value.nodes) || !isArray(value.edges)) return false;
  return (
    value.nodes.every(isGraphNode) &&
    value.edges.every(isGraphEdge)
  );
}

// ============================================================================
// Search type guards
// ============================================================================

/** Validate a SearchResult structure */
export function isSearchResult(value: unknown): value is SearchResult {
  if (!isObject(value)) return false;
  return (
    isString(value.id) &&
    isString(value.label) &&
    isString(value.type)
  );
}

/** Validate an array of SearchResults */
export function isSearchResultArray(value: unknown): value is SearchResult[] {
  return isArray(value) && value.every(isSearchResult);
}

// ============================================================================
// Path type guards
// ============================================================================

/** Validate a PathStep structure */
export function isPathStep(value: unknown): value is PathStep {
  if (!isObject(value)) return false;
  if (!isObject(value.node)) return false;
  return isGraphNode(value.node);
}

/** Validate a PathResponse structure */
export function isPathResponse(value: unknown): value is PathResponse {
  if (!isObject(value)) return false;
  if (typeof value.found !== "boolean") return false;
  if (!isArray(value.path)) return false;
  if (!value.path.every(isPathStep)) return false;
  if (!isObject(value.graph)) return false;
  return isGraphData(value.graph);
}

// ============================================================================
// Query type guards
// ============================================================================

/** Validate a QueryResponse structure */
export function isQueryResponse(value: unknown): value is QueryResponse {
  if (!isObject(value)) return false;
  if (!isObject(value.results)) return false;

  const results = value.results as Record<string, unknown>;
  if (!isArray(results.headers) || !isArray(results.rows)) return false;

  // graph is optional
  if (value.graph !== undefined && !isGraphData(value.graph)) return false;

  return true;
}

// ============================================================================
// Query History type guards
// ============================================================================

/** Validate a QueryHistoryEntry structure */
export function isQueryHistoryEntry(value: unknown): value is QueryHistoryEntry {
  if (!isObject(value)) return false;
  return (
    isString(value.id) &&
    isString(value.name) &&
    isString(value.query) &&
    isNumber(value.timestamp) &&
    (value.result_count === null || isNumber(value.result_count))
  );
}

/** Validate a PaginatedResponse structure */
export function isPaginatedResponse<T>(
  value: unknown,
  itemGuard: (item: unknown) => item is T
): value is PaginatedResponse<T> {
  if (!isObject(value)) return false;
  if (!isArray(value.entries)) return false;
  if (!isNumber(value.total)) return false;
  if (!isNumber(value.page)) return false;
  if (!isNumber(value.per_page)) return false;
  return value.entries.every(itemGuard);
}

// ============================================================================
// Assertion helpers
// ============================================================================

/**
 * Assert that a value matches the expected type.
 * Throws an error with details if validation fails.
 */
export function assertValidResponse<T>(
  value: unknown,
  guard: (v: unknown) => v is T,
  typeName: string
): asserts value is T {
  if (!guard(value)) {
    throw new Error(
      `Invalid API response: expected ${typeName}, got ${JSON.stringify(value).slice(0, 100)}`
    );
  }
}
