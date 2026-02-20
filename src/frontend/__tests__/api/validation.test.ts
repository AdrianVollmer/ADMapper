/**
 * Tests for API response validation.
 */

import { describe, it, expect } from "vitest";
import {
  isGraphNode,
  isGraphEdge,
  isGraphData,
  isSearchResult,
  isSearchResultArray,
  isPathStep,
  isPathResponse,
  isQueryResponse,
  isQueryHistoryEntry,
  isPaginatedResponse,
  assertValidResponse,
} from "../../api/validation";

// ============================================================================
// isGraphNode
// ============================================================================

describe("isGraphNode", () => {
  it("returns true for valid node", () => {
    expect(isGraphNode({ id: "1", name: "Test", type: "User" })).toBe(true);
  });

  it("returns true for node with optional properties", () => {
    expect(
      isGraphNode({
        id: "1",
        name: "Test",
        type: "User",
        properties: { enabled: true },
      })
    ).toBe(true);
  });

  it("returns false for missing id", () => {
    expect(isGraphNode({ name: "Test", type: "User" })).toBe(false);
  });

  it("returns false for non-string id", () => {
    expect(isGraphNode({ id: 123, name: "Test", type: "User" })).toBe(false);
  });

  it("returns false for null", () => {
    expect(isGraphNode(null)).toBe(false);
  });

  it("returns false for non-object", () => {
    expect(isGraphNode("string")).toBe(false);
  });
});

// ============================================================================
// isGraphEdge
// ============================================================================

describe("isGraphEdge", () => {
  it("returns true for valid edge", () => {
    expect(isGraphEdge({ source: "a", target: "b", type: "MemberOf" })).toBe(true);
  });

  it("returns false for missing source", () => {
    expect(isGraphEdge({ target: "b", type: "MemberOf" })).toBe(false);
  });
});

// ============================================================================
// isGraphData
// ============================================================================

describe("isGraphData", () => {
  it("returns true for valid graph data", () => {
    expect(
      isGraphData({
        nodes: [{ id: "1", name: "A", type: "User" }],
        edges: [{ source: "1", target: "2", type: "MemberOf" }],
      })
    ).toBe(true);
  });

  it("returns true for empty graph", () => {
    expect(isGraphData({ nodes: [], edges: [] })).toBe(true);
  });

  it("returns false for invalid node in array", () => {
    expect(
      isGraphData({
        nodes: [{ id: "1" }], // Missing label and type
        edges: [],
      })
    ).toBe(false);
  });

  it("returns false for missing nodes", () => {
    expect(isGraphData({ edges: [] })).toBe(false);
  });
});

// ============================================================================
// isSearchResult
// ============================================================================

describe("isSearchResult", () => {
  it("returns true for valid result", () => {
    expect(isSearchResult({ id: "1", name: "Admin", type: "User" })).toBe(true);
  });

  it("returns false for missing type", () => {
    expect(isSearchResult({ id: "1", name: "Admin" })).toBe(false);
  });
});

describe("isSearchResultArray", () => {
  it("returns true for valid array", () => {
    expect(
      isSearchResultArray([
        { id: "1", name: "A", type: "User" },
        { id: "2", name: "B", type: "Group" },
      ])
    ).toBe(true);
  });

  it("returns true for empty array", () => {
    expect(isSearchResultArray([])).toBe(true);
  });

  it("returns false for non-array", () => {
    expect(isSearchResultArray("not array")).toBe(false);
  });

  it("returns false for array with invalid item", () => {
    expect(isSearchResultArray([{ id: "1" }])).toBe(false);
  });
});

// ============================================================================
// isPathStep
// ============================================================================

describe("isPathStep", () => {
  it("returns true for valid step", () => {
    expect(
      isPathStep({
        node: { id: "1", name: "A", type: "User" },
        edge_type: "MemberOf",
      })
    ).toBe(true);
  });

  it("returns true for step without edge_type", () => {
    expect(
      isPathStep({
        node: { id: "1", name: "A", type: "User" },
      })
    ).toBe(true);
  });

  it("returns false for missing node", () => {
    expect(isPathStep({ edge_type: "MemberOf" })).toBe(false);
  });
});

// ============================================================================
// isPathResponse
// ============================================================================

describe("isPathResponse", () => {
  it("returns true for found path", () => {
    expect(
      isPathResponse({
        found: true,
        path: [
          { node: { id: "a", name: "A", type: "User" }, edge_type: "MemberOf" },
          { node: { id: "b", name: "B", type: "Group" } },
        ],
        graph: { nodes: [], edges: [] },
      })
    ).toBe(true);
  });

  it("returns true for not found path", () => {
    expect(
      isPathResponse({
        found: false,
        path: [],
        graph: { nodes: [], edges: [] },
      })
    ).toBe(true);
  });

  it("returns false for missing found", () => {
    expect(
      isPathResponse({
        path: [],
        graph: { nodes: [], edges: [] },
      })
    ).toBe(false);
  });

  it("returns false for non-boolean found", () => {
    expect(
      isPathResponse({
        found: "yes",
        path: [],
        graph: { nodes: [], edges: [] },
      })
    ).toBe(false);
  });
});

// ============================================================================
// isQueryResponse
// ============================================================================

describe("isQueryResponse", () => {
  it("returns true for valid response", () => {
    expect(
      isQueryResponse({
        results: {
          headers: ["x"],
          rows: [[1]],
        },
      })
    ).toBe(true);
  });

  it("returns true for response with graph", () => {
    expect(
      isQueryResponse({
        results: { headers: [], rows: [] },
        graph: { nodes: [], edges: [] },
      })
    ).toBe(true);
  });

  it("returns false for missing results", () => {
    expect(isQueryResponse({})).toBe(false);
  });

  it("returns false for invalid graph", () => {
    expect(
      isQueryResponse({
        results: { headers: [], rows: [] },
        graph: { invalid: true },
      })
    ).toBe(false);
  });
});

// ============================================================================
// isQueryHistoryEntry
// ============================================================================

describe("isQueryHistoryEntry", () => {
  it("returns true for valid entry", () => {
    expect(
      isQueryHistoryEntry({
        id: "1",
        name: "Test Query",
        query: "MATCH (n:Node) RETURN n",
        timestamp: 1234567890,
        result_count: 5,
      })
    ).toBe(true);
  });

  it("returns true for null result_count", () => {
    expect(
      isQueryHistoryEntry({
        id: "1",
        name: "Test",
        query: "RETURN 1",
        timestamp: 123,
        result_count: null,
      })
    ).toBe(true);
  });

  it("returns false for missing timestamp", () => {
    expect(
      isQueryHistoryEntry({
        id: "1",
        name: "Test",
        query: "RETURN 1",
        result_count: null,
      })
    ).toBe(false);
  });
});

// ============================================================================
// isPaginatedResponse
// ============================================================================

describe("isPaginatedResponse", () => {
  it("returns true for valid paginated response", () => {
    expect(
      isPaginatedResponse(
        {
          entries: [{ id: "1", name: "Q", query: "RETURN 1", timestamp: 1, result_count: null }],
          total: 1,
          page: 1,
          per_page: 10,
        },
        isQueryHistoryEntry
      )
    ).toBe(true);
  });

  it("returns true for empty entries", () => {
    expect(isPaginatedResponse({ entries: [], total: 0, page: 1, per_page: 10 }, isQueryHistoryEntry)).toBe(true);
  });

  it("returns false for invalid entry", () => {
    expect(
      isPaginatedResponse(
        {
          entries: [{ invalid: true }],
          total: 1,
          page: 1,
          per_page: 10,
        },
        isQueryHistoryEntry
      )
    ).toBe(false);
  });

  it("returns false for missing total", () => {
    expect(isPaginatedResponse({ entries: [], page: 1, per_page: 10 }, isQueryHistoryEntry)).toBe(false);
  });
});

// ============================================================================
// assertValidResponse
// ============================================================================

describe("assertValidResponse", () => {
  it("does not throw for valid data", () => {
    expect(() => assertValidResponse({ id: "1", name: "A", type: "User" }, isGraphNode, "GraphNode")).not.toThrow();
  });

  it("throws for invalid data", () => {
    expect(() => assertValidResponse({ invalid: true }, isGraphNode, "GraphNode")).toThrow(
      "Invalid API response: expected GraphNode"
    );
  });

  it("includes truncated value in error", () => {
    expect(() => assertValidResponse({ wrong: "data" }, isGraphNode, "GraphNode")).toThrow(/got.*wrong/);
  });
});
