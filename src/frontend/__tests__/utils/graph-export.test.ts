/**
 * Tests for parseExportJSON — converts an ADMapper JSON export back to RawADGraph.
 */

import { describe, it, expect } from "vitest";
import { parseExportJSON } from "../../utils/graph-export";

const VALID_EXPORT = {
  exportedAt: "2024-01-01T00:00:00.000Z",
  nodeCount: 2,
  edgeCount: 1,
  nodes: [
    { id: "n1", label: "Alice", type: "User", x: 10, y: 20, properties: { objectid: "S-1" } },
    { id: "n2", label: "Admins", type: "Group", x: 50, y: 60, properties: {} },
  ],
  relationships: [{ source: "n1", target: "n2", type: "MemberOf" }],
};

// ============================================================================
// Happy path
// ============================================================================

describe("parseExportJSON", () => {
  it("returns a RawADGraph with correctly mapped nodes", () => {
    const { graph } = parseExportJSON(JSON.stringify(VALID_EXPORT));
    expect(graph.nodes).toHaveLength(2);
    expect(graph.nodes[0]).toMatchObject({ id: "n1", name: "Alice", type: "User", x: 10, y: 20 });
    expect(graph.nodes[1]).toMatchObject({ id: "n2", name: "Admins", type: "Group" });
  });

  it("maps export 'label' field to RawADNode 'name'", () => {
    const { graph } = parseExportJSON(JSON.stringify(VALID_EXPORT));
    expect(graph.nodes[0]!.name).toBe("Alice");
  });

  it("preserves node properties", () => {
    const { graph } = parseExportJSON(JSON.stringify(VALID_EXPORT));
    expect(graph.nodes[0]!.properties).toEqual({ objectid: "S-1" });
  });

  it("returns correctly mapped relationships", () => {
    const { graph } = parseExportJSON(JSON.stringify(VALID_EXPORT));
    expect(graph.relationships).toHaveLength(1);
    expect(graph.relationships[0]).toMatchObject({ source: "n1", target: "n2", type: "MemberOf" });
  });

  it("handles empty nodes and relationships arrays", () => {
    const empty = { ...VALID_EXPORT, nodes: [], relationships: [], nodeCount: 0, edgeCount: 0 };
    const { graph } = parseExportJSON(JSON.stringify(empty));
    expect(graph.nodes).toEqual([]);
    expect(graph.relationships).toEqual([]);
  });

  it("falls back to id when label is missing from a node", () => {
    const noLabel = {
      ...VALID_EXPORT,
      nodes: [{ id: "n1", type: "User", x: 0, y: 0, properties: {} }],
    };
    const { graph } = parseExportJSON(JSON.stringify(noLabel));
    expect(graph.nodes[0]!.name).toBe("n1");
  });

  it("returns the query field when present", () => {
    const withQuery = { ...VALID_EXPORT, query: "MATCH (n:User) RETURN n" };
    const { query } = parseExportJSON(JSON.stringify(withQuery));
    expect(query).toBe("MATCH (n:User) RETURN n");
  });

  it("returns undefined query when field is absent", () => {
    const { query } = parseExportJSON(JSON.stringify(VALID_EXPORT));
    expect(query).toBeUndefined();
  });

  it("ignores non-string query field", () => {
    const badQuery = { ...VALID_EXPORT, query: 42 };
    const { query } = parseExportJSON(JSON.stringify(badQuery));
    expect(query).toBeUndefined();
  });
});

// ============================================================================
// Validation errors
// ============================================================================

describe("parseExportJSON — validation errors", () => {
  it("throws on malformed JSON", () => {
    expect(() => parseExportJSON("{not valid json")).toThrow();
  });

  it("throws when nodes is missing", () => {
    const bad = { relationships: [] };
    expect(() => parseExportJSON(JSON.stringify(bad))).toThrow(/nodes/i);
  });

  it("throws when nodes is not an array", () => {
    const bad = { nodes: "oops", relationships: [] };
    expect(() => parseExportJSON(JSON.stringify(bad))).toThrow(/nodes/i);
  });

  it("throws when relationships is missing", () => {
    const bad = { nodes: [] };
    expect(() => parseExportJSON(JSON.stringify(bad))).toThrow(/relationships/i);
  });

  it("throws when relationships is not an array", () => {
    const bad = { nodes: [], relationships: 42 };
    expect(() => parseExportJSON(JSON.stringify(bad))).toThrow(/relationships/i);
  });
});
