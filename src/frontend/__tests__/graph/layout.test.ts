/**
 * Tests for the graph layout utilities, specifically validateAndFixPositions.
 */

import { describe, it, expect } from "vitest";
import { validateAndFixPositions } from "../../graph/layout";
import { createGraph } from "../../graph/ADGraph";
import type { ADGraphType } from "../../graph/ADGraph";

const NODE_DEFAULTS = { size: 10, color: "#000", image: "", label: "Node", nodeType: "User" };

function makeGraph(nodes: Array<{ id: string; x: number; y: number }>): ADGraphType {
  const graph = createGraph();
  for (const n of nodes) {
    graph.addNode(n.id, { ...NODE_DEFAULTS, x: n.x, y: n.y });
  }
  return graph;
}

// ============================================================================
// validateAndFixPositions — baseline behaviour
// ============================================================================

describe("validateAndFixPositions", () => {
  it("returns 0 for an empty graph", () => {
    const graph = createGraph();
    expect(validateAndFixPositions(graph)).toBe(0);
  });

  it("returns 0 when all positions are valid", () => {
    const graph = makeGraph([
      { id: "a", x: 0, y: 0 },
      { id: "b", x: 100, y: 100 },
      { id: "c", x: -50, y: 200 },
    ]);
    expect(validateAndFixPositions(graph)).toBe(0);
  });

  it("fixes NaN positions", () => {
    const graph = makeGraph([
      { id: "a", x: 0, y: 0 },
      { id: "b", x: NaN, y: NaN },
    ]);
    const fixed = validateAndFixPositions(graph);
    expect(fixed).toBe(1);
    const x = graph.getNodeAttribute("b", "x");
    const y = graph.getNodeAttribute("b", "y");
    expect(Number.isFinite(x)).toBe(true);
    expect(Number.isFinite(y)).toBe(true);
  });

  it("reassigns to grid when layout is degenerate (all at same point)", () => {
    const graph = makeGraph([
      { id: "a", x: 0, y: 0 },
      { id: "b", x: 0, y: 0 },
      { id: "c", x: 0, y: 0 },
    ]);
    const fixed = validateAndFixPositions(graph);
    expect(fixed).toBe(3);
  });
});

// ============================================================================
// validateAndFixPositions — with hiddenNodeIds
// ============================================================================

describe("validateAndFixPositions with hiddenNodeIds", () => {
  it("does not touch hidden nodes with invalid positions", () => {
    // "hidden" has NaN position but should not be included in fix count.
    const graph = makeGraph([
      { id: "visible-a", x: 0, y: 0 },
      { id: "visible-b", x: 100, y: 100 },
      { id: "hidden", x: NaN, y: NaN },
    ]);
    const hiddenNodeIds = new Set(["hidden"]);
    const fixed = validateAndFixPositions(graph, hiddenNodeIds);

    // Only visible nodes considered; none of them are invalid, so 0 fixed.
    expect(fixed).toBe(0);

    // The hidden node's position must remain untouched (NaN).
    expect(graph.getNodeAttribute("hidden", "x")).toBeNaN();
  });

  it("degenerate detection ignores hidden nodes", () => {
    // visible-a and visible-b are at the same point: degenerate for visible nodes.
    // hidden has a distinct position, but it must not save the visible layout.
    const graph = makeGraph([
      { id: "visible-a", x: 0, y: 0 },
      { id: "visible-b", x: 0, y: 0 },
      { id: "hidden", x: 500, y: 500 },
    ]);
    const hiddenNodeIds = new Set(["hidden"]);
    const fixed = validateAndFixPositions(graph, hiddenNodeIds);

    // Visible nodes are degenerate and must be reassigned.
    expect(fixed).toBe(2);

    // The hidden node must not be repositioned.
    expect(graph.getNodeAttribute("hidden", "x")).toBe(500);
    expect(graph.getNodeAttribute("hidden", "y")).toBe(500);
  });

  it("returns 0 when all invalid positions belong to hidden nodes", () => {
    // Only the hidden nodes have bad positions; visible nodes are fine.
    const graph = makeGraph([
      { id: "visible-a", x: 0, y: 0 },
      { id: "visible-b", x: 200, y: 100 },
      { id: "hidden-1", x: NaN, y: NaN },
      { id: "hidden-2", x: Infinity, y: 0 },
    ]);
    const hiddenNodeIds = new Set(["hidden-1", "hidden-2"]);
    const fixed = validateAndFixPositions(graph, hiddenNodeIds);

    expect(fixed).toBe(0);
  });

  it("fixes visible nodes with invalid positions while skipping hidden ones", () => {
    const graph = makeGraph([
      { id: "good", x: 0, y: 0 },
      { id: "bad-visible", x: NaN, y: 0 },
      { id: "bad-hidden", x: NaN, y: 0 },
    ]);
    const hiddenNodeIds = new Set(["bad-hidden"]);
    const fixed = validateAndFixPositions(graph, hiddenNodeIds);

    // Only bad-visible is fixed.
    expect(fixed).toBe(1);
    expect(Number.isFinite(graph.getNodeAttribute("bad-visible", "x"))).toBe(true);
    // bad-hidden stays NaN.
    expect(graph.getNodeAttribute("bad-hidden", "x")).toBeNaN();
  });

  it("behaves identically to no-hidden-ids when hiddenNodeIds is empty", () => {
    const graph = makeGraph([
      { id: "a", x: 0, y: 0 },
      { id: "b", x: 100, y: 100 },
    ]);
    const fixed = validateAndFixPositions(graph, new Set());
    expect(fixed).toBe(0);
  });
});
