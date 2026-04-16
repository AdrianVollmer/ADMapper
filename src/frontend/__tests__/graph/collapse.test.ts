/**
 * Tests for graph collapse functionality.
 */

import { describe, it, expect, beforeEach } from "vitest";
import {
  isNodeCollapsed,
  getHiddenChildCount,
  collapseNode,
  expandNode,
  toggleNodeCollapse,
  clearCollapseState,
  getNodeCollapseInfo,
  getHiddenNodeIds,
  autoCollapseGraph,
} from "../../graph/collapse";
import { createGraph } from "../../graph/ADGraph";
import type { ADGraphType } from "../../graph/ADGraph";

const NODE = { x: 0, y: 0, size: 10, color: "#000", image: "" };

/**
 * Build a small test graph.
 *
 *   member-1 ─┐
 *   member-2 ─┤
 *   member-3 ─┼─▶ hub ──▶ target
 *   member-4 ─┤
 *   member-5 ─┘
 *
 * All members are leaves (inDegree 0). Hub has outDegree 1.
 */
function buildTestGraph(): ADGraphType {
  const graph = createGraph();

  graph.addNode("hub", { ...NODE, label: "Hub", nodeType: "Group" });

  for (let i = 1; i <= 5; i++) {
    const id = `member-${i}`;
    graph.addNode(id, { ...NODE, label: `Member ${i}`, nodeType: "User" });
    graph.addEdge(id, "hub", { relationshipType: "MemberOf", label: "MemberOf" });
  }

  graph.addNode("target", { ...NODE, label: "Target", nodeType: "Domain" });
  graph.addEdge("hub", "target", { relationshipType: "GenericAll", label: "GenericAll" });

  return graph;
}

beforeEach(() => {
  clearCollapseState();
});

// ============================================================================
// collapseNode
// ============================================================================

describe("collapseNode", () => {
  it("hides incoming leaf neighbors, not outgoing", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "hub");

    expect(isNodeCollapsed("hub")).toBe(true);
    expect(getHiddenChildCount("hub")).toBe(5);

    const hidden = getHiddenNodeIds();
    expect(hidden.has("member-1")).toBe(true);
    expect(hidden.has("member-5")).toBe(true);
    expect(hidden.has("target")).toBe(false);
  });

  it("does not hide non-leaf incoming neighbors", () => {
    const graph = createGraph();

    //  src ──▶ mid ──▶ hub ──▶ target
    //                   ▲
    //          leaf ────┘
    graph.addNode("hub", { ...NODE, label: "Hub", nodeType: "Group" });
    graph.addNode("target", { ...NODE, label: "Target", nodeType: "Domain" });
    graph.addNode("mid", { ...NODE, label: "Mid", nodeType: "Group" });
    graph.addNode("src", { ...NODE, label: "Src", nodeType: "User" });
    graph.addNode("leaf", { ...NODE, label: "Leaf", nodeType: "User" });

    graph.addEdge("hub", "target", { relationshipType: "GenericAll", label: "GenericAll" });
    graph.addEdge("mid", "hub", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("src", "mid", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("leaf", "hub", { relationshipType: "MemberOf", label: "MemberOf" });

    collapseNode(graph, "hub");

    expect(isNodeCollapsed("hub")).toBe(true);
    // Only "leaf" is hidden (inDegree 0). "mid" has inDegree 1, so it stays.
    expect(getHiddenChildCount("hub")).toBe(1);

    const hidden = getHiddenNodeIds();
    expect(hidden.has("leaf")).toBe(true);
    expect(hidden.has("mid")).toBe(false);
  });

  it("does nothing for a node with no incoming edges", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "member-1");

    expect(isNodeCollapsed("member-1")).toBe(false);
  });

  it("does nothing for a non-existent node", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "does-not-exist");
    expect(isNodeCollapsed("does-not-exist")).toBe(false);
  });

  it("does nothing for a node with no outgoing edges (pure sink)", () => {
    const graph = createGraph();

    graph.addNode("sink", { ...NODE, label: "Sink", nodeType: "Group" });
    graph.addNode("src-1", { ...NODE, label: "Src 1", nodeType: "User" });
    graph.addNode("src-2", { ...NODE, label: "Src 2", nodeType: "User" });
    graph.addEdge("src-1", "sink", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("src-2", "sink", { relationshipType: "MemberOf", label: "MemberOf" });

    collapseNode(graph, "sink");
    expect(isNodeCollapsed("sink")).toBe(false);
  });

  it("does nothing when all incoming neighbors are non-leaves", () => {
    const graph = createGraph();

    // src ──▶ mid ──▶ hub ──▶ target
    graph.addNode("hub", { ...NODE, label: "Hub", nodeType: "Group" });
    graph.addNode("target", { ...NODE, label: "Target", nodeType: "Domain" });
    graph.addNode("mid", { ...NODE, label: "Mid", nodeType: "Group" });
    graph.addNode("src", { ...NODE, label: "Src", nodeType: "User" });

    graph.addEdge("hub", "target", { relationshipType: "GenericAll", label: "GenericAll" });
    graph.addEdge("mid", "hub", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("src", "mid", { relationshipType: "MemberOf", label: "MemberOf" });

    collapseNode(graph, "hub");
    // "mid" has inDegree 1, not a leaf -- nothing to collapse
    expect(isNodeCollapsed("hub")).toBe(false);
  });
});

// ============================================================================
// expandNode / toggleNodeCollapse
// ============================================================================

describe("expandNode", () => {
  it("clears collapse state for a single node", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "hub");
    expect(isNodeCollapsed("hub")).toBe(true);

    expandNode("hub");
    expect(isNodeCollapsed("hub")).toBe(false);
    expect(getHiddenChildCount("hub")).toBe(0);
  });
});

describe("toggleNodeCollapse", () => {
  it("toggles between collapsed and expanded", () => {
    const graph = buildTestGraph();

    const collapsed = toggleNodeCollapse(graph, "hub");
    expect(collapsed).toBe(true);
    expect(isNodeCollapsed("hub")).toBe(true);

    const expanded = toggleNodeCollapse(graph, "hub");
    expect(expanded).toBe(false);
    expect(isNodeCollapsed("hub")).toBe(false);
  });
});

// ============================================================================
// getNodeCollapseInfo
// ============================================================================

describe("getNodeCollapseInfo", () => {
  it("returns correct info for collapsed node", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "hub");

    const info = getNodeCollapseInfo(graph, "hub");
    expect(info.isCollapsed).toBe(true);
    expect(info.hiddenCount).toBe(5);
    expect(info.totalLeaves).toBe(5);
  });

  it("returns correct info for non-collapsed node", () => {
    const graph = buildTestGraph();

    const info = getNodeCollapseInfo(graph, "hub");
    expect(info.isCollapsed).toBe(false);
    expect(info.hiddenCount).toBe(0);
    expect(info.totalLeaves).toBe(5);
  });
});

// ============================================================================
// autoCollapseGraph
// ============================================================================

describe("autoCollapseGraph", () => {
  it("returns 0 and does nothing when threshold is 0", () => {
    const graph = buildTestGraph();
    const count = autoCollapseGraph(graph, 0);

    expect(count).toBe(0);
    expect(isNodeCollapsed("hub")).toBe(false);
  });

  it("returns 0 and does nothing when threshold is negative", () => {
    const graph = buildTestGraph();
    const count = autoCollapseGraph(graph, -1);

    expect(count).toBe(0);
  });

  it("collapses nodes whose leaf-incoming count exceeds threshold", () => {
    const graph = buildTestGraph();
    // hub has 5 incoming leaves, threshold 3
    const count = autoCollapseGraph(graph, 3);

    expect(count).toBe(1);
    expect(isNodeCollapsed("hub")).toBe(true);
    expect(getHiddenChildCount("hub")).toBe(5);
  });

  it("does not collapse nodes at or below the threshold", () => {
    const graph = buildTestGraph();
    // hub has 5 incoming leaves, threshold 5 (not strictly greater)
    const count = autoCollapseGraph(graph, 5);

    expect(count).toBe(0);
    expect(isNodeCollapsed("hub")).toBe(false);
  });

  it("does not collapse nodes below the threshold", () => {
    const graph = buildTestGraph();
    const count = autoCollapseGraph(graph, 10);

    expect(count).toBe(0);
    expect(isNodeCollapsed("hub")).toBe(false);
  });

  it("collapses multiple qualifying nodes", () => {
    const graph = createGraph();

    graph.addNode("hub-a", { ...NODE, label: "Hub A", nodeType: "Group" });
    graph.addNode("hub-b", { ...NODE, label: "Hub B", nodeType: "Group" });
    graph.addNode("domain", { ...NODE, label: "Domain", nodeType: "Domain" });

    graph.addEdge("hub-a", "domain", { relationshipType: "GenericAll", label: "GenericAll" });
    graph.addEdge("hub-b", "domain", { relationshipType: "GenericAll", label: "GenericAll" });

    for (let i = 1; i <= 4; i++) {
      const id = `a-member-${i}`;
      graph.addNode(id, { ...NODE, label: id, nodeType: "User" });
      graph.addEdge(id, "hub-a", { relationshipType: "MemberOf", label: "MemberOf" });
    }

    for (let i = 1; i <= 3; i++) {
      const id = `b-member-${i}`;
      graph.addNode(id, { ...NODE, label: id, nodeType: "User" });
      graph.addEdge(id, "hub-b", { relationshipType: "MemberOf", label: "MemberOf" });
    }

    const count = autoCollapseGraph(graph, 2);
    expect(count).toBe(2);
    expect(isNodeCollapsed("hub-a")).toBe(true);
    expect(isNodeCollapsed("hub-b")).toBe(true);
  });

  it("skips nodes with no outgoing edges even if above threshold", () => {
    const graph = createGraph();

    graph.addNode("sink", { ...NODE, label: "Sink", nodeType: "Group" });
    for (let i = 1; i <= 5; i++) {
      const id = `src-${i}`;
      graph.addNode(id, { ...NODE, label: id, nodeType: "User" });
      graph.addEdge(id, "sink", { relationshipType: "MemberOf", label: "MemberOf" });
    }

    const count = autoCollapseGraph(graph, 2);
    expect(count).toBe(0);
    expect(isNodeCollapsed("sink")).toBe(false);
  });

  it("counts only leaf neighbors toward the threshold", () => {
    const graph = createGraph();

    // hub has 2 incoming: mid (non-leaf, has its own incoming) and leaf
    graph.addNode("hub", { ...NODE, label: "Hub", nodeType: "Group" });
    graph.addNode("target", { ...NODE, label: "Target", nodeType: "Domain" });
    graph.addNode("mid", { ...NODE, label: "Mid", nodeType: "Group" });
    graph.addNode("src", { ...NODE, label: "Src", nodeType: "User" });
    graph.addNode("leaf", { ...NODE, label: "Leaf", nodeType: "User" });

    graph.addEdge("hub", "target", { relationshipType: "GenericAll", label: "GenericAll" });
    graph.addEdge("mid", "hub", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("src", "mid", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("leaf", "hub", { relationshipType: "MemberOf", label: "MemberOf" });

    // inDegree of hub is 2, but only 1 is a leaf -- threshold 0 should NOT collapse
    // (threshold 0 is a no-op anyway), but threshold 1 should not either
    const count = autoCollapseGraph(graph, 1);
    expect(count).toBe(0);
    expect(isNodeCollapsed("hub")).toBe(false);

    // threshold 0 (disabled) still a no-op
    expect(autoCollapseGraph(graph, 0)).toBe(0);
  });
});

// ============================================================================
// clearCollapseState
// ============================================================================

describe("clearCollapseState", () => {
  it("clears all collapse state", () => {
    const graph = buildTestGraph();
    collapseNode(graph, "hub");
    expect(isNodeCollapsed("hub")).toBe(true);

    clearCollapseState();
    expect(isNodeCollapsed("hub")).toBe(false);
    expect(getHiddenNodeIds().size).toBe(0);
  });

  it("clears multiple collapsed nodes", () => {
    const graph = createGraph();

    graph.addNode("hub-a", { ...NODE, label: "Hub A", nodeType: "Group" });
    graph.addNode("hub-b", { ...NODE, label: "Hub B", nodeType: "Group" });
    graph.addNode("src", { ...NODE, label: "Src", nodeType: "User" });
    graph.addNode("domain", { ...NODE, label: "Domain", nodeType: "Domain" });
    graph.addEdge("src", "hub-a", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("src", "hub-b", { relationshipType: "MemberOf", label: "MemberOf" });
    graph.addEdge("hub-a", "domain", { relationshipType: "GenericAll", label: "GenericAll" });
    graph.addEdge("hub-b", "domain", { relationshipType: "GenericAll", label: "GenericAll" });

    collapseNode(graph, "hub-a");
    collapseNode(graph, "hub-b");
    expect(isNodeCollapsed("hub-a")).toBe(true);
    expect(isNodeCollapsed("hub-b")).toBe(true);

    clearCollapseState();
    expect(isNodeCollapsed("hub-a")).toBe(false);
    expect(isNodeCollapsed("hub-b")).toBe(false);
    expect(getHiddenNodeIds().size).toBe(0);
  });

  it("is a no-op when nothing is collapsed", () => {
    clearCollapseState();
    expect(getHiddenNodeIds().size).toBe(0);
  });
});
