/**
 * Tests for ADGraph utilities.
 */

import { describe, it, expect } from "vitest";
import {
  createGraph,
  loadGraph,
  addNode,
  addEdge,
  getNodesByType,
  getNeighbors,
  getReachableNodes,
  getGraphStats,
  clearGraph,
  exportGraph,
} from "../../graph/ADGraph";
import type { RawADGraph, RawADNode, RawADEdge, ADNodeType } from "../../graph/types";

// ============================================================================
// createGraph
// ============================================================================

describe("createGraph", () => {
  it("creates an empty directed multi-graph", () => {
    const graph = createGraph();
    expect(graph.order).toBe(0); // No nodes
    expect(graph.size).toBe(0); // No edges
    expect(graph.type).toBe("directed");
    expect(graph.multi).toBe(true);
  });

  it("allows self loops", () => {
    const graph = createGraph();
    graph.addNode("a", { label: "A", nodeType: "User", x: 0, y: 0, size: 10, color: "#000", image: "" });
    graph.addEdge("a", "a", { edgeType: "MemberOf", label: "Self" });
    expect(graph.size).toBe(1);
  });
});

// ============================================================================
// loadGraph
// ============================================================================

describe("loadGraph", () => {
  it("loads empty graph", () => {
    const data: RawADGraph = { nodes: [], edges: [] };
    const graph = loadGraph(data);
    expect(graph.order).toBe(0);
    expect(graph.size).toBe(0);
  });

  it("loads nodes with correct attributes", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "user1", name: "jsmith@corp.local", type: "User" },
        { id: "group1", name: "Domain Admins", type: "Group" },
      ],
      edges: [],
    };

    const graph = loadGraph(data);
    expect(graph.order).toBe(2);

    const userAttrs = graph.getNodeAttributes("user1");
    expect(userAttrs.label).toBe("jsmith@corp.local");
    expect(userAttrs.nodeType).toBe("User");
    expect(typeof userAttrs.x).toBe("number");
    expect(typeof userAttrs.y).toBe("number");
  });

  it("loads edges between existing nodes", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "user1", name: "jsmith", type: "User" },
        { id: "group1", name: "Admins", type: "Group" },
      ],
      edges: [{ source: "user1", target: "group1", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    expect(graph.size).toBe(1);
    expect(graph.hasEdge("user1", "group1")).toBe(true);
  });

  it("ignores edges with missing source node", () => {
    const data: RawADGraph = {
      nodes: [{ id: "group1", name: "Admins", type: "Group" }],
      edges: [{ source: "missing", target: "group1", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    expect(graph.size).toBe(0);
  });

  it("ignores edges with missing target node", () => {
    const data: RawADGraph = {
      nodes: [{ id: "user1", name: "jsmith", type: "User" }],
      edges: [{ source: "user1", target: "missing", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    expect(graph.size).toBe(0);
  });

  it("handles unknown node types", () => {
    const data: RawADGraph = {
      nodes: [{ id: "x", name: "Unknown Thing", type: "SomethingNew" as unknown as ADNodeType }],
      edges: [],
    };

    const graph = loadGraph(data);
    const attrs = graph.getNodeAttributes("x");
    expect(attrs.nodeType).toBe("Unknown");
  });

  it("preserves node properties", () => {
    const data: RawADGraph = {
      nodes: [
        {
          id: "user1",
          name: "jsmith",
          type: "User",
          properties: { enabled: true, admincount: 1 },
        },
      ],
      edges: [],
    };

    const graph = loadGraph(data);
    const attrs = graph.getNodeAttributes("user1");
    expect(attrs.properties).toEqual({ enabled: true, admincount: 1 });
  });
});

// ============================================================================
// Edge Curvature (tested through loadGraph)
// ============================================================================

describe("edge curvatures", () => {
  it("assigns no curvature to single edges", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
      ],
      edges: [{ source: "a", target: "b", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    const edge = graph.edges()[0];
    expect(graph.getEdgeAttribute(edge, "curvature")).toBe(0);
    expect(graph.getEdgeAttribute(edge, "type")).toBe("tapered");
  });

  it("assigns same curvatures to bidirectional edges (visually opposite)", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "b", target: "a", type: "AdminTo" },
      ],
    };

    const graph = loadGraph(data);
    const edges = graph.edges();
    expect(edges).toHaveLength(2);

    const curvatures = edges.map((e) => graph.getEdgeAttribute(e, "curvature"));
    // Both edges get the same positive curvature (0.2)
    // Since they go opposite directions, same curvature = visually opposite arcs
    expect(curvatures[0]).toBe(0.2);
    expect(curvatures[1]).toBe(0.2);
  });

  it("distributes curvatures evenly for multiple parallel edges", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "a", target: "b", type: "GenericAll" },
        { source: "a", target: "b", type: "WriteDacl" },
      ],
    };

    const graph = loadGraph(data);
    const edges = graph.edges();
    expect(edges).toHaveLength(3);

    const curvatures = edges.map((e) => graph.getEdgeAttribute(e, "curvature"));
    // All should be different
    expect(new Set(curvatures).size).toBe(3);
    // All should use curvedArrow type
    for (const e of edges) {
      expect(graph.getEdgeAttribute(e, "type")).toBe("curvedArrow");
    }
  });

  it("handles self-loops", () => {
    const data: RawADGraph = {
      nodes: [{ id: "a", name: "A", type: "User" }],
      edges: [{ source: "a", target: "a", type: "GenericAll" }],
    };

    const graph = loadGraph(data);
    expect(graph.size).toBe(1);
    // Self-loop should still get curvature assigned
    const edge = graph.edges()[0];
    expect(graph.getEdgeAttribute(edge, "curvature")).toBeDefined();
  });
});

// ============================================================================
// getNodesByType
// ============================================================================

describe("getNodesByType", () => {
  it("returns empty array for empty graph", () => {
    const graph = createGraph();
    expect(getNodesByType(graph, "User")).toEqual([]);
  });

  it("returns nodes of specified type", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "u1", name: "User 1", type: "User" },
        { id: "u2", name: "User 2", type: "User" },
        { id: "g1", name: "Group 1", type: "Group" },
      ],
      edges: [],
    };

    const graph = loadGraph(data);
    const users = getNodesByType(graph, "User");
    expect(users).toHaveLength(2);
    expect(users).toContain("u1");
    expect(users).toContain("u2");
  });

  it("returns empty array when no nodes match", () => {
    const data: RawADGraph = {
      nodes: [{ id: "g1", name: "Group 1", type: "Group" }],
      edges: [],
    };

    const graph = loadGraph(data);
    expect(getNodesByType(graph, "Computer")).toEqual([]);
  });
});

// ============================================================================
// getNeighbors
// ============================================================================

describe("getNeighbors", () => {
  it("returns empty array for non-existent node", () => {
    const graph = createGraph();
    expect(getNeighbors(graph, "missing")).toEqual([]);
  });

  it("returns empty array for isolated node", () => {
    const data: RawADGraph = {
      nodes: [{ id: "a", name: "A", type: "User" }],
      edges: [],
    };

    const graph = loadGraph(data);
    expect(getNeighbors(graph, "a")).toEqual([]);
  });

  it("returns all neighbors (in and out)", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
        { id: "c", name: "C", type: "Computer" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "c", target: "a", type: "HasSession" },
      ],
    };

    const graph = loadGraph(data);
    const neighbors = getNeighbors(graph, "a");
    expect(neighbors).toHaveLength(2);
    expect(neighbors).toContain("b");
    expect(neighbors).toContain("c");
  });
});

// ============================================================================
// getReachableNodes
// ============================================================================

describe("getReachableNodes", () => {
  it("returns only start node when isolated", () => {
    const data: RawADGraph = {
      nodes: [{ id: "a", name: "A", type: "User" }],
      edges: [],
    };

    const graph = loadGraph(data);
    const reachable = getReachableNodes(graph, "a");
    expect(reachable.size).toBe(1);
    expect(reachable.has("a")).toBe(true);
  });

  it("finds all reachable nodes via outgoing edges", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
        { id: "c", name: "C", type: "Computer" },
        { id: "d", name: "D", type: "Domain" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "b", target: "c", type: "AdminTo" },
        // d is not reachable from a
      ],
    };

    const graph = loadGraph(data);
    const reachable = getReachableNodes(graph, "a");
    expect(reachable.size).toBe(3);
    expect(reachable.has("a")).toBe(true);
    expect(reachable.has("b")).toBe(true);
    expect(reachable.has("c")).toBe(true);
    expect(reachable.has("d")).toBe(false);
  });

  it("respects maxDepth parameter", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
        { id: "c", name: "C", type: "Computer" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "b", target: "c", type: "AdminTo" },
      ],
    };

    const graph = loadGraph(data);

    // Depth 1: only a and b
    const reachable1 = getReachableNodes(graph, "a", 1);
    expect(reachable1.size).toBe(2);
    expect(reachable1.has("c")).toBe(false);

    // Depth 2: a, b, and c
    const reachable2 = getReachableNodes(graph, "a", 2);
    expect(reachable2.size).toBe(3);
    expect(reachable2.has("c")).toBe(true);
  });

  it("handles cycles without infinite loop", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
        { id: "c", name: "C", type: "Computer" },
      ],
      edges: [
        { source: "a", target: "b", type: "MemberOf" },
        { source: "b", target: "c", type: "AdminTo" },
        { source: "c", target: "a", type: "HasSession" }, // Cycle back to a
      ],
    };

    const graph = loadGraph(data);
    const reachable = getReachableNodes(graph, "a");
    expect(reachable.size).toBe(3);
  });
});

// ============================================================================
// getGraphStats
// ============================================================================

describe("getGraphStats", () => {
  it("returns zeros for empty graph", () => {
    const graph = createGraph();
    const stats = getGraphStats(graph);
    expect(stats.nodeCount).toBe(0);
    expect(stats.edgeCount).toBe(0);
    expect(stats.nodesByType).toEqual({});
  });

  it("counts nodes and edges correctly", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "u1", name: "User 1", type: "User" },
        { id: "u2", name: "User 2", type: "User" },
        { id: "g1", name: "Group 1", type: "Group" },
      ],
      edges: [
        { source: "u1", target: "g1", type: "MemberOf" },
        { source: "u2", target: "g1", type: "MemberOf" },
      ],
    };

    const graph = loadGraph(data);
    const stats = getGraphStats(graph);
    expect(stats.nodeCount).toBe(3);
    expect(stats.edgeCount).toBe(2);
    expect(stats.nodesByType).toEqual({ User: 2, Group: 1 });
  });
});

// ============================================================================
// clearGraph
// ============================================================================

describe("clearGraph", () => {
  it("removes all nodes and edges", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
      ],
      edges: [{ source: "a", target: "b", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    expect(graph.order).toBe(2);
    expect(graph.size).toBe(1);

    clearGraph(graph);
    expect(graph.order).toBe(0);
    expect(graph.size).toBe(0);
  });
});

// ============================================================================
// exportGraph
// ============================================================================

describe("exportGraph", () => {
  it("exports empty graph", () => {
    const graph = createGraph();
    const exported = exportGraph(graph);
    expect(exported.nodes).toEqual([]);
    expect(exported.edges).toEqual([]);
  });

  it("exports nodes with correct structure", () => {
    const data: RawADGraph = {
      nodes: [
        {
          id: "user1",
          name: "jsmith",
          type: "User",
          properties: { enabled: true },
        },
      ],
      edges: [],
    };

    const graph = loadGraph(data);
    const exported = exportGraph(graph);

    expect(exported.nodes).toHaveLength(1);
    expect(exported.nodes[0]!.id).toBe("user1");
    expect(exported.nodes[0]!.name).toBe("jsmith");
    expect(exported.nodes[0]!.type).toBe("User");
    expect(exported.nodes[0]!.properties).toEqual({ enabled: true });
  });

  it("exports edges with correct structure", () => {
    const data: RawADGraph = {
      nodes: [
        { id: "a", name: "A", type: "User" },
        { id: "b", name: "B", type: "Group" },
      ],
      edges: [{ source: "a", target: "b", type: "MemberOf" }],
    };

    const graph = loadGraph(data);
    const exported = exportGraph(graph);

    expect(exported.edges).toHaveLength(1);
    expect(exported.edges[0]!.source).toBe("a");
    expect(exported.edges[0]!.target).toBe("b");
    expect(exported.edges[0]!.type).toBe("MemberOf");
  });

  it("round-trips graph data", () => {
    const original: RawADGraph = {
      nodes: [
        { id: "u1", name: "User 1", type: "User" },
        { id: "g1", name: "Group 1", type: "Group" },
      ],
      edges: [{ source: "u1", target: "g1", type: "MemberOf" }],
    };

    const graph = loadGraph(original);
    const exported = exportGraph(graph);

    // Re-import
    const graph2 = loadGraph(exported);
    const stats1 = getGraphStats(graph);
    const stats2 = getGraphStats(graph2);

    expect(stats2.nodeCount).toBe(stats1.nodeCount);
    expect(stats2.edgeCount).toBe(stats1.edgeCount);
  });
});

// ============================================================================
// addNode / addEdge (unit tests)
// ============================================================================

describe("addNode", () => {
  it("adds a new node", () => {
    const graph = createGraph();
    const node: RawADNode = { id: "n1", name: "Node 1", type: "User" };
    addNode(graph, node);

    expect(graph.hasNode("n1")).toBe(true);
    expect(graph.getNodeAttribute("n1", "label")).toBe("Node 1");
  });

  it("merges attributes when node already exists", () => {
    const graph = createGraph();
    const node1: RawADNode = { id: "n1", name: "Original", type: "User" };
    const node2: RawADNode = { id: "n1", name: "Updated", type: "User" };

    addNode(graph, node1);
    addNode(graph, node2);

    expect(graph.order).toBe(1);
    expect(graph.getNodeAttribute("n1", "label")).toBe("Updated");
  });
});

describe("addEdge", () => {
  it("adds a new edge", () => {
    const graph = createGraph();
    addNode(graph, { id: "a", name: "A", type: "User" });
    addNode(graph, { id: "b", name: "B", type: "Group" });

    const edge: RawADEdge = { source: "a", target: "b", type: "MemberOf" };
    addEdge(graph, edge);

    expect(graph.size).toBe(1);
    expect(graph.hasEdge("a", "b")).toBe(true);
  });

  it("does not add duplicate edge with same type", () => {
    const graph = createGraph();
    addNode(graph, { id: "a", name: "A", type: "User" });
    addNode(graph, { id: "b", name: "B", type: "Group" });

    const edge: RawADEdge = { source: "a", target: "b", type: "MemberOf" };
    addEdge(graph, edge);
    addEdge(graph, edge);

    expect(graph.size).toBe(1);
  });

  it("allows multiple edges with different types", () => {
    const graph = createGraph();
    addNode(graph, { id: "a", name: "A", type: "User" });
    addNode(graph, { id: "b", name: "B", type: "Group" });

    addEdge(graph, { source: "a", target: "b", type: "MemberOf" });
    addEdge(graph, { source: "a", target: "b", type: "GenericAll" });

    expect(graph.size).toBe(2);
  });
});
