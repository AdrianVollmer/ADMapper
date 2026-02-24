/**
 * ADGraph: Core graph data structure for AD visualization.
 *
 * Wraps graphology with AD-specific types and utilities.
 */

import Graph from "graphology";
import type { ADNodeAttributes, ADEdgeAttributes, RawADGraph, RawADNode, RawADEdge, ADNodeType } from "./types";
import { NODE_COLORS, DEFAULT_EDGE_SIZE, DEFAULT_EDGE_COLOR } from "./theme";
import { getNodeIcon, getNodeTypeColor, NODE_SIZE } from "./icons";

export type ADGraphType = Graph<ADNodeAttributes, ADEdgeAttributes>;

/** Create an empty AD graph */
export function createGraph(): ADGraphType {
  return new Graph<ADNodeAttributes, ADEdgeAttributes>({
    type: "directed",
    multi: true, // Allow multiple edges between same nodes (e.g., MemberOf + GenericAll)
    allowSelfLoops: true,
  });
}

/** Convert a raw node to graphology attributes */
function rawNodeToAttributes(node: RawADNode): ADNodeAttributes {
  // Use Unknown type for unrecognized node types
  const nodeType = NODE_COLORS[node.type] ? node.type : "Unknown";

  const attrs: ADNodeAttributes = {
    label: node.name, // Display name from backend's "name" field
    nodeType: nodeType,
    x: node.x ?? Math.random() * 1000,
    y: node.y ?? Math.random() * 1000,
    size: NODE_SIZE,
    color: getNodeTypeColor(nodeType),
    image: getNodeIcon(nodeType),
  };
  if (node.properties) {
    attrs.properties = node.properties;
  }
  return attrs;
}

/** Convert a raw edge to graphology attributes */
function rawEdgeToAttributes(edge: RawADEdge): ADEdgeAttributes {
  const attrs: ADEdgeAttributes = {
    edgeType: edge.type,
    label: edge.label ?? edge.type, // Use edge type as label if not provided
    color: DEFAULT_EDGE_COLOR,
    size: DEFAULT_EDGE_SIZE,
    type: "triangle", // Default to triangle (tapered), will be updated for multi-edges
  };
  return attrs;
}

/** Add a node to the graph */
export function addNode(graph: ADGraphType, node: RawADNode): void {
  if (graph.hasNode(node.id)) {
    graph.mergeNodeAttributes(node.id, rawNodeToAttributes(node));
  } else {
    graph.addNode(node.id, rawNodeToAttributes(node));
  }
}

/** Add an edge to the graph */
export function addEdge(graph: ADGraphType, edge: RawADEdge): void {
  const edgeKey = `${edge.source}-${edge.type}-${edge.target}`;
  if (!graph.hasEdge(edgeKey)) {
    graph.addEdgeWithKey(edgeKey, edge.source, edge.target, rawEdgeToAttributes(edge));
  }
}

/** Load raw graph data into a graphology instance */
export function loadGraph(data: RawADGraph): ADGraphType {
  const graph = createGraph();

  for (const node of data.nodes) {
    addNode(graph, node);
  }

  for (const edge of data.edges) {
    // Only add edge if both endpoints exist
    if (graph.hasNode(edge.source) && graph.hasNode(edge.target)) {
      addEdge(graph, edge);
    }
  }

  // Assign curvature to parallel edges (multiple edges between same node pair)
  assignEdgeCurvatures(graph);

  return graph;
}

/** Assign curvature values to edges to spread out parallel edges */
function assignEdgeCurvatures(graph: ADGraphType): void {
  // Group edges by their node pair (ignoring direction for grouping)
  const edgeGroups = new Map<string, Array<{ key: string; source: string; target: string }>>();

  graph.forEachEdge((edgeKey, _attrs, source, target) => {
    // Create a canonical key for the node pair (smaller id first)
    const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
    const group = edgeGroups.get(pairKey) ?? [];
    group.push({ key: edgeKey, source, target });
    edgeGroups.set(pairKey, group);
  });

  // Assign curvature to edges in groups with multiple edges
  for (const [pairKey, edges] of edgeGroups.entries()) {
    if (edges.length === 1) {
      // Single edge: triangle (tapered)
      const edge = edges[0]!;
      graph.setEdgeAttribute(edge.key, "type", "triangle");
      graph.setEdgeAttribute(edge.key, "curvature", 0);
    } else {
      // Multiple edges: separate by direction and spread with curvature
      const [canonicalSource] = pairKey.split("|");

      // Separate edges by direction
      const forward = edges.filter((e) => e.source === canonicalSource);
      const backward = edges.filter((e) => e.source !== canonicalSource);

      // Assign curvatures: both directions get POSITIVE curvature
      // Since backward edges go the opposite direction, positive curvature
      // on them will visually curve to the opposite side of forward edges
      const assignCurvatures = (edgeList: typeof edges) => {
        let i = 0;
        for (const edge of edgeList) {
          const curvature = 0.2 + i * 0.15;
          graph.setEdgeAttribute(edge.key, "type", "curvedArrow");
          graph.setEdgeAttribute(edge.key, "curvature", curvature);
          graph.setEdgeAttribute(edge.key, "size", 3);
          i++;
        }
      };

      assignCurvatures(forward);
      assignCurvatures(backward);
    }
  }
}

/** Get all nodes of a specific type */
export function getNodesByType(graph: ADGraphType, type: ADNodeType): string[] {
  const nodes: string[] = [];
  graph.forEachNode((nodeId, attrs) => {
    if (attrs.nodeType === type) {
      nodes.push(nodeId);
    }
  });
  return nodes;
}

/** Get immediate neighbors of a node */
export function getNeighbors(graph: ADGraphType, nodeId: string): string[] {
  if (!graph.hasNode(nodeId)) return [];
  return graph.neighbors(nodeId);
}

/** Get all nodes reachable from a starting node (BFS) */
export function getReachableNodes(graph: ADGraphType, startId: string, maxDepth = Infinity): Set<string> {
  const visited = new Set<string>();
  const queue: Array<{ id: string; depth: number }> = [{ id: startId, depth: 0 }];

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || current.depth > maxDepth) continue;
    if (visited.has(current.id)) continue;

    visited.add(current.id);

    for (const neighbor of graph.outNeighbors(current.id)) {
      if (!visited.has(neighbor)) {
        queue.push({ id: neighbor, depth: current.depth + 1 });
      }
    }
  }

  return visited;
}

/** Get statistics about the graph */
export function getGraphStats(graph: ADGraphType): {
  nodeCount: number;
  edgeCount: number;
  nodesByType: Record<string, number>;
} {
  const nodesByType: Record<string, number> = {};

  graph.forEachNode((_, attrs) => {
    nodesByType[attrs.nodeType] = (nodesByType[attrs.nodeType] ?? 0) + 1;
  });

  return {
    nodeCount: graph.order,
    edgeCount: graph.size,
    nodesByType,
  };
}

/** Clear all nodes and edges from the graph */
export function clearGraph(graph: ADGraphType): void {
  graph.clear();
}

/** Export graph to JSON (for debugging/persistence) */
export function exportGraph(graph: ADGraphType): RawADGraph {
  const nodes: RawADNode[] = [];
  const edges: RawADEdge[] = [];

  graph.forEachNode((id, attrs) => {
    const node: RawADNode = {
      id,
      name: attrs.label, // Export display label as "name" to match backend
      type: attrs.nodeType,
      x: attrs.x,
      y: attrs.y,
    };
    if (attrs.properties) {
      node.properties = attrs.properties;
    }
    nodes.push(node);
  });

  graph.forEachEdge((_, attrs, source, target) => {
    const edge: RawADEdge = {
      source,
      target,
      type: attrs.edgeType,
    };
    if (attrs.label) {
      edge.label = attrs.label;
    }
    edges.push(edge);
  });

  return { nodes, edges };
}
