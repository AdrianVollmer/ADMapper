/**
 * ADGraph: Core graph data structure for AD visualization.
 *
 * Wraps graphology with AD-specific types and utilities.
 */

import Graph from "graphology";
import type {
  ADNodeAttributes,
  ADEdgeAttributes,
  RawADGraph,
  RawADNode,
  RawADEdge,
  ADNodeType,
} from "./types";
import { NODE_COLORS, NODE_SIZES, EDGE_COLORS, DEFAULT_EDGE_SIZE } from "./theme";

export type ADGraphType = Graph<ADNodeAttributes, ADEdgeAttributes>;

/** Create an empty AD graph */
export function createGraph(): ADGraphType {
  return new Graph<ADNodeAttributes, ADEdgeAttributes>({
    type: "directed",
    multi: false,
    allowSelfLoops: false,
  });
}

/** Convert a raw node to graphology attributes */
function rawNodeToAttributes(node: RawADNode): ADNodeAttributes {
  const attrs: ADNodeAttributes = {
    label: node.label,
    nodeType: node.type,
    x: node.x ?? Math.random() * 1000,
    y: node.y ?? Math.random() * 1000,
    size: NODE_SIZES[node.type] ?? NODE_SIZES.Unknown,
    color: NODE_COLORS[node.type] ?? NODE_COLORS.Unknown,
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
    color: EDGE_COLORS[edge.type] ?? EDGE_COLORS.Unknown,
    size: DEFAULT_EDGE_SIZE,
  };
  if (edge.label) {
    attrs.label = edge.label;
  }
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

  return graph;
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
export function getReachableNodes(
  graph: ADGraphType,
  startId: string,
  maxDepth = Infinity
): Set<string> {
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
      label: attrs.label,
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
