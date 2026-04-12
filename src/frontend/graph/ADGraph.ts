/**
 * ADGraph: Core graph data structure for AD visualization.
 *
 * Wraps graphology with AD-specific types and utilities.
 */

import Graph from "graphology";
import type {
  ADNodeAttributes,
  ADEdgeAttributes,
  ADEdgeType,
  RawADGraph,
  RawADNode,
  RawADEdge,
  ADNodeType,
} from "./types";
import { NODE_COLORS, DEFAULT_EDGE_SIZE, DEFAULT_EDGE_COLOR } from "./theme";
import { getNodeIcon, getNodeTypeColor, NODE_SIZE } from "./icons";

export type ADGraphType = Graph<ADNodeAttributes, ADEdgeAttributes>;

/** Create an empty AD graph */
export function createGraph(): ADGraphType {
  return new Graph<ADNodeAttributes, ADEdgeAttributes>({
    type: "directed",
    multi: true, // Allow multiple relationships between same nodes (e.g., MemberOf + GenericAll)
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

/** Convert a raw relationship to graphology attributes */
function rawEdgeToAttributes(relationship: RawADEdge): ADEdgeAttributes {
  const attrs: ADEdgeAttributes = {
    edgeType: relationship.type,
    label: relationship.label ?? relationship.type, // Use relationship type as label if not provided
    color: DEFAULT_EDGE_COLOR,
    size: DEFAULT_EDGE_SIZE,
    type: "tapered", // Use tapered for antialiased cone-shaped relationships
  };
  if (relationship.exploit_likelihood !== undefined) {
    attrs.properties = { exploit_likelihood: relationship.exploit_likelihood };
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

/** Add an relationship to the graph */
export function addEdge(graph: ADGraphType, relationship: RawADEdge): void {
  const edgeKey = `${relationship.source}-${relationship.type}-${relationship.target}`;
  if (!graph.hasEdge(edgeKey)) {
    graph.addEdgeWithKey(edgeKey, relationship.source, relationship.target, rawEdgeToAttributes(relationship));
  }
}

/** Load raw graph data into a graphology instance */
export function loadGraph(data: RawADGraph): ADGraphType {
  const graph = createGraph();

  for (const node of data.nodes) {
    addNode(graph, node);
  }

  for (const relationship of data.relationships) {
    // Only add relationship if both endpoints exist
    if (graph.hasNode(relationship.source) && graph.hasNode(relationship.target)) {
      addEdge(graph, relationship);
    }
  }

  // Collapse parallel edges: merge multiple edges between same (source, target) into one
  collapseParallelEdges(graph);

  // Assign curvature to remaining edges (at most 2 per node pair: one per direction)
  assignEdgeCurvatures(graph);

  return graph;
}

/** Collapse parallel edges between the same (source, target) into a single representative edge.
 *  Preserves direction: A->B and B->A are separate groups, yielding at most 2 edges per node pair. */
function collapseParallelEdges(graph: ADGraphType): void {
  // Group edges by directed pair (source, target)
  const directedGroups = new Map<string, Array<{ key: string; type: ADEdgeType }>>();

  graph.forEachEdge((edgeKey, attrs, source, target) => {
    const dirKey = `${source}\0${target}`;
    const group = directedGroups.get(dirKey) ?? [];
    group.push({ key: edgeKey, type: attrs.edgeType });
    directedGroups.set(dirKey, group);
  });

  for (const edges of directedGroups.values()) {
    if (edges.length <= 1) continue;

    // Keep the first edge as representative, drop the rest
    const representative = edges[0]!;
    const types = edges.map((e) => e.type);

    // Collect per-type exploit likelihoods before dropping non-representative edges
    const typeExploitLikelihoods: Record<string, number | undefined> = {};
    for (const edge of edges) {
      const props = graph.getEdgeAttribute(edge.key, "properties") as Record<string, unknown> | undefined;
      const el = props?.exploit_likelihood;
      typeExploitLikelihoods[edge.type] = typeof el === "number" ? el : undefined;
    }

    // Build a compact label: single type, or "Type +N"
    const label = types.length === 1 ? types[0]! : `${types[0]} +${types.length - 1}`;

    graph.setEdgeAttribute(representative.key, "collapsedTypes", types);
    graph.setEdgeAttribute(representative.key, "typeExploitLikelihoods", typeExploitLikelihoods);
    graph.setEdgeAttribute(representative.key, "label", label);

    // Drop all other edges from the graph (they're still in the backend)
    for (let i = 1; i < edges.length; i++) {
      graph.dropEdge(edges[i]!.key);
    }
  }
}

/** Assign curvature values to relationships to spread out parallel relationships */
function assignEdgeCurvatures(graph: ADGraphType): void {
  // Group relationships by their node pair (ignoring direction for grouping)
  const edgeGroups = new Map<string, Array<{ key: string; source: string; target: string }>>();

  graph.forEachEdge((edgeKey, _attrs, source, target) => {
    // Create a canonical key for the node pair (smaller id first)
    const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
    const group = edgeGroups.get(pairKey) ?? [];
    group.push({ key: edgeKey, source, target });
    edgeGroups.set(pairKey, group);
  });

  // Assign curvature to relationships in groups with multiple relationships
  for (const [pairKey, relationships] of edgeGroups.entries()) {
    if (relationships.length === 1) {
      // Single relationship: tapered relationship with antialiasing
      const relationship = relationships[0]!;
      graph.setEdgeAttribute(relationship.key, "type", "tapered");
      graph.setEdgeAttribute(relationship.key, "curvature", 0);
    } else {
      // Multiple relationships: separate by direction and spread with curvature
      const [canonicalSource] = pairKey.split("|");

      // Separate relationships by direction
      const forward = relationships.filter((e) => e.source === canonicalSource);
      const backward = relationships.filter((e) => e.source !== canonicalSource);

      // Assign curvatures: both directions get POSITIVE curvature
      // Since backward relationships go the opposite direction, positive curvature
      // on them will visually curve to the opposite side of forward relationships
      const assignCurvatures = (edgeList: typeof relationships) => {
        let i = 0;
        for (const relationship of edgeList) {
          const curvature = 0.2 + i * 0.15;
          graph.setEdgeAttribute(relationship.key, "type", "curvedArrow");
          graph.setEdgeAttribute(relationship.key, "curvature", curvature);
          graph.setEdgeAttribute(relationship.key, "size", 3);
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

/** Clear all nodes and relationships from the graph */
export function clearGraph(graph: ADGraphType): void {
  graph.clear();
}

/** Export graph to JSON (for debugging/persistence) */
export function exportGraph(graph: ADGraphType): RawADGraph {
  const nodes: RawADNode[] = [];
  const relationships: RawADEdge[] = [];

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
    const relationship: RawADEdge = {
      source,
      target,
      type: attrs.edgeType,
    };
    if (attrs.label) {
      relationship.label = attrs.label;
    }
    relationships.push(relationship);
  });

  return { nodes, relationships };
}
