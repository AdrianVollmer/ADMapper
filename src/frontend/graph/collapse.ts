/**
 * Collapsible Graph Functionality
 *
 * Provides functionality to collapse/expand nodes and edges in large graphs.
 * - Nodes with many children can be collapsed to show only the parent
 * - Multiple edges between same nodes are collapsed to show count
 */

import type { ADGraphType } from "./ADGraph";

/** Threshold for auto-collapsing (graph must have this many nodes) */
const AUTO_COLLAPSE_THRESHOLD = 100;

/** Minimum children to collapse a node */
const MIN_CHILDREN_TO_COLLAPSE = 5;

/** State for collapsed nodes - maps node ID to its hidden children */
const collapsedNodes = new Map<string, Set<string>>();

/** State for collapsed edges - maps edge pair key to collapsed edge keys */
const collapsedEdges = new Map<string, string[]>();

/** Check if a node is collapsed (has hidden children) */
export function isNodeCollapsed(nodeId: string): boolean {
  return collapsedNodes.has(nodeId);
}

/** Get the number of hidden children for a collapsed node */
export function getHiddenChildCount(nodeId: string): number {
  return collapsedNodes.get(nodeId)?.size ?? 0;
}

/** Check if a node is hidden (child of a collapsed parent) */
export function isNodeHidden(_graph: ADGraphType, nodeId: string): boolean {
  // Check if this node is in any collapsed set
  for (const hiddenSet of collapsedNodes.values()) {
    if (hiddenSet.has(nodeId)) {
      return true;
    }
  }
  return false;
}

/** Get direct children of a node (outgoing edges) */
function getDirectChildren(graph: ADGraphType, nodeId: string): string[] {
  const children: string[] = [];
  graph.forEachOutNeighbor(nodeId, (neighbor) => {
    children.push(neighbor);
  });
  return children;
}

/** Collapse a node - hide its direct children */
export function collapseNode(graph: ADGraphType, nodeId: string): void {
  if (!graph.hasNode(nodeId)) return;

  const children = getDirectChildren(graph, nodeId);
  if (children.length === 0) return;

  collapsedNodes.set(nodeId, new Set(children));
}

/** Expand a node - show its hidden children */
export function expandNode(nodeId: string): void {
  collapsedNodes.delete(nodeId);
}

/** Toggle collapse state of a node */
export function toggleNodeCollapse(graph: ADGraphType, nodeId: string): boolean {
  if (isNodeCollapsed(nodeId)) {
    expandNode(nodeId);
    return false;
  } else {
    collapseNode(graph, nodeId);
    return true;
  }
}

/** Auto-collapse nodes with many children when graph exceeds threshold */
export function autoCollapseGraph(graph: ADGraphType): void {
  // Clear previous state
  collapsedNodes.clear();

  // Only auto-collapse if graph is large enough
  if (graph.order < AUTO_COLLAPSE_THRESHOLD) {
    return;
  }

  // Find nodes with many children and collapse them
  graph.forEachNode((nodeId) => {
    const children = getDirectChildren(graph, nodeId);
    if (children.length >= MIN_CHILDREN_TO_COLLAPSE) {
      collapsedNodes.set(nodeId, new Set(children));
    }
  });
}

/** Clear all collapse state */
export function clearCollapseState(): void {
  collapsedNodes.clear();
  collapsedEdges.clear();
}

/** Get collapse info for a node (for badge display) */
export function getNodeCollapseInfo(
  graph: ADGraphType,
  nodeId: string
): { isCollapsed: boolean; hiddenCount: number; totalChildren: number } {
  const children = getDirectChildren(graph, nodeId);
  const hiddenChildren = collapsedNodes.get(nodeId);

  return {
    isCollapsed: hiddenChildren !== undefined,
    hiddenCount: hiddenChildren?.size ?? 0,
    totalChildren: children.length,
  };
}

/** Get all currently collapsed node IDs */
export function getCollapsedNodeIds(): string[] {
  return Array.from(collapsedNodes.keys());
}

/** Get all currently hidden node IDs */
export function getHiddenNodeIds(): Set<string> {
  const hidden = new Set<string>();
  for (const hiddenSet of collapsedNodes.values()) {
    for (const nodeId of hiddenSet) {
      hidden.add(nodeId);
    }
  }
  return hidden;
}

/** Collapse edges between same node pair */
export function collapseParallelEdges(graph: ADGraphType, source: string, target: string): void {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const edges: string[] = [];

  graph.forEachEdge(source, target, (edge) => {
    edges.push(edge);
  });
  graph.forEachEdge(target, source, (edge) => {
    edges.push(edge);
  });

  if (edges.length > 1) {
    collapsedEdges.set(pairKey, edges);
  }
}

/** Expand collapsed edges */
export function expandParallelEdges(source: string, target: string): void {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  collapsedEdges.delete(pairKey);
}

/** Toggle collapse state of parallel edges */
export function toggleEdgeCollapse(graph: ADGraphType, source: string, target: string): boolean {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  if (collapsedEdges.has(pairKey)) {
    expandParallelEdges(source, target);
    return false;
  } else {
    collapseParallelEdges(graph, source, target);
    return true;
  }
}

/** Check if edges between a node pair are collapsed */
export function areEdgesCollapsed(source: string, target: string): boolean {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  return collapsedEdges.has(pairKey);
}

/** Get collapsed edge info */
export function getCollapsedEdgeInfo(
  source: string,
  target: string
): { isCollapsed: boolean; edgeCount: number } | null {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const edges = collapsedEdges.get(pairKey);
  if (!edges) return null;

  return {
    isCollapsed: true,
    edgeCount: edges.length,
  };
}

/** Get the first visible edge for a collapsed edge group */
export function getVisibleEdgeForCollapsedGroup(source: string, target: string): string | null {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const edges = collapsedEdges.get(pairKey);
  if (!edges || edges.length === 0) return null;
  return edges[0] ?? null;
}

/** Check if an edge should be hidden (part of collapsed group but not the first) */
export function isEdgeHidden(edgeKey: string, source: string, target: string): boolean {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const edges = collapsedEdges.get(pairKey);
  if (!edges) return false;

  // Only the first edge is visible
  return edges.indexOf(edgeKey) > 0;
}
