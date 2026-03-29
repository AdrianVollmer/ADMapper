/**
 * Collapsible Graph Functionality
 *
 * Provides functionality to collapse/expand nodes in large graphs.
 * Collapse direction is **incoming**: nodes with many incoming neighbors
 * (e.g., a Group with 200 MemberOf edges pointing at it) get collapsed,
 * hiding those incoming neighbors behind a badge.
 */

import type { ADGraphType } from "./ADGraph";

// Module state: mutable Map tracking which nodes are collapsed and their hidden incoming neighbors.
const collapsedNodes = new Map<string, Set<string>>();

/** Check if a node is collapsed (has hidden incoming neighbors) */
export function isNodeCollapsed(nodeId: string): boolean {
  return collapsedNodes.has(nodeId);
}

/** Get the number of hidden incoming neighbors for a collapsed node */
export function getHiddenChildCount(nodeId: string): number {
  return collapsedNodes.get(nodeId)?.size ?? 0;
}

/** Get incoming leaf neighbors of a node -- neighbors that point into this
 *  node and themselves have 0 incoming edges (terminal / leaf nodes). */
function getIncomingLeaves(graph: ADGraphType, nodeId: string): string[] {
  const leaves: string[] = [];
  graph.forEachInNeighbor(nodeId, (neighbor) => {
    if (graph.inDegree(neighbor) === 0) {
      leaves.push(neighbor);
    }
  });
  return leaves;
}

/** Collapse a node -- hide its incoming leaf neighbors.
 *  Nodes with no outgoing neighbors are not collapsible (pure sinks stay expanded).
 *  Only incoming neighbors that are themselves leaves (inDegree 0) get hidden. */
export function collapseNode(graph: ADGraphType, nodeId: string): void {
  if (!graph.hasNode(nodeId)) return;
  if (graph.outDegree(nodeId) === 0) return;

  const leaves = getIncomingLeaves(graph, nodeId);
  if (leaves.length === 0) return;

  collapsedNodes.set(nodeId, new Set(leaves));
}

/** Expand a node - show its hidden incoming neighbors */
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

/** Clear all collapse state */
export function clearCollapseState(): void {
  collapsedNodes.clear();
}

/** Get collapse info for a node (for badge display) */
export function getNodeCollapseInfo(
  graph: ADGraphType,
  nodeId: string
): { isCollapsed: boolean; hiddenCount: number; totalLeaves: number } {
  const leaves = getIncomingLeaves(graph, nodeId);
  const hiddenNeighbors = collapsedNodes.get(nodeId);

  return {
    isCollapsed: hiddenNeighbors !== undefined,
    hiddenCount: hiddenNeighbors?.size ?? 0,
    totalLeaves: leaves.length,
  };
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

/**
 * Auto-collapse nodes with more incoming connections than the threshold.
 * Returns the number of nodes that were collapsed.
 * A threshold of 0 is a no-op.
 */
/** Count incoming leaf neighbors (inDegree 0) for a node. */
function countIncomingLeaves(graph: ADGraphType, nodeId: string): number {
  let count = 0;
  graph.forEachInNeighbor(nodeId, (neighbor) => {
    if (graph.inDegree(neighbor) === 0) count++;
  });
  return count;
}

export function autoCollapseGraph(graph: ADGraphType, threshold: number): number {
  if (threshold <= 0) return 0;

  let count = 0;
  graph.forEachNode((nodeId) => {
    if (graph.outDegree(nodeId) > 0 && countIncomingLeaves(graph, nodeId) > threshold) {
      collapseNode(graph, nodeId);
      count++;
    }
  });
  return count;
}
