/**
 * Collapsible Graph Functionality
 *
 * Provides functionality to collapse/expand nodes in large graphs.
 * - Nodes with many children can be collapsed to show only the parent
 */

import type { ADGraphType } from "./ADGraph";

// Module state: mutable Map tracking which nodes are collapsed and their hidden children.
const collapsedNodes = new Map<string, Set<string>>();

/** Check if a node is collapsed (has hidden children) */
export function isNodeCollapsed(nodeId: string): boolean {
  return collapsedNodes.has(nodeId);
}

/** Get the number of hidden children for a collapsed node */
export function getHiddenChildCount(nodeId: string): number {
  return collapsedNodes.get(nodeId)?.size ?? 0;
}

/** Get direct children of a node (outgoing relationships) */
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

/** Clear all collapse state */
export function clearCollapseState(): void {
  collapsedNodes.clear();
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