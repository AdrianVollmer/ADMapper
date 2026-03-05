/**
 * Collapsible Graph Functionality
 *
 * Provides functionality to collapse/expand nodes and relationships in large graphs.
 * - Nodes with many children can be collapsed to show only the parent
 * - Multiple relationships between same nodes are collapsed to show count
 */

import type { ADGraphType } from "./ADGraph";

/** State for collapsed nodes - maps node ID to its hidden children */
const collapsedNodes = new Map<string, Set<string>>();

/** State for collapsed relationships - maps relationship pair key to collapsed relationship keys */
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

/** Collapse relationships between same node pair */
export function collapseParallelEdges(graph: ADGraphType, source: string, target: string): void {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const relationships: string[] = [];

  graph.forEachEdge(source, target, (relationship) => {
    relationships.push(relationship);
  });
  graph.forEachEdge(target, source, (relationship) => {
    relationships.push(relationship);
  });

  if (relationships.length > 1) {
    collapsedEdges.set(pairKey, relationships);
  }
}

/** Expand collapsed relationships */
export function expandParallelEdges(source: string, target: string): void {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  collapsedEdges.delete(pairKey);
}

/** Toggle collapse state of parallel relationships */
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

/** Check if relationships between a node pair are collapsed */
export function areEdgesCollapsed(source: string, target: string): boolean {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  return collapsedEdges.has(pairKey);
}

/** Get collapsed relationship info */
export function getCollapsedEdgeInfo(
  source: string,
  target: string
): { isCollapsed: boolean; edgeCount: number } | null {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const relationships = collapsedEdges.get(pairKey);
  if (!relationships) return null;

  return {
    isCollapsed: true,
    edgeCount: relationships.length,
  };
}

/** Get the first visible relationship for a collapsed relationship group */
export function getVisibleEdgeForCollapsedGroup(source: string, target: string): string | null {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const relationships = collapsedEdges.get(pairKey);
  if (!relationships || relationships.length === 0) return null;
  return relationships[0] ?? null;
}

/** Check if an relationship should be hidden (part of collapsed group but not the first) */
export function isEdgeHidden(edgeKey: string, source: string, target: string): boolean {
  const pairKey = source < target ? `${source}|${target}` : `${target}|${source}`;
  const relationships = collapsedEdges.get(pairKey);
  if (!relationships) return false;

  // Only the first relationship is visible
  return relationships.indexOf(edgeKey) > 0;
}
