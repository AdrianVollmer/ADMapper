/**
 * Layout algorithms for AD graph positioning.
 *
 * All layouts are computed server-side using visgraph (Rust) via
 * POST /api/graph/layout. This provides consistent, high-performance
 * layout computation for graphs of any size.
 *
 * Supported algorithms:
 * - force: Fruchterman-Reingold force-directed layout
 * - hierarchical: Layered hierarchical layout
 * - circular: Nodes evenly distributed on a circle
 */

import type { ADGraphType } from "./ADGraph";
import { api } from "../api/client";
import type { LayoutResponse, ServerLayoutAlgorithm } from "../api/types";
import { getServerLayoutSettings } from "../components/settings";

/** Available layout algorithms */
export type LayoutType = "force" | "hierarchical" | "circular" | "grid" | "lattice";

export interface LayoutOptions {
  /** Layout algorithm to use */
  type?: LayoutType;
}

/** Map from LayoutType to the server's algorithm name */
const ALGORITHM_MAP: Record<LayoutType, ServerLayoutAlgorithm> = {
  force: "force_directed",
  hierarchical: "hierarchical",
  circular: "circular",
  grid: "grid",
  lattice: "lattice",
};

/**
 * Apply layout to the graph (async, server-side).
 *
 * Sends the graph structure to the backend, which uses visgraph (Rust) to
 * compute positions. Reads iterations, temperature, and direction from the
 * persisted layout settings.
 */
export async function applyLayoutAsync(
  graph: ADGraphType,
  options: LayoutOptions = {},
  onProgress?: (progress: number) => void
): Promise<void> {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";
  const algorithm = ALGORITHM_MAP[layoutType];
  const settings = getServerLayoutSettings();

  // Build node list, labels, and index map
  const nodes: string[] = [];
  const nodeLabels: string[] = [];
  const nodeIndexMap = new Map<string, number>();
  graph.forEachNode((nodeId, attrs) => {
    nodeIndexMap.set(nodeId, nodes.length);
    nodes.push(nodeId);
    nodeLabels.push(attrs.nodeType ?? "");
  });

  // Build edge list as index pairs
  const edges: [number, number][] = [];
  graph.forEachEdge((_, _attrs, source, target) => {
    const si = nodeIndexMap.get(source);
    const ti = nodeIndexMap.get(target);
    if (si !== undefined && ti !== undefined) {
      edges.push([si, ti]);
    }
  });

  const response = await api.post<LayoutResponse>("/api/graph/layout", {
    nodes,
    edges,
    algorithm,
    direction: settings.direction,
    iterations: settings.iterations,
    temperature: settings.temperature,
    node_labels: nodeLabels,
  });

  // Apply positions from server response
  for (const pos of response.positions) {
    if (graph.hasNode(pos.id)) {
      graph.setNodeAttribute(pos.id, "x", pos.x);
      graph.setNodeAttribute(pos.id, "y", pos.y);
    }
  }

  validateAndFixPositions(graph);
  if (onProgress) onProgress(1);
}

/**
 * Validate and fix invalid node positions.
 *
 * Handles several problematic cases:
 * 1. NaN or Infinity positions (from non-converging algorithms)
 * 2. Extreme position values (> 1e6) that cause rendering issues
 * 3. Degenerate layouts where all nodes are at the same point
 * 4. Very small position ranges that make the graph invisible
 *
 * @returns Number of nodes that had positions fixed
 */
export function validateAndFixPositions(graph: ADGraphType): number {
  if (graph.order === 0) return 0;

  // Thresholds for detecting problematic positions
  const MAX_COORDINATE = 1e6; // Positions larger than this cause issues
  const MIN_RANGE = 1; // If position range is smaller, graph is degenerate

  // First pass: collect positions and detect invalid ones
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  let validCount = 0;

  const invalidNodes: string[] = [];
  const allNodes: string[] = [];

  graph.forEachNode((nodeId, attrs) => {
    allNodes.push(nodeId);
    const x = attrs.x;
    const y = attrs.y;

    // Check for NaN, Infinity, or extreme values
    if (!Number.isFinite(x) || !Number.isFinite(y) || Math.abs(x) > MAX_COORDINATE || Math.abs(y) > MAX_COORDINATE) {
      invalidNodes.push(nodeId);
    } else {
      minX = Math.min(minX, x);
      maxX = Math.max(maxX, x);
      minY = Math.min(minY, y);
      maxY = Math.max(maxY, y);
      validCount++;
    }
  });

  // Check for degenerate layout (all nodes at same point or very small range)
  const rangeX = maxX - minX;
  const rangeY = maxY - minY;
  const isDegenerate = validCount > 0 && validCount === allNodes.length && rangeX < MIN_RANGE && rangeY < MIN_RANGE;

  // If layout is degenerate, reassign ALL nodes to a grid
  if (isDegenerate) {
    console.warn(
      `[layout] Degenerate layout detected: all ${allNodes.length} nodes in range (${rangeX.toFixed(2)}, ${rangeY.toFixed(2)}). Reassigning to grid.`
    );
    applyFallbackGridLayout(graph, allNodes);
    return allNodes.length;
  }

  // If we have invalid nodes, fix just those
  if (invalidNodes.length > 0) {
    console.warn(
      `[layout] Found ${invalidNodes.length} nodes with invalid positions (NaN/Infinity/extreme). Reassigning to grid.`
    );

    // If all nodes are invalid, use a default range
    if (validCount === 0) {
      applyFallbackGridLayout(graph, invalidNodes);
    } else {
      // Assign invalid nodes to a grid next to the valid bounds
      const margin = 100;
      const gridStartX = maxX + margin;
      const gridStartY = minY;
      const gridSpacing = 100;
      const columns = Math.ceil(Math.sqrt(invalidNodes.length));

      for (let i = 0; i < invalidNodes.length; i++) {
        const nodeId = invalidNodes[i]!;
        const col = i % columns;
        const row = Math.floor(i / columns);

        graph.setNodeAttribute(nodeId, "x", gridStartX + col * gridSpacing);
        graph.setNodeAttribute(nodeId, "y", gridStartY + row * gridSpacing);
      }
    }

    return invalidNodes.length;
  }

  return 0;
}

/**
 * Apply a simple grid layout as a fallback when other layouts fail.
 */
function applyFallbackGridLayout(graph: ADGraphType, nodes: string[]): void {
  const spacing = 100;
  const columns = Math.ceil(Math.sqrt(nodes.length));
  const gridWidth = (columns - 1) * spacing;
  const rows = Math.ceil(nodes.length / columns);
  const gridHeight = (rows - 1) * spacing;
  const startX = -gridWidth / 2;
  const startY = -gridHeight / 2;

  for (let i = 0; i < nodes.length; i++) {
    const nodeId = nodes[i]!;
    const col = i % columns;
    const row = Math.floor(i / columns);

    graph.setNodeAttribute(nodeId, "x", startX + col * spacing);
    graph.setNodeAttribute(nodeId, "y", startY + row * spacing);
  }
}
