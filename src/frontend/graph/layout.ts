/**
 * Layout algorithms for AD graph positioning.
 *
 * Supports:
 * - ForceAtlas2: Force-directed layout, good for exploring relationships
 * - Hierarchical: Left-to-right layered layout, edges flow from sources to targets
 * - Grid: Simple grid arrangement
 * - Circular: Concentric circles based on node depth, sinks at center
 */

import forceAtlas2 from "graphology-layout-forceatlas2";
import noverlap from "graphology-layout-noverlap";
import dagre from "dagre";
import type { ADGraphType } from "./ADGraph";

/** Available layout algorithms */
export type LayoutType = "force" | "hierarchical" | "grid" | "circular";

export interface LayoutOptions {
  /** Layout algorithm to use */
  type?: LayoutType;
  /** Number of iterations to run (for force layout) */
  iterations?: number;
  /** Settings for ForceAtlas2 */
  settings?: ForceAtlas2Settings;
  /** Settings for hierarchical layout */
  hierarchical?: HierarchicalSettings;
  /** Settings for grid layout */
  grid?: GridSettings;
  /** Settings for circular layout */
  circular?: CircularSettings;
}

export interface HierarchicalSettings {
  /** Horizontal spacing between layers */
  layerSpacing?: number;
  /** Vertical spacing between nodes in the same layer */
  nodeSpacing?: number;
  /** Direction of the layout */
  direction?: "left-to-right" | "top-to-bottom";
}

export interface GridSettings {
  /** Horizontal spacing between nodes */
  columnSpacing?: number;
  /** Vertical spacing between nodes */
  rowSpacing?: number;
  /** Number of columns (if not set, computed from node count) */
  columns?: number;
}

export interface CircularSettings {
  /** Spacing between concentric rings */
  ringSpacing?: number;
  /** Minimum radius for first ring (when center is empty) */
  minRadius?: number;
}

export interface ForceAtlas2Settings {
  /** Attraction strength */
  gravity?: number;
  /** Scaling factor */
  scalingRatio?: number;
  /** Slow down factor */
  slowDown?: number;
  /** Prevent node overlap */
  barnesHutOptimize?: boolean;
  /** Theta for Barnes-Hut optimization */
  barnesHutTheta?: number;
  /** Adjust speed based on graph size */
  adjustSizes?: boolean;
  /** Edge weight influence */
  edgeWeightInfluence?: number;
  /** LinLog mode (better for clusters) */
  linLogMode?: boolean;
  /** Strong gravity mode */
  strongGravityMode?: boolean;
}

/** Default layout settings optimized for AD graphs */
const DEFAULT_FORCE_SETTINGS: ForceAtlas2Settings = {
  gravity: 0.5,
  scalingRatio: 10,
  slowDown: 1,
  barnesHutOptimize: true,
  barnesHutTheta: 0.5,
  adjustSizes: true,
  edgeWeightInfluence: 1,
  linLogMode: true, // Better for graphs with hub nodes (common in AD)
  strongGravityMode: false,
};

/** User-configurable force layout settings */
export interface UserForceSettings {
  gravity: number;
  scalingRatio: number;
  adjustSizes: boolean;
}

/** Current user force settings (loaded from settings API) */
let userForceSettings: UserForceSettings | null = null;

/** Set user force settings (called from settings component) */
export function setUserForceSettings(settings: UserForceSettings | null): void {
  userForceSettings = settings;
}

/** Get current user force settings */
export function getUserForceSettings(): UserForceSettings | null {
  return userForceSettings;
}

const DEFAULT_HIERARCHICAL_SETTINGS: HierarchicalSettings = {
  layerSpacing: 200,
  nodeSpacing: 80,
  direction: "left-to-right",
};

const DEFAULT_GRID_SETTINGS: GridSettings = {
  columnSpacing: 150,
  rowSpacing: 150,
};

const DEFAULT_CIRCULAR_SETTINGS: CircularSettings = {
  ringSpacing: 150,
  minRadius: 100,
};

/** Default iterations based on graph size */
function getDefaultIterations(nodeCount: number): number {
  // ForceAtlas2 needs many iterations to converge properly
  if (nodeCount < 50) return 500;
  if (nodeCount < 200) return 400;
  if (nodeCount < 500) return 300;
  if (nodeCount < 1000) return 200;
  if (nodeCount < 5000) return 150;
  return 100; // Large graphs: fewer iterations, rely on Barnes-Hut
}

/**
 * Apply layout to the graph.
 *
 * This modifies node positions in place.
 */
export function applyLayout(graph: ADGraphType, options: LayoutOptions = {}): void {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";

  switch (layoutType) {
    case "hierarchical":
      applyHierarchicalLayout(graph, options.hierarchical);
      break;
    case "grid":
      applyGridLayout(graph, options.grid);
      break;
    case "circular":
      applyCircularLayout(graph, options.circular);
      break;
    default:
      applyForceLayout(graph, options);
      break;
  }
}

/** Apply ForceAtlas2 force-directed layout */
function applyForceLayout(graph: ADGraphType, options: LayoutOptions): void {
  const nodeCount = graph.order;
  const iterations = options.iterations ?? getDefaultIterations(nodeCount);

  // Merge defaults with user settings (if any) and explicit options
  let settings = { ...DEFAULT_FORCE_SETTINGS };
  if (userForceSettings) {
    settings = {
      ...settings,
      gravity: userForceSettings.gravity,
      scalingRatio: userForceSettings.scalingRatio,
      adjustSizes: userForceSettings.adjustSizes,
    };
  }
  if (options.settings) {
    settings = { ...settings, ...options.settings };
  }

  // Run ForceAtlas2 to compute initial positions
  forceAtlas2.assign(graph, {
    iterations,
    settings,
  });

  // Post-process with noverlap to eliminate any remaining overlaps
  // This spreads out nodes that are still too close together
  noverlap.assign(graph, {
    maxIterations: 50,
    settings: {
      ratio: 1.5, // How much to expand the layout to remove overlaps
      margin: 10, // Minimum margin between nodes
    },
  });
}

/**
 * Apply hierarchical layout using dagre.
 *
 * Dagre is a JavaScript library for laying out directed graphs.
 * It handles layer assignment, edge crossing minimization, and coordinate assignment.
 *
 * Falls back to grid layout if there are no edges (single level).
 * Scales the layout wider for graphs with few levels.
 *
 * @returns true if hierarchical was applied, false if fell back to grid
 */
function applyHierarchicalLayout(graph: ADGraphType, options: HierarchicalSettings = {}): boolean {
  // If no edges, fall back to grid layout (all nodes would be on same level)
  if (graph.size === 0) {
    applyGridLayout(graph);
    return false;
  }

  const settings = { ...DEFAULT_HIERARCHICAL_SETTINGS, ...options };
  const { layerSpacing, nodeSpacing, direction } = settings;

  // Create a dagre graph
  const g = new dagre.graphlib.Graph();

  // Configure the graph layout
  g.setGraph({
    rankdir: direction === "left-to-right" ? "LR" : "TB",
    nodesep: nodeSpacing,
    ranksep: layerSpacing,
    marginx: 0,
    marginy: 0,
  });

  // Default edge label (required by dagre)
  g.setDefaultEdgeLabel(() => ({}));

  // Add nodes to dagre graph
  graph.forEachNode((nodeId) => {
    g.setNode(nodeId, { width: 40, height: 40 });
  });

  // Add edges to dagre graph
  graph.forEachEdge((_, _attrs, source, target) => {
    g.setEdge(source, target);
  });

  // Run dagre layout
  dagre.layout(g);

  // Collect positions from dagre layout
  const positions: Array<{ nodeId: string; x: number; y: number }> = [];

  g.nodes().forEach((nodeId) => {
    const node = g.node(nodeId);
    if (node) {
      positions.push({ nodeId, x: node.x, y: node.y });
    }
  });

  // Calculate bounds
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  for (const pos of positions) {
    minX = Math.min(minX, pos.x);
    minY = Math.min(minY, pos.y);
    maxX = Math.max(maxX, pos.x);
    maxY = Math.max(maxY, pos.y);
  }

  // Normalize positions to fill a standard coordinate space with 20% padding on all sides
  // Target: fit within [-800, 800] leaving 20% padding (so full range is [-1000, 1000])
  const targetSize = 800; // 80% of 1000, leaving 20% padding
  const currentWidth = maxX - minX || 1;
  const currentHeight = maxY - minY || 1;

  // Scale uniformly to fit within the target bounds (preserve aspect ratio)
  // But also ensure we USE the available space in both dimensions
  const scaleX = (targetSize * 2) / currentWidth;
  const scaleY = (targetSize * 2) / currentHeight;

  // Apply positions with scaling, centered around origin
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;

  for (const pos of positions) {
    if (graph.hasNode(pos.nodeId)) {
      graph.setNodeAttribute(pos.nodeId, "x", (pos.x - centerX) * scaleX);
      graph.setNodeAttribute(pos.nodeId, "y", (pos.y - centerY) * scaleY);
    }
  }

  return true;
}

/**
 * Apply grid layout - arranges nodes in a simple grid pattern.
 */
function applyGridLayout(graph: ADGraphType, options: GridSettings = {}): void {
  const settings = { ...DEFAULT_GRID_SETTINGS, ...options };
  const { columnSpacing, rowSpacing } = settings;

  // Get all nodes and sort for consistent ordering
  const nodes: string[] = [];
  graph.forEachNode((nodeId) => nodes.push(nodeId));
  nodes.sort((a, b) => {
    // Sort by type first, then by label
    const typeA = graph.getNodeAttribute(a, "nodeType") || "";
    const typeB = graph.getNodeAttribute(b, "nodeType") || "";
    if (typeA !== typeB) return typeA.localeCompare(typeB);
    const labelA = graph.getNodeAttribute(a, "label") || a;
    const labelB = graph.getNodeAttribute(b, "label") || b;
    return labelA.localeCompare(labelB);
  });

  // Compute grid dimensions
  const columns = settings.columns ?? Math.ceil(Math.sqrt(nodes.length));
  const rows = Math.ceil(nodes.length / columns);

  // Center the grid
  const gridWidth = (columns - 1) * columnSpacing!;
  const gridHeight = (rows - 1) * rowSpacing!;
  const startX = -gridWidth / 2;
  const startY = -gridHeight / 2;

  // Assign positions
  for (let i = 0; i < nodes.length; i++) {
    const col = i % columns;
    const row = Math.floor(i / columns);
    graph.setNodeAttribute(nodes[i], "x", startX + col * columnSpacing!);
    graph.setNodeAttribute(nodes[i], "y", startY + row * rowSpacing!);
  }
}

/**
 * Apply circular layout - arranges nodes in concentric circles based on depth.
 *
 * Sinks (nodes with no outgoing edges) are at the center or innermost ring.
 * If there's exactly one sink, it's placed at the center.
 * Otherwise, sinks form the first ring around an empty center.
 * Higher-depth nodes form outer rings, positioned at angular centroid of their children.
 */
function applyCircularLayout(graph: ADGraphType, options: CircularSettings = {}): void {
  const settings = { ...DEFAULT_CIRCULAR_SETTINGS, ...options };
  const { ringSpacing, minRadius } = settings;

  // Compute layers from sinks (reverse of hierarchical)
  const nodeLayers = computeSinkBasedLayers(graph);

  // Group nodes by layer
  const layerGroups = new Map<number, string[]>();
  let maxLayer = 0;
  for (const [nodeId, layer] of nodeLayers.entries()) {
    const group = layerGroups.get(layer) ?? [];
    group.push(nodeId);
    layerGroups.set(layer, group);
    maxLayer = Math.max(maxLayer, layer);
  }

  // Initial ordering - sort by degree for first pass
  for (const nodes of layerGroups.values()) {
    nodes.sort((a, b) => {
      const degDiff = graph.degree(b) - graph.degree(a);
      return degDiff !== 0 ? degDiff : a.localeCompare(b);
    });
  }

  // Check if there's exactly one sink (layer 0)
  const sinks = layerGroups.get(0) ?? [];
  const singleCenterNode = sinks.length === 1;

  // Assign initial angles to all nodes
  const nodeAngles = new Map<string, number>();

  if (singleCenterNode) {
    // Center node has no angle (it's at origin)
    nodeAngles.set(sinks[0]!, 0);

    // Assign initial angles to other layers
    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      assignAngles(nodes, nodeAngles);
    }
  } else {
    // Assign initial angles to all layers
    for (let layer = 0; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      assignAngles(nodes, nodeAngles);
    }
  }

  // Barycenter iterations: position outer nodes at angular centroid of their children
  const iterations = 4;
  for (let iter = 0; iter < iterations; iter++) {
    // Work from outer layers inward - position parents based on children
    for (let layer = maxLayer; layer >= (singleCenterNode ? 1 : 0); layer--) {
      const nodes = layerGroups.get(layer) ?? [];
      reorderByAngularBarycenter(graph, nodes, nodeAngles, nodeLayers);
      // Reassign angles after reordering
      assignAngles(nodes, nodeAngles);
    }
  }

  // Place nodes at final positions
  if (singleCenterNode) {
    graph.setNodeAttribute(sinks[0], "x", 0);
    graph.setNodeAttribute(sinks[0], "y", 0);

    for (let layer = 1; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;
      const radius = minRadius! + (layer - 1) * ringSpacing!;
      placeNodesOnCircleWithAngles(graph, nodes, radius, nodeAngles);
    }
  } else {
    for (let layer = 0; layer <= maxLayer; layer++) {
      const nodes = layerGroups.get(layer) ?? [];
      if (nodes.length === 0) continue;
      const radius = minRadius! + layer * ringSpacing!;
      placeNodesOnCircleWithAngles(graph, nodes, radius, nodeAngles);
    }
  }
}

/**
 * Assign evenly distributed angles to nodes.
 */
function assignAngles(nodes: string[], nodeAngles: Map<string, number>): void {
  const angleStep = (2 * Math.PI) / nodes.length;
  const startAngle = -Math.PI / 2;

  for (let i = 0; i < nodes.length; i++) {
    nodeAngles.set(nodes[i]!, startAngle + i * angleStep);
  }
}

/**
 * Reorder nodes based on angular barycenter of their children (outgoing neighbors in inner layers).
 */
function reorderByAngularBarycenter(
  graph: ADGraphType,
  nodes: string[],
  angles: Map<string, number>,
  nodeLayers: Map<string, number>
): void {
  if (nodes.length === 0) return;
  const currentLayer = nodeLayers.get(nodes[0]!) ?? 0;

  // Compute angular barycenter for each node based on children (nodes in inner layers)
  const barycenters: { node: string; angle: number; hasChildren: boolean }[] = [];

  for (const node of nodes) {
    const childAngles: number[] = [];

    // Get outgoing neighbors that are in inner layers (closer to center)
    graph.forEachOutNeighbor(node, (neighbor) => {
      const neighborLayer = nodeLayers.get(neighbor) ?? 0;
      if (neighborLayer < currentLayer) {
        const angle = angles.get(neighbor);
        if (angle !== undefined) {
          childAngles.push(angle);
        }
      }
    });

    if (childAngles.length > 0) {
      // Compute circular mean angle
      const meanAngle = circularMean(childAngles);
      barycenters.push({ node, angle: meanAngle, hasChildren: true });
    } else {
      // Keep original angle for nodes without children
      barycenters.push({ node, angle: angles.get(node) ?? 0, hasChildren: false });
    }
  }

  // Sort by angle
  barycenters.sort((a, b) => {
    // Normalize angles to [0, 2π) for sorting
    const angleA = ((a.angle % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
    const angleB = ((b.angle % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
    return angleA - angleB;
  });

  // Update the nodes array in place
  nodes.length = 0;
  for (const { node } of barycenters) {
    nodes.push(node);
  }
}

/**
 * Compute circular mean of angles.
 */
function circularMean(angles: number[]): number {
  let sinSum = 0;
  let cosSum = 0;

  for (const angle of angles) {
    sinSum += Math.sin(angle);
    cosSum += Math.cos(angle);
  }

  return Math.atan2(sinSum / angles.length, cosSum / angles.length);
}

/**
 * Place nodes on a circle using precomputed angles.
 */
function placeNodesOnCircleWithAngles(
  graph: ADGraphType,
  nodes: string[],
  radius: number,
  angles: Map<string, number>
): void {
  for (const node of nodes) {
    const angle = angles.get(node) ?? 0;
    const x = radius * Math.cos(angle);
    const y = radius * Math.sin(angle);
    graph.setNodeAttribute(node, "x", x);
    graph.setNodeAttribute(node, "y", y);
  }
}

/**
 * Compute layers based on distance from sink nodes (nodes with no outgoing edges).
 * Sinks are at layer 0, nodes that connect directly to sinks are at layer 1, etc.
 */
function computeSinkBasedLayers(graph: ADGraphType): Map<string, number> {
  const layers = new Map<string, number>();

  // Find sink nodes (no outgoing edges)
  const sinks: string[] = [];
  graph.forEachNode((nodeId) => {
    if (graph.outDegree(nodeId) === 0) {
      sinks.push(nodeId);
    }
  });

  // If no sinks (fully cyclic or all nodes have outgoing edges), use nodes with minimum out-degree
  if (sinks.length === 0) {
    let minOutDegree = Infinity;
    graph.forEachNode((nodeId) => {
      const outDeg = graph.outDegree(nodeId);
      if (outDeg < minOutDegree) {
        minOutDegree = outDeg;
        sinks.length = 0;
        sinks.push(nodeId);
      } else if (outDeg === minOutDegree) {
        sinks.push(nodeId);
      }
    });
  }

  // BFS from sinks, traversing edges in reverse
  const queue: string[] = [];
  for (const sink of sinks) {
    layers.set(sink, 0);
    queue.push(sink);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current)!;

    // Traverse incoming edges (reverse direction)
    graph.forEachInNeighbor(current, (neighbor) => {
      if (!layers.has(neighbor)) {
        layers.set(neighbor, currentLayer + 1);
        queue.push(neighbor);
      }
    });
  }

  // Handle any remaining unvisited nodes (isolated)
  graph.forEachNode((nodeId) => {
    if (!layers.has(nodeId)) {
      layers.set(nodeId, 0);
    }
  });

  return layers;
}

/**
 * Apply hierarchical layout asynchronously using a Web Worker.
 *
 * Runs dagre in a separate thread to avoid blocking the UI.
 */
async function applyHierarchicalLayoutAsync(graph: ADGraphType, options: HierarchicalSettings = {}): Promise<boolean> {
  // If no edges, fall back to grid layout (all nodes would be on same level)
  if (graph.size === 0) {
    applyGridLayout(graph);
    return false;
  }

  const settings = { ...DEFAULT_HIERARCHICAL_SETTINGS, ...options };

  // Extract node and edge data for the worker
  const nodes: Array<{ id: string }> = [];
  const edges: Array<{ source: string; target: string }> = [];

  graph.forEachNode((nodeId) => {
    nodes.push({ id: nodeId });
  });

  graph.forEachEdge((_, _attrs, source, target) => {
    edges.push({ source, target });
  });

  // Create worker and run layout
  const worker = new Worker(new URL("./layout-worker.ts", import.meta.url), { type: "module" });

  const positions = await new Promise<Array<{ nodeId: string; x: number; y: number }>>((resolve, reject) => {
    worker.onmessage = (event) => {
      resolve(event.data.positions);
      worker.terminate();
    };
    worker.onerror = (error) => {
      reject(error);
      worker.terminate();
    };

    worker.postMessage({
      nodes,
      edges,
      settings: {
        layerSpacing: settings.layerSpacing,
        nodeSpacing: settings.nodeSpacing,
        direction: settings.direction,
      },
    });
  });

  // Calculate bounds
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  for (const pos of positions) {
    minX = Math.min(minX, pos.x);
    minY = Math.min(minY, pos.y);
    maxX = Math.max(maxX, pos.x);
    maxY = Math.max(maxY, pos.y);
  }

  // Normalize positions to fill a standard coordinate space with 20% padding on all sides
  const targetSize = 800;
  const currentWidth = maxX - minX || 1;
  const currentHeight = maxY - minY || 1;

  const scaleX = (targetSize * 2) / currentWidth;
  const scaleY = (targetSize * 2) / currentHeight;

  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;

  for (const pos of positions) {
    if (graph.hasNode(pos.nodeId)) {
      graph.setNodeAttribute(pos.nodeId, "x", (pos.x - centerX) * scaleX);
      graph.setNodeAttribute(pos.nodeId, "y", (pos.y - centerY) * scaleY);
    }
  }

  return true;
}

/**
 * Apply layout with progress callback.
 *
 * Runs layout asynchronously to allow UI updates.
 * Hierarchical uses a Web Worker; force layout runs in chunks.
 */
export async function applyLayoutAsync(
  graph: ADGraphType,
  options: LayoutOptions = {},
  onProgress?: (progress: number) => void
): Promise<void> {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const layoutType = options.type ?? "force";

  // Hierarchical layout runs in a Web Worker
  if (layoutType === "hierarchical") {
    await applyHierarchicalLayoutAsync(graph, options.hierarchical);
    if (onProgress) onProgress(1);
    return;
  }

  // Grid and circular are fast enough to run synchronously
  if (layoutType === "grid") {
    applyGridLayout(graph, options.grid);
    if (onProgress) onProgress(1);
    return;
  }
  if (layoutType === "circular") {
    applyCircularLayout(graph, options.circular);
    if (onProgress) onProgress(1);
    return;
  }

  // Force layout: run in chunks
  const totalIterations = options.iterations ?? getDefaultIterations(nodeCount);

  // Merge defaults with user settings (if any) and explicit options
  let settings = { ...DEFAULT_FORCE_SETTINGS };
  if (userForceSettings) {
    settings = {
      ...settings,
      gravity: userForceSettings.gravity,
      scalingRatio: userForceSettings.scalingRatio,
      adjustSizes: userForceSettings.adjustSizes,
    };
  }
  if (options.settings) {
    settings = { ...settings, ...options.settings };
  }

  const chunkSize = 20;
  let completed = 0;

  while (completed < totalIterations) {
    const remaining = totalIterations - completed;
    const iterations = Math.min(chunkSize, remaining);

    forceAtlas2.assign(graph, {
      iterations,
      settings,
    });

    completed += iterations;

    if (onProgress) {
      // Reserve last 5% for noverlap
      onProgress((completed / totalIterations) * 0.95);
    }

    // Yield to allow UI updates
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  // Post-process with noverlap to eliminate any remaining overlaps
  noverlap.assign(graph, {
    maxIterations: 50,
    settings: {
      ratio: 1.5,
      margin: 10,
    },
  });

  if (onProgress) {
    onProgress(1);
  }
}

/**
 * Scale and center the graph positions.
 *
 * Useful after layout to fit the graph in a specific viewport.
 */
export function normalizePositions(graph: ADGraphType, width = 1000, height = 1000, padding = 50): void {
  if (graph.order === 0) return;

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;

  // Find bounds
  graph.forEachNode((_, attrs) => {
    minX = Math.min(minX, attrs.x);
    maxX = Math.max(maxX, attrs.x);
    minY = Math.min(minY, attrs.y);
    maxY = Math.max(maxY, attrs.y);
  });

  const currentWidth = maxX - minX || 1;
  const currentHeight = maxY - minY || 1;

  const targetWidth = width - padding * 2;
  const targetHeight = height - padding * 2;

  const scale = Math.min(targetWidth / currentWidth, targetHeight / currentHeight);

  const offsetX = padding + (targetWidth - currentWidth * scale) / 2 - minX * scale;
  const offsetY = padding + (targetHeight - currentHeight * scale) / 2 - minY * scale;

  // Apply transformation
  graph.forEachNode((nodeId, attrs) => {
    graph.setNodeAttribute(nodeId, "x", attrs.x * scale + offsetX);
    graph.setNodeAttribute(nodeId, "y", attrs.y * scale + offsetY);
  });
}
