/**
 * Layout algorithms for AD graph positioning.
 *
 * Supports:
 * - ForceAtlas2: Force-directed layout, good for exploring relationships
 * - Hierarchical: Left-to-right layered layout, relationships flow from sources to targets
 * - Grid: Simple grid arrangement
 * - Circular: Concentric circles based on node depth, sinks at center
 */

import forceAtlas2 from "graphology-layout-forceatlas2";
import noverlap from "graphology-layout-noverlap";
import dagre from "dagre";
import type { ADGraphType } from "./ADGraph";

/** Available layout algorithms */
export type LayoutType = "force" | "hierarchical" | "grid" | "circular" | "lattice";

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
  /** Settings for lattice layout */
  lattice?: LatticeSettings;
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

export interface LatticeSettings {
  /** Spacing between nodes (before rotation) */
  spacing?: number;
  /** Rotation angle in degrees (default: 26.57 - arctan(0.5) for optimal label separation) */
  angleDegrees?: number;
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
  /** Relationship weight influence */
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

const DEFAULT_LATTICE_SETTINGS: LatticeSettings = {
  spacing: 180,
  // arctan(0.5) ≈ 26.57° - creates a 2:1 aspect ratio tilt
  // This angle ensures nodes don't align horizontally or vertically,
  // giving labels maximum separation
  angleDegrees: 26.57,
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
    case "lattice":
      applyLatticeLayout(graph, options.lattice);
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

  const settings = mergeForceSettings(userForceSettings, options.settings);

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
 * It handles layer assignment, relationship crossing minimization, and coordinate assignment.
 *
 * Falls back to lattice layout if there are no relationships (single level).
 * Scales the layout wider for graphs with few levels.
 *
 * @returns true if hierarchical was applied, false if fell back to grid
 */
function applyHierarchicalLayout(graph: ADGraphType, options: HierarchicalSettings = {}): boolean {
  // If no relationships, fall back to lattice layout (all nodes would be on same level)
  if (graph.size === 0) {
    applyLatticeLayout(graph);
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

  // Default relationship label (required by dagre)
  g.setDefaultEdgeLabel(() => ({}));

  // Add nodes to dagre graph
  graph.forEachNode((nodeId) => {
    g.setNode(nodeId, { width: 40, height: 40 });
  });

  // Add relationships to dagre graph
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

  // Normalize and apply positions
  const normalized = normalizeGraphPositions(positions);
  for (const pos of normalized) {
    if (graph.hasNode(pos.nodeId)) {
      graph.setNodeAttribute(pos.nodeId, "x", pos.x);
      graph.setNodeAttribute(pos.nodeId, "y", pos.y);
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
 * Apply lattice layout - a tilted grid that prevents horizontal label collision.
 *
 * Creates a grid pattern rotated by ~26.57° (arctan(0.5)), which ensures
 * nodes don't align horizontally or vertically, giving labels maximum separation.
 * This is ideal for displaying isolated nodes (e.g., stale objects query results).
 */
function applyLatticeLayout(graph: ADGraphType, options: LatticeSettings = {}): void {
  const settings = { ...DEFAULT_LATTICE_SETTINGS, ...options };
  const { spacing, angleDegrees } = settings;

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
  const columns = Math.ceil(Math.sqrt(nodes.length));
  const rows = Math.ceil(nodes.length / columns);

  // Create grid positions centered at origin
  const gridWidth = (columns - 1) * spacing!;
  const gridHeight = (rows - 1) * spacing!;
  const startX = -gridWidth / 2;
  const startY = -gridHeight / 2;

  // Convert angle to radians
  const angle = (angleDegrees! * Math.PI) / 180;
  const cos = Math.cos(angle);
  const sin = Math.sin(angle);

  // Assign positions with rotation
  for (let i = 0; i < nodes.length; i++) {
    const col = i % columns;
    const row = Math.floor(i / columns);

    // Original grid position
    const x = startX + col * spacing!;
    const y = startY + row * spacing!;

    // Apply rotation around origin
    const rotatedX = x * cos - y * sin;
    const rotatedY = x * sin + y * cos;

    graph.setNodeAttribute(nodes[i], "x", rotatedX);
    graph.setNodeAttribute(nodes[i], "y", rotatedY);
  }
}

/**
 * Apply circular layout - arranges nodes in concentric circles based on depth.
 *
 * Sinks (nodes with no outgoing relationships) are at the center or innermost ring.
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
 * Compute layers based on distance from sink nodes (nodes with no outgoing relationships).
 * Sinks are at layer 0, nodes that connect directly to sinks are at layer 1, etc.
 */
function computeSinkBasedLayers(graph: ADGraphType): Map<string, number> {
  const layers = new Map<string, number>();

  // Find sink nodes (no outgoing relationships)
  const sinks: string[] = [];
  graph.forEachNode((nodeId) => {
    if (graph.outDegree(nodeId) === 0) {
      sinks.push(nodeId);
    }
  });

  // If no sinks (fully cyclic or all nodes have outgoing relationships), use nodes with minimum out-degree
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

  // BFS from sinks, traversing relationships in reverse
  const queue: string[] = [];
  for (const sink of sinks) {
    layers.set(sink, 0);
    queue.push(sink);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current)!;

    // Traverse incoming relationships (reverse direction)
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
  // If no relationships, fall back to lattice layout (all nodes would be on same level)
  if (graph.size === 0) {
    applyLatticeLayout(graph);
    return false;
  }

  const settings = { ...DEFAULT_HIERARCHICAL_SETTINGS, ...options };

  // Extract node and relationship data for the worker
  const nodes: Array<{ id: string }> = [];
  const relationships: Array<{ source: string; target: string }> = [];

  graph.forEachNode((nodeId) => {
    nodes.push({ id: nodeId });
  });

  graph.forEachEdge((_, _attrs, source, target) => {
    relationships.push({ source, target });
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
      relationships,
      settings: {
        layerSpacing: settings.layerSpacing,
        nodeSpacing: settings.nodeSpacing,
        direction: settings.direction,
      },
    });
  });

  // Normalize and apply positions
  const normalized = normalizeGraphPositions(positions);
  for (const pos of normalized) {
    if (graph.hasNode(pos.nodeId)) {
      graph.setNodeAttribute(pos.nodeId, "x", pos.x);
      graph.setNodeAttribute(pos.nodeId, "y", pos.y);
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
    validateAndFixPositions(graph);
    if (onProgress) onProgress(1);
    return;
  }

  // Grid, circular, and lattice are fast enough to run synchronously
  if (layoutType === "grid") {
    applyGridLayout(graph, options.grid);
    validateAndFixPositions(graph);
    if (onProgress) onProgress(1);
    return;
  }
  if (layoutType === "circular") {
    applyCircularLayout(graph, options.circular);
    validateAndFixPositions(graph);
    if (onProgress) onProgress(1);
    return;
  }
  if (layoutType === "lattice") {
    applyLatticeLayout(graph, options.lattice);
    validateAndFixPositions(graph);
    if (onProgress) onProgress(1);
    return;
  }

  // Force layout: run in chunks
  const totalIterations = options.iterations ?? getDefaultIterations(nodeCount);

  const settings = mergeForceSettings(userForceSettings, options.settings);

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

  // Validate positions - ForceAtlas2 can produce NaN/Infinity in edge cases
  validateAndFixPositions(graph);

  if (onProgress) {
    onProgress(1);
  }
}

/**
 * Normalize positions to fill a standard coordinate space centered at origin.
 *
 * Calculates bounds, computes per-axis scale factors, and centers the positions.
 * Target: fit within [-targetSize, targetSize] on each axis (default 800,
 * leaving 20% padding in a [-1000, 1000] viewport).
 *
 * This is the canonical implementation — used by both sync and async
 * hierarchical layout paths.
 */
export function normalizeGraphPositions(
  positions: Array<{ nodeId: string; x: number; y: number }>,
  targetSize = 800
): Array<{ nodeId: string; x: number; y: number }> {
  if (positions.length === 0) return positions;

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

  const currentWidth = maxX - minX || 1;
  const currentHeight = maxY - minY || 1;

  // Scale per-axis to fill the target bounds
  const scaleX = (targetSize * 2) / currentWidth;
  const scaleY = (targetSize * 2) / currentHeight;

  // Center around origin
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;

  return positions.map((pos) => ({
    nodeId: pos.nodeId,
    x: (pos.x - centerX) * scaleX,
    y: (pos.y - centerY) * scaleY,
  }));
}

/**
 * Merge force layout settings: defaults + user overrides + explicit options.
 *
 * Canonical implementation used by both sync (applyForceLayout) and async
 * (applyLayoutAsync) force layout paths.
 */
export function mergeForceSettings(
  userSettings: UserForceSettings | null,
  explicitSettings?: ForceAtlas2Settings
): ForceAtlas2Settings {
  let settings: ForceAtlas2Settings = { ...DEFAULT_FORCE_SETTINGS };
  if (userSettings) {
    settings = {
      ...settings,
      gravity: userSettings.gravity,
      scalingRatio: userSettings.scalingRatio,
      adjustSizes: userSettings.adjustSizes,
    };
  }
  if (explicitSettings) {
    settings = { ...settings, ...explicitSettings };
  }
  return settings;
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
