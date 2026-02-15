/**
 * Layout algorithms for AD graph positioning.
 *
 * Uses ForceAtlas2 for force-directed layout, which works well for
 * hierarchical AD structures.
 */

import forceAtlas2 from "graphology-layout-forceatlas2";
import type { ADGraphType } from "./ADGraph";

export interface LayoutOptions {
  /** Number of iterations to run */
  iterations?: number;
  /** Settings for ForceAtlas2 */
  settings?: ForceAtlas2Settings;
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
const DEFAULT_SETTINGS: ForceAtlas2Settings = {
  gravity: 1,
  scalingRatio: 2,
  slowDown: 1,
  barnesHutOptimize: true,
  barnesHutTheta: 0.5,
  adjustSizes: false,
  edgeWeightInfluence: 1,
  linLogMode: false,
  strongGravityMode: false,
};

/** Default iterations based on graph size */
function getDefaultIterations(nodeCount: number): number {
  if (nodeCount < 100) return 100;
  if (nodeCount < 1000) return 50;
  if (nodeCount < 5000) return 30;
  return 20; // Large graphs: fewer iterations, rely on Barnes-Hut
}

/**
 * Apply ForceAtlas2 layout to the graph.
 *
 * This modifies node positions in place.
 * For very large graphs (10k+), consider using the web worker version.
 */
export function applyLayout(graph: ADGraphType, options: LayoutOptions = {}): void {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const iterations = options.iterations ?? getDefaultIterations(nodeCount);
  const settings = { ...DEFAULT_SETTINGS, ...options.settings };

  forceAtlas2.assign(graph, {
    iterations,
    settings,
  });
}

/**
 * Apply layout with progress callback.
 *
 * Runs layout in chunks to allow UI updates.
 */
export async function applyLayoutAsync(
  graph: ADGraphType,
  options: LayoutOptions = {},
  onProgress?: (progress: number) => void
): Promise<void> {
  const nodeCount = graph.order;
  if (nodeCount === 0) return;

  const totalIterations = options.iterations ?? getDefaultIterations(nodeCount);
  const settings = { ...DEFAULT_SETTINGS, ...options.settings };

  // Run in chunks of 10 iterations
  const chunkSize = 10;
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
      onProgress(completed / totalIterations);
    }

    // Yield to allow UI updates
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

/**
 * Scale and center the graph positions.
 *
 * Useful after layout to fit the graph in a specific viewport.
 */
export function normalizePositions(
  graph: ADGraphType,
  width = 1000,
  height = 1000,
  padding = 50
): void {
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
