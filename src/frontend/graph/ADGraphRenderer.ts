/**
 * ADGraphRenderer: Sigma.js renderer for AD graphs.
 *
 * Handles WebGL rendering, user interaction, and visual state management.
 */

import Sigma from "sigma";
import { EdgeTriangleProgram } from "sigma/rendering";
import { createNodeImageProgram } from "@sigma/node-image";
import { createEdgeCurveProgram } from "@sigma/edge-curve";
import type { ADGraphType } from "./ADGraph";
import type { ADNodeAttributes, ADEdgeAttributes } from "./types";
import {
  HIGHLIGHT_COLORS,
  HIGHLIGHT_SIZE_MULTIPLIER,
  BACKGROUND_COLOR,
  LABEL_COLOR,
  DEFAULT_EDGE_COLOR,
} from "./theme";
import {
  isNodeCollapsed,
  getHiddenChildCount,
  getHiddenNodeIds,
  toggleNodeCollapse,
  isEdgeHidden,
  getCollapsedEdgeInfo,
} from "./collapse";
import { getLabelParts } from "./label-visibility";

/**
 * Check if canvas blur filter is supported.
 * Some webviews (e.g., Tauri on certain platforms) don't support it properly.
 */
const isBlurFilterSupported = (() => {
  try {
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) return false;

    // Try to set a blur filter
    ctx.filter = "blur(1px)";
    // Check if it was actually set (unsupported browsers may ignore it or set it to "none")
    return ctx.filter === "blur(1px)";
  } catch {
    return false;
  }
})();

export interface RendererOptions {
  /** Container element or selector */
  container: HTMLElement | string;
  /** Initial graph to render */
  graph: ADGraphType;
  /** Theme: light or dark */
  theme?: "light" | "dark";
  /** Enable hover highlighting */
  enableHover?: boolean;
  /** Callback when a node is clicked */
  onNodeClick?: (nodeId: string, attrs: ADNodeAttributes) => void;
  /** Callback when an edge is clicked */
  onEdgeClick?: (edgeId: string, attrs: ADEdgeAttributes, source: string, target: string) => void;
  /** Callback when the background is clicked */
  onBackgroundClick?: () => void;
  /** Callback when a node is hovered */
  onNodeHover?: (nodeId: string | null, attrs: ADNodeAttributes | null) => void;
  /** Callback when a node is double-clicked */
  onNodeDoubleClick?: (nodeId: string, attrs: ADNodeAttributes) => void;
}

export interface ADGraphRenderer {
  /** The underlying Sigma instance */
  sigma: Sigma<ADNodeAttributes, ADEdgeAttributes>;
  /** Currently hovered node */
  hoveredNode: string | null;
  /** Currently selected nodes */
  selectedNodes: Set<string>;
  /** Destroy the renderer and clean up resources */
  destroy: () => void;
  /** Refresh the rendering */
  refresh: () => void;
  /** Focus camera on a specific node */
  focusNode: (nodeId: string, animate?: boolean) => void;
  /** Reset camera to show all nodes */
  resetCamera: (animate?: boolean) => void;
  /** Select a node */
  selectNode: (nodeId: string) => void;
  /** Clear selection */
  clearSelection: () => void;
  /** Set theme */
  setTheme: (theme: "light" | "dark") => void;
  /** Highlight a path (list of node IDs) */
  highlightPath: (path: string[]) => void;
}

/** Create an AD graph renderer */
export function createRenderer(options: RendererOptions): ADGraphRenderer {
  const {
    container,
    graph,
    theme = "dark",
    enableHover = true,
    onNodeClick,
    onEdgeClick,
    onBackgroundClick,
    onNodeHover,
    onNodeDoubleClick,
  } = options;

  // Resolve container element
  const containerEl = typeof container === "string" ? document.querySelector<HTMLElement>(container) : container;

  if (!containerEl) {
    throw new Error(`Container not found: ${container}`);
  }

  // State
  let hoveredNode: string | null = null;
  let hoveredReachableEdges: Set<string> = new Set();
  const selectedNodes = new Set<string>();
  const highlightedPath = new Set<string>();
  const highlightedPathEdges = new Set<string>();
  let currentTheme = theme;
  let draggedNode: string | null = null;

  /** Compute all edges reachable via outgoing edges (transitive) */
  function computeReachableEdges(startNode: string): Set<string> {
    const visitedNodes = new Set<string>();
    const reachableEdges = new Set<string>();
    const queue = [startNode];

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (visitedNodes.has(current)) continue;
      visitedNodes.add(current);

      // Follow outgoing edges
      graph.forEachOutEdge(current, (edge, _attrs, _source, target) => {
        reachableEdges.add(edge);
        if (!visitedNodes.has(target)) {
          queue.push(target);
        }
      });
    }

    return reachableEdges;
  }

  // Custom label renderer: draws label below node, centered
  // Also draws collapse badge for collapsed nodes
  function drawLabel(
    context: CanvasRenderingContext2D,
    data: { label: string | null; x: number; y: number; size: number; color: string },
    settings: { labelSize: number; labelWeight: string; labelColor: { color?: string } },
    nodeId?: string
  ): void {
    const parts = getLabelParts(data.label);
    if (!parts) return;

    const size = settings.labelSize;
    const font = `${settings.labelWeight} ${size}px sans-serif`;
    const color = settings.labelColor.color ?? LABEL_COLOR[currentTheme];

    context.font = font;
    context.fillStyle = color;
    context.textBaseline = "top";

    // Position below the node with a small gap
    const yOffset = data.size + 4;
    const y = data.y + yOffset;

    // Calculate total width for centering
    const clearWidth = parts.clear ? context.measureText(parts.clear).width : 0;
    const blurredWidth = parts.blurred ? context.measureText(parts.blurred).width : 0;
    const totalWidth = clearWidth + blurredWidth;
    let x = data.x - totalWidth / 2;

    // Draw clear part (no blur)
    if (parts.clear) {
      context.textAlign = "left";
      context.fillText(parts.clear, x, y);
      x += clearWidth;
    }

    // Draw blurred part with blur filter (or opacity fallback)
    if (parts.blurred) {
      context.save();
      if (isBlurFilterSupported) {
        context.filter = "blur(3px)";
      } else {
        // Fallback: use reduced opacity for webviews that don't support blur
        context.globalAlpha = 0.4;
      }
      context.textAlign = "left";
      context.fillText(parts.blurred, x, y);
      context.restore();
    }

    // Draw collapse badge if node is collapsed
    if (nodeId && isNodeCollapsed(nodeId)) {
      const hiddenCount = getHiddenChildCount(nodeId);
      if (hiddenCount > 0) {
        const badgeText = hiddenCount > 99 ? "99+" : String(hiddenCount);
        const badgeSize = Math.max(12, data.size * 0.6);

        // Position badge at top-right of node
        const badgeX = data.x + data.size * 0.7;
        const badgeY = data.y - data.size * 0.7;

        // Draw badge background (red circle)
        context.beginPath();
        context.arc(badgeX, badgeY, badgeSize / 2, 0, Math.PI * 2);
        context.fillStyle = "#ef4444";
        context.fill();

        // Draw badge text
        context.font = `bold ${badgeSize * 0.7}px sans-serif`;
        context.fillStyle = "#ffffff";
        context.textAlign = "center";
        context.textBaseline = "middle";
        context.fillText(badgeText, badgeX, badgeY);
      }
    }
  }

  // Custom hover renderer: draws a glow effect behind the hovered node
  // Selected nodes get a red, tighter, more intense glow
  function drawNodeHover(
    context: CanvasRenderingContext2D,
    data: { x: number; y: number; size: number; color: string },
    _settings: unknown,
    nodeId?: string
  ): void {
    const isSelected = nodeId ? selectedNodes.has(nodeId) : false;

    if (isSelected) {
      // Selected: red, tight, intense glow
      const glowRadius = data.size * 1.8;
      const gradient = context.createRadialGradient(data.x, data.y, data.size * 0.8, data.x, data.y, glowRadius);
      gradient.addColorStop(0, "rgba(255, 50, 50, 1)");
      gradient.addColorStop(0.6, "rgba(255, 50, 50, 0.6)");
      gradient.addColorStop(1, "rgba(255, 50, 50, 0)");

      context.beginPath();
      context.arc(data.x, data.y, glowRadius, 0, Math.PI * 2);
      context.fillStyle = gradient;
      context.fill();
    } else {
      // Hovered: yellow, softer glow
      const glowRadius = data.size * 2;
      const gradient = context.createRadialGradient(data.x, data.y, data.size * 0.5, data.x, data.y, glowRadius);
      gradient.addColorStop(0, "rgba(255, 247, 0, 0.6)");
      gradient.addColorStop(0.5, "rgba(255, 247, 0, 0.2)");
      gradient.addColorStop(1, "rgba(255, 247, 0, 0)");

      context.beginPath();
      context.arc(data.x, data.y, glowRadius, 0, Math.PI * 2);
      context.fillStyle = gradient;
      context.fill();
    }
  }

  // Create node image program for rendering icons with high-quality settings
  // "background" mode draws the node color as a circle behind the icon
  const NodeImageProgram = createNodeImageProgram({
    size: { mode: "force", value: 512 }, // Higher resolution for crisp icons
    drawingMode: "background",
    colorAttribute: "color",
    imageAttribute: "image",
    padding: 0.12, // Slightly less padding for better icon visibility
    keepWithinCircle: true,
  });

  // Create curved edge program with smooth arrows
  const CurvedArrowProgram = createEdgeCurveProgram({
    arrowHead: {
      extremity: "target",
      lengthToThicknessRatio: 3.5,
      widenessToThicknessRatio: 3,
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any;

  // Create Sigma instance with high-quality rendering settings
  const sigma = new Sigma(graph, containerEl, {
    allowInvalidContainer: false,
    renderLabels: true,
    renderEdgeLabels: true,
    labelDensity: 0.08,
    labelGridCellSize: 80,
    labelRenderedSizeThreshold: 5,
    zIndex: true,
    defaultNodeColor: "#adb5bd",
    defaultEdgeColor: "#6c757d",
    labelColor: { color: LABEL_COLOR[theme] },
    defaultDrawNodeLabel: drawLabel,
    defaultDrawNodeHover: drawNodeHover,
    defaultNodeType: "image",
    nodeProgramClasses: {
      image: NodeImageProgram,
    },
    // Use curved arrows for better visual appearance
    defaultEdgeType: "curvedArrow",
    edgeProgramClasses: {
      triangle: EdgeTriangleProgram,
      curvedArrow: CurvedArrowProgram,
    },
    // Better performance settings
    minCameraRatio: 0.01,
    maxCameraRatio: 10,
    // Improved edge rendering
    enableEdgeEvents: true, // Enable for edge click handling
    // Smooth camera transitions
    stagePadding: 30,

    // Node reducer: bring hovered/selected/path nodes to front, hide collapsed children
    nodeReducer: (nodeId, data) => {
      const res: Record<string, unknown> = { ...data };

      // Hide nodes that are children of collapsed parents
      const hiddenNodes = getHiddenNodeIds();
      if (hiddenNodes.has(nodeId)) {
        res.hidden = true;
        return res;
      }

      // Path nodes get highlighted
      if (highlightedPath.has(nodeId)) {
        res.zIndex = 2;
        res.highlighted = true;
      }

      // Selected nodes get higher z-index and will show stronger glow via drawNodeHover
      if (selectedNodes.has(nodeId)) {
        res.zIndex = 2;
        res.highlighted = true;
      }

      // Bring hovered node to front
      if (nodeId === hoveredNode) {
        res.zIndex = 3;
      }

      // Collapsed nodes get highlighted to indicate they can be expanded
      if (isNodeCollapsed(nodeId)) {
        res.forceLabel = true; // Always show label for collapsed nodes
      }

      return res;
    },

    // Edge reducer: highlight path edges, transitive edges on hover, hide collapsed edges
    edgeReducer: (edge, data) => {
      const res: Record<string, unknown> = { ...data };

      // Get source and target from graph
      const edgeSource = graph.source(edge);
      const edgeTarget = graph.target(edge);

      // Hide edges connected to hidden nodes
      const hiddenNodes = getHiddenNodeIds();
      if (hiddenNodes.has(edgeSource) || hiddenNodes.has(edgeTarget)) {
        res.hidden = true;
        return res;
      }

      // Hide edges that are part of collapsed groups (except the first)
      if (isEdgeHidden(edge, edgeSource, edgeTarget)) {
        res.hidden = true;
        return res;
      }

      // Check if this edge is the visible one in a collapsed group
      const collapsedInfo = getCollapsedEdgeInfo(edgeSource, edgeTarget);
      if (collapsedInfo && collapsedInfo.edgeCount > 1) {
        // Show count in label
        res.label = `${collapsedInfo.edgeCount} edges`;
        res.forceLabel = true;
      }

      // Path edges get special highlight
      if (highlightedPathEdges.has(edge)) {
        res.color = "#22c55e"; // Green for path
        res.size = ((data.size as number | undefined) ?? 3) * 1.5;
        res.zIndex = 2;
      } else if (hoveredNode && hoveredReachableEdges.has(edge)) {
        // On hover: highlight edges reachable from hovered node
        res.color = HIGHLIGHT_COLORS.edge;
        res.size = ((data.size as number | undefined) ?? 3) * HIGHLIGHT_SIZE_MULTIPLIER;
        res.zIndex = 1;
      } else {
        // Default: uniform color (no dimming)
        res.color = DEFAULT_EDGE_COLOR;
      }

      return res;
    },
  });

  // Set initial background and label color
  updateThemeStyles(currentTheme);

  function updateThemeStyles(t: "light" | "dark") {
    containerEl!.style.backgroundColor = BACKGROUND_COLOR[t];
    sigma.setSetting("labelColor", { color: LABEL_COLOR[t] });
  }

  // Listen for theme changes from the app
  function handleThemeChange(event: Event) {
    const customEvent = event as CustomEvent<{ theme: "light" | "dark" }>;
    currentTheme = customEvent.detail.theme;
    updateThemeStyles(currentTheme);
    sigma.refresh();
  }
  window.addEventListener("themechange", handleThemeChange);

  // Handle WebGL context loss and restoration
  // Browsers may reclaim WebGL contexts under memory pressure or when too many exist
  const webglCanvases = Object.values(sigma.getCanvases());

  function handleContextLost(event: Event) {
    // Prevent default to signal we want the context restored
    event.preventDefault();
    console.warn("WebGL context lost, waiting for restoration...");
  }

  function handleContextRestored() {
    console.info("WebGL context restored, refreshing renderer...");
    sigma.refresh();
  }

  for (const canvas of webglCanvases) {
    canvas.addEventListener("webglcontextlost", handleContextLost);
    canvas.addEventListener("webglcontextrestored", handleContextRestored);
  }

  // Event handlers
  if (enableHover) {
    sigma.on("enterNode", (event) => {
      hoveredNode = event.node;
      // Compute transitive outgoing edges
      hoveredReachableEdges = computeReachableEdges(event.node);
      sigma.refresh();
      if (onNodeHover) {
        const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
        onNodeHover(event.node, attrs);
      }
    });

    sigma.on("leaveNode", () => {
      hoveredNode = null;
      hoveredReachableEdges = new Set();
      sigma.refresh();
      if (onNodeHover) {
        onNodeHover(null, null);
      }
    });
  }

  sigma.on("clickNode", (event) => {
    if (onNodeClick) {
      const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
      onNodeClick(event.node, attrs);
    }
  });

  sigma.on("clickEdge", (event) => {
    if (onEdgeClick) {
      const attrs = graph.getEdgeAttributes(event.edge) as ADEdgeAttributes;
      const source = graph.source(event.edge);
      const target = graph.target(event.edge);
      onEdgeClick(event.edge, attrs, source, target);
    }
  });

  // Double-click handler for toggling node collapse
  sigma.on("doubleClickNode", (event) => {
    // Toggle collapse state
    toggleNodeCollapse(graph, event.node);
    sigma.refresh();

    if (onNodeDoubleClick) {
      const attrs = graph.getNodeAttributes(event.node) as ADNodeAttributes;
      onNodeDoubleClick(event.node, attrs);
    }
  });

  sigma.on("clickStage", () => {
    if (onBackgroundClick) {
      onBackgroundClick();
    }
  });

  // Node dragging: start drag on mousedown over a node
  sigma.on("downNode", (event) => {
    draggedNode = event.node;
    // Disable camera drag while dragging a node
    sigma.getCamera().disable();
  });

  // Track mouse movement for dragging
  sigma.getMouseCaptor().on("mousemovebody", (event) => {
    if (!draggedNode) return;

    // Convert viewport coordinates to graph coordinates
    const pos = sigma.viewportToGraph(event);

    // Update node position in the graph
    graph.setNodeAttribute(draggedNode, "x", pos.x);
    graph.setNodeAttribute(draggedNode, "y", pos.y);
  });

  // End drag on mouseup
  sigma.getMouseCaptor().on("mouseup", () => {
    if (draggedNode) {
      draggedNode = null;
      // Re-enable camera drag
      sigma.getCamera().enable();
    }
  });

  // Also handle mouse leaving the container
  sigma.getMouseCaptor().on("mouseleave", () => {
    if (draggedNode) {
      draggedNode = null;
      sigma.getCamera().enable();
    }
  });

  // Public API
  const renderer: ADGraphRenderer = {
    sigma: sigma as unknown as Sigma<ADNodeAttributes, ADEdgeAttributes>,

    get hoveredNode() {
      return hoveredNode;
    },

    get selectedNodes() {
      return selectedNodes;
    },

    destroy() {
      window.removeEventListener("themechange", handleThemeChange);
      for (const canvas of webglCanvases) {
        canvas.removeEventListener("webglcontextlost", handleContextLost);
        canvas.removeEventListener("webglcontextrestored", handleContextRestored);
      }
      sigma.kill();
    },

    refresh() {
      sigma.refresh();
    },

    focusNode(nodeId: string, animate = true) {
      if (!graph.hasNode(nodeId)) return;

      const nodePosition = sigma.getNodeDisplayData(nodeId);
      if (!nodePosition) return;

      if (animate) {
        sigma.getCamera().animate({ x: nodePosition.x, y: nodePosition.y, ratio: 0.5 }, { duration: 300 });
      } else {
        sigma.getCamera().setState({ x: nodePosition.x, y: nodePosition.y, ratio: 0.5 });
      }
    },

    resetCamera(animate = true) {
      // Use ratio > 1 to zoom out slightly, creating padding around the graph
      // ratio 1.15 = content fills ~87% of viewport, leaving ~6-7% padding on each side
      const paddedRatio = 1.15;
      if (animate) {
        sigma.getCamera().animate({ x: 0.5, y: 0.5, ratio: paddedRatio, angle: 0 }, { duration: 300 });
      } else {
        sigma.getCamera().setState({ x: 0.5, y: 0.5, ratio: paddedRatio, angle: 0 });
      }
    },

    selectNode(nodeId: string) {
      selectedNodes.clear();
      selectedNodes.add(nodeId);
      sigma.refresh();
    },

    clearSelection() {
      selectedNodes.clear();
      highlightedPath.clear();
      highlightedPathEdges.clear();
      hoveredNode = null;
      hoveredReachableEdges = new Set();
      sigma.refresh();
    },

    setTheme(t: "light" | "dark") {
      currentTheme = t;
      updateThemeStyles(t);
      sigma.refresh();
    },

    highlightPath(path: string[]) {
      // Clear ALL highlight state first
      selectedNodes.clear();
      highlightedPath.clear();
      highlightedPathEdges.clear();
      hoveredNode = null;
      hoveredReachableEdges = new Set();

      if (path.length === 0) {
        sigma.refresh();
        return;
      }

      // Add path nodes to highlight set
      for (const nodeId of path) {
        highlightedPath.add(nodeId);
      }

      // Find and highlight edges between consecutive path nodes
      // Check both directions since the path might traverse edges in reverse
      for (let i = 0; i < path.length - 1; i++) {
        const source = path[i];
        const target = path[i + 1];
        // Try source -> target
        graph.forEachEdge(source, target, (edge) => {
          highlightedPathEdges.add(edge);
        });
        // Also try target -> source (in case edge is reversed)
        graph.forEachEdge(target, source, (edge) => {
          highlightedPathEdges.add(edge);
        });
      }

      // Refresh to apply highlight styles
      sigma.refresh();

      // Reset camera to show all nodes (the path graph should only contain path nodes)
      sigma.getCamera().animatedReset({ duration: 300 });
    },
  };

  return renderer;
}
