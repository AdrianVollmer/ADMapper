/**
 * Magnifying Lens for Graph View
 *
 * Creates a circular magnified view that follows the mouse cursor,
 * using a second Sigma.js renderer sharing the same Graphology graph.
 */

import Sigma from "sigma";
import { createNodeImageProgram } from "@sigma/node-image";
import { createEdgeCurveProgram } from "@sigma/edge-curve";
import { TaperedEdgeProgram } from "./TaperedEdgeProgram";
import { BACKGROUND_COLOR, LABEL_COLOR, DEFAULT_EDGE_COLOR } from "./theme";
import { getHiddenNodeIds } from "./collapse";
import { getLabelParts } from "./label-visibility";

/** Lens diameter in pixels */
const LENS_SIZE = 250;

/** How much more zoomed the lens is vs the main camera (lower = more zoomed) */
const LENS_ZOOM_FACTOR = 0.08;

/** Scale factor for node sizes inside the lens (smaller = more readable spacing) */
const LENS_NODE_SCALE = 0.2;

/** Scroll sensitivity: how much each wheel tick changes the zoom */
const WHEEL_ZOOM_SPEED = 0.15;

/** Minimum lens zoom factor (most zoomed in) */
const MIN_LENS_ZOOM = 0.005;

/** Maximum lens zoom factor (least zoomed in, must still be tighter than main) */
const MAX_LENS_ZOOM = 0.5;

// Module state
let lensContainer: HTMLDivElement | null = null;
let sigmaContainer: HTMLDivElement | null = null;
let lensSigma: Sigma | null = null;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let mainSigmaRef: Sigma<any, any, any> | null = null;
let active = false;
let currentTheme: "light" | "dark" = "dark";
let pendingFrame: number | null = null;
let lensZoomFactor = LENS_ZOOM_FACTOR;

// Event handler references for cleanup
let mouseMoveHandler: ((e: MouseEvent) => void) | null = null;
let mouseLeaveHandler: (() => void) | null = null;
let themeChangeHandler: ((e: Event) => void) | null = null;
let wheelHandler: ((e: WheelEvent) => void) | null = null;

/** Toggle the magnifier on/off */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function toggleMagnifier(mainSigma: Sigma<any, any, any>): void {
  if (active) {
    destroyMagnifier();
  } else {
    createMagnifier(mainSigma);
  }
}

/** Check if magnifier is currently active */
export function isMagnifierActive(): boolean {
  return active;
}

/** Destroy the magnifier and clean up all resources */
export function destroyMagnifier(): void {
  if (pendingFrame !== null) {
    cancelAnimationFrame(pendingFrame); // eslint-disable-line no-undef
    pendingFrame = null;
  }

  // Remove event listeners
  if (mainSigmaRef) {
    const container = mainSigmaRef.getContainer();
    if (mouseMoveHandler) container.removeEventListener("mousemove", mouseMoveHandler);
    if (mouseLeaveHandler) container.removeEventListener("mouseleave", mouseLeaveHandler);
    if (wheelHandler) container.removeEventListener("wheel", wheelHandler);
  }
  if (themeChangeHandler) {
    window.removeEventListener("themechange", themeChangeHandler);
  }

  mouseMoveHandler = null;
  mouseLeaveHandler = null;
  themeChangeHandler = null;
  wheelHandler = null;
  lensZoomFactor = LENS_ZOOM_FACTOR;

  if (lensSigma) {
    lensSigma.kill();
    lensSigma = null;
  }

  if (lensContainer) {
    lensContainer.remove();
    lensContainer = null;
  }
  sigmaContainer = null;

  mainSigmaRef = null;
  active = false;
}

/** Initialize the lens Sigma instance. Called once on the first mousemove,
 *  when the container is visible and has real dimensions for WebGL. */
function initLensSigma(): void {
  if (lensSigma || !mainSigmaRef || !sigmaContainer) return;

  const graph = mainSigmaRef.getGraph();

  const NodeImageProgram = createNodeImageProgram({
    size: { mode: "force", value: 512 },
    drawingMode: "background",
    colorAttribute: "color",
    imageAttribute: "image",
    padding: 0.12,
    keepWithinCircle: true,
  });

  const CurvedArrowProgram = createEdgeCurveProgram({
    arrowHead: {
      extremity: "target",
      lengthToThicknessRatio: 3.5,
      widenessToThicknessRatio: 3,
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any;

  // Simple label renderer for the lens (no collapse badges needed)
  function drawLensLabel(
    context: CanvasRenderingContext2D,
    data: { label: string | null; x: number; y: number; size: number; color: string },
    settings: { labelSize: number; labelWeight: string; labelColor: { color?: string } }
  ): void {
    const parts = getLabelParts(data.label);
    if (!parts) return;

    const size = settings.labelSize;
    const font = `${settings.labelWeight} ${size}px sans-serif`;
    const color = settings.labelColor.color ?? LABEL_COLOR[currentTheme];

    context.font = font;
    context.fillStyle = color;
    context.textBaseline = "top";

    const yOffset = data.size + 4;
    const y = data.y + yOffset;

    const clearWidth = parts.clear ? context.measureText(parts.clear).width : 0;
    const blurredWidth = parts.blurred ? context.measureText(parts.blurred).width : 0;
    const totalWidth = clearWidth + blurredWidth;
    let x = data.x - totalWidth / 2;

    if (parts.clear) {
      context.textAlign = "left";
      context.fillText(parts.clear, x, y);
      x += clearWidth;
    }
    if (parts.blurred) {
      context.textAlign = "left";
      context.fillText(parts.blurred, x, y);
    }
  }

  lensSigma = new Sigma(graph, sigmaContainer, {
    renderLabels: true,
    renderEdgeLabels: true,
    labelSize: 8,
    labelDensity: 1,
    labelGridCellSize: 50,
    labelRenderedSizeThreshold: 2,
    zIndex: true,
    defaultNodeColor: "#adb5bd",
    defaultEdgeColor: "#6c757d",
    labelColor: { color: LABEL_COLOR[currentTheme] },
    defaultDrawNodeLabel: drawLensLabel,
    defaultNodeType: "image",
    nodeProgramClasses: {
      image: NodeImageProgram,
    },
    defaultEdgeType: "tapered",
    edgeProgramClasses: {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      tapered: TaperedEdgeProgram as any,
      curvedArrow: CurvedArrowProgram,
    },
    minCameraRatio: 0.001,
    maxCameraRatio: 10,
    enableEdgeEvents: false,
    stagePadding: 0,
    // Always use screen-space sizing so zooming in spreads nodes apart
    // without enlarging them, letting the user see clustered detail.
    itemSizesReference: "screen",

    // Node reducer: hide collapsed children + shrink nodes so the zoomed-in
    // view emphasises spacing rather than making everything bigger.
    nodeReducer: (nodeId, data) => {
      const res: Record<string, unknown> = { ...data };
      const hiddenNodes = getHiddenNodeIds();
      if (hiddenNodes.has(nodeId)) {
        res.hidden = true;
      }
      const size = (data.size as number | undefined) ?? 3;
      res.size = size * LENS_NODE_SCALE;
      return res;
    },

    // Edge reducer: hide edges to hidden nodes
    edgeReducer: (relationship, data) => {
      const res: Record<string, unknown> = { ...data };
      const edgeSource = graph.source(relationship);
      const edgeTarget = graph.target(relationship);
      const hiddenNodes = getHiddenNodeIds();
      if (hiddenNodes.has(edgeSource) || hiddenNodes.has(edgeTarget)) {
        res.hidden = true;
      } else {
        res.color = DEFAULT_EDGE_COLOR;
      }
      return res;
    },
  });

  // User interaction is already blocked by pointer-events: none on the container.
  // Do NOT call camera.disable() -- it prevents programmatic setState() too.

  sigmaContainer.style.backgroundColor = BACKGROUND_COLOR[currentTheme];
}

/** Create the magnifier lens */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function createMagnifier(mainSigma: Sigma<any, any, any>): void {
  mainSigmaRef = mainSigma;
  const mainContainer = mainSigma.getContainer();

  // Detect current theme from background color
  const bg = mainContainer.style.backgroundColor;
  currentTheme = bg === BACKGROUND_COLOR.light ? "light" : "dark";

  // Create lens container (starts hidden)
  lensContainer = document.createElement("div");
  lensContainer.style.cssText = `
    position: fixed;
    width: ${LENS_SIZE}px;
    height: ${LENS_SIZE}px;
    border-radius: 50%;
    overflow: hidden;
    pointer-events: none;
    z-index: 1000;
    border: 2px solid rgba(255, 255, 255, 0.4);
    box-shadow: 0 0 20px rgba(0, 0, 0, 0.5);
    display: none;
  `;

  // Create inner container for sigma (must be rectangular for WebGL)
  sigmaContainer = document.createElement("div");
  sigmaContainer.style.cssText = `
    width: 100%;
    height: 100%;
  `;
  lensContainer.appendChild(sigmaContainer);
  document.body.appendChild(lensContainer);

  // Sigma is NOT created here -- it's deferred to the first mousemove so that
  // the container is visible and has real dimensions for the WebGL context.

  // Mouse move handler: position lens, lazy-init Sigma, sync camera
  mouseMoveHandler = (e: MouseEvent) => {
    if (!lensContainer || !mainSigmaRef) return;

    // Make the lens visible and position it
    lensContainer.style.left = `${e.clientX - LENS_SIZE / 2}px`;
    lensContainer.style.top = `${e.clientY - LENS_SIZE / 2}px`;
    lensContainer.style.display = "block";

    // Lazy-init: create Sigma on first mousemove when the container is visible
    if (!lensSigma) {
      initLensSigma();
      if (!lensSigma) return;
    }

    // Throttle camera updates to animation frames
    if (pendingFrame !== null) return;
    pendingFrame = requestAnimationFrame(() => {
      pendingFrame = null;
      if (!lensSigma || !mainSigmaRef || !lensContainer) return;
      // Skip if the container is hidden (no dimensions for WebGL)
      if (lensContainer.style.display === "none") return;

      // Convert mouse position to framed-graph coordinates (the coordinate
      // system the camera uses, NOT raw graph node coordinates).
      const rect = mainContainer.getBoundingClientRect();
      const viewportX = e.clientX - rect.left;
      const viewportY = e.clientY - rect.top;
      const graphPos = mainSigmaRef.viewportToFramedGraph({ x: viewportX, y: viewportY });

      // Set lens camera: same position, much tighter zoom
      const mainRatio = mainSigmaRef.getCamera().ratio;
      lensSigma.getCamera().setState({
        x: graphPos.x,
        y: graphPos.y,
        ratio: mainRatio * lensZoomFactor,
        angle: mainSigmaRef.getCamera().angle,
      });
    });
  };

  // Mouse wheel handler: adjust lens zoom level instead of zooming the main graph
  wheelHandler = (e: WheelEvent) => {
    if (!lensSigma || !mainSigmaRef || !lensContainer) return;
    // Only intercept when the lens is visible
    if (lensContainer.style.display === "none") return;

    e.preventDefault();
    e.stopPropagation();

    // deltaY > 0 = scroll down = zoom out (increase factor), < 0 = zoom in (decrease)
    const direction = Math.sign(e.deltaY);
    lensZoomFactor *= 1 + direction * WHEEL_ZOOM_SPEED;
    lensZoomFactor = Math.max(MIN_LENS_ZOOM, Math.min(MAX_LENS_ZOOM, lensZoomFactor));

    // Immediately update the lens camera with new zoom
    const mainCamera = mainSigmaRef.getCamera();
    const lensCamera = lensSigma.getCamera();
    lensCamera.setState({
      x: lensCamera.x,
      y: lensCamera.y,
      ratio: mainCamera.ratio * lensZoomFactor,
      angle: mainCamera.angle,
    });
  };

  // Hide lens when mouse leaves the graph
  mouseLeaveHandler = () => {
    if (pendingFrame !== null) {
      cancelAnimationFrame(pendingFrame); // eslint-disable-line no-undef
      pendingFrame = null;
    }
    if (lensContainer) {
      lensContainer.style.display = "none";
    }
  };

  // Theme sync
  themeChangeHandler = (event: Event) => {
    const customEvent = event as CustomEvent<{ theme: "light" | "dark" }>;
    currentTheme = customEvent.detail.theme;
    if (lensSigma) {
      const container = lensSigma.getContainer();
      container.style.backgroundColor = BACKGROUND_COLOR[currentTheme];
      lensSigma.setSetting("labelColor", { color: LABEL_COLOR[currentTheme] });
      lensSigma.refresh();
    }
  };

  mainContainer.addEventListener("mousemove", mouseMoveHandler);
  mainContainer.addEventListener("mouseleave", mouseLeaveHandler);
  mainContainer.addEventListener("wheel", wheelHandler, { passive: false });
  window.addEventListener("themechange", themeChangeHandler);

  active = true;
}
