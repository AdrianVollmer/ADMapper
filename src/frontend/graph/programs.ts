/**
 * Shared Sigma.js program factories and label drawing utilities.
 *
 * Centralises configuration that is used by both the main renderer
 * (ADGraphRenderer) and the magnifier lens, so changes only need to
 * be made in one place.
 */

import { createNodeImageProgram } from "@sigma/node-image";
import { createEdgeCurveProgram } from "@sigma/edge-curve";
import { getLabelParts } from "./label-visibility";
import { LABEL_COLOR } from "./theme";

/** Create the NodeImageProgram with shared high-quality settings. */
export function createSharedNodeImageProgram() {
  return createNodeImageProgram({
    size: { mode: "force", value: 512 },
    drawingMode: "background",
    colorAttribute: "color",
    imageAttribute: "image",
    padding: 0.12,
    keepWithinCircle: true,
  });
}

/** Create the CurvedArrowProgram with shared arrow-head settings. */
export function createSharedCurvedArrowProgram() {
  return createEdgeCurveProgram({
    arrowHead: {
      extremity: "target",
      lengthToThicknessRatio: 3.5,
      widenessToThicknessRatio: 3,
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as any;
}

/**
 * Draw a node label below the node, centered, with clear/blurred parts.
 *
 * This is the shared core used by both the main renderer's `drawLabel`
 * and the magnifier's `drawLensLabel`.  It intentionally does NOT draw
 * the collapse badge — callers that need it can do so after this returns.
 */
export function drawNodeLabel(
  context: CanvasRenderingContext2D,
  data: { label: string | null; x: number; y: number; size: number; color: string },
  settings: { labelSize: number; labelWeight: string; labelColor: { color?: string } },
  currentTheme: "light" | "dark"
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
