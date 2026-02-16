/**
 * Export Component
 *
 * Export the currently visible graph as PNG, SVG, or JSON.
 */

import { getRenderer } from "./graph-view";

/** Export the graph as PNG */
export async function exportPNG(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    alert("No graph to export. Please load a graph first.");
    return;
  }

  try {
    // Get the sigma canvas layers
    const sigma = renderer.sigma;
    const container = sigma.getContainer();
    const canvases = container.querySelectorAll("canvas");

    if (canvases.length === 0) {
      alert("No graph canvas found.");
      return;
    }

    // Create a combined canvas
    const width = container.clientWidth;
    const height = container.clientHeight;
    const combinedCanvas = document.createElement("canvas");
    combinedCanvas.width = width * 2; // Higher resolution
    combinedCanvas.height = height * 2;
    const ctx = combinedCanvas.getContext("2d");

    if (!ctx) {
      alert("Failed to create export canvas.");
      return;
    }

    // Scale for higher resolution
    ctx.scale(2, 2);

    // Fill background
    ctx.fillStyle = "#111827"; // bg-gray-900
    ctx.fillRect(0, 0, width, height);

    // Draw each canvas layer
    for (const canvas of canvases) {
      ctx.drawImage(canvas, 0, 0, width, height);
    }

    // Convert to blob and download
    combinedCanvas.toBlob((blob) => {
      if (!blob) {
        alert("Failed to create image.");
        return;
      }
      downloadBlob(blob, "admapper-graph.png");
    }, "image/png");
  } catch (err) {
    console.error("PNG export failed:", err);
    alert("Failed to export PNG: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Export the graph as SVG */
export async function exportSVG(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    alert("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const sigma = renderer.sigma;
    const graph = sigma.getGraph();
    const camera = sigma.getCamera();
    const container = sigma.getContainer();
    const width = container.clientWidth;
    const height = container.clientHeight;

    // Build SVG manually from graph data
    let svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="#111827"/>
  <g transform="translate(${width / 2}, ${height / 2}) scale(${1 / camera.ratio})">
`;

    // Draw edges
    svg += '  <g class="edges">\n';
    graph.forEachEdge((_edge: string, _attrs: unknown, source: string, target: string) => {
      const sourcePos = graph.getNodeAttributes(source);
      const targetPos = graph.getNodeAttributes(target);
      if (sourcePos && targetPos) {
        const x1 = ((sourcePos.x || 0) - camera.x) * 100;
        const y1 = ((sourcePos.y || 0) - camera.y) * 100;
        const x2 = ((targetPos.x || 0) - camera.x) * 100;
        const y2 = ((targetPos.y || 0) - camera.y) * 100;
        svg += `    <line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="#4b5563" stroke-width="1"/>\n`;
      }
    });
    svg += "  </g>\n";

    // Draw nodes
    svg += '  <g class="nodes">\n';
    graph.forEachNode(
      (_node: string, attrs: { x?: number; y?: number; size?: number; color?: string; label?: string }) => {
        const x = ((attrs.x || 0) - camera.x) * 100;
        const y = ((attrs.y || 0) - camera.y) * 100;
        const size = (attrs.size || 5) * 2;
        const color = attrs.color || "#6b7280";
        svg += `    <circle cx="${x}" cy="${y}" r="${size}" fill="${color}"/>\n`;
        if (attrs.label) {
          svg += `    <text x="${x}" y="${y + size + 10}" text-anchor="middle" fill="#9ca3af" font-size="10">${escapeXml(attrs.label)}</text>\n`;
        }
      }
    );
    svg += "  </g>\n";

    svg += "</g>\n</svg>";

    const blob = new Blob([svg], { type: "image/svg+xml" });
    downloadBlob(blob, "admapper-graph.svg");
  } catch (err) {
    console.error("SVG export failed:", err);
    alert("Failed to export SVG: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Export the graph as JSON */
export async function exportJSON(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    alert("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const graph = renderer.sigma.getGraph();

    // Export nodes and edges
    const nodes: Array<{
      id: string;
      label: string;
      type: string;
      x: number;
      y: number;
      properties: Record<string, unknown>;
    }> = [];

    const edges: Array<{
      source: string;
      target: string;
      type: string;
    }> = [];

    graph.forEachNode(
      (
        node: string,
        attrs: { label?: string; nodeType?: string; x?: number; y?: number; properties?: Record<string, unknown> }
      ) => {
        nodes.push({
          id: node,
          label: attrs.label || node,
          type: attrs.nodeType || "Unknown",
          x: attrs.x || 0,
          y: attrs.y || 0,
          properties: attrs.properties || {},
        });
      }
    );

    graph.forEachEdge((_edge: string, attrs: { edgeType?: string }, source: string, target: string) => {
      edges.push({
        source,
        target,
        type: attrs.edgeType || "Unknown",
      });
    });

    const data = {
      exportedAt: new Date().toISOString(),
      nodeCount: nodes.length,
      edgeCount: edges.length,
      nodes,
      edges,
    };

    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    downloadBlob(blob, "admapper-graph.json");
  } catch (err) {
    console.error("JSON export failed:", err);
    alert("Failed to export JSON: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Download a blob as a file */
function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

/** Escape XML special characters */
function escapeXml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}
