/**
 * Export Component
 *
 * Export the currently visible graph as PNG, SVG, JSON, or interactive HTML.
 */

import { getRenderer } from "./graph-view";
import { showError, showInfo, showSuccess } from "../utils/notifications";
import { api, isRunningInTauri } from "../api/client";
import htmlTemplate from "../export-graph-template.html?raw";

/** Export the graph as PNG */
export async function exportPNG(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    showInfo("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const sigma = renderer.sigma;
    const container = sigma.getContainer();

    // Force a synchronous render to ensure canvas content is fresh
    sigma.refresh();

    // Get all canvas layers (Sigma uses multiple: relationships, nodes, labels, hovers, etc.)
    const canvases = container.querySelectorAll("canvas");

    if (canvases.length === 0) {
      showError("No graph canvas found.");
      return;
    }

    // Create a combined canvas at 2x resolution for crisp output
    const width = container.clientWidth;
    const height = container.clientHeight;
    const combinedCanvas = document.createElement("canvas");
    combinedCanvas.width = width * 2; // Higher resolution
    combinedCanvas.height = height * 2;
    const ctx = combinedCanvas.getContext("2d");

    if (!ctx) {
      showError("Failed to create export canvas.");
      return;
    }

    // Scale for higher resolution
    ctx.scale(2, 2);

    // Fill background (respecting current theme)
    const isDark = document.documentElement.classList.contains("dark");
    ctx.fillStyle = isDark ? "#111827" : "#f9fafb"; // bg-gray-900 / bg-gray-50
    ctx.fillRect(0, 0, width, height);

    // Draw each canvas layer in order (they stack properly)
    for (const canvas of canvases) {
      ctx.drawImage(canvas, 0, 0, width, height);
    }

    // Convert to blob and download
    combinedCanvas.toBlob((blob) => {
      if (!blob) {
        showError("Failed to create image.");
        return;
      }
      downloadBlob(blob, "admapper-graph.png");
    }, "image/png");
  } catch (err) {
    console.error("PNG export failed:", err);
    showError("Failed to export PNG: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Export the graph as SVG */
export async function exportSVG(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    showInfo("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const sigma = renderer.sigma;
    const graph = sigma.getGraph();
    const container = sigma.getContainer();
    const width = container.clientWidth;
    const height = container.clientHeight;

    // Determine background color based on theme
    const isDark = document.documentElement.classList.contains("dark");
    const bgColor = isDark ? "#111827" : "#f9fafb";
    const labelColor = isDark ? "#9ca3af" : "#374151";
    const edgeColor = isDark ? "#4b5563" : "#9ca3af";

    // Build SVG manually from graph data using viewport coordinates
    let svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="${bgColor}"/>
`;

    // Draw relationships first (so they appear behind nodes)
    svg += '  <g class="relationships">\n';
    graph.forEachEdge((_edge: string, attrs: { color?: string; size?: number }, source: string, target: string) => {
      // Use getNodeDisplayData to get viewport coordinates (already transformed by camera)
      const sourceDisplay = sigma.getNodeDisplayData(source);
      const targetDisplay = sigma.getNodeDisplayData(target);
      if (sourceDisplay && targetDisplay) {
        const color = attrs.color ?? edgeColor;
        const strokeWidth = attrs.size ?? 1;
        svg += `    <line x1="${sourceDisplay.x}" y1="${sourceDisplay.y}" x2="${targetDisplay.x}" y2="${targetDisplay.y}" stroke="${color}" stroke-width="${strokeWidth}"/>\n`;
      }
    });
    svg += "  </g>\n";

    // Draw nodes
    svg += '  <g class="nodes">\n';
    graph.forEachNode((node: string) => {
      // Use getNodeDisplayData to get viewport coordinates and display properties
      const display = sigma.getNodeDisplayData(node);
      if (display && !display.hidden) {
        const color = display.color ?? "#6b7280";
        const size = display.size ?? 5;
        svg += `    <circle cx="${display.x}" cy="${display.y}" r="${size}" fill="${color}"/>\n`;
        // Label is in display data
        const label = display.label;
        if (label) {
          svg += `    <text x="${display.x}" y="${display.y + size + 12}" text-anchor="middle" fill="${labelColor}" font-size="10" font-family="sans-serif">${escapeXml(label)}</text>\n`;
        }
      }
    });
    svg += "  </g>\n";

    svg += "</svg>";

    const blob = new Blob([svg], { type: "image/svg+xml" });
    downloadBlob(blob, "admapper-graph.svg");
  } catch (err) {
    console.error("SVG export failed:", err);
    showError("Failed to export SVG: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Export the graph as JSON */
export async function exportJSON(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    showInfo("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const graph = renderer.sigma.getGraph();

    // Export nodes and relationships
    const nodes: Array<{
      id: string;
      label: string;
      type: string;
      x: number;
      y: number;
      properties: Record<string, unknown>;
    }> = [];

    const relationships: Array<{
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
          label: attrs.label ?? node,
          type: attrs.nodeType ?? "Unknown",
          x: attrs.x ?? 0,
          y: attrs.y ?? 0,
          properties: attrs.properties ?? {},
        });
      }
    );

    graph.forEachEdge((_edge: string, attrs: { relationshipType?: string }, source: string, target: string) => {
      relationships.push({
        source,
        target,
        type: attrs.relationshipType ?? "Unknown",
      });
    });

    const data = {
      exportedAt: new Date().toISOString(),
      nodeCount: nodes.length,
      edgeCount: relationships.length,
      nodes,
      relationships,
    };

    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    downloadBlob(blob, "admapper-graph.json");
  } catch (err) {
    console.error("JSON export failed:", err);
    showError("Failed to export JSON: " + (err instanceof Error ? err.message : String(err)));
  }
}

interface NodeWithProperties {
  properties: Record<string, unknown>;
}

/** Export the graph as an interactive HTML file. */
export async function exportHTML(): Promise<void> {
  const renderer = getRenderer();
  if (!renderer) {
    showInfo("No graph to export. Please load a graph first.");
    return;
  }

  try {
    const graph = renderer.sigma.getGraph();

    // Collect basic node info from graphology (positions, labels, types)
    const nodeIds: string[] = [];
    const nodeBasics = new Map<string, { label: string; type: string; x: number; y: number }>();

    graph.forEachNode((node: string, attrs: { label?: string; nodeType?: string; x?: number; y?: number }) => {
      nodeIds.push(node);
      nodeBasics.set(node, {
        label: attrs.label ?? node,
        type: attrs.nodeType ?? "Unknown",
        x: attrs.x ?? 0,
        y: attrs.y ?? 0,
      });
    });

    if (nodeIds.length === 0) {
      showInfo("No nodes to export.");
      return;
    }

    // Fetch full properties for all visible nodes in parallel.
    // The graph is loaded with minimal GraphNode data (no properties); they
    // are only retrieved on demand. We fetch them all here so the exported
    // HTML has a populated detail panel.
    const propertyResults = await Promise.all(
      nodeIds.map((id) =>
        api
          .get<NodeWithProperties>(`/api/graph/node/${encodeURIComponent(id)}`)
          .then((n) => ({ id, properties: n.properties }))
          .catch(() => ({ id, properties: {} as Record<string, unknown> }))
      )
    );
    const propertiesMap = new Map(propertyResults.map((r) => [r.id, r.properties]));

    const nodes = nodeIds.map((id) => {
      const basic = nodeBasics.get(id)!;
      return {
        id,
        label: basic.label,
        type: basic.type,
        x: basic.x,
        y: basic.y,
        properties: propertiesMap.get(id) ?? {},
      };
    });

    const edges: Array<{ id: string; source: string; target: string; type: string }> = [];
    graph.forEachEdge((_edge: string, attrs: { relationshipType?: string }, source: string, target: string) => {
      edges.push({
        id: `${source}->${target}:${attrs.relationshipType ?? ""}`,
        source,
        target,
        type: attrs.relationshipType ?? "Unknown",
      });
    });

    const data = {
      exportedAt: new Date().toISOString(),
      nodes,
      edges,
    };

    const html = htmlTemplate.replace("__GRAPH_DATA_PLACEHOLDER__", JSON.stringify(data));
    const blob = new Blob([html], { type: "text/html" });
    downloadBlob(blob, "admapper-graph.html");
  } catch (err) {
    console.error("HTML export failed:", err);
    showError("Failed to export HTML: " + (err instanceof Error ? err.message : String(err)));
  }
}

/** Download a blob as a file */
async function downloadBlob(blob: Blob, filename: string): Promise<void> {
  // In Tauri mode, use native save dialog
  if (isRunningInTauri() && window.__TAURI_PLUGIN_DIALOG__?.save && window.__TAURI__?.core.invoke) {
    try {
      // Determine file extension and filter
      const ext = filename.split(".").pop() || "";
      const filterName =
        ext === "png"
          ? "PNG Image"
          : ext === "svg"
            ? "SVG Image"
            : ext === "json"
              ? "JSON File"
              : ext === "html"
                ? "HTML File"
                : "File";

      const savePath = await window.__TAURI_PLUGIN_DIALOG__.save({
        defaultPath: filename,
        filters: [{ name: filterName, extensions: [ext] }],
        title: `Save ${filterName}`,
      });

      if (!savePath) return; // User cancelled

      // Convert blob to base64 for transfer to Rust
      const arrayBuffer = await blob.arrayBuffer();
      const bytes = Array.from(new Uint8Array(arrayBuffer));

      // Write file using Tauri command
      await window.__TAURI__.core.invoke("write_file", {
        path: savePath,
        contents: bytes,
      });

      showSuccess(`Exported to ${savePath}`);
      return;
    } catch (err) {
      console.error("Tauri save failed:", err);
      // Fall through to web download
    }
  }

  // Web fallback: use download anchor
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.style.display = "none";
  a.rel = "noopener"; // Security best practice

  // Use a click event that doesn't bubble to avoid duplicate triggers
  const clickEvent = new MouseEvent("click", {
    bubbles: false,
    cancelable: false,
    view: window,
  });

  document.body.appendChild(a);
  a.dispatchEvent(clickEvent);

  // Clean up after a short delay to ensure download starts
  setTimeout(() => {
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, 100);
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
