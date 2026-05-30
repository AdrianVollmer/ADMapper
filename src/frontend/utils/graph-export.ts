/**
 * Utilities for parsing ADMapper's JSON export format back into graph data.
 */

import type { RawADGraph } from "../graph/types";

/**
 * Parse a JSON string produced by "Export JSON Data" and return a RawADGraph
 * suitable for passing to loadGraphData().
 *
 * The export format stores the display name in a field called "label"; RawADNode
 * calls the same field "name". Everything else maps 1-to-1.
 *
 * @throws Error with a descriptive message on malformed or structurally invalid input.
 */
export function parseExportJSON(json: string): RawADGraph {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    throw new Error("Invalid JSON: the file could not be parsed.");
  }

  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("Invalid export file: expected a JSON object.");
  }

  const obj = parsed as Record<string, unknown>;

  if (!Array.isArray(obj["nodes"])) {
    throw new Error("Invalid export file: 'nodes' must be an array.");
  }
  if (!Array.isArray(obj["relationships"])) {
    throw new Error("Invalid export file: 'relationships' must be an array.");
  }

  const nodes = (obj["nodes"] as Record<string, unknown>[]).map((n) => {
    const node: import("../graph/types").RawADNode = {
      id: String(n["id"] ?? ""),
      name: String(n["label"] ?? n["id"] ?? ""),
      type: String(n["type"] ?? "Unknown"),
    };
    if (typeof n["x"] === "number") node.x = n["x"];
    if (typeof n["y"] === "number") node.y = n["y"];
    if (typeof n["properties"] === "object" && n["properties"] !== null) {
      node.properties = n["properties"] as Record<string, unknown>;
    }
    return node;
  });

  const relationships = (obj["relationships"] as Record<string, unknown>[]).map((r) => ({
    source: String(r["source"] ?? ""),
    target: String(r["target"] ?? ""),
    type: String(r["type"] ?? "Unknown"),
  }));

  return { nodes, relationships };
}
