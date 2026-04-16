/**
 * Visual theme configuration for AD graph rendering.
 *
 * Relationship type colors are loaded from the shared definitions
 * (src/shared/relationship_types.json) to maintain a single source of truth.
 */

import sharedDefs from "../../shared/relationship_types.json";

/** Color palette for node types - vibrant, modern colors for visual appeal */
export const NODE_COLORS: Record<string, string> = {
  User: "#22b8cf", // Cyan - users are common, approachable color
  Group: "#fab005", // Golden yellow - groups connect users to permissions
  Computer: "#f03e3e", // Vibrant red - computers are attack targets
  Domain: "#7950f2", // Vivid purple - domains are tier 0
  GPO: "#fd7e14", // Bright orange - GPOs control configuration
  OU: "#20c997", // Mint green - organizational containers
  Container: "#db2777", // Pink - generic containers (matches CSS bg-pink-600)
  CertTemplate: "#f06595", // Rose pink - certificate templates (PKI)
  EnterpriseCA: "#f06595", // Rose pink - enterprise CAs (PKI)
  RootCA: "#f06595", // Rose pink - root CAs (PKI)
  AIACA: "#f06595", // Rose pink - AIA CAs (PKI)
  NTAuthStore: "#f06595", // Rose pink - NTAuth store (PKI)
  Unknown: "#adb5bd", // Light gray - unknown types
};

/** Color palette for relationship types, derived from shared definitions */
export const RELATIONSHIP_COLORS: Record<string, string> = Object.fromEntries([
  ...sharedDefs.relationship_types.map((t) => [t.name, t.color]),
  ["Unknown", "#adb5bd"],
]);

/** Default relationship size (controls arrow head size) */
export const DEFAULT_EDGE_SIZE = 5;

/** Default relationship color (uniform for all relationship types) */
export const DEFAULT_RELATIONSHIP_COLOR = "#6c757d";

/** Highlighted relationship size multiplier */
export const HIGHLIGHT_SIZE_MULTIPLIER = 2;

/** Colors for highlighted/selected states */
export const HIGHLIGHT_COLORS = {
  node: "#fff700",
  relationship: "#fff700",
  neighbor: "#ffffff",
};

/** Colors for dimmed/faded states */
export const DIM_COLORS = {
  node: "#2a2a2a",
  relationship: "#1a1a1a",
};

/** Background color for the graph canvas */
export const BACKGROUND_COLOR = {
  light: "#ffffff",
  dark: "#1a1a2e",
};

/** Label color for node labels */
export const LABEL_COLOR = {
  light: "#1a1a1a",
  dark: "#e0e0e0",
};

/** Get node color, considering highlight state */
export function getNodeColor(type: string, highlighted?: boolean, dimmed?: boolean): string {
  if (highlighted) return HIGHLIGHT_COLORS.node;
  if (dimmed) return DIM_COLORS.node;
  return NODE_COLORS[type] ?? "#adb5bd";
}
