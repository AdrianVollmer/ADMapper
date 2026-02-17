/**
 * SVG icons for AD node types.
 * Icons are embedded as data URLs for use with @sigma/node-image.
 * Using high-quality SVG paths with antialiasing hints for crisp rendering.
 */

import type { ADNodeType } from "./types";

/** Convert SVG string to data URL using URL encoding (more reliable than base64) */
function svgToDataUrl(svg: string): string {
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}

/** Create an SVG icon with consistent styling and antialiasing hints */
function createIcon(path: string, color: string = "#fff"): string {
  // Use shape-rendering="geometricPrecision" for crisp rendering
  return `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 24 24" fill="${color}" shape-rendering="geometricPrecision">${path}</svg>`;
}

// Icon paths - crisp, recognizable shapes designed for both small and large sizes
// Using shape-rendering hints and cleaner geometry for better antialiasing
const ICON_PATHS: Record<ADNodeType, string> = {
  // User: refined person silhouette with smooth curves
  User: `<circle cx="12" cy="7" r="4.5" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M4 19c0-3.5 3.5-6 8-6s8 2.5 8 6v1H4v-1z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/>`,

  // Group: overlapping people silhouettes
  Group: `<circle cx="9" cy="7" r="3.5"/><circle cx="16" cy="7" r="3.5"/><path d="M2 18c0-2.8 2.8-4.5 7-4.5s7 1.7 7 4.5v1H2v-1z"/><path d="M16 13.5c2.5 0 6 1.2 6 3.5v2h-6v-2c0-1.3-.5-2.5-1.2-3.3.4-.1.8-.2 1.2-.2z" fill-opacity="0.8"/>`,

  // Computer: modern monitor with screen glare
  Computer: `<rect x="2" y="3" width="20" height="13" rx="2" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><rect x="4" y="5" width="16" height="9" rx="0.5" fill-opacity="0.3"/><path d="M4 5h6l-4 9H4V5z" fill-opacity="0.15"/><rect x="8" y="18" width="8" height="2" rx="0.5"/><rect x="10" y="16" width="4" height="2"/>`,

  // Domain: Active Directory pyramid with depth
  Domain: `<path d="M12 2L3 20h18L12 2z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 6l-6 11h12l-6-11z" fill-opacity="0.25"/><path d="M12 10l-3 6h6l-3-6z" fill-opacity="0.2"/>`,

  // GPO: policy document with folded corner
  GPO: `<path d="M6 2h8l6 6v12c0 1.1-.9 2-2 2H6c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M14 2v6h6" fill-opacity="0.3"/><rect x="7" y="12" width="10" height="1.5" rx="0.5"/><rect x="7" y="15" width="7" height="1.5" rx="0.5"/>`,

  // OU: modern folder with depth
  OU: `<path d="M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M22 8l-20 0v-2c0-1.1.9-2 2-2h6l2 2h8c1.1 0 2 .9 2 2z" fill-opacity="0.2"/>`,

  // Container: 3D box with perspective
  Container: `<path d="M21 7L12 2 3 7v10l9 5 9-5V7z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 12L3 7v10l9 5V12z" fill-opacity="0.2"/><path d="M12 12l9-5v10l-9 5V12z" fill-opacity="0.1"/><path d="M12 2l9 5-9 5-9-5 9-5z" fill-opacity="0.3"/>`,

  // CertTemplate: certificate with seal
  CertTemplate: `<rect x="3" y="2" width="18" height="15" rx="2" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><circle cx="12" cy="9" r="3.5" fill-opacity="0.3" stroke="currentColor" stroke-width="1"/><circle cx="12" cy="9" r="1.5"/><path d="M9 21l3-4 3 4-3-1-3 1z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/>`,

  // EnterpriseCA: shield with checkmark
  EnterpriseCA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 3L5 6v5c0 4.5 3 8.5 7 9.5V3z" fill-opacity="0.15"/><path d="M9.5 12l2 2.5 4-5" stroke-width="2.5" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" fill="none"/>`,

  // RootCA: shield with key
  RootCA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 3L5 6v5c0 4.5 3 8.5 7 9.5V3z" fill-opacity="0.15"/><circle cx="12" cy="8" r="2.5"/><rect x="11" y="10.5" width="2" height="6" rx="0.5"/><rect x="13" y="13" width="2" height="1.5" rx="0.5"/>`,

  // AIACA: shield with link
  AIACA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 3L5 6v5c0 4.5 3 8.5 7 9.5V3z" fill-opacity="0.15"/><circle cx="12" cy="10" r="4" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="12" cy="10" r="2"/>`,

  // NTAuthStore: database cylinder with sections
  NTAuthStore: `<ellipse cx="12" cy="5" rx="8" ry="3" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M4 5v14c0 1.66 3.58 3 8 3s8-1.34 8-3V5" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><ellipse cx="12" cy="10" rx="8" ry="3" fill-opacity="0.2"/><ellipse cx="12" cy="15" rx="8" ry="3" fill-opacity="0.2"/>`,

  // Unknown: question mark badge
  Unknown: `<circle cx="12" cy="12" r="10" stroke-width="0.5" stroke="rgba(0,0,0,0.2)"/><path d="M12 17.5a1 1 0 100-2 1 1 0 000 2z"/><path d="M12 14v-1c0-1.5 1.5-2 2.5-2.5s1.5-1.5 1.5-2.5c0-2-1.5-3-4-3s-4 1.5-4 3" stroke-width="2" stroke="currentColor" stroke-linecap="round" fill="none"/>`,
};

// Node colors for icon backgrounds (matching theme.ts - vibrant, modern colors)
const ICON_COLORS: Record<ADNodeType, string> = {
  User: "#22b8cf",
  Group: "#fab005",
  Computer: "#f03e3e",
  Domain: "#7950f2",
  GPO: "#fd7e14",
  OU: "#20c997",
  Container: "#868e96",
  CertTemplate: "#f06595",
  EnterpriseCA: "#f06595",
  RootCA: "#f06595",
  AIACA: "#f06595",
  NTAuthStore: "#f06595",
  Unknown: "#adb5bd",
};

/** Generate icon data URLs for all node types */
function generateIcons(): Record<ADNodeType, string> {
  const icons: Partial<Record<ADNodeType, string>> = {};

  for (const [type, path] of Object.entries(ICON_PATHS)) {
    icons[type as ADNodeType] = svgToDataUrl(createIcon(path, "#ffffff"));
  }

  return icons as Record<ADNodeType, string>;
}

/** Pre-generated icon data URLs */
export const NODE_ICONS: Record<ADNodeType, string> = generateIcons();

/** Get the icon URL for a node type */
export function getNodeIcon(type: ADNodeType): string {
  return NODE_ICONS[type] || NODE_ICONS.Unknown;
}

/** Get the color for a node type */
export function getNodeTypeColor(type: ADNodeType): string {
  return ICON_COLORS[type] || ICON_COLORS.Unknown;
}

/** Default node size (uniform for all types) */
export const NODE_SIZE = 12;

/** Get SVG path for a node type (for inline rendering) */
export function getNodeIconPath(type: ADNodeType): string {
  return ICON_PATHS[type] || ICON_PATHS.Unknown;
}
