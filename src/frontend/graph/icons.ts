/**
 * SVG icons for AD node types.
 * Icons are embedded as data URLs for use with @sigma/node-image.
 */

import type { ADNodeType } from "./types";

/** Convert SVG string to data URL using URL encoding (more reliable than base64) */
function svgToDataUrl(svg: string): string {
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}

/** Create an SVG icon with consistent styling */
function createIcon(path: string, color: string = "#fff"): string {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="${color}">${path}</svg>`;
}

// Icon paths - clean, recognizable shapes optimized for small sizes
const ICON_PATHS: Record<ADNodeType, string> = {
  // User: simple person bust
  User: `<circle cx="12" cy="8" r="4"/><path d="M12 14c-5 0-8 2.5-8 5v1h16v-1c0-2.5-3-5-8-5z"/>`,

  // Group: two people
  Group: `<circle cx="9" cy="8" r="3.5"/><circle cx="15" cy="8" r="3.5"/><path d="M9 13c-4 0-7 2-7 4v1h14v-1c0-2-3-4-7-4z"/><path d="M15 13c1 0 2 .2 3 .6 2 1 3 2.2 3 3.4v1h-5v-1c0-1.5-.5-2.8-1.5-4z"/>`,

  // Computer: simple monitor
  Computer: `<rect x="2" y="3" width="20" height="13" rx="1"/><rect x="4" y="5" width="16" height="9"/><rect x="8" y="18" width="8" height="2"/><rect x="10" y="16" width="4" height="2"/>`,

  // Domain: AD pyramid/triangle (classic Active Directory icon)
  Domain: `<path d="M12 2L3 20h18L12 2z"/><path d="M12 7l-5 9h10l-5-9z" fill-opacity="0.3"/>`,

  // GPO: scroll/policy document
  GPO: `<path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6z"/><path d="M14 2v6h6"/><rect x="7" y="12" width="10" height="2"/><rect x="7" y="16" width="7" height="2"/>`,

  // OU: folder
  OU: `<path d="M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2z"/>`,

  // Container: box/cube outline
  Container: `<path d="M21 7L12 2 3 7v10l9 5 9-5V7z"/><path d="M12 12L3 7m9 5l9-5m-9 5v10" stroke-width="1" stroke="currentColor" fill="none"/>`,

  // CertTemplate: certificate/ribbon
  CertTemplate: `<rect x="4" y="2" width="16" height="14" rx="1"/><circle cx="12" cy="9" r="3"/><path d="M9 20l3-4 3 4"/><path d="M10 16h4"/>`,

  // EnterpriseCA: shield with checkmark
  EnterpriseCA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z"/><path d="M10 12l2 2 4-4" stroke-width="2" stroke="currentColor" fill="none"/>`,

  // RootCA: shield with key
  RootCA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z"/><circle cx="12" cy="9" r="2"/><rect x="11" y="11" width="2" height="5"/><rect x="10" y="14" width="4" height="2"/>`,

  // AIACA: linked certificates
  AIACA: `<path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z"/><circle cx="12" cy="10" r="3"/><circle cx="12" cy="10" r="1.5"/>`,

  // NTAuthStore: database/cylinder
  NTAuthStore: `<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.66 3.58 3 8 3s8-1.34 8-3V5"/><path d="M4 10c0 1.66 3.58 3 8 3s8-1.34 8-3"/><path d="M4 15c0 1.66 3.58 3 8 3s8-1.34 8-3"/>`,

  // Unknown: question mark in circle
  Unknown: `<circle cx="12" cy="12" r="10"/><path d="M12 17h.01M12 14v-1c0-1 1-2 2-2s2-1 2-2-1-2-2-2-2 .5-2 1.5" stroke-width="2" stroke="currentColor" fill="none"/>`,
};

// Node colors for icon backgrounds (matching theme.ts)
const ICON_COLORS: Record<ADNodeType, string> = {
  User: "#17a2b8",
  Group: "#ffc107",
  Computer: "#dc3545",
  Domain: "#6f42c1",
  GPO: "#fd7e14",
  OU: "#20c997",
  Container: "#6c757d",
  CertTemplate: "#e83e8c",
  EnterpriseCA: "#e83e8c",
  RootCA: "#e83e8c",
  AIACA: "#e83e8c",
  NTAuthStore: "#e83e8c",
  Unknown: "#6c757d",
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
