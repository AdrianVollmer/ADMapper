/**
 * SVG icons for AD node types using Lucide icons.
 * Icons are converted to data URLs for use with @sigma/node-image.
 */

import {
  User,
  Users,
  Monitor,
  Building2,
  FileText,
  Folder,
  Box,
  FileKey,
  ShieldCheck,
  KeyRound,
  Link,
  Database,
  HelpCircle,
  type IconNode,
} from "lucide";
import { NODE_COLORS } from "./theme";

/** Convert Lucide IconNode to SVG string */
function iconNodeToSvg(iconNode: IconNode, color: string = "#fff", size: number = 64): string {
  const children = iconNode
    .map(([tag, attrs]) => {
      const attrStr = Object.entries(attrs)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => `${k}="${v}"`)
        .join(" ");
      return `<${tag} ${attrStr}/>`;
    })
    .join("");

  return `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${children}</svg>`;
}

/** Convert SVG string to data URL */
function svgToDataUrl(svg: string): string {
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}

// Map AD node types to Lucide icons
const LUCIDE_ICONS: Record<string, IconNode> = {
  User: User,
  Group: Users, // Shows 3 people
  Computer: Monitor,
  Domain: Building2,
  GPO: FileText,
  OU: Folder,
  Container: Box,
  CertTemplate: FileKey,
  EnterpriseCA: ShieldCheck,
  RootCA: KeyRound,
  AIACA: Link,
  NTAuthStore: Database,
  Unknown: HelpCircle,
};

/** Generate icon data URLs for all node types */
function generateIcons(): Record<string, string> {
  const icons: Record<string, string> = {};

  for (const [type, iconNode] of Object.entries(LUCIDE_ICONS)) {
    const svg = iconNodeToSvg(iconNode, "#ffffff", 64);
    icons[type] = svgToDataUrl(svg);
  }

  return icons;
}

/** Pre-generated icon data URLs */
export const NODE_ICONS: Record<string, string> = generateIcons();

/** Get the icon URL for a node type */
export function getNodeIcon(type: string): string {
  return NODE_ICONS[type] ?? NODE_ICONS.Unknown;
}

/** Get the color for a node type */
export function getNodeTypeColor(type: string): string {
  return NODE_COLORS[type] ?? NODE_COLORS.Unknown;
}

/** Default node size (uniform for all types) */
export const NODE_SIZE = 12;

/** Get Lucide icon node for a node type (for inline rendering) */
export function getNodeIconNode(type: string): IconNode {
  return LUCIDE_ICONS[type] ?? LUCIDE_ICONS.Unknown;
}

/** Get SVG inner content for inline rendering (stroke-based, for use inside an SVG element) */
export function getNodeIconPath(type: string): string {
  const iconNode = LUCIDE_ICONS[type] ?? LUCIDE_ICONS.Unknown;
  return iconNode
    .map(([tag, attrs]) => {
      const attrStr = Object.entries(attrs)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => `${k}="${v}"`)
        .join(" ");
      return `<${tag} ${attrStr}/>`;
    })
    .join("");
}
