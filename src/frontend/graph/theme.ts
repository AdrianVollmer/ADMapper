/**
 * Visual theme configuration for AD graph rendering.
 *
 * Colors are chosen to match common BloodHound conventions while
 * being accessible and distinguishable.
 */

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

/** Color palette for relationship types (grouped by category) */
export const EDGE_COLORS: Record<string, string> = {
  // Membership/structure (neutral)
  MemberOf: "#6c757d",
  Contains: "#6c757d",
  GPLink: "#6c757d",
  LocalGroupMember: "#6c757d",

  // Session/access (blue)
  HasSession: "#0d6efd",
  CanRDP: "#0d6efd",
  CanPSRemote: "#0d6efd",
  ExecuteDCOM: "#0d6efd",

  // Dangerous permissions (red/orange)
  AdminTo: "#dc3545",
  GenericAll: "#dc3545",
  GenericWrite: "#fd7e14",
  WriteOwner: "#fd7e14",
  WriteDacl: "#fd7e14",
  Owns: "#dc3545",
  ForceChangePassword: "#fd7e14",
  AddMember: "#fd7e14",
  AllExtendedRights: "#fd7e14",
  AddKeyCredentialLink: "#fd7e14",
  WriteSPN: "#fd7e14",
  WriteAccountRestrictions: "#fd7e14",

  // Delegation (purple)
  AllowedToDelegate: "#6f42c1",
  AllowedToAct: "#6f42c1",
  AddAllowedToAct: "#6f42c1",
  TrustedBy: "#6f42c1",

  // DCSync / replication (critical - bright red)
  DCSync: "#ff0040",
  GetChanges: "#ff0040",
  GetChangesAll: "#ff0040",
  GetChangesInFilteredSet: "#ff0040",

  // Credential access (pink)
  ReadLAPSPassword: "#e83e8c",
  ReadGMSAPassword: "#e83e8c",

  // Generic ACE (gray)
  ACE: "#adb5bd",

  Unknown: "#adb5bd",
};

/** Default relationship size (controls arrow head size) */
export const DEFAULT_EDGE_SIZE = 5;

/** Default relationship color (uniform for all relationship types) */
export const DEFAULT_EDGE_COLOR = "#6c757d";

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
  return NODE_COLORS[type] ?? NODE_COLORS.Unknown;
}

/** Get relationship color, considering highlight state */
export function getEdgeColor(type: string, highlighted?: boolean, dimmed?: boolean): string {
  if (highlighted) return HIGHLIGHT_COLORS.relationship;
  if (dimmed) return DIM_COLORS.relationship;
  return EDGE_COLORS[type] ?? EDGE_COLORS.Unknown;
}
