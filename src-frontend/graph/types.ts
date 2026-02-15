/**
 * Types for AD graph visualization.
 *
 * These types represent Active Directory objects and their relationships
 * as they appear in BloodHound-style graph data.
 */

/** AD object types that can appear as nodes */
export type ADNodeType =
  | "User"
  | "Group"
  | "Computer"
  | "Domain"
  | "GPO"
  | "OU"
  | "Container"
  | "CertTemplate"
  | "EnterpriseCA"
  | "RootCA"
  | "AIACA"
  | "NTAuthStore"
  | "Unknown";

/** Common AD edge/relationship types */
export type ADEdgeType =
  | "MemberOf"
  | "HasSession"
  | "AdminTo"
  | "CanRDP"
  | "CanPSRemote"
  | "ExecuteDCOM"
  | "AllowedToDelegate"
  | "AllowedToAct"
  | "AddMember"
  | "ForceChangePassword"
  | "GenericAll"
  | "GenericWrite"
  | "WriteOwner"
  | "WriteDacl"
  | "Owns"
  | "Contains"
  | "GPLink"
  | "TrustedBy"
  | "DCSync"
  | "GetChanges"
  | "GetChangesAll"
  | "AllExtendedRights"
  | "AddKeyCredentialLink"
  | "AddAllowedToAct"
  | "ReadLAPSPassword"
  | "ReadGMSAPassword"
  | "GetChangesInFilteredSet"
  | "WriteSPN"
  | "WriteAccountRestrictions"
  | "LocalGroupMember"
  | "ACE"
  | "Unknown";

/** Node data stored in graphology */
export interface ADNodeAttributes {
  /** Display label */
  label: string;
  /** AD object type (named nodeType to avoid Sigma's reserved 'type' attribute) */
  nodeType: ADNodeType;
  /** Node x position */
  x: number;
  /** Node y position */
  y: number;
  /** Node size (for rendering) */
  size: number;
  /** Node color */
  color: string;
  /** Icon image URL for rendering */
  image: string;
  /** Original AD object properties (objectid, distinguishedname, etc.) */
  properties?: Record<string, unknown>;
  /** Whether this node is currently highlighted */
  highlighted?: boolean;
}

/** Edge data stored in graphology */
export interface ADEdgeAttributes {
  /** Display label */
  label?: string;
  /** Relationship type (named edgeType to avoid Sigma's reserved 'type' attribute) */
  edgeType: ADEdgeType;
  /** Edge color */
  color?: string;
  /** Edge size/weight */
  size?: number;
  /** Whether this edge is currently highlighted */
  highlighted?: boolean;
}

/** Raw node data as received from server */
export interface RawADNode {
  id: string;
  label: string;
  type: ADNodeType;
  properties?: Record<string, unknown>;
  x?: number;
  y?: number;
}

/** Raw edge data as received from server */
export interface RawADEdge {
  source: string;
  target: string;
  type: ADEdgeType;
  label?: string;
}

/** Graph data as received from server */
export interface RawADGraph {
  nodes: RawADNode[];
  edges: RawADEdge[];
}
