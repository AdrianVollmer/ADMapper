/**
 * Types for AD graph visualization.
 *
 * These types represent Active Directory objects and their relationships
 * as they appear in BloodHound-style graph data.
 */

/**
 * AD node and relationship types are plain strings. The backend is the single
 * source of truth (GET /api/graph/node-types, GET /api/graph/relationship-types).
 * The frontend must handle any type the backend sends, falling back to
 * "Unknown" styling when no visual mapping exists.
 */
export type ADNodeType = string;
export type ADRelationshipType = string;

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

/** Relationship data stored in graphology */
export interface ADEdgeAttributes {
  /** Display label */
  label?: string;
  /** Relationship type (named relationshipType to avoid Sigma's reserved 'type' attribute) */
  relationshipType: ADRelationshipType;
  /** Relationship color */
  color?: string;
  /** Relationship size/weight */
  size?: number;
  /** Whether this relationship is currently highlighted */
  highlighted?: boolean;
  /** Sigma relationship type: "tapered" for straight cone-shaped, "curvedArrow" for curved */
  type?: "tapered" | "curvedArrow";
  /** Curvature for curved relationships (0 = straight, positive = curve one way, negative = other) */
  curvature?: number;
  /** When multiple relationships between same nodes are collapsed, stores all types */
  collapsedTypes?: ADRelationshipType[];
  /** Per-type exploit likelihoods for collapsed multi-type edges */
  typeExploitLikelihoods?: Record<string, number | undefined>;
  /** Original relationship properties */
  properties?: Record<string, unknown>;
}

/** Raw node data as received from server */
export interface RawADNode {
  id: string;
  /** Display name (from BloodHound's name property) */
  name: string;
  /** Cypher label (User, Computer, Group, etc.) - serialized as "type" by backend */
  type: ADNodeType;
  properties?: Record<string, unknown> | undefined;
  x?: number;
  y?: number;
}

/** Raw relationship data as received from server */
export interface RawADEdge {
  source: string;
  target: string;
  type: ADRelationshipType;
  label?: string;
  exploit_likelihood?: number;
}

/** Graph data as received from server */
export interface RawADGraph {
  nodes: RawADNode[];
  relationships: RawADEdge[];
}
