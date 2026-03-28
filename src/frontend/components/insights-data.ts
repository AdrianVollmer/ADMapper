/**
 * Security Insights Data Layer
 *
 * Data type definitions and loader functions for the Security Insights modal.
 * Each loader returns data (or throws) without touching UI state.
 */

import { executeQuery } from "../utils/query";
import { api } from "../api/client";

// Re-export for convenience so insights.ts can import from a single module
export { QueryAbortedError, getQueryErrorMessage } from "../utils/query";

// ── Type definitions ────────────────────────────────────────────────

/** Generic per-tab state wrapper */
export interface TabState<T> {
  loading: boolean;
  error: string | null;
  data: T | null;
}

/** Domain Admin Analysis data */
export interface DAAnalysisData {
  effectiveCount: number;
  realCount: number;
  ratio: number;
}

/** Reachability data for a principal */
export interface ReachabilityData {
  principalName: string;
  principalSid: string;
  count: number;
}

/** Account Exposure data */
export interface AccountExposureData {
  kerberoastable: number;
  asrepRoastable: number;
  unconstrainedDelegation: number;
  protectedUsers: number;
}

/** Stale Objects data */
export interface StaleObjectsData {
  users: number;
  computers: number;
  thresholdDays: number;
}

/** A single tier violation edge */
export interface TierViolationEdge {
  source_id: string;
  target_id: string;
  rel_type: string;
}

/** A single tier violation category */
export interface TierViolationCategory {
  source_zone: number;
  target_zone: number;
  count: number;
  edges: TierViolationEdge[];
}

/** Tier Violations response from API */
export interface TierViolationsData {
  violations: TierViolationCategory[];
  total_nodes: number;
  total_edges: number;
}

/** Choke Point data */
export interface ChokePointData {
  source_id: string;
  source_name: string;
  source_label: string;
  target_id: string;
  target_name: string;
  target_label: string;
  rel_type: string;
  betweenness: number;
  source_tier: number;
}

/** Choke Points response */
export interface ChokePointsData {
  choke_points: ChokePointData[];
  unexpected_choke_points: ChokePointData[];
  total_edges: number;
  total_nodes: number;
}

// ── Helper utilities ────────────────────────────────────────────────

/** Convert days to Windows FileTime threshold */
export function daysToWindowsFileTime(days: number): number {
  const now = Date.now();
  const thresholdMs = now - days * 24 * 60 * 60 * 1000;
  // Windows FileTime is 100-nanosecond intervals since Jan 1, 1601
  // Unix epoch is Jan 1, 1970 - difference is 11644473600 seconds
  const FILETIME_UNIX_DIFF = 116444736000000000n;
  const fileTime = BigInt(thresholdMs) * 10000n + FILETIME_UNIX_DIFF;
  return Number(fileTime);
}

// ── Loader functions ────────────────────────────────────────────────
// Each function returns a Promise of its data type.
// On QueryAbortedError, returns null (caller should silently ignore).
// On other errors, throws with a user-friendly message string.

/** Load Domain Admin Analysis data */
export async function loadDAAnalysis(): Promise<DAAnalysisData | null> {
  const [effectiveResult, realResult] = await Promise.all([
    executeQuery(
      `MATCH (u:User), (g:Group), p = shortestPath((u)-[*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN DISTINCT u`,
      { extractGraph: false, background: true }
    ),
    executeQuery(
      `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN DISTINCT u`,
      { extractGraph: false, background: true }
    ),
  ]);

  const effectiveCount = effectiveResult.resultCount;
  const realCount = realResult.resultCount;
  const ratio = realCount > 0 ? effectiveCount / realCount : effectiveCount > 0 ? Infinity : 1;

  return { effectiveCount, realCount, ratio };
}

/** Load Reachability data */
export async function loadReachability(): Promise<ReachabilityData[] | null> {
  const principals = [
    { name: "Domain Users", sid: "-513" },
    { name: "Domain Computers", sid: "-515" },
    { name: "Authenticated Users", sid: "-S-1-5-11" },
    { name: "Everyone", sid: "-S-1-1-0" },
  ];

  const queries = principals.map(async (p) => {
    try {
      const query = `
        MATCH (g:Group)-[r]->(target)
        WHERE g.objectid ENDS WITH '${p.sid}'
        AND type(r) <> 'MemberOf'
        RETURN DISTINCT target
      `;
      const result = await executeQuery(query, { extractGraph: false, background: true });
      return { principalName: p.name, principalSid: p.sid, count: result.resultCount };
    } catch {
      return { principalName: p.name, principalSid: p.sid, count: -1 };
    }
  });

  return Promise.all(queries);
}

/** Load Stale Objects data */
export async function loadStaleObjects(thresholdDays: number): Promise<StaleObjectsData | null> {
  const threshold = daysToWindowsFileTime(thresholdDays);

  const [usersResult, computersResult] = await Promise.all([
    executeQuery(`MATCH (u:User) WHERE u.enabled = true AND u.lastlogon < ${threshold} RETURN u`, {
      extractGraph: false,
      background: true,
    }),
    executeQuery(`MATCH (c:Computer) WHERE c.enabled = true AND c.lastlogon < ${threshold} RETURN c`, {
      extractGraph: false,
      background: true,
    }),
  ]);

  return {
    users: usersResult.resultCount,
    computers: computersResult.resultCount,
    thresholdDays,
  };
}

/** Load Account Exposure data */
export async function loadAccountExposure(): Promise<AccountExposureData | null> {
  const [kerbResult, asrepResult, delegationResult, protectedResult] = await Promise.all([
    executeQuery(`MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true RETURN u`, {
      extractGraph: false,
      background: true,
    }),
    executeQuery(`MATCH (u:User) WHERE u.dontreqpreauth = true AND u.enabled = true RETURN u`, {
      extractGraph: false,
      background: true,
    }),
    executeQuery(`MATCH (c:Computer) WHERE c.unconstraineddelegation = true AND c.enabled = true RETURN c`, {
      extractGraph: false,
      background: true,
    }),
    executeQuery(
      `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..]->(g)) WHERE g.objectid ENDS WITH '-525' RETURN DISTINCT u`,
      { extractGraph: false, background: true }
    ),
  ]);

  return {
    kerberoastable: kerbResult.resultCount,
    asrepRoastable: asrepResult.resultCount,
    unconstrainedDelegation: delegationResult.resultCount,
    protectedUsers: protectedResult.resultCount,
  };
}

/** Load Choke Points data */
export async function loadChokePoints(): Promise<ChokePointsData | null> {
  return api.get<ChokePointsData>("/api/graph/choke-points");
}

/** Load Tier Violations data */
export async function loadTierViolations(): Promise<TierViolationsData | null> {
  return api.get<TierViolationsData>("/api/graph/tier-violations");
}
