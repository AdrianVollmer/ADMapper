/**
 * Built-in queries for Active Directory analysis
 */

import type { QueryCategory } from "./types";

/**
 * Well-known tier-0 group RIDs in Active Directory.
 * Used across insights, sidebar, and built-in queries.
 */
export const HIGH_VALUE_RIDS = [
  "-519", // Enterprise Admins
  "-512", // Domain Admins
  "-518", // Schema Admins
  "-516", // Domain Controllers
  "-498", // Enterprise Read-only Domain Controllers
  "-544", // Administrators
  "-548", // Account Operators
  "-549", // Server Operators
  "-551", // Backup Operators
];

/** Build a WHERE clause matching any of the given RID suffixes on a variable */
export function ridWhereClause(variable: string, rids: string[]): string {
  return rids.map((rid) => `${variable}.objectid ENDS WITH '${rid}'`).join(" OR ");
}

/** Built-in queries organized by category */
export const BUILTIN_QUERIES: QueryCategory[] = [
  {
    id: "domain-recon",
    name: "Domain Recon",
    expanded: true,
    queries: [
      {
        id: "all-domain-admins",
        name: "Domain Admins",
        description: "All members of Domain Admins group (SID -512)",
        query: `MATCH p = (u:User)-[:MemberOf]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
      },
      {
        id: "all-dcs",
        name: "Domain Controllers",
        description: "All domain controller computers (RID 516, 498, 521, SID S-1-5-9)",
        query: `MATCH p = (c:Computer)-[:MemberOf]->(g:Group) WHERE g.objectid ENDS WITH '-516' OR g.objectid ENDS WITH '-498' OR g.objectid ENDS WITH '-521' OR g.objectid ENDS WITH '-S-1-5-9' RETURN p`,
      },
      {
        id: "domain-trusts",
        name: "Domain Trusts",
        description: "All domains and their trust relationships",
        query: `MATCH (d:Domain) RETURN d AS result UNION ALL MATCH p = (d:Domain)-[:SameForestTrust|CrossForestTrust]->(t:Domain) RETURN p AS result`,
      },
    ],
  },
  {
    id: "dangerous-privileges",
    name: "Dangerous Privileges",
    expanded: true,
    subcategories: [
      {
        id: "dcsync",
        name: "DCSync",
        queries: [
          {
            id: "dcsync-rights",
            name: "DCSync Rights",
            description: "Principals with DCSync privileges",
            query: `MATCH p = (n)-[:DCSync]->(d:Domain) RETURN p`,
          },
          {
            id: "dcsync-non-da",
            name: "DCSync (Non-Domain Admins)",
            description: "Non-DA principals with DCSync",
            query: `MATCH p = (n)-[:DCSync]->(d:Domain) RETURN p`,
          },
        ],
      },
      {
        id: "delegation",
        name: "Delegation",
        queries: [
          {
            id: "unconstrained",
            name: "Unconstrained Delegation",
            description: "Computers with unconstrained delegation",
            query: `MATCH (c:Computer) WHERE c.unconstraineddelegation = true RETURN c`,
          },
          {
            id: "constrained",
            name: "Constrained Delegation",
            description: "Principals with constrained delegation",
            query: `MATCH p = (n)-[:AllowedToDelegate]->(t) RETURN p`,
          },
          {
            id: "rbcd",
            name: "Resource-Based Constrained Delegation",
            description: "Principals that can RBCD to a target",
            query: `MATCH p = (n)-[:AllowedToAct]->(t) RETURN p`,
          },
        ],
      },
      {
        id: "acl-abuse",
        name: "ACL Abuse",
        queries: [
          {
            id: "genericall",
            name: "GenericAll Rights",
            description: "Principals with GenericAll on objects",
            query: `MATCH p = (n)-[:GenericAll]->(t) RETURN p`,
          },
          {
            id: "genericwrite",
            name: "GenericWrite Rights",
            description: "Principals with GenericWrite on objects",
            query: `MATCH p = (n)-[:GenericWrite]->(t) RETURN p`,
          },
          {
            id: "writedacl",
            name: "WriteDacl Rights",
            description: "Principals that can modify DACLs",
            query: `MATCH p = (n)-[:WriteDacl]->(t) RETURN p`,
          },
          {
            id: "writeowner",
            name: "WriteOwner Rights",
            description: "Principals that can change ownership",
            query: `MATCH p = (n)-[:WriteOwner]->(t) RETURN p`,
          },
        ],
      },
      {
        id: "low-priv-permissions",
        name: "Low Privilege Group Permissions",
        queries: [
          {
            id: "domain-users-permissions",
            name: "Domain Users Permissions",
            description: "Non-MemberOf relationships from Domain Users",
            query: `MATCH p = (g:Group)-[r]->(target) WHERE g.objectid ENDS WITH '-513' AND type(r) <> 'MemberOf' RETURN p`,
          },
          {
            id: "domain-computers-permissions",
            name: "Domain Computers Permissions",
            description: "Non-MemberOf relationships from Domain Computers",
            query: `MATCH p = (g:Group)-[r]->(target) WHERE g.objectid ENDS WITH '-515' AND type(r) <> 'MemberOf' RETURN p`,
          },
          {
            id: "authenticated-users-permissions",
            name: "Authenticated Users Permissions",
            description: "Non-MemberOf relationships from Authenticated Users",
            query: `MATCH p = (g:Group)-[r]->(target) WHERE g.objectid ENDS WITH '-S-1-5-11' AND type(r) <> 'MemberOf' RETURN p`,
          },
          {
            id: "everyone-permissions",
            name: "Everyone Permissions",
            description: "Non-MemberOf relationships from Everyone",
            query: `MATCH p = (g:Group)-[r]->(target) WHERE g.objectid ENDS WITH '-S-1-1-0' AND type(r) <> 'MemberOf' RETURN p`,
          },
          {
            id: "all-low-priv-permissions",
            name: "All Low Privilege Groups",
            description: "Non-MemberOf relationships from any well-known low privilege group",
            query: `MATCH p = (g:Group)-[r]->(target) WHERE (g.objectid ENDS WITH '-513' OR g.objectid ENDS WITH '-515' OR g.objectid ENDS WITH '-S-1-5-11' OR g.objectid ENDS WITH '-S-1-1-0') AND type(r) <> 'MemberOf' RETURN p`,
          },
        ],
      },
    ],
  },
  {
    id: "kerberos",
    name: "Kerberos Attacks",
    queries: [
      {
        id: "kerberoastable",
        name: "Kerberoastable Users",
        description: "Users with SPNs (kerberoastable)",
        query: `MATCH (u:User) WHERE u.hasspn = true RETURN u`,
      },
      {
        id: "asreproastable",
        name: "AS-REP Roastable",
        description: "Users without preauth required",
        query: `MATCH (u:User) WHERE u.dontreqpreauth = true RETURN u`,
      },
      {
        id: "kerberoastable-with-path",
        name: "Kerberoastable with Path to Computer",
        description: "Enabled kerberoastable users with a path to at least one computer",
        query: `MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true MATCH p = shortestPath((u)-[*1..50]->(c:Computer)) RETURN p`,
      },
      {
        id: "asreproastable-with-path",
        name: "AS-REP Roastable with Path to Computer",
        description: "Enabled AS-REP roastable users with a path to at least one computer",
        query: `MATCH (u:User) WHERE u.dontreqpreauth = true AND u.enabled = true MATCH p = shortestPath((u)-[*1..50]->(c:Computer)) RETURN p`,
      },
    ],
  },
  {
    id: "sessions",
    name: "Sessions & Access",
    queries: [
      {
        id: "admin-sessions",
        name: "Admin Sessions",
        description: "Where users have sessions on computers",
        query: `MATCH p = (u:User)-[:HasSession]->(c:Computer) RETURN p`,
      },
      {
        id: "rdp-users",
        name: "RDP Access",
        description: "Users with RDP access to computers",
        query: `MATCH p = (u:User)-[:CanRDP]->(c:Computer) RETURN p`,
      },
      {
        id: "local-admins",
        name: "Local Admins",
        description: "Users with local admin rights",
        query: `MATCH p = (u:User)-[:AdminTo]->(c:Computer) RETURN p`,
      },
    ],
  },
  {
    id: "tier-analysis",
    name: "Tier Analysis",
    subcategories: [
      {
        id: "tier-zero-assets",
        name: "Tier 0 Assets",
        queries: [
          {
            id: "tier-zero-groups",
            name: "Tier 0 Groups",
            description: "Privileged groups by well-known SID",
            query: `MATCH (g:Group) WHERE ${ridWhereClause("g", [...HIGH_VALUE_RIDS, "-S-1-5-9"])} RETURN g`,
          },
          {
            id: "tier-zero-users",
            name: "Tier 0 Users",
            description: "Users who are members of tier-0 groups",
            query: `MATCH p = (u:User)-[:MemberOf]->(g:Group) WHERE ${ridWhereClause("g", [...HIGH_VALUE_RIDS, "-S-1-5-9"])} RETURN p`,
          },
          {
            id: "tier-zero-computers",
            name: "Tier 0 Computers",
            description: "Computers that are domain controllers",
            query: `MATCH p = (c:Computer)-[:MemberOf]->(g:Group) WHERE ${ridWhereClause("g", ["-516", "-498", "-521", "-S-1-5-9"])} RETURN p`,
          },
        ],
      },
      {
        id: "targeting-tier-0",
        name: "Targeting Tier 0",
        queries: [
          {
            id: "tier1-to-tier0",
            name: "Tier 1 → Tier 0",
            description: "Single-hop relationships from tier 1 nodes to tier 0 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 1 AND b.tier = 0 RETURN p`,
          },
          {
            id: "tier2-to-tier0",
            name: "Tier 2 → Tier 0",
            description: "Single-hop relationships from tier 2 nodes to tier 0 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 2 AND b.tier = 0 RETURN p`,
          },
          {
            id: "tier3-to-tier0",
            name: "Tier 3 → Tier 0",
            description: "Single-hop relationships from tier 3 nodes to tier 0 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 3 AND b.tier = 0 RETURN p`,
          },
          {
            id: "any-to-tier0",
            name: "Any → Tier 0",
            description: "All single-hop relationships reaching tier 0 from higher-numbered tiers",
            query: `MATCH p = (a)-[r]->(b) WHERE b.tier = 0 AND a.tier > 0 RETURN p`,
          },
        ],
      },
      {
        id: "targeting-tier-1",
        name: "Targeting Tier 1",
        queries: [
          {
            id: "tier2-to-tier1",
            name: "Tier 2 → Tier 1",
            description: "Single-hop relationships from tier 2 nodes to tier 1 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 2 AND b.tier = 1 RETURN p`,
          },
          {
            id: "tier3-to-tier1",
            name: "Tier 3 → Tier 1",
            description: "Single-hop relationships from tier 3 nodes to tier 1 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 3 AND b.tier = 1 RETURN p`,
          },
          {
            id: "any-to-tier1",
            name: "Any → Tier 1",
            description: "All single-hop relationships reaching tier 1 from higher-numbered tiers",
            query: `MATCH p = (a)-[r]->(b) WHERE b.tier = 1 AND a.tier > 1 RETURN p`,
          },
        ],
      },
      {
        id: "targeting-tier-2",
        name: "Targeting Tier 2",
        queries: [
          {
            id: "tier3-to-tier2",
            name: "Tier 3 → Tier 2",
            description: "Single-hop relationships from tier 3 nodes to tier 2 nodes",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier = 3 AND b.tier = 2 RETURN p`,
          },
          {
            id: "any-to-tier2",
            name: "Any → Tier 2",
            description: "All single-hop relationships reaching tier 2 from higher-numbered tiers",
            query: `MATCH p = (a)-[r]->(b) WHERE b.tier = 2 AND a.tier > 2 RETURN p`,
          },
        ],
      },
      {
        id: "all-tier-violations",
        name: "All Tier Violations",
        queries: [
          {
            id: "all-cross-tier",
            name: "All Cross-Tier Relationships",
            description: "All single-hop relationships where source tier > target tier (any privilege escalation path)",
            query: `MATCH p = (a)-[r]->(b) WHERE a.tier IS NOT NULL AND b.tier IS NOT NULL AND a.tier > b.tier RETURN p`,
          },
        ],
      },
    ],
  },
  {
    id: "certificates",
    name: "Certificates (ADCS)",
    queries: [
      {
        id: "vuln-templates",
        name: "Vulnerable Templates",
        description: "Certificate templates with dangerous settings",
        query: `MATCH (t:CertTemplate) WHERE t.enrolleesuppliessubject = true RETURN t`,
      },
      {
        id: "esc1",
        name: "ESC1 - Enrollee Supplies Subject",
        description: "Templates vulnerable to ESC1",
        query: `MATCH p = (t:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA) WHERE t.enrolleesuppliessubject = true RETURN p`,
      },
    ],
  },
  {
    id: "paths",
    name: "Path Analysis",
    expanded: true,
    queries: [
      {
        id: "paths-to-da",
        name: "Reachable Domain Admins (EL ≥ 0.5)",
        description: "Shortest path from each user to Domain Admins where every hop has exploit likelihood ≥ 0.5",
        query: `MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..50]->(da)) WHERE da.objectid ENDS WITH '-512' AND ALL(r IN relationships(p) WHERE r.exploit_likelihood >= 0.5) RETURN p`,
      },
    ],
  },
];
