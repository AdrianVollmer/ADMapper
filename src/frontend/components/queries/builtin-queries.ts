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
            query: `MATCH (c:Computer) WHERE c.properties CONTAINS 'unconstraineddelegation' RETURN c`,
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
        query: `MATCH (u:User) WHERE u.properties CONTAINS 'hasspn' RETURN u`,
      },
      {
        id: "asreproastable",
        name: "AS-REP Roastable",
        description: "Users without preauth required",
        query: `MATCH (u:User) WHERE u.properties CONTAINS 'dontreqpreauth' RETURN u`,
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
    id: "tier-zero",
    name: "Tier 0 Targets",
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
        description: "Computers who are members of tier-0 groups (e.g., Domain Controllers)",
        query: `MATCH p = (c:Computer)-[:MemberOf]->(g:Group) WHERE ${ridWhereClause("g", ["-516", "-498", "-521", "-S-1-5-9"])} RETURN p`,
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
        query: `MATCH (t:CertTemplate) WHERE t.properties CONTAINS 'enrolleesuppliessubject' RETURN t`,
      },
      {
        id: "esc1",
        name: "ESC1 - Enrollee Supplies Subject",
        description: "Templates vulnerable to ESC1",
        query: `MATCH p = (t:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA) WHERE t.properties CONTAINS 'enrolleesuppliessubject' RETURN p`,
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
        name: "Shortest Paths to Domain Admins",
        description: "Find shortest path from each user to Domain Admins (SID -512)",
        query: `MATCH (u:User), (da:Group), p = shortestPath((u)-[*1..50]->(da)) WHERE da.objectid ENDS WITH '-512' RETURN p`,
      },
    ],
  },
];
