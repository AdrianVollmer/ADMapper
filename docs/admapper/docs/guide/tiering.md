# Tiering Model

ADMapper implements an enterprise tiering model to classify Active Directory
objects by privilege level. This helps identify tier violations — relationships
that cross tier boundaries and could allow privilege escalation.

## Overview

The Enterprise Access Model (formerly Administrative Tier Model) divides AD
objects into tiers based on their criticality:

| Tier | Name | Examples |
|------|------|----------|
| **0** | Identity infrastructure | Domain Controllers, Domain Admins, Enterprise Admins, KRBTGT, PKI servers, AdminSDHolder |
| **1** | Servers and enterprise apps | Member servers, application servers, service accounts |
| **2** | Workstations and standard users | End-user workstations, standard user accounts |
| **3** | Unclassified (default) | All objects that have not been explicitly classified |

Lower tier numbers represent higher privilege. All objects start at tier 3
(unclassified) until explicitly assigned.

## Assigning Tiers

Open the **Edit Tiers** modal from the toolbar to assign tiers to nodes.

### Recommended Workflow

1. All nodes start at tier 3 (default)
2. Assign tier 0 to your most critical objects first
3. Work upward through tier 1 and tier 2
4. Leave unclassified objects at tier 3

### Filter Modes

The Edit Tiers modal supports several ways to select nodes for tier assignment:

#### Name Regex

Use a regular expression to match node names. For example:

- `ADMIN` — matches all nodes with "ADMIN" in the name
- `^DC\d+` — matches nodes starting with "DC" followed by digits
- `SERVER|SRV` — matches nodes containing "SERVER" or "SRV"

#### Node Type

Filter by AD object type (User, Group, Computer, OU, Domain, GPO, etc.).

#### Group Membership

Search for a group by name and assign a tier to all its transitive members.
This follows `MemberOf` relationships recursively.

Example: Select "Domain Admins" to assign tier 0 to all direct and indirect
members of the Domain Admins group.

#### OU Containment

Search for an Organizational Unit and assign a tier to all objects it
recursively contains. This follows `Contains` relationships.

Example: Select "Tier 0 Servers" OU to assign tier 0 to all objects within
that OU and its sub-OUs.

#### Tag Visible Nodes

If you have a graph currently displayed, click **Tag Visible Nodes** to assign
a tier to all nodes currently visible in the graph view. This is useful after
running a query that surfaces a specific set of objects.

### Typical Tier 0 Objects

- Domain Admins, Enterprise Admins, Schema Admins
- Domain Controllers
- KRBTGT account
- AdminSDHolder
- PKI / AD CS servers and templates
- Account Operators, Server Operators, Backup Operators

## Analyzing Tier Violations

### Assigned vs. Effective Tier

ADMapper distinguishes between two tier concepts:

- **Assigned tier** — the tier you explicitly set on a node (stored as the
  `tier` property)
- **Effective tier** — the lowest (most privileged) tier the node can
  transitively reach via any relationship path

A **tier violation** occurs when a node's effective tier is lower than its
assigned tier. This means the node can reach a higher-privilege tier than
intended.

### Computing Effective Tiers

Open the **Security Insights** modal and navigate to the **Tier Violations**
tab. Click **Analyze Tier Violations** to compute effective tiers.

The algorithm uses multi-source reverse BFS:

1. For each tier level (0, 1, 2), collect all nodes assigned to that tier
2. Perform a reverse BFS from those seed nodes, following all relationships
   backwards
3. Each reached node's effective tier becomes the minimum of its current
   effective tier and the seed tier

This runs in O(V + E) time per tier level, making it efficient even for
large graphs.

### Reading the Results

The Tier Violations tab shows cross-zone boundary violations:

- **Zone 1 to Zone 0** — Server admin zone reaching domain admin zone
- **Zone 2 to Zone 1** — Workstation zone reaching server admin zone
- **Zone 3 to Zone 2** — Default zone reaching workstation zone

Click on a violation count to visualize the violating relationships in the
graph.

### Remediation

Common remediation steps for tier violations:

- Remove unnecessary group memberships that span tiers
- Restrict delegation permissions that cross tier boundaries
- Separate service accounts by tier
- Review and remove stale ACL entries
- Implement proper tiered administration (separate admin accounts per tier)

After making changes in your AD environment, re-import the data and
recompute effective tiers to verify the violations are resolved.
