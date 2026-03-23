# Security Insights

ADMapper provides automated security analysis to identify common AD vulnerabilities.
Open the insights modal from the toolbar to view findings across several tabs.

## Domain Admin Analysis

Compares users with any path to Domain Admins (effective DAs) against direct or
transitive group members (real DAs). The ratio shows how much privilege expansion
exists in the environment. Click on a count to visualize the paths in the graph.

## Reachability

Shows how many objects well-known principals (Domain Users, Domain Computers,
Authenticated Users, Everyone) can reach via non-MemberOf relationships. High
counts indicate overly permissive configurations.

## Stale Objects

Counts enabled users and computers whose last logon exceeds a configurable
threshold (30, 60, 90, or 180 days). Click on a count to view the stale objects
in the graph.

## Account Exposure

Counts enabled accounts and computers with risky configurations:

- **Kerberoastable Users** -- accounts with SPNs set (`hasspn = true`), vulnerable
  to offline password cracking
- **AS-REP Roastable Users** -- accounts that do not require Kerberos
  pre-authentication (`dontreqpreauth = true`)
- **Unconstrained Delegation** -- computers that can impersonate any user who
  authenticates to them
- **Protected Users Members** -- accounts in the Protected Users group (RID 525),
  which receive additional credential protections

Click on any count to view the matching objects in the graph.

## Choke Points

Edges with high betweenness centrality. These are critical relationships where many attack paths converge:

- Removing these relationships would disrupt many potential attack paths
- Good candidates for remediation prioritization

### How It Works

ADMapper computes relationship betweenness centrality using Brandes' algorithm:

1. For each node, compute shortest paths to all other nodes
2. Count how many shortest paths pass through each relationship
3. Edges with high counts are choke points

Results are cached and recomputed when the graph changes.

## Unexpected Choke Points

A filtered view of choke points where the source node is neither a tier-0
target nor a domain infrastructure object (Domain, OU, GPO, Container, etc.).
These represent surprising attack paths from low-privilege entities and are
often the most actionable findings, since they highlight relationships that
should not carry high centrality.

## Tier Violations

Analyzes relationships that cross tier zone boundaries. Each node is placed
in a zone based on the most privileged (lowest) tier it can transitively
reach. Edges from a lower-privilege zone to a higher-privilege zone are
flagged as violations.

Click **Analyze Tier Violations** to compute effective tiers using reverse
BFS, then view the results broken down by zone crossing (1 to 0, 2 to 1,
3 to 2). Click on a count to visualize the violating edges in the graph.

For a detailed explanation of the tiering model, see the
[Tiering Guide](tiering.md).

