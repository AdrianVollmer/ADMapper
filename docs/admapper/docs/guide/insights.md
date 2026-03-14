# Security Insights

ADMapper provides automated security analysis to identify common AD vulnerabilities.

## Insights Panel

Open the Insights panel from the sidebar to view automated findings.

## High-Value Targets

Groups and accounts with elevated privileges:

- Domain Admins
- Enterprise Admins
- Schema Admins
- Backup Operators
- Account Operators

Click any item to navigate to it in the graph.

## Kerberoastable Accounts

User accounts with Service Principal Names (SPNs) set. These can be targeted for offline password cracking:

- Accounts with `hasspn = true`
- Only enabled accounts are shown

## AS-REP Roastable

Accounts that do not require Kerberos pre-authentication:

- Accounts with `dontreqpreauth = true`
- Vulnerable to offline password attacks

## Unconstrained Delegation

Computers configured for unconstrained delegation. These can impersonate any user that authenticates to them:

- Servers with delegation enabled
- Excludes domain controllers (expected behavior)

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

A filtered view of choke points where the source node is neither a high-value
target nor a domain infrastructure object (Domain, OU, GPO, Container, etc.).
These represent surprising attack paths from low-privilege entities and are
often the most actionable findings, since they highlight relationships that
should not carry high centrality.

## Paths to High-Value Targets

For any selected node, ADMapper shows whether attack paths exist to high-value targets:

- Green: No path to HVT
- Red: Path exists

Use shortest path queries to explore the actual paths.

## Owned Nodes

Mark nodes as "owned" to track compromised accounts:

1. Select a node
2. Click "Mark as Owned" in the details panel

Owned nodes are highlighted in the graph. Use this to:

- Track lateral movement progress
- Identify remaining paths from owned positions

## Export

Export insights as JSON for reporting:

```bash
curl http://localhost:9191/api/graph/insights
```

Response includes all insight categories with node details.
