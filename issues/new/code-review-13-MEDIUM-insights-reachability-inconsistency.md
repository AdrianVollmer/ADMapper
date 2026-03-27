# Security insights compute different reachability across backends

**Severity: MEDIUM** | **Category: inconsistency**

## Problem

The `get_security_insights` reachability computation differs across backends:

- **CrustDB** (`crustdb/insights.rs`): counts non-MemberOf neighbors from a
  forward adjacency map (correct — excludes group membership)
- **Neo4j** (`neo4j.rs:491-571`): uses `OPTIONAL MATCH (p)-[]->(t)` which
  counts ALL direct neighbors including MemberOf
- **FalkorDB** (`falkordb.rs:573-656`): same as Neo4j

The same dataset will produce different `reachable_count` numbers depending
on which backend is used.

## Solution

Align the Neo4j/FalkorDB Cypher queries to exclude MemberOf relationships,
matching the CrustDB behavior:

```cypher
OPTIONAL MATCH (p)-[r]->(t) WHERE type(r) <> 'MemberOf'
```
