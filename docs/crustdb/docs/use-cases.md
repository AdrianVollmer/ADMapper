# Common Use Cases

CrustDB is suited for applications that need graph query capabilities without external infrastructure. This page describes common patterns and how to implement them.

## Security Analysis

CrustDB was originally designed for analyzing Active Directory permission graphs. It handles typical security analysis patterns efficiently.

### Finding Attack Paths

Identify paths from a compromised account to high-value targets:

```cypher
MATCH p = SHORTEST (src:User {compromised: true})-[:MemberOf|AdminTo|HasSession]-+(dst:Group {highValue: true})
RETURN p
```

### Identifying Choke Points

Find critical edges whose removal would disrupt many attack paths:

```rust
let result = db.edge_betweenness_centrality(
    Some(&["MemberOf", "AdminTo", "GenericAll"]),
    true  // directed
)?;

// Get top 10 critical edges
for (edge_id, score) in result.top_k(10) {
    let edge = db.get_edge(edge_id)?;
    println!("Edge {:?} has betweenness score {}", edge, score);
}
```

### Kerberoastable Accounts

Find service accounts vulnerable to Kerberos ticket attacks:

```cypher
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
RETURN u.name, u.serviceprincipalnames
```

### Unconstrained Delegation

Find computers with unconstrained delegation enabled:

```cypher
MATCH (c:Computer)
WHERE c.unconstraineddelegation = true
RETURN c.name, c.operatingsystem
```

## Knowledge Graphs

Build and query connected information.

### Entity Relationships

```cypher
// Create entities and relationships
CREATE (c:Company {name: 'Acme Corp'})-[:ACQUIRED]->(s:Company {name: 'Startup Inc'})

// Find all acquisitions
MATCH (buyer:Company)-[:ACQUIRED]->(target:Company)
RETURN buyer.name, target.name
```

### Multi-Hop Queries

Find connections up to 3 hops away:

```cypher
MATCH (p:Person {name: 'Alice'})-[:KNOWS*1..3]->(friend:Person)
RETURN DISTINCT friend.name
```

## Dependency Analysis

Track dependencies between software components.

### Build Order

```cypher
// Find components that depend on a library
MATCH (component:Package)-[:DEPENDS_ON]->(lib:Package {name: 'openssl'})
RETURN component.name
```

### Circular Dependencies

```cypher
// Find circular dependencies (simplified)
MATCH (a:Package)-[:DEPENDS_ON]->(b:Package)-[:DEPENDS_ON]->(a)
RETURN a.name, b.name
```

## Network Topology

Model and query network infrastructure.

### Path Between Hosts

```cypher
MATCH p = SHORTEST (src:Host {ip: '10.0.0.1'})-[:CONNECTS_TO]-+(dst:Host {ip: '10.0.0.100'})
RETURN p
```

### Affected Hosts on Switch Failure

```cypher
MATCH (s:Switch {name: 'core-sw-01'})-[:CONNECTS_TO*1..2]->(h:Host)
RETURN h.name, h.ip
```

## Batch Data Loading

For large datasets, use the batch API methods.

### Streaming Import Pattern

When importing data from multiple files where edges may reference nodes not yet loaded:

```rust
// Phase 1: Create placeholder nodes for referenced IDs
for edge_data in edges_to_import {
    // Get or create source node (creates placeholder if missing)
    let source_id = db.get_or_create_node_by_object_id(
        &edge_data.source_id,
        "Placeholder"
    )?;

    // Get or create target node
    let target_id = db.get_or_create_node_by_object_id(
        &edge_data.target_id,
        "Placeholder"
    )?;

    // Create edge
    db.insert_edges_batch(&[(
        source_id,
        target_id,
        edge_data.edge_type.clone(),
        edge_data.properties.clone(),
    )])?;
}

// Phase 2: Upsert full node data (merges into placeholders)
db.upsert_nodes_batch(&full_node_data)?;
```

### Building a Lookup Index

When you need to map external IDs to internal node IDs:

```rust
// Build an in-memory index of object_id -> node_id
let object_id_index = db.build_property_index("object_id")?;

// Use for edge insertion
for edge in edges {
    if let (Some(&src_id), Some(&tgt_id)) = (
        object_id_index.get(&edge.source_object_id),
        object_id_index.get(&edge.target_object_id),
    ) {
        // Insert edge using internal IDs
        db.insert_edges_batch(&[(src_id, tgt_id, edge.edge_type, edge.props)])?;
    }
}
```

## Counting and Aggregation

### Count by Label

```cypher
MATCH (n:User) RETURN count(n) AS user_count
```

### Count Relationships

```cypher
MATCH (u:User)-[r:MemberOf]->(g:Group)
RETURN u.name, count(r) AS group_count
```

## Filtering Patterns

### Property Comparisons

```cypher
MATCH (u:User)
WHERE u.enabled = true AND u.lastlogon > 1700000000
RETURN u.name
```

### String Matching

```cypher
MATCH (c:Computer)
WHERE c.name STARTS WITH 'DC-'
RETURN c.name
```

### Null Checks

```cypher
MATCH (u:User)
WHERE u.email IS NOT NULL
RETURN u.name, u.email
```

## Connection Queries

Get incoming and outgoing connections efficiently:

```rust
// Incoming edges to a node
let (nodes, edges) = db.get_incoming_connections_by_object_id("target-object-id")?;

// Outgoing edges from a node
let (nodes, edges) = db.get_outgoing_connections_by_object_id("source-object-id")?;
```

This is faster than equivalent Cypher queries for large result sets because it uses direct SQL with indexed lookups.
