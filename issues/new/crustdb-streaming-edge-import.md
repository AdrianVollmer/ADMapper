# CrustDB: Streaming Edge Import with Node Upsert

## Summary

Enable CrustDB to import edges without buffering them in memory by supporting node upsert (merge) operations. This would give CrustDB a performance advantage over Neo4j/FalkorDB for large imports.

## Current Behavior

When importing data with edges that reference nodes from other files:
1. All edges are buffered in memory during import
2. Edges are only inserted after all nodes from all files exist
3. Memory usage scales with total edge count

This is necessary because edges may reference nodes that don't exist yet (defined in a later file). Without buffering, we'd create placeholder nodes that become duplicates when the real node is imported.

## Proposed Enhancement

Add support for **node upsert** based on a user-specified unique key property:

1. When inserting a node, check if a node with the same key property value exists
2. If yes, merge/update the existing node with new properties
3. If no, insert as new node

This allows edges to be inserted immediately:
- Edge insertion creates placeholder nodes if needed
- Real node insertion later merges with the placeholder
- No duplicates, no buffering required

## Implementation Considerations

### Challenge: Abstraction

The unique key is domain-specific:
- BloodHound uses `ObjectIdentifier` (stored as `object_id`)
- Other applications may use different properties

CrustDB should remain a general-purpose Cypher database, not tied to BloodHound.

### Possible Approaches

**Option A: User-defined merge key**
```rust
// New trait method
fn upsert_nodes(&self, nodes: &[DbNode], key_property: &str) -> Result<usize>;
```
- Caller specifies which property to use as the unique key
- CrustDB dynamically creates/uses an index on that property
- Most flexible, but adds API complexity

**Option B: Convention-based key**
- Use a standard property name like `_id` as the primary key
- Applications map their ID to `_id`
- Simpler API, but requires data transformation

**Option C: Dedicated column**
- Add an `object_id` column to the nodes table (schema v2)
- Simple and fast, but BloodHound-specific
- Not recommended for general-purpose database

### SQLite Implementation

With a unique index on the key property:
```sql
INSERT INTO nodes (key_col, properties) VALUES (?, ?)
ON CONFLICT(key_col) DO UPDATE SET
  properties = json_patch(properties, excluded.properties)
```

Or using a generated column (SQLite 3.31+):
```sql
ALTER TABLE nodes ADD COLUMN _id TEXT
  GENERATED ALWAYS AS (json_extract(properties, '$._id')) STORED UNIQUE;
```

## Backend Trait Changes

```rust
trait DatabaseBackend {
    // Existing methods...

    /// Whether the backend supports streaming edge import (no buffering)
    fn supports_edge_streaming(&self) -> bool { false }

    /// Upsert nodes using the specified property as merge key
    fn upsert_nodes(&self, nodes: &[DbNode], key_property: &str) -> Result<usize> {
        // Default: fall back to regular insert
        self.insert_nodes(nodes)
    }
}
```

## Importer Changes

```rust
// In BloodHoundImporter
if self.db.supports_edge_streaming() {
    // Insert edges immediately - backend handles node upsert
    self.db.insert_edges(&edges)?;
} else {
    // Buffer edges for later (current behavior)
    self.edge_buffer.extend(edges);
}
```

## Benefits

- **Lower memory usage**: No edge buffering for large imports
- **Streaming progress**: Edges visible immediately during import
- **No placeholder cleanup**: Real nodes merge with placeholders automatically
- **CrustDB advantage**: Feature not available in Neo4j/FalkorDB without APOC

## Priority

Low - Current buffering approach works well and has acceptable memory overhead (~20MB for 100k edges). Revisit if memory becomes an issue for very large imports.
