# Index Selection Planning

## Problem

The query executor always scans by the first label in a pattern, ignoring available property indexes. This can result in full scans when an indexed property lookup would be more efficient.

## Current Behavior

In `executor/pattern.rs`:

```rust
fn match_single_node_pattern(
    pattern: &NodePattern,
    storage: &SqliteStorage,
) -> Result<Vec<Node>> {
    // Always uses label scan as entry point
    if let Some(labels) = &pattern.labels.first() {
        storage.find_nodes_by_label(&labels[0])
    } else {
        storage.scan_all_nodes()
    }
}
```

Even if the WHERE clause has `n.email = 'user@example.com'` and there's an index on `email`, we:
1. Scan all nodes with the label
2. Filter in memory

## Example

```cypher
-- Index exists on Person.email
MATCH (n:Person) WHERE n.email = 'alice@example.com' RETURN n
```

Current plan:
1. Scan all `Person` nodes (could be millions)
2. Filter by email property

Optimal plan:
1. Index lookup on `email = 'alice@example.com'`
2. Verify label is `Person`

## Proposed Solution

### Phase 1: Index Awareness

Track available indexes in storage:

```rust
impl SqliteStorage {
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        // Query sqlite_master for indexes on properties column
    }

    pub fn has_index(&self, property: &str) -> bool {
        // Check if property is indexed
    }
}

struct IndexInfo {
    property: String,
    unique: bool,
}
```

### Phase 2: Predicate Analysis

Extract indexable predicates from WHERE clause:

```rust
fn extract_index_candidates(predicate: &Expression) -> Vec<IndexCandidate> {
    // Find patterns like: n.prop = literal, n.prop IN [...], etc.
}

struct IndexCandidate {
    variable: String,
    property: String,
    operator: IndexOperator,
    value: PropertyValue,
}

enum IndexOperator {
    Eq,
    In,
    // Range operators if we add B-tree indexes
}
```

### Phase 3: Cost-Based Selection

Choose between scan and index lookup:

```rust
fn choose_access_path(
    pattern: &NodePattern,
    predicates: &[IndexCandidate],
    storage: &SqliteStorage,
) -> AccessPath {
    // Estimate costs
    let label_scan_cost = estimate_label_cardinality(pattern, storage);

    for candidate in predicates {
        if storage.has_index(&candidate.property) {
            let index_cost = estimate_index_selectivity(candidate);
            if index_cost < label_scan_cost {
                return AccessPath::IndexLookup(candidate.clone());
            }
        }
    }

    AccessPath::LabelScan(pattern.labels[0].clone())
}
```

### Phase 4: Index-First Execution

Add index lookup path to pattern matching:

```rust
fn match_single_node_pattern(
    pattern: &NodePattern,
    where_clause: Option<&Expression>,
    storage: &SqliteStorage,
) -> Result<Vec<Node>> {
    let access_path = choose_access_path(pattern, where_clause, storage);

    match access_path {
        AccessPath::IndexLookup(candidate) => {
            let nodes = storage.find_node_by_property(
                &candidate.property,
                &candidate.value,
            )?;
            // Filter by label if needed
            filter_by_label(nodes, &pattern.labels)
        }
        AccessPath::LabelScan(label) => {
            storage.find_nodes_by_label(&label)
        }
    }
}
```

## Statistics Collection

For cost-based decisions, collect:

```rust
impl SqliteStorage {
    pub fn estimate_label_count(&self, label: &str) -> Result<u64> {
        // COUNT(*) or cached statistics
    }

    pub fn estimate_property_cardinality(&self, property: &str) -> Result<u64> {
        // COUNT(DISTINCT json_extract(properties, '$.' || property))
    }
}
```

## Integration with Query Planner

This naturally fits into the deferred query planner implementation:

```rust
enum PlanOperator {
    // Existing
    NodeScan { label: Option<String> },

    // New
    IndexLookup {
        variable: String,
        property: String,
        value: PropertyValue,
        label_filter: Option<String>,
    },
}
```

## Implementation Steps

1. Add `list_indexes()` and `has_index()` to SqliteStorage
2. Implement predicate analysis to find indexable conditions
3. Add simple heuristic: prefer index if it exists
4. (Optional) Add cardinality estimation for cost-based selection
5. Integrate with query planner when implemented

## Complexity

Medium - the basic version (use index if available) is straightforward. Cost-based selection adds complexity.

## See Also

- `src/crustdb/src/storage.rs` - `build_property_index()` creates indexes
- `issues/new/query-planner-implementation.md` - Full planner design
- `issues/new/code-review-crustdb.md` - Original identification
