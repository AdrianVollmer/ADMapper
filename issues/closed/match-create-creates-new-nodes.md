# MATCH...CREATE creates new nodes instead of using matched ones

## Summary

When using `MATCH...CREATE` to create relationships between existing nodes, CrustDB creates new nodes instead of using the nodes matched by the MATCH clause.

## Reproduction

```cypher
-- Create two nodes
CREATE (:Group {objectid: 'G1', name: 'Group1'})
CREATE (:Group {objectid: 'G2', name: 'Group2'})

-- Verify they exist (works)
MATCH (g:Group {objectid: 'G1'}) RETURN g.objectid
-- Returns: G1

-- Try to create relationship between them
MATCH (a:Group {objectid: 'G1'}), (b:Group {objectid: 'G2'})
CREATE (a)-[:MemberOf]->(b)
```

## Expected Behavior

- The MATCH clause should find the existing nodes G1 and G2
- The CREATE clause should create a relationship between those existing nodes
- Node count should remain 2, relationship count should be 1

## Actual Behavior

- The CREATE clause creates 2 NEW nodes (without the original properties)
- A relationship is created between the NEW nodes, not the matched ones
- Node count becomes 4, relationship count is 1
- The original G1 and G2 nodes remain unconnected

## Evidence

```rust
// After MATCH...CREATE:
// Result: QueryStats { nodes_created: 2, relationships_created: 1, ... }
// Stats: 4 nodes, 1 rel (was 2 nodes, 0 rels)
```

## Root Cause

In `src/query/executor/create.rs`, the `execute_create` function maintains its own `bindings: HashMap<String, i64>` and does not receive bindings from a preceding MATCH clause. When it encounters a variable reference, it creates a new node instead of looking up the existing binding.

The planner also appears to treat CREATE after MATCH as a standalone statement rather than a continuation that should use MATCH bindings.

## Workaround

Use the direct relationship creation API instead of Cypher:

```rust
let source_id = db.find_node_by_property("objectid", "G1").unwrap().unwrap();
let target_id = db.find_node_by_property("objectid", "G2").unwrap().unwrap();
db.insert_relationships_batch(&[(source_id, target_id, "MemberOf".to_string(), json!({}))]).unwrap();
```

## Impact

- High: This breaks a fundamental Cypher pattern used extensively in graph databases
- Prevents users from creating relationships between existing nodes via Cypher
- Forces workarounds using internal APIs

## Related Files

- `src/query/executor/create.rs` - `execute_create` function
- `src/query/planner.rs` - `plan_create` function
- `src/query/parser.rs` - Statement parsing
