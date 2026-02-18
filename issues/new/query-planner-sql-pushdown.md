# Query Planner with SQL Pushdown Detection

## Problem

Currently, all Cypher queries go through the same execution path:
1. Parse Cypher → AST
2. Execute pattern matching (fetch all matching nodes/edges)
3. Apply WHERE filtering in Rust
4. Apply aggregations in Rust
5. Apply SKIP/LIMIT in Rust

This is inefficient for queries that could be pushed down to SQLite.

## Goal

Add a query planner phase that detects "pushable" query patterns and rewrites them to use optimized SQL paths.

## Pushable Patterns

### Fully Pushable (single SQL query)
```cypher
MATCH (n) RETURN COUNT(n)                    → SELECT COUNT(*) FROM nodes
MATCH (n:Person) RETURN COUNT(n)             → SELECT COUNT(*) FROM nodes JOIN ...
MATCH ()-[r]->() RETURN COUNT(r)             → SELECT COUNT(*) FROM edges
MATCH (n:Person) RETURN n LIMIT 10           → ... LIMIT 10
MATCH (n) WHERE n.id = 5 RETURN n            → ... WHERE id = 5
```

### Partially Pushable (filter in SQL, process in Rust)
```cypher
MATCH (n:Person) WHERE n.age > 25 RETURN n   → Push label filter, property filter needs JSON
MATCH (a)-[:KNOWS]->(b) RETURN a, b          → Push edge type filter
```

### Not Pushable (require graph traversal)
```cypher
MATCH p = (a)-[*]->(b) RETURN p              → Variable-length paths
MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)        → Multi-hop patterns
MATCH p = SHORTEST ...                        → Shortest path algorithms
```

## Architecture

```
Cypher String
    ↓
  Parser → AST
    ↓
  Planner → QueryPlan (new!)
    ↓
  Executor
    ├── SQLPushdownExecutor (simple patterns)
    └── GraphExecutor (complex patterns)
```

## Implementation Steps

1. Define `QueryPlan` enum with variants for different execution strategies
2. Add `analyze()` function to detect pushable patterns from AST
3. Add `SqlPushdownExecutor` for simple COUNT/LIMIT queries
4. Gradually expand coverage to more patterns

## Notes

- Start simple: just COUNT and LIMIT pushdown
- Measure performance gains with benchmarks
- Keep existing executor as fallback for complex queries
- Consider hybrid execution (SQL for initial scan, Rust for graph ops)

## Related

- `planner.rs` already has placeholder structures (currently unused)
- COUNT pushdown being implemented separately as quick win
- LIMIT pushdown being implemented separately as quick win
