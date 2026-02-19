# Code Review: crustdb

## Overview

CrustDB is an embedded graph database with SQLite backend and Cypher query support. The codebase is approximately 5000+ lines of Rust across 7 source files.

## AI Slop Assessment

**Verdict: Not AI slop.** The code shows clear signs of intentional design:

- Well-structured module separation (storage, parser, executor, planner)
- Idiomatic Rust patterns (Result types, thiserror, serde integration)
- Consistent naming conventions and documentation
- Pragmatic abstractions without over-engineering
- Good test coverage with meaningful test cases

The code appears written by someone who understands Rust, database internals, and parser design. It follows the project conventions in AGENTS.md faithfully.

---

## Top 10 Issues to Improve Code Quality

### 1. Monolithic executor.rs (2700+ lines)

**Location:** `src/crustdb/src/query/executor.rs`

The executor handles too many concerns: pattern matching, expression evaluation, aggregation, BFS traversal, mutations. This makes testing individual features difficult and increases cognitive load.

**Recommendation:** Split into submodules:
- `executor/pattern.rs` - Pattern matching (single-node, single-hop, multi-hop, variable-length)
- `executor/eval.rs` - Expression evaluation and comparison functions
- `executor/aggregate.rs` - Aggregate function evaluation
- `executor/mutation.rs` - SET and DELETE execution
- `executor/shortest_path.rs` - BFS/Dijkstra path finding

---

### 2. ~~N+1 Query Pattern in Edge Retrieval~~ ✅ FIXED

**Location:** `storage.rs:577-597`, `storage.rs:737-774`

~~Functions like `find_edges_by_type`, `find_outgoing_edges`, and `find_incoming_edges` first query edge IDs, then call `get_edge()` for each ID separately. This causes N+1 queries.~~

**Fixed:** Added `collect_edges_from_stmt` helper function that fetches all edge data in a single query using JOINs, matching the pattern used by `collect_nodes_from_stmt`. All three functions (`find_edges_by_type`, `find_outgoing_edges`, `find_incoming_edges`) now use this helper.

---

### 3. Silent Error Swallowing with unwrap_or(0)

**Location:** `storage.rs:808`, `storage.rs:824`

```rust
let count: i64 = self.conn.query_row(...).unwrap_or(0);
```

Database errors are silently converted to 0, masking potential issues like schema corruption or connection problems.

**Recommendation:** Propagate errors properly:
```rust
let count: i64 = self.conn.query_row(...)?;
```

---

### 4. SQL/JSON Path Injection Risk

**Location:** `storage.rs:342-347`, `storage.rs:362-366`

Property names are interpolated into SQL queries with only single-quote escaping:
```rust
let query = format!(
    "SELECT id FROM nodes WHERE json_extract(properties, '$.{}') = ?1 LIMIT 1",
    property.replace('\'', "''")
);
```

A property name like `')--` or containing special JSON path characters could cause unexpected behavior.

**Recommendation:** Validate property names against a whitelist pattern (alphanumeric + underscore only) or use parameterized JSON extraction.

---

### 5. Query Planner is a Dead Stub

**Location:** `src/crustdb/src/query/planner.rs`

The planner defines a complete operator hierarchy (`PlanOperator`, `FilterPredicate`, etc.) but `plan()` returns `PlanOperator::Empty`:

```rust
pub fn plan(statement: &Statement) -> Result<QueryPlan> {
    let _ = statement;
    Ok(QueryPlan { root: PlanOperator::Empty })
}
```

This dead code adds 200+ lines without providing value. All "optimization" happens ad-hoc in the executor.

**Recommendation:** Either implement the planner or remove the dead code. If keeping as a placeholder, reduce to minimal types needed for future work.

---

### 6. Mutex Bottleneck for Concurrent Access

**Location:** `lib.rs:44-46`

```rust
pub struct Database {
    storage: Mutex<SqliteStorage>,
}
```

Using `Mutex` serializes all database access, even read-only queries. This becomes a bottleneck under concurrent load.

**Recommendation:** Use `RwLock<SqliteStorage>` to allow concurrent reads:
```rust
storage: RwLock<SqliteStorage>,
```
Then use `storage.read()` for queries and `storage.write()` for mutations.

---

### 7. Duplicated Scan Logic with/without Limit

**Location:** `storage.rs:559-675`, `storage.rs:677-700`

`find_nodes_by_label` and `find_nodes_by_label_limit` contain similar but not identical SQL. Same for `scan_all_nodes` vs `get_all_nodes_limit`. This violates DRY.

**Recommendation:** Consolidate into single functions with an `Option<u64>` limit parameter:
```rust
pub fn find_nodes_by_label(&self, label: &str, limit: Option<u64>) -> Result<Vec<Node>>
```

---

### 8. Magic Constant DEFAULT_MAX_HOPS

**Location:** `executor.rs:692`

```rust
const DEFAULT_MAX_HOPS: usize = 10000;
```

This hardcoded value for shortest path traversal could cause memory exhaustion on dense graphs with no user control.

**Recommendation:** Make configurable via query hints or database settings:
```rust
// Could be part of a QueryOptions struct
pub struct QueryOptions {
    pub max_path_hops: usize,
    pub timeout_ms: Option<u64>,
}
```

---

### 9. Regex DoS Potential

**Location:** `executor.rs:1594-1603`

User-provided regex patterns are compiled without limits:
```rust
BinaryOperator::RegexMatch => match (&left_val, &right_val) {
    (PropertyValue::String(text), PropertyValue::String(pattern)) => {
        match regex::Regex::new(pattern) {  // No timeout or complexity limit
            Ok(re) => Ok(PropertyValue::Bool(re.is_match(text))),
```

Pathological regex patterns (e.g., `(a+)+$`) can cause catastrophic backtracking.

**Recommendation:** Use `regex::RegexBuilder` with `size_limit`:
```rust
let re = regex::RegexBuilder::new(pattern)
    .size_limit(1024 * 1024)  // 1MB compiled size limit
    .build()?;
```

---

### 10. Tight Coupling in PathConstraints

**Location:** `executor.rs:35-139`

The `extract_path_constraints` function assumes specific variable names (`source_var`, `target_var`) and requires callers to know which variables represent path endpoints:

```rust
fn extract_path_constraints(
    predicate: &Expression,
    source_var: &str,  // Caller must know these
    target_var: &str,
) -> PathConstraints
```

This tightly couples constraint extraction to the shortest path execution flow.

**Recommendation:** Make constraint extraction generic over variable roles:
```rust
fn extract_property_constraints(
    predicate: &Expression,
    variable_roles: &HashMap<&str, ConstraintRole>,
) -> HashMap<String, PropertyConstraints>
```

---

## Additional Observations

### Good Patterns Found
- Clean error hierarchy with `thiserror`
- Builder pattern for `Binding` (`with_node`, `with_edge`, `with_path`)
- Schema versioning for migrations
- Comprehensive test coverage in each module
- Good separation between parser grammar (pest) and AST construction

### Minor Issues Not in Top 10
- No prepared statement caching for frequently-used queries
- Expression evaluation re-walks AST for each row (could cache compiled expressions)
- `PropertyValue::Float` comparison uses `f64::EPSILON` which may be too strict for large values
- Some `let _ = node_id;` statements that do nothing (line 349)

### Performance Notes
- BFS path enumeration can be exponential for k>1 shortest paths
- No index selection planning (always scans by first label)
- HashMap lookups in bindings repeated per expression evaluation
