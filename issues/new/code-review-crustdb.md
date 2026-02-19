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

### 1. ~~Monolithic executor.rs (2700+ lines)~~ ✅ FIXED

**Location:** `src/crustdb/src/query/executor.rs` (now `src/crustdb/src/query/executor/`)

~~The executor handles too many concerns: pattern matching, expression evaluation, aggregation, BFS traversal, mutations. This makes testing individual features difficult and increases cognitive load.~~

**Fixed:** Split into submodules:
- `executor/mod.rs` - Main entry point, `execute()` function, core types (`Binding`, `Path`, `PathConstraints`)
- `executor/pattern.rs` - Pattern matching (single-node, single-hop, multi-hop, variable-length, shortest path)
- `executor/eval.rs` - Expression evaluation and comparison functions
- `executor/aggregate.rs` - Aggregate function evaluation
- `executor/mutation.rs` - SET and DELETE execution
- `executor/create.rs` - CREATE statement execution
- `executor/result.rs` - Query result building from bindings

---

### 2. ~~N+1 Query Pattern in Edge Retrieval~~ ✅ FIXED

**Location:** `storage.rs:577-597`, `storage.rs:737-774`

~~Functions like `find_edges_by_type`, `find_outgoing_edges`, and `find_incoming_edges` first query edge IDs, then call `get_edge()` for each ID separately. This causes N+1 queries.~~

**Fixed:** Added `collect_edges_from_stmt` helper function that fetches all edge data in a single query using JOINs, matching the pattern used by `collect_nodes_from_stmt`. All three functions (`find_edges_by_type`, `find_outgoing_edges`, `find_incoming_edges`) now use this helper.

---

### 3. ~~Silent Error Swallowing with unwrap_or(0)~~ ✅ FIXED

**Location:** `storage.rs:807-818`, `storage.rs:823-834`

~~Database errors are silently converted to 0, masking potential issues like schema corruption or connection problems.~~

**Fixed:** Changed `count_incoming_edges_by_object_id` and `count_outgoing_edges_by_object_id` to properly propagate database errors using `?` instead of `unwrap_or(0)`.

Note: The `unwrap_or(0)` in `get_schema_version()` (line 63) is intentional - it handles the case where the schema doesn't exist yet during database initialization.

---

### 4. ~~SQL/JSON Path Injection Risk~~ ✅ FIXED

**Location:** `storage.rs:364-377`, `storage.rs:382-397`

~~Property names are interpolated into SQL queries with only single-quote escaping, allowing potential injection.~~

**Fixed:** Added `validate_property_name()` function that ensures property names contain only alphanumeric characters and underscores. Both `find_node_by_property` and `build_property_index` now call this validation before constructing queries. Malicious property names like `')--`, `name.path`, or `name$` are rejected with an `InvalidProperty` error.

Added test `test_property_name_validation` to verify the validation rejects injection attempts.

---

### 5. Query Planner is a Dead Stub ⏸️ DEFERRED

**Location:** `src/crustdb/src/query/planner.rs`

The planner defines a complete operator hierarchy (`PlanOperator`, `FilterPredicate`, etc.) but `plan()` returns `PlanOperator::Empty`. Meanwhile, optimization logic is scattered ad-hoc throughout the executor (`try_optimized_count`, `get_pushable_limit`, `extract_path_constraints`).

**Decision:** Defer to proper implementation rather than deleting. The planner stub has good abstractions and aligns with M9 (Query Optimization) in the roadmap. Implementing it properly would:
- Consolidate scattered optimization logic into structured planner passes
- Make the executor a simple plan interpreter
- Enable future optimizations (cost-based join ordering, index selection)

**See:** `issues/new/query-planner-implementation.md` for detailed implementation plan.

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

### 7. ~~Duplicated Scan Logic with/without Limit~~ ✅ FIXED

**Location:** `storage.rs:571-601` (was duplicated)

~~`find_nodes_by_label` and `find_nodes_by_label_limit` contain similar but not identical SQL. Same for `scan_all_nodes` vs `get_all_nodes_limit`.~~

**Fixed:** Made the simple functions delegate to their `_limit` counterparts:
- `scan_all_nodes()` now calls `get_all_nodes_limit(None)`
- `find_nodes_by_label(label)` now calls `find_nodes_by_label_limit(label, None)`

This removes ~30 lines of duplicate SQL while maintaining the same public API.

---

### 8. ~~Magic Constant DEFAULT_MAX_HOPS~~ ✅ FIXED

**Location:** `executor/pattern.rs:14-25`

~~Hardcoded value for shortest path traversal buried inside a function with no documentation.~~

**Fixed:**
- Created documented module-level constant `DEFAULT_MAX_PATH_DEPTH = 10000`
- Added comprehensive documentation explaining its purpose and how to override
- Consolidated the inconsistent `100` default in variable-length patterns to use the same constant
- Constant is now `pub` so it's visible in documentation and could be referenced externally

A full `QueryOptions` struct would require changing function signatures throughout the executor; this is a reasonable intermediate step that makes the limit discoverable and documented.

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
