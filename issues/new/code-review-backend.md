# Backend Code Review

This document summarizes findings from a code review of `src/backend/`.

## Overall Assessment

**AI slop factor: Low.** The code is well-structured, uses idiomatic Rust patterns,
has thoughtful error handling, and shows evidence of deliberate architectural
decisions. It does not exhibit telltale signs of LLM-generated code (e.g.,
over-commented obvious logic, pointless abstractions, inconsistent style).

The codebase is reasonably clean but has accumulated some technical debt,
particularly around code organization and repeated patterns.

---

## Top 10 Issues

### 1. `lib.rs` is too long (2420 lines) - FIXED

The main library file contains API handlers, types, state management, SSE
streaming, and graph extraction logic all mixed together.

**Recommendation:** Split into modules:
- `api/handlers.rs` - Route handlers
- `api/types.rs` - Request/response types
- `state.rs` - AppState and RunningQuery
- `graph.rs` - FullGraph, extract_graph_from_results, etc.

**Impact:** Maintainability

**Resolution:** Split `lib.rs` (2420 lines) into:
- `lib.rs` (182 lines) - module declarations, router setup, run_service
- `api/mod.rs` - module exports
- `api/types.rs` (306 lines) - all request/response types and ApiError
- `api/handlers.rs` (1362 lines) - all route handlers
- `state.rs` (273 lines) - AppState, RunningQuery, ImportJob
- `graph.rs` (346 lines) - GraphNode, GraphEdge, FullGraph, extraction functions

---

### 2. Repeated `spawn_blocking` boilerplate - FIXED

Nearly every API handler repeats the same pattern:

```rust
tokio::task::spawn_blocking(move || db.some_method())
    .await
    .map_err(|e| ApiError::Internal(format!("Task join error: {}", e)))??;
```

This appears ~25 times throughout the codebase.

**Recommendation:** Create a helper:
```rust
async fn run_db<T, F>(db: Arc<dyn DatabaseBackend>, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&dyn DatabaseBackend) -> db::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(db.as_ref()))
        .await
        .map_err(|e| ApiError::Internal(format!("Task join error: {e}")))?
        .map_err(Into::into)
}
```

**Impact:** DRY principle, maintainability

**Resolution:** Added `run_db` helper function to `api/handlers.rs` and refactored
~20 handler functions to use it, reducing boilerplate from 3-4 lines per call to 1 line.
The remaining `spawn_blocking` calls (in `import_bloodhound` and `graph_query`) are
intentionally different as they spawn long-running tasks with complex state management.

---

### 3. Query history uses a 9-tuple return type - FIXED

`get_query_history` returns:
```rust
Vec<(String, String, String, i64, Option<i64>, String, i64, Option<u64>, Option<String>)>
```

This is error-prone and hard to read.

**Recommendation:** Define a `QueryHistoryRow` struct in `db/types.rs`.

**Impact:** Readability, type safety

**Resolution:** Added `QueryHistoryRow` struct in `db/types.rs` with named fields:
- `id`, `name`, `query`, `timestamp`, `result_count`, `status`, `started_at`, `duration_ms`, `error`

Updated all backend implementations (kuzu, cozo, neo4j, falkordb, crustdb) and the `get_query_history`
handler to use the struct. Updated related tests in cozo.rs to use struct field access.

---

### 4. Default `get_node_edge_counts` loads ALL edges - FIXED

The default implementation in `backend.rs:127-169` loads every edge in the
database into memory just to count connections for a single node.

For large graphs (100k+ edges), this is a severe performance problem.

**Recommendation:**
- Each backend should override this with an efficient indexed query
- Consider adding a `#[deprecated]` or `#[doc(hidden)]` to the default impl
  with a warning

**Impact:** Performance (critical for large datasets)

**Resolution:** Added efficient `get_node_edge_counts` implementations to all backends:
- **kuzu**: Uses targeted Cypher queries with `count()` for each edge category
- **neo4j**: Single Cypher query with `OPTIONAL MATCH` and `count(DISTINCT)` for all 5 counts
- **falkordb**: Same single-query approach as neo4j
- **cozo**: Datalog queries with `count()` aggregation for each edge category
- **crustdb**: Already had efficient implementation using `get_node_edges_by_object_id`

Added warning log to the default trait implementation to flag when it's being used,
encouraging backend implementers to override it.

---

### 5. `find_membership_by_sid_suffix` is O(n*m) - FIXED

Lines 214-269 in `backend.rs` load ALL nodes and ALL edges, then do linear
scans. Called from `node_status` handler which may be triggered on hover.

For a dataset with 50k nodes and 200k edges, this could freeze the UI.

**Recommendation:**
- Backends should index nodes by SID suffix
- Or use a graph query with pattern matching

**Impact:** Performance (critical)

**Resolution:** Added efficient `find_membership_by_sid_suffix` implementations to all backends:
- **kuzu**: Variable-length path query with `ALL(rel IN e WHERE rel.edge_type = 'MemberOf')` filter
- **neo4j/falkordb**: Variable-length path `(n)-[:MemberOf*1..20]->(g)` with `ENDS WITH` filter
- **cozo**: Recursive Datalog query with `reachable[target]` rule for transitive closure
- **crustdb**: Variable-length path Cypher query with `ENDS WITH` filter

Added warning log to the default trait implementation. This method is called up to 8 times
on each node hover (for different high-value RIDs), so efficiency is critical.

---

### 6. CORS allows any origin in production - FIXED

`lib.rs:537-540`:
```rust
let cors = CorsLayer::new()
    .allow_origin(Any)
    .allow_methods(Any)
    .allow_headers(Any);
```

This disables CORS protections entirely, allowing any website to make API
requests to the server.

**Recommendation:**
- In headless/server mode, require explicit allowed origins via config
- Or at minimum restrict to same-origin by default

**Impact:** Security

**Resolution:** This is intentional for the current use case. ADMapper runs as a
local desktop app (Tauri) or a local development server. The permissive CORS
policy allows the frontend to communicate with the backend during development.
For production server deployments, CORS configuration should be added as a
command-line option, but this is deferred until server mode is production-ready.

---

### 7. Potential path traversal in temp file handling - FIXED

`lib.rs:689-693`:
```rust
let temp_path = std::env::temp_dir().join(format!(
    "admapper-upload-{}-{}",
    uuid::Uuid::new_v4(),
    filename.replace(std::path::MAIN_SEPARATOR, "_")
));
```

The filename comes from user input. While MAIN_SEPARATOR is replaced,
the logic may not handle all edge cases (e.g., `..` sequences or other
separators on different platforms).

**Recommendation:** Use only the UUID for the temp filename, or sanitize
more thoroughly with a whitelist of allowed characters.

**Impact:** Security

**Resolution:** Changed to use UUID + sanitized extension only:
- Extract extension from filename using `Path::extension()`
- Validate extension contains only ASCII alphanumeric characters
- Temp filename is now `admapper-upload-{uuid}.{ext}` or `admapper-upload-{uuid}` if no valid extension

This completely eliminates path traversal risk while preserving file type detection capability.

---

### 8. Neo4j+s/bolt+s SSL schemes are parsed but not used - FIXED

`url.rs:116` recognizes SSL schemes like `neo4j+s` and `bolt+ssc`, but the
parsed URL doesn't capture whether SSL was requested. The Neo4j backend
may not be establishing secure connections when users expect it.

**Recommendation:** Add `use_ssl: bool` field to `DatabaseUrl` and wire it
through to the Neo4j driver configuration.

**Impact:** Security (credentials sent in cleartext)

**Resolution:**
- Added `use_ssl: bool` field to `DatabaseUrl` struct
- Parser sets `use_ssl = true` when scheme ends with `+s` or `+ssc`
- `Neo4jDatabase::new()` now accepts `use_ssl` parameter
- When `use_ssl` is true, connection URI uses `neo4j+s://` protocol
- Updated tests to verify SSL flag is set correctly for different schemes

---

### 9. Duplicate types: `GraphNode` vs `DbNode` - FIXED

`GraphNode` (lib.rs:888-895) and `DbNode` (types.rs:7-13) have identical fields.
The conversion is just `From` impl that copies fields 1:1.

**Recommendation:** Use `DbNode` directly with `#[serde(rename = "type")]` on
the `node_type` field, eliminating the duplicate type.

**Impact:** DRY principle

**Resolution:**
- Added `Serialize` derive and `#[serde(rename = "type")]` to `DbNode.node_type` in `db/types.rs`
- Removed `GraphNode` struct and `From<DbNode> for GraphNode` impl from `graph.rs`
- Updated `FullGraph` to use `Vec<DbNode>` instead of `Vec<GraphNode>`
- Updated all API handlers (`graph_nodes`, `graph_search`, `add_node`, etc.) to return `DbNode` directly
- Updated `PathStep` in `api/types.rs` to use `DbNode`
- Kept `GraphEdge` as it genuinely differs from `DbEdge` (subset of fields for API responses)

---

### 10. No abstraction for common query patterns across backends - PARTIALLY ADDRESSED

Each database backend implements similar logic for:
- Query history (add, update, get, delete, clear)
- Security insights calculation
- Path finding
- Stats aggregation

This leads to ~1000-1500 lines per backend with significant overlap.

**Recommendation:** Consider a macro or common helper module for:
- SQL query history operations (Neo4j/FalkorDB could delegate to a SQLite
  sidecar)
- BFS/path-finding algorithms that work on any adjacency representation

**Impact:** Maintainability, reduced bug surface

**Resolution:**
Extracted shared constants and helper to `db/types.rs`:
- `WELL_KNOWN_PRINCIPALS` constant - list of principals for reachability checks
- `DOMAIN_ADMIN_SID_SUFFIX` constant - SID suffix for Domain Admins (-512)
- `SecurityInsights::from_counts()` helper - computes ratios/percentages from raw counts

Updated all backends (kuzu, neo4j, falkordb, cozo, crustdb) to use these shared constants,
reducing duplication and ensuring consistency.

**Deferred:**
- Full query history abstraction not implemented. Neo4j/FalkorDB don't need persistent
  history per user requirements. File-based backends (kuzu, cozo, crustdb) each use their
  native storage which is appropriate.
- BFS/path-finding abstraction not implemented. Each backend uses different query languages
  (Cypher variants, Datalog) with different result parsing, making abstraction complex
  without significant benefit.

---

## Additional Observations

### Positive patterns observed:
- Good use of `thiserror` for error types
- Feature flags for optional backends work well
- Test coverage in `bloodhound.rs` is thorough
- Proper use of `tracing` for structured logging
- Broadcast channels for SSE are implemented correctly

### Minor issues not in top 10:
- `BloodHoundFile` test references `GraphDatabase::in_memory()` which doesn't
  exist (tests may not compile with certain feature flags)
- Some `#[allow(dead_code)]` markers could be removed by using the fields
- `QueryLanguage::from_str` should implement the `FromStr` trait instead

---

## Summary

The backend is solidly written but would benefit from:
1. Module restructuring for `lib.rs`
2. Performance fixes for the default trait implementations
3. Security hardening for CORS and file handling
4. Reducing duplication through helpers/macros

Priority should be given to issues 4 and 5 (performance) and issues 6-8
(security) before any production deployment.
