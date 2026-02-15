# Rust Code Review

## Overview

The backend consists of ~1,600 lines of Rust across 7 files:
- `main.rs` - CLI entry point (30 lines)
- `lib.rs` - Axum web service and API handlers (823 lines)
- `db/cozo.rs` - CozoDB database layer (682 lines)
- `import/bloodhound.rs` - BloodHound data importer (577 lines)
- `import/types.rs` - Import progress types (72 lines)

The code is generally well-structured and readable. The separation between
database, import, and API layers is appropriate. However, there are
opportunities to reduce duplication and improve type safety.

---

## Refactoring Plan (10 Points)

### 1. Introduce Named Types for Node and Edge Tuples

**Problem:** The tuple `(String, String, String, JsonValue)` appears 20+ times
across `cozo.rs` and `lib.rs`. This is error-prone (which string is which?)
and makes refactoring difficult.

**Solution:** Create dedicated structs in `db/cozo.rs`:

```rust
pub struct DbNode {
    pub id: String,
    pub label: String,
    pub node_type: String,
    pub properties: JsonValue,
}

pub struct DbEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: JsonValue,
}
```

Then implement `From<DbNode>` for `GraphNode` in `lib.rs` to eliminate
repetitive mapping code.

**Files:** `db/cozo.rs`, `lib.rs`

---

### 2. Extract CozoDB Row Parsing Helper

**Problem:** Four functions (`get_all_nodes`, `get_all_edges`, `search_nodes`,
`get_query_history`) share identical row-parsing boilerplate:

```rust
let result = self.db.run_script(...)?;
let json = result.into_json();
let rows = json["rows"].as_array();
let mut items = Vec::new();
if let Some(rows) = rows {
    for row in rows {
        if let (Some(a), Some(b), ...) = (...) {
            items.push(...);
        }
    }
}
```

**Solution:** Create a generic helper:

```rust
fn parse_rows<T, F>(&self, query: &str, parser: F) -> Result<Vec<T>>
where
    F: Fn(&JsonValue) -> Option<T>
```

**Files:** `db/cozo.rs`

---

### 3. Unify flush_nodes() and flush_edges()

**Problem:** `BloodHoundImporter::flush_nodes()` and `flush_edges()` are
nearly identical (18 lines each), differing only in:
- The db method called (`insert_nodes` vs `insert_edges`)
- The progress field updated (`nodes_imported` vs `edges_imported`)

**Solution:** Create a generic `flush_batch()` method that takes a closure
for the insert operation and a mutable reference to the count field.
Alternatively, accept this minor duplication for clarity.

**Files:** `import/bloodhound.rs`

---

### 4. Create Custom API Error Type

**Problem:** Error handling in API handlers is repetitive:

```rust
.map_err(|e| {
    error!(error = %e, "...");
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
})?
```

This pattern appears 15+ times in `lib.rs`.

**Solution:** Create an `ApiError` enum that implements `IntoResponse`:

```rust
enum ApiError {
    Database(DbError),
    BadRequest(String),
    NotFound(String),
}

impl IntoResponse for ApiError { ... }
impl From<DbError> for ApiError { ... }
```

Then handlers can simply use `?` with automatic conversion.

**Files:** `lib.rs` (new `error.rs` module optional)

---

### 5. Extract Graph Building Logic

**Problem:** The pattern of "get nodes by IDs, get edges between them, build
FullGraph" appears in both `graph_path()` and `graph_query()`.

**Solution:** Create a helper function:

```rust
async fn build_subgraph(db: &GraphDatabase, node_ids: &[String])
    -> Result<FullGraph, ApiError>
```

**Files:** `lib.rs`

---

### 6. Make insert_nodes/insert_edges Generic

**Problem:** `insert_nodes()` and `insert_edges()` in `cozo.rs` have
identical structure, only differing in field names and relation name.

**Solution:** Create a generic `import_relation()` helper or accept the
duplication for explicitness. Given the functions are only 30 lines each
and unlikely to change, this is low priority.

**Files:** `db/cozo.rs`

---

### 7. Add Input Validation Layer

**Problem:** Input validation is scattered. For example:
- `graph_search` checks `params.q.len() < 2`
- `get_query_history` clamps pagination values

**Solution:** Use `validator` crate with derive macros on request structs:

```rust
#[derive(Deserialize, Validate)]
struct SearchParams {
    #[validate(length(min = 2))]
    q: String,
    #[validate(range(min = 1, max = 100))]
    limit: usize,
}
```

**Files:** `lib.rs`, add `validator` dependency

---

### 8. Extract BloodHound Edge Extraction into Separate Functions

**Problem:** `extract_edges()` is 185 lines handling many edge types
(Members, Sessions, LocalGroups, ACEs, ContainedBy, AllowedToDelegate,
AllowedToAct, Links, Trusts). This is the longest function in the codebase.

**Solution:** Split into focused helper functions:

```rust
fn extract_member_edges(&self, entity: &JsonValue, object_id: &str) -> Vec<Edge>;
fn extract_session_edges(&self, entity: &JsonValue, object_id: &str) -> Vec<Edge>;
fn extract_ace_edges(&self, entity: &JsonValue, object_id: &str) -> Vec<Edge>;
// etc.
```

**Files:** `import/bloodhound.rs`

---

### 9. Add Tracing Spans to Long Operations

**Problem:** The `shortest_path()` BFS and `search_nodes()` full-scan
operations could benefit from better observability.

**Solution:** Add `#[instrument]` attributes and span events for:
- Number of nodes/edges traversed
- Time spent in BFS vs node lookup
- Search result count before/after filtering

**Files:** `db/cozo.rs`

---

### 10. Consider Connection Pooling for Database

**Problem:** `GraphDatabase` wraps a single `Arc<DbInstance>`. For high
concurrency, this may become a bottleneck.

**Solution:** Evaluate if CozoDB supports connection pooling or if the
current approach is sufficient. The `Arc` wrapper allows shared access,
but write contention could be an issue. Low priority unless performance
issues arise.

**Files:** `db/cozo.rs`

---

## Test Plan

### Unit Tests (db/cozo.rs)

Existing tests cover basic operations. Add:

| Test | Description |
|------|-------------|
| `test_search_nodes_case_insensitive` | Verify case-insensitive search |
| `test_search_nodes_limit` | Verify limit parameter works |
| `test_search_nodes_partial_match` | Verify substring matching |
| `test_shortest_path_direct` | Path between directly connected nodes |
| `test_shortest_path_multi_hop` | Path requiring multiple hops |
| `test_shortest_path_no_path` | No path exists between nodes |
| `test_shortest_path_same_node` | From and to are the same |
| `test_query_history_pagination` | Verify offset/limit work correctly |
| `test_query_history_ordering` | Verify newest-first ordering |
| `test_get_nodes_by_ids_partial` | Some IDs exist, some don't |
| `test_get_edges_between_subset` | Only edges within subset returned |

### Unit Tests (import/bloodhound.rs)

Existing tests cover mapping functions. Add:

| Test | Description |
|------|-------------|
| `test_extract_node_user` | Extract user node from BloodHound JSON |
| `test_extract_node_computer` | Extract computer node |
| `test_extract_node_missing_id` | Handle missing ObjectIdentifier |
| `test_extract_edges_memberof` | Extract MemberOf edges from group |
| `test_extract_edges_sessions` | Extract HasSession edges |
| `test_extract_edges_aces` | Extract ACE permission edges |
| `test_extract_edges_trusts` | Extract domain trust edges |
| `test_import_json_str_users` | Import a users.json file |
| `test_import_json_str_groups` | Import a groups.json file |
| `test_import_json_str_invalid` | Handle malformed JSON |

### Integration Tests (lib.rs / API)

Create `tests/api_tests.rs`:

| Test | Description |
|------|-------------|
| `test_health_check` | GET /api/health returns 200 |
| `test_graph_stats_empty` | Stats on empty database |
| `test_graph_stats_with_data` | Stats after import |
| `test_graph_search_min_length` | Query < 2 chars returns empty |
| `test_graph_search_results` | Valid search returns matches |
| `test_graph_path_found` | Path exists between nodes |
| `test_graph_path_not_found` | No path returns found=false |
| `test_query_history_crud` | Create, read, delete history |
| `test_query_history_pagination` | Page through results |
| `test_import_json_file` | Upload and import JSON |
| `test_import_zip_file` | Upload and import ZIP |
| `test_import_invalid_file` | Reject non-JSON/ZIP files |
| `test_custom_query_valid` | Execute valid CozoDB query |
| `test_custom_query_invalid` | Reject malformed query |

### Test Fixtures

Create `tests/fixtures/` with sample BloodHound data:
- `minimal_users.json` - 2-3 users
- `minimal_groups.json` - 2-3 groups with members
- `minimal_computers.json` - 1-2 computers with sessions
- `test_data.zip` - Combined archive for import testing

### Test Infrastructure

```rust
// tests/common/mod.rs
pub async fn setup_test_app() -> (Router, GraphDatabase) {
    let db = GraphDatabase::in_memory().unwrap();
    let state = AppState::new(db.clone());
    let app = create_router(state);
    (app, db)
}

pub fn load_fixture(name: &str) -> String {
    std::fs::read_to_string(format!("tests/fixtures/{}", name)).unwrap()
}
```

---

## Priority

1. **High:** Points 1, 4 (type safety, error handling)
2. **Medium:** Points 2, 5, 8 (code deduplication)
3. **Low:** Points 3, 6, 7, 9, 10 (minor improvements)

Focus on adding tests first, then refactor with confidence.
