# Massive code duplication between FalkorDB and Neo4j backends

## Severity: CRITICAL

## Problem

`src/backend/src/db/falkordb.rs` (1052 lines) and `src/backend/src/db/neo4j.rs`
(992 lines) are near-identical copies. Methods like `get_all_nodes`,
`get_all_edges`, `get_nodes_by_ids`, `get_edges_between`, `get_edge_types`,
`get_node_types`, `search_nodes`, `resolve_node_identifier`, `shortest_path`,
`find_paths_to_domain_admins`, and all initialization logic are duplicated
with only minor differences:

- Query execution: string-based (FalkorDB) vs parameterized (Neo4j)
- Result parsing: JSON arrays vs typed Row objects
- String escaping style

Every bug fix or feature change must be manually replicated in both files.
This is the single largest code smell in the codebase — ~2000 lines of
near-duplicate code.

## Solution

Extract shared logic into a common module or base implementation. The
differences (query execution, result parsing) can be abstracted behind a
small trait or generic helper, while the shared query construction,
node/edge mapping, and business logic live in one place.

Possible approach:
1. Create `src/backend/src/db/cypher_backend.rs` with shared query
   construction and result mapping logic.
2. Have FalkorDB and Neo4j implement only the transport layer (how queries
   are sent and results parsed).
3. This should eliminate ~1000 lines of duplication.
