# CrustDB: get_incoming/outgoing_connections are massive copy-paste

**Severity: MEDIUM** | **Category: duplicate-code**

## Problem

In `storage/query.rs:570-780`, `get_incoming_connections_by_objectid` and
`get_outgoing_connections_by_objectid` are ~100 lines each, nearly identical.
They differ only in:

- `source` vs `target` in SQL JOIN/WHERE clauses
- Variable naming (`src` vs `tgt`)

The SQL queries, row mapping logic, and node collection code are all
duplicated.

Similarly, `properties_to_json` (in `crustdb/query.rs:128`) and
`props_to_json` (in `crustdb/connections.rs:256`) are identical functions
with different names.

Also, node extraction from CrustDB results is duplicated between
`crustdb/nodes.rs:166` (`extract_db_node_from_result`) and
`crustdb/connections.rs:89` (inline construction).

## Solution

1. Parameterize the connection query with a `direction` enum/string.
2. Remove `props_to_json`, use `properties_to_json` everywhere.
3. Reuse `extract_db_node_from_result` in `connections.rs`.
