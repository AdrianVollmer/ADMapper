# Hardcoded admin relationship types repeated in 4+ places

## Severity: CRITICAL

## Problem

The admin relationship type list `['AdminTo', 'GenericAll', 'GenericWrite',
'Owns', 'WriteDacl', 'WriteOwner', 'AllExtendedRights',
'ForceChangePassword', 'AddMember']` is hardcoded directly in Cypher query
strings in:

- `src/backend/src/db/falkordb.rs` (lines ~799, ~874)
- `src/backend/src/db/neo4j.rs` (lines ~661, ~712)

This is despite `ADMIN_RELATIONSHIP_TYPES` already existing as a constant in
`src/backend/src/db/types.rs:76`.

Adding a new admin relationship type requires updating 4+ separate string
literals in Cypher queries. A typo in any one of them would silently produce
wrong results.

## Solution

Build the Cypher filter string dynamically from the
`ADMIN_RELATIONSHIP_TYPES` constant. For example:

```rust
fn admin_types_cypher_list() -> String {
    let types: Vec<String> = ADMIN_RELATIONSHIP_TYPES
        .iter()
        .map(|t| format!("'{}'", t))
        .collect();
    format!("[{}]", types.join(", "))
}
```

Use this helper wherever the admin types list appears in queries.
