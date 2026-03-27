# flatten_node_properties triplicated across all 3 DB backends

**Severity: HIGH** | **Category: duplicate-code**

## Problem

The `flatten_node_properties` method is implemented separately in all three
database backends with nearly identical logic:

- `db/crustdb/nodes.rs:84-112`
- `db/neo4j.rs:181-211`
- `db/falkordb.rs:158-188`

All three: insert `objectid`, insert `name`, iterate properties, skip nulls
and empty arrays, skip core fields. The only variation: Neo4j/FalkorDB
lowercase property keys, CrustDB does not.

Similarly, `json_to_cypher_props` / `json_value_to_cypher` are triplicated
across the same files, differing only in quote escaping style (`''` vs `\'`).

Also related: `admin_types` HashSet (9 admin relationship types) is
constructed identically in `db/backend.rs:165-176` and
`db/crustdb/backend_impl.rs:109-121`, and appears as string literals in
Neo4j/FalkorDB Cypher queries.

## Solution

1. Extract `flatten_node_properties` as a method on `DbNode` or a free
   function in `db/types.rs`, taking a `lowercase_keys: bool` parameter.
2. Extract `json_to_cypher_props` into a shared utility with a configurable
   escape style.
3. Define `ADMIN_RELATIONSHIP_TYPES` as a constant in `db/types.rs`.
