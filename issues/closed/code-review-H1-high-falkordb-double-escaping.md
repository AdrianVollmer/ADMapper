# Double escaping in FalkorDB string interpolation

## Severity: HIGH

## Problem

In `src/backend/src/db/falkordb.rs` (lines 380-398), properties are
serialized with `serde_json::to_string()` which handles escaping, and then
manually escaped again with `.replace('\\', "\\\\").replace('\'', "\\'")`.

This double-escaping can corrupt data containing special characters (e.g.
backslashes in Windows paths, quotes in display names).

Neo4j avoids this entirely by using parameterized queries.

## Solution

Switch FalkorDB to parameterized queries where possible. If the FalkorDB
client doesn't support parameterized queries for all operations, at minimum
remove the manual escaping when `serde_json::to_string()` is already used,
since serde already handles JSON string escaping.

This is closely related to the C1 duplication issue — once backends share a
common base, the safer Neo4j approach can be adopted for both.
