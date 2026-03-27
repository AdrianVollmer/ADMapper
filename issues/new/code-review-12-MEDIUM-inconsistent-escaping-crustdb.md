# Inconsistent SQL/Cypher escaping within CrustDB

**Severity: MEDIUM** | **Category: inconsistency**

## Problem

CrustDB uses two different escaping strategies for single quotes internally:

- `crustdb/nodes.rs:138` and `edges.rs:163`: SQL-style doubling (`''`)
- `crustdb/connections.rs:34`: backslash escaping (`\'`)

Neo4j and FalkorDB consistently use backslash escaping. CrustDB is
inconsistent with itself.

## Solution

Pick one escaping strategy for CrustDB (SQL-style `''` is correct for
SQLite) and apply it consistently. Extract the escaping into a shared helper
to prevent future drift.
