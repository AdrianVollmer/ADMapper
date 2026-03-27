# CrustDB schema version mismatch: create_schema_v1 creates version 3

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

In `storage/schema.rs`:

- `SCHEMA_VERSION` is `6` (line 8)
- `create_schema_v1` inserts version `'3'` (line 62)
- `init_schema` then runs migrations 3→4, 4→5, 5→6 on a brand new database

The function is called `create_schema_v1` but produces schema version 3.
A fresh database runs three unnecessary migrations (JSONB conversion on
empty tables, adding columns that could be in the initial DDL, etc.).

## Solution

Rename `create_schema_v1` to `create_schema` (or `create_schema_v6`) and
have it create the current schema directly, setting the version to
`SCHEMA_VERSION`. Keep the migration functions for upgrading existing
databases from older versions.
