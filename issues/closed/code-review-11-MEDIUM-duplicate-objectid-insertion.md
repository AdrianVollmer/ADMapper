# Duplicate objectid insertion — copy-paste artifact

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

Both `neo4j.rs:186-187` and `falkordb.rs:163-164` insert the same key twice:

```rust
props.insert("objectid".to_string(), json!(node.id));
props.insert("objectid".to_string(), json!(node.id));
```

The comment says "include both objectid (BloodHound standard) and objectid
(internal standard)" — but it's literally the same key, so the second insert
is a no-op overwrite. This looks like a copy-paste remnant from when two
distinct keys existed (e.g., `objectid` and `object_id`).

Similarly, `neo4j.rs:91-92` and `falkordb.rs:254-257` do redundant
double-lookups: `.get("objectid").or_else(|| .get("objectid"))`.

The CrustDB backend (`nodes.rs:88`) correctly has only one insertion.

## Solution

Remove the duplicate lines in `neo4j.rs` and `falkordb.rs`. Remove the
redundant `.or_else` fallbacks.
