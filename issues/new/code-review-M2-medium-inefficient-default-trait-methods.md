# Inefficient default trait methods in DatabaseBackend

## Severity: MEDIUM

## Problem

`src/backend/src/db/backend.rs` (lines 154-314) provides default
implementations for three methods that load ALL edges or ALL nodes:

- `get_node_relationship_counts()` — O(n) in total relationships
- `is_member_of()` — loads all edges, builds adjacency, BFS
- `find_membership_by_sid_suffix()` — loads all nodes AND all edges

These emit `tracing::warn!` at runtime but there's no compile-time signal
that a backend should override them. If a backend forgets, performance
silently degrades.

## Solution

Either:
1. Remove default implementations, making them required — forces each
   backend to provide an efficient version.
2. Or mark them with a `#[deprecated]` attribute and add integration tests
   that verify performance characteristics.

Option 1 is preferred since the defaults are documented as intentionally
inefficient fallbacks.
