# 21 ignored API tests (35% of test suite)

## Severity: HIGH

## Problem

In `src/backend/tests/api_tests.rs`, 21 test functions are marked
`#[ignore]` with the reason "CrustDB MATCH queries can hang in tokio test
context". These cover critical API endpoints:

- `test_graph_all_with_data`
- `test_graph_nodes_with_data`
- `test_graph_edges_with_data`
- Path finding tests
- Node/edge query tests

The entire `/api/graph/*` endpoint suite (except stats and search) is
untested in CI. This is a real coverage gap for core functionality.

## Solution

Investigate why CrustDB MATCH queries hang in the tokio test runtime. Likely
causes:

1. Blocking SQLite calls on the async runtime without `spawn_blocking`
2. Deadlock between the query executor and the tokio test runtime's
   single-threaded executor

Fix the underlying issue so these tests can run in CI. If the fix is
non-trivial, consider running them in a separate test binary with
`#[tokio::test(flavor = "multi_thread")]`.
