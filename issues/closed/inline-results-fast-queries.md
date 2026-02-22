# Inline Results for Fast Queries

## Problem

There is a race condition in the async query system where fast queries (< 10ms) can complete before the client even makes its first polling request. Currently, we use TTL-based cleanup to keep completed queries in the map for 2 minutes, but this is a workaround rather than a solution.

The current flow:
1. Client sends POST /api/graph/query
2. Server returns `{ query_id }` immediately
3. Server spawns query execution in background
4. Client opens SSE connection to /api/query/progress/:id
5. If query completes before step 4, client sees cached final_state

For very fast queries (COUNT, simple lookups), the overhead of:
- Generating a UUID
- Setting up broadcast channels
- Establishing SSE connection
- Polling for results

...is much larger than the query itself.

## Proposed Solution

Detect fast queries and return results inline in the initial POST response.

### Detection Approach

1. **Query Analysis**: Before execution, analyze the query to predict if it's fast:
   - COUNT queries without complex filters
   - Simple LIMIT 1 queries
   - Node lookups by ID
   - Edge type counts

2. **Threshold-based**: Execute with a short timeout (e.g., 5ms), and if the query completes within that window, return results inline instead of switching to async mode.

### API Changes

The POST /api/graph/query response would become:

```json
// Fast query - inline results
{
  "mode": "sync",
  "results": { "columns": [...], "rows": [...] },
  "duration_ms": 2
}

// Slow query - async mode
{
  "mode": "async",
  "query_id": "abc-123"
}
```

The client would check `mode` and either:
- Use results directly (sync)
- Subscribe to SSE progress (async)

### Implementation

```rust
// In graph_query handler

// Try to execute query with short timeout
let fast_timeout = std::time::Duration::from_millis(5);
let fast_result = tokio::time::timeout(fast_timeout, async {
    // Execute query directly (not in spawn_blocking for fast path)
    db.run_custom_query(&query)
}).await;

if let Ok(Ok(results)) = fast_result {
    // Query completed fast - return inline
    return Ok(Json(QueryResponse::Sync { results, duration_ms }));
}

// Query took too long or errored - switch to async mode
// ... existing async flow ...
```

## Benefits

1. **Reduced latency** for simple queries (no SSE overhead)
2. **Simpler client code** for common cases
3. **Less server resource usage** (no broadcast channels, no cleanup)
4. **Better UX** for COUNT and aggregate queries

## Considerations

- Need to handle queries that start fast but slow down (timeout must be strict)
- Client needs to handle both response modes
- Should still support query cancellation for sync queries (via request cancellation)
- May want configurable threshold

## Related

- TTL-based cleanup (current workaround) in lib.rs
- COUNT optimization in crustdb executor
- SQL pushdown detection could help identify fast queries
