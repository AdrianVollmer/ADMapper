# Async Choke Points Computation

**Criticality: High** | **Complexity: High**

## Problem

Edge betweenness centrality (choke points) is O(V*E) even with sampling.
For large graphs, this blocks the API request for seconds, causing:
- UI timeout/spinner
- Poor user experience
- Server thread blocked

Current e2e benchmark: 3124ms (>3000x slower than FalkorDB)

## Solution

### Background Computation Model

1. **Trigger on import**: When graph data is imported, queue choke points
   computation as a background job

2. **Progressive results**: Return cached/stale results immediately,
   compute fresh results in background

3. **Polling API**: Client polls for completion status

```
POST /api/choke-points
  -> 202 Accepted { "job_id": "...", "cached_results": [...] }

GET /api/choke-points/status/{job_id}
  -> 200 { "status": "computing", "progress": 45 }
  -> 200 { "status": "complete", "results": [...] }
```

### Implementation

1. Add background job queue (could use existing query_history table pattern)
2. Spawn computation in separate thread/task
3. Store results in cache with TTL
4. Return stale-while-revalidate on subsequent requests

### Alternative: Approximation Algorithms

Instead of exact Brandes algorithm, use faster approximations:
- Random sampling (already implemented, but could be more aggressive)
- k-path centrality (faster but less accurate)
- Only compute for "interesting" edges (high-degree nodes)

## Complexity

**Background Job Approach:**
- Implementation: High (~300-400 lines)
- New infrastructure: job queue, status tracking, polling API
- Risk: Medium (concurrency, cache invalidation)

**Approximation Approach:**
- Implementation: Low-Medium (~100 lines)
- Already have sampling; could add more aggressive options
- Risk: Low

## Payoff

**Impact: High**
- Choke points is the slowest operation (3124ms)
- Users see spinner for seconds on large graphs
- Background computation eliminates perceived latency

## Recommended Approach

Start with more aggressive sampling (Phase 1), then add background
computation (Phase 2) if still too slow:

**Phase 1**: Reduce sample threshold from 100 to 50, add progress callback
**Phase 2**: Background job with polling API

## Files to Modify

- `src/backend/src/api/handlers.rs` - Add async endpoint
- `src/backend/src/api/core.rs` - Add job queue
- `src/crustdb/src/query/executor/algorithms.rs` - Add progress callback
- `src/frontend/components/insights.ts` - Add polling UI
