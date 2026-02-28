# Cache API

Manage the query result cache.

## GET /api/cache/stats

Get cache statistics.

**Response:**

```json
{
  "entries": 42,
  "hits": 150,
  "misses": 28,
  "hit_rate": 0.84,
  "memory_bytes": 1048576
}
```

| Field | Type | Description |
|-------|------|-------------|
| `entries` | number | Number of cached queries |
| `hits` | number | Cache hit count |
| `misses` | number | Cache miss count |
| `hit_rate` | number | Hit ratio (0.0 - 1.0) |
| `memory_bytes` | number | Approximate memory usage |

## POST /api/cache/clear

Clear all cached query results.

**Response:**

```json
{
  "success": true,
  "cleared": 42
}
```

| Field | Type | Description |
|-------|------|-------------|
| `success` | boolean | Operation succeeded |
| `cleared` | number | Number of entries cleared |

## Cache Behavior

- Query results are cached automatically
- Cache is invalidated when the graph is modified (imports, CREATE, SET, DELETE)
- Identical queries return cached results
- Cache persists for the session duration
