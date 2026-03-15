# Query API

Execute Cypher queries against the graph.

## POST /api/graph/query

Execute a Cypher query.

**Request:**

```json
{
  "query": "MATCH (u:User) WHERE u.admincount = true RETURN u.name"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | Yes | Cypher query |
| `extract_graph` | boolean | No | Extract graph structure from path results (default: `false`) |
| `background` | boolean | No | Mark as background query, excluded from back navigation (default: `false`) |
| `sync` | boolean | No | Wait for completion instead of returning async mode (default: `false`) |
| `language` | string | No | Query language override (defaults to backend's default) |

**Response:**

The response has two modes, indicated by the `mode` field.

**Sync mode** -- the query completed inline (either fast enough or `sync: true`
was set):

```json
{
  "mode": "sync",
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "columns": ["u.name"],
  "rows": [
    ["ADMIN@CORP.LOCAL"],
    ["SYSADMIN@CORP.LOCAL"]
  ],
  "execution_time_ms": 45
}
```

**Async mode** -- the query is still running. Subscribe to progress events
via the progress endpoint:

```json
{
  "mode": "async",
  "query_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

When `extract_graph` is set, sync mode responses include graph data instead of
rows:

```json
{
  "mode": "sync",
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "nodes": [...],
  "relationships": [...],
  "execution_time_ms": 120
}
```

**Errors:**

| Status | Description |
|--------|-------------|
| 400 | Invalid query syntax |
| 503 | No database connected |

## GET /api/query/progress/:id

Stream query progress for long-running queries via SSE.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `id` | string | Query ID |

**Response:**

`Content-Type: text/event-stream`

```
data: {"progress": 50, "rows_processed": 500}

data: {"complete": true, "result": {...}}
```

## Query Syntax

ADMapper uses CrustDB's Cypher implementation. See the [CrustDB documentation](https://example.com/crustdb) for supported features.

### Supported Clauses

- `MATCH` - Pattern matching
- `WHERE` - Filtering
- `RETURN` - Result projection
- `ORDER BY` - Sorting
- `LIMIT` / `SKIP` - Pagination
- `CREATE` - Create nodes/relationships
- `SET` - Update properties
- `DELETE` - Remove nodes/relationships

### Examples

Count users:

```cypher
MATCH (u:User) RETURN count(u) AS total
```

Find domain admins:

```cypher
MATCH (u:User)-[:MemberOf*1..]->(g:Group)
WHERE g.name CONTAINS 'Domain Admins'
RETURN DISTINCT u.name
```

Shortest path:

```cypher
MATCH p = SHORTEST (src:User)-[:MemberOf|AdminTo|HasSession*1..]->(dst:Group)
WHERE src.name = 'JSMITH@CORP.LOCAL'
  AND dst.name = 'Domain Admins@CORP.LOCAL'
RETURN p
```

## Caching

Query results are cached by default. See the [Cache API](cache.md) for cache management.
