# Query History API

Manage saved query history.

## GET /api/query-history

Get query history.

**Query Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | number | 50 | Maximum entries |
| `offset` | number | 0 | Pagination offset |

**Response:**

```json
{
  "queries": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "query": "MATCH (u:User) RETURN u.name LIMIT 10",
      "executed_at": "2024-01-15T10:30:00Z",
      "execution_time_ms": 45,
      "row_count": 10
    },
    {
      "id": "660e8400-e29b-41d4-a716-446655440001",
      "query": "MATCH (g:Group) WHERE g.name CONTAINS 'Admin' RETURN g",
      "executed_at": "2024-01-15T10:25:00Z",
      "execution_time_ms": 120,
      "row_count": 5
    }
  ],
  "total": 42
}
```

## POST /api/query-history

Save a query to history (typically done automatically).

**Request:**

```json
{
  "query": "MATCH (n) RETURN count(n)"
}
```

**Response:**

```json
{
  "id": "770e8400-e29b-41d4-a716-446655440002"
}
```

## DELETE /api/query-history/:id

Delete a query from history.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `id` | string | Query history entry ID |

**Response:**

```json
{
  "success": true
}
```

**Errors:**

| Status | Description |
|--------|-------------|
| 404 | Entry not found |

## DELETE /api/query-history

Clear all query history.

**Response:**

```json
{
  "success": true,
  "deleted": 42
}
```
