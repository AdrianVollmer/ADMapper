# Database API

Manage database connections.

## GET /api/database/status

Get current database connection status.

**Response:**

```json
{
  "connected": true,
  "backend": "crustdb",
  "url": "crustdb:///data/graph.db"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `connected` | boolean | Whether a database is connected |
| `backend` | string | Backend type: `crustdb`, `neo4j`, or `falkordb` |
| `url` | string | Connection URL (may be redacted for security) |

## POST /api/database/connect

Connect to a database.

**Request:**

```json
{
  "url": "crustdb:///path/to/database.db"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | Yes | Database connection URL |

**URL Formats:**

```
crustdb:///path/to/file.db
neo4j://user:password@host:7687
falkordb://host:6379
```

**Response:**

```json
{
  "success": true,
  "backend": "crustdb"
}
```

**Errors:**

| Status | Description |
|--------|-------------|
| 400 | Invalid URL format |
| 500 | Connection failed |

## POST /api/database/disconnect

Disconnect from the current database.

**Response:**

```json
{
  "success": true
}
```
