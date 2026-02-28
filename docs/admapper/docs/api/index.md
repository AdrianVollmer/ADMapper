# API Reference

ADMapper provides a REST API for programmatic access to all functionality.

## Base URL

When running in headless mode:

```
http://localhost:9191/api
```

## Response Format

All endpoints return JSON. Successful responses have HTTP status 2xx. Errors return appropriate status codes with a JSON body:

```json
{
  "error": "Error message"
}
```

## Endpoints

| Category | Endpoint | Description |
|----------|----------|-------------|
| [Health](#health) | `GET /health` | Server status |
| [Database](database.md) | `/database/*` | Connection management |
| [Import](import.md) | `/import/*` | BloodHound data import |
| [Graph](graph.md) | `/graph/*` | Graph operations |
| [Query](query.md) | `/graph/query` | Cypher queries |
| [Cache](cache.md) | `/cache/*` | Query cache |
| [Settings](settings.md) | `/settings` | Application settings |
| [History](history.md) | `/query-history` | Query history |

## Health

### GET /api/health

Check server status.

**Response:**

```json
{
  "status": "ok"
}
```

## Authentication

The API does not require authentication. Ensure the server is only accessible from trusted networks.

## Server-Sent Events

Some endpoints use SSE for streaming progress updates:

- `GET /api/import/progress/:id` - Import progress
- `GET /api/query/progress/:id` - Query progress

SSE endpoints return `text/event-stream` content type with events in the format:

```
data: {"progress": 50, "message": "Processing..."}

data: {"complete": true, "result": {...}}
```
