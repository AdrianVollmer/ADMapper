# Import API

Import BloodHound data into the graph database.

## POST /api/import

Start a data import operation.

**Request:**

`Content-Type: multipart/form-data`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | file | Yes | BloodHound JSON or ZIP file |

**Response:**

```json
{
  "import_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

Use the returned `import_id` to track progress via SSE.

**Errors:**

| Status | Description |
|--------|-------------|
| 400 | Invalid file format |
| 503 | No database connected |

## GET /api/import/progress/:id

Stream import progress via Server-Sent Events.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `id` | string | Import ID from POST /api/import |

**Response:**

`Content-Type: text/event-stream`

Progress events:

```
data: {"stage": "parsing", "progress": 25, "message": "Parsing users..."}

data: {"stage": "importing", "progress": 50, "message": "Importing nodes..."}

data: {"stage": "importing", "progress": 75, "message": "Importing relationships..."}

data: {"complete": true, "stats": {"nodes": 1500, "relationships": 8200}}
```

Error event:

```
data: {"error": "Failed to parse JSON"}
```

**Event Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `stage` | string | Current stage: `parsing`, `importing` |
| `progress` | number | Percentage complete (0-100) |
| `message` | string | Human-readable status |
| `complete` | boolean | True when finished |
| `stats` | object | Final import statistics |
| `error` | string | Error message if failed |

## Example

Using curl:

```bash
# Start import
IMPORT_ID=$(curl -X POST http://localhost:9191/api/import \
  -F "file=@bloodhound_data.zip" | jq -r .import_id)

# Watch progress
curl -N http://localhost:9191/api/import/progress/$IMPORT_ID
```

Using JavaScript:

```javascript
const formData = new FormData();
formData.append('file', fileInput.files[0]);

const response = await fetch('/api/import', {
  method: 'POST',
  body: formData
});
const { import_id } = await response.json();

const events = new EventSource(`/api/import/progress/${import_id}`);
events.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.complete) {
    console.log('Import complete:', data.stats);
    events.close();
  } else if (data.error) {
    console.error('Import failed:', data.error);
    events.close();
  } else {
    console.log(`${data.progress}%: ${data.message}`);
  }
};
```
