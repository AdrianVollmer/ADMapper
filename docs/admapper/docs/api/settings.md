# Settings API

Manage application settings.

## GET /api/settings

Get current settings.

**Response:**

```json
{
  "theme": "dark",
  "graph": {
    "layout": "force",
    "node_size": "medium",
    "show_labels": true,
    "edge_style": "curved"
  },
  "query": {
    "default_limit": 100,
    "timeout_ms": 30000
  }
}
```

## POST /api/settings

Update settings.

**Request:**

```json
{
  "theme": "light",
  "graph": {
    "show_labels": false
  }
}
```

Settings are merged with existing values. Only include fields you want to change.

**Response:**

```json
{
  "success": true,
  "settings": {
    "theme": "light",
    "graph": {
      "layout": "force",
      "node_size": "medium",
      "show_labels": false,
      "edge_style": "curved"
    },
    "query": {
      "default_limit": 100,
      "timeout_ms": 30000
    }
  }
}
```

## Settings Reference

### Theme

| Value | Description |
|-------|-------------|
| `dark` | Dark mode (default) |
| `light` | Light mode |

### Graph Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `layout` | string | `force` | Layout algorithm: `force`, `hierarchical`, `circular` |
| `node_size` | string | `medium` | Node size: `small`, `medium`, `large` |
| `show_labels` | boolean | `true` | Display node labels |
| `edge_style` | string | `curved` | Relationship style: `straight`, `curved` |

### Query Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_limit` | number | `100` | Default LIMIT for queries |
| `timeout_ms` | number | `30000` | Query timeout in milliseconds |
