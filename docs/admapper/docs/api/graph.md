# Graph API

Query and manipulate the graph.

## GET /api/graph/stats

Get basic graph statistics.

**Response:**

```json
{
  "node_count": 1500,
  "edge_count": 8200
}
```

## GET /api/graph/detailed-stats

Get detailed statistics including counts by label.

**Response:**

```json
{
  "node_count": 1500,
  "edge_count": 8200,
  "nodes_by_label": {
    "User": 850,
    "Computer": 200,
    "Group": 350,
    "Domain": 1,
    "GPO": 50,
    "OU": 49
  },
  "edges_by_type": {
    "MemberOf": 3500,
    "HasSession": 1200,
    "AdminTo": 800,
    "GenericAll": 150,
    "WriteDacl": 100
  }
}
```

## POST /api/graph/clear

Clear all nodes and edges from the graph.

**Response:**

```json
{
  "success": true
}
```

## GET /api/graph/node/:id

Get a node by ID.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `id` | string | Node ID (internal or object ID) |

**Response:**

```json
{
  "id": 42,
  "labels": ["User"],
  "properties": {
    "name": "JSMITH@CORP.LOCAL",
    "displayname": "John Smith",
    "enabled": true,
    "admincount": false,
    "hasspn": false,
    "objectid": "S-1-5-21-..."
  }
}
```

**Errors:**

| Status | Description |
|--------|-------------|
| 404 | Node not found |

## GET /api/graph/node/:id/counts

Get relationship counts for a node.

**Response:**

```json
{
  "incoming": 5,
  "outgoing": 12
}
```

## GET /api/graph/node/:id/connections/:direction

Get connected nodes and edges.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `id` | string | Node ID |
| `direction` | string | `incoming`, `outgoing`, or `both` |

**Query Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | number | 100 | Maximum results |
| `offset` | number | 0 | Pagination offset |

**Response:**

```json
{
  "nodes": [
    {
      "id": 43,
      "labels": ["Group"],
      "properties": {
        "name": "Domain Admins@CORP.LOCAL"
      }
    }
  ],
  "edges": [
    {
      "id": 100,
      "type": "MemberOf",
      "source": 42,
      "target": 43,
      "properties": {}
    }
  ]
}
```

## GET /api/graph/search

Search for nodes by name or property.

**Query Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `limit` | number | No | Maximum results (default: 20) |
| `labels` | string | No | Comma-separated label filter |

**Response:**

```json
{
  "results": [
    {
      "id": 42,
      "labels": ["User"],
      "name": "JSMITH@CORP.LOCAL",
      "displayname": "John Smith"
    }
  ]
}
```

## GET /api/graph/path

Find shortest path between two nodes.

**Query Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source` | string | Yes | Source node ID or name |
| `target` | string | Yes | Target node ID or name |
| `max_depth` | number | No | Maximum path length (default: 10) |

**Response:**

```json
{
  "found": true,
  "path": {
    "nodes": [
      {"id": 42, "labels": ["User"], "properties": {...}},
      {"id": 43, "labels": ["Group"], "properties": {...}},
      {"id": 44, "labels": ["Group"], "properties": {...}}
    ],
    "edges": [
      {"id": 100, "type": "MemberOf", "source": 42, "target": 43},
      {"id": 101, "type": "MemberOf", "source": 43, "target": 44}
    ]
  }
}
```

When no path exists:

```json
{
  "found": false,
  "path": null
}
```

## GET /api/graph/insights

Get security insights.

**Response:**

```json
{
  "high_value_targets": [
    {"id": 50, "name": "Domain Admins@CORP.LOCAL", "labels": ["Group"]}
  ],
  "kerberoastable": [
    {"id": 60, "name": "SVC_SQL@CORP.LOCAL", "labels": ["User"]}
  ],
  "asrep_roastable": [
    {"id": 70, "name": "LEGACY_USER@CORP.LOCAL", "labels": ["User"]}
  ],
  "unconstrained_delegation": [
    {"id": 80, "name": "SERVER01.CORP.LOCAL", "labels": ["Computer"]}
  ]
}
```

## GET /api/graph/choke-points

Get edges with high betweenness centrality.

**Query Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | number | 10 | Maximum results |

**Response:**

```json
{
  "choke_points": [
    {
      "edge": {
        "id": 150,
        "type": "AdminTo",
        "source": 42,
        "target": 80
      },
      "source_name": "JSMITH@CORP.LOCAL",
      "target_name": "SERVER01.CORP.LOCAL",
      "centrality": 0.85
    }
  ]
}
```

Higher centrality values indicate edges that many shortest paths pass through.
