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

Clear all nodes and relationships from the graph.

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

Get connected nodes and relationships.

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
  "relationships": [
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
    "relationships": [
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

Get security insights including domain admin analysis and reachability from
well-known principals.

**Response:**

```json
{
  "effective_da_count": 42,
  "real_da_count": 5,
  "da_ratio": 8.4,
  "total_users": 1500,
  "effective_da_percentage": 2.8,
  "reachability": [
    {
      "principal_name": "Domain Users",
      "principal_sid": "S-1-5-21-...-513",
      "reachable_count": 120
    }
  ],
  "effective_das": [
    ["S-1-5-21-...", "JSMITH@CORP.LOCAL", 3]
  ],
  "real_das": [
    ["S-1-5-21-...", "ADMIN@CORP.LOCAL"]
  ]
}
```

## GET /api/graph/choke-points

Get relationships with high betweenness centrality. Returns the top 50 choke
points and the top 50 unexpected choke points (where the source is neither a
tier-0 target nor a domain infrastructure object).

**Response:**

```json
{
  "choke_points": [
    {
      "source_id": "S-1-5-21-...",
      "source_name": "JSMITH@CORP.LOCAL",
      "source_label": "User",
      "target_id": "S-1-5-21-...",
      "target_name": "SERVER01.CORP.LOCAL",
      "target_label": "Computer",
      "rel_type": "AdminTo",
      "betweenness": 4250.5,
      "source_tier": 3
    }
  ],
  "unexpected_choke_points": [],
  "total_edges": 8200,
  "total_nodes": 1500
}
```

Higher betweenness values indicate relationships that more shortest paths pass through.
