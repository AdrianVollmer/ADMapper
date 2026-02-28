# Graph Navigation

ADMapper provides an interactive graph visualization for exploring Active Directory relationships.

## Basic Controls

### Mouse

| Action | Effect |
|--------|--------|
| Click node | Select node, show details |
| Click edge | Select edge, show relationship info |
| Click background | Deselect all |
| Drag node | Move node position |
| Drag background | Pan the view |
| Scroll | Zoom in/out |

### Keyboard

| Key | Effect |
|-----|--------|
| `Escape` | Deselect all |
| `+` / `-` | Zoom in/out |
| Arrow keys | Pan the view |

## Node Details

Clicking a node shows its properties in the side panel:

- **Labels**: Node type (User, Computer, Group, etc.)
- **Properties**: All AD attributes
- **Connections**: Incoming and outgoing relationships

## Expanding Nodes

Double-click a node to load its connections. This fetches:

- Direct incoming relationships
- Direct outgoing relationships

Connected nodes appear around the selected node.

## Filtering

### By Node Type

Toggle visibility of node types using the filter panel:

- Show/hide Users
- Show/hide Computers
- Show/hide Groups
- etc.

### By Edge Type

Filter which relationship types are displayed:

- Membership edges
- Session edges
- Dangerous permission edges
- etc.

## Layout

The graph uses a force-directed layout that automatically positions nodes. Controls:

| Action | Effect |
|--------|--------|
| Reset layout | Recompute positions |
| Pin node | Fix node position |
| Unpin all | Release all pinned nodes |

## Search

Use the search bar to find nodes:

```
admin
```

Search matches against:

- Node name
- Distinguished name
- Object ID

Results appear in a dropdown. Click to navigate to the node.

## Highlighting

### Path Highlighting

When viewing a path (from shortest path query or path finding), the path is highlighted:

- Path nodes: Full color
- Path edges: Highlighted
- Other elements: Dimmed

### Neighbor Highlighting

Hovering over a node highlights its direct neighbors.
