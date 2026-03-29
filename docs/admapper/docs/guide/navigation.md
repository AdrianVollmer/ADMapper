# Graph Navigation

ADMapper provides an interactive graph visualization for exploring
Active Directory relationships.

## Basic Controls

### Mouse

| Action             | Effect                                      |
|--------------------|---------------------------------------------|
| Click node         | Select node, show details                   |
| Click relationship | Select relationship, show relationship info |
| Click background   | Deselect all                                |
| Drag node          | Move node position                          |
| Drag background    | Pan the view                                |
| Scroll             | Zoom in/out                                 |

### Keyboard

| Key        | Effect                |
|------------|-----------------------|
| `Escape`   | Deselect all          |
| `+` / `-`  | Zoom in/out           |
| Arrow keys | Pan the view          |
| `Ctrl-M`   | Open magnifying glass |

## Node Details

Clicking a node shows its properties in the side panel:

- **Labels**: Node type (User, Computer, Group, etc.)
- **Properties**: All AD attributes
- **Connections**: Incoming and outgoing relationships
- **Value**: Security-relevant flags such as tier 0 membership, paths to
  privileged groups, ownership status, and whether the account is
  disabled

## Search

Use the search bar to find nodes:

    admin

Search matches against:

- Node name
- Distinguished name
- Object ID

Results appear in a dropdown. Click to navigate to the node.

## Shortest Path

Find the shortest path between two objects by entering search terms in
the respective input fields in the navigation bar.
