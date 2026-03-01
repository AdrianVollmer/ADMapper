# Importing Data

ADMapper imports BloodHound collection data in JSON format (typically packaged as ZIP files).

## Supported Formats

- BloodHound ZIP files containing JSON data
- Individual JSON files (users, computers, groups, domains, etc.)

## Collection Tools

### SharpHound (Windows)

Run SharpHound on a domain-joined Windows machine:

```powershell
.\SharpHound.exe -c All
```

This produces a ZIP file like `20240101_BloodHound.zip`.

### BloodHound.py (Python)

Run from any machine with network access to the domain:

```bash
bloodhound-python -u user -p password -d domain.local -c All
```

This produces individual JSON files. ZIP them before importing:

```bash
zip bloodhound_data.zip *.json
```

## Import Methods

### UI Import

1. Click the import icon in the toolbar
2. Select your ZIP file
3. Monitor progress in the status bar

### API Import

```bash
curl -X POST http://localhost:9191/api/import \
  -F "file=@bloodhound_data.zip"
```

Response:

```json
{
  "job_id": "abc123",
  "status": "processing"
}
```

### Check Progress

```bash
curl http://localhost:9191/api/import/progress/abc123
```

## Import Behavior

### Node Handling

- Nodes are identified by their `object_id` property
- Duplicate imports update existing nodes rather than creating duplicates
- Properties are merged on re-import

### Relationship Handling

- Edges are created between nodes based on relationships in the data
- Missing target nodes create placeholder entries
- Placeholders are updated when the full node data is imported

## Clearing Data

To start fresh, clear the database:

### Via UI

Use the clear button in the database menu.

### Via API

```bash
curl -X POST http://localhost:9191/api/graph/clear
```

## Large Imports

For large environments (100k+ objects):

- CrustDB handles imports efficiently with batch operations
- Import progress is streamed via Server-Sent Events
- The UI remains responsive during background imports
