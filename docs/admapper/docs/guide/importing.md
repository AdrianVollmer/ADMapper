# Importing Data

ADMapper imports BloodHound collection data in JSON format (typically
packaged as ZIP files).

## Supported Formats

- BloodHound ZIP files containing JSON data
- Individual JSON files (users, computers, groups, domains, etc.)

## Collection Tools

### SharpHound (Windows)

Run SharpHound on a domain-joined Windows machine:

``` powershell
.\SharpHound.exe -c All
```

This produces a ZIP file like `20240101_BloodHound.zip`.

### BloodHound.py (Python)

SharpHound as been implemented in
[Python](https://github.com/dirkjanm/bloodhound.py). Run from any
machine with network access to the domain:

``` bash
bloodhound-python -u user -p password -d DOMAIN.LOCAL -c All
```

This produces individual JSON files. ZIP them before importing:

``` bash
zip bloodhound_data.zip *.json
```

### RustHound-CE

There is also a Rust-implementation called
[RustHound-CE](https://github.com/g0h4n/RustHound-CE):

``` bash
rusthound-ce -d DOMAIN.LOCAL -u USERNAME@DOMAIN.LOCAL -z
```

## Import Methods

### UI Import

1.  Click on Tools-\>Import BloodHound data...
2.  Select your ZIP file
3.  Monitor progress in the status dialog

## Import Behavior

### Node Handling

- Nodes are identified by their `objectid` property
- Duplicate imports update existing nodes rather than creating
  duplicates
- Properties are merged on re-import

### Relationship Handling

- Edges are created between nodes based on relationships in the data
- Missing target nodes create placeholder entries
- Placeholders are updated when the full node data is imported

## Clearing Data

To start fresh, use the clear button in the database menu.
