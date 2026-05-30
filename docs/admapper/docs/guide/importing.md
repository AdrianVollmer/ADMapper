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

### Automatic Tier Assignment

After import completes, ADMapper automatically assigns tiers to nodes based on
well-known group membership (using RID suffix matching):

- **Tier 0** — the group objects themselves and direct members of all well-known
  privileged groups (Domain Admins, Domain Controllers, Enterprise Admins,
  Schema Admins, Administrators, Account Operators, Backup Operators, etc.)
- **Tier 2** — the Domain Computers group object and its direct members
- **Tier 3** — direct members of Domain Users

These assignments are applied in priority order: tier 0 is set first, so a
Domain Admin who is also a member of Domain Users receives tier 0, not tier 3.

Tier 1 (member servers, application servers) has no automatic assignment and
must be classified manually using the **Edit Tiers** modal.

## Clearing Data

To start fresh, use the clear button in the database menu.
