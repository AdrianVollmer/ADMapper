# Getting Started

This guide covers installation and basic usage of ADMapper.

## Installation

### Pre-built Binaries

Download the latest release from the [releases page](https://github.com/AdrianVollmer/ADMapper/releases) for your platform:

- **Windows**: `admapper-windows.exe`
- **macOS**: `admapper-macos`
- **Linux**: `admapper-linux`

### Building from Source

Requirements:

- Rust 1.75+
- Node.js 18+ (for frontend)

```bash
# Clone the repository
git clone https://github.com/AdrianVollmer/ADMapper.git
cd ADMapper

# Build the backend
cd src/backend
cargo build --release

# Build the frontend
cd ../frontend
npm install
npm run build
```

## Running ADMapper

### Desktop Mode

Launch the application directly:

```bash
./admapper
```

This opens a native window with the graph visualization interface.

### Headless Mode

Run as a web server for remote access:

```bash
./admapper --headless --port 9191 crustdb://./data.db
```

Options:

- `--headless`: Run without GUI
- `--port <PORT>`: HTTP server port (default: 9191)
- `--bind <ADDR>`: Bind address (default: 127.0.0.1)

Access the web interface at `http://localhost:9191`.

## Importing Data

ADMapper imports BloodHound collection data. Collect data from your AD environment using:

- [SharpHound](https://github.com/BloodHoundAD/SharpHound) (Windows)
- [BloodHound.py](https://github.com/dirkjanm/BloodHound.py) (Python)

### Import via UI

1. Click the import button in the toolbar
2. Select your BloodHound ZIP file
3. Wait for the import to complete

### Import via API

```bash
curl -X POST http://localhost:9191/api/import \
  -F "file=@bloodhound_data.zip"
```

## First Query

After importing data, try these queries:

### Count all nodes

```cypher
MATCH (n) RETURN count(n)
```

### Find Domain Admins

```cypher
MATCH (g:Group)
WHERE g.name CONTAINS 'Domain Admins'
RETURN g
```

### Show users with sessions

```cypher
MATCH (u:User)-[:HasSession]->(c:Computer)
RETURN u.name, c.name
LIMIT 10
```

## Next Steps

- [Importing Data](guide/importing.md) - Detailed import options
- [Graph Navigation](guide/navigation.md) - Explore the graph interactively
- [Running Queries](guide/queries.md) - Cypher query examples
- [Security Insights](guide/insights.md) - Automated security analysis
