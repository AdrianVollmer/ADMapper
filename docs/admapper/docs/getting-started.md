# Getting Started

This guide covers installation and basic usage of ADMapper.

## Installation

### Pre-built Binaries

Download the latest release from the [releases
page](https://github.com/AdrianVollmer/ADMapper/releases) for your
platform:

- **Windows**: `admapper-windows-amd64.exe`
- **macOS**: `admapper-macos-universal`
- **Linux**: `admapper-linux-amd64`

### Building from Source

Requirements:

- Rust 1.85+ (edition 2024)
- Node.js 18+ (for frontend)

On Debian/Ubuntu, install the build dependencies for Tauri:

``` bash
sudo apt-get install -y \
  libwebkit2gtk-4.1-dev \
  libssl-dev \
  libgtk-3-dev \
  libayatana-appindicator3-dev \
  librsvg2-dev
```

``` bash
# Clone the repository
git clone https://github.com/AdrianVollmer/ADMapper.git
cd ADMapper

# Build the frontend (assets are embedded in the final binary)
cd src/frontend
npm install
npm run build

# Build the backend
cd ../backend
cargo build --release
```

## Running ADMapper

### Desktop Mode

Launch the application directly:

``` bash
./admapper
```

This opens a native window with the graph visualization interface.

### Headless Mode

Run as a web server for remote access:

``` bash
./admapper --headless --port 9191 crustdb://./data.db
```

Options:

- `--headless`: Run without GUI
- `--port <PORT>`: HTTP server port (default: 9191)
- `--bind <ADDR>`: Bind address (default: 127.0.0.1)

Access the web interface at `http://localhost:9191`.

### Docker

There is also a Docker image you can run. Only headless mode is
supported. Mount a data directory for usage with CrustDB:

``` bash
docker run --rm -it --init -p 9191:9191 -v ./data:/data \
    ghcr.io/adrianvollmer/admapper crustdb:///data/admapper.db
```

Environment variables:

- `ADMAPPER_HOST`: Bind address (default: `0.0.0.0`)
- `ADMAPPER_PORT`: HTTP server port (default: `9191`)
- `RUST_LOG`: Log level (default: `info`)


### Alternative Databases

See [Database Backends](backends.md#running-with-docker) for how to run ADMapper
alongside Neo4j or FalkorDB.

## Importing Data

ADMapper imports BloodHound collection data. Collect data from your AD
environment using:

- [SharpHound](https://github.com/BloodHoundAD/SharpHound) (Windows) --
  greatly preferred, most complete and reliable collector
- [BloodHound.py](https://github.com/dirkjanm/BloodHound.py) (Python)
- [RustHound](https://github.com/NH-RED-TEAM/RustHound) (Rust)

### Import via UI

1.  Click the import button in the toolbar
2.  Select your BloodHound ZIP file
3.  Wait for the import to complete

### Import via API

``` bash
curl -X POST http://localhost:9191/api/import \
  -F "file=@bloodhound_data.zip"
```

## First Query

After importing data, try these queries:

### Find Domain Admins

``` cypher
MATCH (g:Group)
WHERE g.objectid ENDS WITH '-512'
RETURN g
```

### Show users with sessions

``` cypher
MATCH p=(u:User)-[:HasSession]->(c:Computer)
RETURN p
LIMIT 10
```

## Next Steps

- [Importing Data](guide/importing.md) - Detailed import options
- [Graph Navigation](guide/navigation.md) - Explore the graph
  interactively
- [Running Queries](guide/queries.md) - Cypher query examples
- [Security Insights](guide/insights.md) - Automated security analysis
