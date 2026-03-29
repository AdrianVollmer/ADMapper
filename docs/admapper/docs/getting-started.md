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

## Alternative Databases

CrustDB is an experimental embedded graph database which supports
[openCypher](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf).
It uses SQLite under the hood and is written in Rust. It does not aim at
competing with a commercial product like Neo4j in terms of performance.
Especially for larger datasets, it is recommended to use Neo4j or
FalkorDB.

It's easiest to run these in a container using Docker or Podman.

``` bash
docker run --rm -it --init -p 7687:7687 \
    --userns $UID=7474 \
    -e NEO4J_AUTH=none -v ./data:/data \
    neo4j:5
```

Just like CrustDB or most other local data storage, this does not use
authentication. Adjust according to your threat model. Being able to
mount different volumes to `/data` means you can easily switch between
separate projects.

Similarly, for FalkorDB:

``` bash
docker run --rm -it --init -p 6379:6379 \
    -v ./data:/data \
    docker.io/falkordb/falkordb:v4.2.1
```

If you run ADMapper natively, just connect to `localhost` in the
respective tab of the connection dialog.

If you run ADMapper also inside a container, it's best to use
`docker-compose`. Example:

``` yml
services:
  admapper:
    image: admapper
    ports:
      - "9191:9191"
    volumes:
      - ${DATA_DIR}:/data
    command: ["crustdb:///data/admapper.db"]
    depends_on:
      neo4j:
        condition: service_healthy

  neo4j:
    image: docker.io/neo4j:5
    init: true
    environment:
      NEO4J_AUTH: none
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - ${DATA_DIR}/neo4j:/data
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:7474"]
      interval: 5s
      timeout: 5s
      retries: 12
```

Then you can easily get going by running
`DATA_DIR=./data docker compose up`.

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
MATCH (u:User)-[:HasSession]->(c:Computer)
RETURN u.name, c.name
LIMIT 10
```

## Next Steps

- [Importing Data](guide/importing.md) - Detailed import options
- [Graph Navigation](guide/navigation.md) - Explore the graph
  interactively
- [Running Queries](guide/queries.md) - Cypher query examples
- [Security Insights](guide/insights.md) - Automated security analysis
