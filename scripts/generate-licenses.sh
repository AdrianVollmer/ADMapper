#!/usr/bin/env bash
# Generate public/licenses.json from upstream license files.
# Run this whenever dependencies change.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT="$ROOT_DIR/public/licenses.json"
TMP="$(mktemp -d)"

trap 'rm -rf "$TMP"' EXIT

echo "Downloading license texts..."

declare -A URLS=(
  [BloodHound]="https://raw.githubusercontent.com/SpecterOps/BloodHound/main/LICENSE"
  [GrandCypher]="https://raw.githubusercontent.com/aplbrain/grandcypher/master/LICENSE"
  [open-cypher-mit]="https://raw.githubusercontent.com/a-poor/open-cypher/main/LICENSE-MIT"
  [open-cypher-apache]="https://raw.githubusercontent.com/a-poor/open-cypher/main/LICENSE-APACHE"
  [Lucide]="https://raw.githubusercontent.com/lucide-icons/lucide/main/LICENSE"
  [Sigma.js]="https://raw.githubusercontent.com/jacomyal/sigma.js/main/LICENSE.txt"
  [Graphology]="https://raw.githubusercontent.com/graphology/graphology/master/LICENSE.txt"
  [Dagre]="https://raw.githubusercontent.com/dagrejs/dagre/master/LICENSE"
)

for name in "${!URLS[@]}"; do
  curl -sfL "${URLS[$name]}" -o "$TMP/$name.txt" || {
    echo "ERROR: Failed to download license for $name" >&2
    exit 1
  }
done

echo "Building $OUT..."

python3 - "$TMP" "$OUT" << 'PYEOF'
import json, sys, os

tmp_dir, out_path = sys.argv[1], sys.argv[2]


def read(name):
    with open(os.path.join(tmp_dir, name + ".txt")) as f:
        return f.read().strip()


entries = [
    {
        "name": "BloodHound",
        "url": "https://github.com/SpecterOps/BloodHound",
        "licenseType": "Apache-2.0",
        "description": "Active Directory reconnaissance tool. ADMapper is a frontend for BloodHound data.",
        "licenseText": read("BloodHound"),
    },
    {
        "name": "GrandCypher",
        "url": "https://github.com/aplbrain/grandcypher",
        "licenseType": "Apache-2.0",
        "description": "Python openCypher implementation. CrustDB test fixtures are derived from GrandCypher.",
        "licenseText": read("GrandCypher"),
    },
    {
        "name": "open-cypher",
        "url": "https://github.com/a-poor/open-cypher",
        "licenseType": "Apache-2.0 / MIT",
        "description": "Pest grammar for openCypher. CrustDB's Cypher parser is derived from this grammar.",
        "licenseText": "=== MIT License ===\n\n"
        + read("open-cypher-mit")
        + "\n\n=== Apache License ===\n\n"
        + read("open-cypher-apache"),
    },
    {
        "name": "Sigma.js",
        "url": "https://github.com/jacomyal/sigma.js",
        "licenseType": "MIT",
        "description": "Graph rendering library for the web.",
        "licenseText": read("Sigma.js"),
    },
    {
        "name": "Graphology",
        "url": "https://github.com/graphology/graphology",
        "licenseType": "MIT",
        "description": "Graph data structure library for JavaScript.",
        "licenseText": read("Graphology"),
    },
    {
        "name": "Dagre",
        "url": "https://github.com/dagrejs/dagre",
        "licenseType": "MIT",
        "description": "Directed graph layout for JavaScript.",
        "licenseText": read("Dagre"),
    },
    {
        "name": "Lucide",
        "url": "https://github.com/lucide-icons/lucide",
        "licenseType": "ISC",
        "description": "Icon library, derived from Feather Icons.",
        "licenseText": read("Lucide"),
    },
]

with open(out_path, "w") as f:
    json.dump(entries, f, indent=2)

print(f"Wrote {len(entries)} license entries to {out_path}")
PYEOF
