#!/usr/bin/env python3
"""
Generate expected statistics from BloodHound test data.

This script is called automatically by run-tests.sh before each test run.
It parses the BloodHound JSON files and counts unique objects by type,
deduplicating by ObjectIdentifier to get accurate expected counts.

Output: e2e/golden/expected_stats.json (generated, not committed)
"""

import json
import os
import sys
import zipfile
from collections import defaultdict
from pathlib import Path


def parse_bloodhound_json(data: dict) -> tuple[str, list[dict]]:
    """Parse a BloodHound JSON file and return (type, items)."""
    meta = data.get("meta", {})
    items = data.get("data", [])
    node_type = meta.get("type", "unknown")
    return node_type, items


def get_objectidentifier(item: dict) -> str | None:
    """Extract the unique identifier from a BloodHound item."""
    # Check common identifier fields
    if "ObjectIdentifier" in item:
        return item["ObjectIdentifier"]
    if "Properties" in item:
        props = item["Properties"]
        if "objectid" in props:
            return props["objectid"]
        if "objectidentifier" in props:
            return props["objectidentifier"]
    return None


def count_unique_objects(data_dir: Path) -> dict:
    """Count unique objects by type across all JSON files."""
    # Use sets to track unique ObjectIdentifiers per type
    unique_objects: dict[str, set[str]] = defaultdict(set)
    edge_count = 0

    json_files = sorted(data_dir.glob("*.json"))

    for json_file in json_files:
        with open(json_file) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse {json_file}: {e}", file=sys.stderr)
                continue

        node_type, items = parse_bloodhound_json(data)

        # Normalize type names to match the backend's labeling
        type_map = {
            "users": "User",
            "computers": "Computer",
            "groups": "Group",
            "domains": "Domain",
            "gpos": "GPO",
            "ous": "OU",
            "containers": "Container",
            "certtemplates": "CertTemplate",
            "enterprisecas": "EnterpriseCA",
            "aiacas": "AIACA",
            "rootcas": "RootCA",
            "ntauthstores": "NTAuthStore",
        }
        normalized_type = type_map.get(node_type, node_type.title())

        for item in items:
            obj_id = get_objectidentifier(item)
            if obj_id:
                unique_objects[normalized_type].add(obj_id)

            # Count edges (ACEs, group memberships, etc.)
            edge_count += len(item.get("Aces", []))
            edge_count += len(item.get("Members", []))
            edge_count += len(item.get("AllowedToDelegate", []))
            edge_count += len(item.get("SPNTargets", []))
            edge_count += len(item.get("HasSIDHistory", []))
            if item.get("PrimaryGroupSID"):
                edge_count += 1

    # Convert sets to counts
    node_counts = {t: len(ids) for t, ids in unique_objects.items()}
    total_nodes = sum(node_counts.values())

    # Output in format expected by test runner (lowercase keys)
    return {
        "total_nodes": total_nodes,
        # Note: edge count from source files won't match actual imports
        # because some edges reference non-existent nodes
        "total_edges": edge_count,
        # Individual type counts (lowercase to match test expectations)
        "users": node_counts.get("User", 0),
        "computers": node_counts.get("Computer", 0),
        "groups": node_counts.get("Group", 0),
        "domains": node_counts.get("Domain", 0),
        "ous": node_counts.get("OU", 0),
        "gpos": node_counts.get("GPO", 0),
    }


def main():
    import argparse
    import tempfile

    parser = argparse.ArgumentParser(
        description="Generate expected statistics from BloodHound test data"
    )
    parser.add_argument(
        "zip_file",
        type=Path,
        help="Path to BloodHound data zip file",
    )
    args = parser.parse_args()
    zip_file = args.zip_file

    if not zip_file.exists():
        print(f"Error: Zip file not found: {zip_file}", file=sys.stderr)
        sys.exit(1)

    # Extract to temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        print(f"Extracting {zip_file}...", file=sys.stderr)
        with zipfile.ZipFile(zip_file) as zf:
            zf.extractall(tmp_path)

        # Find the extracted data directory (may be nested)
        json_files = list(tmp_path.rglob("*.json"))
        if not json_files:
            print(f"Error: No JSON files found in {zip_file}", file=sys.stderr)
            sys.exit(1)

        # Use the parent directory of the first JSON file
        data_dir = json_files[0].parent

        print(f"Counting objects in {data_dir}...", file=sys.stderr)
        stats = count_unique_objects(data_dir)

        # Write golden file
        golden_file = Path(os.environ["GOLDEN_FILE"])
        golden_dir = golden_file.parent
        golden_dir.mkdir(exist_ok=True)

        with open(golden_file, "w") as f:
            json.dump(stats, f, indent=2, sort_keys=True)

        print(f"Generated: {golden_file}", file=sys.stderr)
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
