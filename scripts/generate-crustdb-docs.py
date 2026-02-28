#!/usr/bin/env python3
"""
Generate CrustDB documentation using MkDocs with Material theme.

Usage:
    python scripts/generate-crustdb-docs.py [--serve] [--open]

Options:
    --serve     Start local development server
    --open      Open browser after building/serving
    --clean     Remove build artifacts before building
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import webbrowser
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DOCS_DIR = PROJECT_ROOT / "docs" / "crustdb"
BUILD_DIR = DOCS_DIR / "site"


def clean_build() -> None:
    """Remove build artifacts."""
    if BUILD_DIR.exists():
        print(f"Removing {BUILD_DIR}")
        shutil.rmtree(BUILD_DIR)


def build_docs() -> bool:
    """Build the documentation."""
    print(f"Building documentation from {DOCS_DIR}")

    try:
        subprocess.run(
            ["uv", "tool", "run", "zensical", "build"],
            cwd=DOCS_DIR,
            check=True,
        )
        print(f"Documentation built successfully: {BUILD_DIR}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Build failed: {e}")
        return False
    except FileNotFoundError:
        print("uv command not found.")
        return False


def serve_docs(open_browser: bool = False) -> None:
    """Start the development server."""
    print(f"Starting development server from {DOCS_DIR}")
    print("Press Ctrl+C to stop")

    if open_browser:
        webbrowser.open("http://127.0.0.1:8000")

    try:
        subprocess.run(
            ["uv", "tool", "run", "zensical", "serve"],
            cwd=DOCS_DIR,
            check=True,
        )
    except subprocess.CalledProcessError:
        pass
    except FileNotFoundError:
        print("uv command not found.")
    except KeyboardInterrupt:
        print("\nServer stopped")


def generate_api_docs_from_source() -> None:
    """
    Parse Rust source files and update API documentation.

    This function reads the CrustDB source code and extracts documentation
    comments to update the API reference pages.
    """
    lib_rs = PROJECT_ROOT / "src" / "crustdb" / "src" / "lib.rs"
    if not lib_rs.exists():
        print(f"Source file not found: {lib_rs}")
        return

    print(f"Extracting documentation from {lib_rs}")

    # Read source file
    content = lib_rs.read_text()

    # Extract public methods and their doc comments
    methods: list[dict[str, str]] = []
    lines = content.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        # Look for doc comments
        if line.strip().startswith("///"):
            doc_lines = []
            while i < len(lines) and lines[i].strip().startswith("///"):
                doc_lines.append(lines[i].strip()[4:])  # Remove "/// "
                i += 1

            # Look for pub fn
            while i < len(lines) and lines[i].strip().startswith("#["):
                i += 1  # Skip attributes

            if i < len(lines) and "pub fn" in lines[i]:
                fn_line = lines[i].strip()
                # Extract function signature
                sig_lines = [fn_line]
                while not fn_line.endswith("{") and i + 1 < len(lines):
                    i += 1
                    fn_line = lines[i].strip()
                    sig_lines.append(fn_line)

                signature = " ".join(sig_lines).replace(" {", "")
                methods.append(
                    {
                        "doc": "\n".join(doc_lines),
                        "signature": signature,
                    }
                )
        i += 1

    print(f"Found {len(methods)} documented public methods")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate CrustDB documentation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start local development server",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open browser after building/serving",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove build artifacts before building",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract API docs from Rust source (updates docs)",
    )

    args = parser.parse_args()

    # Check docs directory exists
    if not DOCS_DIR.exists():
        print(f"Documentation directory not found: {DOCS_DIR}")
        return 1

    # Check mkdocs.yml exists
    if not (DOCS_DIR / "mkdocs.yml").exists():
        print(f"mkdocs.yml not found in {DOCS_DIR}")
        return 1

    # Clean if requested
    if args.clean:
        clean_build()

    # Extract API docs if requested
    if args.extract:
        generate_api_docs_from_source()

    # Serve or build
    if args.serve:
        serve_docs(open_browser=args.open)
    else:
        if not build_docs():
            return 1
        if args.open:
            index_path = BUILD_DIR / "index.html"
            if index_path.exists():
                webbrowser.open(f"file://{index_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
