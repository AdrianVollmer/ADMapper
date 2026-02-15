#!/usr/bin/env bash
# Format all code (frontend + backend)

set -e

cd "$(dirname "$0")/.."

echo "Formatting frontend (Prettier)..."
npm run format

echo "Formatting backend (Rust)..."
cargo fmt --manifest-path src-backend/Cargo.toml

echo "Done."
