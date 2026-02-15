#!/usr/bin/env bash
# Run all checks (lint, typecheck, format check)

set -e

cd "$(dirname "$0")/.."

echo "=== Frontend Checks ==="

echo "Running ESLint..."
npm run lint

echo "Running TypeScript typecheck..."
npm run typecheck

echo "Checking Prettier formatting..."
npm run format:check

echo ""
echo "=== Backend Checks ==="

echo "Running Cargo clippy..."
cargo clippy --manifest-path src-backend/Cargo.toml --no-default-features -- -D warnings

echo "Checking Rust formatting..."
cargo fmt --manifest-path src-backend/Cargo.toml --check

echo ""
echo "All checks passed!"
