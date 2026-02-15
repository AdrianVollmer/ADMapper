#!/usr/bin/env bash
#
# Check script for ADMapper
#
# Runs all checks: lint, typecheck, format check
#
# Usage:
#   ./scripts/check.sh
#
# Environment variables:
#   IN_CONTAINER  - If set to non-empty value, run inside the dev container
#
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory for consistent paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Container image name
CONTAINER_IMAGE="admapper-dev"

# Detect container runtime (prefer podman, fall back to docker)
if command -v podman >/dev/null 2>&1; then
	RUNTIME="podman"
elif command -v docker >/dev/null 2>&1; then
	RUNTIME="docker"
else
	RUNTIME=""
fi

# If IN_CONTAINER is set, delegate to container
if [ -n "$IN_CONTAINER" ]; then
	if [ -z "$RUNTIME" ]; then
		echo -e "${RED}[ERROR]${NC} Neither podman nor docker found. Cannot run in container."
		exit 1
	fi

	# Ensure the dev container image is built
	if ! $RUNTIME image inspect "$CONTAINER_IMAGE" >/dev/null 2>&1; then
		echo -e "${GREEN}[INFO]${NC} Building dev container image with $RUNTIME..."
		$RUNTIME build -t "$CONTAINER_IMAGE" -f dev/Dockerfile .
	fi

	# Run the checks inside the container (without IN_CONTAINER to avoid recursion)
	echo -e "${GREEN}[INFO]${NC} Running checks inside container with $RUNTIME..."
	exec $RUNTIME run --rm \
		-v "$PROJECT_ROOT:/workspace" \
		-w /workspace \
		"$CONTAINER_IMAGE" \
		scripts/check.sh "$@"
fi

log_info() {
	echo -e "${GREEN}[INFO]${NC} $1"
}

# Check if npm dependencies are installed
check_npm() {
	if [ ! -d "node_modules" ]; then
		log_info "Installing npm dependencies..."
		npm ci
	fi
}

echo "=== Frontend Checks ==="
check_npm

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
log_info "All checks passed!"
