#!/usr/bin/env bash
#
# Test script for ADMapper
#
# Usage:
#   ./scripts/test.sh [target]
#
# Targets:
#   all (default) - Run all tests (frontend + backend)
#   frontend      - Run frontend tests only (Vitest)
#   backend       - Run backend tests only (Cargo)
#   crustdb       - Run CrustDB tests only (Cargo)
#   coverage      - Run all tests with coverage
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

	# Run the tests inside the container (without IN_CONTAINER to avoid recursion)
	echo -e "${GREEN}[INFO]${NC} Running tests inside container with $RUNTIME..."
	exec $RUNTIME run --rm -it --init \
		-v "$PROJECT_ROOT:/workspace" \
		-w /workspace \
		"$CONTAINER_IMAGE" \
		scripts/test.sh "$@"
fi

log_info() {
	echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
	echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1"
}

# Check if npm dependencies are installed
check_npm() {
	if [ ! -d "node_modules" ]; then
		log_info "Installing npm dependencies..."
		npm ci
	fi
}

test_frontend() {
	log_info "Running frontend tests (Vitest)..."
	check_npm
	npm test
	log_info "Frontend tests passed!"
}

test_frontend_coverage() {
	log_info "Running frontend tests with coverage..."
	check_npm
	npm run test:coverage
	log_info "Frontend tests with coverage complete!"
}

test_crustdb() {
	log_info "Running CrustDB tests (Cargo)..."
	cargo test --manifest-path src/crustdb/Cargo.toml
	log_info "CrustDB tests passed!"
}

test_backend() {
	log_info "Running backend tests (Cargo)..."
	cargo test --manifest-path src/backend/Cargo.toml --no-default-features -F crustdb -F neo4j -F falkordb
	log_info "Backend tests passed!"
}

test_all() {
	log_info "Running all tests..."
	echo ""
	echo "=== Frontend Tests ==="
	test_frontend
	echo ""
	echo "=== Backend Tests ==="
	test_backend
	echo "=== CrustDB Tests ==="
	test_crustdb
	echo ""
	log_info "All tests passed!"
}

test_coverage() {
	log_info "Running all tests with coverage..."
	echo ""
	echo "=== Frontend Tests (with coverage) ==="
	test_frontend_coverage
	echo ""
	echo "=== Backend Tests ==="
	test_backend
	echo "=== CrustDB Tests ==="
	test_crustdb
	echo ""
	log_info "All tests with coverage complete!"
}

# Parse command
TARGET="${1:-all}"

case "$TARGET" in
all)
	test_all
	;;
frontend)
	test_frontend
	;;
backend)
	test_backend
	;;
crustdb)
	test_crustdb
	;;
coverage)
	test_coverage
	;;
*)
	log_error "Unknown target: $TARGET"
	echo ""
	echo "Available targets:"
	echo "  all (default) - Run all tests (frontend + backend)"
	echo "  frontend      - Run frontend tests only (Vitest)"
	echo "  backend       - Run backend tests only (Cargo)"
	echo "  crustdb       - Run CrustDB tests only (Cargo)"
	echo "  coverage      - Run all tests with coverage"
	exit 1
	;;
esac
