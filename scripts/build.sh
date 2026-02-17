#!/usr/bin/env bash
#
# Build script for ADMapper
#
# Usage:
#   ./scripts/build.sh [target]
#
# Targets:
#   all (default) - Build everything (frontend + Tauri)
#   frontend      - Build frontend only (Vite)
#   backend       - Build backend only (no Tauri, --no-default-features)
#   backend-debug - Build backend only (no Tauri, --no-default-features, debug)
#   tauri         - Build Tauri desktop app
#   tauri-debug   - Build Tauri desktop app (debug)
#   clean         - Remove all build artifacts
#
# Environment variables:
#   IN_CONTAINER  - If set to non-empty value, build inside the dev container
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
		echo -e "${RED}[ERROR]${NC} Neither podman nor docker found. Cannot build in container."
		exit 1
	fi

	# Ensure the dev container image is built
	if ! $RUNTIME image inspect "$CONTAINER_IMAGE" >/dev/null 2>&1; then
		echo -e "${GREEN}[INFO]${NC} Building dev container image with $RUNTIME..."
		$RUNTIME build -t "$CONTAINER_IMAGE" -f dev/Dockerfile .
	fi

	# Run the build inside the container (without IN_CONTAINER to avoid recursion)
	echo -e "${GREEN}[INFO]${NC} Running build inside container with $RUNTIME..."
	exec $RUNTIME run --rm -it --init \
		-v "$PROJECT_ROOT:/workspace" \
		-w /workspace \
		"$CONTAINER_IMAGE" \
		scripts/build.sh "$@"
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

build_frontend() {
	log_info "Building frontend..."
	check_npm
	npm run build
	log_info "Frontend built to build/"
}

build_backend() {
	log_info "Building backend (no Tauri)..."
	cargo build --manifest-path src/backend/Cargo.toml --no-default-features --release --features kuzu,crustdb
	log_info "Backend built to src/backend/target/release/"
}

build_backend_debug() {
	log_info "Building backend (no Tauri, debug)..."
	cargo build --manifest-path src/backend/Cargo.toml --no-default-features --features kuzu,crustdb
	log_info "Backend built to src/backend/target/debug/"
}

generate_icons() {
	if [ ! -f "src/backend/icons/32x32.png" ]; then
		log_info "Generating icons..."
		bash scripts/generate-icons.sh
	fi
}

build_tauri() {
	log_info "Building Tauri desktop app (release)..."
	check_npm
	generate_icons
	npm run tauri build
	log_info "Tauri app built to src/backend/target/release/"
}

build_tauri_debug() {
	log_info "Building Tauri desktop app (debug)..."
	check_npm
	generate_icons
	npm run tauri build -- --debug
	log_info "Tauri app built to src/backend/target/debug/"
}

build_all() {
	log_info "Building everything..."
	build_frontend
	build_tauri
	log_info "Build complete!"
}

clean() {
	log_info "Cleaning build artifacts..."
	rm -rf build
	rm -rf src/backend/target
	rm -rf src/backend/icons
	log_info "Clean complete!"
}

# Parse command
TARGET="${1:-all}"

case "$TARGET" in
all)
	build_all
	;;
frontend)
	build_frontend
	;;
backend)
	build_backend
	;;
backend-debug)
	build_backend_debug
	;;
tauri)
	build_tauri
	;;
tauri-debug)
	build_tauri_debug
	;;
clean)
	clean
	;;
*)
	log_error "Unknown target: $TARGET"
	echo ""
	echo "Available targets:"
	echo "  all (default) - Build everything (frontend + Tauri)"
	echo "  frontend      - Build frontend only"
	echo "  backend       - Build backend only (no Tauri)"
	echo "  backend-debug - Build backend only (no Tauri, debug)"
	echo "  tauri         - Build Tauri desktop app (release)"
	echo "  tauri-debug   - Build Tauri desktop app (debug)"
	echo "  clean         - Remove all build artifacts"
	exit 1
	;;
esac
