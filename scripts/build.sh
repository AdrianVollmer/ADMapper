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
#   tauri         - Build Tauri desktop app
#   tauri-debug   - Build Tauri desktop app (debug)
#   clean         - Remove all build artifacts
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

build_tauri() {
	log_info "Building Tauri desktop app (release)..."
	check_npm
	npm run tauri build
	log_info "Tauri app built to src-backend/target/release/"
}

build_tauri_debug() {
	log_info "Building Tauri desktop app (debug)..."
	check_npm
	npm run tauri build -- --debug
	log_info "Tauri app built to src-backend/target/debug/"
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
	rm -rf src-backend/target
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
	echo "  tauri         - Build Tauri desktop app (release)"
	echo "  tauri-debug   - Build Tauri desktop app (debug)"
	echo "  clean         - Remove all build artifacts"
	exit 1
	;;
esac
