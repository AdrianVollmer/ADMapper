#!/usr/bin/env bash
set -euo pipefail

# Version Bump Script
#
# Usage: ./scripts/bump-version.sh <new-version>
# Example: ./scripts/bump-version.sh 0.2.0
#
# Set IN_CONTAINER=1 to use cargo check for Cargo.lock update (requires build env).
# Without it, Cargo.lock is updated via sed (no build env required).
#
# This script:
# 1. Updates version in src/backend/Cargo.toml
# 2. Updates version in package.json
# 3. Updates version in package-lock.json
# 4. Updates version in src/backend/tauri.conf.json
# 5. Updates src/backend/Cargo.lock
# 6. Creates a git commit
# 7. Creates a git tag (v<version>)

if [ $# -ne 1 ]; then
	echo "Usage: $0 <new-version>"
	echo "Example: $0 0.2.0"
	exit 1
fi

NEW_VERSION="$1"

# Validate version format (semantic versioning: X.Y.Z)
if ! echo "$NEW_VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
	echo "Error: Version must be in format X.Y.Z (e.g., 0.2.0)"
	exit 1
fi

# Get script directory for consistent paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Run lint and format checks before making any changes
echo "Running lint and format checks..."
if ! "$SCRIPT_DIR/check.sh"; then
	echo "Error: Lint/format checks failed. Fix issues before bumping version."
	exit 1
fi
echo ""

# Get current version from src/backend/Cargo.toml
CURRENT_VERSION=$(grep '^version = ' src/backend/Cargo.toml | head -n1 | sed 's/version = "\(.*\)"/\1/')

echo "Bumping version from $CURRENT_VERSION to $NEW_VERSION"

# Update src/backend/Cargo.toml
echo "Updating src/backend/Cargo.toml..."
sed -i.bak "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" src/backend/Cargo.toml
rm src/backend/Cargo.toml.bak

# Update package.json
echo "Updating package.json..."
sed -i.bak "s/\"version\": \"$CURRENT_VERSION\"/\"version\": \"$NEW_VERSION\"/" package.json
rm package.json.bak

# Update package-lock.json (appears in multiple places)
echo "Updating package-lock.json..."
sed -i.bak "s/\"version\": \"$CURRENT_VERSION\"/\"version\": \"$NEW_VERSION\"/g" package-lock.json
rm package-lock.json.bak

# Update src/backend/tauri.conf.json
echo "Updating src/backend/tauri.conf.json..."
sed -i.bak "s/\"version\": \"$CURRENT_VERSION\"/\"version\": \"$NEW_VERSION\"/" src/backend/tauri.conf.json
rm src/backend/tauri.conf.json.bak

# Update Cargo.lock
if [ -n "${IN_CONTAINER:-}" ]; then
	echo "Updating src/backend/Cargo.lock (via cargo check in container)..."
	# Detect container runtime (prefer podman, fall back to docker)
	if command -v podman >/dev/null 2>&1; then
		RUNTIME="podman"
	elif command -v docker >/dev/null 2>&1; then
		RUNTIME="docker"
	else
		echo "Error: Neither podman nor docker found. Cannot run cargo check in container."
		exit 1
	fi
	$RUNTIME run --rm \
		-v "$PROJECT_ROOT:/workspace" \
		-w /workspace \
		admapper-dev \
		cargo check --manifest-path src/backend/Cargo.toml --quiet
else
	echo "Updating src/backend/Cargo.lock (via sed, no build env)..."
	sed -i.bak "/^name = \"admapper\"/{n;s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/;}" src/backend/Cargo.lock
	rm src/backend/Cargo.lock.bak
fi

# Check if there are changes
if ! git diff --quiet src/backend/Cargo.toml package.json package-lock.json src/backend/Cargo.lock src/backend/tauri.conf.json; then
	echo "Creating git commit and tag..."
	git add src/backend/Cargo.toml package.json package-lock.json src/backend/Cargo.lock src/backend/tauri.conf.json
	git commit -m "$(
		cat <<EOF
Bump version to $NEW_VERSION

Updated version number across:
- src/backend/Cargo.toml
- package.json
- package-lock.json
- src/backend/tauri.conf.json
- src/backend/Cargo.lock
EOF
	)"

	# Create git tag
	git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

	echo ""
	echo "✓ Version bumped to $NEW_VERSION"
	echo "✓ Commit created"
	echo "✓ Tag v$NEW_VERSION created"
	echo ""
	echo "To push the changes and trigger Docker build:"
	echo "  git push origin main"
	echo "  git push origin v$NEW_VERSION"
else
	echo "No changes detected. Version might already be $NEW_VERSION"
	exit 1
fi
