#!/usr/bin/env bash
#
# Generate Tauri icons from SVG
#
# Requires: rsvg-convert (librsvg2-bin) or ImageMagick (convert)
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ICONS_DIR="$PROJECT_ROOT/src-backend/icons"
SVG_SOURCE="$ICONS_DIR/icon.svg"

cd "$PROJECT_ROOT"

if [ ! -f "$SVG_SOURCE" ]; then
    echo "Error: $SVG_SOURCE not found"
    exit 1
fi

# Prefer rsvg-convert for better SVG rendering, fall back to ImageMagick
if command -v rsvg-convert &> /dev/null; then
    CONVERT_CMD="rsvg"
elif command -v convert &> /dev/null; then
    CONVERT_CMD="imagemagick"
else
    echo "Error: Neither rsvg-convert nor ImageMagick found"
    echo "Install with: apt-get install librsvg2-bin"
    exit 1
fi

generate_png() {
    local size=$1
    local output=$2

    echo "Generating $output (${size}x${size})..."

    if [ "$CONVERT_CMD" = "rsvg" ]; then
        rsvg-convert -w "$size" -h "$size" "$SVG_SOURCE" -o "$output"
    else
        convert -background none -resize "${size}x${size}" "$SVG_SOURCE" "$output"
    fi
}

# Generate PNG icons
generate_png 32 "$ICONS_DIR/32x32.png"
generate_png 128 "$ICONS_DIR/128x128.png"
generate_png 256 "$ICONS_DIR/128x128@2x.png"

# Generate ICO for Windows (multi-size)
if command -v convert &> /dev/null; then
    echo "Generating icon.ico..."
    convert "$ICONS_DIR/32x32.png" "$ICONS_DIR/128x128.png" "$ICONS_DIR/icon.ico"
else
    echo "Skipping ICO (requires ImageMagick)"
    cp "$ICONS_DIR/128x128.png" "$ICONS_DIR/icon.ico" 2>/dev/null || true
fi

# Generate ICNS for macOS
if command -v png2icns &> /dev/null; then
    echo "Generating icon.icns..."
    png2icns "$ICONS_DIR/icon.icns" "$ICONS_DIR/128x128.png" "$ICONS_DIR/128x128@2x.png"
elif command -v iconutil &> /dev/null; then
    # macOS native tool
    mkdir -p "$ICONS_DIR/icon.iconset"
    cp "$ICONS_DIR/128x128.png" "$ICONS_DIR/icon.iconset/icon_128x128.png"
    cp "$ICONS_DIR/128x128@2x.png" "$ICONS_DIR/icon.iconset/icon_128x128@2x.png"
    iconutil -c icns "$ICONS_DIR/icon.iconset" -o "$ICONS_DIR/icon.icns"
    rm -rf "$ICONS_DIR/icon.iconset"
else
    echo "Skipping ICNS (requires png2icns or iconutil)"
    # Create a placeholder
    cp "$ICONS_DIR/128x128.png" "$ICONS_DIR/icon.icns" 2>/dev/null || true
fi

echo "Icons generated successfully!"
ls -la "$ICONS_DIR"
