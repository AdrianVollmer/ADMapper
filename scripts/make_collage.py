#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Generate light/dark collages from screenshots.

For each page+viewport pair, combines the light and dark screenshots
into a single image split diagonally with a glowing divider line.

Requires ImageMagick (`convert`) to be installed.

Usage:
    uv run scripts/make_collage.py
    uv run scripts/make_collage.py --input docs/screenshots --output docs/collages
"""

import argparse
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = WORKSPACE / "docs" / "screenshots"
DEFAULT_OUTPUT = WORKSPACE / "docs" / "collages"

# Matches: {page}-{viewport}-{theme}.png
FILENAME_RE = re.compile(r"^(.+)-(desktop|tablet|mobile)-(light|dark)\.png$")


def find_pairs(input_dir: Path) -> list[tuple[str, Path, Path, int, int]]:
    """Find matching light/dark screenshot pairs.

    Returns (stem, light_path, dark_path, width, height) tuples.
    The stem is "{page}-{viewport}".
    """
    files: dict[str, dict[str, Path]] = {}
    for f in sorted(input_dir.glob("*.png")):
        m = FILENAME_RE.match(f.name)
        if not m:
            continue
        page, viewport, theme = m.group(1), m.group(2), m.group(3)
        stem = f"{page}-{viewport}"
        files.setdefault(stem, {})[theme] = f

    pairs: list[tuple[str, Path, Path, int, int]] = []
    for stem, themes in sorted(files.items()):
        if "light" in themes and "dark" in themes:
            # Get image dimensions from the light image
            result = subprocess.run(
                ["identify", "-format", "%w %h", str(themes["light"])],
                capture_output=True,
                text=True,
                check=True,
            )
            w, h = (int(x) for x in result.stdout.strip().split())
            pairs.append((stem, themes["light"], themes["dark"], w, h))

    return pairs


def make_collage(
    stem: str,
    light: Path,
    dark: Path,
    w: int,
    h: int,
    output_dir: Path,
) -> Path:
    """Create a diagonal light/dark collage for one pair."""
    output = output_dir / f"{stem}-collage.png"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Diagonal mask: light on top-left, dark on bottom-right
        subprocess.run(
            [
                "convert",
                "-size",
                f"{w}x{h}",
                "xc:black",
                "-fill",
                "white",
                "-draw",
                f"polygon 0,0 {w},0 {w},{h}",
                str(tmp / "mask.png"),
            ],
            check=True,
        )

        # Apply mask to light image
        subprocess.run(
            [
                "convert",
                str(light),
                str(tmp / "mask.png"),
                "-alpha",
                "off",
                "-compose",
                "CopyOpacity",
                "-composite",
                str(tmp / "light_masked.png"),
            ],
            check=True,
        )

        # Composite: dark base + masked light overlay
        subprocess.run(
            [
                "convert",
                str(dark),
                str(tmp / "light_masked.png"),
                "-compose",
                "Over",
                "-composite",
                str(tmp / "merged.png"),
            ],
            check=True,
        )

        # Glowing diagonal line
        subprocess.run(
            [
                "convert",
                "-size",
                f"{w}x{h}",
                "xc:transparent",
                "-stroke",
                "rgba(255,255,255,0.95)",
                "-strokewidth",
                "3",
                "-draw",
                f"line 0,0 {w},{h}",
                str(tmp / "line.png"),
            ],
            check=True,
        )

        subprocess.run(
            [
                "convert",
                str(tmp / "line.png"),
                "-blur",
                "0x8",
                "-level",
                "0%,100%,0.4",
                str(tmp / "glow.png"),
            ],
            check=True,
        )

        subprocess.run(
            [
                "convert",
                str(tmp / "glow.png"),
                str(tmp / "line.png"),
                "-composite",
                str(tmp / "line_final.png"),
            ],
            check=True,
        )

        subprocess.run(
            [
                "convert",
                str(tmp / "merged.png"),
                str(tmp / "line_final.png"),
                "-composite",
                str(output),
            ],
            check=True,
        )

    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate light/dark collages from screenshots."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Screenshots directory (default: {DEFAULT_INPUT.relative_to(WORKSPACE)})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT.relative_to(WORKSPACE)})",
    )
    args = parser.parse_args()

    if not shutil.which("convert"):
        print("Error: ImageMagick 'convert' not found. Install it first.")
        raise SystemExit(1)

    if not shutil.which("identify"):
        print("Error: ImageMagick 'identify' not found. Install it first.")
        raise SystemExit(1)

    if not args.input.is_dir():
        print(f"Error: Input directory not found: {args.input}")
        raise SystemExit(1)

    pairs = find_pairs(args.input)
    if not pairs:
        print(f"No matching light/dark pairs found in {args.input}")
        raise SystemExit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    print(f"Generating {len(pairs)} collages...")
    for stem, light, dark, w, h in pairs:
        output = make_collage(stem, light, dark, w, h, args.output)
        print(f"  {output.name}")

    print(f"\nDone! Collages saved to {args.output}/")


if __name__ == "__main__":
    main()
