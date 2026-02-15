#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Generate a hero image from desktop, tablet, and mobile collages.

Layers the three viewport collages with slight overlap, offset,
rounded corners, and drop shadows on a transparent background.

Requires ImageMagick (`convert`) to be installed.

Usage:
    uv run scripts/make_hero.py
    uv run scripts/make_hero.py --page spending-category
    uv run scripts/make_hero.py --output docs/hero.png
"""

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
DEFAULT_COLLAGES = WORKSPACE / "docs" / "collages"
DEFAULT_OUTPUT = WORKSPACE / "docs" / "hero.png"


def get_image_size(path: Path) -> tuple[int, int]:
    """Get width and height of an image."""
    result = subprocess.run(
        ["identify", "-format", "%w %h", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    w, h = (int(x) for x in result.stdout.strip().split())
    return w, h


def add_rounded_corners(src: Path, dst: Path, radius: int, w: int, h: int) -> None:
    """Apply rounded corners to an image via an alpha mask."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mask = Path(tmpdir) / "mask.png"
        # White rounded rectangle on black background
        subprocess.run(
            [
                "convert",
                "-size",
                f"{w}x{h}",
                "xc:black",
                "-fill",
                "white",
                "-draw",
                f"roundrectangle 0,0,{w - 1},{h - 1},{radius},{radius}",
                str(mask),
            ],
            check=True,
        )
        subprocess.run(
            [
                "convert",
                str(src),
                str(mask),
                "-alpha",
                "off",
                "-compose",
                "CopyOpacity",
                "-composite",
                str(dst),
            ],
            check=True,
        )


def add_shadow(src: Path, dst: Path) -> None:
    """Add a drop shadow behind an image, preserving transparency."""
    subprocess.run(
        [
            "convert",
            str(src),
            "(",
            "+clone",
            "-background",
            "black",
            "-shadow",
            "50x20+0+12",
            ")",
            "+swap",
            "-background",
            "transparent",
            "-layers",
            "merge",
            "+repage",
            str(dst),
        ],
        check=True,
    )


def make_hero(
    desktop: Path,
    tablet: Path,
    mobile: Path,
    output: Path,
) -> None:
    """Compose the three collages into a hero image."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Scale factors — shrink so they fit together nicely
        desktop_scale = 50
        tablet_scale = 50
        mobile_scale = 50

        # Scale images
        layers: list[tuple[str, Path, int, int]] = []
        for name, src, scale in [
            ("desktop", desktop, desktop_scale),
            ("tablet", tablet, tablet_scale),
            ("mobile", mobile, mobile_scale),
        ]:
            orig_w, orig_h = get_image_size(src)
            new_w = orig_w * scale // 100
            new_h = orig_h * scale // 100

            scaled = tmp / f"{name}_scaled.png"
            subprocess.run(
                [
                    "convert",
                    str(src),
                    "-resize",
                    f"{new_w}x{new_h}!",
                    str(scaled),
                ],
                check=True,
            )

            rounded = tmp / f"{name}_rounded.png"
            corner_radius = 12 if name != "mobile" else 16
            add_rounded_corners(scaled, rounded, corner_radius, new_w, new_h)

            shadowed = tmp / f"{name}_shadow.png"
            add_shadow(rounded, shadowed)

            sw, sh = get_image_size(shadowed)
            layers.append((name, shadowed, sw, sh))

        # Position layers: desktop back-left, tablet middle, mobile front-right
        # Each offset slightly right and down from the previous
        desktop_layer = layers[0]
        tablet_layer = layers[1]
        mobile_layer = layers[2]

        # Offsets (x, y) for each layer
        pad = 40  # extra padding around the whole image
        desktop_x, desktop_y = pad, pad + 40
        tablet_x = desktop_x + desktop_layer[2] - tablet_layer[2] - 60
        tablet_y = desktop_y + 20
        mobile_x = tablet_x + tablet_layer[2] - mobile_layer[2] + 20
        mobile_y = tablet_y + 20

        # Canvas size
        canvas_w = (
            max(
                desktop_x + desktop_layer[2],
                tablet_x + tablet_layer[2],
                mobile_x + mobile_layer[2],
            )
            + pad
        )
        canvas_h = (
            max(
                desktop_y + desktop_layer[3],
                tablet_y + tablet_layer[3],
                mobile_y + mobile_layer[3],
            )
            + pad
        )

        # Composite all layers onto transparent canvas
        # Desktop (back) → tablet (middle) → mobile (front)
        subprocess.run(
            [
                "convert",
                "-size",
                f"{canvas_w}x{canvas_h}",
                "xc:transparent",
                str(desktop_layer[1]),
                "-geometry",
                f"+{desktop_x}+{desktop_y}",
                "-composite",
                str(tablet_layer[1]),
                "-geometry",
                f"+{tablet_x}+{tablet_y}",
                "-composite",
                str(mobile_layer[1]),
                "-geometry",
                f"+{mobile_x}+{mobile_y}",
                "-composite",
                str(output),
            ],
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a hero image from viewport collages."
    )
    parser.add_argument(
        "--page",
        default="dashboard",
        help="Page name to use for collages (default: dashboard)",
    )
    parser.add_argument(
        "--collages",
        type=Path,
        default=DEFAULT_COLLAGES,
        help="Collages directory",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output file path (default: docs/hero.png)",
    )
    args = parser.parse_args()

    for cmd in ("convert", "identify"):
        if not shutil.which(cmd):
            print(f"Error: ImageMagick '{cmd}' not found. Install it first.")
            raise SystemExit(1)

    desktop = args.collages / f"{args.page}-desktop-collage.png"
    tablet = args.collages / f"{args.page}-tablet-collage.png"
    mobile = args.collages / f"{args.page}-mobile-collage.png"

    for f in (desktop, tablet, mobile):
        if not f.exists():
            print(f"Error: {f} not found")
            raise SystemExit(1)

    print(f"Creating hero image for '{args.page}'...")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    make_hero(desktop, tablet, mobile, args.output)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
