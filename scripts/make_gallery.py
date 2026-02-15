#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Generate a gallery image from multiple tablet collages.

Places 5 collages side by side in a clean row with rounded corners,
drop shadows, and a transparent background.

Requires ImageMagick (`convert`, `identify`) to be installed.

Usage:
    uv run scripts/make_gallery.py
    uv run scripts/make_gallery.py --pages spending-category spending-flow transactions balances positions
    uv run scripts/make_gallery.py --output docs/gallery.png
"""

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
DEFAULT_COLLAGES = WORKSPACE / "docs" / "collages"
DEFAULT_OUTPUT = WORKSPACE / "docs" / "gallery.png"

DEFAULT_PAGES = [
    "spending-category",
    "spending-over-time",
    "spending-monthly",
    "spending-flow",
    "transactions",
]


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


def prepare_card(src: Path, scale: int, radius: int, tmp: Path, name: str) -> Path:
    """Scale, round corners, and add drop shadow to a single image."""
    orig_w, orig_h = get_image_size(src)
    new_w = orig_w * scale // 100
    new_h = orig_h * scale // 100

    scaled = tmp / f"{name}_scaled.png"
    subprocess.run(
        ["convert", str(src), "-resize", f"{new_w}x{new_h}!", str(scaled)],
        check=True,
    )

    # Round corners via alpha mask
    mask = tmp / f"{name}_mask.png"
    subprocess.run(
        [
            "convert",
            "-size",
            f"{new_w}x{new_h}",
            "xc:black",
            "-fill",
            "white",
            "-draw",
            f"roundrectangle 0,0,{new_w - 1},{new_h - 1},{radius},{radius}",
            str(mask),
        ],
        check=True,
    )

    rounded = tmp / f"{name}_rounded.png"
    subprocess.run(
        [
            "convert",
            str(scaled),
            str(mask),
            "-alpha",
            "off",
            "-compose",
            "CopyOpacity",
            "-composite",
            str(rounded),
        ],
        check=True,
    )

    # Drop shadow
    shadowed = tmp / f"{name}_shadow.png"
    subprocess.run(
        [
            "convert",
            str(rounded),
            "(",
            "+clone",
            "-background",
            "black",
            "-shadow",
            "40x15+0+8",
            ")",
            "+swap",
            "-background",
            "transparent",
            "-layers",
            "merge",
            "+repage",
            str(shadowed),
        ],
        check=True,
    )

    return shadowed


def make_gallery(collages: list[Path], output: Path) -> None:
    """Compose collages into a wide gallery image."""
    n = len(collages)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        scale = 40
        radius = 10
        gap = 30

        # Prepare all cards
        cards: list[tuple[Path, int, int]] = []
        for i, src in enumerate(collages):
            card = prepare_card(src, scale, radius, tmp, f"card{i}")
            w, h = get_image_size(card)
            cards.append((card, w, h))

        card_w = cards[0][1]
        card_h = cards[0][2]

        pad = 40
        canvas_w = pad + n * card_w + (n - 1) * gap + pad
        canvas_h = pad + card_h + pad

        # Build composite command: place cards side by side
        cmd: list[str] = [
            "convert",
            "-size",
            f"{canvas_w}x{canvas_h}",
            "xc:transparent",
        ]

        for i, (card_path, cw, ch) in enumerate(cards):
            x = pad + i * (card_w + gap)
            y = pad
            cmd += [str(card_path), "-geometry", f"+{x}+{y}", "-composite"]

        cmd.append(str(output))
        subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a gallery image from tablet collages."
    )
    parser.add_argument(
        "--pages",
        nargs="+",
        default=DEFAULT_PAGES,
        help="Page names to include (default: 4 chart views + transactions)",
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
        help="Output file path (default: docs/gallery.png)",
    )
    args = parser.parse_args()

    for cmd in ("convert", "identify"):
        if not shutil.which(cmd):
            print(f"Error: ImageMagick '{cmd}' not found. Install it first.")
            raise SystemExit(1)

    collage_paths: list[Path] = []
    for page in args.pages:
        path = args.collages / f"{page}-tablet-collage.png"
        if not path.exists():
            print(f"Error: {path} not found")
            raise SystemExit(1)
        collage_paths.append(path)

    print(f"Creating gallery from {len(collage_paths)} collages...")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    make_gallery(collage_paths, args.output)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
