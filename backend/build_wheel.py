"""
Build script for stock-screener-api wheel package.

Copies root-level source files into src/stock_screener/, converts bare imports
to relative package imports, then runs `python -m build` to produce the .whl.

Usage:
    python build_wheel.py            # build wheel + sdist
    python build_wheel.py --check    # dry-run: show what would be copied
"""

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
SRC_PKG_DIR = BACKEND_DIR / "src" / "stock_screener"

# Root files → package destination (root name → package name)
FILE_MAP = {
    "database.py":  "database.py",
    "screeners.py": "screeners.py",
    "models.py":    "models.py",
    "main.py":      "app.py",        # main.py becomes app.py in the package
}

# Bare imports that must become relative inside the package
# Pattern: "from <module> import ..." → "from .<module> import ..."
IMPORT_REWRITES = [
    (re.compile(r"^from database import", re.MULTILINE),  "from .database import"),
    (re.compile(r"^from models import", re.MULTILINE),    "from .models import"),
    (re.compile(r"^from screeners import", re.MULTILINE), "from .screeners import"),
]


def sync_file(src_name: str, dst_name: str, dry_run: bool = False) -> list[str]:
    """Copy a root file to the package dir, rewriting imports."""
    src_path = BACKEND_DIR / src_name
    dst_path = SRC_PKG_DIR / dst_name

    if not src_path.exists():
        return [f"  SKIP {src_name} (not found)"]

    content = src_path.read_text(encoding="utf-8")
    changes = []

    for pattern, replacement in IMPORT_REWRITES:
        new_content, count = pattern.subn(replacement, content)
        if count:
            changes.append(f"    {pattern.pattern!r} → {replacement!r} ({count}x)")
            content = new_content

    if dry_run:
        status = "WOULD COPY" if changes or not dst_path.exists() else "UNCHANGED"
        lines = [f"  {status} {src_name} → src/stock_screener/{dst_name}"]
        lines.extend(changes)
        return lines

    dst_path.write_text(content, encoding="utf-8")
    lines = [f"  COPIED {src_name} → src/stock_screener/{dst_name}"]
    lines.extend(changes)
    return lines


def main():
    parser = argparse.ArgumentParser(description="Build stock-screener-api wheel")
    parser.add_argument(
        "--check", action="store_true",
        help="Dry-run: show what would be synced without building",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Stock Screener API — Wheel Build")
    print("=" * 60)

    # Ensure package dir exists
    SRC_PKG_DIR.mkdir(parents=True, exist_ok=True)

    # Sync files
    print("\n1. Syncing root files → src/stock_screener/")
    for src_name, dst_name in FILE_MAP.items():
        for line in sync_file(src_name, dst_name, dry_run=args.check):
            print(line)

    if args.check:
        print("\n[dry-run] No files were modified. Remove --check to build.")
        return

    # Clean old build artifacts
    print("\n2. Cleaning old build artifacts...")
    for d in ["dist", "build"]:
        p = BACKEND_DIR / d
        if p.exists():
            shutil.rmtree(p)
            print(f"  Removed {d}/")

    # Build
    print("\n3. Building wheel...")
    result = subprocess.run(
        [sys.executable, "-m", "build"],
        cwd=str(BACKEND_DIR),
    )

    if result.returncode != 0:
        print("\nBuild FAILED.")
        sys.exit(1)

    # Show output
    dist_dir = BACKEND_DIR / "dist"
    print("\n4. Build artifacts:")
    for f in sorted(dist_dir.iterdir()):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}  ({size_kb:.0f} KB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
