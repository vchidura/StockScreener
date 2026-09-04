"""Build the canonical stock-screener-api wheel package.

Usage:
    python build_wheel.py            # build wheel + sdist
    python build_wheel.py --check    # dry-run: show what would be copied
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
def main():
    parser = argparse.ArgumentParser(description="Build stock-screener-api wheel")
    parser.add_argument(
        "--check", action="store_true",
        help="Validate canonical package inputs without building",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Stock Screener API — Wheel Build")
    print("=" * 60)

    if args.check:
        required = [
            BACKEND_DIR / "main.py",
            BACKEND_DIR / "equity",
            BACKEND_DIR / "options",
            BACKEND_DIR / "research",
            BACKEND_DIR / "migrations" / "000_canonical_schema.sql",
            BACKEND_DIR / "scripts" / "bootstrap_fresh_data.py",
            BACKEND_DIR / "scripts" / "initialize_database.py",
            BACKEND_DIR / "stock_screener" / "schema.py",
            BACKEND_DIR / "stock_screener" / "cli.py",
        ]
        missing = [str(path.relative_to(BACKEND_DIR)) for path in required if not path.exists()]
        if missing:
            raise SystemExit(f"Missing canonical package inputs: {', '.join(missing)}")
        print("Canonical package inputs are present; no generated source copies are used.")
        return

    # Clean old build artifacts
    print("\n1. Cleaning old build artifacts...")
    for d in ["dist", "build"]:
        p = BACKEND_DIR / d
        if p.exists():
            shutil.rmtree(p)
            print(f"  Removed {d}/")

    # Build
    print("\n2. Building wheel...")
    result = subprocess.run(
        [sys.executable, "-m", "build"],
        cwd=str(BACKEND_DIR),
    )

    if result.returncode != 0:
        print("\nBuild FAILED.")
        sys.exit(1)

    # Show output
    dist_dir = BACKEND_DIR / "dist"
    print("\n3. Build artifacts:")
    for f in sorted(dist_dir.iterdir()):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}  ({size_kb:.0f} KB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
