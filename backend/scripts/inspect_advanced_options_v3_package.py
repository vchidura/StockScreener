#!/usr/bin/env python3
"""Provider-free PLAN, preflight and status for a future V3 package."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_v3_deployment import (  # noqa: E402
    advanced_options_v3_package_plan,
    advanced_options_v3_package_status,
    provider_free_advanced_options_v3_preflight,
    validate_disabled_advanced_options_v3_package,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    return parser.parse_args(argv)


def _package(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("V3 package must contain a JSON object")
    return value


def main(argv=None) -> int:
    args = parse_args(argv)
    package = validate_disabled_advanced_options_v3_package(
        _package(args.package.resolve()),
        repository_root=REPOSITORY_ROOT,
    )
    if args.preflight:
        result = provider_free_advanced_options_v3_preflight(
            package,
            repository_root=REPOSITORY_ROOT,
        )
    elif args.status:
        result = advanced_options_v3_package_status(package)
    else:
        result = advanced_options_v3_package_plan(package)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
