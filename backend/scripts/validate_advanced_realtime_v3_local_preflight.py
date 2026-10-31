#!/usr/bin/env python3
"""Create or validate the provider-free local-first V3 clock preflight."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_v3_local_preflight import (  # noqa: E402
    build_authorization_clock_report,
    validate_manifest_authorization_clocks,
)
from realtime.contracts import canonical_sha256  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output = args.output.resolve()
    if args.manifest is not None:
        args.manifest = args.manifest.resolve()
    if (
        not args.execute
        or args.output.exists()
        or not args.output.parent.is_dir()
        or (
            args.manifest is not None
            and not args.manifest.is_file()
        )
    ):
        parser.error(
            "local preflight requires exact inputs, a new output and --execute"
        )
    return args


def execute(args):
    if args.manifest is None:
        report = build_authorization_clock_report(args.session_date)
        manifest_file_sha256 = None
    else:
        body = args.manifest.read_bytes()
        manifest = json.loads(body)
        if not isinstance(manifest, dict):
            raise TypeError("manifest must be a JSON object")
        if manifest.get("session_date") != args.session_date.isoformat():
            raise ValueError("manifest session differs from local preflight")
        report = validate_manifest_authorization_clocks(manifest)
        manifest_file_sha256 = hashlib.sha256(body).hexdigest()
        report.pop("semantic_sha256")
        report["manifest"] = str(args.manifest)
        report["manifest_file_sha256"] = manifest_file_sha256
        report["semantic_sha256"] = canonical_sha256(report)
    encoded = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    return {
        "status": report["status"],
        "mode": report["mode"],
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "manifest_file_sha256": manifest_file_sha256,
        "output": str(args.output),
        "effects": report["effects"],
    }


def main(argv=None):
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
