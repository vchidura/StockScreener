#!/usr/bin/env python3
"""Prepare one sealed synthetic current-session development fixture E."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_development_fixture import (  # noqa: E402
    build_development_fixture_e,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--equity-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--qualification-semantic-sha256", required=True)
    parser.add_argument("--qualification-file-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    args.output = args.output.resolve()
    if args.output.exists() or not args.output.parent.is_dir():
        parser.error("fixture E output must be new in an existing directory")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    document = build_development_fixture_e(
        session_date=args.session_date,
        equity_cutoff=args.equity_cutoff,
        authorization_reference=args.authorization_reference,
        qualification_semantic_sha256=(
            args.qualification_semantic_sha256
        ),
        qualification_file_sha256=args.qualification_file_sha256,
    )
    encoded = (
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    print(json.dumps({
        "status": "PREPARED_DEVELOPMENT_DIAGNOSTIC_FIXTURE_E",
        "semantic_sha256": document["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "output": str(args.output),
        "database_writes": 0,
        "provider_requests": 0,
        "production_eligible": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
