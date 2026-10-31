#!/usr/bin/env python3
"""Freeze a provider-free QQQ/SPY 0-60 DTE WebSocket subscription plan."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_selection import (  # noqa: E402
    p7_transport_candidate_from_payload,
)
from realtime.advanced_options_v3_broad_stream import (  # noqa: E402
    broad_options_stream_plan_payload,
    build_broad_options_stream_plan,
)


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--reviewed-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--capability-profile-sha256", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    args.candidates = args.candidates.resolve()
    args.output = args.output.resolve()
    if (
        not args.candidates.is_file()
        or args.output.exists()
        or not args.output.parent.is_dir()
        or not args.authorization_reference.strip()
    ):
        parser.error("broad Options stream preparation scope is invalid")
    return args


def execute(args):
    payload = json.loads(args.candidates.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("broad Options candidates must be a JSON array")
    candidates = tuple(
        p7_transport_candidate_from_payload(row)
        for row in payload
    )
    plan = build_broad_options_stream_plan(
        candidates,
        session_date=args.session_date,
        reviewed_at=args.reviewed_at,
        capability_profile_sha256=args.capability_profile_sha256,
        authorization_reference=args.authorization_reference,
    )
    result = broad_options_stream_plan_payload(plan)
    _write(args.output, result)
    return result


def main(argv=None):
    result = execute(parse_args(argv))
    print(json.dumps({
        "status": "READY",
        "contract_count": result["contract_count"],
        "subscription_count": result["subscription_count"],
        "manifest_sha256": result["manifest_sha256"],
        "plan_sha256": result["plan_sha256"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
