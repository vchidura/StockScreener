#!/usr/bin/env python3
"""Replay one sealed V3 admission capture without provider or production effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    p8_activity_selection_capture_from_sealed,
)
from realtime.contracts import canonical_sha256  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--capture-file-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.capture = args.capture.resolve()
    args.output = args.output.resolve()
    if (
        not args.capture.is_file()
        or len(args.capture_file_sha256) != 64
        or any(
            value not in "0123456789abcdef"
            for value in args.capture_file_sha256
        )
        or args.output.exists()
        or not args.output.parent.is_dir()
        or not args.execute
    ):
        parser.error(
            "local replay requires an exact capture, new output and --execute"
        )
    return args


def execute(args):
    body = args.capture.read_bytes()
    if hashlib.sha256(body).hexdigest() != args.capture_file_sha256:
        raise ValueError("sealed capture file identity changed")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise TypeError("sealed capture must be a JSON object")
    document = dict(value)
    semantic_sha256 = document.pop("semantic_sha256", None)
    if (
        not isinstance(semantic_sha256, str)
        or canonical_sha256(document) != semantic_sha256
    ):
        raise ValueError("sealed capture semantic identity changed")
    capture = p8_activity_selection_capture_from_sealed(value)
    report = {
        "schema": "advanced-options-v3-local-capture-replay-v1",
        "status": "PASS",
        "mode": "PROVIDER_FREE_LOCAL_REPLAY",
        "replayed_at_utc": datetime.now(UTC).isoformat(),
        "capture": {
            "path": str(args.capture),
            "file_sha256": args.capture_file_sha256,
            "semantic_sha256": semantic_sha256,
            "recorded_provider_requests": capture.request_count,
            "decoded_bytes": capture.decoded_bytes,
        },
        "selection": {
            "status": capture.selection.status.value,
            "policy_sha256": capture.selection.policy.sha256,
            "reviewed_at_utc": (
                capture.selection.reviewed_at.isoformat()
            ),
            "valid_until_utc": (
                capture.selection.valid_until.isoformat()
            ),
            "source_sha256": capture.selection.source_sha256,
            "selection_sha256": capture.selection.selection_sha256,
            "contracts": [
                {
                    "underlyer": row.underlyer,
                    "contract_ticker": row.contract_ticker,
                    "option_type": row.option_type,
                    "expiration_date": row.expiration_date.isoformat(),
                    "strike": str(row.strike),
                    "bid": str(row.bid),
                    "ask": str(row.ask),
                    "day_volume": row.day_volume,
                    "open_interest": row.open_interest,
                    "market_data_time": row.market_data_time.isoformat(),
                    "source_sha256": row.source_sha256,
                }
                for row in capture.selection.selected
            ],
        },
        "effects": {
            "provider_requests": 0,
            "database_reads": 0,
            "database_writes": 0,
            "docker_actions": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
        },
    }
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
        "contract_count": len(report["selection"]["contracts"]),
        "effects": report["effects"],
        "output": str(args.output),
    }


def main(argv=None):
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
