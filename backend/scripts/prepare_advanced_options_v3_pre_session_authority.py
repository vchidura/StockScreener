#!/usr/bin/env python3
"""Prepare one immutable pre-opening Options V3 session authority."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_options_v3_authority import (  # noqa: E402
    prospective_opening_admission_payload,
    session_acquisition_authorization_payload,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.run_advanced_options_v3_market_loop import (  # noqa: E402
    _load_bound_json,
    _prepare_session_authority,
    _provider,
    _write_json,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--capture-semantic-sha256", required=True)
    parser.add_argument("--capture-file-sha256", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--package-nonce", type=UUID, required=True)
    parser.add_argument(
        "--acquisition-start",
        type=datetime.fromisoformat,
    )
    parser.add_argument("--hard-stop", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or not args.authorization_reference.strip()
        or (
            args.acquisition_start is not None
            and (
                args.acquisition_start.tzinfo is None
                or args.acquisition_start.utcoffset() is None
            )
        )
        or args.hard_stop.tzinfo is None
        or args.hard_stop.utcoffset() is None
        or not args.execute
    ):
        parser.error("pre-session authority scope is invalid")
    if args.acquisition_start is not None:
        args.acquisition_start = args.acquisition_start.astimezone(UTC)
    args.hard_stop = args.hard_stop.astimezone(UTC)
    calendar = OptionExchangeCalendar()
    session_open = calendar.session_open(args.session_date)
    decision_stop = (
        calendar.session_close(args.session_date)
        + timedelta(seconds=300)
    )
    if (
        args.hard_stop.date() != args.session_date
        or not session_open < args.hard_stop
        <= decision_stop
        or (
            args.acquisition_start is not None
            and not (
                args.acquisition_start.date() == args.session_date
                and args.acquisition_start <= session_open
            )
        )
    ):
        parser.error("pre-session authority hard stop changed")
    args.capture = args.capture.resolve()
    args.output = args.output.resolve()
    if not args.capture.is_file() or args.output.exists():
        parser.error("capture must exist and output must be new")
    for name in (
        "capture_semantic_sha256",
        "capture_file_sha256",
    ):
        value = getattr(args, name)
        if len(value) != 64 or any(
            character not in "0123456789abcdef"
            for character in value
        ):
            parser.error(f"--{name.replace('_', '-')} is not a SHA-256")
    return args


def execute(args):
    capture = _load_bound_json(
        args.capture,
        args.capture_semantic_sha256,
        args.capture_file_sha256,
        "semantic_sha256",
    )
    captured_at = datetime.fromisoformat(
        str(capture["captured_at_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    boundary = OptionExchangeCalendar().session_open(args.session_date)
    if (
        capture.get("session_date") != args.session_date.isoformat()
        or captured_at >= boundary
    ):
        raise ValueError(
            "pre-session authority requires an actual pre-opening capture"
        )
    cohort_id = uuid5(
        NAMESPACE_URL,
        (
            f"stockscreener:{args.authorization_reference}:"
            f"{args.package_nonce}:MARKET_OPTIONS"
        ),
    )
    provider, pool, _ = _provider()
    try:
        admission, readiness, authority = _prepare_session_authority(
            provider,
            capture,
            args=SimpleNamespace(
                session_date=args.session_date,
                package_nonce=args.package_nonce,
                authorization_reference=args.authorization_reference,
                hard_stop=args.hard_stop,
            ),
            cohort_id=cohort_id,
            prepared_at=captured_at,
            capture_boundary=boundary,
            acquisition_start=args.acquisition_start,
        )
    finally:
        pool.closeall()
    document = {
        "contract": "advanced-options-v3-pre-session-authority-v1",
        "session_date": args.session_date.isoformat(),
        "target_cohort_id": str(cohort_id),
        "package_nonce": str(args.package_nonce),
        "authorization_reference": args.authorization_reference,
        "source_capture_semantic_sha256": (
            args.capture_semantic_sha256
        ),
        "source_capture_file_sha256": args.capture_file_sha256,
        "admission": prospective_opening_admission_payload(admission),
        "admitted_contracts": [
            row.contract_ticker for row in admission.contracts
        ],
        "readiness_sha256": readiness.readiness_sha256,
        "session_acquisition": (
            session_acquisition_authorization_payload(authority)
        ),
        "provider_requests": 0,
        "database_writes": 0,
    }
    document["semantic_sha256"] = canonical_sha256(document)
    _write_json(args.output, document)
    return {
        "status": "COMPLETE",
        "target_cohort_id": str(cohort_id),
        "semantic_sha256": document["semantic_sha256"],
        "file_sha256": hashlib.sha256(
            args.output.read_bytes()
        ).hexdigest(),
        "output": str(args.output),
        "provider_requests": 0,
        "database_writes": 0,
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
