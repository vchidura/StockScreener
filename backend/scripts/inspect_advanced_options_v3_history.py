#!/usr/bin/env python3
"""Verify and report the exact immutable Advanced Options V3 H binding."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_control_plane import (  # noqa: E402
    advanced_options_history_binding_payload,
)
from realtime.advanced_options_v3_history import (  # noqa: E402
    AdvancedOptionsHistoryBindingIdentityError,
    AdvancedOptionsHistoryCoverageError,
    advanced_options_v3_history_source_configuration,
    load_verified_advanced_options_v3_history_binding_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--target-cohort-id", type=UUID, required=True)
    parser.add_argument("--known-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.known_at.tzinfo is None
        or args.known_at.utcoffset() is None
        or args.output.exists()
        or not args.authorization_reference.strip()
    ):
        parser.error("V3 H inspection scope is invalid")
    args.output = args.output.resolve()
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _write(path: Path, report) -> None:
    encoded = (
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, pool = _provider()
    report = {
        "contract": "advanced-options-v3-history-preflight-v1",
        "mode": "READ_ONLY",
        "status": "BLOCKED",
        "failure_class": None,
        "source_configuration": (
            advanced_options_v3_history_source_configuration()
        ),
        "binding": None,
        "evidence": None,
        "effects": {
            "provider_requests": 0,
            "provider_websocket": 0,
            "production_database_writes": 0,
            "service_actions": 0,
            "publication": 0,
            "stored_read_changes": 0,
        },
    }
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            verified = (
                load_verified_advanced_options_v3_history_binding_cursor(
                    unit.cursor,
                    target_cohort_id=args.target_cohort_id,
                    session_date=args.session_date,
                    known_at=args.known_at,
                    authorization_reference=(
                        args.authorization_reference
                    ),
                )
            )
        report.update({
            "status": "PASS",
            "binding": advanced_options_history_binding_payload(
                verified.binding
            ),
            "evidence": verified.evidence,
        })
    except AdvancedOptionsHistoryBindingIdentityError as error:
        report["failure_class"] = "H_BINDING_IDENTITY_DEFECT"
        report["failure_detail"] = str(error)
    except AdvancedOptionsHistoryCoverageError as error:
        report["failure_class"] = "H_COVERAGE_MISSING"
        report["failure_detail"] = str(error)
    finally:
        pool.closeall()
    report["semantic_sha256"] = canonical_sha256(report)
    _write(args.output, report)
    result = {
        "status": report["status"],
        "failure_class": report["failure_class"],
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(
            args.output.read_bytes()
        ).hexdigest(),
        "output": str(args.output),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
