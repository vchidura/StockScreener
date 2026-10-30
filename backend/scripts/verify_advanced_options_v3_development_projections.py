#!/usr/bin/env python3
"""Verify non-serving Equity and Options diagnostic projections."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_projection import (  # noqa: E402
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
)
from realtime.advanced_options_validation_mode import (  # noqa: E402
    AdvancedOptionsValidationMode,
    advanced_options_projection_policy,
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
    parser.add_argument("--equity-cohort-id", type=UUID, required=True)
    parser.add_argument("--equity-window-id", type=UUID, required=True)
    parser.add_argument("--options-cohort-id", type=UUID, required=True)
    parser.add_argument("--options-window-id", type=UUID, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    args.output = args.output.resolve()
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.output.exists()
    ):
        parser.error("development projection verification scope is invalid")
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
            statement_timeout_ms=10_000,
        ),
    )
    provider.verify()
    return provider, pool


def _single_receipt(cursor, window_id, kind):
    cursor.execute(
        "SELECT receipt_id::text,cohort_id::text,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id "
        "FROM rt.cycle_receipts "
        "WHERE window_id=%s AND receipt_kind=%s",
        (str(window_id), kind),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    if len(rows) != 1:
        raise RuntimeError(
            f"diagnostic projection requires one {kind} receipt"
        )
    return rows[0]


def _validate_summary(summary):
    expected_types = set(OPTIONS_PROJECTION_TYPES) | set(
        OPTIONS_PAGE_SNAPSHOT_TYPES.values()
    )
    if (
        summary["validation_mode"]
        != AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
        or summary["equity_receipt"]["cohort_id"]
        != summary["equity_cohort_id"]
        or summary["options_receipt"]["cohort_id"]
        != summary["options_cohort_id"]
        or set(summary["options_snapshot_types"]) != expected_types
        or summary["release_count"] != 0
        or summary["pointer_count"] != 0
        or summary["read_pin_count"] != 0
        or summary["outbox_count"] != 0
    ):
        raise RuntimeError(
            "development portal projection postcondition failed"
        )
    return summary


def main(argv=None) -> int:
    args = parse_args(argv)
    environment = {
        **{
            str(key): str(value)
            for key, value in dotenv_values(BACKEND_DIR / ".env").items()
            if value is not None
        },
        **os.environ,
    }
    policy = advanced_options_projection_policy(environment)
    if (
        policy.validation_mode
        is not AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
        or policy.release_allowed
        or policy.pointer_allowed
        or policy.read_pin_allowed
        or policy.normal_portal_serving_allowed
    ):
        raise RuntimeError(
            "development projection verifier requires non-serving mode"
        )
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            equity_receipt = _single_receipt(
                unit.cursor,
                args.equity_window_id,
                policy.equity_terminal_receipt,
            )
            options_receipt = _single_receipt(
                unit.cursor,
                args.options_window_id,
                policy.options_terminal_receipt,
            )
            expected_types = tuple(sorted(
                set(OPTIONS_PROJECTION_TYPES)
                | set(OPTIONS_PAGE_SNAPSHOT_TYPES.values())
            ))
            unit.cursor.execute(
                "SELECT snapshot_type FROM rt.snapshot_catalog "
                "WHERE cohort_id=%s "
                "AND provider_tier='MASSIVE_ADVANCED' "
                "AND snapshot_key='CURRENT' "
                "AND generated_at=%s "
                "AND snapshot_type=ANY(%s) "
                "ORDER BY snapshot_type",
                (
                    str(args.options_cohort_id),
                    options_receipt["cutoff"],
                    list(expected_types),
                ),
            )
            snapshot_types = [
                str(row["snapshot_type"])
                for row in unit.cursor.fetchall()
            ]
            unit.cursor.execute(
                "SELECT "
                "(SELECT count(*) FROM rt.portal_releases "
                "WHERE cohort_id=ANY(%s::uuid[])) AS release_count,"
                "(SELECT count(*) FROM rt.portal_release_current "
                "WHERE cohort_id=ANY(%s::uuid[])) AS pointer_count,"
                "(SELECT count(*) FROM rt.advanced_options_read_pins "
                "WHERE cohort_id=%s) AS read_pin_count,"
                "(SELECT count(*) FROM rt.alert_outbox "
                "WHERE cohort_id=ANY(%s::uuid[])) AS outbox_count",
                (
                    [
                        str(args.equity_cohort_id),
                        str(args.options_cohort_id),
                    ],
                    [
                        str(args.equity_cohort_id),
                        str(args.options_cohort_id),
                    ],
                    str(args.options_cohort_id),
                    [
                        str(args.equity_cohort_id),
                        str(args.options_cohort_id),
                    ],
                ),
            )
            effects = dict(unit.cursor.fetchone())
    finally:
        pool.closeall()

    report = _validate_summary({
        "contract": (
            "advanced-options-v3-development-projection-verification-v1"
        ),
        "validation_mode": policy.validation_mode.value,
        "equity_cohort_id": str(args.equity_cohort_id),
        "equity_window_id": str(args.equity_window_id),
        "equity_receipt": equity_receipt,
        "options_cohort_id": str(args.options_cohort_id),
        "options_window_id": str(args.options_window_id),
        "options_receipt": options_receipt,
        "options_snapshot_types": snapshot_types,
        "release_count": int(effects["release_count"]),
        "pointer_count": int(effects["pointer_count"]),
        "read_pin_count": int(effects["read_pin_count"]),
        "outbox_count": int(effects["outbox_count"]),
        "provider_requests": 0,
        "database_writes": 0,
        "normal_portal_serving": False,
    })
    report["semantic_sha256"] = canonical_sha256(report)
    encoded = (
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    print(json.dumps({
        "status": "PASS_DEVELOPMENT_PROJECTIONS_NON_SERVING",
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
