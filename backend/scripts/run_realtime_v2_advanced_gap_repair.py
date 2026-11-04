#!/usr/bin/env python3
"""Plan or fill the earliest minutes missing from one Advanced session's bars."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_preparation import (  # noqa: E402
    BoundedPolygonEquityPreparationClient,
)
from realtime.advanced_ingress_plan import (  # noqa: E402
    AUTOMATIC_CATCH_UP_RESPONSE_BYTE_LIMIT,
)
from realtime.advanced_stream_gap_repair import (  # noqa: E402
    first_missing_minute_range,
    load_stored_minutes_cursor,
    load_stream_gap_repair_scope_cursor,
    repair_advanced_stream_hole,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
# A journal-backed minute normally lands within seconds of its bar closing.
DEFAULT_STORAGE_GRACE_SECONDS = 180
EXIT_COVERAGE_PENDING = 3


def _aware(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--gap-start", type=_aware)
    parser.add_argument("--gap-end", type=_aware)
    parser.add_argument(
        "--response-byte-limit",
        type=int,
        default=AUTOMATIC_CATCH_UP_RESPONSE_BYTE_LIMIT,
    )
    parser.add_argument(
        "--storage-grace-seconds",
        type=int,
        default=DEFAULT_STORAGE_GRACE_SECONDS,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced gap repair targets only stocks_realtime_v2")
    if (args.gap_start is None) != (args.gap_end is None):
        parser.error("--gap-start and --gap-end go together")
    if args.response_byte_limit <= 0 or args.storage_grace_seconds < 0:
        parser.error("gap repair limits must be positive")
    args.report_path = args.report_path.resolve()
    return args


def _provider(config):
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=config.app_dsn,
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


def find_session_hole(provider, plan_id, *, checked_at, grace_seconds):
    """The earliest hole older than the storage grace, or None."""
    calendar = OptionExchangeCalendar("XNYS")
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        scope = load_stream_gap_repair_scope_cursor(unit.cursor, plan_id)
        opened = calendar.session_open(scope.session_date)
        through = min(
            calendar.session_close(scope.session_date),
            checked_at - timedelta(seconds=grace_seconds),
        ).replace(second=0, microsecond=0)
        if through <= opened:
            return None
        instrument_ids = tuple(
            member.instrument_id for member in scope.members
        )
        present = load_stored_minutes_cursor(
            unit.cursor,
            cohort_id=scope.cohort_id,
            instrument_ids=instrument_ids,
            start=opened,
            end=through,
        )
    return first_missing_minute_range(
        present,
        instrument_ids=instrument_ids,
        start=opened,
        end=through,
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    env_file = args.env_file or BACKEND_DIR / ".env"
    file_environment = (
        {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }
        if env_file.is_file()
        else {}
    )
    config = RealtimeV2PipelineConfig.from_mapping({
        **file_environment,
        **os.environ,
    },
        process=RealtimeV2Process.EQUITY_PREPARATION,
    )
    provider, pool = _provider(config)
    try:
        checked_at = datetime.now(UTC)
        hole = (
            (args.gap_start, args.gap_end)
            if args.gap_start is not None
            else find_session_hole(
                provider,
                args.plan_id,
                checked_at=checked_at,
                grace_seconds=args.storage_grace_seconds,
            )
        )
        report = {
            "mode": "PLAN" if args.plan else "EXECUTE",
            "checked_at": checked_at.isoformat(),
            "plan_id": str(args.plan_id),
            "hole_start": hole[0].isoformat() if hole else None,
            "hole_end": hole[1].isoformat() if hole else None,
            "provider_requests": 0,
        }
        exit_code = 0
        if args.plan or hole is None:
            report["status"] = "NO_HOLE" if hole is None else "PLANNED"
        else:
            report.update(
                repair_advanced_stream_hole(
                    provider,
                    BoundedPolygonEquityPreparationClient(
                        config.massive_api_key
                    ),
                    plan_id=args.plan_id,
                    hole_start=hole[0],
                    hole_end=hole[1],
                    response_byte_limit=args.response_byte_limit,
                )
            )
            report["mode"] = "EXECUTE"
            if report["status"] != "COMPLETE":
                report["status"] = "RETRYABLE_COVERAGE_PENDING"
                exit_code = EXIT_COVERAGE_PENDING
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return exit_code
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
