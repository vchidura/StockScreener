#!/usr/bin/env python3
"""Run a bounded sequence of consecutive V2 Developer windows."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2
from scripts.realtime_script_support import write_report
from scripts.run_realtime_v2_developer_window import main as run_window

REQUESTS_PER_WINDOW = 18
DEVELOPER_DELAY = timedelta(minutes=15)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--window-count", type=int, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--report-directory", type=Path, required=True)
    parser.add_argument("--summary-report", type=Path, required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Developer service targets only stocks_realtime_v2")
    if args.first_cutoff.tzinfo is None or args.first_cutoff.utcoffset() is None:
        parser.error("--first-cutoff must be timezone-aware")
    args.first_cutoff = args.first_cutoff.astimezone(timezone.utc)
    if (
        args.first_cutoff.minute % 15
        or args.first_cutoff.second
        or args.first_cutoff.microsecond
    ):
        parser.error("--first-cutoff must be a 15-minute boundary")
    if not 1 <= args.window_count <= 26:
        parser.error("--window-count must be in [1, 26]")
    expected_requests = args.window_count * REQUESTS_PER_WINDOW
    if args.maximum_provider_requests != expected_requests:
        parser.error(
            f"--maximum-provider-requests must equal {expected_requests}"
        )
    cutoffs = tuple(
        args.first_cutoff + timedelta(minutes=15 * index)
        for index in range(args.window_count)
    )
    calendar = exchange_calendars.get_calendar("XNYS")
    if (
        not calendar.is_session(str(args.session_date))
        or any(cutoff.date() != args.session_date for cutoff in cutoffs)
        or any(
            not calendar.session_open(str(args.session_date)).to_pydatetime()
            < cutoff
            < calendar.session_close(str(args.session_date)).to_pydatetime()
            for cutoff in cutoffs
        )
    ):
        parser.error("Developer service cutoffs must remain inside one XNYS session")
    for path in (*(
        args.report_directory / f"window-{cutoff:%H%M}.json"
        for cutoff in cutoffs
    ), args.summary_report):
        if path.exists():
            parser.error(f"report path already exists: {path}")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    args.cutoffs = cutoffs
    args.session_open = calendar.session_open(
        str(args.session_date)
    ).to_pydatetime()
    return args


def main(argv=None):
    args = parse_args(argv)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "cutoffs": [cutoff.isoformat() for cutoff in args.cutoffs],
        "window_count": args.window_count,
        "maximum_provider_requests": args.maximum_provider_requests,
    }
    if not args.execute:
        print(json.dumps({**plan, "provider_requests": 0, "database_writes": 0}, sort_keys=True))
        return 0
    reports = []
    started_at = datetime.now(timezone.utc)
    for cutoff in args.cutoffs:
        available_at = cutoff + DEVELOPER_DELAY
        remaining = (available_at - datetime.now(timezone.utc)).total_seconds()
        if remaining > 0:
            time.sleep(remaining + 0.25)
        report_path = args.report_directory / f"window-{cutoff:%H%M}.json"
        cache_path = args.cache_root / f"window-{cutoff:%H%M}"
        code = run_window([
            "--database", args.database,
            "--confirm-database-name", args.confirm_database_name,
            "--cohort-id", str(args.cohort_id),
            "--window-start", args.session_open.isoformat(),
            "--cutoff", cutoff.isoformat(),
            "--cache-directory", str(cache_path),
            "--report-path", str(report_path),
            "--authorization-reference",
            f"{args.authorization_reference}:{cutoff.isoformat()}",
            "--wait-for-input-receipt",
            "--execute",
        ])
        if code != 0 or not report_path.is_file():
            raise RuntimeError(f"Developer service window failed: {cutoff.isoformat()}")
        reports.append(json.loads(report_path.read_text(encoding="utf-8")))
    completed_at = datetime.now(timezone.utc)
    provider_requests = sum(
        int(report["provider_requests"])
        for report in reports
    )
    if provider_requests > args.maximum_provider_requests:
        raise RuntimeError("Developer service exceeded its provider request ceiling")
    summary = {
        **plan,
        "status": "COMPLETE",
        "provider_requests": provider_requests,
        "provider_cache_hits": sum(
            int(report["provider_cache_hits"])
            for report in reports
        ),
        "windows": reports,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": (completed_at - started_at).total_seconds(),
    }
    write_report(args.summary_report.resolve(), summary)
    print(json.dumps({
        "state": "COMPLETE",
        "cutoffs": plan["cutoffs"],
        "provider_requests": provider_requests,
        "duration_seconds": summary["duration_seconds"],
        "summary_report": str(args.summary_report.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
