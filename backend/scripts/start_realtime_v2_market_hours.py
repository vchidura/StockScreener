#!/usr/bin/env python3
"""Plan or start the Realtime V2 service for one derived XNYS market day."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.session_windows import developer_session_windows  # noqa: E402
from scripts.run_realtime_v2_services import main as run_services  # noqa: E402


EXCHANGE_TIME_ZONE = ZoneInfo("America/New_York")
REQUESTS_PER_WINDOW = 18
STANDING_AUTHORIZATION_REFERENCE = (
    "standing-realtime-v2-developer-market-hours-v1"
)


@dataclass(frozen=True, slots=True)
class MarketDayScope:
    session_date: date
    first_cutoff: datetime
    window_count: int
    daily_request_ceiling: int


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--session-date",
        type=date.fromisoformat,
        help="XNYS session to run; default is the current America/New_York date",
    )
    parser.add_argument(
        "--authorization-reference",
        default=STANDING_AUTHORIZATION_REFERENCE,
    )
    parser.add_argument("--api-port", type=int, default=8002)
    parser.add_argument("--frontend-port", type=int, default=5174)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.authorization_reference = args.authorization_reference.strip()
    if args.execute and not args.authorization_reference:
        parser.error("--execute requires --authorization-reference")
    if (
        args.api_port == args.frontend_port
        or min(args.api_port, args.frontend_port) <= 0
    ):
        parser.error("API and frontend ports must be distinct positive values")
    return args


def exchange_today() -> date:
    return datetime.now(EXCHANGE_TIME_ZONE).date()


def market_day_scope(session_date: date) -> MarketDayScope | None:
    calendar = exchange_calendars.get_calendar("XNYS")
    if not calendar.is_session(session_date.isoformat()):
        return None
    windows = developer_session_windows(
        session_date,
        completion_budget_seconds=300,
        serving_lag_seconds=30,
    )
    if not windows:
        raise RuntimeError("XNYS session has no Developer windows")
    return MarketDayScope(
        session_date=session_date,
        first_cutoff=windows[0].cutoff,
        window_count=len(windows),
        daily_request_ceiling=len(windows) * REQUESTS_PER_WINDOW,
    )


def main(argv=None):
    args = parse_args(argv)
    session_date = args.session_date or exchange_today()
    scope = market_day_scope(session_date)
    if scope is None:
        print(json.dumps({
            "mode": "EXECUTE" if args.execute else "PLAN",
            "status": "SKIPPED_NOT_XNYS_SESSION",
            "session_date": session_date.isoformat(),
            "provider_requests": 0,
            "database_writes": 0,
            "services_started": False,
        }, sort_keys=True))
        return 0
    command = [
        "--session-date",
        scope.session_date.isoformat(),
        "--continuous-ingestion",
        "--first-cutoff",
        scope.first_cutoff.isoformat(),
        "--daily-request-ceiling",
        str(scope.daily_request_ceiling),
        "--authorization-reference",
        args.authorization_reference,
        "--api-port",
        str(args.api_port),
        "--frontend-port",
        str(args.frontend_port),
    ]
    if args.execute:
        command.append("--execute")
    if not args.execute:
        print(json.dumps({
            "derived_market_day": {
                **asdict(scope),
                "session_date": scope.session_date.isoformat(),
                "first_cutoff": scope.first_cutoff.isoformat(),
            },
            "standing_authorization_reference": args.authorization_reference,
        }, indent=2, default=str))
    return run_services(command)


if __name__ == "__main__":
    raise SystemExit(main())
