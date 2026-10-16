#!/usr/bin/env python3
"""Append one fresh exact-nine reference generation to canonical stocks_realtime_v2."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.reference_repository import persist_reference_result  # noqa: E402
from realtime.reference_seed import SeedStatus  # noqa: E402
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from realtime.reference_transports import (  # noqa: E402
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
    build_live_reference_service,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402

APPROVED_SESSION_DATE = date(2026, 10, 15)
EXPECTED_REQUESTS = 31
MAXIMUM_REQUESTS = 64
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5
ETF_TICKERS = frozenset({"SPY", "QQQ", "IWM"})
SEED_SCOPE = "EQUITY_VALIDATION_REFERENCES"


class PacedAccountingReferenceTransport:
    def __init__(
        self,
        delegate,
        *,
        maximum_requests: int = MAXIMUM_REQUESTS,
        minimum_interval_seconds: float = MINIMUM_REQUEST_INTERVAL_SECONDS,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        if maximum_requests <= 0 or minimum_interval_seconds <= 0:
            raise ValueError("reference request bounds must be positive")
        self.delegate = delegate
        self.maximum_requests = maximum_requests
        self.minimum_interval_seconds = minimum_interval_seconds
        self.monotonic = monotonic
        self.sleep = sleep
        self.requests = []
        self._last_completed_at = None
        self._lock = threading.Lock()

    def get(self, url, params, headers):
        with self._lock:
            if len(self.requests) >= self.maximum_requests:
                raise RuntimeError("approved reference request ceiling reached")
            if self._last_completed_at is not None:
                remaining = (
                    self._last_completed_at
                    + self.minimum_interval_seconds
                    - self.monotonic()
                )
                if remaining > 0:
                    self.sleep(remaining)
            started = datetime.now(timezone.utc)
            response = self.delegate.get(url, params, headers)
            completed = datetime.now(timezone.utc)
            self._last_completed_at = self.monotonic()
            self.requests.append({
                "host": urlsplit(url).hostname,
                "path": urlsplit(url).path,
                "status_code": response.status_code,
                "response_bytes": len(response.body),
                "started_at": started.isoformat(),
                "completed_at": completed.isoformat(),
            })
            return response


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--output")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the V2 reference refresh targets only stocks_realtime_v2")
    if args.session_date != APPROVED_SESSION_DATE:
        parser.error(f"the approved reference session is {APPROVED_SESSION_DATE}")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    return args


def main(argv=None):
    args = parse_args(argv)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "scope": SEED_SCOPE,
        "session_date": args.session_date.isoformat(),
        "tickers": list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
        "expected_requests": EXPECTED_REQUESTS,
        "maximum_requests": MAXIMUM_REQUESTS,
        "request_in_flight": 1,
        "minimum_request_interval_seconds": MINIMUM_REQUEST_INTERVAL_SECONDS,
        "fundamentals": False,
        "options": False,
    }
    if not args.execute:
        print(json.dumps({**plan, "provider_requests": 0, "database_writes": 0}, sort_keys=True))
        return 0
    settings = admin_settings(BACKEND_DIR / ".env")
    load_dotenv(BACKEND_DIR / ".env", override=True)
    massive_key = os.getenv("POLYGON_API_KEY", "").strip()
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not massive_key or not finnhub_key:
        raise RuntimeError("V2 reference refresh requires POLYGON_API_KEY and FINNHUB_API_KEY")
    started_at = datetime.now(timezone.utc)
    transport = PacedAccountingReferenceTransport(RequestsReferenceHttpTransport())
    service = build_live_reference_service(
        massive_api_key=massive_key,
        finnhub_api_key=finnhub_key,
        etf_tickers=ETF_TICKERS,
        http=transport,
        massive_base_url="https://api.massive.com",
        limits=ReferenceTransportLimits(
            maximum_requests=MAXIMUM_REQUESTS,
            maximum_response_bytes=2_000_000,
            maximum_total_bytes=16_000_000,
            maximum_rows=50_000,
            maximum_pages=16,
        ),
        include_fundamentals=False,
    )
    result = service.run_once(ReferenceSeedRequest(
        EQUITY_VALIDATION_UNIVERSE,
        args.session_date,
        started_at,
        started_at + timedelta(minutes=1),
        started_at + timedelta(hours=6),
    ))
    if (
        result.provider_requests != len(transport.requests)
        or result.provider_requests > MAXIMUM_REQUESTS
        or result.manifest.status is not SeedStatus.READY
        or len(result.manifest.members) != 29
        or any(request["status_code"] != 200 for request in transport.requests)
    ):
        raise RuntimeError("V2 reference refresh failed its request, member or READY contract")
    with closing(connect(args.database, settings, application_name="refresh_v2_references")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_database()::text,lineage_name FROM rt.database_identity"
            )
            if tuple(cursor.fetchone()) != (args.database, "advanced_realtime_v2"):
                raise RuntimeError("V2 reference refresh target identity mismatch")
            cursor.execute(
                "SELECT COALESCE(MAX(generation),0)+1 FROM rt.seed_current WHERE scope=%s",
                (SEED_SCOPE,),
            )
            generation = int(cursor.fetchone()[0])
        connection.rollback()
        persist_reference_result(
            connection,
            result,
            generation=generation,
            scope=SEED_SCOPE,
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current.generation,seed.seed_id,seed.status,seed.member_count,"
                "seed.effective_at,seed.valid_until "
                "FROM rt.seed_current AS current JOIN rt.seed_manifests AS seed USING(seed_id) "
                "WHERE current.scope=%s",
                (SEED_SCOPE,),
            )
            current = cursor.fetchone()
            cursor.execute(
                "SELECT COUNT(DISTINCT ticker) FROM ref.security_revisions "
                "WHERE ticker=ANY(%s) AND active AND effective_from<=clock_timestamp() "
                "AND effective_to>clock_timestamp() AND observed_at<=clock_timestamp()",
                (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),),
            )
            valid_tickers = int(cursor.fetchone()[0])
        connection.rollback()
    if (
        current is None
        or current[0] != generation
        or str(current[1]) != str(result.manifest.seed_id)
        or current[2] != "READY"
        or current[3] != 29
        or valid_tickers != 9
    ):
        raise RuntimeError("persisted V2 reference generation failed current identity validation")
    completed_at = datetime.now(timezone.utc)
    report = {
        **plan,
        "status": "REFERENCE_REFRESH_COMPLETE",
        "generation": generation,
        "seed_id": str(result.manifest.seed_id),
        "seed_sha256": result.manifest.seed_sha256,
        "member_count": len(result.manifest.members),
        "provider_requests": result.provider_requests,
        "response_bytes": result.response_bytes,
        "request_log": transport.requests,
        "valid_tickers": valid_tickers,
        "effective_at": current[4].isoformat(),
        "valid_until": current[5].isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": (completed_at - started_at).total_seconds(),
    }
    if args.output:
        output = Path(args.output).resolve()
        if output.exists():
            raise ValueError("V2 reference refresh report already exists")
        write_report(output, report)
        report["output"] = str(output)
    print(json.dumps({
        "state": "COMPLETE",
        "status": report["status"],
        "generation": generation,
        "provider_requests": result.provider_requests,
        "response_bytes": result.response_bytes,
        "valid_tickers": valid_tickers,
        "valid_until": report["valid_until"],
        "duration_seconds": report["duration_seconds"],
        "output": report.get("output"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
