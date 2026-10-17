#!/usr/bin/env python3
"""Execute the bounded SPY pre-stream 1m/5m Advanced REST repair."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

import requests
from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import BarAvailabilityMode, BarSourceKind  # noqa: E402
from equity.derivation import derive_minute_bars  # noqa: E402
from equity.polygon import normalize_native_bars  # noqa: E402
from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.acquisition_ledger import (  # noqa: E402
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
    finish_provider_request_cursor,
    load_raw_payload_cursor,
    load_recorded_provider_request_cursor,
    persist_raw_payload_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_bar_adapter import (  # noqa: E402
    RealtimeEquityBarFact,
    adapt_derived_equity_bar,
)
from realtime.equity_bar_repository import (  # noqa: E402
    persist_realtime_equity_bars_cursor,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
)
from realtime.preflight import ProviderTier  # noqa: E402
from realtime.equity_interval_finalizer import (  # noqa: E402
    _load_finalizer_security_cursor,
)
from realtime.provider_budget import (  # noqa: E402
    ProviderBudgetAdmission,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


INTERVALS = {"1m": 1}
REQUEST_CEILING = 2
TOTAL_BYTE_CEILING = 8_000_000
RESERVED_BYTES_PER_REQUEST = 4_000_000
RESPONSE_BYTE_LIMIT = 3_999_999


def _reserve_or_load_sealed_response_cursor(
    cursor,
    scope: ProviderBudgetScope,
    reservation: ProviderRequestReservation,
) -> tuple[
    ProviderBudgetAdmission | None,
    tuple[RecordedProviderRequest, bytes] | None,
]:
    recorded = load_recorded_provider_request_cursor(
        cursor,
        reservation.request_id,
    )
    if recorded is None:
        cursor.execute(
            "SELECT 1 FROM rt.provider_request_ledger WHERE request_id=%s",
            (str(reservation.request_id),),
        )
        if cursor.fetchone() is not None:
            raise RuntimeError(
                "Advanced repair sealed response is unavailable"
            )
        admission = reserve_coordinated_provider_request_cursor(
            cursor,
            scope,
            reservation,
            reserved_response_bytes=RESERVED_BYTES_PER_REQUEST,
        )
        return admission, None

    stored = recorded.reservation
    expected_identity = (
        reservation.request_id,
        reservation.account_alias,
        reservation.lane,
        reservation.endpoint_class,
        reservation.request_sha256,
        reservation.attempt,
        reservation.cohort_id,
        reservation.window_id,
        reservation.stage_run_id,
        scope.session_date,
        scope.authorization_reference,
        RESERVED_BYTES_PER_REQUEST,
        scope.request_ceiling,
        scope.decoded_byte_ceiling,
    )
    stored_identity = (
        stored.request_id,
        stored.account_alias,
        stored.lane,
        stored.endpoint_class,
        stored.request_sha256,
        stored.attempt,
        stored.cohort_id,
        stored.window_id,
        stored.stage_run_id,
        stored.budget_session_date,
        stored.authorization_reference,
        stored.reserved_response_bytes,
        stored.account_request_ceiling,
        stored.account_decoded_byte_ceiling,
    )
    if stored_identity != expected_identity:
        raise RuntimeError(
            "Advanced repair sealed response identity does not match"
        )
    if (
        recorded.completion.state is not ProviderRequestState.COMPLETE
        or recorded.completion.payload_id is None
    ):
        raise RuntimeError("Advanced repair sealed response is unavailable")
    body = load_raw_payload_cursor(
        cursor,
        recorded.completion.payload_id,
    )
    return None, (recorded, body)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--requested-from", type=datetime.fromisoformat, required=True)
    parser.add_argument("--requested-to", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in ("requested_from", "requested_to"):
        value = getattr(args, name)
        if value.tzinfo is None or value.utcoffset() is None:
            parser.error(f"--{name.replace('_', '-')} must be timezone-aware")
        setattr(args, name, value.astimezone(timezone.utc))
    if (
        args.session_date != date(2026, 10, 16)
        or args.requested_from
        != datetime(2026, 10, 16, 13, 30, tzinfo=timezone.utc)
        or args.requested_to
        != datetime(2026, 10, 16, 18, 15, tzinfo=timezone.utc)
    ):
        parser.error("repair scope must match the approved SPY interval")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if not args.execute:
        parser.error("Advanced intraday repair requires --execute")
    return args


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    pool = ThreadedConnectionPool(1, 2, dsn=config.app_dsn)
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix="stocks_realtime_v2",
            statement_timeout_ms=60_000,
        ),
    )
    provider.verify()
    session = requests.Session()
    transport = RequestsPolygonHttpTransport(
        config.massive_api_key,
        timeout_seconds=30,
        session_factory=lambda: session,
        maximum_response_bytes=RESPONSE_BYTE_LIMIT,
    )
    try:
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT member.instrument_id,reference.security_id "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_members AS member USING(manifest_id) "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "JOIN ref.security_revisions AS reference USING(security_id) "
                "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                "AND reference.ticker='SPY' AND reference.active "
                "AND reference.effective_from<=clock_timestamp() "
                "AND (reference.effective_to IS NULL "
                "OR reference.effective_to>clock_timestamp()) "
                "ORDER BY reference.observed_at DESC LIMIT 1",
                (str(args.cohort_id),),
            )
            identity = cursor.fetchone()
            if identity is None:
                raise ValueError("Advanced SPY repair identity is unavailable")
            instrument_id, security_id = int(identity[0]), UUID(str(identity[1]))
        scope = ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=args.session_date,
            authorization_reference=args.authorization_reference,
            provider_authorization_reference="stocks-advanced",
            expected_cost="no incremental charge expected",
            request_ceiling=REQUEST_CEILING,
            decoded_byte_ceiling=TOTAL_BYTE_CEILING,
            lanes=frozenset({ProviderRequestLane.EQUITY_CURRENT}),
            authorized_until=datetime.now(timezone.utc).replace(
                hour=23, minute=59, second=59
            ),
        )
        facts = []
        requests_used = []
        for interval, width in INTERVALS.items():
            request_sha256 = canonical_sha256({
                "contract": "advanced-intraday-rest-repair-v1",
                "cohort_id": args.cohort_id,
                "ticker": "SPY",
                "interval": interval,
                "requested_from": args.requested_from,
                "requested_to": args.requested_to,
                "authorization_reference": args.authorization_reference,
            })
            request_id = uuid5(
                NAMESPACE_URL,
                f"stockscreener:advanced-repair:{request_sha256}",
            )
            reserved_at = datetime.now(timezone.utc)
            reservation = ProviderRequestReservation(
                request_id,
                scope.account_alias,
                ProviderRequestLane.EQUITY_CURRENT,
                "ADVANCED_INTRADAY_REPAIR",
                request_sha256,
                reserved_at,
                cohort_id=args.cohort_id,
            )
            with provider.transaction(APP_ROLE) as unit:
                admission, replay = _reserve_or_load_sealed_response_cursor(
                    unit.cursor,
                    scope,
                    reservation,
                )
            provider_request_performed = False
            if admission is not None and admission.newly_reserved:
                start_ms = int(args.requested_from.timestamp() * 1000)
                end_ms = int(args.requested_to.timestamp() * 1000)
                url = (
                    "https://api.massive.com/v2/aggs/ticker/"
                    f"{quote('SPY', safe='')}/range/{width}/minute/"
                    f"{start_ms}/{end_ms}"
                )
                started_at = datetime.now(timezone.utc)
                response = transport.get(
                    url,
                    {"adjusted": "false", "sort": "asc", "limit": "50000"},
                )
                completed_at = datetime.now(timezone.utc)
                provider_request_performed = True
            else:
                if replay is None:
                    raise RuntimeError(
                        "Advanced repair sealed response is unavailable"
                    )
                recorded, body = replay
                response = type("ReplayResponse", (), {
                    "status_code": 200,
                    "body": body,
                })()
                started_at = (
                    recorded.completion.started_at
                    or recorded.reservation.reserved_at
                )
                completed_at = (
                    recorded.completion.completed_at
                    or recorded.reservation.reserved_at
                )
            if response.status_code != 200:
                with provider.transaction(APP_ROLE) as unit:
                    finish_provider_request_cursor(
                        unit.cursor,
                        request_id,
                        ProviderRequestCompletion(
                            ProviderRequestState.FAILED,
                            started_at=started_at,
                            completed_at=completed_at,
                            outcome=ProviderRequestOutcome.HTTP_ERROR,
                            http_status=response.status_code,
                            response_bytes=len(response.body),
                            failure_code=f"HTTP_{response.status_code}",
                        ),
                    )
                raise RuntimeError(
                    f"Advanced repair provider HTTP {response.status_code}"
                )
            payload = json.loads(response.body)
            rows = payload.get("results")
            if not isinstance(rows, list) or payload.get("next_url"):
                raise RuntimeError("Advanced repair response is incomplete")
            revisions = normalize_native_bars(
                security_id,
                "SPY",
                interval,
                rows,
                observed_at=completed_at,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                replay_available_at=None,
                ingestion_segment_id=request_id,
                adjusted=False,
            )
            expected = int(
                (args.requested_to - args.requested_from).total_seconds()
                // (width * 60)
            )
            selected = tuple(
                row
                for row in revisions
                if row.bar_start >= args.requested_from
                and row.bar_end <= args.requested_to
            )
            if (
                len(selected) != expected
                or selected[0].bar_start != args.requested_from
                or selected[-1].bar_end != args.requested_to
            ):
                raise RuntimeError("Advanced repair lacks exact bar coverage")
            if provider_request_performed:
                with provider.transaction(APP_ROLE) as unit:
                    payload_id = persist_raw_payload_cursor(
                        unit.cursor,
                        response.body,
                        arrived_at=completed_at,
                        request_id=request_id,
                    )
                    finish_provider_request_cursor(
                        unit.cursor,
                        request_id,
                        ProviderRequestCompletion(
                            ProviderRequestState.COMPLETE,
                            started_at=started_at,
                            completed_at=completed_at,
                            outcome=ProviderRequestOutcome.OK,
                            http_status=200,
                            response_bytes=len(response.body),
                            row_count=len(rows),
                            pages_fetched=1,
                            requested_from=args.requested_from,
                            requested_to=args.requested_to,
                            price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
                            completeness=RequestCompleteness.COMPLETE,
                            payload_id=payload_id,
                        ),
                    )
            for revision in selected:
                repaired = replace(
                    revision,
                    source_kind=BarSourceKind.NATIVE_REST,
                    quality_codes=tuple(dict.fromkeys((
                        *revision.quality_codes,
                        "EXPLICIT_ADVANCED_REST_REPAIR_V1",
                    ))),
                )
                facts.append(
                    RealtimeEquityBarFact(
                        args.cohort_id,
                        ProviderTier.MASSIVE_ADVANCED,
                        instrument_id,
                        repaired,
                    )
                )
            requests_used.append({
                "interval": interval,
                "request_id": str(request_id),
                "status_code": 200,
                "response_bytes": len(response.body),
                "rows": len(rows),
                "provider_request_performed": provider_request_performed,
            })
        database = V2WorkerDatabase(
            provider,
            RealtimeDatabaseRole.COMPUTE,
        )
        with database.transaction(read_only=True) as cursor:
            security = _load_finalizer_security_cursor(
                cursor,
                instrument_id=instrument_id,
                window_end=args.requested_to,
                as_of=datetime.now(timezone.utc),
            )
        source_revisions = tuple(fact.revision for fact in facts)
        derived_segment = uuid5(
            NAMESPACE_URL,
            "stockscreener:advanced-repair-5m:"
            f"{args.cohort_id}:{args.requested_from.isoformat()}:"
            f"{args.requested_to.isoformat()}",
        )
        derived = derive_minute_bars(
            security,
            source_revisions,
            target_interval="5m",
            observed_at=datetime.now(timezone.utc),
            ingestion_segment_id=derived_segment,
        )
        if len(derived) != 57:
            raise RuntimeError(
                "Advanced repair did not derive exact five-minute coverage"
            )
        facts.extend(
            adapt_derived_equity_bar(
                revision,
                cohort_id=args.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                instrument_id=instrument_id,
            )
            for revision in derived
        )
        with database.transaction() as cursor:
            for offset in range(0, len(facts), 1000):
                persist_realtime_equity_bars_cursor(
                    cursor,
                    tuple(facts[offset:offset + 1000]),
                )
        report = {
            "status": "COMPLETE",
            "cohort_id": str(args.cohort_id),
            "symbols": ("SPY",),
            "intervals": ("1m", "5m"),
            "requested_from": args.requested_from.isoformat(),
            "requested_to": args.requested_to.isoformat(),
            "provider_requests": sum(
                row["provider_request_performed"]
                for row in requests_used
            ),
            "response_bytes": sum(
                row["response_bytes"] for row in requests_used
            ),
            "bars_persisted": len(facts),
            "requests": requests_used,
            "quality_code": "EXPLICIT_ADVANCED_REST_REPAIR_V1",
            "five_minute_source": "DERIVED_FROM_EXPLICIT_REPAIR_1M",
            "publication_advanced": False,
            "credentials_exposed": False,
        }
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2))
        return 0
    finally:
        session.close()
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
