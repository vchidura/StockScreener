#!/usr/bin/env python3
"""Plan or execute exact QQQ/SPY V3 pre-session snapshot admission."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.advanced_options_p8_production import (  # noqa: E402
    _PostgresSelectionSpots,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsSelectionSource,
    P8_SELECTION_MAXIMUM_DECODED_BYTES,
    P8_SELECTION_REQUEST_COUNT,
)
from realtime.contracts import UniverseManifest, canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_recent_repair import (  # noqa: E402
    AuthorizedNativeRepairTransport,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_adapters import _option_fact  # noqa: E402
from realtime.reference_facts import (  # noqa: E402
    ReferenceFact,
    ReferenceFactRelation,
)
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.reference_seed import ReferenceKind  # noqa: E402
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from scripts.realtime_script_support import (  # noqa: E402
    PacedPolygonTransport,
    write_report,
)


UTC = timezone.utc
UNDERLYINGS = ("QQQ", "SPY")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--source-cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--reference-valid-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--maximum-decoded-bytes", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--capture-path", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("V3 pre-session admission targets stocks_realtime_v2")
    if (
        args.authorized_until.tzinfo is None
        or args.reference_valid_until.tzinfo is None
        or args.maximum_provider_requests != P8_SELECTION_REQUEST_COUNT
        or args.maximum_decoded_bytes
        != P8_SELECTION_MAXIMUM_DECODED_BYTES
        or not args.authorization_reference.strip()
    ):
        parser.error("V3 pre-session scope or budget is invalid")
    args.authorized_until = args.authorized_until.astimezone(UTC)
    args.reference_valid_until = args.reference_valid_until.astimezone(UTC)
    args.capture_path = args.capture_path.resolve()
    args.report_path = args.report_path.resolve()
    if args.capture_path.exists() or args.report_path.exists():
        parser.error("capture and report paths must be new")
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
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    return provider, pool


def _facts(selected_rows, selected_snapshots, request, settlement_session):
    facts = []
    selected = []
    for row in selected_rows:
        raw = dict(selected_snapshots[row.contract_ticker])
        details = dict(raw["details"])
        details["underlying_ticker"] = row.underlyer
        catalog = _option_fact(row.underlyer, details, request)
        contract_id = dict(catalog.values)["contract_id"]
        open_interest = raw.get("open_interest")
        if not isinstance(open_interest, int) or open_interest < 0:
            raise ValueError("selected option open interest is invalid")
        digest = canonical_sha256(raw)
        oi = ReferenceFact.build(
            ReferenceFactRelation.OI_BASELINE,
            ReferenceKind.OI_COVERAGE.value,
            row.underlyer,
            baseline_id=uuid5(
                NAMESPACE_URL,
                "advanced-options-v3-oi:"
                f"{row.contract_ticker}:{settlement_session}:"
                f"{request.created_at.isoformat()}:{digest}",
            ),
            contract_id=contract_id,
            settlement_session=settlement_session,
            open_interest=open_interest,
            source="massive",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
            observed_session=request.session_date,
            observed_at=request.created_at,
            received_at=request.created_at,
            payload_sha256=digest,
            supersedes_baseline_id=None,
        )
        facts.extend((catalog, oi))
        selected.append({
            "underlyer": row.underlyer,
            "contract_ticker": row.contract_ticker,
            "expiration_date": row.expiration_date.isoformat(),
            "option_type": row.option_type,
            "open_interest": open_interest,
        })
    if (
        len(selected) != 4
        or {row["underlyer"] for row in selected} != set(UNDERLYINGS)
        or any(
            row["expiration_date"] != request.session_date.isoformat()
            for row in selected
        )
    ):
        raise ValueError("V3 selected contract membership changed")
    return tuple(facts), tuple(selected)


def _select(candidates):
    selected = []
    for underlyer in UNDERLYINGS:
        for option_type in ("C", "P"):
            ranked = sorted(
                (
                    row
                    for row in candidates
                    if row.underlyer == underlyer
                    and row.option_type == option_type
                ),
                key=lambda row: (
                    row.moneyness_fraction,
                    -row.open_interest,
                    -row.day_volume,
                    row.relative_spread,
                    row.contract_ticker,
                ),
            )
            if not ranked:
                raise ValueError(
                    f"{underlyer} {option_type} pre-session scope is empty"
                )
            selected.append(ranked[0])
    return tuple(selected)


def main(argv=None) -> int:
    args = parse_args(argv)
    checked_at = datetime.now(UTC)
    if (
        checked_at.date() != args.session_date
        or not checked_at <= args.authorized_until <= checked_at + timedelta(hours=1)
        or args.reference_valid_until <= args.authorized_until
        or args.reference_valid_until.date() != args.session_date
    ):
        raise ValueError("V3 pre-session clocks are invalid")
    plan = {
        "contract": "advanced-options-v3-pre-session-snapshot-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "source_cohort_id": str(args.source_cohort_id),
        "session_date": args.session_date.isoformat(),
        "underlyings": UNDERLYINGS,
        "channels": ("T", "Q"),
        "selected_contracts_per_underlying": 2,
        "maximum_provider_requests": args.maximum_provider_requests,
        "maximum_decoded_bytes": args.maximum_decoded_bytes,
        "authorization_reference": args.authorization_reference,
    }
    if not args.execute:
        report = {
            **plan,
            "status": "PLANNED",
            "provider_requests": 0,
            "database_writes": 0,
            "semantic_sha256": canonical_sha256(plan),
        }
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("V3 pre-session admission requires POLYGON_API_KEY")
    request = ReferenceSeedRequest(
        UniverseManifest(
            "advanced-options-v3-pre-session-v1",
            UNDERLYINGS,
            UNDERLYINGS,
        ),
        args.session_date,
        checked_at,
        checked_at,
        args.reference_valid_until,
    )
    settlement = OptionExchangeCalendar().previous_session_date(
        args.session_date
    )
    provider, pool = _provider()
    try:
        transport = PacedPolygonTransport(
            AuthorizedNativeRepairTransport(
                RequestsPolygonHttpTransport(
                    api_key,
                    timeout_seconds=30,
                    maximum_response_bytes=args.maximum_decoded_bytes,
                ),
                args.authorized_until,
                lambda: datetime.now(UTC),
            ),
            minimum_interval_seconds=0.5,
        )
        spots = _PostgresSelectionSpots(
            provider,
            args.source_cohort_id,
        )
        source = MassiveAdvancedOptionsSelectionSource(
            transport,
            spots,
            clock=lambda: datetime.now(UTC),
        )
        spot_values = spots.load_spots(
            underlyings=UNDERLYINGS,
            available_before=checked_at,
        )
        candidates = []
        decoded_bytes = 0
        snapshot_times = []
        responses = []
        snapshots = {}
        for underlyer in UNDERLYINGS:
            (
                rows,
                response_bytes,
                snapshot_at,
                payload,
                raw_snapshots,
            ) = source._request(
                underlyer,
                spot=spot_values[underlyer],
                session_date=args.session_date,
            )
            candidates.extend(rows)
            decoded_bytes += response_bytes
            snapshot_times.append(snapshot_at)
            snapshots.update(raw_snapshots)
            responses.append({
                "underlyer": underlyer,
                "request_id": payload.get("request_id"),
                "response_sha256": canonical_sha256(payload),
                "row_count": len(payload["results"]),
            })
        selected_rows = _select(tuple(candidates))
        selected_snapshots = {
            row.contract_ticker: snapshots[row.contract_ticker]
            for row in selected_rows
        }
        if (
            len(responses) != args.maximum_provider_requests
            or decoded_bytes > args.maximum_decoded_bytes
            or any(value > checked_at + timedelta(seconds=5)
                   for value in snapshot_times)
        ):
            raise RuntimeError("V3 pre-session provider budget changed")
        selection_sha256 = canonical_sha256({
            "schema": "advanced-options-v3-pre-session-selection-v1",
            "reviewed_at": checked_at,
            "responses": responses,
            "selected": tuple(
                row.contract_ticker for row in selected_rows
            ),
        })
        capture_document = {
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "captured_at_utc": datetime.now(UTC).isoformat(),
            "session_date": args.session_date.isoformat(),
            "source_cohort_id": str(args.source_cohort_id),
            "provider_requests": len(responses),
            "decoded_bytes": decoded_bytes,
            "responses": responses,
            "selection_sha256": selection_sha256,
            "selected_snapshots": selected_snapshots,
        }
        capture_document["semantic_sha256"] = canonical_sha256(
            capture_document
        )
        write_report(args.capture_path, capture_document)
        facts, selected = _facts(
            selected_rows,
            selected_snapshots,
            request,
            settlement,
        )
        with provider.transaction(APP_ROLE) as unit:
            persist_reference_facts_cursor(unit.cursor, facts)
    finally:
        pool.closeall()
    report = {
        **plan,
        "status": "COMPLETE",
        "provider_requests": len(responses),
        "provider_response_bytes": decoded_bytes,
        "source_snapshot_at_utc": min(snapshot_times).isoformat(),
        "selection_sha256": selection_sha256,
        "selected": selected,
        "settlement_session": settlement.isoformat(),
        "database_writes": len(facts),
        "fact_sha256": canonical_sha256(facts),
        "credentials_exposed": False,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
