#!/usr/bin/env python3
"""Read-only retained-state audit for exact Advanced Options P7 inputs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import dotenv_values
from psycopg2.extensions import make_dsn


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402


UTC = timezone.utc
UNDERLYERS = ("SPY", "QQQ")


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include an explicit UTC offset"
        )
    return parsed.astimezone(UTC)


def _json(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    return value


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--start-utc", type=_timestamp, required=True)
    value.add_argument("--stop-utc", type=_timestamp, required=True)
    value.add_argument("--authorization-reference", required=True)
    value.add_argument("--owner-non-overlap-confirmation", required=True)
    value.add_argument("--output", type=Path, required=True)
    value.add_argument("--allow-admin-read-only", action="store_true")
    return value


def _dsn(*, allow_admin_read_only: bool) -> tuple[str, str]:
    values = dotenv_values(BACKEND_DIR / ".env")
    api_dsn = str(
        os.getenv("REALTIME_V2_API_DSN")
        or values.get("REALTIME_V2_API_DSN")
        or ""
    ).strip()
    if not allow_admin_read_only:
        if not api_dsn:
            raise RuntimeError(
                "retained P7 input audit requires REALTIME_V2_API_DSN"
            )
        return api_dsn, "API_READ_ONLY"
    admin_dsn = str(
        os.getenv("REALTIME_V2_ADMIN_DSN")
        or values.get("REALTIME_V2_ADMIN_DSN")
        or ""
    ).strip()
    if admin_dsn:
        return admin_dsn, "ADMIN_READ_ONLY"
    settings = {
        name: str(os.getenv(name) or values.get(name) or "").strip()
        for name in (
            "DB_HOST",
            "DB_PORT",
            "POSTGRES_ADMIN_USER",
            "POSTGRES_ADMIN_PASSWORD",
        )
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "retained P7 admin read-only audit requires: "
            + ", ".join(missing)
        )
    return make_dsn(
        dbname=EXPECTED_V2_DATABASE_NAME,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
    ), "ADMIN_READ_ONLY"


def _contract_candidates(cursor, start: datetime) -> list[dict[str, object]]:
    cursor.execute(
        """
        WITH latest_underlyer AS (
            SELECT DISTINCT ON (revision.security_id)
                revision.security_id,
                revision.ticker
            FROM ref.security_revisions AS revision
            WHERE revision.effective_from <= %s
            ORDER BY revision.security_id,
                     revision.effective_from DESC,
                     revision.observed_at DESC
        ),
        slot_membership AS (
            SELECT
                member.cohort_id,
                member.version AS slot_version,
                member.instrument_id,
                member.catalog_version_id,
                member.purpose,
                member.priority,
                array_agg(DISTINCT member.channel ORDER BY member.channel)
                    AS channels,
                bool_or(current.version = member.version) AS active_slot
            FROM rt.slot_set_members AS member
            LEFT JOIN rt.slot_set_current AS current
              ON current.cohort_id = member.cohort_id
            GROUP BY member.cohort_id,
                     member.version,
                     member.instrument_id,
                     member.catalog_version_id,
                     member.purpose,
                     member.priority
        )
        SELECT
            slot.cohort_id::text,
            slot.slot_version,
            slot.active_slot,
            slot.purpose,
            slot.priority,
            slot.channels,
            contract.contract_ticker,
            underlyer.ticker AS underlyer,
            version.version_id,
            version.expiration,
            version.call_put,
            version.strike,
            version.eligibility,
            version.effective_from,
            version.effective_to,
            quote.event_at AS quote_event_at,
            quote.journal_generation::text AS quote_generation,
            quote.source_offset AS quote_offset,
            trade.event_at AS trade_event_at,
            trade.journal_generation::text AS trade_generation,
            trade.source_offset AS trade_offset
        FROM slot_membership AS slot
        JOIN rt.instruments AS instrument
          ON instrument.instrument_id = slot.instrument_id
         AND instrument.asset_class = 'OPTION'
        JOIN ref.option_contracts AS contract
          ON contract.contract_id = instrument.contract_id
        JOIN ref.option_contract_versions AS version
          ON version.version_id = slot.catalog_version_id
         AND version.contract_id = contract.contract_id
        JOIN latest_underlyer AS underlyer
          ON underlyer.security_id = contract.underlying_security_id
        LEFT JOIN rt.quote_current AS quote
          ON quote.provider = 'massive'
         AND quote.instrument_id = slot.instrument_id
        LEFT JOIN rt.trade_current AS trade
          ON trade.provider = 'massive'
         AND trade.instrument_id = slot.instrument_id
        WHERE underlyer.ticker = ANY(%s)
          AND version.eligibility = 'VALIDATED_ACTIVE'
          AND version.effective_from <= %s
          AND (version.effective_to IS NULL OR version.effective_to > %s)
          AND version.expiration >= %s
        ORDER BY underlyer.ticker,
                 slot.active_slot DESC,
                 CASE slot.purpose WHEN 'PRIMARY' THEN 0 ELSE 1 END,
                 (quote.event_at IS NOT NULL AND trade.event_at IS NOT NULL) DESC,
                 LEAST(quote.event_at, trade.event_at) DESC NULLS LAST,
                 version.expiration,
                 slot.priority DESC,
                 contract.contract_ticker
        """,
        (
            start,
            list(UNDERLYERS),
            start,
            start,
            start.date(),
        ),
    )
    columns = tuple(row.name for row in cursor.description)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def _window_conflicts(
    cursor,
    start: datetime,
    stop: datetime,
) -> list[dict[str, object]]:
    cursor.execute(
        """
        SELECT
            plan.plan_id::text,
            plan.cohort_id::text,
            plan.stage_catalog_version,
            plan.enabled_stages,
            run.window_id::text,
            run.window_key,
            run.state,
            run.release_at,
            run.cutoff,
            run.deadline
        FROM rt.service_plans AS plan
        JOIN rt.window_runs AS run USING(plan_id)
        WHERE run.release_at < %s
          AND run.deadline > %s
        ORDER BY run.release_at,run.window_key
        """,
        (stop, start),
    )
    columns = tuple(row.name for row in cursor.description)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def _choose(
    candidates: list[dict[str, object]],
    start: datetime,
) -> dict[str, dict[str, object]]:
    selected = {}
    for underlyer in UNDERLYERS:
        rows = [
            row for row in candidates
            if row["underlyer"] == underlyer
            and set(row["channels"]) == {"T", "Q"}
        ]
        preferred = [
            row for row in rows
            if 7 <= (row["expiration"] - start.date()).days <= 30
        ]
        choice = (preferred or rows)
        if choice:
            selected[underlyer] = choice[0]
    return selected


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.stop_utc - args.start_utc != timedelta(minutes=10):
        raise ValueError("P7 retained-state audit window must be ten minutes")
    if args.start_utc.date() != args.stop_utc.date():
        raise ValueError("P7 retained-state audit must remain in one UTC date")
    if not args.authorization_reference.strip():
        raise ValueError("authorization reference cannot be blank")
    if not args.owner_non_overlap_confirmation.strip():
        raise ValueError("owner non-overlap confirmation cannot be blank")
    output = args.output.resolve()
    if not output.parent.is_dir():
        raise FileNotFoundError("P7 retained-state audit output parent is absent")
    dsn, database_access = _dsn(
        allow_admin_read_only=args.allow_admin_read_only
    )
    with psycopg2.connect(dsn) as connection:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute("SELECT current_database()")
            database = str(cursor.fetchone()[0])
            if database != EXPECTED_V2_DATABASE_NAME:
                raise RuntimeError(
                    "P7 retained-state audit targets the wrong database"
                )
            candidates = _contract_candidates(cursor, args.start_utc)
            conflicts = _window_conflicts(
                cursor,
                args.start_utc,
                args.stop_utc,
            )
            selected = _choose(candidates, args.start_utc)
        connection.rollback()
    status = (
        "READY"
        if set(selected) == set(UNDERLYERS) and not conflicts
        else "BLOCKED"
    )
    report = {
        "version": "advanced-options-p7-input-audit-v1",
        "status": status,
        "database": EXPECTED_V2_DATABASE_NAME,
        "database_access": database_access,
        "window": {
            "start_utc": args.start_utc,
            "stop_utc": args.stop_utc,
        },
        "authorization_reference": args.authorization_reference.strip(),
        "owner_non_overlap_confirmation": (
            args.owner_non_overlap_confirmation.strip()
        ),
        "provider_requests": 0,
        "database_writes": 0,
        "selected": selected,
        "candidate_count": len(candidates),
        "candidates": candidates[:20],
        "recorded_window_conflicts": conflicts,
        "blockers": [
            *(
                [
                    "EXACT_RETAINED_SPY_QQQ_TQ_CONTRACTS_UNAVAILABLE"
                ]
                if set(selected) != set(UNDERLYERS)
                else []
            ),
            *(["RECORDED_WINDOW_OVERLAP"] if conflicts else []),
        ],
    }
    body = (
        json.dumps(report, indent=2, sort_keys=True, default=_json) + "\n"
    ).encode("utf-8")
    with output.open("xb") as handle:
        handle.write(body)
    summary = {
        "status": status,
        "output": str(output),
        "sha256": hashlib.sha256(body).hexdigest(),
        "selected": {
            key: value["contract_ticker"]
            for key, value in selected.items()
        },
        "candidate_count": len(candidates),
        "recorded_window_conflicts": len(conflicts),
        "provider_requests": 0,
        "database_writes": 0,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if status == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
