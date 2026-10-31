"""Record and reconcile provider-free Advanced Realtime desired membership."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_desired_state import (  # noqa: E402
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    MemberPreparationStatus,
    default_advanced_realtime_desired_state,
)
from realtime.advanced_realtime_desired_state_repository import (  # noqa: E402
    apply_desired_state,
    reconcile_desired_state,
    update_desired_member_preparation,
)


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply = subparsers.add_parser("apply")
    apply.add_argument("--source-cohort-id", type=UUID, required=True)
    apply.add_argument("--source-slot-version", type=int, default=1)
    apply.add_argument("--observed-at", type=_datetime)
    apply.add_argument("--equity", action="append", default=[])
    apply.add_argument("--sector-etf", action="append", default=[])
    apply.add_argument("--option-underlying", action="append", default=[])
    apply.add_argument("--disabled", action="append", default=[])

    readiness = subparsers.add_parser("readiness")
    readiness.add_argument("--revision-id", type=UUID, required=True)
    readiness.add_argument("--ticker", required=True)
    readiness.add_argument(
        "--kind",
        type=DesiredMemberKind,
        choices=tuple(DesiredMemberKind),
        required=True,
    )
    readiness.add_argument(
        "--status",
        type=MemberPreparationStatus,
        choices=tuple(MemberPreparationStatus),
        required=True,
    )
    readiness.add_argument("--reason", action="append", default=[])
    readiness.add_argument("--checked-at", type=_datetime)

    reconcile = subparsers.add_parser("reconcile")
    reconcile.add_argument("--revision-id", type=UUID, required=True)
    reconcile.add_argument("--materialized-at", type=_datetime)
    return parser.parse_args(argv)


def _change(args) -> DesiredStateChange:
    if not args.equity and not args.sector_etf and not args.option_underlying:
        if args.disabled:
            raise ValueError("--disabled requires explicit desired members")
        return default_advanced_realtime_desired_state(
            args.source_cohort_id,
            source_slot_version=args.source_slot_version,
        )
    disabled = {ticker.strip().upper() for ticker in args.disabled}
    members = tuple(
        DesiredMemberIntent(
            ticker=ticker,
            kind=kind,
            enabled=ticker.strip().upper() not in disabled,
        )
        for values, kind in (
            (args.equity, DesiredMemberKind.EQUITY),
            (args.sector_etf, DesiredMemberKind.SECTOR_ETF),
            (args.option_underlying, DesiredMemberKind.OPTION_UNDERLYING),
        )
        for ticker in values
    )
    known = {member.ticker for member in members}
    unknown_disabled = disabled - known
    if unknown_disabled:
        raise ValueError(
            "disabled desired members are absent: "
            + ", ".join(sorted(unknown_disabled))
        )
    has_enabled_options = any(
        member.enabled and member.kind is DesiredMemberKind.OPTION_UNDERLYING
        for member in members
    )
    return DesiredStateChange(
        source_cohort_id=args.source_cohort_id,
        source_slot_version=(
            args.source_slot_version if has_enabled_options else None
        ),
        members=members,
    )


def _result_payload(result) -> dict[str, object]:
    snapshot = result.snapshot
    return {
        "state": result.state,
        "revision_id": str(result.revision.revision_id),
        "revision_sha256": result.revision.revision_sha256,
        "effective_session_date": (
            result.revision.effective_session_date.isoformat()
        ),
        "member_readiness": {
            row.ticker + ":" + row.kind.value: row.status.value
            for row in result.preparations
        },
        "reason_codes": list(result.reason_codes),
        "snapshot": (
            None
            if snapshot is None
            else {
                "manifest_id": str(snapshot.manifest_id),
                "cohort_id": str(snapshot.cohort_id),
                "slot_set_version": snapshot.slot_set_version,
                "snapshot_sha256": snapshot.snapshot_sha256,
            }
        ),
        "provider_effects": 0,
        "production_effects": 0,
        "publication_effects": 0,
    }


def execute(args, connection) -> dict[str, object]:
    if args.command == "apply":
        return _result_payload(
            apply_desired_state(
                connection,
                _change(args),
                observed_at=args.observed_at or _now_utc(),
            )
        )
    if args.command == "readiness":
        preparation = update_desired_member_preparation(
            connection,
            args.revision_id,
            ticker=args.ticker,
            kind=args.kind,
            status=args.status,
            checked_at=args.checked_at or _now_utc(),
            reasons=tuple(args.reason),
        )
        return {
            "revision_id": str(preparation.revision_id),
            "member": preparation.ticker + ":" + preparation.kind.value,
            "status": preparation.status.value,
            "provider_effects": 0,
            "production_effects": 0,
            "publication_effects": 0,
        }
    return _result_payload(
        reconcile_desired_state(
            connection,
            args.revision_id,
            materialized_at=args.materialized_at or _now_utc(),
        )
    )


def _dsn() -> str:
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    return dsn


def main(argv=None) -> int:
    args = parse_args(argv)
    with psycopg2.connect(dsn=_dsn(), options="-c timezone=UTC") as connection:
        result = execute(args, connection)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
