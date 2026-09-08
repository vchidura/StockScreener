#!/usr/bin/env python3
"""Report change in open interest between two settlement sessions.

Read-only, no writes. Open interest is the only unambiguous evidence that contracts were
opened rather than churned, because volume counts opens, closes and round trips
identically. This surfaces that evidence per contract and per underlying.

Deliberately not a strategy module. Open interest settles once daily and is constant
across every intraday cycle, so this runs on a different rhythm from everything else in
the engine, and it can only ever be forward-validated: no vendor sells historical open
interest at any tier, so there is no backtest that could justify trading on it yet.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.open_interest_change import (  # noqa: E402
    OpenInterestObservation,
    VolumeConfirmation,
    classify_open_interest_change,
    rank_open_interest_builds,
    summarize_underlying,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.repositories.daily_facts import OptionDailyFactRepository  # noqa: E402

DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_open_interest_change.json"

SQL_UNDERLYERS = """
    SELECT DISTINCT underlying FROM option_daily_contract_facts ORDER BY underlying
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default=None, help="Comma-separated subset.")
    parser.add_argument("--session", default=None, help="ISO settlement session.")
    parser.add_argument("--prior-session", default=None, help="ISO prior settlement session.")
    parser.add_argument("--minimum-change", type=int, default=100)
    parser.add_argument("--limit", type=int, default=10, help="Contracts listed per underlying.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repository = OptionDailyFactRepository()
    calendar = OptionExchangeCalendar()

    if args.underlyers:
        underlyers = tuple(v.strip().upper() for v in args.underlyers.split(",") if v.strip())
    else:
        with get_db_cursor() as cursor:
            cursor.execute(SQL_UNDERLYERS)
            underlyers = tuple(row["underlying"] for row in cursor.fetchall())

    if not underlyers:
        print("No open-interest facts recorded yet.")
        return 1

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "minimum_change": args.minimum_change,
        "underlyers": {},
    }

    for underlying in underlyers:
        if args.session and args.prior_session:
            session = date.fromisoformat(args.session)
            prior_session = date.fromisoformat(args.prior_session)
        else:
            pair = repository.latest_session_pair(underlying)
            if pair is None:
                print(f"{underlying}: fewer than two settlement sessions recorded")
                continue
            session, prior_session = pair

        rows = repository.open_interest_change_detail(underlying, session, prior_session)
        if not rows:
            print(f"{underlying}: no contracts present in both sessions")
            continue

        sessions_spanned = calendar.sessions_between(prior_session, session)
        changes = tuple(
            classify_open_interest_change(
                OpenInterestObservation(
                    contract_id=row["contract_id"],
                    underlying=row["underlying"],
                    contract_type=row["contract_type"],
                    strike=row["strike"],
                    expiration_date=row["expiration_date"],
                    settlement_session=row["settlement_session"],
                    prior_settlement_session=row["prior_settlement_session"],
                    open_interest=row["open_interest"],
                    prior_open_interest=row["prior_open_interest"],
                    session_volume=row["session_volume"],
                    session_volume_is_final=row["session_volume_is_final"],
                    sessions_spanned=sessions_spanned,
                    open_interest_revision_count=row[
                        "open_interest_revision_count"
                    ],
                    prior_open_interest_revision_count=row[
                        "prior_open_interest_revision_count"
                    ],
                )
            )
            for row in rows
        )
        summary = summarize_underlying(underlying, changes)
        ranked = rank_open_interest_builds(
            changes, minimum_absolute_change=args.minimum_change, limit=args.limit
        )
        confirmations = {
            member.value: sum(1 for c in changes if c.confirmation is member)
            for member in VolumeConfirmation
        }

        print(f"\n=== {underlying}  {prior_session} -> {session} ===")
        span_note = (
            "consecutive sessions"
            if sessions_spanned == 1
            else f"**{sessions_spanned} sessions spanned - not a one-session move**"
        )
        print(f"  {span_note}")
        print(
            f"  {summary.contract_count} contracts in both sessions"
            f"  |  opening {summary.opening_contract_count}"
            f"  unwinding {summary.unwinding_contract_count}"
        )
        print(
            f"  net call OI {summary.call_change:+,}"
            f"   net put OI {summary.put_change:+,}"
            f"   total {summary.prior_total_open_interest:,}"
            f" -> {summary.total_open_interest:,}"
        )
        unavailable = confirmations[VolumeConfirmation.VOLUME_UNAVAILABLE.value]
        if unavailable:
            print(
                f"  volume confirmation unavailable for {unavailable} contracts"
                " (no session volume recorded)"
            )
        provisional = sum(
            1 for c in changes if "PROVISIONAL_SESSION_VOLUME" in c.reasons
        )
        if provisional:
            print(
                f"  {provisional} contracts scored on a running intraday volume;"
                " their opening share is an upper bound"
            )
        for change in ranked:
            fraction = (
                f"{change.change_fraction:+.0%}" if change.change_fraction is not None else "n/a"
            )
            flag = " ".join(change.reasons)
            print(
                f"    {change.contract_type:<4} {float(change.strike):>9.2f}"
                f" {change.expiration_date}"
                f"  {change.prior_open_interest:>8,} -> {change.open_interest:>8,}"
                f"  {change.change:>+9,} ({fraction:>6})"
                f"  {change.flow.value}"
                + (f"  [{flag}]" if flag else "")
            )

        report["underlyers"][underlying] = {
            "settlement_session": session.isoformat(),
            "prior_settlement_session": prior_session.isoformat(),
            "sessions_spanned": sessions_spanned,
            "summary": {
                "contract_count": summary.contract_count,
                "call_change": summary.call_change,
                "put_change": summary.put_change,
                "opening_contract_count": summary.opening_contract_count,
                "unwinding_contract_count": summary.unwinding_contract_count,
                "total_open_interest": summary.total_open_interest,
                "prior_total_open_interest": summary.prior_total_open_interest,
            },
            "confirmation_counts": confirmations,
            "largest_changes": [
                {
                    "contract_id": change.contract_id,
                    "contract_type": change.contract_type,
                    "strike": float(change.strike),
                    "expiration_date": change.expiration_date.isoformat(),
                    "prior_open_interest": change.prior_open_interest,
                    "open_interest": change.open_interest,
                    "change": change.change,
                    "change_fraction": change.change_fraction,
                    "flow": change.flow.value,
                    "confirmation": change.confirmation.value,
                    "opening_share": change.opening_share,
                    "session_volume_is_final": change.session_volume_is_final,
                    "reasons": list(change.reasons),
                }
                for change in ranked
            ],
        }

    if not report["underlyers"]:
        print("\nNothing to report.")
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, default=str, allow_nan=False),
        encoding="utf-8",
    )
    print(f"\nWrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
