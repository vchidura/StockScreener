#!/usr/bin/env python3
"""Backfill settlement marks for expired contracts into the daily fact table.

Purpose is an implied-volatility history. Open interest cannot be bought retroactively,
but option closes can: the entitlement serves daily aggregates for expired contracts
roughly four years back. Running those closes through this platform's own Black-Scholes
solver is what makes `option_iv_context_snapshots` fillable, what lets the realized-
volatility forecast be tested against implied volatility, and what gives the six
non-open-interest strategies a mark at a past decision time.

Two properties of the data that the writer preserves rather than hides:

The aggregates are trade-based, so a session with no print produces no bar. Per-contract
coverage is never complete and the gaps are informative, not errors.

A daily close is a settlement-style mark, which is a different valuation policy from the
intraday mark the live pipeline computes. It is written under its own `mark_source` and
must never be mixed with live snapshots when solving.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.data.polygon_developer import PolygonDeveloperEngine  # noqa: E402
from options.errors import OptionProviderError  # noqa: E402
from options.repositories.daily_facts import (  # noqa: E402
    DailyMarkRecord,
    OptionDailyFactRepository,
)
from options.repositories.ingestion import OptionIngestionRepository  # noqa: E402
from options.repositories.catalog import HISTORICAL_BACKFILL_REASON  # noqa: E402

DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_mark_backfill.json"

SQL_CATALOGUED_CONTRACTS = """
    SELECT c.contract_id, c.contract_ticker, c.underlying,
           v.expiration_date, v.strike, v.contract_type
    FROM option_contract_catalog c
    JOIN LATERAL (
        SELECT expiration_date, strike, contract_type, exclusion_reasons
        FROM option_contract_catalog_versions
        WHERE contract_id = c.contract_id AND contract_type IS NOT NULL
        ORDER BY valid_from DESC
        LIMIT 1
    ) v ON TRUE
    WHERE c.underlying = ANY(%s)
    ORDER BY c.underlying, v.expiration_date, v.strike
"""

SQL_UNDERLYING_CLOSE = """
    SELECT close_price
    FROM equity_bar_revisions
    WHERE interval = '1d' AND is_final AND session_scope = 'RTH' AND adjusted = false
      AND ticker = %s AND session_date <= %s
    ORDER BY session_date DESC
    LIMIT 1
"""

SQL_COMPLETED_POLICY_CONTRACTS = """
        SELECT DISTINCT contract_id
        FROM option_daily_contract_mark_revisions
        WHERE valuation_policy_sha256 = %s
            AND contract_id = ANY(%s)
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default=None, help="Comma-separated subset.")
    parser.add_argument(
        "--moneyness-band",
        type=float,
        default=0.06,
        help="Keep strikes within this fraction of the underlying close near expiry.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1400,
        help="Oldest expiration to attempt; the entitlement stops near four years.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=60,
        help="Sessions before expiry to request per contract.",
    )
    parser.add_argument("--maximum-contracts", type=int, default=None)
    parser.add_argument(
        "--refetch-existing",
        action="store_true",
        help="Fetch contracts that already have marks under the active settlement policy.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the marks. Without this the script only reports what it would fetch.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _underlying_close(ticker: str, on_or_before: date) -> float | None:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_UNDERLYING_CLOSE, (ticker, on_or_before))
        row = cursor.fetchone()
    return float(row["close_price"]) if row else None


def _within_backfill_universe(
    contract: dict[str, Any],
    spot: float,
    moneyness_band: float,
) -> bool:
    if HISTORICAL_BACKFILL_REASON in (contract.get("exclusion_reasons") or ()):
        return True
    return abs(float(contract["strike"]) / spot - 1.0) <= moneyness_band


def _select_contracts(args: argparse.Namespace, underlyers: tuple[str, ...]) -> list[dict[str, Any]]:
    today = date.today()
    oldest = today - timedelta(days=args.lookback_days)
    with get_db_cursor() as cursor:
        cursor.execute(SQL_CATALOGUED_CONTRACTS, (list(underlyers),))
        rows = [dict(row) for row in cursor.fetchall()]

    # One reference close per (underlying, expiration) keeps the moneyness filter cheap
    # and anchors it to the price that was current when the contract mattered.
    anchors: dict[tuple[str, date], float | None] = {}
    selected: list[dict[str, Any]] = []
    for row in rows:
        expiration = row["expiration_date"]
        if expiration >= today or expiration < oldest:
            continue
        key = (row["underlying"], expiration)
        if key not in anchors:
            anchors[key] = _underlying_close(row["underlying"], expiration)
        spot = anchors[key]
        if spot is None or spot <= 0:
            continue
        moneyness = abs(float(row["strike"]) / spot - 1.0)
        if not _within_backfill_universe(row, spot, args.moneyness_band):
            continue
        row["moneyness"] = moneyness
        selected.append(row)

    selected.sort(key=lambda item: (item["underlying"], item["expiration_date"], item["moneyness"]))
    return selected


def _completed_policy_contract_ids(
    contracts: list[dict[str, Any]],
    valuation_policy_sha256: str,
) -> frozenset[int]:
    if not contracts:
        return frozenset()
    with get_db_cursor() as cursor:
        cursor.execute(
            SQL_COMPLETED_POLICY_CONTRACTS,
            (
                valuation_policy_sha256,
                [contract["contract_id"] for contract in contracts],
            ),
        )
        return frozenset(int(row["contract_id"]) for row in cursor.fetchall())


def _select_pending_contracts(
    eligible_contracts: list[dict[str, Any]],
    completed_contract_ids: frozenset[int],
    maximum_contracts: int | None,
) -> list[dict[str, Any]]:
    if maximum_contracts is not None and maximum_contracts <= 0:
        raise ValueError("maximum contracts must be positive")
    pending = [
        contract for contract in eligible_contracts
        if contract["contract_id"] not in completed_contract_ids
    ]
    return pending[:maximum_contracts] if maximum_contracts else pending


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    if args.underlyers:
        underlyers = tuple(v.strip().upper() for v in args.underlyers.split(",") if v.strip())
    else:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT DISTINCT underlying FROM option_contract_catalog ORDER BY 1")
            underlyers = tuple(row["underlying"] for row in cursor.fetchall())

    eligible_contracts = _select_contracts(args, underlyers)
    completed_contract_ids = (
        frozenset()
        if args.refetch_existing
        else _completed_policy_contract_ids(
            eligible_contracts,
            configuration.settlement_valuation_policy_sha256,
        )
    )
    contracts = _select_pending_contracts(
        eligible_contracts,
        completed_contract_ids,
        args.maximum_contracts,
    )
    per_underlying: dict[str, int] = defaultdict(int)
    for contract in contracts:
        per_underlying[contract["underlying"]] += 1

    print(f"{len(eligible_contracts)} expired near-the-money contracts eligible")
    print(f"{len(completed_contract_ids)} policy-complete contracts skipped")
    print(f"{len(contracts)} contracts selected for this run")
    for underlying, count in sorted(per_underlying.items()):
        print(f"  {underlying:<6} {count}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to fetch and write.")
        return 0

    engine = PolygonDeveloperEngine(configuration, OptionIngestionRepository())
    repository = OptionDailyFactRepository()

    written = 0
    fetched_contracts = 0
    empty_contracts = 0
    entitlement_blocked = 0
    failures: dict[str, int] = defaultdict(int)
    sessions_by_underlying: dict[str, set[date]] = defaultdict(set)

    for index, contract in enumerate(contracts, start=1):
        expiration = contract["expiration_date"]
        start = expiration - timedelta(days=args.window_days)
        try:
            bars = engine.get_option_daily_aggregates(
                contract["contract_ticker"], start, expiration,
                adjusted=(
                    configuration.settlement_valuation_policy
                    .option_aggregates_adjusted
                ),
            )
        except OptionProviderError as error:
            message = str(error)
            if "plan" in message.lower() or "upgrade" in message.lower():
                entitlement_blocked += 1
            else:
                failures[type(error).__name__] += 1
            continue
        except Exception as error:  # noqa: BLE001
            failures[type(error).__name__] += 1
            continue

        fetched_contracts += 1
        if not bars:
            empty_contracts += 1
            continue

        records = [
            DailyMarkRecord(
                contract_id=contract["contract_id"],
                settlement_session=bar.session_date,
                underlying=contract["underlying"],
                close=bar.close,
                observed_at=datetime.now(timezone.utc),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                volume=bar.volume,
                transaction_count=bar.transaction_count,
                mark_source=(
                    configuration.settlement_valuation_policy.mark_source
                ),
                mark_adjusted=(
                    configuration.settlement_valuation_policy
                    .option_aggregates_adjusted
                ),
                valuation_policy_version=(
                    configuration.settlement_valuation_policy.policy_version
                ),
                valuation_policy_sha256=(
                    configuration.settlement_valuation_policy_sha256
                ),
            )
            for bar in bars
        ]
        written += repository.persist_marks(records)
        sessions_by_underlying[contract["underlying"]].update(
            bar.session_date for bar in bars
        )
        if index % 50 == 0:
            print(f"  {index}/{len(contracts)} contracts, {written} marks written")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "settlement_valuation_policy_version": (
            configuration.settlement_valuation_policy.policy_version
        ),
        "settlement_valuation_policy_sha256": (
            configuration.settlement_valuation_policy_sha256
        ),
        "option_aggregates_adjusted": (
            configuration.settlement_valuation_policy.option_aggregates_adjusted
        ),
        "moneyness_band": args.moneyness_band,
        "lookback_days": args.lookback_days,
        "window_days": args.window_days,
        "contracts_eligible": len(eligible_contracts),
        "contracts_skipped_existing": len(completed_contract_ids),
        "contracts_selected": len(contracts),
        "contracts_fetched": fetched_contracts,
        "contracts_with_no_bars": empty_contracts,
        "contracts_blocked_by_entitlement": entitlement_blocked,
        "failures": dict(failures),
        "marks_written": written,
        "distinct_sessions_by_underlying": {
            underlying: len(sessions) for underlying, sessions in sorted(sessions_by_underlying.items())
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print(f"\nwrote {written} marks from {fetched_contracts} contracts")
    print(f"  no bars returned: {empty_contracts}")
    print(f"  blocked by entitlement: {entitlement_blocked}")
    if failures:
        print(f"  failures: {dict(failures)}")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
