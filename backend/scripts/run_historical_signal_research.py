#!/usr/bin/env python3
"""Run a registered or plugin signal adapter over reconstructed research inputs."""
from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor
from research.historical_signal_replay import (
    BUILTIN_ADAPTERS,
    HistoricalSignalAdapter,
    evaluate_historical_signals,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--signal", default="gap-formation-v1")
    result.add_argument(
        "--adapter",
        help="Plugin object as module:attribute; overrides --signal",
    )
    result.add_argument("--list-signals", action="store_true")
    result.add_argument("--start", help="First signal session YYYY-MM-DD")
    result.add_argument("--end", help="Last signal session YYYY-MM-DD")
    result.add_argument("--policy-version", default="liquid_us_common_stocks_v2")
    result.add_argument(
        "--tickers",
        help="Optional comma-separated universe subset for a controlled replay.",
    )
    result.add_argument("--shard-count", type=int, default=1)
    result.add_argument("--shard-index", type=int, default=0)
    result.add_argument(
        "--adjusted", action="store_true",
        help="Detect on the split-adjusted lineage. Must match the outcome "
             "runner; detecting and evaluating on different lineages is incoherent.",
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--events-output", type=Path)
    return result


def load_adapter(args: argparse.Namespace) -> HistoricalSignalAdapter:
    if args.adapter:
        module_name, separator, attribute = args.adapter.partition(":")
        if not separator or not module_name or not attribute:
            raise ValueError("--adapter must use module:attribute")
        adapter = getattr(importlib.import_module(module_name), attribute)
        _validate_adapter(adapter)
        return adapter
    try:
        adapter = BUILTIN_ADAPTERS[args.signal]
    except KeyError as exc:
        raise ValueError(f"unknown signal adapter: {args.signal}") from exc
    _validate_adapter(adapter)
    return adapter


def _validate_adapter(adapter: Any) -> None:
    for attribute in (
        "source_name", "source_version", "minimum_bars",
        "excluded_action_types", "candidate_dates", "evaluate",
    ):
        if not hasattr(adapter, attribute):
            raise TypeError(f"historical signal adapter is missing {attribute}")
    if not callable(adapter.candidate_dates) or not callable(adapter.evaluate):
        raise TypeError("historical signal adapter methods must be callable")


def run(args: argparse.Namespace) -> dict[str, Any]:
    adapter = load_adapter(args)
    if args.shard_count <= 0:
        raise ValueError("--shard-count must be positive")
    if not 0 <= args.shard_index < args.shard_count:
        raise ValueError("--shard-index must be within --shard-count")
    requested_tickers = tuple(
        dict.fromkeys(
            value.strip().upper()
            for value in (args.tickers or "").split(",")
            if value.strip()
        )
    )
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    if start_date and end_date and start_date > end_date:
        raise ValueError("--start must be on or before --end")
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT run.universe_run_id, run.effective_from::date AS signal_date,
                   member.ticker
            FROM equity_universe_runs run
            JOIN equity_universe_members member
              ON member.universe_run_id = run.universe_run_id
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
              AND (%s IS NULL OR run.effective_from::date >= %s)
              AND (%s IS NULL OR run.effective_from::date <= %s)
                            AND (cardinality(%s::TEXT[]) = 0 OR member.ticker = ANY(%s::TEXT[]))
            ORDER BY signal_date, member.member_rank, member.ticker
            """,
                        (
                                args.policy_version, start_date, start_date, end_date, end_date,
                                list(requested_tickers), list(requested_tickers),
                        ),
        )
        membership_rows = cursor.fetchall()
    if args.shard_count > 1:
        all_tickers = sorted({row["ticker"] for row in membership_rows})
        shard_tickers = set(all_tickers[args.shard_index::args.shard_count])
        membership_rows = [
            row for row in membership_rows if row["ticker"] in shard_tickers
        ]
    if not membership_rows:
        raise ValueError("no reconstructed universe sessions matched the request")

    members_by_session: dict[date, set[str]] = {}
    universe_ids_by_session = {}
    for row in membership_rows:
        signal_date = row["signal_date"]
        members_by_session.setdefault(signal_date, set()).add(row["ticker"])
        universe_ids_by_session[signal_date] = row["universe_run_id"]
    first_session = min(members_by_session)
    last_session = max(members_by_session)
    union = sorted({ticker for values in members_by_session.values() for ticker in values})
    history_start = first_session - timedelta(days=max(adapter.minimum_bars * 3, 60))

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (ticker, session_date)
                   ticker, session_date, bar_end, open_price AS open,
                   high_price AS high, low_price AS low, close_price AS close,
                   volume, bar_revision_id
            FROM equity_bar_revisions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND interval = '1d'
                            AND quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
              AND adjusted = %s
              AND ticker = ANY(%s)
              AND session_date BETWEEN %s AND %s
              AND is_final = TRUE
            ORDER BY ticker, session_date, replay_available_at DESC, created_at DESC
            """,
            (args.adjusted, union, history_start, last_session),
        )
        bar_rows = cursor.fetchall()
        cursor.execute(
            """
            SELECT ticker, action_type, effective_date, source_key
            FROM equity_corporate_actions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND ticker = ANY(%s)
              AND effective_date BETWEEN %s AND %s
            ORDER BY effective_date, ticker, action_type, source_key
            """,
                        (union, history_start, last_session),
        )
        action_rows = cursor.fetchall()

    bar_frame = pd.DataFrame([dict(row) for row in bar_rows])
    frames_by_ticker = {}
    for ticker, group in bar_frame.groupby("ticker", sort=False):
        frame = group.sort_values("session_date").set_index("session_date")
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frames_by_ticker[ticker] = frame
    actions_by_key: dict[tuple[date, str], list[dict[str, Any]]] = {}
    for row in action_rows:
        actions_by_key.setdefault((row["effective_date"], row["ticker"]), []).append(
            dict(row)
        )
    replay = evaluate_historical_signals(
        adapter,
        frames_by_ticker,
        members_by_session={
            key: frozenset(value) for key, value in members_by_session.items()
        },
        universe_ids_by_session=universe_ids_by_session,
        universe_policy_version=args.policy_version,
        actions_by_session_ticker={
            key: tuple(value) for key, value in actions_by_key.items()
        },
    )
    event_rows = [row.as_dict() for row in replay.events]
    summarize = getattr(adapter, "summarize", None)
    adapter_summary = dict(summarize(replay.events)) if summarize else {}
    report = {
        "source_name": adapter.source_name,
        "source_version": adapter.source_version,
        "shard_count": args.shard_count,
        "shard_index": args.shard_index,
        "universe_policy_version": args.policy_version,
        "bar_lineage": "ADJUSTED" if args.adjusted else "UNADJUSTED",
        "first_session": first_session.isoformat(),
        "last_session": last_session.isoformat(),
        "sessions": len(members_by_session),
        "union_tickers": len(union),
        "candidate_count": replay.candidate_count,
        "event_count": len(replay.events),
        "exclusion_counts": dict(replay.exclusion_counts),
        "adapter_summary": adapter_summary,
        "events_output": str(args.events_output) if args.events_output else None,
    }
    if args.events_output:
        args.events_output.parent.mkdir(parents=True, exist_ok=True)
        args.events_output.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in event_rows),
            encoding="utf-8",
        )
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return report


def main() -> int:
    args = parser().parse_args()
    if args.list_signals:
        print(json.dumps(sorted(BUILTIN_ADAPTERS)))
        return 0
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())