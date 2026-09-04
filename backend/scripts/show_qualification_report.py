#!/usr/bin/env python3
"""Print the published qualification lanes for a scanner.

Reads only `equity_qualification_revisions` joined to `equity_outcome_policies`,
which is the record retained after working rows are purged, so this keeps
working once a study's evidence and outcomes are discarded.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-name")
    parser.add_argument("--interval")
    parser.add_argument("--evaluation-version")
    parser.add_argument("--json", action="store_true")
    arguments = parser.parse_args()

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT source_name, source_version, interval, direction, horizon_key,
                   outcome_policy_key, qualification_state, sample_size,
                   independent_periods, mean_net_alpha, alpha_t_stat, alpha_fdr_q,
                   evaluation_version, effective_from, metrics
            FROM equity_qualification_revisions
            WHERE (%s IS NULL OR source_name = %s)
              AND (%s IS NULL OR interval = %s)
              AND (%s IS NULL OR evaluation_version = %s)
            ORDER BY source_name, interval, outcome_policy_key, direction, horizon_key
            """,
            (
                arguments.source_name, arguments.source_name,
                arguments.interval, arguments.interval,
                arguments.evaluation_version, arguments.evaluation_version,
            ),
        )
        rows = [dict(row) for row in cursor.fetchall()]

    if arguments.json:
        print(json.dumps(rows, indent=2, default=str))
        return 0

    if not rows:
        print("no qualification revisions matched")
        return 1

    print(f"{len(rows)} lane(s)\n")
    header = (
        f"{'mode':<7} {'dir':>4} {'horizon':>8} {'events':>8} {'periods':>8} "
        f"{'mean alpha':>11} {'t':>7} {'q':>8}  state"
    )
    print(header)
    print("-" * len(header))
    states: dict[str, int] = {}
    for row in rows:
        mode = "PLAN" if "RECOMMENDATION_PLAN" in row["outcome_policy_key"] else "SIGNED"
        state = row["qualification_state"]
        states[state] = states.get(state, 0) + 1

        def number(value, spec):
            return format(float(value), spec) if value is not None else "-"

        print(
            f"{mode:<7} {row['direction']:>+4} {row['horizon_key']:>8} "
            f"{row['sample_size']:>8,} {row['independent_periods']:>8,} "
            f"{number(row['mean_net_alpha'], '>11.5f')} "
            f"{number(row['alpha_t_stat'], '>7.2f')} "
            f"{number(row['alpha_fdr_q'], '>8.4f')}  {state}"
        )

    print("\nstates:", ", ".join(f"{k}={v}" for k, v in sorted(states.items())))

    metrics = rows[0]["metrics"] or {}
    if isinstance(metrics, str):
        metrics = json.loads(metrics)
    print("\ncohort (from the retained metrics):")
    for key in ("distinct_tickers", "top5_concentration", "first_signal_time",
                "last_signal_time", "qualification_metrics_version"):
        if key in metrics:
            print(f"  {key:<30} {metrics[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
