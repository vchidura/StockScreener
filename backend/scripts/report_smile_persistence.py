#!/usr/bin/env python3
"""Measure how long volatility-smile distortions survive across consecutive matrices.

Read-only. Recomputes smile residuals from persisted snapshots using the same pure
functions the live detector uses, then follows each qualifying distortion forward to
the next matrices for the same underlying.

The decision this informs: whether a smile kink lasts long enough to be tradeable at
the configured slot cadence. A survival rate at lag 1 that is close to the unconditional
base rate means the detector is finding noise, not a persistent dislocation.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from options.analytics.smile import (  # noqa: E402
    SmileInput,
    fit_smile_groups,
    qualifying_distortions,
)
from options.config import load_option_runtime_configuration  # noqa: E402
from options.domain import ContractType  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default=None, help="Comma-separated subset.")
    parser.add_argument("--max-lag", type=int, default=4)
    parser.add_argument(
        "--max-gap-minutes",
        type=int,
        default=30,
        help="Two matrices are consecutive only within this gap.",
    )
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def _connect():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        options="-c timezone=UTC",
    )


def _load_matrices(cursor, underlyers: tuple[str, ...] | None) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT matrix_id, batch_id, underlying, market_time
        FROM option_analysis_runs
        WHERE status = 'COMPLETE'
          AND (%s::text[] IS NULL OR underlying = ANY(%s::text[]))
        ORDER BY underlying, market_time
        """,
        (list(underlyers) if underlyers else None, list(underlyers) if underlyers else None),
    )
    return [dict(row) for row in cursor.fetchall()]


def _load_residuals(cursor, batch_id, minimum_strikes: int):
    """Recompute residuals for every model-valid contract in one matrix."""
    cursor.execute(
        """
        SELECT DISTINCT ON (contract_id)
               contract_id, contract_type, expiration_date, strike, spot, local_iv
        FROM option_chain_snapshots
        WHERE batch_id = %s
          AND model_mark IS NOT NULL
          AND iv_converged
          AND local_iv IS NOT NULL
        ORDER BY contract_id, revision DESC
        """,
        (batch_id,),
    )
    rows = tuple(
        SmileInput(
            contract_id=row["contract_id"],
            contract_type=ContractType(row["contract_type"]),
            expiration_date=row["expiration_date"],
            strike=row["strike"],
            spot=row["spot"],
            local_iv=float(row["local_iv"]),
        )
        for row in cursor.fetchall()
    )
    return fit_smile_groups(rows, minimum_strikes=minimum_strikes)


def _describe(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "median": round(statistics.median(ordered), 4),
        "p25": round(ordered[len(ordered) // 4], 4),
        "p75": round(ordered[(3 * len(ordered)) // 4], 4),
        "min": round(ordered[0], 4),
        "max": round(ordered[-1], 4),
    }


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    smile_policy = configuration.strategy_policy.smile
    threshold = smile_policy.minimum_absolute_robust_z
    underlyers = (
        tuple(item.strip().upper() for item in args.underlyers.split(",") if item.strip())
        if args.underlyers
        else None
    )
    max_gap = timedelta(minutes=args.max_gap_minutes)

    connection = _connect()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    matrices = _load_matrices(cursor, underlyers)
    by_underlyer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in matrices:
        by_underlyer[row["underlying"]].append(row)

    # contract_id -> robust_z, per matrix, plus qualifying sets.
    survival = {lag: {"followed": 0, "survived": 0} for lag in range(1, args.max_lag + 1)}
    decay: dict[int, list[float]] = {lag: [] for lag in range(1, args.max_lag + 1)}
    sign_flips = {lag: {"followed": 0, "flipped": 0} for lag in range(1, args.max_lag + 1)}
    base_rate = {"followed": 0, "became": 0}
    follow_through = {"kinks_at_lag_1_window": 0, "present_later": 0, "absent_later": 0}
    strike_count_buckets: dict[str, dict[str, int]] = defaultdict(
        lambda: {"qualifying": 0, "evaluated": 0}
    )
    z_by_strike_count: list[tuple[int, float]] = []
    per_underlyer: dict[str, dict[str, int]] = defaultdict(
        lambda: {"matrices": 0, "kinks": 0, "pairs": 0}
    )
    total_groups = 0
    total_contracts = 0

    for underlyer, runs in sorted(by_underlyer.items()):
        cache: dict[int, dict[int, float]] = {}
        qualifying: dict[int, set[int]] = {}
        for position, run in enumerate(runs):
            fits = _load_residuals(cursor, run["batch_id"], smile_policy.minimum_strikes)
            scores: dict[int, float] = {}
            flagged: set[int] = set()
            for fit in fits:
                total_groups += 1
                total_contracts += fit.input_count
                bucket = (
                    "07-09" if fit.distinct_strike_count < 10
                    else "10-19" if fit.distinct_strike_count < 20
                    else "20-39" if fit.distinct_strike_count < 40
                    else "40+"
                )
                for entry in fit.residuals:
                    scores[entry.contract_id] = entry.robust_z
                    if not entry.is_edge:
                        strike_count_buckets[bucket]["evaluated"] += 1
                        z_by_strike_count.append(
                            (fit.distinct_strike_count, abs(entry.robust_z))
                        )
                for entry in qualifying_distortions(
                    fit, minimum_absolute_robust_z=threshold
                ):
                    flagged.add(entry.contract_id)
                    strike_count_buckets[bucket]["qualifying"] += 1
            cache[position] = scores
            qualifying[position] = flagged
            per_underlyer[underlyer]["matrices"] += 1
            per_underlyer[underlyer]["kinks"] += len(flagged)

        for position, run in enumerate(runs):
            for lag in range(1, args.max_lag + 1):
                target = position + lag
                if target >= len(runs):
                    continue
                gap = runs[target]["market_time"] - run["market_time"]
                if gap > max_gap * lag:
                    continue
                if lag == 1:
                    per_underlyer[underlyer]["pairs"] += 1
                later_scores = cache[target]
                later_flagged = qualifying[target]
                for contract_id in qualifying[position]:
                    if lag == 1:
                        follow_through["kinks_at_lag_1_window"] += 1
                        if contract_id in later_scores:
                            follow_through["present_later"] += 1
                        else:
                            follow_through["absent_later"] += 1
                    if contract_id not in later_scores:
                        continue
                    survival[lag]["followed"] += 1
                    if contract_id in later_flagged:
                        survival[lag]["survived"] += 1
                    start = cache[position][contract_id]
                    end = later_scores[contract_id]
                    if abs(start) > 0:
                        decay[lag].append(abs(end) / abs(start))
                    sign_flips[lag]["followed"] += 1
                    if (start > 0) != (end > 0):
                        sign_flips[lag]["flipped"] += 1
                # Unconditional base rate: any non-flagged contract becoming flagged.
                if lag == 1:
                    for contract_id in cache[position]:
                        if contract_id in qualifying[position]:
                            continue
                        if contract_id not in later_scores:
                            continue
                        base_rate["followed"] += 1
                        if contract_id in later_flagged:
                            base_rate["became"] += 1

    connection.close()

    def rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "strategy_policy_version": configuration.strategy_policy.strategy_version,
        "minimum_absolute_robust_z": threshold,
        "minimum_strikes": smile_policy.minimum_strikes,
        "max_gap_minutes": args.max_gap_minutes,
        "coverage": {
            "underlyings": len(by_underlyer),
            "complete_matrices": len(matrices),
            "sessions": len({row["market_time"].date() for row in matrices}),
            "fitted_groups": total_groups,
            "fitted_contract_observations": total_contracts,
        },
        "survival": {
            f"lag_{lag}": {
                "followed": survival[lag]["followed"],
                "survived": survival[lag]["survived"],
                "survival_rate": rate(
                    survival[lag]["survived"], survival[lag]["followed"]
                ),
            }
            for lag in range(1, args.max_lag + 1)
        },
        "unconditional_base_rate_lag_1": {
            "followed": base_rate["followed"],
            "became_kink": base_rate["became"],
            "rate": rate(base_rate["became"], base_rate["followed"]),
        },
        "follow_through_coverage_lag_1": {
            "kinks_in_a_followable_window": follow_through["kinks_at_lag_1_window"],
            "present_in_next_fitted_set": follow_through["present_later"],
            "absent_from_next_fitted_set": follow_through["absent_later"],
            "coverage": rate(
                follow_through["present_later"],
                follow_through["kinks_at_lag_1_window"],
            ),
            "note": (
                "A contract disappears when its expiration/type group drops below "
                "minimum_strikes or its MAD collapses to zero in the later matrix, "
                "so the fitted universe itself is unstable between slots."
            ),
        },
        "abs_z_ratio_vs_entry": {
            f"lag_{lag}": _describe(decay[lag]) for lag in range(1, args.max_lag + 1)
        },
        "sign_flip": {
            f"lag_{lag}": {
                "followed": sign_flips[lag]["followed"],
                "flipped": sign_flips[lag]["flipped"],
                "rate": rate(sign_flips[lag]["flipped"], sign_flips[lag]["followed"]),
            }
            for lag in range(1, args.max_lag + 1)
        },
        "detection_rate_by_strike_count": {
            bucket: {
                "evaluated": counts["evaluated"],
                "qualifying": counts["qualifying"],
                "rate": rate(counts["qualifying"], counts["evaluated"]),
            }
            for bucket, counts in sorted(strike_count_buckets.items())
        },
        "per_underlyer": {
            name: dict(counts) for name, counts in sorted(per_underlyer.items())
        },
    }

    text = json.dumps(report, indent=2, default=str, allow_nan=False)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
