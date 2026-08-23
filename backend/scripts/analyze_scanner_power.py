"""Estimate whether the confidence study could have detected the alpha it observed.

Distinguishes "no edge" from "not enough data". Reports, per row, the effect size the
Benjamini-Hochberg threshold demanded and how many independent periods the observed
effect would need to clear it. Uses only already-computed study statistics.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import NormalDist

BACKEND_DIR = Path(__file__).resolve().parent.parent
DEFAULT_STUDY = BACKEND_DIR / "research" / "scanner_confidence_study.json"


def required_t(rank: int, family_size: int, q: float) -> float:
    """Two-sided critical t for the Benjamini-Hochberg threshold at this rank."""
    threshold = q * rank / family_size
    return NormalDist().inv_cdf(1.0 - threshold / 2.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--study", default=str(DEFAULT_STUDY))
    parser.add_argument("--q", type=float, default=0.05)
    parser.add_argument(
        "--small-family", type=int, default=10,
        help="Hypothetical pre-registered family size for comparison",
    )
    args = parser.parse_args()

    with open(args.study, encoding="utf-8") as handle:
        study = json.load(handle)

    report = [
        row for row in study["report"]
        if row.get("absolute_p_value") is not None and row.get("alpha_t_stat")
    ]
    family_size = len(report)
    ranked = sorted(report, key=lambda row: row["absolute_p_value"])

    print(f"study: {study['generated_at']}")
    print(f"testable rows: {family_size} (of {len(study['report'])})")
    print(f"target q: {args.q}\n")

    print("Rows closest to surviving correction:\n")
    header = (f"  {'scanner':28s} {'int':4s} {'dir':>3s} {'h':>3s} {'slice':26s} "
              f"{'periods':>7s} {'alpha':>8s} {'t':>6s} "
              f"{'need t':>7s} {'MDE':>8s} {'periods@MDE':>11s}")
    print(header)
    for rank, row in enumerate(ranked[:15], start=1):
        observed_t = float(row["alpha_t_stat"])
        if observed_t <= 0:
            continue
        alpha = float(row["mean_net_alpha"])
        periods = int(row["independent_periods"])
        standard_error = alpha / observed_t
        need_t = required_t(rank, family_size, args.q)
        mde = need_t * abs(standard_error)
        # Standard error shrinks with sqrt(periods), so scale periods to close the t gap.
        periods_needed = periods * (need_t / observed_t) ** 2
        print(f"  {row['scanner_name'][:28]:28s} {row['interval']:4s} "
              f"{row['direction']:>3d} {row['horizon_bars']:>3d} {row['slice_name'][:26]:26s} "
              f"{periods:>7d} {alpha:>7.3%} {observed_t:>6.2f} "
              f"{need_t:>7.2f} {mde:>7.3%} {periods_needed:>11,.0f}")

    print(f"\nIf the family had been pre-registered at {args.small_family} hypotheses:\n")
    print(header)
    for rank, row in enumerate(ranked[:15], start=1):
        observed_t = float(row["alpha_t_stat"])
        if observed_t <= 0:
            continue
        alpha = float(row["mean_net_alpha"])
        periods = int(row["independent_periods"])
        standard_error = alpha / observed_t
        capped_rank = min(rank, args.small_family)
        need_t = required_t(capped_rank, args.small_family, args.q)
        mde = need_t * abs(standard_error)
        periods_needed = periods * (need_t / observed_t) ** 2
        flag = "  <-- would pass" if observed_t >= need_t else ""
        print(f"  {row['scanner_name'][:28]:28s} {row['interval']:4s} "
              f"{row['direction']:>3d} {row['horizon_bars']:>3d} {row['slice_name'][:26]:26s} "
              f"{periods:>7d} {alpha:>7.3%} {observed_t:>6.2f} "
              f"{need_t:>7.2f} {mde:>7.3%} {periods_needed:>11,.0f}{flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
