#!/usr/bin/env python3
"""Daily option performance review: realized profit and loss by strategy.

Read-only. Intended to be run once per session and read by a human before anything is
trusted, which is why it reports coverage and data quality alongside the numbers rather
than only the numbers.

What the report will and will not support:

It shows realized profit and loss from `option_signal_decay_outcomes`, which are measured
against delayed provider marks under a recorded valuation policy. They are not fills. A
positive total here is evidence that the selection produced marks that moved favourably,
not that the trade could have been executed at those prices.

Raw P&L summaries carry `NOT_BASELINE_ADJUSTED`. The separate baseline section compares
complete translated structures and keeps inferential statistics scoped to one measurement
horizon; mixed-horizon summaries are descriptive only.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.analytics.performance import (  # noqa: E402
    BaselineComparison,
    BaselinePair,
    BaselineStructureLeg,
    OptionConfidenceSummary,
    OutcomeRow,
    PerformanceSummary,
    PerformanceVerdict,
    compare_against_baseline,
    evaluate_structure_baseline,
    group_by,
    summarize_outcomes,
    summarize_option_confidence,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.outcomes import delayed_proxy_commission_policy  # noqa: E402

DEFAULT_OUTPUT_DIR = BACKEND_DIR.parent / "docs" / "option_performance"

SQL_OUTCOMES = """
    SELECT c.strategy_name,
           c.underlying,
           o.measurement_type,
           (o.market_time AT TIME ZONE 'America/New_York')::date AS session_date,
           o.net_pnl,
           o.net_return,
           o.capital_at_risk,
           o.estimated_cost,
           o.availability_flag,
           o.valuation_policy_sha256
    FROM option_signal_decay_outcomes o
    JOIN option_strategy_candidates c ON c.candidate_id = o.candidate_id
    WHERE o.market_time >= %s
      AND (%s::text IS NULL OR c.strategy_name = %s)
    ORDER BY o.market_time
"""

SQL_COHORT = """
    SELECT c.strategy_name, c.status, c.candidate_kind,
           count(*) AS candidates,
           count(DISTINCT c.candidate_id) FILTER (
               WHERE EXISTS (
                   SELECT 1 FROM option_signal_decay_outcomes o
                   WHERE o.candidate_id = c.candidate_id
                      AND (%s::text IS NULL OR o.measurement_type = %s)
               )
           ) AS measured
    FROM option_strategy_candidates c
    WHERE c.market_data_time >= %s
            AND (%s::text IS NULL OR c.strategy_name = %s)
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
"""

SQL_AVAILABILITY = """
    SELECT o.availability_flag, count(*) AS outcomes,
           count(o.net_pnl) AS priced
        FROM option_signal_decay_outcomes o
        JOIN option_strategy_candidates c USING (candidate_id)
    WHERE o.market_time >= %s
            AND (%s::text IS NULL OR c.strategy_name = %s)
            AND (%s::text IS NULL OR o.measurement_type = %s)
    GROUP BY 1 ORDER BY 2 DESC
"""

SQL_SUPPRESSIONS = """
    SELECT unnest(c.reason_codes) AS reason, count(*) AS candidates
    FROM option_strategy_candidates c
    WHERE c.market_data_time >= %s AND c.status = 'SUPPRESSED'
            AND (%s::text IS NULL OR c.strategy_name = %s)
    GROUP BY 1 ORDER BY 2 DESC LIMIT 15
"""

SQL_BASELINE_TEMPLATES = """
SELECT outcome.outcome_id, outcome.measurement_type,
             outcome.net_return AS strategy_return,
             outcome.source_batch_id AS exit_batch_id,
             candidate.candidate_id, candidate.strategy_name,
             candidate.structure_type, candidate.underlying,
             candidate.expiration_date,
             candidate.market_data_time AS candidate_market_time,
               (candidate.market_data_time AT TIME ZONE 'America/New_York')::date
                   AS session_date,
             analysis.batch_id AS entry_batch_id,
             leg.leg_index, leg.side, leg.ratio, leg.multiplier,
             leg.contract_type, leg.strike, leg.spot
FROM option_signal_decay_outcomes outcome
JOIN option_strategy_candidates candidate USING (candidate_id)
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_candidate_legs leg USING (candidate_id)
WHERE outcome.market_time >= %s
    AND (%s::text IS NULL OR candidate.strategy_name = %s)
    AND outcome.net_return IS NOT NULL
    AND outcome.source_batch_id IS NOT NULL
    AND candidate.expiration_date IS NOT NULL
ORDER BY outcome.outcome_id, leg.leg_index
"""

SQL_BASELINE_MARKS = """
WITH measured_batches AS (
        SELECT DISTINCT analysis.batch_id
        FROM option_signal_decay_outcomes outcome
        JOIN option_strategy_candidates candidate USING (candidate_id)
        JOIN option_analysis_runs analysis USING (matrix_id)
        WHERE outcome.market_time >= %s
            AND (%s::text IS NULL OR candidate.strategy_name = %s)
            AND outcome.net_return IS NOT NULL
        UNION
        SELECT DISTINCT outcome.source_batch_id
        FROM option_signal_decay_outcomes outcome
        JOIN option_strategy_candidates candidate USING (candidate_id)
        WHERE outcome.market_time >= %s
            AND (%s::text IS NULL OR candidate.strategy_name = %s)
            AND outcome.net_return IS NOT NULL
            AND outcome.source_batch_id IS NOT NULL
)
SELECT DISTINCT ON (snapshot.batch_id, snapshot.contract_id)
             snapshot.batch_id, snapshot.contract_id, snapshot.underlying,
             snapshot.expiration_date, snapshot.contract_type, snapshot.strike,
             snapshot.model_mark
FROM option_chain_snapshots snapshot
JOIN measured_batches batch USING (batch_id)
WHERE snapshot.model_mark IS NOT NULL
ORDER BY snapshot.batch_id, snapshot.contract_id,
                 snapshot.first_observed_at DESC, snapshot.revision DESC,
                 snapshot.snapshot_id
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days.")
    parser.add_argument("--strategy", default=None, help="Restrict to one strategy.")
    parser.add_argument(
        "--horizon",
        default=None,
        help="Restrict to one measurement type, e.g. CLOSE or 60MIN.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Daily artifacts are written here, one file per run date.",
    )
    parser.add_argument("--no-write", action="store_true", help="Print only.")
    return parser.parse_args()


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _finite_or_none(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def _summary_payload(summary: PerformanceSummary) -> dict[str, Any]:
    return {
        "label": summary.label,
        "verdict": summary.verdict.value,
        "observations": summary.observations,
        "priced_observations": summary.priced_observations,
        "independent_sessions": summary.independent_sessions,
        "win_rate": summary.win_rate,
        "mean_net_return": summary.mean_net_return,
        "median_net_return": summary.median_net_return,
        "total_net_pnl": str(summary.total_net_pnl),
        "total_estimated_cost": str(summary.total_estimated_cost),
        "mean_capital_at_risk": summary.mean_capital_at_risk,
        "profit_factor": summary.profit_factor,
        "largest_gain": str(summary.largest_gain) if summary.largest_gain is not None else None,
        "largest_loss": str(summary.largest_loss) if summary.largest_loss is not None else None,
        "valuation_policies": list(summary.valuation_policies),
        "reasons": list(summary.reasons),
    }


def _baseline_payload(comparison: BaselineComparison) -> dict[str, Any]:
    return {
        "label": comparison.label,
        "verdict": comparison.verdict.value,
        "pairs": comparison.pairs,
        "period_observations": comparison.period_observations,
        "independent_sessions": comparison.independent_sessions,
        "measurement_types": list(comparison.measurement_types),
        "mean_strategy_return": comparison.mean_strategy_return,
        "mean_baseline_return": comparison.mean_baseline_return,
        "mean_advantage": comparison.mean_advantage,
        "median_advantage": comparison.median_advantage,
        "beat_baseline_rate": comparison.beat_baseline_rate,
        "statistic": _finite_or_none(comparison.statistic),
        "p_value": _finite_or_none(comparison.p_value),
        "reasons": list(comparison.reasons),
    }


def _confidence_payload(summary: OptionConfidenceSummary) -> dict[str, Any]:
    return {
        "strategy_name": summary.strategy_name,
        "measurement_type": summary.measurement_type,
        "events": summary.events,
        "independent_periods": summary.independent_periods,
        "mean_strategy_return": summary.mean_strategy_return,
        "strategy_t_stat": summary.strategy_t_stat,
        "early_strategy_return": summary.early_strategy_return,
        "late_strategy_return": summary.late_strategy_return,
        "mean_advantage": summary.mean_advantage,
        "advantage_t_stat": summary.advantage_t_stat,
        "early_advantage": summary.early_advantage,
        "late_advantage": summary.late_advantage,
        "strategy_p_value": summary.strategy_p_value,
        "advantage_p_value": summary.advantage_p_value,
        "strategy_fdr_q": summary.strategy_fdr_q,
        "advantage_fdr_q": summary.advantage_fdr_q,
        "status": summary.status,
        "robustness_status": summary.robustness_status,
    }


def _print_baseline(comparison: BaselineComparison, indent: str = "  ") -> None:
    if comparison.pairs == 0:
        print(f"{indent}{comparison.label:<28} no pairs")
        return
    print(
        f"{indent}{comparison.label:<28} {comparison.verdict.value:<12}"
        f" pairs={comparison.pairs:<5} periods={comparison.period_observations:<4}"
        f" sessions={comparison.independent_sessions:<4}"
        f" strategy={comparison.mean_strategy_return:+.2%}"
        f" naive={comparison.mean_baseline_return:+.2%}"
    )
    if "MIXED_MEASUREMENT_TYPES" in comparison.reasons:
        significance = "p=n/a (mixed measurement horizons)"
    elif comparison.p_value != comparison.p_value:
        significance = (
            f"p=n/a ({comparison.period_observations} independent periods, needs 3)"
        )
    else:
        significance = f"p={comparison.p_value:.4f}"
    print(
        f"{indent}{'':<28} advantage {comparison.mean_advantage:+.2%}"
        f"  beat naive {comparison.beat_baseline_rate:.0%} of periods"
        f"  {significance}"
    )
    if comparison.reasons:
        print(f"{indent}{'':<28} [{' '.join(comparison.reasons)}]")


def _print_summary(summary: PerformanceSummary, indent: str = "  ") -> None:
    if summary.verdict is PerformanceVerdict.INSUFFICIENT:
        print(
            f"{indent}{summary.label:<28} {summary.verdict.value}"
            f"  ({summary.priced_observations} priced of {summary.observations})"
        )
        return
    headline = (
        f"{indent}{summary.label:<28} {summary.verdict.value:<12}"
        f" n={summary.priced_observations:<5} sessions={summary.independent_sessions:<4}"
        f" win={summary.win_rate:.0%}"
    )
    if summary.mean_net_return is not None:
        headline += f" mean_ret={summary.mean_net_return:+.2%}"
    print(headline)
    detail = (
        f"{indent}{'':<28} net P&L {summary.total_net_pnl:+,.2f}"
        f"  costs {summary.total_estimated_cost:,.2f}"
    )
    if summary.profit_factor is not None:
        detail += f"  profit factor {summary.profit_factor:.2f}"
    if summary.largest_gain is not None:
        detail += f"  best {summary.largest_gain:+,.2f} worst {summary.largest_loss:+,.2f}"
    print(detail)
    flagged = [reason for reason in summary.reasons if reason != "NOT_BASELINE_ADJUSTED"]
    if flagged:
        print(f"{indent}{'':<28} [{' '.join(flagged)}]")


def _print_confidence(summary: OptionConfidenceSummary) -> None:
    advantage = (
        f"{summary.mean_advantage:+.2%}"
        if summary.mean_advantage is not None
        else "unavailable"
    )
    advantage_t = (
        f"{summary.advantage_t_stat:.2f}"
        if summary.advantage_t_stat is not None
        else "unavailable"
    )
    advantage_q = (
        f"{summary.advantage_fdr_q:.4f}"
        if summary.advantage_fdr_q is not None
        else "unavailable"
    )
    print(
        f"  {summary.strategy_name:<28} {summary.measurement_type:<10}"
        f" {summary.robustness_status:<12} events={summary.events:<5}"
        f" periods={summary.independent_periods:<4} advantage={advantage}"
        f" t={advantage_t} q={advantage_q}"
    )


def _structure_baseline_pairs(
    templates: list[dict[str, Any]],
    marks: list[dict[str, Any]],
    *,
    horizon: str | None,
) -> tuple[tuple[BaselinePair, ...], dict[str, int]]:
    by_outcome: dict[Any, list[dict[str, Any]]] = {}
    for row in templates:
        if horizon is not None and row["measurement_type"] != horizon:
            continue
        by_outcome.setdefault(row["outcome_id"], []).append(row)

    by_contract = {
        (row["batch_id"], row["contract_id"]): row
        for row in marks
    }
    by_strike: dict[tuple[Any, str, date, str], list[dict[str, Any]]] = {}
    for row in marks:
        key = (
            row["batch_id"],
            row["underlying"],
            row["expiration_date"],
            row["contract_type"],
        )
        by_strike.setdefault(key, []).append(row)
    for values in by_strike.values():
        values.sort(key=lambda row: row["strike"])

    commission = delayed_proxy_commission_policy().commission_per_contract_per_side
    pairs: list[BaselinePair] = []
    rejected_unbounded = 0
    exact_packages = 0
    for leg_rows in by_outcome.values():
        leg_rows.sort(key=lambda row: row["leg_index"])
        reference = leg_rows[0]
        structure_type = reference["structure_type"]
        anchor_index = (
            1
            if structure_type in {"CALL_BUTTERFLY", "PUT_BUTTERFLY", "IRON_CONDOR"}
            else 0
        )
        anchor_leg = next(
            row for row in leg_rows if row["leg_index"] == anchor_index
        )
        if structure_type == "IRON_CONDOR":
            short_strikes = [row["strike"] for row in leg_rows if row["side"] == "SELL"]
            original_center = sum(short_strikes, Decimal("0")) / len(short_strikes)
        elif structure_type in {"CALL_BUTTERFLY", "PUT_BUTTERFLY"}:
            original_center = anchor_leg["strike"]
        else:
            original_center = leg_rows[0]["strike"]
        anchors = by_strike.get(
            (
                reference["entry_batch_id"],
                reference["underlying"],
                reference["expiration_date"],
                anchor_leg["contract_type"],
            ),
            (),
        )
        anchors = sorted(
            anchors,
            key=lambda row: (
                abs(
                    original_center
                    + row["strike"]
                    - anchor_leg["strike"]
                    - reference["spot"]
                ),
                abs(row["strike"] - anchor_leg["strike"]),
                row["strike"],
            ),
        )
        translated: tuple[BaselineStructureLeg, ...] | None = None
        for anchor in anchors:
            offset = anchor["strike"] - anchor_leg["strike"]
            selected: list[BaselineStructureLeg] = []
            for leg in leg_rows:
                target_strike = leg["strike"] + offset
                entry = next(
                    (
                        row for row in by_strike.get(
                            (
                                reference["entry_batch_id"],
                                reference["underlying"],
                                reference["expiration_date"],
                                leg["contract_type"],
                            ),
                            (),
                        )
                        if row["strike"] == target_strike
                    ),
                    None,
                )
                if entry is None:
                    break
                exit_mark = by_contract.get(
                    (reference["exit_batch_id"], entry["contract_id"])
                )
                if exit_mark is None:
                    break
                selected.append(
                    BaselineStructureLeg(
                        leg_index=int(leg["leg_index"]),
                        side=leg["side"],
                        ratio=int(leg["ratio"]),
                        multiplier=int(leg["multiplier"]),
                        contract_type=leg["contract_type"],
                        strike=entry["strike"],
                        entry_mark=Decimal(entry["model_mark"]),
                        exit_mark=Decimal(exit_mark["model_mark"]),
                    )
                )
            if len(selected) == len(leg_rows):
                translated = tuple(selected)
                break
        if translated is None:
            continue
        exact_packages += 1
        result = evaluate_structure_baseline(
            translated,
            commission_per_contract_per_side=commission,
        )
        if result is None:
            rejected_unbounded += 1
            continue
        pairs.append(
            BaselinePair(
                strategy_name=reference["strategy_name"],
                session_date=reference["session_date"],
                measurement_type=reference["measurement_type"],
                strategy_return=float(reference["strategy_return"]),
                baseline_return=result.net_return,
            )
        )
    return tuple(pairs), {
        "eligible_templates": len(by_outcome),
        "exact_packages": exact_packages,
        "unmatched_packages": len(by_outcome) - exact_packages,
        "bounded_packages": len(pairs),
        "rejected_invalid_risk_packages": rejected_unbounded,
    }


def _session_ordinals(pairs: tuple[BaselinePair, ...]) -> dict[date, int]:
    sessions = sorted({pair.session_date for pair in pairs})
    if not sessions:
        return {}
    calendar = OptionExchangeCalendar()
    result = {sessions[0]: 0}
    for prior, current in zip(sessions, sessions[1:]):
        result[current] = result[prior] + calendar.sessions_between(prior, current)
    return result


def main() -> int:
    args = _parse_args()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    with get_db_cursor() as cursor:
        cursor.execute(SQL_OUTCOMES, (since, args.strategy, args.strategy))
        raw = cursor.fetchall()
        cursor.execute(
            SQL_COHORT,
            (
                args.horizon, args.horizon,
                since, args.strategy, args.strategy,
            ),
        )
        cohort = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            SQL_AVAILABILITY,
            (
                since, args.strategy, args.strategy,
                args.horizon, args.horizon,
            ),
        )
        availability = [dict(row) for row in cursor.fetchall()]
        cursor.execute(SQL_SUPPRESSIONS, (since, args.strategy, args.strategy))
        suppressions = [dict(row) for row in cursor.fetchall()]
        cursor.execute(SQL_BASELINE_TEMPLATES, (since, args.strategy, args.strategy))
        baseline_templates = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            SQL_BASELINE_MARKS,
            (
                since, args.strategy, args.strategy,
                since, args.strategy, args.strategy,
            ),
        )
        baseline_marks = [dict(row) for row in cursor.fetchall()]

    rows = tuple(
        OutcomeRow(
            strategy_name=row["strategy_name"],
            underlying=row["underlying"],
            measurement_type=row["measurement_type"],
            session_date=row["session_date"],
            net_pnl=_decimal(row["net_pnl"]),
            net_return=_decimal(row["net_return"]),
            capital_at_risk=_decimal(row["capital_at_risk"]),
            estimated_cost=_decimal(row["estimated_cost"]),
            availability_flag=row["availability_flag"],
            valuation_policy_sha256=row["valuation_policy_sha256"],
        )
        for row in raw
        if args.horizon is None or row["measurement_type"] == args.horizon
    )

    print(f"=== option performance, last {args.days} days ===")
    print(f"window opens {since.date().isoformat()}   outcomes {len(rows)}")

    print("\n--- cohort coverage ---")
    if not cohort:
        print("  no candidates in the window")
    for row in cohort:
        print(
            f"  {row['strategy_name']:<28} {row['status']:<10} {row['candidate_kind']:<16}"
            f" candidates={row['candidates']:<6} measured={row['measured']}"
        )

    print("\n--- outcome availability ---")
    if not availability:
        print("  no outcomes recorded")
    for row in availability:
        print(
            f"  {row['availability_flag']:<28} outcomes={row['outcomes']:<6}"
            f" priced={row['priced']}"
        )

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": args.days,
        "window_start": since.isoformat(),
        "strategy_filter": args.strategy,
        "horizon_filter": args.horizon,
        "outcome_count": len(rows),
        "cohort": cohort,
        "availability": availability,
        "suppressions": suppressions,
        "overall": None,
        "by_strategy": {},
        "by_horizon": {},
        "by_underlying": {},
        "baseline": {
            "overall": None,
            "by_strategy": {},
            "by_horizon": {},
            "by_strategy_horizon": {},
            "coverage": {},
        },
        "confidence": [],
    }

    if rows:
        overall = summarize_outcomes("ALL", rows)
        report["overall"] = _summary_payload(overall)
        print("\n--- overall realized P&L ---")
        _print_summary(overall)

        for section, key in (
            ("by_strategy", "strategy_name"),
            ("by_horizon", "measurement_type"),
            ("by_underlying", "underlying"),
        ):
            print(f"\n--- {section.replace('_', ' ')} ---")
            for label, group in group_by(rows, key).items():
                summary = summarize_outcomes(label, group)
                report[section][label] = _summary_payload(summary)
                _print_summary(summary)
    else:
        print("\nNo priced outcomes in this window; nothing to measure yet.")

    if suppressions:
        print("\n--- top suppression reasons ---")
        for row in suppressions:
            print(f"  {row['reason']:<44} {row['candidates']}")

    pairs, baseline_coverage = _structure_baseline_pairs(
        baseline_templates, baseline_marks, horizon=args.horizon
    )
    eligible_outcomes = sum(
        row["net_return"] is not None
        and (args.horizon is None or row["measurement_type"] == args.horizon)
        for row in raw
    )
    baseline_coverage["eligible_outcomes"] = eligible_outcomes
    baseline_coverage["coverage_fraction"] = (
        len(pairs) / eligible_outcomes if eligible_outcomes else None
    )
    report["baseline"]["coverage"] = baseline_coverage
    print("\n--- versus structure-matched naive baseline ---")
    print(
        f"  exact coherent packages={baseline_coverage['exact_packages']}"
        f"  bounded={baseline_coverage['bounded_packages']}"
        f"  eligible outcomes={eligible_outcomes}"
    )
    if not pairs:
        print("  no pairs yet; the naive comparison needs chain snapshots for the")
        print("  candidate's matrix and for its measurement watermark")
    else:
        overall_baseline = compare_against_baseline("ALL", pairs)
        report["baseline"]["overall"] = _baseline_payload(overall_baseline)
        _print_baseline(overall_baseline)
        by_strategy: dict[str, list[BaselinePair]] = {}
        for pair in pairs:
            by_strategy.setdefault(pair.strategy_name, []).append(pair)
        for name, group in sorted(by_strategy.items()):
            comparison = compare_against_baseline(name, tuple(group))
            report["baseline"]["by_strategy"][name] = _baseline_payload(comparison)
            _print_baseline(comparison)

        print("\n--- baseline by measurement horizon ---")
        by_horizon: dict[str, list[BaselinePair]] = {}
        by_strategy_horizon: dict[tuple[str, str], list[BaselinePair]] = {}
        for pair in pairs:
            by_horizon.setdefault(pair.measurement_type, []).append(pair)
            by_strategy_horizon.setdefault(
                (pair.strategy_name, pair.measurement_type), []
            ).append(pair)
        for horizon, group in sorted(by_horizon.items()):
            comparison = compare_against_baseline(horizon, tuple(group))
            report["baseline"]["by_horizon"][horizon] = _baseline_payload(comparison)
            _print_baseline(comparison)
        for (name, horizon), group in sorted(by_strategy_horizon.items()):
            comparison = compare_against_baseline(
                f"{name}:{horizon}", tuple(group)
            )
            report["baseline"]["by_strategy_horizon"].setdefault(name, {})[
                horizon
            ] = _baseline_payload(comparison)

        confidence = summarize_option_confidence(
            pairs, session_ordinals=_session_ordinals(pairs)
        )
        report["confidence"] = [_confidence_payload(item) for item in confidence]
        print("\n--- equity-style confidence qualification ---")
        for summary in confidence:
            _print_confidence(summary)

    print(
        "\nCAVEATS: marks are delayed provider marks under a recorded valuation policy,"
        " not fills. The realized P&L sections are raw; only the baseline section"
        " separates selection from market direction."
    )

    if not args.no_write:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        target = args.output_dir / f"{date.today().isoformat()}.json"
        target.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"Wrote {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
