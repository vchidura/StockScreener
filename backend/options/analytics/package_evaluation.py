"""Pure WP7 cohort, split, variant, and descriptive evaluation contracts."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import prod
from statistics import fmean, median
from typing import Literal, Mapping, Sequence

import exchange_calendars
from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256


EvaluationSplit = Literal["TRAIN", "VALIDATION", "TEST", "EMBARGO", "OUTSIDE"]
EvaluationVariant = Literal[
    "EXISTING_CANDIDATE_BASELINE",
    "MINIMAL_STOCK_GATES",
    "ALIGNED_RELATIVE_STRENGTH_SIGN",
]
MeasurementType = Literal["60MIN", "CLOSE", "NEXT_OPEN"]


class EvaluationWindow(Contract):
    start_session: date
    end_session: date


class OptionPackageEvaluationManifest(Contract):
    schema_version: Literal["option_package_evaluation_manifest_v1"] = (
        "option_package_evaluation_manifest_v1"
    )
    study_id: Name
    frozen_at: AwareDatetime
    underlyers: tuple[Name, ...]
    directional_strategies: tuple[Name, ...]
    variants: tuple[EvaluationVariant, ...]
    primary_measurement: MeasurementType
    sensitivity_measurements: tuple[MeasurementType, ...]
    train: EvaluationWindow
    validation: EvaluationWindow
    test: EvaluationWindow
    embargo_sessions: int = Field(strict=True, ge=1, le=5)
    maximum_package_rows: int = Field(strict=True, ge=1, le=50000)
    minimum_outcome_coverage: float = Field(strict=True, gt=0, le=1)
    minimum_independent_clusters: int = Field(strict=True, ge=2)
    package_assessment_policy_sha256: Sha256
    stock_behavior_launch_sha256: Sha256
    detector_policy_sha256: Sha256
    strategy_policy_sha256: Sha256
    valuation_policy_sha256: Sha256
    outcome_availability_policy_sha256: Sha256
    implementation_sha256: Sha256
    outcome_basis: Literal["INDICATIVE_OPTION_MARKS_NET_COMMISSION"]
    probability_status: Literal["UNAVAILABLE"] = "UNAVAILABLE"
    calibration_enabled: Literal[False] = False
    research_only: Literal[True] = True
    execution_permission: Literal[False] = False
    artifact_destination: str

    @model_validator(mode="after")
    def validate_manifest(self):
        if len(self.underlyers) != len(set(self.underlyers)):
            raise ValueError("evaluation underlyers must be unique")
        if not self.artifact_destination.startswith("backups/options-package-evaluation/"):
            raise ValueError("evaluation artifact must remain under its backup root")
        if not self.artifact_destination.endswith(".json") or ".." in self.artifact_destination:
            raise ValueError("evaluation artifact destination must be a safe JSON path")
        if self.variants != (
            "EXISTING_CANDIDATE_BASELINE",
            "MINIMAL_STOCK_GATES",
            "ALIGNED_RELATIVE_STRENGTH_SIGN",
        ):
            raise ValueError("evaluation variants and order are frozen")
        if self.primary_measurement in self.sensitivity_measurements:
            raise ValueError("primary measurement cannot repeat as sensitivity")
        calendar = exchange_calendars.get_calendar("XNYS")
        windows = (self.train, self.validation, self.test)
        for window in windows:
            if not calendar.is_session(window.start_session) or not calendar.is_session(
                window.end_session
            ):
                raise ValueError("evaluation windows require XNYS sessions")
            if window.end_session < window.start_session:
                raise ValueError("evaluation window end precedes start")
        if not self.train.end_session < self.validation.start_session:
            raise ValueError("training must precede validation")
        if not self.validation.end_session < self.test.start_session:
            raise ValueError("validation must precede test")
        for left, right in (
            (self.train.end_session, self.validation.start_session),
            (self.validation.end_session, self.test.start_session),
        ):
            cursor = left
            for _ in range(self.embargo_sessions + 1):
                cursor = calendar.next_session(cursor).date()
            if right < cursor:
                raise ValueError("evaluation split does not preserve its embargo")
        return self


def evaluation_split(
    session: date, manifest: OptionPackageEvaluationManifest,
) -> EvaluationSplit:
    if manifest.train.start_session <= session <= manifest.train.end_session:
        return "TRAIN"
    if manifest.validation.start_session <= session <= manifest.validation.end_session:
        return "VALIDATION"
    if manifest.test.start_session <= session <= manifest.test.end_session:
        return "TEST"
    if manifest.train.end_session < session < manifest.validation.start_session:
        return "EMBARGO"
    if manifest.validation.end_session < session < manifest.test.start_session:
        return "EMBARGO"
    return "OUTSIDE"


def stock_variant_states(
    assessment: Mapping[str, object] | None,
) -> dict[EvaluationVariant, dict[str, object]]:
    states: dict[EvaluationVariant, dict[str, object]] = {
        "EXISTING_CANDIDATE_BASELINE": {"included": True, "reason": None},
    }
    if assessment is None:
        missing = {"included": False, "reason": "STOCK_ASSESSMENT_UNAVAILABLE"}
        states["MINIMAL_STOCK_GATES"] = missing
        states["ALIGNED_RELATIVE_STRENGTH_SIGN"] = missing
        return states
    if assessment.get("disposition") != "ELIGIBLE_RESEARCH":
        reason = f"STOCK_DISPOSITION_{assessment.get('disposition', 'UNAVAILABLE')}"
        states["MINIMAL_STOCK_GATES"] = {"included": False, "reason": reason}
        states["ALIGNED_RELATIVE_STRENGTH_SIGN"] = {
            "included": False, "reason": reason,
        }
        return states
    states["MINIMAL_STOCK_GATES"] = {"included": True, "reason": None}
    gate = next(
        (
            row for row in assessment.get("gates", ())
            if row.get("gate_id") == "RELATIVE_STRENGTH_EVIDENCE"
        ),
        None,
    )
    value = gate.get("actual_float") if gate else None
    thesis = assessment.get("directional_thesis")
    aligned = value is not None and (
        thesis == "BULLISH" and value > 0
        or thesis == "BEARISH" and value < 0
    )
    states["ALIGNED_RELATIVE_STRENGTH_SIGN"] = {
        "included": aligned,
        "reason": None if aligned else "RELATIVE_STRENGTH_SIGN_NOT_ALIGNED",
    }
    return states


def first_candidate_cohorts(rows: Sequence[Mapping[str, object]]):
    grouped = defaultdict(list)
    for row in rows:
        key = (
            row["entry_session"], row["underlying"],
            row["strategy_name"], row["structure_type"],
        )
        grouped[key].append(row)
    return tuple(
        min(
            candidates,
            key=lambda row: (row["candidate_rank"], str(row["candidate_id"])),
        )
        for _, candidates in sorted(grouped.items())
    )


def summarize_evaluation(
    rows: Sequence[Mapping[str, object]],
    manifest: OptionPackageEvaluationManifest,
) -> dict[str, object]:
    summaries = []
    for measurement in (manifest.primary_measurement, *manifest.sensitivity_measurements):
        measurement_rows = [row for row in rows if row["measurement_type"] == measurement]
        baseline_measured = [
            row for row in measurement_rows if row.get("net_return") is not None
        ]
        for variant in manifest.variants:
            common = []
            excluded_reasons = defaultdict(int)
            for row in baseline_measured:
                state = row["variant_states"][variant]
                if not state["included"]:
                    excluded_reasons[state["reason"]] += 1
                common.append({
                    **row,
                    "arm_return": row["net_return"] if state["included"] else 0.0,
                })
            clusters = defaultdict(list)
            for row in common:
                clusters[(row["entry_session"], row["underlying"])].append(
                    float(row["arm_return"])
                )
            cluster_returns = [fmean(values) for _, values in sorted(clusters.items())]
            daily = defaultdict(list)
            for (session, _), values in clusters.items():
                daily[session].append(fmean(values))
            daily_returns = [fmean(values) for _, values in sorted(daily.items())]
            wealth = 1.0
            peak = 1.0
            max_drawdown = 0.0
            for value in daily_returns:
                wealth *= 1.0 + value
                peak = max(peak, wealth)
                max_drawdown = min(max_drawdown, wealth / peak - 1.0)
            coverage = (
                len(baseline_measured) / len(measurement_rows)
                if measurement_rows else 0.0
            )
            sufficient = (
                coverage >= manifest.minimum_outcome_coverage
                and len(clusters) >= manifest.minimum_independent_clusters
            )
            summaries.append({
                "measurement_type": measurement,
                "variant": variant,
                "candidate_cohorts": len(measurement_rows),
                "measured_baseline_outcomes": len(baseline_measured),
                "outcome_coverage": coverage,
                "included_packages": sum(
                    row["variant_states"][variant]["included"]
                    for row in baseline_measured
                ),
                "cash_abstentions": sum(
                    not row["variant_states"][variant]["included"]
                    for row in baseline_measured
                ),
                "exclusion_reasons": dict(sorted(excluded_reasons.items())),
                "independent_session_underlying_clusters": len(clusters),
                "distinct_sessions": len(daily),
                "cluster_mean_net_return": fmean(cluster_returns)
                if cluster_returns else None,
                "cluster_median_net_return": median(cluster_returns)
                if cluster_returns else None,
                "equal_weight_compounded_return": prod(
                    1.0 + value for value in daily_returns
                ) - 1.0 if daily_returns else None,
                "equal_weight_max_drawdown": max_drawdown if daily_returns else None,
                "verdict": "DESCRIPTIVE" if sufficient else "INCONCLUSIVE",
                "probability": None,
                "execution_permission": False,
            })
    return {
        "summaries": summaries,
        "calibration_status": "NOT_ATTEMPTED",
        "probability": None,
        "research_only": True,
        "execution_permission": False,
    }