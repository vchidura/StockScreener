#!/usr/bin/env python3
"""Run a bounded read-only WP7 prospective package comparison."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env", override=True)

from database import get_db_connection  # noqa: E402
from options.analytics.package_evaluation import (  # noqa: E402
    OptionPackageEvaluationManifest,
    evaluation_split,
    first_candidate_cohorts,
    stock_variant_states,
    summarize_evaluation,
)


def _arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args()


def _serializable(value):
    if isinstance(value, dict):
        return {str(key): _serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serializable(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    return value


def _digest(value) -> str:
    return hashlib.sha256(
        json.dumps(
            _serializable(value), sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _load_manifest(path: Path) -> OptionPackageEvaluationManifest:
    return OptionPackageEvaluationManifest.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def run_report(manifest_path: Path) -> dict[str, object]:
    manifest = _load_manifest(manifest_path)
    implementation_sha256 = hashlib.sha256(
        json.dumps({
            path.relative_to(BACKEND_DIR).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in (
                BACKEND_DIR / "options/analytics/package_evaluation.py",
                Path(__file__),
            )
        }, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    if implementation_sha256 != manifest.implementation_sha256:
        raise ValueError("evaluation implementation hash differs from frozen manifest")
    overall_start = manifest.train.start_session
    overall_end = manifest.test.end_session
    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout='30s'")
                cursor.execute("SELECT transaction_timestamp() AS cutoff")
                cutoff = cursor.fetchone()["cutoff"]
                cursor.execute(
                    """
                    SELECT assessment.candidate_id, candidate.candidate_identity,
                           candidate.underlying, candidate.strategy_name,
                           candidate.structure_type, candidate.candidate_rank,
                           candidate.market_data_time, candidate.observed_time,
                           assessment.package_terms_sha256,
                           assessment.payload_sha256 AS package_assessment_sha256,
                           assessment.package_payload_text,
                           (candidate.market_data_time AT TIME ZONE 'America/New_York')::date
                               AS entry_session
                    FROM option_package_assessments AS assessment
                    JOIN option_strategy_candidates AS candidate USING(candidate_id)
                    WHERE assessment.assessment_status='READY'
                      AND assessment.assessment_policy_sha256=%s
                      AND candidate.policy_sha256=%s
                      AND candidate.strategy_name=ANY(%s)
                      AND candidate.underlying=ANY(%s)
                      AND (candidate.market_data_time AT TIME ZONE 'America/New_York')::date
                          BETWEEN %s AND %s
                      AND assessment.recorded_at<=%s
                    ORDER BY entry_session, candidate.underlying,
                             candidate.strategy_name, candidate.structure_type,
                             candidate.candidate_rank, candidate.candidate_id
                    LIMIT %s
                    """,
                    (
                        manifest.package_assessment_policy_sha256,
                        manifest.strategy_policy_sha256,
                        list(manifest.directional_strategies), list(manifest.underlyers),
                        overall_start, overall_end, cutoff,
                        manifest.maximum_package_rows + 1,
                    ),
                )
                package_rows = [dict(row) for row in cursor.fetchall()]
                if len(package_rows) > manifest.maximum_package_rows:
                    raise ValueError("evaluation package row bound exceeded")
                cohorts = first_candidate_cohorts(package_rows)
                candidate_ids = [row["candidate_id"] for row in cohorts]
                stock_rows = []
                outcome_rows = []
                unavailable_rows = []
                if candidate_ids:
                    cursor.execute(
                        """
                        SELECT DISTINCT ON (candidate_id)
                               candidate_id, payload_text, payload_sha256,
                               disposition, recorded_at
                        FROM option_stock_behavior_assessments
                        WHERE candidate_id=ANY(%s::uuid[])
                          AND launch_manifest_sha256=%s
                          AND detector_policy_sha256=%s
                          AND recorded_at<=%s
                        ORDER BY candidate_id, decision_at DESC, recorded_at DESC,
                                 assessment_id
                        """,
                        (
                            candidate_ids, manifest.stock_behavior_launch_sha256,
                            manifest.detector_policy_sha256, cutoff,
                        ),
                    )
                    stock_rows = [dict(row) for row in cursor.fetchall()]
                    cursor.execute(
                        """
                        SELECT candidate_id, measurement_type, net_return, net_pnl,
                               estimated_cost, market_time, observed_time,
                               valuation_policy_sha256, source_snapshot_ids,
                               source_batch_id, quality_flags
                        FROM option_signal_decay_outcomes
                        WHERE candidate_id=ANY(%s::uuid[])
                          AND measurement_type=ANY(%s)
                          AND valuation_policy_sha256=%s
                          AND created_at<=%s
                        """,
                        (
                            candidate_ids,
                            [manifest.primary_measurement,
                             *manifest.sensitivity_measurements],
                            manifest.valuation_policy_sha256, cutoff,
                        ),
                    )
                    outcome_rows = [dict(row) for row in cursor.fetchall()]
                    cursor.execute(
                        """
                        SELECT candidate_id, measurement_type, missing_contract_ids,
                               availability_policy_sha256, payload_sha256
                        FROM option_outcome_unavailable_evidence
                        WHERE candidate_id=ANY(%s::uuid[])
                          AND measurement_type=ANY(%s)
                          AND valuation_policy_sha256=%s
                          AND availability_policy_sha256=%s
                          AND recorded_at<=%s
                        """,
                        (
                            candidate_ids,
                            [manifest.primary_measurement,
                             *manifest.sensitivity_measurements],
                            manifest.valuation_policy_sha256,
                            manifest.outcome_availability_policy_sha256, cutoff,
                        ),
                    )
                    unavailable_rows = [dict(row) for row in cursor.fetchall()]
        finally:
            if not connection.closed:
                connection.rollback()
                connection.set_session(
                    readonly=False, isolation_level="READ COMMITTED"
                )
    stock_by_candidate = {
        row["candidate_id"]: json.loads(row["payload_text"])
        for row in stock_rows
    }
    outcome_by_key = {
        (row["candidate_id"], row["measurement_type"]): row
        for row in outcome_rows
    }
    unavailable_by_key = {
        (row["candidate_id"], row["measurement_type"]): row
        for row in unavailable_rows
    }
    rows = []
    for cohort in cohorts:
        states = stock_variant_states(stock_by_candidate.get(cohort["candidate_id"]))
        split = evaluation_split(cohort["entry_session"], manifest)
        for measurement in (
            manifest.primary_measurement, *manifest.sensitivity_measurements,
        ):
            outcome = outcome_by_key.get((cohort["candidate_id"], measurement))
            unavailable = unavailable_by_key.get((cohort["candidate_id"], measurement))
            rows.append({
                "candidate_id": str(cohort["candidate_id"]),
                "candidate_identity_sha256": cohort["candidate_identity"],
                "underlying": cohort["underlying"],
                "strategy_name": cohort["strategy_name"],
                "structure_type": cohort["structure_type"],
                "entry_session": cohort["entry_session"],
                "split": split,
                "measurement_type": measurement,
                "outcome_status": (
                    "MEASURED" if outcome else "UNAVAILABLE" if unavailable
                    else "PENDING_OR_NOT_MATURE"
                ),
                "net_return": float(outcome["net_return"])
                if outcome and outcome["net_return"] is not None else None,
                "net_pnl": float(outcome["net_pnl"])
                if outcome and outcome["net_pnl"] is not None else None,
                "estimated_cost": float(outcome["estimated_cost"])
                if outcome and outcome["estimated_cost"] is not None else None,
                "missing_contract_ids": list(
                    unavailable["missing_contract_ids"]
                ) if unavailable else [],
                "package_terms_sha256": cohort["package_terms_sha256"],
                "package_assessment_sha256": cohort["package_assessment_sha256"],
                "variant_states": states,
            })
    concentration = {
        "packages_by_underlying": dict(Counter(
            row["underlying"] for row in cohorts
        )),
        "packages_by_strategy": dict(Counter(
            row["strategy_name"] for row in cohorts
        )),
        "packages_by_structure": dict(Counter(
            row["structure_type"] for row in cohorts
        )),
        "outcome_states": dict(Counter(row["outcome_status"] for row in rows)),
        "split_packages": dict(Counter(
            evaluation_split(row["entry_session"], manifest) for row in cohorts
        )),
    }
    split_reports = {
        split: summarize_evaluation(
            [row for row in rows if row["split"] == split], manifest,
        )
        for split in ("TRAIN", "VALIDATION", "TEST")
    }
    report = {
        "schema_version": "option_package_evaluation_report_v1",
        "study_id": manifest.study_id,
        "manifest": manifest.model_dump(mode="json"),
        "manifest_sha256": manifest.sha256,
        "generated_at": cutoff,
        "transaction_read_only": True,
        "source_counts": {
            "package_rows": len(package_rows), "cohorts": len(cohorts),
            "stock_assessments": len(stock_rows),
            "measured_outcomes": len(outcome_rows),
            "unavailable_outcomes": len(unavailable_rows),
        },
        "concentration_before_returns": concentration,
        "split_reports": split_reports,
        "rows": rows,
        "input_hashes": {
            "packages": _digest(package_rows), "stock": _digest(stock_rows),
            "outcomes": _digest(outcome_rows),
            "unavailable": _digest(unavailable_rows),
        },
        "calibration_status": "NOT_ATTEMPTED",
        "probability": None,
        "research_only": True,
        "execution_permission": False,
        "limitations": [
            "Prospective train/validation/test windows are still accruing.",
            "Overlapping contracts and horizons are grouped descriptively, not independent trials.",
            "Cash abstention is reported separately from matched package comparisons.",
            "Indicative marks include declared commission but not unavailable quote slippage.",
        ],
    }
    serializable = _serializable(report)
    serializable["report_sha256"] = _digest(serializable)
    return serializable


def main() -> int:
    args = _arguments()
    manifest_path = args.manifest.resolve()
    report = run_report(manifest_path)
    manifest = _load_manifest(manifest_path)
    if not args.no_write:
        output = BACKEND_DIR / manifest.artifact_destination
        if output.exists():
            raise FileExistsError(f"evaluation report already exists: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "study_id": report["study_id"],
        "manifest_sha256": report["manifest_sha256"],
        "report_sha256": report["report_sha256"],
        "source_counts": report["source_counts"],
        "concentration_before_returns": report["concentration_before_returns"],
        "train_verdicts": [
            row["verdict"] for row in report["split_reports"]["TRAIN"]["summaries"]
        ],
        "validation_rows": len(report["split_reports"]["VALIDATION"]["summaries"]),
        "test_rows": len(report["split_reports"]["TEST"]["summaries"]),
        "probability": None,
        "execution_permission": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())