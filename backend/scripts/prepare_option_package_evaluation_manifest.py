#!/usr/bin/env python3
"""Freeze a prospective WP7 package evaluation manifest without running the study."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.package_evaluation import (  # noqa: E402
    EvaluationWindow,
    OptionPackageEvaluationManifest,
)
from options.config import load_option_runtime_configuration  # noqa: E402
from options.outcome_contracts import (  # noqa: E402
    PACKAGE_ASSESSMENT_POLICY,
    OptionOutcomeAvailabilityPolicy,
)
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY  # noqa: E402


IMPLEMENTATION_FILES = (
    BACKEND_DIR / "options/analytics/package_evaluation.py",
    BACKEND_DIR / "scripts/report_option_package_evaluation.py",
)


def _arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--study-id", default="option_package_prospective_v1")
    return parser.parse_args()


def _implementation_sha256() -> str:
    payload = {
        path.relative_to(BACKEND_DIR).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in IMPLEMENTATION_FILES
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def main() -> int:
    args = _arguments()
    output = args.output.resolve()
    expected_parent = (BACKEND_DIR / "research/inputs").resolve()
    if output.parent != expected_parent or output.suffix != ".json":
        raise ValueError("evaluation manifest must be JSON under backend/research/inputs")
    if output.exists():
        raise FileExistsError(f"evaluation manifest already exists: {output}")
    runtime = load_option_runtime_configuration()
    launch = runtime.stock_behavior_shadow_launch
    if launch is None:
        raise ValueError("prospective evaluation requires configured stock behavior launch")
    manifest = OptionPackageEvaluationManifest(
        study_id=args.study_id,
        frozen_at=datetime.now(timezone.utc),
        underlyers=runtime.settings.underlyers,
        directional_strategies=(
            "DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD",
        ),
        variants=(
            "EXISTING_CANDIDATE_BASELINE",
            "MINIMAL_STOCK_GATES",
            "ALIGNED_RELATIVE_STRENGTH_SIGN",
        ),
        primary_measurement="NEXT_OPEN",
        sensitivity_measurements=("60MIN", "CLOSE"),
        train=EvaluationWindow(
            start_session=date(2026, 9, 18), end_session=date(2026, 10, 2),
        ),
        validation=EvaluationWindow(
            start_session=date(2026, 10, 6), end_session=date(2026, 10, 16),
        ),
        test=EvaluationWindow(
            start_session=date(2026, 10, 20), end_session=date(2026, 10, 30),
        ),
        embargo_sessions=1,
        maximum_package_rows=50000,
        minimum_outcome_coverage=0.80,
        minimum_independent_clusters=20,
        package_assessment_policy_sha256=PACKAGE_ASSESSMENT_POLICY.sha256,
        stock_behavior_launch_sha256=launch.sha256,
        detector_policy_sha256=STOCK_BEHAVIOR_GATE_POLICY.sha256,
        strategy_policy_sha256=runtime.strategy_policy_sha256,
        valuation_policy_sha256=runtime.valuation_policy_sha256,
        outcome_availability_policy_sha256=OptionOutcomeAvailabilityPolicy().sha256,
        implementation_sha256=_implementation_sha256(),
        outcome_basis="INDICATIVE_OPTION_MARKS_NET_COMMISSION",
        artifact_destination=(
            "backups/options-package-evaluation/option-package-prospective-v1.json"
        ),
    )
    output.write_text(
        json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "FROZEN", "study_id": manifest.study_id,
        "manifest_sha256": manifest.sha256,
        "implementation_sha256": manifest.implementation_sha256,
        "output": output.relative_to(BACKEND_DIR.parent).as_posix(),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())