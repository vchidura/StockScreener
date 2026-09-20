#!/usr/bin/env python3
"""Prepare a reviewed WP5 shadow launch manifest without activating it."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta, timezone
from pathlib import Path

import exchange_calendars
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY  # noqa: E402
from options.stock_behavior_shadow_launch import (  # noqa: E402
    OptionStockBehaviorShadowLaunch,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-id", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--continuous-development", action="store_true")
    parser.add_argument("--technical-forward", action="store_true", help="Prepare the separate pinned technical detector launch; never activates it.")
    parser.add_argument("--check-only", action="store_true", help="Validate a technical launch without writing its manifest.")
    parser.add_argument("--stock-ledger", default="backups/equity-shadow/stock-ideas-forward-v2/forward.sqlite")
    parser.add_argument("--usage-window-seconds", type=int, default=86400)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-assessments", type=int, default=1000)
    parser.add_argument("--maximum-payload-bytes", type=int, default=50_000_000)
    parser.add_argument("--maximum-candidates-per-matrix", type=int, default=100)
    parser.add_argument("--minimum-assessments-before-rate-stops", type=int, default=25)
    parser.add_argument("--maximum-unavailable-fraction", type=float, default=0.50)
    parser.add_argument("--maximum-p95-decision-lag-seconds", type=int, default=120)
    parser.add_argument("--artifact-destination", required=True)
    return parser.parse_args()


def main() -> int:
    args = _arguments()
    output = args.output.resolve()
    research_root = (BACKEND_DIR / "research").resolve()
    if output.parent != research_root or output.suffix != ".json":
        raise ValueError("launch manifest output must be JSON directly under backend/research")
    if output.exists():
        raise FileExistsError(f"launch manifest already exists: {output}")
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(pd.Timestamp(args.session_date), direction="none")
    starts_at = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    session_end = (
        calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        + timedelta(minutes=1)
    )
    configuration = load_option_runtime_configuration()
    if args.technical_forward:
        from options.detector_launch import prepare_detector_forward_launch
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
        from datetime import datetime

        environment = dict(os.environ, OPTION_STRATEGY_POLICY_FILE="options/policies/strategy_technical_forward_v1.json",
            OPTION_VALUATION_POLICY_FILE="options/policies/valuation_raw_spot_v2.json")
        configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
        launch = prepare_detector_forward_launch(backend_dir=BACKEND_DIR, configuration=configuration,
            dataset_id=args.launch_id, effective_from=starts_at, stock_ledger=args.stock_ledger)
        cutoff = datetime.now(timezone.utc)
        source_repository = OptionStockBehaviorAssessmentRepository()
        source_repository.detector_package_sources(configuration=configuration, candidate_ids=(), as_of=cutoff)
        technical_sources = source_repository.detector_technical_sources(underlyers=launch.underlyers, market_cutoff=cutoff, as_of=cutoff)
        if not args.check_only:
            with output.open("x", encoding="utf-8") as destination:
                destination.write(json.dumps(launch.model_dump(mode="json"), indent=2, sort_keys=True) + "\n")
        print(f"TECHNICAL_LAUNCH_PREPARED dataset={launch.dataset_id} sha256={launch.sha256} effective_from={launch.effective_from.isoformat()} underlyers={len(launch.underlyers)} current_technical_sources={len(technical_sources['sources'])} activated=False")
        return 0
    if args.check_only:
        raise ValueError("--check-only requires --technical-forward")
    launch = OptionStockBehaviorShadowLaunch(
        schema_version=(
            "option_stock_behavior_shadow_launch_v2"
            if args.continuous_development
            else "option_stock_behavior_shadow_launch_v1"
        ),
        mode=("CONTINUOUS_DEVELOPMENT" if args.continuous_development else "BOUNDED"),
        launch_id=args.launch_id,
        starts_at=starts_at,
        ends_at=None if args.continuous_development else session_end,
        usage_window_seconds=args.usage_window_seconds,
        underlyers=configuration.settings.underlyers,
        maximum_assessments=args.maximum_assessments,
        maximum_payload_bytes=args.maximum_payload_bytes,
        maximum_candidates_per_matrix=args.maximum_candidates_per_matrix,
        minimum_assessments_before_rate_stops=(
            args.minimum_assessments_before_rate_stops
        ),
        maximum_unavailable_fraction=args.maximum_unavailable_fraction,
        maximum_p95_decision_lag_seconds=args.maximum_p95_decision_lag_seconds,
        artifact_destination=args.artifact_destination,
        detector_policy_sha256=STOCK_BEHAVIOR_GATE_POLICY.sha256,
        behavior_definition_sha256=DEFINITION_SHA256,
        behavior_policy_sha256=OPTIONS_SWING_PROFILE.sha256,
        assessment_only=True,
        execution_permission=False,
    )
    output.write_text(
        json.dumps(launch.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "SHADOW_LAUNCH_PREPARED "
        f"launch_id={launch.launch_id} sha256={launch.sha256} "
        f"mode={launch.mode} starts_at={launch.starts_at.isoformat()} "
        f"ends_at={launch.ends_at.isoformat() if launch.ends_at else 'CONTINUOUS'} "
        f"underlyers={len(launch.underlyers)} output={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())