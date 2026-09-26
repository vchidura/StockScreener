#!/usr/bin/env python3
"""Prepare a reviewed WP5 shadow launch manifest without activating it."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
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
    parser.add_argument("--effective-from", type=datetime.fromisoformat, help="Optional aware in-session cutoff for a prospective technical launch; existing completed slots remain untouched.")
    parser.add_argument("--continuous-development", action="store_true")
    parser.add_argument("--technical-forward", action="store_true", help="Prepare the separate pinned technical detector launch; never activates it.")
    parser.add_argument("--intraday-confirmation", action="store_true", help="Pin reviewed O1 v3 latest-available completed-30m confirmation for development observation alerts.")
    parser.add_argument("--stock-setup-wait", action="store_true", help="Wait boundedly for the exact S1/S2 stock publication while causal option activity remains valid.")
    parser.add_argument("--approve-stock-runtime-transition", action="store_true", help="Explicitly pin reviewed current stock code before its first new publication; old publications remain ineligible under the new pins.")
    parser.add_argument("--check-only", action="store_true", help="Validate a technical launch without writing its manifest.")
    parser.add_argument("--strategy-policy-file", choices=(
        "options/policies/strategy_technical_forward_v1.json",
        "options/policies/strategy_technical_forward_v2.json",
        "options/policies/strategy_o3_credit_v1.json",
        "options/policies/strategy_o3_credit_v2.json",
    ), default="options/policies/strategy_technical_forward_v1.json")
    parser.add_argument("--stock-ledger", default="backups/equity-shadow/stock-ideas-forward-v2/forward.sqlite")
    parser.add_argument("--reuse-stock-source-launch", type=Path,
        help="Reuse exact reviewed stock source pins from an existing detector launch for an option-only runtime transition.")
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
    if (args.approve_stock_runtime_transition or args.effective_from is not None or args.intraday_confirmation
            or args.stock_setup_wait) and not args.technical_forward:
        raise ValueError("stock runtime transition requires a technical forward launch")
    if args.reuse_stock_source_launch is not None and not args.technical_forward:
        raise ValueError("stock source launch reuse requires a technical forward launch")
    if args.stock_setup_wait and not args.intraday_confirmation:
        raise ValueError("stock setup wait requires intraday confirmation")
    output = args.output.resolve()
    research_root = (BACKEND_DIR / "research").resolve()
    if output.parent != research_root or output.suffix != ".json":
        raise ValueError("launch manifest output must be JSON directly under backend/research")
    if output.exists():
        raise FileExistsError(f"launch manifest already exists: {output}")
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(pd.Timestamp(args.session_date), direction="none")
    starts_at = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    if args.effective_from is not None:
        if (args.effective_from.utcoffset() is None or not starts_at <= args.effective_from
                <= calendar.session_close(session).to_pydatetime()):
            raise ValueError("technical effective cutoff must be aware and inside the requested exchange session")
        starts_at = args.effective_from.astimezone(timezone.utc)
    session_end = (
        calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        + timedelta(minutes=1)
    )
    configuration = load_option_runtime_configuration()
    if args.technical_forward:
        from options.detector_launch import prepare_detector_forward_launch
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
        environment = dict(os.environ, OPTION_STRATEGY_POLICY_FILE=args.strategy_policy_file,
            OPTION_VALUATION_POLICY_FILE="options/policies/valuation_raw_spot_v2.json")
        configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
        stock_source_launch = None
        if args.reuse_stock_source_launch is not None:
            from options.detector_launch import decode_detector_forward_launch

            source_path = args.reuse_stock_source_launch.resolve()
            if source_path.parent != (BACKEND_DIR / "research").resolve() or not source_path.is_file():
                raise ValueError("reused stock source launch must be an existing research manifest")
            stock_source_launch = decode_detector_forward_launch(source_path.read_text(encoding="utf-8"))
        launch = prepare_detector_forward_launch(backend_dir=BACKEND_DIR, configuration=configuration,
            dataset_id=args.launch_id, effective_from=starts_at, stock_ledger=args.stock_ledger,
            approve_stock_runtime_transition=args.approve_stock_runtime_transition,
            intraday_confirmation=args.intraday_confirmation, stock_setup_wait=args.stock_setup_wait,
            stock_source_launch=stock_source_launch)
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