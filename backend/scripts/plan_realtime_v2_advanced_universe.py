#!/usr/bin/env python3
"""Propose one manual-gated Advanced universe ladder move."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_scale import (  # noqa: E402
    AdvancedScaleMetrics,
    recommend_advanced_scale,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.equity_pipeline_config import RealtimeV2PipelineConfig  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics-report", type=Path, required=True)
    parser.add_argument("--desired-target", type=int, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args(argv)
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists():
            parser.error("--report-path must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    policy = catalog.scale_policy
    catalog.profile_for_target(args.desired_target)
    value = json.loads(args.metrics_report.read_text(encoding="utf-8"))
    metrics = AdvancedScaleMetrics(**value)
    recommendation = recommend_advanced_scale(
        policy,
        metrics,
        desired_target=args.desired_target,
    )
    report = {
        "action": recommendation.action.value,
        "current_target": recommendation.current_target,
        "desired_target": recommendation.desired_target,
        "proposed_target": recommendation.proposed_target,
        "reason_codes": recommendation.reason_codes,
        "policy_sha256": recommendation.policy_sha256,
        "runtime_catalog_version": catalog.version,
        "requires_operator_approval": (
            recommendation.requires_operator_approval
        ),
        "database_writes": 0,
        "provider_requests": 0,
    }
    if args.report_path is not None:
        write_report(args.report_path, report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
