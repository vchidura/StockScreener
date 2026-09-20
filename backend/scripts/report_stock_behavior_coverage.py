#!/usr/bin/env python3
"""Report current retained stock-behavior metric and source-contract coverage."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.behavior_coverage import (
    summarize_adjusted_bar_inventory, summarize_coverage,
    summarize_hourly_split_continuity,
)
from equity.behavior_sources import (
    BEHAVIOR_SOURCE_SELECTION_POLICY, OPTIONS_SWING_HYBRID_SOURCE_POLICY,
    source_tail_bars_by_interval,
)
from equity.domain import DecisionWatermark
from equity.repositories import EquityBarRepository, EquityCorporateActionRepository, EquityEvidenceRepository
from options.config import load_option_runtime_configuration


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional JSON report path; omitted means no file write.")
    parser.add_argument("--setup-publications", action="store_true", help="Inspect bounded retained setup publication evidence only.")
    parser.add_argument("--dual-origin-readiness", action="store_true", help="Inspect current O1 activity and S1 setup inventory without writes or detector activation.")
    parser.add_argument("--detector-sources", action="store_true", help="Read and bind the latest complete retained detector cycle; diagnostic only, no writes.")
    parser.add_argument("--technical-replay-preflight", type=date.fromisoformat, metavar="YYYY-MM-DD", help="Read-only strict-as-of technical replay gate for one session; optional new audit file only.")
    parser.add_argument("--detector-schema", action="store_true", help="Read-only migration-053 preflight/postflight; no output file or source scan.")
    parser.add_argument("--publication-limit", type=int, default=12, choices=range(1, 49), metavar="1..48")
    parser.add_argument("--original-setup-ledger", type=Path, help="Compare exact retained publication keys against this SQLite ledger in query-only mode.")
    args = parser.parse_args()
    if args.technical_replay_preflight and (args.detector_sources or args.detector_schema or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--technical-replay-preflight cannot be combined with other reports")
    if args.detector_sources and (args.output or args.detector_schema or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--detector-sources cannot be combined with other reports or output writes")
    if args.detector_schema and (args.output or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--detector-schema cannot be combined with source reports or output writes")
    if args.dual_origin_readiness and (args.output or args.setup_publications or args.original_setup_ledger):
        parser.error("--dual-origin-readiness is read-only and cannot be combined with output or setup options")
    if args.original_setup_ledger and not args.setup_publications:
        parser.error("--original-setup-ledger requires --setup-publications")
    return args


def main() -> int:
    args = parse_args()
    if args.detector_schema:
        import hashlib
        from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

        report = OptionAlertEvaluationRepository().schema_readiness()
        report["migration_sha256"] = hashlib.sha256((BACKEND_DIR / "migrations/053_option_detector_evaluations.sql").read_bytes()).hexdigest()
        print(json.dumps(report, sort_keys=True, indent=2, default=str))
        return 0
    runtime = load_option_runtime_configuration()
    if args.technical_replay_preflight:
        import hashlib
        from options.alert_plans import TECHNICAL_EXIT_POLICY
        from options.dual_origin import TECHNICAL_QUALIFICATION_POLICY
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

        cutoff = datetime.now(timezone.utc)
        inputs = OptionStockBehaviorAssessmentRepository().technical_replay_preflight(configuration=runtime,
            session_date=args.technical_replay_preflight, as_of=cutoff)
        expected = len(runtime.settings.underlyers)
        complete = [row for row in inputs["rows"] if row["matrix_count"] == expected and row["covered"] == expected]
        report = dict(version="option_technical_replay_preflight_v1", status="BLOCKED_NOT_REPLAYED",
            replay_dataset_id=f"options-technical-strict-{args.technical_replay_preflight}-v1",
            session_date=args.technical_replay_preflight.isoformat(), as_of=cutoff.isoformat(),
            evidence_mode="STRICT_HISTORICAL_AS_OF", underlyers=runtime.settings.underlyers,
            configuration_sha256=runtime.configuration_sha256, management_policy_sha256=TECHNICAL_EXIT_POLICY.sha256,
            qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256,
            complete_cycles=len({row["scheduled_cycle"] for row in complete}),
            incomplete_cycles=len({row["scheduled_cycle"] for row in inputs["rows"] if row not in complete}),
            matrices=len(complete), directional_candidates=sum(row["candidate_count"] for row in complete),
            timely_directional_candidates=sum(row["timely_candidates"] for row in complete),
            matrices_with_stock_behavior=sum(row["has_stock_behavior"] for row in complete),
            matrices_with_potential_technical_evidence=sum(row["has_potential_technical_evidence"] for row in complete),
            publications=inputs["publications"], rows=complete,
            remaining_gates=["EXACT_TECHNICAL_SOURCE_POLICY_AND_RAW_PRICE_BINDING", "S1_S2_HISTORICAL_DIRECT_RECEIPTS",
                "PRODUCTION_PACKAGE_SOURCE_ASSEMBLY", "SEPARATE_REPLAY_EVIDENCE_ADMISSION"],
            replay_rows_written=0, forward_activated=False, execution_permission=False)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(json.dumps({**{key: value for key, value in report.items() if key != "rows"},
            "report_sha256": hashlib.sha256(rendered.encode("utf-8")).hexdigest()}, indent=2, sort_keys=True, default=str))
        return 0
    if args.detector_sources:
        from collections import Counter, defaultdict
        from equity.repositories import EquityReferenceRepository
        from options.detector_collection import RetainedDetectorSourceReader
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
        from options.surface_detection import assess_surface_first

        cutoff = datetime.now(timezone.utc)
        repository = OptionStockBehaviorAssessmentRepository()
        cycles = defaultdict(dict)
        for row in repository.completed_matrices(configuration=runtime, as_of=cutoff):
            cycles[row["scheduled_cycle"]][row["underlying"]] = row["matrix_id"]
        complete = [cycle for cycle, matrices in cycles.items() if set(matrices) == set(runtime.settings.underlyers)]
        report = dict(status="NO_COMPLETE_RETAINED_CYCLE", as_of=cutoff, publication_permission=False,
            execution_permission=False, source_policy_approved=False)
        if complete:
            cycle = max(complete)
            sources = RetainedDetectorSourceReader(repository, EquityReferenceRepository(), EquityEvidenceRepository()).read(
                configuration=runtime, scheduled_cycle=cycle, completed_matrices=cycles[cycle], as_of=cutoff)
            observations = [assess_surface_first(**values) for values in sources["surface_inputs"]]
            report.update(status="DIAGNOSTIC_NOT_APPROVED", scheduled_cycle=cycle, received_at=sources["received_at"],
                matrices=len(sources["matrices"]), retained_candidates=len(sources["candidates"]),
                activity_dispositions=dict(Counter(row["finding"].disposition for row in sources["activity"])),
                activity_reasons=dict(Counter(reason for row in sources["activity"] for reason in row["finding"].reasons)),
                surface_dispositions=dict(Counter(row.finding_disposition for row in observations)),
                source_exclusions=sources["rejections"],
                pending=["APPROVED_MANAGEMENT", "TRUSTED_S1_S2_SOURCE_PINS", "PACKAGE_SOURCE_ASSEMBLY", "LIVE_COLLECTION"])
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0
    if args.dual_origin_readiness:
        from equity.stock_alert_results import read_setup_publication_inventory
        from equity.behavior_setup import summarize_setup_publications
        from options.repositories.daily_facts import OptionDailyFactRepository

        cutoff = datetime.now(timezone.utc)
        activity = OptionDailyFactRepository().participation_inventory(configuration=runtime, as_of=cutoff)
        rows, received_at = read_setup_publication_inventory(limit=args.publication_limit)
        setup = summarize_setup_publications(rows, runtime.settings.underlyers, received_at)
        report = dict(status="DIAGNOSTIC_NOT_APPROVED", as_of=cutoff, received_at=received_at,
            universe=runtime.settings.underlyers, activity_matrices=activity,
            setup=dict(status=setup["status"], counts=setup["counts"],
                distinct_hourly_acceptance_episodes=setup["distinct_hourly_acceptance_episodes"],
                missing_hourly_acceptance_tickers=setup["missing_hourly_acceptance_tickers"],
                excluded_record_count=len(setup["excluded_records"])),
            unverified=["OPTION_STOCK_SECURITY_AND_PRICE_BASIS_BINDING", "FRESH_CROSS_MARKET_RECEIPT",
                "TRUSTED_S1_SOURCE_POLICY_APPROVAL", "DUAL_ORIGIN_WRITER_INTEGRATION"],
            source_policy_approved=False, publication_permission=False, execution_permission=False)
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0
    if args.setup_publications:
        from equity.stock_alert_results import read_setup_publication_inventory
        from equity.behavior_setup import summarize_setup_publications

        rows, received_at = read_setup_publication_inventory(limit=args.publication_limit)
        report = summarize_setup_publications(rows, runtime.settings.underlyers, received_at)
        if args.original_setup_ledger and rows:
            from equity.stock_alert_results import read_original_setup_publications
            from equity.behavior_setup import compare_original_setup_publication

            original_policy, originals = read_original_setup_publications(
                args.original_setup_ledger, [row["record_id"] for row in rows],
            )
            report["original_source_comparison"] = [compare_original_setup_publication(
                row, originals.get(row["record_id"]), original_policy,
            ) for row in rows]
        rendered = json.dumps(report, indent=2, sort_keys=True)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    cutoff = datetime.now(timezone.utc)
    watermark = DecisionWatermark(cutoff, cutoff)
    tickers = runtime.settings.underlyers
    intervals = ("1d", "1h", "30m")
    reads = EquityEvidenceRepository().read_behavior_feature_sources(
        tickers, watermark, intervals=intervals,
        maximum_source_bars=max(source_tail_bars_by_interval().values()),
        source_bars_by_interval=source_tail_bars_by_interval(),
    )
    selected_bars = [bar for read in reads for bar in read.bars]
    coverage_rows, actions_by_coverage = ((), {})
    if selected_bars:
        coverage_rows, actions_by_coverage = EquityCorporateActionRepository().read_behavior_split_coverage(
            tickers, watermark,
            window_start=min(bar.session_date for bar in selected_bars),
            window_end=max(bar.session_date for bar in selected_bars),
        )
    received_at = datetime.now(timezone.utc)
    report = summarize_coverage(
        reads, tickers, intervals, watermark, received_at,
        coverage_rows, actions_by_coverage,
    )
    bar_repository = EquityBarRepository()
    adjusted_daily_reads = bar_repository.read_behavior_adjusted_daily(
        tickers, watermark, limit_per_ticker=273,
    )
    raw_grouped_daily_reads = bar_repository.read_behavior_grouped_daily(
        tickers, watermark, adjusted=False, limit_per_ticker=273,
    )
    adjusted_by_interval = {
        "1d": {row.ticker: row.bars for row in adjusted_daily_reads},
        **{interval: bar_repository.list_final_for_tickers_as_of(
            tickers, interval, watermark, limit_per_ticker=limit, adjusted=True,
        ) for interval, limit in source_tail_bars_by_interval().items() if interval != "1d"},
    }
    report["adjusted_bar_inventory"] = summarize_adjusted_bar_inventory(
        reads, adjusted_by_interval, tickers,
        {row.ticker: row.bar_created_ats for row in adjusted_daily_reads}, received_at,
    )
    linked_actions = [action for actions in actions_by_coverage.values() for action in actions]
    report["linked_split_action_count"] = len(linked_actions)
    report["linked_split_action_tickers"] = sorted({action["ticker"] for action in linked_actions})
    report["hybrid_source_policy_sha256"] = OPTIONS_SWING_HYBRID_SOURCE_POLICY.sha256
    report["hourly_split_continuity"] = summarize_hourly_split_continuity(
        reads, raw_grouped_daily_reads, adjusted_daily_reads,
        coverage_rows, actions_by_coverage,
        tickers, received_at,
    )
    report["raw_grouped_daily_tickers"] = sorted(row.ticker for row in raw_grouped_daily_reads)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "STOCK_BEHAVIOR_COVERAGE "
        f"received_at={report['received_at']} "
        f"source_policy={BEHAVIOR_SOURCE_SELECTION_POLICY.sha256} "
        f"hybrid_policy={OPTIONS_SWING_HYBRID_SOURCE_POLICY.sha256} "
        f"expected={report['expected_rows']} observed={report['observed_rows']} "
        f"math={json.dumps(report['mathematical_status_counts'], sort_keys=True)} "
        f"contract={json.dumps(report['contract_status_counts'], sort_keys=True)}"
    )
    print("BLOCKERS " + json.dumps(report["blocker_counts"], sort_keys=True))
    print(
        "SPLIT_COVERAGE "
        f"rows={report['split_coverage_rows']} "
        f"response_bound={report['response_bound_split_coverage_rows']} "
        f"tickers={len(report['response_bound_split_coverage_tickers'])} "
        f"window={report['split_coverage_window_start']}..{report['split_coverage_window_end']} "
        f"bar_modes={json.dumps(report['feature_bar_mode_counts'], sort_keys=True)} "
        f"coverage_modes={json.dumps(report['response_bound_split_coverage_mode_counts'], sort_keys=True)}"
    )
    print(
        "INTERVALS "
        f"contract={json.dumps(report['contract_status_counts_by_interval'], sort_keys=True)} "
        f"windows={json.dumps(report['source_windows_by_interval'], sort_keys=True)} "
        f"blockers={json.dumps(report['blocker_counts_by_interval'], sort_keys=True)}"
    )
    print(
        "ADJUSTED_INVENTORY "
        f"bars={json.dumps(report['adjusted_bar_inventory']['bar_counts_by_interval'], sort_keys=True)} "
        f"math_ready_names={json.dumps(report['adjusted_bar_inventory']['mathematical_history_ready_counts_by_interval'], sort_keys=True)} "
        f"derived_candidates={json.dumps(report['adjusted_bar_inventory']['derived_evidence_candidate_counts_by_interval'], sort_keys=True)} "
        f"ready_tickers={json.dumps(report['adjusted_bar_inventory']['mathematical_history_ready_tickers_by_interval'], sort_keys=True)} "
        f"unready_tickers={json.dumps(report['adjusted_bar_inventory']['mathematical_history_unready_tickers_by_interval'], sort_keys=True)} "
        f"prospective_ready={report['adjusted_bar_inventory']['prospective_behavior_contract_ready']} "
        f"blockers={json.dumps(report['adjusted_bar_inventory']['blocker_counts'], sort_keys=True)} "
        f"linked_split_actions={report['linked_split_action_count']} "
        f"split_tickers={json.dumps(report['linked_split_action_tickers'])}"
    )
    print(
        "HOURLY_SPLIT_CONTINUITY "
        f"status={json.dumps(report['hourly_split_continuity']['status_counts'], sort_keys=True)} "
        f"candidates={json.dumps(report['hourly_split_continuity']['candidate_tickers'])} "
        f"unavailable={json.dumps(report['hourly_split_continuity']['unavailable_tickers'])} "
        f"blockers={json.dumps(report['hourly_split_continuity']['blocker_counts'], sort_keys=True)}"
    )
    print(f"RAW_GROUPED_DAILY tickers={json.dumps(report['raw_grouped_daily_tickers'])}")
    print(
        "HOURLY_CONTINUITY_EXCEPTIONS " + json.dumps([
            row for row in report["hourly_split_continuity"]["rows"]
            if row["status"] == "UNAVAILABLE"
        ], sort_keys=True)
    )
    if args.output is not None:
        print(f"REPORT_WRITTEN path={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())