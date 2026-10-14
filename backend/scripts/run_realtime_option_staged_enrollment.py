from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from uuid import UUID, uuid4

from dotenv import load_dotenv
from psycopg2 import pool


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from realtime.contracts import UniverseManifest
from realtime.forward_capacity import build_developer_forward_capacity_plan
from realtime.forward_launcher import DeveloperForwardEnrollmentReceipt
from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler
from realtime.forward_storage import decode_forward_artifact, encode_forward_artifact
from realtime.option_manifest_cohort_repository import DeveloperForwardCohortBinding
from realtime.option_pre_session_runtime import build_option_pre_session_runtime
from realtime.option_pre_session_staged_runner import (
    PreparedStagedOptionEnrollment, StagedTargetOptionPreSessionRunner,
)
from realtime.option_pre_session_staging import StagedAccountScheduler
from scripts.run_realtime_option_pre_session import build_developer_forward_process_plan
from scripts.realtime_script_support import write_report as _write_report


DEFAULT_UNIVERSE = BACKEND_DIR / "realtime" / "manifests" / "developer_forward_nine_v1.json"


def _load_universe(path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get("universe", payload)
    if set(value) != {"version", "equity_tickers", "option_underlyings"}:
        raise ValueError("staged universe manifest keys changed")
    return UniverseManifest(value["version"], tuple(value["equity_tickers"]), tuple(value["option_underlyings"]))


def _environment(args, universe, *, execute):
    if execute:
        load_dotenv(args.env_file, override=True)
    environment = dict(os.environ) if execute else {"POLYGON_API_KEY": ""}
    etfs = tuple(ticker for ticker in universe.equity_tickers if ticker in {"SPY", "QQQ", "IWM"})
    stocks = tuple(ticker for ticker in universe.equity_tickers if ticker not in etfs)
    environment.update(OPTION_DATA_ENGINE="polygon_developer", OPTION_UNIVERSE_MODE="fixed",
        OPTION_FIXED_STOCK_UNDERLYERS=",".join(stocks), OPTION_FIXED_ETF_UNDERLYERS=",".join(etfs),
        OPTION_STOCK_UNIVERSE_SIZE=str(len(stocks)), OPTION_ETF_UNIVERSE_SIZE=str(len(etfs)),
        OPTION_START_READ_ONLY="true", OPTION_TRADE_INGESTION_ENABLED="false",
        OPTION_POLICY_FILE=args.developer_policy, OPTION_VALUATION_POLICY_FILE=args.valuation_policy,
        OPTION_STRATEGY_POLICY_FILE=args.strategy_policy,
        OPTION_RISK_FREE_RATE_SOURCE="us_treasury_par_yield_v1",
        OPTION_DEFAULT_DIVIDEND_YIELD="0")
    environment.pop("OPTION_DIVIDEND_INPUT_SOURCE", None)
    return environment


def _load_prepared(path):
    envelope = json.loads(path.read_text(encoding="utf-8"))
    value = decode_forward_artifact(envelope["payload_text"], envelope["payload_sha256"])
    if not isinstance(value, PreparedStagedOptionEnrollment):
        raise ValueError("staged run file type drift")
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Prepare/acquire/finalize scalable per-underlying option enrollment.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--prepare", action="store_true")
    mode.add_argument("--acquire", action="store_true")
    mode.add_argument("--finalize", action="store_true")
    parser.add_argument("--database", default="stocks_realtime_v1")
    parser.add_argument("--confirm-database-name", default="stocks_realtime_v1")
    parser.add_argument("--universe-file", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--deadline", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-requests", type=int, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--package-maximum-requests", type=int, default=120)
    parser.add_argument("--package-maximum-response-bytes", type=int, default=67_108_864)
    parser.add_argument("--package-workers", type=int, default=4)
    parser.add_argument("--developer-policy", default="options/policies/developer_forward_liquidity_v2.json")
    parser.add_argument("--valuation-policy", default="options/policies/valuation_raw_spot_v2.json")
    parser.add_argument("--strategy-policy", default="options/policies/strategy_technical_forward_v2.json")
    parser.add_argument("--target-dtes", type=int, nargs="+", default=(7, 21, 45))
    parser.add_argument("--maximum-target-dte-deviation", type=int, default=4)
    parser.add_argument("--staged-run-file", type=Path)
    parser.add_argument("--staged-run-output", type=Path)
    parser.add_argument("--enrollment-output", type=Path)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--underlyer")
    parser.add_argument("--owner-id", type=UUID, default=uuid4())
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.database != "stocks_realtime_v1" or args.confirm_database_name != args.database:
        parser.error("staged enrollment requires exact stocks_realtime_v1 confirmation")
    args.target_dtes = tuple(args.target_dtes)
    if args.deadline.utcoffset() is None:
        parser.error("staged enrollment deadline must be timezone-aware")
    if args.execute and args.report_path is None:
        parser.error("staged execution requires a new report path")
    if args.prepare and args.execute and args.staged_run_output is None:
        parser.error("prepare execution requires staged-run-output")
    if (args.acquire or args.finalize) and args.staged_run_file is None:
        parser.error("acquire/finalize require staged-run-file")
    if args.finalize and args.execute and args.enrollment_output is None:
        parser.error("finalize execution requires enrollment-output")
    return args


def _process(args, universe, *, execute):
    environment = _environment(args, universe, execute=execute)
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    plan = build_developer_forward_process_plan(args.database, environ=environment,
        configuration=configuration, maximum_provider_requests=args.maximum_requests,
        maximum_response_bytes=args.maximum_response_bytes,
        maximum_target_dte_deviation=args.maximum_target_dte_deviation,
        target_dtes=args.target_dtes, universe=universe, staged=True)
    return environment, configuration, plan


def _runtime(args, environment, plan, prepared=None):
    clock = lambda: datetime.now(timezone.utc)
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(
        maximum_requests=args.package_maximum_requests,
        maximum_response_bytes=args.package_maximum_response_bytes,
        maximum_attempts=3), clock=clock, wait=time.sleep)
    scheduled = prepared.target_plan.scheduled_cycle if prepared else clock()
    effective = prepared.target_plan.effective_at if prepared else scheduled
    runtime = build_option_pre_session_runtime(plan, scheduled_cycle=scheduled,
        effective_at=effective, environ=environment, backend_dir=BACKEND_DIR,
        pool_factory=pool.SimpleConnectionPool, clock=clock,
        request_scheduler=scheduler, source_deadline=prepared.collection_deadline if prepared else args.deadline,
        prepared_target_plan=prepared.target_plan if prepared else None)
    runner = StagedTargetOptionPreSessionRunner(runtime.runner.references_database,
        runtime.runner.control_database, runtime.runner.source, clock=clock)
    if prepared is not None:
        scheduler = StagedAccountScheduler(runtime.runner.references_database,
            prepared.run_id, DeveloperForwardBudget(
                maximum_requests=args.package_maximum_requests,
                maximum_response_bytes=args.package_maximum_response_bytes,
                maximum_attempts=3), clock=clock, wait=time.sleep)
        runtime.runner.source.request_transport.scheduler = scheduler
    return runtime, runner, scheduler


def _finalization_result(receipt, result_value):
    if receipt.cohort_id is None:
        return {"status": "STAGED_SELECTION_BLOCKED", "cohort_id": None,
            "selection_id": str(result_value.selection_id),
            "reason_codes": list(result_value.plan.reason_codes),
            "selected_contract_count": result_value.plan.selected_contract_count}
    return {"status": "STAGED_ENROLLMENT_FINALIZED", "cohort_id": str(receipt.cohort_id),
        "selection_id": str(result_value.selection_id),
        "reason_codes": [],
        "selected_contract_count": result_value.plan.selected_contract_count}


def main(argv=None):
    args = parse_args(argv)
    universe = _load_universe(args.universe_file)
    environment, configuration, process_plan = _process(args, universe, execute=False)
    capacity = build_developer_forward_capacity_plan(universe,
        package_workers=args.package_workers,
        selected_slots_per_underlying=process_plan.manifest_policy.maximum_contracts_per_underlying)
    report = {"mode": "EXECUTE" if args.execute else "PLAN", "database": args.database,
        "universe_sha256": universe.sha256, "equity_count": len(universe.equity_tickers),
        "option_underlying_count": len(universe.option_underlyings),
        "process_plan_sha256": process_plan.sha256, "configuration_sha256": configuration.configuration_sha256,
        "maximum_requests": args.maximum_requests, "maximum_response_bytes": args.maximum_response_bytes,
        "package_maximum_requests": args.package_maximum_requests,
        "package_maximum_response_bytes": args.package_maximum_response_bytes,
        "package_workers": capacity.package_workers,
        "selected_slot_count": capacity.selected_slot_count,
        "detector_shard_count": len(capacity.detector_shards),
        "detector_shards": [list(row.underlyings) for row in capacity.detector_shards],
        "capacity_sha256": capacity.capacity_sha256,
        "target_dtes": args.target_dtes, "maximum_target_dte_deviation": args.maximum_target_dte_deviation,
        "dividend_input_source": configuration.settings.dividend_input_source,
        "default_dividend_yield": str(configuration.settings.default_dividend_yield),
        "migration_required": "019_realtime_option_pre_session_staging",
        "provider_requests": 0, "database_writes": False, "values_exposed": False}
    if not args.execute:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    for path in (args.report_path, args.staged_run_output if args.prepare else None,
            args.enrollment_output if args.finalize else None):
        if path is not None and (path.exists() or not path.resolve().is_relative_to(WORKSPACE_DIR)):
            raise ValueError("staged enrollment requires new workspace-local evidence paths")
    environment, execution_configuration, execution_plan = _process(args, universe, execute=True)
    if execution_plan.sha256 != process_plan.sha256:
        raise ValueError("staged execution process plan drift")
    prepared = _load_prepared(args.staged_run_file) if args.staged_run_file else None
    if prepared and (prepared.process_plan_sha256 != process_plan.sha256
            or prepared.configuration_sha256 != execution_configuration.configuration_sha256
            or prepared.collection_deadline != args.deadline):
        raise ValueError("staged run/process/configuration/deadline drift")
    runtime = None
    try:
        runtime, runner, scheduler = _runtime(args, environment, execution_plan, prepared)
        if args.prepare:
            run_id = runner.prepare(runtime.target_plan, collection_deadline=args.deadline)
            prepared = PreparedStagedOptionEnrollment(process_plan.sha256,
                execution_configuration.configuration_sha256, run_id, runtime.target_plan,
                args.deadline, datetime.now(timezone.utc))
            payload, digest = encode_forward_artifact(prepared)
            _write_report(args.staged_run_output, {"payload_text": payload, "payload_sha256": digest})
            result = {"status": "STAGED_RUN_PREPARED", "run_id": str(run_id)}
        elif args.acquire:
            receipt = runner.acquire_one(runtime.target_plan, args.owner_id,
                underlyer=args.underlyer.upper() if args.underlyer else None)
            result = {"status": "PACKAGE_COMPLETE" if receipt else "NO_PENDING_PACKAGE",
                "underlyer": receipt.underlyer if receipt else None}
        elif args.finalize:
            result_value = runner.finalize(runtime.target_plan,
                cohort_binding=execution_plan.cohort_binding)
            receipt = DeveloperForwardEnrollmentReceipt(process_plan.sha256, result_value,
                execution_plan.cohort_binding, runtime.target_plan.effective_at)
            result = _finalization_result(receipt, result_value)
            if receipt.cohort_id is not None:
                payload, digest = encode_forward_artifact(receipt)
                _write_report(args.enrollment_output, {"payload_text": payload, "payload_sha256": digest})
        else:
            raise ValueError("staged execution requires prepare, acquire or finalize")
        report.update(result, state="COMPLETE", provider_requests=scheduler.requests,
            response_bytes=scheduler.response_bytes, database_writes=True)
        _write_report(args.report_path, report)
    except Exception as error:
        _write_report(args.report_path, {**report, "state": "FAILED", "error_type": type(error).__name__,
            "error_reason": str(error)[:240] if isinstance(error, ValueError) else None,
            "database_write_state": "UNKNOWN_INSPECT_BEFORE_RETRY"})
        raise
    finally:
        if runtime is not None:
            runtime.close()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())