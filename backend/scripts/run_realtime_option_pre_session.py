#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from math import isfinite
from pathlib import Path
from typing import Callable

from dotenv import dotenv_values, load_dotenv
import psycopg2
from psycopg2 import pool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_developer_policy, load_option_runtime_configuration, load_valuation_policy
from realtime.forward_launcher import DeveloperForwardEnrollmentLauncher
from realtime.forward_storage import encode_forward_artifact
from realtime.contracts import CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE, UniverseManifest, canonical_sha256
from realtime.option_pre_session_producer import OptionPreSessionProducerResult
from realtime.option_manifest_cohort_repository import DeveloperForwardCohortBinding
from realtime.option_session_manifest import BoundedOptionManifestPolicy
from realtime.preflight import DetectorFamily


PROCESS_PLAN_VERSION = "realtime_option_pre_session_process_v2"
MAXIMUM_DEVELOPER_PROVIDER_REQUESTS = 320
MAXIMUM_DEVELOPER_RESPONSE_BYTES = 8 * 67_108_864


def _execution_environment() -> dict[str, str]:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    environ = dict(os.environ)
    environ.pop("OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE", None)
    environ.update({
        "OPTION_DATA_ENGINE": "polygon_developer",
        "OPTION_EQUITY_CONTEXT_ENABLED": "false",
        "OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED": "false",
        "OPTION_PACKAGE_ASSESSMENTS_ENABLED": "false",
        "OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED": "false",
        "OPTION_EXECUTION_ENGINE": "paper_proxy",
        "OPTION_UNIVERSE_MODE": "fixed",
        "OPTION_FIXED_STOCK_UNDERLYERS": "AAPL,MSFT,NVDA,AMZN,TSLA",
        "OPTION_FIXED_ETF_UNDERLYERS": "SPY,QQQ,IWM",
        "OPTION_STOCK_UNIVERSE_SIZE": "5",
        "OPTION_ETF_UNIVERSE_SIZE": "3",
        "OPTION_RAW_ARCHIVE_ENABLED": "false",
        "OPTION_START_READ_ONLY": "true",
        "OPTION_TRADE_INGESTION_ENABLED": "false",
    })
    return environ


def _production_runtime_factory(process_plan, args):
    from realtime.option_pre_session_runtime import build_option_pre_session_runtime

    return build_option_pre_session_runtime(
        process_plan,
        scheduled_cycle=args.scheduled_cycle,
        effective_at=args.effective_at,
        environ=_execution_environment(),
        backend_dir=BACKEND_DIR,
        pool_factory=pool.SimpleConnectionPool,
    )


RUNTIME_FACTORY: Callable | None = _production_runtime_factory


def _policy_environment() -> dict[str, str]:
    values = dotenv_values(BACKEND_DIR / ".env")
    return {
        name: str(values[name])
        for name in ("OPTION_POLICY_FILE", "OPTION_VALUATION_POLICY_FILE")
        if values.get(name)
    }


def _reviewed_policy_path(
    environ: dict[str, str],
    name: str,
    default: str,
) -> Path:
    value = Path(str(environ.get(name) or default))
    resolved = value.resolve() if value.is_absolute() else (BACKEND_DIR / value).resolve()
    policy_root = (BACKEND_DIR / "options" / "policies").resolve()
    if not resolved.is_relative_to(policy_root):
        raise ValueError(f"{name} must remain under options/policies")
    return resolved


@dataclass(frozen=True, slots=True)
class OptionPreSessionProcessPlan:
    version: str
    database: str
    universe_sha256: str
    option_underlyings: tuple[str, ...]
    discovery_policy_sha256: str
    valuation_policy_sha256: str
    manifest_policy: BoundedOptionManifestPolicy
    maximum_provider_requests: int
    maximum_response_bytes: int
    reference_role: str
    control_role: str
    universe: UniverseManifest = CANARY_UNIVERSE
    cohort_binding: DeveloperForwardCohortBinding | None = None

    @property
    def sha256(self) -> str:
        payload = {
            "version": self.version,
            "database": self.database,
            "universe_sha256": self.universe_sha256,
            "option_underlyings": self.option_underlyings,
            "discovery_policy_sha256": self.discovery_policy_sha256,
            "valuation_policy_sha256": self.valuation_policy_sha256,
            "manifest_policy_sha256": self.manifest_policy.sha256,
            "maximum_provider_requests": self.maximum_provider_requests,
            "maximum_response_bytes": self.maximum_response_bytes,
            "reference_role": self.reference_role,
            "control_role": self.control_role,
        }
        if self.cohort_binding is not None:
            payload["cohort_binding_sha256"] = canonical_sha256(self.cohort_binding)
        return canonical_sha256(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or run one bounded realtime option pre-session pass."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--scheduled-cycle", type=datetime.fromisoformat)
    parser.add_argument("--effective-at", type=datetime.fromisoformat)
    parser.add_argument("--forward-nine", action="store_true")
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--deadline", type=datetime.fromisoformat)
    parser.add_argument("--maximum-requests", type=int)
    parser.add_argument("--maximum-response-bytes", type=int)
    parser.add_argument("--developer-policy")
    parser.add_argument("--valuation-policy")
    parser.add_argument("--strategy-policy")
    parser.add_argument("--research-variant", choices=("zero-yield-research-v1",))
    parser.add_argument("--enrollment-output", type=Path)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--server-max-connections", type=int)
    parser.add_argument("--reserved-connections", type=int)
    parser.add_argument("--maximum-target-dte-deviation", type=int, default=0)
    parser.add_argument("--target-dtes", type=int, nargs="+", default=(7, 21, 45, 60))
    args = parser.parse_args()
    if (
        args.database != "stocks_realtime_v1"
        or args.confirm_database_name != args.database
    ):
        parser.error("pre-session task requires exact stocks_realtime_v1 confirmation")
    args.target_dtes = tuple(args.target_dtes)
    if not args.forward_nine and (args.maximum_target_dte_deviation != 0 or args.target_dtes != (7, 21, 45, 60)):
        parser.error("modified expiry allocation requires --forward-nine")
    if args.forward_nine:
        required = (args.session_date, args.deadline, args.maximum_requests, args.maximum_response_bytes,
            args.developer_policy, args.valuation_policy, args.strategy_policy)
        if any(value is None for value in required):
            parser.error("--forward-nine requires explicit policies, budgets, session/deadline and valuation inputs")
        if args.execute and args.research_variant is None:
            parser.error("new nine-name execution requires an explicit research variant")
        if args.execute and any(value is None for value in (args.enrollment_output, args.report_path,
                args.server_max_connections, args.reserved_connections)):
            parser.error("nine-name execution requires new enrollment/report paths and explicit connection ceilings")
    elif args.execute and (args.scheduled_cycle is None or args.effective_at is None):
        parser.error("--execute requires --scheduled-cycle and --effective-at")
    return args


def build_process_plan(
    database: str,
    *,
    environ: dict[str, str] | None = None,
    universe: UniverseManifest = CANARY_UNIVERSE,
    maximum_provider_requests: int = MAXIMUM_DEVELOPER_PROVIDER_REQUESTS,
    maximum_response_bytes: int = MAXIMUM_DEVELOPER_RESPONSE_BYTES,
    maximum_target_dte_deviation: int = 0,
    target_dtes: tuple[int, ...] = (7, 21, 45, 60),
    reviewed_forward: bool = False,
) -> OptionPreSessionProcessPlan:
    if (database != "stocks_realtime_v1" or not isinstance(universe, UniverseManifest)
            or universe not in (CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE) and not reviewed_forward):
        raise ValueError("pre-session plan requires the exact target and a reviewed universe")
    if (type(maximum_target_dte_deviation) is not int or not 0 <= maximum_target_dte_deviation <= 4
            or target_dtes not in ((7, 21, 45), (7, 21, 45, 60))
            or (not reviewed_forward and universe != DEVELOPER_FORWARD_UNIVERSE
                and (maximum_target_dte_deviation != 0 or target_dtes != (7, 21, 45, 60)))):
        raise ValueError("bounded expiry deviation is limited to 0-4 days and reviewed forward allocations")
    maximum_request_limit = 100_000 if reviewed_forward else 1_000
    if (type(maximum_provider_requests) is not int or not 1 <= maximum_provider_requests <= maximum_request_limit
            or type(maximum_response_bytes) is not int or maximum_response_bytes <= 0):
        raise ValueError("pre-session plan requires bounded explicit source budgets")
    policy_environ = _policy_environment() if environ is None else dict(environ)
    discovery = load_developer_policy(
        _reviewed_policy_path(
            policy_environ,
            "OPTION_POLICY_FILE",
            "options/policies/developer_v1.json",
        )
    )
    valuation = load_valuation_policy(
        _reviewed_policy_path(
            policy_environ,
            "OPTION_VALUATION_POLICY_FILE",
            "options/policies/valuation_v1.json",
        )
    )
    contract_filter = discovery.policy.contract_filter
    manifest_policy = BoundedOptionManifestPolicy(
        version=("developer-forward-three-lane-v1" if target_dtes == (7, 21, 45)
            else "developer-forward-four-lane-v1" if reviewed_forward
            else "developer-canary-strategy-coverage-v1"),
        target_dtes=target_dtes,
        maximum_target_dte_deviation=maximum_target_dte_deviation,
        strikes_per_side_per_expiry=2,
        minimum_dte=max(1, contract_filter.minimum_dte),
        maximum_dte=contract_filter.maximum_dte,
        strike_corridor_fraction=contract_filter.strike_corridor_fraction,
        minimum_day_volume=contract_filter.minimum_day_volume,
        minimum_open_interest=contract_filter.minimum_open_interest,
        liquidity_rule=contract_filter.liquidity_rule,
        maximum_contracts_per_underlying=len(target_dtes) * 4,
        maximum_option_quote_contracts=1_000,
        reserved_option_quote_contracts=200,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O3,
        ),
        unavailable_detectors=(DetectorFamily.O2,),
    )
    return OptionPreSessionProcessPlan(
        version=PROCESS_PLAN_VERSION,
        database=database,
        universe_sha256=universe.sha256,
        option_underlyings=universe.option_underlyings,
        discovery_policy_sha256=discovery.sha256,
        valuation_policy_sha256=valuation.policy_sha256,
        manifest_policy=manifest_policy,
        maximum_provider_requests=maximum_provider_requests,
        maximum_response_bytes=maximum_response_bytes,
        reference_role="REFERENCES",
        control_role="CONTROL",
        universe=universe,
    )


def build_developer_forward_process_plan(database, *, environ, configuration, maximum_provider_requests, maximum_response_bytes,
    maximum_target_dte_deviation=0, target_dtes=(7, 21, 45, 60),
    universe=DEVELOPER_FORWARD_UNIVERSE, staged=False):
    maximum_bytes = 68_719_476_736 if staged else 67_108_864
    if (type(maximum_response_bytes) is not int or not 1 <= maximum_response_bytes <= maximum_bytes
            or len(universe.option_underlyings) > 1_000):
        raise ValueError("Developer preparation requires a reviewed bounded staged/one-shot scope")
    plan = build_process_plan(database, environ=environ, universe=universe, reviewed_forward=True,
        maximum_provider_requests=maximum_provider_requests, maximum_response_bytes=maximum_response_bytes,
        maximum_target_dte_deviation=maximum_target_dte_deviation, target_dtes=target_dtes)
    if (plan.discovery_policy_sha256 != configuration.developer_policy_sha256
            or plan.valuation_policy_sha256 != configuration.valuation_policy_sha256):
        raise ValueError("forward preparation and reviewed configuration policy identities differ")
    return replace(plan, cohort_binding=DeveloperForwardCohortBinding.from_configuration(configuration, universe))


def _plan_report(plan: OptionPreSessionProcessPlan) -> dict[str, object]:
    return {
        "status": "PLAN",
        "version": plan.version,
        "process_plan_sha256": plan.sha256,
        "database": plan.database,
        "provider_tier": "MASSIVE_DEVELOPER",
        "universe_sha256": plan.universe_sha256,
        "option_underlyings": plan.option_underlyings,
        "option_underlying_count": len(plan.option_underlyings),
        "target_dtes": plan.manifest_policy.target_dtes,
        "maximum_target_dte_deviation": plan.manifest_policy.maximum_target_dte_deviation,
        "minimum_day_volume": plan.manifest_policy.minimum_day_volume,
        "minimum_open_interest": plan.manifest_policy.minimum_open_interest,
        "liquidity_rule": plan.manifest_policy.liquidity_rule,
        "maximum_contracts_per_underlying": (
            plan.manifest_policy.maximum_contracts_per_underlying
        ),
        "planned_primary_contracts": (
            len(plan.option_underlyings)
            * plan.manifest_policy.maximum_contracts_per_underlying
        ),
        "reserved_option_quote_contracts": (
            plan.manifest_policy.reserved_option_quote_contracts
        ),
        "maximum_option_quote_contracts": (
            plan.manifest_policy.maximum_option_quote_contracts
        ),
        "maximum_provider_requests": plan.maximum_provider_requests,
        "maximum_response_bytes": plan.maximum_response_bytes,
        "database_roles": (plan.reference_role, plan.control_role),
        "profile_identity": "TARGET_REQUIRED_AT_EXECUTION",
        "database_writes": False,
        "provider_requests": 0,
        "canonical_fallback": False,
        "runtime_factory_installed": RUNTIME_FACTORY is not None,
    }


def _result_report(
    process_plan: OptionPreSessionProcessPlan,
    result: OptionPreSessionProducerResult,
) -> dict[str, object]:
    plan = result.plan
    return {
        "status": plan.status.value,
        "version": process_plan.version,
        "process_plan_sha256": process_plan.sha256,
        "selection_sha256": plan.selection_sha256,
        "manifest_sha256": plan.manifest.sha256 if plan.manifest is not None else None,
        "provider_tier": plan.scope.provider_tier.value,
        "universe_sha256": plan.scope.universe.sha256,
        "discovery_batch_count": len(result.discovery_batch_ids),
        "selected_contract_count": plan.selected_contract_count,
        "disposition_count": len(plan.dispositions),
        "coverage_count": len(plan.coverage),
        "reason_codes": plan.reason_codes,
        "provider_requests": result.provider_requests,
        "response_bytes": result.response_bytes,
        "database_writes": True,
        "canonical_fallback": False,
    "secrets_exposed": False,
    }


def _forward_nine_environment(args, *, execute):
    if execute:
        load_dotenv(args.env_file, override=True)
    environment = dict(os.environ) if execute else {}
    if not execute:
        environment["POLYGON_API_KEY"] = ""
    environment.pop("OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE", None)
    environment.update(OPTION_DATA_ENGINE="polygon_developer", OPTION_UNIVERSE_MODE="fixed",
        OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMZN,MSFT,NFLX,NVDA,TSLA", OPTION_FIXED_ETF_UNDERLYERS="SPY,QQQ,IWM",
        OPTION_STOCK_UNIVERSE_SIZE="6", OPTION_ETF_UNIVERSE_SIZE="3", OPTION_START_READ_ONLY="true",
        OPTION_TRADE_INGESTION_ENABLED="false", OPTION_RAW_ARCHIVE_ENABLED="false",
        OPTION_EQUITY_CONTEXT_ENABLED="false", OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED="false",
        OPTION_PACKAGE_ASSESSMENTS_ENABLED="false", OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED="false",
        OPTION_EXECUTION_ENGINE="paper_proxy", OPTION_RISK_FREE_RATE_SOURCE="us_treasury_par_yield_v1",
        OPTION_DEFAULT_DIVIDEND_YIELD="0")
    environment.pop("OPTION_DIVIDEND_INPUT_SOURCE", None)
    variant = getattr(args, "research_variant", None)
    if variant is not None:
        if (variant != "zero-yield-research-v1"
                or Path(args.valuation_policy).name != "valuation_zero_yield_research_v1.json"):
            raise ValueError("zero-yield research requires its separately identified valuation policy")
        environment["OPTION_VALIDATION_RESEARCH_VARIANT"] = variant
    for name, value in (("OPTION_POLICY_FILE", args.developer_policy),
            ("OPTION_VALUATION_POLICY_FILE", args.valuation_policy), ("OPTION_STRATEGY_POLICY_FILE", args.strategy_policy)):
        environment[name] = str(_reviewed_policy_path({name: value}, name, value))
    return environment


def _run_forward_nine(args):
    environment = _forward_nine_environment(args, execute=False)
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    if args.deadline.utcoffset() is None:
        raise ValueError("nine-name preparation requires an aware deadline")
    process_plan = build_developer_forward_process_plan(args.database, environ=environment, configuration=configuration,
        maximum_provider_requests=args.maximum_requests, maximum_response_bytes=args.maximum_response_bytes,
        maximum_target_dte_deviation=args.maximum_target_dte_deviation, target_dtes=args.target_dtes)
    report = dict(_plan_report(process_plan), mode="EXECUTE" if args.execute else "PLAN",
        research_variant=getattr(args, "research_variant", None),
        valuation_assumption="EXPLICIT_ZERO_YIELD_RESEARCH" if getattr(args, "research_variant", None) else "LEGACY_UNLABELED",
        configuration_sha256=configuration.configuration_sha256, strategy_policy_sha256=configuration.strategy_policy_sha256,
        valuation_policy_sha256=configuration.valuation_policy_sha256,
        valuation_inputs_sha256=canonical_sha256(dict(
            risk_free_rate_source=configuration.settings.risk_free_rate_source,
            dividend_input_source=configuration.settings.dividend_input_source,
            maximum_age_seconds=configuration.settings.dividend_input_max_age_seconds)),
        session_date=args.session_date.isoformat(), deadline=args.deadline.isoformat(),
        data_ready=False, starts_services=False, unlocks_principals=False)
    if not args.execute:
        print(json.dumps(report, indent=2, sort_keys=True))
        print("No pools, HTTP clients, provider requests, or database writes constructed.")
        return 0
    for path in (args.enrollment_output, args.report_path):
        if path.exists() or not path.resolve().is_relative_to(BACKEND_DIR.parent):
            raise ValueError("nine-name execution requires new workspace-local evidence paths")
    if args.enrollment_output.resolve() == args.report_path.resolve():
        raise ValueError("enrollment and lifecycle reports must be distinct")
    from scripts.realtime_script_support import write_report as _write_report

    started_at = datetime.now(timezone.utc)
    _write_report(args.report_path, dict(report, state="RUNNING", started_at=started_at.isoformat()))
    launcher = None
    try:
        execution_environment = _forward_nine_environment(args, execute=True)
        launcher = DeveloperForwardEnrollmentLauncher(process_plan, configuration, environ=execution_environment,
            backend_dir=BACKEND_DIR, session_date=args.session_date, deadline=args.deadline,
            pool_factory=pool.SimpleConnectionPool, clock=lambda: datetime.now(timezone.utc), wait=time.sleep,
            server_max_connections=args.server_max_connections, reserved_connections=args.reserved_connections)
        receipt = launcher.run_once()
        if receipt.process_plan_sha256 != process_plan.sha256 or receipt.cohort_binding != process_plan.cohort_binding:
            raise ValueError("nine-name execution receipt changed reviewed process/configuration identities")
        payload, digest = encode_forward_artifact(receipt)
        _write_report(args.enrollment_output, dict(payload_text=payload, payload_sha256=digest))
        report.update(_result_report(process_plan, receipt.result), cohort_id=str(receipt.cohort_id) if receipt.cohort_id else None,
            catalog_contract_count=len(receipt.result.catalog), state="COMPLETE", started_at=started_at.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(), data_ready=False)
        _write_report(args.report_path, report)
    except Exception as error:
        reason = str(error)[:240] if isinstance(error, ValueError) else None
        database_error = {}
        if isinstance(error, psycopg2.Error):
            database_error = dict(database_sqlstate=error.pgcode,
                database_constraint=error.diag.constraint_name,
                database_table=error.diag.table_name)
        _write_report(args.report_path, dict(report, state="FAILED", error_type=type(error).__name__,
            error_reason=reason, provider_requests=getattr(launcher, "last_provider_requests", 0),
            response_bytes=getattr(launcher, "last_response_bytes", 0),
            database_write_state="UNKNOWN_INSPECT_BEFORE_RETRY", started_at=started_at.isoformat(),
            **database_error))
        raise RuntimeError(f"nine-name preparation failed: {type(error).__name__}; inspect its report before retrying") from None
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def main() -> int:
    args = parse_args()
    if args.forward_nine:
        return _run_forward_nine(args)
    process_plan = build_process_plan(
        args.database,
        environ=(
            _execution_environment()
            if args.execute
            else _policy_environment()
        ),
    )
    if not args.execute:
        print(json.dumps(_plan_report(process_plan), indent=2, sort_keys=True))
        print("No pools, HTTP clients, provider requests, or database writes constructed.")
        return 0
    if RUNTIME_FACTORY is None:
        raise RuntimeError(
            "pre-session execution runtime is not installed; provider/database action remains blocked"
        )
    runtime = RUNTIME_FACTORY(process_plan, args)
    try:
        result = runtime.runner.run_once(runtime.target_plan)
        if not isinstance(result, OptionPreSessionProducerResult):
            raise TypeError("pre-session runtime returned an invalid producer result")
        if (
            result.plan.scope.universe.sha256 != process_plan.universe_sha256
            or result.plan.policy.sha256 != process_plan.manifest_policy.sha256
        ):
            raise ValueError("pre-session runtime result changed the reviewed process plan")
        if (
            result.provider_requests > process_plan.maximum_provider_requests
            or result.response_bytes > process_plan.maximum_response_bytes
        ):
            raise ValueError("pre-session runtime result exceeded the process source budget")
        print(json.dumps(_result_report(process_plan, result), indent=2, sort_keys=True))
    finally:
        runtime.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
