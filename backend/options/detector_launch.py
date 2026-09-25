from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import time
from typing import ClassVar, Literal

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from equity.behavior_setup import DirectSetupSourcePolicy, DirectResumptionSourcePolicy
from options.alert_plans import TECHNICAL_EXIT_POLICY
from options.dual_origin import TECHNICAL_QUALIFICATION_POLICY


LOGGER = logging.getLogger(__name__)
RUNTIME_FILES = (
    "options/alert_plans.py", "options/alert_qualification.py", "options/analytics/alert_selection.py",
    "options/analytics/marks.py", "options/config.py", "options/data/polygon_developer.py",
    "options/detector_collection.py", "options/detector_launch.py", "options/dual_origin.py",
    "options/orchestration.py", "options/repositories/alert_evaluations.py",
    "options/repositories/stock_behavior_assessments.py", "options/stock_setup_binding.py",
    "options/strategies/engine.py", "options/surface_detection.py", "options/worker.py", "equity/stock_alert_results.py",
    "equity/polygon.py", "equity/materialization.py", "equity/setup_composition.py",
    "scripts/run_option_worker.py",
)
INTRADAY_RUNTIME_FILES = (*RUNTIME_FILES, "options/intraday_participation.py", "equity/behavior.py",
    "equity/behavior_sources.py", "options/stock_behavior_gates.py")
OPTIMIZED_RUNTIME_FILES = (*RUNTIME_FILES, "options/outcome_service.py", "options/repositories/outcomes.py",
    "options/repositories/trades.py", "options/strategy_orchestration.py",
    "scripts/run_option_outcome_worker.py", "scripts/run_worker_group.py")
OPTIMIZED_INTRADAY_RUNTIME_FILES = (*OPTIMIZED_RUNTIME_FILES, "options/intraday_participation.py",
    "equity/behavior.py", "equity/behavior_sources.py", "options/stock_behavior_gates.py")


class DetectorForwardLaunch(Contract):
    runtime_files: ClassVar[tuple[str, ...]] = RUNTIME_FILES
    schema_version: Literal["option_detector_forward_launch_v1"] = "option_detector_forward_launch_v1"
    dataset_id: Name
    effective_from: AwareDatetime
    underlyers: tuple[Name, ...] = Field(min_length=1, max_length=13)
    configuration_sha256: Sha256
    strategy_policy_sha256: Sha256
    valuation_policy_sha256: Sha256
    technical_policy_sha256: Sha256 = TECHNICAL_EXIT_POLICY.sha256
    qualification_policy_sha256: Sha256 = TECHNICAL_QUALIFICATION_POLICY.sha256
    stock_ledger: str
    acceptance_source: DirectSetupSourcePolicy
    resumption_source: DirectResumptionSourcePolicy
    runtime_sources: tuple[tuple[str, Sha256], ...]
    maximum_new_alerts: Literal[20] = 20
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_launch(self):
        if (len(set(self.underlyers)) != len(self.underlyers)
                or self.technical_policy_sha256 != TECHNICAL_EXIT_POLICY.sha256
                or self.qualification_policy_sha256 != TECHNICAL_QUALIFICATION_POLICY.sha256
                or self.acceptance_source.instance_id != self.resumption_source.instance_id
                or {name for name, _ in self.runtime_sources} != set(self.runtime_files)
                or len(self.runtime_sources) != len(self.runtime_files)):
            raise ValueError("detector launch policies, universe or runtime pins mismatch")
        return self


class SourceReadyDetectorForwardLaunch(DetectorForwardLaunch):
    schema_version: Literal["option_detector_forward_launch_v2"] = "option_detector_forward_launch_v2"
    stock_readiness_policy: Literal["PER_MODEL_COMPLETED_WINDOW_V1"] = "PER_MODEL_COMPLETED_WINDOW_V1"


class IntradayDetectorForwardLaunch(SourceReadyDetectorForwardLaunch):
    runtime_files: ClassVar[tuple[str, ...]] = INTRADAY_RUNTIME_FILES
    schema_version: Literal["option_detector_forward_launch_v3"] = "option_detector_forward_launch_v3"
    o1_confirmation_policy_sha256: Sha256
    alert_mode: Literal["DEVELOPMENT_OBSERVATIONS"] = "DEVELOPMENT_OBSERVATIONS"

    @model_validator(mode="after")
    def validate_intraday(self):
        from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY_V2

        if self.o1_confirmation_policy_sha256 != INTRADAY_ALIGNMENT_POLICY_V2.sha256:
            raise ValueError("intraday launch requires the reviewed O1 policy")
        return self


class LatestCompletedDetectorForwardLaunch(SourceReadyDetectorForwardLaunch):
    runtime_files: ClassVar[tuple[str, ...]] = INTRADAY_RUNTIME_FILES
    schema_version: Literal["option_detector_forward_launch_v4"] = "option_detector_forward_launch_v4"
    o1_confirmation_policy_sha256: Sha256
    alert_mode: Literal["DEVELOPMENT_OBSERVATIONS"] = "DEVELOPMENT_OBSERVATIONS"

    @model_validator(mode="after")
    def validate_latest_completed(self):
        from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY

        if self.o1_confirmation_policy_sha256 != INTRADAY_ALIGNMENT_POLICY.sha256:
            raise ValueError("latest-completed launch requires the reviewed O1 policy")
        return self


class StockSetupPublicationWaitPolicy(Contract):
    version: Literal["stock_setup_exact_publication_wait_v1"] = "stock_setup_exact_publication_wait_v1"
    maximum_wait_seconds: Literal[600] = 600
    poll_seconds: Literal[2] = 2
    wait_reasons: tuple[Literal["STOCK_WINDOW_NOT_AVAILABLE", "STOCK_WINDOW_NOT_PUBLISHED"], ...] = (
        "STOCK_WINDOW_NOT_AVAILABLE", "STOCK_WINDOW_NOT_PUBLISHED")
    deadline_basis: Literal["MIN_SOURCE_DEADLINE_OPTION_ACTIVITY_VALIDITY_HARD_CAP"] = "MIN_SOURCE_DEADLINE_OPTION_ACTIVITY_VALIDITY_HARD_CAP"
    prior_window_fallback: Literal[False] = False
    changes_episode_expiry: Literal[False] = False
    execution_permission: Literal[False] = False


STOCK_SETUP_PUBLICATION_WAIT_POLICY = StockSetupPublicationWaitPolicy()


class StockSetupReadyDetectorForwardLaunch(LatestCompletedDetectorForwardLaunch):
    schema_version: Literal["option_detector_forward_launch_v5"] = "option_detector_forward_launch_v5"
    stock_readiness_policy: Literal["PER_MODEL_EXACT_WINDOW_WAIT_V2"] = "PER_MODEL_EXACT_WINDOW_WAIT_V2"
    stock_setup_wait_policy_sha256: Literal[STOCK_SETUP_PUBLICATION_WAIT_POLICY.sha256] = STOCK_SETUP_PUBLICATION_WAIT_POLICY.sha256


class OptimizedStockSetupReadyDetectorForwardLaunch(StockSetupReadyDetectorForwardLaunch):
    runtime_files: ClassVar[tuple[str, ...]] = OPTIMIZED_INTRADAY_RUNTIME_FILES
    schema_version: Literal["option_detector_forward_launch_v6"] = "option_detector_forward_launch_v6"


def read_stock_setup_windows_when_ready(reader, *, path, policies, windows, as_of, wait_deadline,
                                        progress_callback=None, clock=None, wait=None):
    from research.stock_idea_forward import readiness_deadline

    if (as_of.utcoffset() is None or wait_deadline.utcoffset() is None or wait_deadline < as_of
            or not windows or any(boundary is None for _, boundary in windows.values())):
        return reader(path, policies=policies, windows=windows, as_of=as_of)
    clock = clock or (lambda: datetime.now(timezone.utc))
    wait = wait or time.sleep
    source_deadline = max(readiness_deadline(boundary) for _, boundary in windows.values())
    deadline = min(source_deadline, wait_deadline,
        as_of + timedelta(seconds=STOCK_SETUP_PUBLICATION_WAIT_POLICY.maximum_wait_seconds))
    current = as_of
    waiting = False
    while True:
        evidence, rejections = reader(path, policies=policies, windows=windows,
            as_of=current, clock=lambda: current)
        pending = any(any(key.endswith(reason) for reason in STOCK_SETUP_PUBLICATION_WAIT_POLICY.wait_reasons)
            and count > 0 for key, count in rejections.items())
        if not pending or current >= deadline:
            if waiting:
                LOGGER.info("stock setup exact publication wait %s at=%s deadline=%s reasons=%s",
                    "ready" if not pending else "deadline_reached", current.isoformat(), deadline.isoformat(),
                    dict(sorted(rejections.items())))
            return evidence, rejections
        if not waiting:
            LOGGER.info("stock setup exact publication wait pending at=%s deadline=%s reasons=%s",
                current.isoformat(), deadline.isoformat(), dict(sorted(rejections.items())))
            waiting = True
        if progress_callback is not None:
            progress_callback()
        wait(min(STOCK_SETUP_PUBLICATION_WAIT_POLICY.poll_seconds, (deadline - current).total_seconds()))
        advanced = clock().astimezone(timezone.utc)
        if advanced <= current:
            raise ValueError("stock setup publication wait clock did not advance")
        current = min(advanced, deadline)


def decode_detector_forward_launch(payload):
    data = json.loads(payload)
    contracts = {"option_detector_forward_launch_v1": DetectorForwardLaunch,
        "option_detector_forward_launch_v2": SourceReadyDetectorForwardLaunch,
        "option_detector_forward_launch_v3": IntradayDetectorForwardLaunch,
        "option_detector_forward_launch_v4": LatestCompletedDetectorForwardLaunch,
        "option_detector_forward_launch_v5": StockSetupReadyDetectorForwardLaunch,
        "option_detector_forward_launch_v6": OptimizedStockSetupReadyDetectorForwardLaunch}
    contract = contracts.get(data.get("schema_version"))
    if contract is None:
        raise ValueError("unsupported detector forward launch schema")
    return contract.model_validate(data)


def aligned_stock_windows(market_cutoffs, *, calendar=None):
    from options.calendar import OptionExchangeCalendar

    calendar = calendar or OptionExchangeCalendar()
    return {ticker: (cutoff, calendar.latest_delayed_slot(cutoff, interval=timedelta(minutes=30),
        provider_delay=timedelta(0), publication_grace=timedelta(0))) for ticker, cutoff in market_cutoffs.items()}


def _scoped_path(backend_dir, relative):
    path = (backend_dir / relative).resolve()
    if not path.is_relative_to(backend_dir.resolve()):
        raise ValueError("detector launch path must stay inside backend")
    return path


def _source_hashes(backend_dir, names):
    return tuple(sorted((name, hashlib.sha256(_scoped_path(backend_dir, name).read_bytes()).hexdigest()) for name in names))


def prepare_detector_forward_launch(*, backend_dir, configuration, dataset_id, effective_from, stock_ledger,
                                   approve_stock_runtime_transition=False, intraday_confirmation=False,
                                   stock_setup_wait=False):
    from equity.stock_alert_results import _decode_original_setup_payload
    from research.stock_alert_results import strategy_instance
    from research.stock_idea_engine import digest

    path = _scoped_path(backend_dir, stock_ledger)
    if not path.is_file():
        raise ValueError("forward launch requires the existing stock source ledger")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        manifest = connection.execute("SELECT policy_hash,CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_manifest WHERE singleton=1").fetchone()
        checkpoint = connection.execute("SELECT CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_checkpoint WHERE singleton=1").fetchone()
        latest = connection.execute("SELECT CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_publications ORDER BY window_key DESC LIMIT 1").fetchone()
        if not manifest or not manifest[1] or not checkpoint or not latest:
            raise ValueError("forward launch source manifest, checkpoint or publication is missing")
        config = json.loads(manifest[1])
        state = _decode_original_setup_payload(checkpoint[0])
        publication = _decode_original_setup_payload(latest[0])
    if digest(config) != manifest[0] or config["policy_version"] != "stock_ideas_forward_quality_v2":
        raise ValueError("forward launch stock source identity mismatch")
    instance = strategy_instance(config, state, "intraday")
    runtime = tuple(sorted(publication["runtime_sources"].items()))
    current_runtime = _source_hashes(backend_dir, [name for name, _ in runtime])
    if runtime != current_runtime:
        if not approve_stock_runtime_transition:
            raise ValueError("stock worker publication runtime differs from current source files; revalidation required")
        from research.stock_idea_forward import forward_config, runtime_sources

        if (set(publication["runtime_sources"]) != set(runtime_sources())
                or digest(forward_config(quality_version=2)) != manifest[0]):
            raise ValueError("reviewed stock runtime transition cannot change source scope or enrolled policy")
        runtime = current_runtime
    common = dict(instance_id=instance["instance_id"], instance_policy_sha256=manifest[0],
        publication_policy_sha256=publication["policy_hash"], runtime_sources=runtime)
    from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY

    launch_type = (OptimizedStockSetupReadyDetectorForwardLaunch if stock_setup_wait else
        LatestCompletedDetectorForwardLaunch if intraday_confirmation else SourceReadyDetectorForwardLaunch)
    if stock_setup_wait and not intraday_confirmation:
        raise ValueError("stock setup publication wait requires intraday confirmation")
    launch = launch_type(dataset_id=dataset_id, effective_from=effective_from,
        underlyers=configuration.settings.underlyers, configuration_sha256=configuration.configuration_sha256,
        strategy_policy_sha256=configuration.strategy_policy_sha256, valuation_policy_sha256=configuration.valuation_policy_sha256,
        stock_ledger=stock_ledger, acceptance_source=DirectSetupSourcePolicy(**common),
        resumption_source=DirectResumptionSourcePolicy(**common), runtime_sources=_source_hashes(backend_dir, launch_type.runtime_files),
        **(dict(o1_confirmation_policy_sha256=INTRADAY_ALIGNMENT_POLICY.sha256) if intraday_confirmation else {}))
    validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)
    return launch


def validate_detector_forward_launch(launch, *, configuration, backend_dir):
    launch = decode_detector_forward_launch(launch.canonical_json())
    if (configuration.strategy_policy.forward_admission is None
            or configuration.valuation_policy.underlying_price_basis != "RAW_FINALIZED_MINUTE_CLOSE_V1"
            or configuration.valuation_policy.maximum_source_age_seconds != 1800
            or not configuration.settings.start_read_only
            or launch.configuration_sha256 != configuration.configuration_sha256
            or launch.strategy_policy_sha256 != configuration.strategy_policy_sha256
            or launch.valuation_policy_sha256 != configuration.valuation_policy_sha256
            or launch.underlyers != configuration.settings.underlyers
            or launch.runtime_sources != _source_hashes(backend_dir, launch.runtime_files)):
        raise ValueError("detector launch does not match reviewed configuration, policy or source code")
    for policy in (launch.acceptance_source, launch.resumption_source):
        if policy.runtime_sources != _source_hashes(backend_dir, [name for name, _ in policy.runtime_sources]):
            raise ValueError("pinned stock runtime source changed")
    ledger = _scoped_path(backend_dir, launch.stock_ledger)
    if not isinstance(launch, SourceReadyDetectorForwardLaunch) and not ledger.is_file():
        raise ValueError("pinned stock ledger is unavailable")
    return launch


def load_detector_forward_launch(path, *, expected_sha256, configuration, backend_dir):
    path = _scoped_path(backend_dir, path)
    if path.stat().st_size > 262144:
        raise ValueError("detector launch manifest exceeds size bound")
    launch = decode_detector_forward_launch(path.read_text(encoding="utf-8"))
    if launch.sha256 != expected_sha256:
        raise ValueError("detector launch manifest checksum mismatch")
    return validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)


def build_detector_collector(launch, *, configuration, backend_dir):
    from equity.repositories import EquityCorporateActionRepository, EquityEvidenceRepository, EquityReferenceRepository
    from equity.stock_alert_results import (
        read_direct_stock_setups, read_direct_stock_setup_shadow_windows, read_direct_stock_setup_windows,
    )
    from options.detector_collection import DetectorCycleCollector, ProductionDetectorSourceReader, RetainedDetectorSourceReader
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    launch = validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)
    repository = OptionStockBehaviorAssessmentRepository()
    aligned = isinstance(launch, SourceReadyDetectorForwardLaunch)
    wait_for_setups = isinstance(launch, StockSetupReadyDetectorForwardLaunch)
    def setups(*, market_cutoff, as_of, market_cutoffs=None, wait_deadline=None, progress_callback=None):
        if aligned:
            path = _scoped_path(backend_dir, launch.stock_ledger)
            policies = (launch.acceptance_source, launch.resumption_source)
            windows = aligned_stock_windows(market_cutoffs)
            if wait_for_setups:
                if wait_deadline is None:
                    raise ValueError("stock setup publication wait requires an option activity deadline")
                return read_stock_setup_windows_when_ready(read_direct_stock_setup_windows,
                    path=path, policies=policies, windows=windows, as_of=as_of,
                    wait_deadline=wait_deadline, progress_callback=progress_callback)
            return read_direct_stock_setup_windows(path, policies=policies, windows=windows, as_of=as_of)
        return read_direct_stock_setups(_scoped_path(backend_dir, launch.stock_ledger),
            policies=(launch.acceptance_source, launch.resumption_source), underlyers=launch.underlyers,
            market_cutoff=market_cutoff, as_of=as_of)
    def setup_shadows(*, market_cutoff, as_of, market_cutoffs=None, **_):
        if not aligned:
            return (), {}
        return read_direct_stock_setup_shadow_windows(_scoped_path(backend_dir, launch.stock_ledger),
            policies=(launch.acceptance_source, launch.resumption_source),
            windows=aligned_stock_windows(market_cutoffs), as_of=as_of)
    sources = ProductionDetectorSourceReader(RetainedDetectorSourceReader(repository, EquityReferenceRepository(), EquityEvidenceRepository()),
        repository, EquityCorporateActionRepository(), setup_reader=setups, setup_shadow_reader=setup_shadows,
        aligned_setup_windows=aligned, setup_wait_enabled=wait_for_setups,
        intraday_confirmation=isinstance(launch, (IntradayDetectorForwardLaunch, LatestCompletedDetectorForwardLaunch)))
    return DetectorCycleCollector(sources, OptionAlertEvaluationRepository())