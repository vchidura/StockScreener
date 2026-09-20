from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from equity.behavior_setup import DirectSetupSourcePolicy, DirectResumptionSourcePolicy
from options.alert_plans import TECHNICAL_EXIT_POLICY
from options.dual_origin import TECHNICAL_QUALIFICATION_POLICY


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


class DetectorForwardLaunch(Contract):
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
                or {name for name, _ in self.runtime_sources} != set(RUNTIME_FILES)
                or len(self.runtime_sources) != len(RUNTIME_FILES)):
            raise ValueError("detector launch policies, universe or runtime pins mismatch")
        return self


def _scoped_path(backend_dir, relative):
    path = (backend_dir / relative).resolve()
    if not path.is_relative_to(backend_dir.resolve()):
        raise ValueError("detector launch path must stay inside backend")
    return path


def _source_hashes(backend_dir, names):
    return tuple(sorted((name, hashlib.sha256(_scoped_path(backend_dir, name).read_bytes()).hexdigest()) for name in names))


def prepare_detector_forward_launch(*, backend_dir, configuration, dataset_id, effective_from, stock_ledger):
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
    if runtime != _source_hashes(backend_dir, [name for name, _ in runtime]):
        raise ValueError("stock worker publication runtime differs from current source files; revalidation required")
    common = dict(instance_id=instance["instance_id"], instance_policy_sha256=manifest[0],
        publication_policy_sha256=publication["policy_hash"], runtime_sources=runtime)
    launch = DetectorForwardLaunch(dataset_id=dataset_id, effective_from=effective_from,
        underlyers=configuration.settings.underlyers, configuration_sha256=configuration.configuration_sha256,
        strategy_policy_sha256=configuration.strategy_policy_sha256, valuation_policy_sha256=configuration.valuation_policy_sha256,
        stock_ledger=stock_ledger, acceptance_source=DirectSetupSourcePolicy(**common),
        resumption_source=DirectResumptionSourcePolicy(**common), runtime_sources=_source_hashes(backend_dir, RUNTIME_FILES))
    validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)
    return launch


def validate_detector_forward_launch(launch, *, configuration, backend_dir):
    launch = DetectorForwardLaunch.model_validate_json(launch.canonical_json())
    if (configuration.strategy_policy.forward_admission is None
            or configuration.valuation_policy.underlying_price_basis != "RAW_FINALIZED_MINUTE_CLOSE_V1"
            or configuration.valuation_policy.maximum_source_age_seconds != 1800
            or not configuration.settings.start_read_only
            or launch.configuration_sha256 != configuration.configuration_sha256
            or launch.strategy_policy_sha256 != configuration.strategy_policy_sha256
            or launch.valuation_policy_sha256 != configuration.valuation_policy_sha256
            or launch.underlyers != configuration.settings.underlyers
            or launch.runtime_sources != _source_hashes(backend_dir, RUNTIME_FILES)):
        raise ValueError("detector launch does not match reviewed configuration, policy or source code")
    for policy in (launch.acceptance_source, launch.resumption_source):
        if policy.runtime_sources != _source_hashes(backend_dir, [name for name, _ in policy.runtime_sources]):
            raise ValueError("pinned stock runtime source changed")
    if not _scoped_path(backend_dir, launch.stock_ledger).is_file():
        raise ValueError("pinned stock ledger is unavailable")
    return launch


def load_detector_forward_launch(path, *, expected_sha256, configuration, backend_dir):
    path = _scoped_path(backend_dir, path)
    if path.stat().st_size > 262144:
        raise ValueError("detector launch manifest exceeds size bound")
    launch = DetectorForwardLaunch.model_validate_json(path.read_text(encoding="utf-8"))
    if launch.sha256 != expected_sha256:
        raise ValueError("detector launch manifest checksum mismatch")
    return validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)


def build_detector_collector(launch, *, configuration, backend_dir):
    from equity.repositories import EquityCorporateActionRepository, EquityEvidenceRepository, EquityReferenceRepository
    from equity.stock_alert_results import read_direct_stock_setups
    from options.detector_collection import DetectorCycleCollector, ProductionDetectorSourceReader, RetainedDetectorSourceReader
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    launch = validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend_dir)
    repository = OptionStockBehaviorAssessmentRepository()
    def setups(*, market_cutoff, as_of):
        return read_direct_stock_setups(_scoped_path(backend_dir, launch.stock_ledger),
            policies=(launch.acceptance_source, launch.resumption_source), underlyers=launch.underlyers,
            market_cutoff=market_cutoff, as_of=as_of)
    sources = ProductionDetectorSourceReader(RetainedDetectorSourceReader(repository, EquityReferenceRepository(), EquityEvidenceRepository()),
        repository, EquityCorporateActionRepository(), setup_reader=setups)
    return DetectorCycleCollector(sources, OptionAlertEvaluationRepository())