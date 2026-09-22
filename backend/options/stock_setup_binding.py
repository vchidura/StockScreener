"""In-memory setup/package validation; no persistence, API or worker wiring."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256, StockBehaviorSnapshot
from equity.behavior_setup import DirectSetupSourcePolicy, DirectStockSetupEvidence
from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind
from options.calendar import OptionExchangeCalendar
from options.domain import CatalogEligibility, validate_standard_contract
from options.outcome_contracts import PACKAGE_ASSESSMENT_POLICY, OptionPackageAssessment, assess_option_package
from options.stock_setup_gates import DirectHourlyAcceptancePolicy, SetupGateAssessment, evaluate_hourly_acceptance
from research.stock_idea_engine import entry_gate


SETUP_RESEARCH_UNDERLYERS = frozenset({
    "AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "PLTR", "SOFI", "TSLA", "SPY", "QQQ", "IWM",
})
SETUP_BINDING_CODE_PATHS = (
    "equity/behavior.py", "equity/behavior_setup.py", "equity/domain.py", "equity/stock_alert_results.py",
    "options/calendar.py", "options/domain.py", "options/outcome_contracts.py",
    "options/stock_setup_binding.py", "options/stock_setup_gates.py", "options/strategies/payoff.py",
    "research/stock_idea_engine.py",
)


def _relative_path(value: str, prefix: str, suffix: str) -> str:
    path = PurePosixPath(value)
    if (path.is_absolute() or "\\" in value or ":" in value or ".." in path.parts
            or not value.startswith(prefix) or path.suffix != suffix or str(path) != value):
        raise ValueError("setup path must be canonical and remain in its declared backend root")
    return value


class SetupResearchManifest(Contract):
    schema_version: Literal["option_setup_research_manifest_v1"] = "option_setup_research_manifest_v1"
    study_id: Name
    frozen_at: AwareDatetime
    starts_at: AwareDatetime
    ends_at: AwareDatetime
    underlyers: tuple[Name, ...]
    source_ledger: str
    source_policy: DirectSetupSourcePolicy
    detector_policy: DirectHourlyAcceptancePolicy
    option_strategy_policy_sha256: Sha256
    option_configuration_sha256: Sha256
    option_market_policy_sha256: Sha256
    option_analysis_policy_sha256: Sha256
    valuation_policy_sha256: Sha256
    package_assessment_policy_sha256: Sha256 = PACKAGE_ASSESSMENT_POLICY.sha256
    implementation_sha256s: tuple[tuple[Name, Sha256], ...]
    maximum_candidates_per_read: int = Field(strict=True, ge=1, le=20)
    maximum_holding_seconds: int = Field(strict=True, ge=60, le=23400)
    holding_basis: Literal["SAME_SESSION_EXPLICIT_TIMES"] = "SAME_SESSION_EXPLICIT_TIMES"
    research_only: Literal[True] = True
    collection_enabled: Literal[False] = False
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_manifest(self):
        if not self.frozen_at <= self.starts_at < self.ends_at or self.ends_at - self.starts_at > timedelta(days=7):
            raise ValueError("setup research requires a prospective window of at most seven days")
        if len(self.underlyers) != 13 or set(self.underlyers) != SETUP_RESEARCH_UNDERLYERS:
            raise ValueError("setup research requires the fixed thirteen-name cohort")
        _relative_path(self.source_ledger, "backups/equity-shadow/", ".sqlite")
        if self.detector_policy.setup_source_policy_sha256 != self.source_policy.sha256:
            raise ValueError("setup detector must pin the manifest source policy")
        if self.package_assessment_policy_sha256 != PACKAGE_ASSESSMENT_POLICY.sha256:
            raise ValueError("unsupported package assessment policy")
        if tuple(name for name, _ in self.implementation_sha256s) != SETUP_BINDING_CODE_PATHS:
            raise ValueError("setup manifest must pin exact ordered implementation owners")
        return self

    def source_path(self, backend_dir: Path) -> Path:
        root = backend_dir.resolve()
        path = (root / self.source_ledger).resolve()
        if not path.is_relative_to((root / "backups/equity-shadow").resolve()):
            raise ValueError("setup source path escapes its approved root")
        return path


def load_setup_research_manifest(path: Path, *, expected_sha256: str, backend_dir: Path) -> SetupResearchManifest:
    with path.open("rb") as source:
        payload = source.read(65537)
    if len(payload) > 65536:
        raise ValueError("setup research manifest exceeds read bound")
    manifest = SetupResearchManifest.model_validate_json(payload)
    if manifest.sha256 != expected_sha256:
        raise ValueError("setup manifest differs from independently pinned hash")
    manifest.source_path(backend_dir)
    for name, expected in manifest.implementation_sha256s:
        if hashlib.sha256((backend_dir / name).read_bytes()).hexdigest() != expected:
            raise ValueError("setup implementation differs from frozen manifest")
    return manifest


class SetupLegBasis(Contract):
    contract_id: int = Field(strict=True, gt=0)
    snapshot_id: UUID
    snapshot_sha256: Sha256
    reference_sha256: Sha256
    raw_bar_revision_id: UUID
    raw_bar_sha256: Sha256
    spot: Decimal = Field(gt=0, allow_inf_nan=False)
    spot_at: AwareDatetime
    available_at: AwareDatetime
    basis: Literal["EXACT_RAW_RTH_CLOSE_STANDARD_100_SHARES"] = "EXACT_RAW_RTH_CLOSE_STANDARD_100_SHARES"


class SetupPackageBinding(Contract):
    schema_version: Literal["option_setup_package_binding_v1"] = "option_setup_package_binding_v1"
    manifest: SetupResearchManifest
    manifest_sha256: Sha256
    package_assessment: OptionPackageAssessment
    stock: StockBehaviorSnapshot
    setup: DirectStockSetupEvidence
    stock_assessment: SetupGateAssessment
    directional_thesis: Literal["BULLISH", "BEARISH"]
    security_revision_id: UUID
    security_reference_sha256: Sha256
    resolved_inputs_sha256: Sha256
    leg_basis: tuple[SetupLegBasis, ...] = Field(min_length=1, max_length=4)
    market_cutoff: AwareDatetime
    source_received_at: AwareDatetime
    decision_at: AwareDatetime
    planned_entry_at: AwareDatetime
    entry_deadline: AwareDatetime
    planned_exit_at: AwareDatetime
    entry_geometry_status: Name
    assessment_mode: Literal["ORIGINAL_CANDIDATE_TIME", "LATER_RESEARCH_ASSESSMENT"]
    research_only: Literal[True] = True
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_binding(self):
        manifest = self.manifest
        package = self.package_assessment.package
        if self.manifest_sha256 != manifest.sha256 or package is None or self.package_assessment.status != "READY":
            raise ValueError("setup binding requires exact manifest and READY package")
        if (self.package_assessment.assessment_policy_sha256 != manifest.package_assessment_policy_sha256
                or self.package_assessment.valuation_policy_sha256 != manifest.valuation_policy_sha256
                or package.strategy_policy_sha256 != manifest.option_strategy_policy_sha256
                or package.underlyer not in manifest.underlyers):
            raise ValueError("setup package differs from manifest scope/policies")
        if (self.package_assessment.candidate_id != package.candidate_id
                or self.package_assessment.candidate_identity_sha256 != package.candidate_identity_sha256
                or self.package_assessment.option_matrix_id != package.option_matrix_id):
            raise ValueError("setup package assessment identity mismatch")
        if self.setup.source_policy != manifest.source_policy or self.stock.security_id != self.setup.source.security_id:
            raise ValueError("setup source/security differs from manifest or stock evidence")
        if self.stock.security_revision_id != self.security_revision_id:
            raise ValueError("setup stock reference revision mismatch")
        if not manifest.starts_at <= self.decision_at < manifest.ends_at:
            raise ValueError("setup decision is outside manifest window")
        if (package.valid_until is None or not package.market_time <= self.market_cutoff <= self.decision_at
                or not package.observed_at <= self.source_received_at <= self.decision_at < self.planned_entry_at
                <= self.entry_deadline < min(package.valid_until, self.setup.source.valid_until)
                or not self.planned_entry_at < self.planned_exit_at <= manifest.ends_at
                or self.planned_exit_at - self.planned_entry_at > timedelta(seconds=manifest.maximum_holding_seconds)):
            raise ValueError("setup option entry/receipt/holding clocks are invalid")
        if (self.setup.source.received_at > self.source_received_at or self.stock.available_at > self.source_received_at
                or any(row.available_at > self.source_received_at for row in self.leg_basis)
                or self.package_assessment.assessed_at > self.source_received_at):
            raise ValueError("setup binding predates received evidence")
        expected_mode = "ORIGINAL_CANDIDATE_TIME" if self.decision_at == package.observed_at else "LATER_RESEARCH_ASSESSMENT"
        if self.assessment_mode != expected_mode:
            raise ValueError("setup decision must preserve original versus later assessment timing")
        calendar = exchange_calendars.get_calendar("XNYS")
        session = self.planned_entry_at.astimezone(ZoneInfo("America/New_York")).date()
        if (not calendar.is_session(session)
                or self.setup.source.market_time.astimezone(ZoneInfo("America/New_York")).date() != session
                or not calendar.session_open(session) <= self.planned_entry_at < self.planned_exit_at <= calendar.session_close(session)):
            raise ValueError("setup binding requires same-session RTH entry and exit")
        if self.planned_exit_at >= min(OptionExchangeCalendar().expiration_cutoff(leg.expiration_date) for leg in package.ordered_legs):
            raise ValueError("setup planned exit must precede actual expiration cutoff")
        if tuple(row.contract_id for row in self.leg_basis) != tuple(leg.contract_id for leg in package.ordered_legs):
            raise ValueError("setup price-basis evidence must preserve exact leg order")
        if len({(row.spot, row.spot_at) for row in self.leg_basis}) != 1:
            raise ValueError("setup package requires one coherent raw underlying spot")
        if any(not self.setup.source.market_time <= row.spot_at <= package.market_time for row in self.leg_basis):
            raise ValueError("setup option spot must be at or after trigger and within package cutoff")
        target = next((row for row in manifest.detector_policy.targets
                       if row.strategy_name == package.strategy_name and row.structure_type == package.structure), None)
        if target is None or self.directional_thesis != target.directional_thesis:
            raise ValueError("setup package strategy/structure/thesis mismatch")
        expected_type = "CALL" if target.directional_thesis == "BULLISH" else "PUT"
        legs = package.ordered_legs
        if (len(legs) != (2 if "VERTICAL" in package.structure else 1)
                or any(leg.contract_type != expected_type or leg.ratio != 1 or leg.multiplier != 100 for leg in legs)
                or legs[0].side != "BUY" or len(legs) == 2 and legs[1].side != "SELL"):
            raise ValueError("setup requires the declared long/debit leg orientation")
        if len(legs) == 2 and not (
            legs[0].strike < legs[1].strike if expected_type == "CALL" else legs[0].strike > legs[1].strike
        ):
            raise ValueError("setup debit spread strike orientation mismatch")
        for leg, basis in zip(package.ordered_legs, self.leg_basis):
            if leg.snapshot_id != basis.snapshot_id or leg.spot != basis.spot or leg.valuation_policy_sha256 != manifest.valuation_policy_sha256:
                raise ValueError("setup leg snapshot/spot/policy mismatch")
        expected_assessment = dict(candidate_id=package.candidate_id,
            candidate_identity_sha256=package.candidate_identity_sha256, matrix_id=package.option_matrix_id,
            decision_at=self.decision_at, stock_market_cutoff=self.market_cutoff,
            detector_policy_version=manifest.detector_policy.version, detector_policy_sha256=manifest.detector_policy.sha256,
            stock_snapshot_id=self.stock.snapshot_id, stock_payload_sha256=self.stock.sha256,
            setup_payload_sha256=self.setup.sha256, setup_source_policy_sha256=manifest.source_policy.sha256,
            episode_id=self.setup.candidate.episode_id)
        if (any(getattr(self.stock_assessment, key) != value for key, value in expected_assessment.items())
                or self.stock_assessment.disposition not in ("ELIGIBLE_RESEARCH", "BLOCKED")
                or not self.stock_assessment.checks
                or any((row.verdict == "PASS") != (row.reason is None) for row in self.stock_assessment.checks)
                or (self.stock_assessment.disposition == "ELIGIBLE_RESEARCH") != all(row.verdict == "PASS" for row in self.stock_assessment.checks)):
            raise ValueError("setup gate assessment does not match the bound facts")
        if self.entry_geometry_status not in ("PASS", "INVALID_ENTRY", "ENTRY_OUTSIDE_BRACKET", "INSUFFICIENT_TARGET_ROOM", "ENTRY_CHASE_OR_BOUNDARY_FAILED"):
            raise ValueError("unsupported setup entry geometry status")
        if len(self.canonical_json().encode("ascii")) > 524288:
            raise ValueError("setup package binding exceeds payload bound")
        return self


def bind_option_leg_basis(*, candidate, security, snapshots, references, raw_bars,
                          raw_bar_created_ats, source_received_at, decision_at,
                          source_session, valuation_policy):
    if not len(candidate.legs) == len(snapshots) == len(references) == len(raw_bars) == len(raw_bar_created_ats):
        raise ValueError("setup requires complete ordered leg source evidence")
    if len({snapshot.batch_id for snapshot in snapshots}) != 1:
        raise ValueError("setup option snapshots must belong to one coherent batch")
    basis_rows = []
    for leg, snapshot, reference, bar, created_at in zip(candidate.legs, snapshots, references, raw_bars, raw_bar_created_ats):
        validation = validate_standard_contract(reference)
        if (validation.eligibility_status != CatalogEligibility.VALIDATED_ACTIVE
                or set(json.loads(reference.adjustment_metadata_json)) - {"cfi", "correction"}):
            raise ValueError("setup requires unadjusted standard option deliverables")
        if snapshot.contract_type != validation.contract_type or snapshot.exercise_style != validation.exercise_style:
            raise ValueError("setup option type/style differs from contract reference")
        for field in ("contract_ticker", "strike", "expiration_date", "underlyer"):
            if getattr(reference, field) != getattr(snapshot, field):
                raise ValueError("setup contract reference does not match snapshot")
        if (reference.valid_from > snapshot.market_data_time
                or reference.valid_to is not None and reference.valid_to <= snapshot.market_data_time
                or max(reference.first_observed_at, reference.revised_observed_at or reference.first_observed_at) > candidate.observed_time
                or snapshot.market_data_time > candidate.market_data_time
                or snapshot.first_observed_at > candidate.observed_time
                or snapshot.revised_observed_at is not None and snapshot.revised_observed_at > candidate.observed_time
                or snapshot.expiration_cutoff != OptionExchangeCalendar().expiration_cutoff(snapshot.expiration_date)):
            raise ValueError("setup reference/snapshot clocks are not causal")
        expected = dict(snapshot_id=snapshot.snapshot_id, contract_id=snapshot.contract_id,
            contract_ticker=snapshot.contract_ticker, strike=snapshot.strike, contract_type=snapshot.contract_type,
            expiration_date=snapshot.expiration_date, multiplier=snapshot.shares_per_contract, spot=snapshot.spot,
            model_mark=snapshot.model_mark, local_iv=snapshot.local_iv, time_to_expiration_years=snapshot.time_to_expiration_years,
            risk_free_rate=snapshot.risk_free_rate, dividend_yield=snapshot.dividend_yield,
            source_market_time=snapshot.mark_market_data_time, valuation_policy_sha256=snapshot.valuation_policy_sha256)
        if (any(getattr(leg, key) != value for key, value in expected.items()) or snapshot.underlyer != candidate.underlyer
                or leg.mark_source != snapshot.mark_source.value or leg.model_version != snapshot.model_version
                or snapshot.quality_flags or leg.quality_flags or snapshot.shares_per_contract != 100):
            raise ValueError("setup leg does not match its exact retained snapshot")
        if (snapshot.mark_source not in valuation_policy.allowed_entry_mark_sources
                or not 0 <= (decision_at - snapshot.mark_market_data_time).total_seconds() <= valuation_policy.maximum_source_age_seconds
                or not 0 <= (snapshot.mark_market_data_time - snapshot.spot_market_data_time).total_seconds() <= valuation_policy.maximum_option_spot_skew_seconds):
            raise ValueError("setup leg mark/spot freshness mismatch")
        if (bar.security_id != security.security_id or bar.ticker != security.ticker or bar.adjusted or not bar.is_final
                or bar.source_kind != BarSourceKind.NATIVE_REST or bar.availability_mode != BarAvailabilityMode.LIVE_OBSERVED
                or bar.session_scope != BarSessionScope.RTH or bar.quality_codes
                or bar.bar_end != snapshot.spot_market_data_time or bar.close_price != snapshot.spot
                or bar.session_date != source_session
                or max(bar.system_observed_at, bar.provider_published_at or bar.system_observed_at, created_at) > source_received_at):
            raise ValueError("setup raw stock price basis is unavailable or mismatched")
        basis_rows.append(SetupLegBasis(contract_id=leg.contract_id, snapshot_id=leg.snapshot_id,
            snapshot_sha256=snapshot.normalized_payload_sha256, reference_sha256=reference.payload_sha256,
            raw_bar_revision_id=bar.bar_revision_id, raw_bar_sha256=bar.payload_sha256, spot=bar.close_price,
            spot_at=bar.bar_end, available_at=max(bar.system_observed_at, created_at,
                snapshot.revised_observed_at or snapshot.first_observed_at, reference.revised_observed_at or reference.first_observed_at)))
    return tuple(basis_rows)


def bind_setup_option_package(*, manifest, expected_manifest_sha256, candidate, package_assessment,
                             stock, setup, security, lineage, snapshots, references, raw_bars,
                             raw_bar_created_ats, source_received_at, decision_at,
                             planned_entry_at, entry_deadline, planned_exit_at, valuation_policy):
    if manifest.sha256 != expected_manifest_sha256 or valuation_policy.policy_sha256 != manifest.valuation_policy_sha256:
        raise ValueError("setup binding requires independently resolved manifest and valuation policy")
    if assess_option_package(candidate, valuation_policy_sha256=manifest.valuation_policy_sha256) != package_assessment:
        raise ValueError("setup package does not match exact retained candidate economics/legs")
    if (lineage.matrix_id != candidate.matrix_id or lineage.underlying != candidate.underlyer
            or lineage.market_time != candidate.market_data_time or lineage.observed_time != candidate.observed_time
            or lineage.configuration_sha256 != manifest.option_configuration_sha256
            or lineage.market_policy_sha256 != manifest.option_market_policy_sha256
            or lineage.analysis_policy_sha256 != manifest.option_analysis_policy_sha256):
        raise ValueError("setup option matrix lineage mismatch")
    if (not security.active or security.security_id != stock.security_id or security.ticker != candidate.underlyer
            or security.effective_from > min(setup.source.market_time, candidate.market_data_time)
            or security.observed_at > source_received_at):
        raise ValueError("setup dated security identity is unavailable")
    basis_rows = bind_option_leg_basis(candidate=candidate, security=security,
        snapshots=snapshots, references=references, raw_bars=raw_bars,
        raw_bar_created_ats=raw_bar_created_ats, source_received_at=source_received_at,
        decision_at=decision_at, source_session=setup.source.market_time.astimezone(ZoneInfo("America/New_York")).date(),
        valuation_policy=valuation_policy)
    assessment = evaluate_hourly_acceptance(stock, setup, policy=manifest.detector_policy,
        candidate_id=candidate.candidate_id, candidate_identity_sha256=candidate.identity_sha256,
        matrix_id=candidate.matrix_id, security_id=security.security_id, underlyer=candidate.underlyer,
        strategy_name=candidate.strategy_name, structure_type=candidate.structure_type,
        directional_thesis=candidate.primary_evidence.get("directional_thesis"), market_cutoff=lineage.scheduled_cycle,
        decision_at=decision_at, trusted_source_policy=manifest.source_policy)
    resolved_hash = hashlib.sha256(json.dumps([asdict(security), asdict(lineage),
        [asdict(row) for row in snapshots], [asdict(row) for row in references],
        [asdict(row) for row in raw_bars], raw_bar_created_ats], sort_keys=True, default=str, allow_nan=False).encode("ascii")).hexdigest()
    return SetupPackageBinding(manifest=manifest, manifest_sha256=manifest.sha256,
        package_assessment=package_assessment, stock=stock, setup=setup, stock_assessment=assessment,
        directional_thesis=candidate.primary_evidence.get("directional_thesis"),
        security_revision_id=security.security_revision_id, security_reference_sha256=security.payload_sha256,
        resolved_inputs_sha256=resolved_hash, leg_basis=tuple(basis_rows), market_cutoff=lineage.scheduled_cycle,
        source_received_at=source_received_at, decision_at=decision_at, planned_entry_at=planned_entry_at,
        entry_deadline=entry_deadline, planned_exit_at=planned_exit_at,
        entry_geometry_status=entry_gate(setup.candidate, float(basis_rows[0].spot)) or "PASS",
        assessment_mode="ORIGINAL_CANDIDATE_TIME" if decision_at == candidate.observed_time else "LATER_RESEARCH_ASSESSMENT")