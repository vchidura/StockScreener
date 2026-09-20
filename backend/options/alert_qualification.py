from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import UUID
from zoneinfo import ZoneInfo

from options.calendar import OptionExchangeCalendar
from options.domain import ContractType
from options.strategies.domain import CandidateLeg, OptionSide, StructureRiskClass, StructureType, canonical_json
from options.strategies.gates import GateVerdict
from options.strategies.payoff import evaluate_terminal_payoff
from options.strategies.registry import REGISTRY_BY_NAME


QUALIFICATION_VERSION = "option_alert_qualification_v1"
RETAINED_EVIDENCE_VERSION = "option_alert_retained_evidence_v2"


class AlertCapability(str, Enum):
    OBSERVATION = "OBSERVATION"
    INDICATIVE = "INDICATIVE"
    QUOTE_PAPER = "QUOTE_PAPER"
    EXECUTION = "EXECUTION"


class AlertInput(str, Enum):
    SELECTED_CANDIDATE = "SELECTED_CANDIDATE"
    CONTRACT_IDENTITY = "CONTRACT_IDENTITY"
    SOURCE_CAUSALITY = "SOURCE_CAUSALITY"
    VALUATION = "VALUATION"
    PACKAGE_ECONOMICS = "PACKAGE_ECONOMICS"
    DIRECTION = "DIRECTION"
    RANGE_CONTEXT = "RANGE_CONTEXT"
    OPEN_INTEREST = "OPEN_INTEREST"
    ACTIVITY = "ACTIVITY"
    VOLATILITY_SURFACE = "VOLATILITY_SURFACE"
    GAMMA_CONTEXT = "GAMMA_CONTEXT"
    COMPARABLE_IV_HISTORY = "COMPARABLE_IV_HISTORY"
    EVENT_HORIZON = "EVENT_HORIZON"
    ENTRY_WINDOW = "ENTRY_WINDOW"
    QUOTE_LIQUIDITY = "QUOTE_LIQUIDITY"
    PAPER_RISK = "PAPER_RISK"
    ASSIGNMENT_POLICY = "ASSIGNMENT_POLICY"
    REALTIME_INPUTS = "REALTIME_INPUTS"
    EXECUTION_LEDGER = "EXECUTION_LEDGER"


@dataclass(frozen=True, slots=True)
class AlertInputEvidence:
    verdict: GateVerdict
    source_ids: tuple[str, ...] = ()
    available_at: datetime | None = None
    reasons: tuple[str, ...] = ()

    def __post_init__(self):
        if not isinstance(self.verdict, GateVerdict):
            raise TypeError("evidence verdict must be a GateVerdict")
        if not isinstance(self.source_ids, tuple) or not isinstance(self.reasons, tuple):
            raise TypeError("evidence IDs and reasons must be immutable tuples")
        if self.available_at is not None and self.available_at.utcoffset() is None:
            raise ValueError("evidence availability must be timezone-aware")
        if self.verdict is GateVerdict.PASS and (not self.source_ids or self.available_at is None):
            raise ValueError("passing evidence requires source IDs and availability time")
        if any(not source.strip() for source in self.source_ids):
            raise ValueError("evidence source IDs cannot be blank")


@dataclass(frozen=True, slots=True)
class AlertRequirements:
    required: tuple[AlertInput, ...]
    advisory: tuple[AlertInput, ...]
    applicable: bool = True


def alert_requirements(strategy: str, structure: StructureType, capability: AlertCapability) -> AlertRequirements:
    registration = REGISTRY_BY_NAME.get(strategy)
    if registration is None or structure not in registration.allowed_structure_types:
        raise ValueError("strategy and structure are not a registered pair")
    observation_only = registration.allowed_risk_classes == (StructureRiskClass.RESEARCH_CONTEXT,)
    if observation_only and capability is not AlertCapability.OBSERVATION:
        return AlertRequirements((), (), False)
    required = [AlertInput.SELECTED_CANDIDATE, AlertInput.CONTRACT_IDENTITY, AlertInput.SOURCE_CAUSALITY]
    advisory = [AlertInput.EVENT_HORIZON, AlertInput.GAMMA_CONTEXT, AlertInput.COMPARABLE_IV_HISTORY]
    if strategy in {"VOLUME_OI_ANOMALY", "SWEEP_LIKE_CLUSTER"}:
        required.append(AlertInput.ACTIVITY)
        if strategy == "VOLUME_OI_ANOMALY":
            required.append(AlertInput.OPEN_INTEREST)
    else:
        required.append(AlertInput.VALUATION)
    if strategy == "VOLATILITY_SMILE_DISTORTION":
        required.append(AlertInput.VOLATILITY_SURFACE)
    if capability is not AlertCapability.OBSERVATION:
        required.append(AlertInput.PACKAGE_ECONOMICS)
        if structure in {
            StructureType.LONG_CALL, StructureType.LONG_PUT,
            StructureType.CALL_DEBIT_VERTICAL, StructureType.PUT_DEBIT_VERTICAL,
            StructureType.CALL_CREDIT_VERTICAL, StructureType.PUT_CREDIT_VERTICAL,
        }:
            required.append(AlertInput.DIRECTION)
        if strategy == "SPREAD_RANGE_LOCATOR":
            required.append(AlertInput.OPEN_INTEREST)
            if structure in {StructureType.IRON_CONDOR, StructureType.CALL_BUTTERFLY, StructureType.PUT_BUTTERFLY}:
                required.append(AlertInput.RANGE_CONTEXT)
        if strategy == "ZERO_DTE_GAMMA_SQUEEZE":
            required.extend([AlertInput.GAMMA_CONTEXT, AlertInput.ACTIVITY])
    if capability in {AlertCapability.QUOTE_PAPER, AlertCapability.EXECUTION}:
        required.extend([AlertInput.EVENT_HORIZON, AlertInput.ENTRY_WINDOW, AlertInput.QUOTE_LIQUIDITY, AlertInput.PAPER_RISK])
        if structure not in {StructureType.LONG_CALL, StructureType.LONG_PUT}:
            required.append(AlertInput.ASSIGNMENT_POLICY)
        if strategy == "ZERO_DTE_GAMMA_SQUEEZE":
            required.append(AlertInput.REALTIME_INPUTS)
    if capability is AlertCapability.EXECUTION:
        required.append(AlertInput.EXECUTION_LEDGER)
    return AlertRequirements(tuple(required), tuple(item for item in advisory if item not in required))


def alert_qualification_policy() -> dict[str, object]:
    contracts = []
    for name, registration in REGISTRY_BY_NAME.items():
        for structure in registration.allowed_structure_types:
            for capability in AlertCapability:
                requirements = alert_requirements(name, structure, capability)
                contracts.append({
                    "strategy": name, "structure": structure.value, "capability": capability.value,
                    "applicable": requirements.applicable,
                    "required": [item.value for item in requirements.required],
                    "advisory": [item.value for item in requirements.advisory],
                })
    return {"version": QUALIFICATION_VERSION, "contracts": contracts, "assessment_only": True}


def qualification_policy_sha256() -> str:
    return hashlib.sha256(canonical_json(alert_qualification_policy()).encode("ascii")).hexdigest()


def qualify_option_alert(
    strategy: str,
    structure: StructureType,
    evidence: Mapping[AlertInput, AlertInputEvidence],
    decision_at: datetime,
) -> dict[str, object]:
    if decision_at.utcoffset() is None:
        raise ValueError("decision cutoff must be timezone-aware")
    assessments = []
    for capability in AlertCapability:
        contract = alert_requirements(strategy, structure, capability)
        checks = []
        for required, inputs in ((True, contract.required), (False, contract.advisory)):
            for field in inputs:
                fact = evidence.get(field)
                if fact is None:
                    fact = AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("EVIDENCE_NOT_RECORDED",))
                if fact.available_at is not None and fact.available_at > decision_at:
                    fact = AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("EVIDENCE_AFTER_DECISION_CUTOFF",))
                checks.append({
                    "input": field.value, "required": required, "verdict": fact.verdict.value,
                    "source_ids": list(fact.source_ids),
                    "available_at": fact.available_at.isoformat() if fact.available_at else None,
                    "reasons": list(fact.reasons),
                })
        blocking = [row for row in checks if row["required"] and row["verdict"] != GateVerdict.PASS.value]
        status = (
            "NOT_APPLICABLE" if not contract.applicable else
            "BLOCKED" if any(row["verdict"] == GateVerdict.FAIL.value for row in blocking) else
            "UNAVAILABLE" if blocking else "SATISFIED"
        )
        assessments.append({
            "capability": capability.value, "status": status, "checks": checks,
            "blocking_inputs": [row["input"] for row in blocking],
            "reason": "OBSERVATION_ONLY_MODEL" if not contract.applicable else None,
        })
    return {
        "version": QUALIFICATION_VERSION, "policy_sha256": qualification_policy_sha256(),
        "strategy": strategy, "structure": structure.value,
        "decision_at": decision_at.astimezone(timezone.utc).isoformat(),
        "assessments": assessments, "assessment_only": True,
        "execution_permission": False, "probability": None,
    }


def retained_time(value: object) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if stamp.utcoffset() is None:
        raise ValueError("retained timestamp must be timezone-aware")
    return stamp.astimezone(timezone.utc)


def retained_leg(row: Mapping[str, object]) -> CandidateLeg:
    values = {field.name: row[field.name] for field in fields(CandidateLeg) if field.name in row}
    for key in ("snapshot_id",):
        values[key] = UUID(str(values[key]))
    for key in ("strike", "spot", "model_mark"):
        values[key] = Decimal(str(values[key]))
    for key in ("local_iv", "time_to_expiration_years", "risk_free_rate", "dividend_yield", "local_delta", "local_gamma", "local_theta_per_day", "local_vega_per_vol_point", "local_rho_per_rate_point"):
        values[key] = float(values[key])
    for key in ("leg_index", "contract_id", "ratio", "multiplier"):
        if type(values[key]) is not int:
            raise ValueError("leg identity, ratio and multiplier must be integers")
    values["side"] = OptionSide(values["side"])
    values["contract_type"] = ContractType(values["contract_type"])
    values["expiration_date"] = date.fromisoformat(str(values["expiration_date"]))
    values["source_market_time"] = retained_time(values["source_market_time"])
    values["quality_flags"] = tuple(values.get("quality_flags", ()))
    return CandidateLeg(**values)


def retained_candidate(row, legs):
    from options.strategies.domain import OptionCandidate, CandidateKind, CandidateStatus, StructureRiskClass

    aliases = dict(identity_sha256="candidate_identity", underlyer="underlying", rank="candidate_rank")
    values = {field.name: row[aliases.get(field.name, field.name)] for field in fields(OptionCandidate)
        if field.name not in ("legs", "execution_eligibility")}
    for key, kind in (("candidate_kind", CandidateKind), ("status", CandidateStatus),
                      ("structure_type", StructureType), ("structure_risk_class", StructureRiskClass)):
        values[key] = kind(values[key])
    for key in ("persona_tags", "reason_codes", "breakevens"):
        values[key] = tuple(values[key])
    if row.get("execution_eligibility") is not None:
        raise ValueError("detector source requires research-only candidate permission")
    return OptionCandidate(**values, legs=tuple(retained_leg(leg) for leg in legs), execution_eligibility=None)


def validate_alert_package(structure: StructureType, legs: tuple[CandidateLeg, ...]) -> None:
    if tuple(leg.leg_index for leg in legs) != tuple(range(len(legs))):
        raise ValueError("package leg indexes must be contiguous and ordered")
    if not legs or len({leg.contract_id for leg in legs}) != len(legs):
        raise ValueError("package requires distinct listed contracts")
    if len({leg.expiration_date for leg in legs}) != 1 or any(leg.multiplier != 100 for leg in legs):
        raise ValueError("only same-expiry standard 100-share packages are supported")
    ordered = tuple(sorted(legs, key=lambda leg: leg.strike))
    signatures = {
        StructureType.CASH_SECURED_PUT: (("PUT", "SELL", 1),),
        StructureType.LONG_CALL: (("CALL", "BUY", 1),),
        StructureType.LONG_PUT: (("PUT", "BUY", 1),),
        StructureType.CALL_DEBIT_VERTICAL: (("CALL", "BUY", 1), ("CALL", "SELL", 1)),
        StructureType.PUT_DEBIT_VERTICAL: (("PUT", "SELL", 1), ("PUT", "BUY", 1)),
        StructureType.CALL_CREDIT_VERTICAL: (("CALL", "SELL", 1), ("CALL", "BUY", 1)),
        StructureType.PUT_CREDIT_VERTICAL: (("PUT", "BUY", 1), ("PUT", "SELL", 1)),
        StructureType.IRON_CONDOR: (("PUT", "BUY", 1), ("PUT", "SELL", 1), ("CALL", "SELL", 1), ("CALL", "BUY", 1)),
        StructureType.CALL_BUTTERFLY: (("CALL", "BUY", 1), ("CALL", "SELL", 2), ("CALL", "BUY", 1)),
        StructureType.PUT_BUTTERFLY: (("PUT", "BUY", 1), ("PUT", "SELL", 2), ("PUT", "BUY", 1)),
    }
    if tuple((leg.contract_type.value, leg.side.value, leg.ratio) for leg in ordered) != signatures.get(structure):
        raise ValueError("listed legs do not match the named structure")
    if len({leg.strike for leg in ordered}) != len(ordered):
        raise ValueError("package strikes must be distinct")
    if structure in {StructureType.CALL_BUTTERFLY, StructureType.PUT_BUTTERFLY} and ordered[1].strike - ordered[0].strike != ordered[2].strike - ordered[1].strike:
        raise ValueError("butterfly wings must have equal width")


def retained_open_interest_evidence(detail: Mapping[str, object]) -> AlertInputEvidence:
    candidate = detail["candidate"]
    cutoff = retained_time(candidate["observed_time"])
    market = retained_time(candidate["market_data_time"])
    if candidate["strategy_name"] == "SPREAD_RANGE_LOCATOR":
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("MATRIX_WIDE_OI_LINEAGE_REQUIRED",))
    sources = tuple(detail.get("source_snapshots", ()))
    if not sources:
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_SOURCE_MISSING",))
    try:
        session = market.astimezone(ZoneInfo("America/New_York")).date()
        settlement = OptionExchangeCalendar().previous_session(session)
        links: list[str] = []
        availability: list[datetime] = []
        for snapshot in sources:
            matches = [row for row in detail.get("open_interest_evidence", ())
                       if row.get("contract_id") == snapshot.get("contract_id")
                       and row.get("underlying") == candidate["underlying"]
                       and str(row.get("settlement_session")) == settlement.isoformat()]
            if len(matches) != 1:
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_ROW_MISSING_OR_AMBIGUOUS",))
            row = matches[0]
            original_at = retained_time(row["open_interest_observed_at"])
            if row.get("open_interest_source") != "PROVIDER_CHAIN_SNAPSHOT" or original_at > cutoff:
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_NOT_AVAILABLE_AT_CUTOFF",))
            if str(row.get("open_interest_observed_session")) != session.isoformat():
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_SESSION_MISMATCH",))
            revisions = row.get("open_interest_revision_count", 0)
            if type(revisions) is not int or revisions < 0:
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_REVISION_METADATA_INVALID",))
            value, available_at, version = row["open_interest"], original_at, "original"
            if revisions:
                revised_at = retained_time(row["open_interest_revised_observed_at"])
                if revised_at < original_at:
                    return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_REVISION_METADATA_INVALID",))
                if revised_at <= cutoff:
                    value, available_at, version = row["open_interest_revised_value"], revised_at, f"revision-{revisions}"
                elif revisions > 1:
                    return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_INTERMEDIATE_REVISION_UNAVAILABLE",))
            if type(value) is not int or value < 0 or snapshot.get("open_interest") is None:
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_VALUE_UNAVAILABLE",))
            if value != snapshot["open_interest"]:
                return AlertInputEvidence(GateVerdict.FAIL, reasons=("DATED_OI_SNAPSHOT_MISMATCH",))
            if candidate["strategy_name"] == "VOLUME_OI_ANOMALY":
                primary = candidate.get("primary_evidence") or {}
                if primary.get("contract_id") != snapshot["contract_id"] or primary.get("open_interest") != value:
                    return AlertInputEvidence(GateVerdict.FAIL, reasons=("DATED_OI_DETECTOR_VALUE_MISMATCH",))
                if value == 0:
                    return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("VOLUME_OI_DENOMINATOR_ZERO",))
            links.append(f"option-daily-oi:{snapshot['contract_id']}:{settlement}:{version}:{available_at.isoformat()}:{value}")
            availability.append(available_at)
        return AlertInputEvidence(GateVerdict.PASS, tuple(links), max(availability))
    except (ValueError, TypeError, KeyError):
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("DATED_OI_LINEAGE_INVALID",))


def retained_gamma_evidence(detail: Mapping[str, object]) -> AlertInputEvidence:
    candidate = detail["candidate"]
    primary = candidate.get("primary_evidence") or {}
    if "gamma_evidence_version" in primary or "gamma_matrix_id" in primary:
        if primary.get("gamma_evidence_version") != "gamma_wall_evidence_v1":
            return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_EVIDENCE_VERSION_NOT_SUPPORTED",))
        if primary.get("gamma_matrix_id") != str(candidate["matrix_id"]):
            return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_CANDIDATE_MATRIX_NOT_PINNED",))
    policy_hash = primary.get("gamma_policy_sha256")
    if not isinstance(policy_hash, str) or len(policy_hash) != 64 or any(character not in "0123456789abcdef" for character in policy_hash):
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_POLICY_NOT_PINNED_IN_CANDIDATE",))
    scope = primary.get("gamma_scope")
    if scope not in {"TOTAL", "ZERO_DTE", "WEEKLY", "MONTHLY"} or candidate["strategy_name"] == "ZERO_DTE_GAMMA_SQUEEZE" and scope != "ZERO_DTE":
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_SCOPE_NOT_VERIFIED",))
    matches = [row for row in detail.get("gamma_evidence", ())
               if str(row.get("matrix_id")) == str(candidate["matrix_id"])
               and row.get("underlying") == candidate["underlying"]
               and row.get("scope") == scope and row.get("gamma_policy_sha256") == policy_hash]
    if len(matches) != 1:
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_PROFILE_MISSING_OR_AMBIGUOUS",))
    profile = matches[0]
    try:
        available_at = retained_time(profile["first_observed_at"])
        if available_at > retained_time(candidate["observed_time"]) or retained_time(profile["market_data_time"]) > retained_time(candidate["market_data_time"]):
            return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_PROFILE_AFTER_CANDIDATE_CUTOFF",))
        count = profile["contributing_contract_count"]
        eligible = profile["eligible_contract_count"]
        coverage = Decimal(str(profile["coverage_fraction"]))
        if (profile["quality_reasons"] or type(count) is not int or type(eligible) is not int
                or count <= 0 or eligible < count or not coverage.is_finite() or not 0 < coverage <= 1
                or profile["shares_per_contract"] != 100):
            return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_PROFILE_QUALITY_NOT_VERIFIED",))
        if profile["dealer_convention"] != primary.get("dealer_convention") or profile["regime_at_spot"] != primary.get("gamma_regime"):
            return AlertInputEvidence(GateVerdict.FAIL, reasons=("GAMMA_RECORDED_CONTEXT_MISMATCH",))
        if candidate["strategy_name"] == "ZERO_DTE_GAMMA_SQUEEZE":
            market_session = retained_time(candidate["market_data_time"]).astimezone(ZoneInfo("America/New_York")).date()
            if not detail.get("legs") or any(str(row["expiration_date"]) != market_session.isoformat() for row in detail["legs"]):
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_ZERO_DTE_CONTRACT_MISMATCH",))
            if primary.get("gamma_wall_strike") is None or not any(Decimal(str(row["strike"])) == Decimal(str(primary["gamma_wall_strike"])) for row in detail.get("legs", ())):
                return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_WALL_NOT_LINKED_TO_PACKAGE",))
        return AlertInputEvidence(GateVerdict.PASS, (str(profile["gamma_profile_id"]), policy_hash), available_at,
                                  ("MODELED_DEALER_CONVENTION_NOT_OBSERVED_POSITIONING", "RETAINED_CHAIN_SCOPE_NOT_TOTAL_MARKET"))
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return AlertInputEvidence(GateVerdict.UNAVAILABLE, reasons=("GAMMA_PROFILE_LINEAGE_INVALID",))


def retained_event_horizon(
    detail: Mapping[str, object], window_start: datetime, window_end: datetime,
    *, maximum_age_seconds: int = 43200,
) -> dict[str, object]:
    window_start, window_end = retained_time(window_start), retained_time(window_end)
    candidate = detail["candidate"]
    cutoff = retained_time(candidate["observed_time"])
    if window_start < cutoff or window_end <= window_start or window_end - window_start > timedelta(days=366):
        raise ValueError("event horizon must follow the candidate cutoff and span at most 366 days")
    if type(maximum_age_seconds) is not int or maximum_age_seconds <= 0:
        raise ValueError("event coverage age must be a positive integer")
    source_names = {row.get("source") for row in detail.get("event_coverage_evidence", ()) if row.get("source")}
    checks = []
    for event_type, underlying in (("EARNINGS", candidate["underlying"]), ("FED_RATE_DECISION", None)):
        if event_type == "EARNINGS":
            try:
                snapshots = detail.get("source_snapshots", ())
                exempt = bool(snapshots) and all(row.get("underlying_asset_type") == "ETF"
                    and row["underlying"] == underlying
                    and retained_time(row["underlying_asset_type_observed_at"]) <= cutoff
                    for row in snapshots)
                if exempt:
                    checks.append({"event_type": event_type, "status": "NOT_APPLICABLE",
                                   "source_ids": sorted({f"option-ingestion:{row['batch_id']}" for row in snapshots}),
                                   "reasons": ["VERIFIED_ETF_UNDERLYING"]})
                    continue
            except (ValueError, TypeError, KeyError):
                pass
        coverage = []
        latest_events: dict[tuple[str, str], Mapping[str, object]] = {}
        malformed = len(source_names) > 1
        latest_coverage: dict[tuple[str, str], Mapping[str, object]] = {}
        for row in detail.get("holding_event_coverage", ()):
            if row.get("source") not in source_names:
                continue
            try:
                received = retained_time(row["first_observed_at"])
                if received > cutoff:
                    continue
                key = (row["source"], row["source_key"])
                previous = latest_coverage.get(key)
                if previous is None or received > retained_time(previous["first_observed_at"]):
                    latest_coverage[key] = row
                elif received == retained_time(previous["first_observed_at"]) and row["coverage_id"] != previous["coverage_id"]:
                    malformed = True
            except (ValueError, TypeError, KeyError):
                malformed = True
        for row in latest_coverage.values():
            if row.get("event_type") != event_type or row.get("affected_underlying") != underlying:
                continue
            try:
                received = retained_time(row["first_observed_at"])
                declared = retained_time(row["source_observed_at"])
                start, end = retained_time(row["window_start"]), retained_time(row["window_end"])
                if received <= cutoff and declared <= cutoff and min(received, declared) >= window_start - timedelta(seconds=maximum_age_seconds) and start < end:
                    coverage.append((start, end, str(row["coverage_id"])))
            except (ValueError, TypeError, KeyError):
                malformed = True
        for row in detail.get("holding_event_evidence", ()):
            if row.get("event_type") != event_type or row.get("source") not in source_names:
                continue
            try:
                received = retained_time(row.get("revised_observed_at") or row["first_observed_at"])
                if received > cutoff or retained_time(row["first_observed_at"]) > cutoff:
                    continue
                key = (row["source"], row["source_key"])
                previous = latest_events.get(key)
                if previous is None or received > retained_time(previous.get("revised_observed_at") or previous["first_observed_at"]):
                    latest_events[key] = row
                elif received == retained_time(previous.get("revised_observed_at") or previous["first_observed_at"]) and row["market_event_id"] != previous["market_event_id"]:
                    malformed = True
            except (ValueError, TypeError, KeyError):
                malformed = True
        blocked, uncertain, event_ids = [], [], []
        for row in latest_events.values():
            if row.get("affected_underlying") != underlying:
                continue
            try:
                if row["status"] == "CANCELED":
                    event_ids.append(str(row["market_event_id"]))
                    continue
                if row["status"] not in {"SCHEDULED", "REVISED", "COMPLETED"}:
                    malformed = True
                    continue
                scheduled = retained_time(row["scheduled_time"])
                if row.get("confidence") == "CONFIRMED":
                    if window_start <= scheduled <= window_end:
                        blocked.append(str(row["market_event_id"]))
                elif window_start.astimezone(ZoneInfo("America/New_York")).date() <= scheduled.astimezone(ZoneInfo("America/New_York")).date() <= window_end.astimezone(ZoneInfo("America/New_York")).date():
                    uncertain.append(str(row["market_event_id"]))
            except (ValueError, TypeError, KeyError):
                malformed = True
        through = window_start
        coverage_ids = []
        for start, end, source_id in sorted(coverage):
            if start > through:
                break
            if end > through:
                through = end
                coverage_ids.append(source_id)
            if through >= window_end:
                break
        status = "BLOCKED" if blocked else "UNKNOWN" if uncertain or malformed else "CLEAR" if through >= window_end else "UNAVAILABLE"
        checks.append({"event_type": event_type, "status": status,
                       "source_ids": sorted(set(coverage_ids + event_ids + blocked + uncertain)),
                       "reasons": ["EVENT_IN_HOLDING_WINDOW"] if blocked else ["EVENT_TIME_OR_LINEAGE_UNCERTAIN"] if uncertain or malformed else [] if through >= window_end else ["HOLDING_WINDOW_COVERAGE_MISSING_OR_STALE"]})
    return {
        "version": "option_alert_event_horizon_v1", "window_start": window_start.isoformat(), "window_end": window_end.isoformat(),
        "evidence_cutoff": cutoff.isoformat(), "maximum_age_seconds": maximum_age_seconds,
        "checks": checks, "covered_event_types": ["EARNINGS", "FED_RATE_DECISION"],
        "unavailable_event_types": ["EX_DIVIDEND", "CORPORATE_ACTION"],
        "status": "BLOCKED" if any(row["status"] == "BLOCKED" for row in checks) else "UNAVAILABLE",
        "complete_options_event_coverage": False,
    }


def retained_alert_evidence(detail: Mapping[str, object], decision_at: datetime, *, holding_until: datetime | None = None) -> dict[AlertInput, AlertInputEvidence]:
    candidate = detail["candidate"]
    observed = retained_time(candidate["observed_time"])
    market = retained_time(candidate["market_data_time"])
    candidate_id = str(candidate["candidate_id"])
    evidence: dict[AlertInput, AlertInputEvidence] = {}

    def record(field, verdict, reason=None, source_ids=(candidate_id,)):
        evidence[field] = AlertInputEvidence(verdict, tuple(source_ids), observed, (reason,) if reason else ())

    record(AlertInput.SELECTED_CANDIDATE, GateVerdict.PASS if candidate.get("status") == "SELECTED" else GateVerdict.FAIL, None if candidate.get("status") == "SELECTED" else "CANDIDATE_NOT_SELECTED")
    sources = {str(row["snapshot_id"]): row for row in detail.get("source_snapshots", ())}
    source_ids = tuple(sources)
    rows = detail.get("legs", ())
    observation_only = candidate.get("candidate_kind") == "RESEARCH_ONLY"
    identity_ok = bool(sources)
    if observation_only:
        identity_ok = identity_ok and not rows and len(sources) == 1 and next(iter(sources.values()))["contract_id"] == candidate.get("source_contract_id")
    else:
        identity_ok = identity_ok and len(rows) == len(sources)
        for row in rows:
            source = sources.get(str(row["snapshot_id"]))
            identity_ok = identity_ok and source is not None and all(
                str(row[key]) == str(source[key]) for key in ("contract_id", "contract_ticker", "expiration_date", "contract_type")
            ) and Decimal(str(row["strike"])) == Decimal(str(source["strike"])) and row["multiplier"] == source["shares_per_contract"]
    identity_ok = identity_ok and all(row.get("underlying") == candidate["underlying"] and row.get("shares_per_contract") == 100 and row.get("exercise_style") == "AMERICAN" for row in sources.values())
    record(AlertInput.CONTRACT_IDENTITY, GateVerdict.PASS if identity_ok else GateVerdict.UNAVAILABLE if not sources else GateVerdict.FAIL, None if identity_ok else "SOURCE_IDENTITY_NOT_VERIFIED", source_ids or (candidate_id,))
    causal = bool(sources) and market <= observed <= decision_at
    try:
        causal = causal and all(
            retained_time(row["market_data_time"]) <= market
            and retained_time(row.get("revised_observed_at") or row["first_observed_at"]) <= observed
            and retained_time(row["expiration_cutoff"]) > market
            for row in sources.values()
        ) and all(retained_time(row["source_market_time"]) <= market for row in rows)
    except (ValueError, TypeError, KeyError):
        causal = False
    record(AlertInput.SOURCE_CAUSALITY, GateVerdict.PASS if causal else GateVerdict.UNAVAILABLE, None if causal else "SOURCE_CUTOFF_NOT_VERIFIED", source_ids or (candidate_id,))
    legs: tuple[CandidateLeg, ...] = ()
    if rows:
        try:
            legs = tuple(retained_leg(row) for row in rows)
            validate_alert_package(StructureType(candidate["structure_type"]), legs)
            expected_kind = "SINGLE_CONTRACT" if len(legs) == 1 else "MULTI_LEG"
            if candidate.get("candidate_kind") != expected_kind:
                raise ValueError("candidate kind does not match its listed package")
            provenance = {(leg.valuation_policy_version, leg.valuation_policy_sha256, leg.model_version) for leg in legs}
            source_provenance = {(row.get("valuation_policy_version"), row.get("valuation_policy_sha256"), row.get("model_version")) for row in sources.values()}
            valuation_ok = identity_ok and causal and len(provenance) == 1 and provenance == source_provenance and all(leg.valuation_policy_sha256 and not leg.quality_flags for leg in legs)
            valuation_ok = valuation_ok and all(
                row.get("iv_converged") is True and Decimal(str(row.get("model_mark"))) == leg.model_mark
                and row.get("mark_source") == leg.mark_source
                and row.get("model_version") == leg.model_version
                for leg in legs for row in (sources[str(leg.snapshot_id)],)
            )
            record(AlertInput.VALUATION, GateVerdict.PASS if valuation_ok else GateVerdict.UNAVAILABLE, None if valuation_ok else "VALUATION_LINEAGE_NOT_VERIFIED", source_ids or (candidate_id,))
            payoff = evaluate_terminal_payoff(legs)
            economics_ok = payoff.bounded_maximum_loss and payoff.maximum_loss is not None and payoff.maximum_loss > 0
            for key in ("net_premium", "maximum_profit", "maximum_loss"):
                expected = getattr(payoff, key)
                retained = candidate.get(key)
                economics_ok = economics_ok and ((expected is None and retained is None) or (expected is not None and retained is not None and abs(expected - Decimal(str(retained))) <= Decimal("0.01")))
            capital = Decimal(str(candidate.get("capital_at_risk")))
            economics_ok = economics_ok and capital.is_finite() and capital >= payoff.maximum_loss
            breakevens = tuple(Decimal(str(value)) for value in candidate.get("breakevens", ()))
            economics_ok = economics_ok and len(breakevens) == len(payoff.breakevens) and all(
                value.is_finite() and abs(value - expected) <= Decimal("0.01")
                for value, expected in zip(breakevens, payoff.breakevens)
            )
            if candidate["structure_type"] == "CASH_SECURED_PUT":
                collateral = Decimal(str(candidate.get("collateral_required")))
                economics_ok = economics_ok and collateral.is_finite() and collateral >= legs[0].strike * legs[0].multiplier * legs[0].ratio
            record(AlertInput.PACKAGE_ECONOMICS, GateVerdict.PASS if economics_ok else GateVerdict.FAIL, None if economics_ok else "PACKAGE_PAYOFF_MISMATCH")
        except (KeyError, ValueError, TypeError, ArithmeticError):
            record(AlertInput.PACKAGE_ECONOMICS, GateVerdict.FAIL, "PACKAGE_CONTRACT_INVALID")
    context = candidate.get("decision_context") or {}
    bullish = candidate["structure_type"] in {"LONG_CALL", "CALL_DEBIT_VERTICAL", "PUT_CREDIT_VERTICAL"}
    bearish = candidate["structure_type"] in {"LONG_PUT", "PUT_DEBIT_VERTICAL", "CALL_CREDIT_VERTICAL"}
    if context.get("equity_context_snapshot_id") and (bullish or bearish) and context.get("qualified_direction"):
        aligned = context["qualified_direction"] == ("BULLISH" if bullish else "BEARISH") and context.get("equity_context_status") != "CONFLICTED" and not context.get("equity_reason_codes")
        record(AlertInput.DIRECTION, GateVerdict.PASS if aligned else GateVerdict.FAIL, None if aligned else "DIRECTION_OPPOSED_OR_CONFLICTED", (str(context["equity_context_snapshot_id"]),))
    primary = candidate.get("primary_evidence") or {}
    if candidate["strategy_name"] == "VOLUME_OI_ANOMALY" and identity_ok and causal:
        source = next(iter(sources.values()))
        if primary.get("day_volume") is not None and primary.get("day_volume") == source.get("day_volume") and primary.get("contract_id") == source.get("contract_id"):
            record(AlertInput.ACTIVITY, GateVerdict.PASS, source_ids=source_ids)
    if identity_ok and causal:
        evidence[AlertInput.OPEN_INTEREST] = retained_open_interest_evidence(detail)
    else:
        record(AlertInput.OPEN_INTEREST, GateVerdict.UNAVAILABLE, "DATED_OI_SOURCE_NOT_VERIFIED")
    if holding_until is None:
        record(AlertInput.EVENT_HORIZON, GateVerdict.UNAVAILABLE, "FULL_HOLDING_HORIZON_NOT_DEFINED")
    else:
        horizon = retained_event_horizon(detail, decision_at, holding_until)
        links = tuple(sorted({source for check in horizon["checks"] for source in check["source_ids"]}))
        record(AlertInput.EVENT_HORIZON, GateVerdict.FAIL if horizon["status"] == "BLOCKED" else GateVerdict.UNAVAILABLE,
               "EVENT_IN_HOLDING_WINDOW" if horizon["status"] == "BLOCKED" else "EX_DIVIDEND_AND_CORPORATE_ACTION_COVERAGE_UNAVAILABLE", links or (candidate_id,))
    if identity_ok and causal:
        evidence[AlertInput.GAMMA_CONTEXT] = retained_gamma_evidence(detail)
    else:
        record(AlertInput.GAMMA_CONTEXT, GateVerdict.UNAVAILABLE, "GAMMA_SOURCE_NOT_VERIFIED")
    record(AlertInput.COMPARABLE_IV_HISTORY, GateVerdict.UNAVAILABLE, "COMPARABLE_IV_LINEAGE_NOT_ASSESSED")
    if candidate.get("valid_until"):
        still_open = decision_at < retained_time(candidate["valid_until"])
        record(AlertInput.ENTRY_WINDOW, GateVerdict.UNAVAILABLE if still_open else GateVerdict.FAIL, "ENTRY_PLAN_NOT_DEFINED" if still_open else "ORIGINAL_ENTRY_WINDOW_EXPIRED")
    for gate_name, field in (("QUOTE_LIQUIDITY", AlertInput.QUOTE_LIQUIDITY), ("RISK_ENGINE", AlertInput.PAPER_RISK)):
        gates = [gate for gate in detail.get("execution_gates", ()) if gate["gate_name"] == gate_name]
        if gates and gates[0]["verdict"] != "PASS":
            gate = gates[0]
            evidence[field] = AlertInputEvidence(GateVerdict(gate["verdict"]), (f"{candidate_id}:{gate['ledger_version']}:{gate_name}",), retained_time(gate["evaluated_at"]), tuple(gate["reason_codes"]))
    record(AlertInput.EXECUTION_LEDGER, GateVerdict.UNAVAILABLE, "CURRENT_EXECUTION_AUTHORIZATION_NOT_ASSESSED")
    return evidence