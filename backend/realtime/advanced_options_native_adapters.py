"""Pure Advanced O1/O3 adapters over exact frozen cycle inputs."""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Mapping
from uuid import UUID

from options.domain import ContractType, OptionContractSnapshot
from options.dual_origin import (
    ActivitySource,
    CanonicalTrendSignalDecision,
    LatestCompletedSignalDecision,
    apply_activity_liquidity_floor,
    assess_canonical_stock_first,
)
from options.stock_bar_detection import CanonicalStockSignal
from options.strategies.domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    ExecutionEligibility,
    OptionCandidate,
    OptionSide,
    StructureRiskClass,
    StructureType,
)

from .advanced_options_cycle import (
    AdvancedOptionsDetectorContext,
    AdvancedOptionsNativeEvaluation,
    AdvancedOptionsNativeLaneResult,
    NativeDetectorExecutor,
    NativeLaneState,
)
from .advanced_options_o1_liquidity_floor import (
    O1LiquidityFloorMode,
    log_o1_liquidity_floor_documents,
    o1_liquidity_floor_documents,
    o1_liquidity_floor_entry,
    o1_liquidity_floor_mode,
    o1_liquidity_floor_policy,
)
from .advanced_options_o1_gates import O1GateRun
from .advanced_options_alert_context import note_o1_alert_context, note_os_alert_context  # G106 alert context
from .contracts import canonical_sha256
from .detector_adapters import (
    run_o1_adapter,
    run_o3_adapter,
)
from .option_portal_adapter import RealtimeOptionPackage
from .preflight import DetectorFamily, ProviderTier


def _plain(value):
    if hasattr(value, "model_dump"):
        return _plain(value.model_dump(mode="json"))
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, SimpleNamespace):
        return {
            str(key): _plain(item)
            for key, item in vars(value).items()
        }
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_plain(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(
        f"Advanced Options detector payload is unsupported: {type(value).__name__}"
    )


def _input_sha256(
    context: AdvancedOptionsDetectorContext,
) -> str:
    return canonical_sha256({
        "input_closure_sha256": context.input_gate.closure_sha256,
        "input_receipt_sha256": canonical_sha256(context.input_receipt),
        "detector": context.detector,
        "cutoff": context.cutoff,
    })


@dataclass(frozen=True, slots=True)
class AdvancedOptionsLaneInputs:
    invocations: tuple[object, ...] = ()
    blocked_reason_codes: tuple[str, ...] = ()
    # G106 AL5 (REALTIME_V3_OS_PARTICIPATION RECORD/ON): what OS policy v2 decided for this lane, logged by the
    # executor once per window. Never part of any identity or comparison; None when the switch is OFF.
    record: object | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        reasons = tuple(
            reason.strip().upper()
            for reason in self.blocked_reason_codes
        )
        if any(not reason for reason in reasons):
            raise ValueError("Advanced Options lane reason cannot be blank")
        if reasons and self.invocations:
            raise ValueError(
                "blocked Advanced Options lane cannot retain invocations"
            )
        object.__setattr__(self, "blocked_reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedO1Invocation:
    event_id: UUID
    source: ActivitySource
    package: RealtimeOptionPackage | None = None
    # G106 AL2 (REALTIME_V3_O1_DEBIT_VERTICAL RECORD/ON): what the debit-vertical rule decided for this event, logged
    # once per window (O1_DEBIT_VERTICAL_RECORD). Not part of any input identity; None when the switch is OFF.
    debit_vertical: object | None = None
    # G106 AL7 (REALTIME_V3_O1_AGGRESSOR_SIDE RECORD/ON): the premium of the contract's stored prints this session at or
    # before the event, by aggressor side (an O1AggressorTally). Never compared; in the input identity only under ON.
    aggressor: object | None = field(default=None, compare=False)


@dataclass(frozen=True, slots=True)
class AdvancedO3Invocation:
    event_id: UUID
    candidate: object
    decision: object
    activity_contract_id: int
    snapshots: tuple
    structural_invalidation: Decimal
    scheduled_cycle: datetime
    package: RealtimeOptionPackage


@dataclass(frozen=True, slots=True)
class AdvancedOSInvocation:
    event_id: UUID
    signal: CanonicalStockSignal
    source: ActivitySource | None
    package: RealtimeOptionPackage | None = None


@dataclass(frozen=True, slots=True)
class AdvancedOSParticipationInvocation:
    """G106 AL5 (REALTIME_V3_OS_PARTICIPATION=ON only): one live stock signal and what OS policy v2 decided for it at
    this cutoff (`outcome`: an OSParticipationOutcome); `package` = the confirming contract at the cutoff mark."""

    event_id: UUID
    signal: CanonicalStockSignal
    outcome: object
    package: RealtimeOptionPackage | None
    policy_sha256: str


def advanced_o3_invocation_document(
    invocation: AdvancedO3Invocation,
) -> dict[str, object]:
    if not isinstance(invocation, AdvancedO3Invocation):
        raise TypeError("Advanced O3 invocation is invalid")
    return {
        "contract": "advanced-options-o3-invocation-v1",
        "event_id": str(invocation.event_id),
        "candidate": _plain(invocation.candidate),
        "decision": _plain(invocation.decision),
        "decision_model": (
            "LATEST_COMPLETED_SIGNAL_DECISION"
            if hasattr(invocation.decision, "model_dump")
            else "ATTRIBUTE_RECORD"
        ),
        "activity_contract_id": invocation.activity_contract_id,
        "snapshots": _plain(invocation.snapshots),
        "snapshot_model": (
            "OPTION_CONTRACT_SNAPSHOT"
            if all(hasattr(row, "model_dump") for row in invocation.snapshots)
            else "ATTRIBUTE_RECORD"
        ),
        "structural_invalidation": str(
            invocation.structural_invalidation
        ),
        "scheduled_cycle": invocation.scheduled_cycle.isoformat(),
        "package_candidate_id": str(invocation.package.candidate_id),
    }


def _candidate_leg(value: Mapping[str, object]) -> CandidateLeg:
    return CandidateLeg(
        leg_index=int(value["leg_index"]),
        snapshot_id=UUID(str(value["snapshot_id"])),
        contract_id=int(value["contract_id"]),
        contract_ticker=str(value["contract_ticker"]),
        side=OptionSide(str(value["side"])),
        ratio=int(value["ratio"]),
        multiplier=int(value["multiplier"]),
        expiration_date=date.fromisoformat(str(value["expiration_date"])),
        strike=Decimal(str(value["strike"])),
        contract_type=ContractType(str(value["contract_type"])),
        spot=Decimal(str(value["spot"])),
        time_to_expiration_years=float(value["time_to_expiration_years"]),
        risk_free_rate=float(value["risk_free_rate"]),
        dividend_yield=float(value["dividend_yield"]),
        model_mark=Decimal(str(value["model_mark"])),
        local_iv=float(value["local_iv"]),
        local_delta=float(value["local_delta"]),
        local_gamma=float(value["local_gamma"]),
        local_theta_per_day=float(value["local_theta_per_day"]),
        local_vega_per_vol_point=float(value["local_vega_per_vol_point"]),
        local_rho_per_rate_point=float(value["local_rho_per_rate_point"]),
        source_market_time=datetime.fromisoformat(
            str(value["source_market_time"])
        ),
        mark_source=str(value["mark_source"]),
        model_version=str(value["model_version"]),
        quality_flags=tuple(value["quality_flags"]),
        valuation_policy_version=(
            None
            if value["valuation_policy_version"] is None
            else str(value["valuation_policy_version"])
        ),
        valuation_policy_sha256=(
            None
            if value["valuation_policy_sha256"] is None
            else str(value["valuation_policy_sha256"])
        ),
    )


def _candidate(value: Mapping[str, object]) -> OptionCandidate:
    return OptionCandidate(
        candidate_id=UUID(str(value["candidate_id"])),
        identity_sha256=str(value["identity_sha256"]),
        matrix_id=UUID(str(value["matrix_id"])),
        strategy_name=str(value["strategy_name"]),
        strategy_version=str(value["strategy_version"]),
        underlyer=str(value["underlyer"]),
        candidate_kind=CandidateKind(str(value["candidate_kind"])),
        strategy_archetype=str(value["strategy_archetype"]),
        persona_tags=tuple(value["persona_tags"]),
        structure_type=StructureType(str(value["structure_type"])),
        structure_risk_class=StructureRiskClass(
            str(value["structure_risk_class"])
        ),
        expiration_date=(
            None
            if value["expiration_date"] is None
            else date.fromisoformat(str(value["expiration_date"]))
        ),
        rank=int(value["rank"]),
        status=CandidateStatus(str(value["status"])),
        primary_metric_name=(
            None
            if value["primary_metric_name"] is None
            else str(value["primary_metric_name"])
        ),
        primary_metric_value=(
            None
            if value["primary_metric_value"] is None
            else float(value["primary_metric_value"])
        ),
        rank_components=dict(value["rank_components"]),
        primary_evidence=dict(value["primary_evidence"]),
        legs=tuple(_candidate_leg(row) for row in value["legs"]),
        net_premium=(
            None
            if value["net_premium"] is None
            else Decimal(str(value["net_premium"]))
        ),
        collateral_required=(
            None
            if value["collateral_required"] is None
            else Decimal(str(value["collateral_required"]))
        ),
        capital_at_risk=(
            None
            if value["capital_at_risk"] is None
            else Decimal(str(value["capital_at_risk"]))
        ),
        maximum_profit=(
            None
            if value["maximum_profit"] is None
            else Decimal(str(value["maximum_profit"]))
        ),
        maximum_loss=(
            None
            if value["maximum_loss"] is None
            else Decimal(str(value["maximum_loss"]))
        ),
        return_on_collateral=(
            None
            if value["return_on_collateral"] is None
            else float(value["return_on_collateral"])
        ),
        return_on_risk=(
            None
            if value["return_on_risk"] is None
            else float(value["return_on_risk"])
        ),
        breakevens=tuple(
            Decimal(str(item)) for item in value["breakevens"]
        ),
        execution_eligibility=(
            None
            if value["execution_eligibility"] is None
            else ExecutionEligibility(str(value["execution_eligibility"]))
        ),
        reason_codes=tuple(value["reason_codes"]),
        management_policy_version=(
            None
            if value["management_policy_version"] is None
            else str(value["management_policy_version"])
        ),
        management_policy=dict(value["management_policy"]),
        policy_sha256=str(value["policy_sha256"]),
        model_version=str(value["model_version"]),
        context_snapshot_id=(
            None
            if value["context_snapshot_id"] is None
            else UUID(str(value["context_snapshot_id"]))
        ),
        iv_context_id=(
            None
            if value["iv_context_id"] is None
            else UUID(str(value["iv_context_id"]))
        ),
        market_data_time=datetime.fromisoformat(
            str(value["market_data_time"])
        ),
        observed_time=datetime.fromisoformat(str(value["observed_time"])),
        valid_until=(
            None
            if value["valid_until"] is None
            else datetime.fromisoformat(str(value["valid_until"]))
        ),
    )


def decode_advanced_o3_invocation_document(
    document: Mapping[str, object],
    *,
    package: RealtimeOptionPackage,
) -> AdvancedO3Invocation:
    if document.get("contract") != "advanced-options-o3-invocation-v1":
        raise ValueError("Advanced O3 invocation document version changed")
    if UUID(str(document["package_candidate_id"])) != package.candidate_id:
        raise ValueError("Advanced O3 invocation package changed")
    def namespace(value):
        if isinstance(value, Mapping):
            return SimpleNamespace(**{
                str(key): namespace(item) for key, item in value.items()
            })
        if isinstance(value, list):
            return tuple(namespace(item) for item in value)
        return value

    if document["decision_model"] == "LATEST_COMPLETED_SIGNAL_DECISION":
        # R44 (Nov 13): the O3 input producer retains the O1 canonical-trend decision (schema v6), the decision the
        # developer path gives O3 (`assess_options_credit_canonical_trend`); any other model document is v4 as before.
        decision_model = (
            CanonicalTrendSignalDecision
            if isinstance(document["decision"], Mapping)
            and document["decision"].get("schema_version") == "dual_origin_signal_decision_v6"
            else LatestCompletedSignalDecision
        )
        decision = decision_model.model_validate(document["decision"])
    else:
        decision_document = dict(document["decision"])
        decision_document["decision_at"] = datetime.fromisoformat(
            str(decision_document["decision_at"])
        )
        decision_document["valid_until"] = datetime.fromisoformat(
            str(decision_document["valid_until"])
        )
        activity = dict(decision_document["activity"])
        activity["episode_id"] = UUID(str(activity["episode_id"]))
        decision_document["activity"] = SimpleNamespace(**activity)
        decision = SimpleNamespace(**decision_document)
    snapshots = (
        tuple(
            OptionContractSnapshot.model_validate(row)
            for row in document["snapshots"]
        )
        if document["snapshot_model"] == "OPTION_CONTRACT_SNAPSHOT"
        else tuple(
            SimpleNamespace(**{
                **{
                    str(key): namespace(item)
                    for key, item in row.items()
                },
                "snapshot_id": UUID(str(row["snapshot_id"])),
            })
            for row in document["snapshots"]
        )
    )
    invocation = AdvancedO3Invocation(
        event_id=UUID(str(document["event_id"])),
        candidate=_candidate(document["candidate"]),
        decision=decision,
        activity_contract_id=int(document["activity_contract_id"]),
        snapshots=snapshots,
        structural_invalidation=Decimal(
            str(document["structural_invalidation"])
        ),
        scheduled_cycle=datetime.fromisoformat(
            str(document["scheduled_cycle"])
        ),
        package=package,
    )
    if invocation.candidate.candidate_id != package.candidate_id:
        raise ValueError("Advanced O3 candidate identity changed")
    return invocation


def _blocked_or_none(
    context: AdvancedOptionsDetectorContext,
    expected: DetectorFamily,
    inputs: AdvancedOptionsLaneInputs,
) -> AdvancedOptionsNativeLaneResult | None:
    if context.detector is not expected:
        raise ValueError("Advanced Options executor detector changed")
    if inputs.blocked_reason_codes:
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.BLOCKED,
            inputs.blocked_reason_codes,
        )
    return None


# Nov 9 hold, lifted Nov 10: O1 packages carried OPTIONS_ALERT_V1_VALUATION_SHA256 while the window marks under the
# session's valuation policy, so every selected package failed its whole window (PACKAGE_ENTRY_VALUATION_POLICY_MISMATCH).
# Packages are now stamped with the session policy (user's decision) and one that cannot be marked no longer fails the
# window (PACKAGE_MARK_UNAVAILABLE). REALTIME_V3_O1_PACKAGE_SELECTION=DISABLED on the window service turns selection
# off again without a new image: detections become OBSERVATION with no package.
O1_PACKAGE_SELECTION_ENABLED = (
    os.environ.get("REALTIME_V3_O1_PACKAGE_SELECTION", "ENABLED").strip().upper() != "DISABLED"
)


def _log_o1_debit_vertical_record(context, entries) -> None:
    """G106 AL2: one O1_DEBIT_VERTICAL_RECORD line per window when the switch is RECORD or ON. Logging never fails
    the window."""
    from .advanced_options_o1_debit_vertical import (
        O1DebitVerticalMode,
        log_o1_debit_vertical_record,
        o1_debit_vertical_mode,
        o1_debit_vertical_record,
    )

    mode = o1_debit_vertical_mode()
    if mode is O1DebitVerticalMode.OFF:
        return
    try:
        log_o1_debit_vertical_record(o1_debit_vertical_record(mode=mode, cutoff=context.cutoff, entries=entries))
    except Exception as error:  # noqa: BLE001
        log_o1_debit_vertical_record({
            "contract": "o1-debit-vertical-record-v1", "mode": mode.value, "status": "RECORD_FAILED",
            "reason": f"{type(error).__name__}: {error}"[:200],
        })


def _log_o1_liquidity_floor_record(context, mode, entries, failures, policy=None) -> None:
    """G102 R87: when REALTIME_V3_O1_LIQUIDITY_FLOOR is RECORD or ON, one O1_LIQUIDITY_FLOOR_RECORD line per detected
    event below the floor and one O1_LIQUIDITY_FLOOR_WINDOW line per window. Logging never fails the window."""
    if mode is O1LiquidityFloorMode.OFF:
        return
    try:
        window, events = o1_liquidity_floor_documents(
            mode=mode,
            cutoff=context.cutoff,
            entries=entries,
            selection_enabled=O1_PACKAGE_SELECTION_ENABLED,
            policy=policy,
        )
        if failures:
            window["floor_failures"] = dict(sorted(failures.items()))
        log_o1_liquidity_floor_documents(window, events)
    except Exception as error:  # noqa: BLE001
        log_o1_liquidity_floor_documents({
            "contract": "o1-liquidity-floor-window-v1", "mode": mode.value, "status": "RECORD_FAILED",
            "reason": f"{type(error).__name__}: {error}"[:200],
        }, ())


# --- G106 D5 F0DTE BEGIN
def _f0dte_evaluations(context) -> tuple:
    """G106 D5 (REALTIME_V3_F0DTE=ON): the F0DTE alerts the window's F0DTE run left for this cutoff
    (`advanced_options_f0dte.run_f0dte_window`); () when OFF / RECORD or on any error. Never raises."""
    try:
        from .advanced_options_f0dte import f0dte_lane_evaluations

        return tuple(f0dte_lane_evaluations(context.cutoff))
    except Exception:  # noqa: BLE001 - F0DTE never fails the window
        return ()
# --- G106 D5 F0DTE END


def build_advanced_o1_executor(
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if any(not isinstance(row, AdvancedO1Invocation) for row in inputs.invocations):
        raise TypeError("Advanced O1 invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, DetectorFamily.O1, inputs)
        if blocked is not None:
            return blocked
        evaluations = []
        unavailable_reasons = None
        vertical_entries = []
        # G102 R87: REALTIME_V3_O1_LIQUIDITY_FLOOR (OFF = today, byte for byte). An invalid value raises here, so the
        # window fails loudly (the window service also refuses to start with one).
        floor_mode = o1_liquidity_floor_mode()
        # The thresholds are the process's (read once, validated at service start): one policy per process.
        floor_policy = None if floor_mode is O1LiquidityFloorMode.OFF else o1_liquidity_floor_policy()
        floor_entries = []
        floor_failures: dict[str, int] = {}
        # G106 AL6 / AL7: REALTIME_V3_O1_ZERO_DTE and REALTIME_V3_O1_AGGRESSOR_SIDE (both OFF = no object: today's path,
        # byte for byte). Applied after the floor: floor, then the 0DTE gate, then the aggressor side. Invalid: raises.
        gates = O1GateRun.for_window()
        for invocation in inputs.invocations:
            below_floor = None  # G106 AL6/AL7 record: whether the floor (RECORD or ON) takes this event out
            result = run_o1_adapter(
                invocation.source,
                cohort_id=context.input_gate.closure.cycle_input.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                input_sha256=_input_sha256(context),
                market_cutoff=invocation.source.market_time,
                decision_at=invocation.source.received_at,
            )
            finding = result.output
            if floor_mode is not O1LiquidityFloorMode.OFF:
                try:
                    floored = apply_activity_liquidity_floor(invocation.source, finding, floor_policy)
                    floor_entry = o1_liquidity_floor_entry(invocation, finding, floored)
                except Exception as error:  # noqa: BLE001
                    # Never fails the window. RECORD keeps today's finding; ON leaves the event out (fail closed:
                    # no alert from an event whose floor could not be judged), as an unavailable contract.
                    reason = f"O1LF_FLOOR_FAILED:{type(error).__name__}"
                    floor_failures[reason] = floor_failures.get(reason, 0) + 1
                    if floor_mode is O1LiquidityFloorMode.ON:
                        unavailable_reasons = unavailable_reasons or (reason,)
                        continue
                else:
                    floor_entries.append(floor_entry)
                    below_floor = floor_entry.below_floor
                    if floor_mode is O1LiquidityFloorMode.ON:
                        # The v2 finding (its policy hash, the floor's inputs and reasons) is the evaluation's payload.
                        finding = floored
            gate_permits = True
            if gates is not None:
                try:
                    # RECORD gates leave the finding as it is; an ON gate makes it the v3 finding (gate policy hash).
                    finding, gate_permits = gates.apply(invocation, finding, below_floor)
                except Exception as error:  # noqa: BLE001
                    # Never fails the window. RECORD keeps the finding; ON leaves the event out (fail closed).
                    reason = f"O1GATE_FAILED:{type(error).__name__}"
                    gates.failures[reason] += 1
                    if gates.any_on:
                        unavailable_reasons = unavailable_reasons or (reason,)
                        continue
            vertical_entries.append((invocation.event_id, finding.disposition, invocation.debit_vertical))
            if finding.disposition == "UNAVAILABLE":
                # Per-contract outcome, Nov 9: one contract without positive prior-day open interest made
                # the lane UNAVAILABLE for every contract. The lane is unavailable only if none can be assessed.
                unavailable_reasons = unavailable_reasons or tuple(finding.reasons)
                continue
            selected = (
                O1_PACKAGE_SELECTION_ENABLED
                and finding.disposition == "DETECTED"
                and invocation.package is not None
                and gate_permits  # G106 AL7 ON: a SOLD / UNCLEAR aggressor side is an observation, no package
            )
            evaluations.append(AdvancedOptionsNativeEvaluation(
                invocation.event_id,
                invocation.source.underlyer,
                (
                    "SELECTED"
                    if selected
                    else "OBSERVATION"
                    if finding.disposition == "DETECTED"
                    else "NOT_SELECTED"
                ),
                finding.decision_at,
                _plain(finding),
                invocation.package if selected else None,
            ))
        _log_o1_debit_vertical_record(context, vertical_entries)
        _log_o1_liquidity_floor_record(context, floor_mode, floor_entries, floor_failures, floor_policy)
        if gates is not None:
            gates.log(context.cutoff, O1_PACKAGE_SELECTION_ENABLED)  # never raises
        # --- G106 O1 tuning (Nov 18): REALTIME_V3_OPTIONS_ALERT_CONTEXT (OFF = nothing); notes only, never raises ---
        note_o1_alert_context(context.cutoff, evaluations, inputs.invocations)
        # --- end G106 alert context ---
        # --- G106 D5 F0DTE BEGIN (REALTIME_V3_F0DTE=ON): this window's 0DTE alerts ride the O1 lane (the cycle's
        # detector set is fixed by the control plane). OFF / RECORD: nothing is added. An UNAVAILABLE O1 lane cannot
        # carry evaluations, so F0DTE adds none then. Never raises.
        if unavailable_reasons is None or evaluations:
            evaluations.extend(_f0dte_evaluations(context))
        # --- G106 D5 F0DTE END
        if unavailable_reasons is not None and not evaluations:
            return AdvancedOptionsNativeLaneResult(
                NativeLaneState.UNAVAILABLE,
                unavailable_reasons,
            )
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


def build_advanced_o3_executor(
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if any(not isinstance(row, AdvancedO3Invocation) for row in inputs.invocations):
        raise TypeError("Advanced O3 invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, DetectorFamily.O3, inputs)
        if blocked is not None:
            return blocked
        evaluations = []
        for invocation in inputs.invocations:
            if invocation.scheduled_cycle != context.cutoff:
                raise ValueError("Advanced O3 scheduled cycle changed")
            result = run_o3_adapter(
                invocation.candidate,
                invocation.decision,
                cohort_id=context.input_gate.closure.cycle_input.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                input_sha256=_input_sha256(context),
                activity_contract_id=invocation.activity_contract_id,
                snapshots=invocation.snapshots,
                structural_invalidation=(
                    invocation.structural_invalidation
                ),
                scheduled_cycle=invocation.scheduled_cycle,
            )
            observation = result.output
            evaluations.append(AdvancedOptionsNativeEvaluation(
                invocation.event_id,
                observation.underlyer,
                "SELECTED",
                observation.decision_at,
                _plain(observation),
                invocation.package,
            ))
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


def build_advanced_os_executor(
    detector: DetectorFamily,
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if detector not in {DetectorFamily.OS1, DetectorFamily.OS2}:
        raise ValueError("Advanced stock-first Options detector is invalid")
    if any(
        not isinstance(row, (AdvancedOSInvocation, AdvancedOSParticipationInvocation))
        for row in inputs.invocations
    ):
        raise TypeError("Advanced stock-first Options invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, detector, inputs)
        if blocked is not None:
            _log_os_participation(inputs.record, ())
            return blocked
        evaluations = []
        for invocation in inputs.invocations:
            if invocation.signal.detector_id != detector.value:
                raise ValueError(
                    "Advanced stock-first Options signal changed detector"
                )
            if isinstance(invocation, AdvancedOSParticipationInvocation):
                evaluations.append(_os_participation_evaluation(detector, invocation, context.cutoff))
                continue
            market_cutoff = max(
                invocation.signal.market_time,
                (
                    invocation.source.market_time
                    if invocation.source is not None
                    else invocation.signal.market_time
                ),
            )
            decision = assess_canonical_stock_first(
                invocation.signal,
                invocation.source,
                market_cutoff=market_cutoff,
                decision_at=context.cutoff,
            )
            selected = (
                decision.disposition == "CONFIRMED"
                and invocation.package is not None
            )
            evaluations.append(AdvancedOptionsNativeEvaluation(
                invocation.event_id,
                invocation.signal.underlyer,
                (
                    "SELECTED"
                    if selected
                    else "OBSERVATION"
                    if decision.disposition == "CONFIRMED"
                    else "NOT_SELECTED"
                ),
                context.cutoff,
                {
                    "contract": "advanced-options-stock-first-evaluation-v1",
                    "detector_id": detector.value,
                    "signal": _plain(invocation.signal),
                    "decision": _plain(decision),
                    "source_event_id": (
                        None
                        if invocation.source is None
                        else str(invocation.source.snapshot_id)
                    ),
                },
                invocation.package if selected else None,
            ))
        _log_os_participation(inputs.record, evaluations)
        # --- G106 O1 tuning (Nov 18): REALTIME_V3_OPTIONS_ALERT_CONTEXT (OFF = nothing); notes only, never raises ---
        note_os_alert_context(detector.value, context.cutoff, evaluations, inputs.record)
        # --- end G106 alert context ---
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


def _log_os_participation(record, evaluations) -> None:
    """G106 AL5: the OS_PARTICIPATION_RECORD / OS_PARTICIPATION_WINDOW lines when the lane carries a v2 record (switch
    RECORD or ON). Never fails the window."""
    if record is None:
        return
    try:
        from .advanced_options_os_participation import log_os_participation

        log_os_participation(record, evaluations)
    except Exception:  # noqa: BLE001 - recording never fails the window
        pass


def _os_participation_evaluation(detector, invocation, cutoff):
    """G106 AL5 (ON): the evaluation of one live stock signal under OS policy v2."""
    from .advanced_options_os_participation import OS_PARTICIPATION_POLICY_VERSION

    outcome = invocation.outcome
    confirmed = outcome.confirmed_contract is not None
    selected = confirmed and invocation.package is not None
    return AdvancedOptionsNativeEvaluation(
        invocation.event_id,
        invocation.signal.underlyer,
        "SELECTED" if selected else "OBSERVATION" if confirmed else "NOT_SELECTED",
        cutoff,
        {
            "contract": "advanced-options-stock-first-evaluation-v2",
            "detector_id": detector.value,
            "signal": _plain(invocation.signal),
            "decision": {
                "policy": OS_PARTICIPATION_POLICY_VERSION,
                "policy_sha256": invocation.policy_sha256,
                "direction": int(invocation.signal.direction),
                "disposition": "CONFIRMED" if confirmed else "UNMATCHED",
                "reasons": list(outcome.reasons),
                "contract": outcome.confirmed_contract,
                "first_window": outcome.signal.first_window,
                "best": _plain(outcome.best),
            },
            "source_event_id": None,
        },
        invocation.package if selected else None,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeInputBundle:
    o1: AdvancedOptionsLaneInputs
    o3: AdvancedOptionsLaneInputs
    os1: AdvancedOptionsLaneInputs = AdvancedOptionsLaneInputs()
    os2: AdvancedOptionsLaneInputs = AdvancedOptionsLaneInputs()
    plan_id: UUID | None = None
    input_sha256: str | None = None
    executor_membership_sha256: str | None = None

    def __post_init__(self) -> None:
        identities = (
            self.plan_id,
            self.input_sha256,
            self.executor_membership_sha256,
        )
        if any(value is None for value in identities) != all(
            value is None for value in identities
        ):
            raise ValueError("native input bundle identity is incomplete")
        if self.plan_id is not None:
            for value, name in (
                (self.input_sha256, "input_sha256"),
                (
                    self.executor_membership_sha256,
                    "executor_membership_sha256",
                ),
            ):
                if (
                    len(value) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in value
                    )
                ):
                    raise ValueError(f"{name} must be a SHA-256 digest")

    def executors(self) -> Mapping[DetectorFamily, NativeDetectorExecutor]:
        return {
            DetectorFamily.O1: build_advanced_o1_executor(self.o1),
            DetectorFamily.O3: build_advanced_o3_executor(self.o3),
            DetectorFamily.OS1: build_advanced_os_executor(
                DetectorFamily.OS1,
                self.os1,
            ),
            DetectorFamily.OS2: build_advanced_os_executor(
                DetectorFamily.OS2,
                self.os2,
            ),
        }
