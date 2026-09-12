from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
from statistics import median
from typing import Iterable
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np

from options.analytics.chain_analysis import ChainHealth
from options.config import GammaExposurePolicy, StrategyPolicy
from options.domain import ContractType, OptionContractSnapshot, OptionExpirationAnalytics, OptionTradeEvent

from .domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    OptionCandidate,
    OptionSide,
    ScenarioResult,
    StrategyContextSnapshot,
    StructureRiskClass,
    StructureType,
    candidate_identity,
)
from .payoff import PayoffSummary, evaluate_terminal_payoff
from .gates import ExecutionGateLedger, evaluate_execution_gates
from .registry import REGISTRY_BY_NAME, STRATEGY_REGISTRY, StrategyRegistration
from options.analytics.gamma_exposure import (
    GammaWall,
    ScopedGammaProfile,
    detect_gamma_walls,
)
from options.analytics.smile import (
    SmileInput,
    coefficient_payload,
    fit_smile_groups,
    qualifying_distortions,
)
from .scenarios import build_scenario_grid


@dataclass(frozen=True, slots=True)
class StrategyScanResult:
    candidates: tuple[OptionCandidate, ...]
    scenarios: tuple[ScenarioResult, ...]
    candidate_gate_ledgers: tuple[tuple[UUID, ExecutionGateLedger], ...] = ()


def _quadratic_coefficient_payload(coefficients: np.ndarray) -> dict[str, float]:
    if coefficients.shape != (3,):
        raise ValueError("quadratic fit must contain exactly three coefficients")
    return coefficient_payload(
        (float(coefficients[0]), float(coefficients[1]), float(coefficients[2]))
    )


class OptionStrategyEngine:
    def __init__(
        self,
        policy: StrategyPolicy,
        policy_sha256: str,
        gamma_policy: GammaExposurePolicy | None = None,
        gamma_policy_sha256: str | None = None,
        read_only: bool = True,
        quotes_available: bool = False,
        risk_engine_available: bool = False,
    ) -> None:
        self.policy = policy
        self.gamma_policy = gamma_policy
        self.read_only = read_only
        self.quotes_available = quotes_available
        self.risk_engine_available = risk_engine_available
        self.strategy_version = policy.strategy_version
        # The gamma policy only enters strategy identity when it actually gates
        # selection, so leaving the walls disabled preserves the published hash.
        if gamma_policy is not None and gamma_policy.require_gamma_wall:
            if not gamma_policy_sha256:
                raise ValueError(
                    "enabling gamma wall gates requires the gamma policy hash"
                )
            self.policy_sha256 = hashlib.sha256(
                f"{policy_sha256}:{gamma_policy_sha256}".encode("utf-8")
            ).hexdigest()
        else:
            self.policy_sha256 = policy_sha256

    def scan(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        chain_health: ChainHealth,
        expirations: tuple[OptionExpirationAnalytics, ...],
        context: StrategyContextSnapshot,
        trades: tuple[OptionTradeEvent, ...] = (),
        gamma_profiles: tuple[ScopedGammaProfile, ...] = (),
    ) -> StrategyScanResult:
        if not snapshots:
            raise ValueError("strategy scan requires a complete non-empty matrix")
        if context.matrix_id != matrix_id:
            raise ValueError("strategy context must belong to the scanned matrix")
        for snapshot in snapshots:
            snapshot.require_available(
                _decision_context(context.market_data_time, context.observed_time)
            )
        if chain_health.status != "COMPLETE":
            reasons = tuple(dict.fromkeys((*chain_health.reasons, "NO_STRATEGY_WORK")))
            candidates = tuple(
                self._suppressed(matrix_id, snapshots[0], registration, reasons)
                for registration in STRATEGY_REGISTRY
            )
            return self._scan_result(candidates, (), context)

        valid = tuple(
            snapshot
            for snapshot in snapshots
            if snapshot.model_mark is not None
            and snapshot.iv_converged
            and snapshot.local_iv is not None
            and snapshot.local_delta is not None
            and snapshot.local_gamma is not None
            and snapshot.local_theta_per_day is not None
            and snapshot.local_vega_per_vol_point is not None
            and snapshot.local_rho_per_rate_point is not None
        )
        outputs: list[OptionCandidate] = []
        outputs.extend(self._income_wheel(matrix_id, valid, context))
        outputs.extend(self._gamma_squeeze(matrix_id, valid, context, gamma_profiles))
        outputs.extend(self._long_premium(matrix_id, valid, context))
        outputs.extend(self._debit_spread(matrix_id, valid, context))
        outputs.extend(self._spread_and_range(matrix_id, valid, expirations, context))
        outputs.extend(self._sweep_like(matrix_id, snapshots, trades, context))
        outputs.extend(self._volume_oi(matrix_id, snapshots, context))
        outputs.extend(self._smile(matrix_id, valid, context))
        candidates = tuple(outputs)
        scenarios = tuple(
            scenario
            for candidate in candidates
            if candidate.status is CandidateStatus.SELECTED and candidate.legs
            for scenario in build_scenario_grid(candidate, self.policy.scenarios)
        )
        return self._scan_result(candidates, scenarios, context)

    def _scan_result(
        self,
        candidates: tuple[OptionCandidate, ...],
        scenarios: tuple[ScenarioResult, ...],
        context: StrategyContextSnapshot,
    ) -> StrategyScanResult:
        return StrategyScanResult(
            candidates,
            scenarios,
            tuple(
                (
                    candidate.candidate_id,
                    self.execution_gate_ledger(
                        context,
                        candidate.candidate_kind,
                        equity_direction_required=(
                            candidate.primary_evidence.get("directional_thesis")
                            in {"BULLISH", "BEARISH"}
                        ),
                    ),
                )
                for candidate in candidates
            ),
        )

    def _income_wheel(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["INCOME_WHEEL"]
        policy = self.policy.income_wheel
        direction_block = self._direction_block(
            context,
            allowed=frozenset({"BULLISH", "NEUTRAL"}),
        )
        if direction_block:
            return (
                self._suppressed(matrix_id, snapshots[0], registration, direction_block),
            )
        dte_eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.contract_type is ContractType.PUT
            and policy.minimum_dte <= snapshot.calendar_dte <= policy.maximum_dte
            and snapshot.strike < snapshot.spot
            and snapshot.model_mark is not None
        ]
        eligible = [
            snapshot
            for snapshot in dte_eligible
            if snapshot.calendar_dte > policy.exit_dte
        ]
        eligible.sort(
            key=lambda row: (
                -float(row.local_iv),
                -float(row.model_mark / row.strike),
                -(row.open_interest or 0),
                -(row.day_volume or 0),
                row.expiration_date,
                -row.strike,
                row.contract_id,
            )
        )
        if not eligible:
            return (
                self._suppressed(
                    matrix_id,
                    snapshots[0],
                    registration,
                    (
                        "NO_WHEEL_CONTRACT_ABOVE_EXIT_DTE"
                        if dte_eligible
                        else "NO_ELIGIBLE_WHEEL_CONTRACT"
                    ,),
                ),
            )
        results = []
        for rank, snapshot in enumerate(eligible[: policy.maximum_candidates], start=1):
            leg = _leg(snapshot, 0, OptionSide.SELL)
            payoff = evaluate_terminal_payoff((leg,))
            collateral = snapshot.strike * snapshot.shares_per_contract
            reasons = self._capability_reasons(context)
            results.append(
                self._candidate(
                    matrix_id,
                    registration,
                    StructureType.CASH_SECURED_PUT,
                    StructureRiskClass.CASH_SECURED,
                    rank,
                    (leg,),
                    payoff,
                    context,
                    primary_metric_name="local_iv",
                    primary_metric_value=snapshot.local_iv,
                    rank_components={
                        "local_iv": snapshot.local_iv,
                        "premium_yield": float(snapshot.model_mark / snapshot.strike),
                        "open_interest": snapshot.open_interest,
                        "day_volume": snapshot.day_volume,
                        "expiration_date": snapshot.expiration_date.isoformat(),
                        "strike": str(snapshot.strike),
                        "contract_id": snapshot.contract_id,
                    },
                    primary_evidence={
                        "distance_otm_fraction": float(
                            (snapshot.spot - snapshot.strike) / snapshot.spot
                        ),
                        "annualized_premium_yield": float(
                            (snapshot.model_mark / snapshot.strike)
                            * Decimal("365")
                            / snapshot.calendar_dte
                        ) if snapshot.calendar_dte else None,
                        "iv_regime": None,
                        "iv_regime_reason": "INSUFFICIENT_COMPLETED_SESSION_HISTORY",
                    },
                    collateral_required=collateral,
                    capital_at_risk=payoff.maximum_loss,
                    return_on_collateral=float(payoff.maximum_profit / collateral)
                    if payoff.maximum_profit is not None and collateral > 0
                    else None,
                    reason_codes=reasons,
                    management_policy={
                        "take_profit_fraction": policy.take_profit_fraction,
                        "stop_loss_multiple": policy.stop_loss_multiple,
                        "exit_dte": policy.exit_dte,
                    },
                )
            )
        return tuple(results)

    def _gamma_squeeze(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: StrategyContextSnapshot,
        gamma_profiles: tuple[ScopedGammaProfile, ...] = (),
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["ZERO_DTE_GAMMA_SQUEEZE"]
        policy = self.policy.gamma_squeeze
        direction_block = self._direction_block(
            context,
            allowed=frozenset({"BULLISH", "BEARISH"}),
        )
        if direction_block:
            return (
                self._suppressed(matrix_id, snapshots[0], registration, direction_block),
            )
        walls: tuple[GammaWall, ...] = ()
        wall_evidence: dict[str, object] = {}
        gamma_policy = self.gamma_policy
        require_wall = gamma_policy is not None and gamma_policy.require_gamma_wall
        if require_wall:
            blocked, walls, wall_evidence = self._gamma_wall_gate(
                gamma_policy, gamma_profiles
            )
            if blocked:
                return (
                    self._suppressed(matrix_id, snapshots[0], registration, blocked),
                )
        wall_strikes = {wall.strike: wall for wall in walls}
        surge_by_strike = _strike_volume_surge(snapshots)
        eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.calendar_dte == 0
            and abs(float(snapshot.strike / snapshot.spot) - 1) <= policy.maximum_moneyness_fraction
            and snapshot.open_interest is not None
            and snapshot.day_volume is not None
            and snapshot.day_volume / max(snapshot.open_interest, 1) >= policy.minimum_volume_oi_ratio
            and float(snapshot.local_gamma) > policy.minimum_gamma
            and (
                not require_wall
                or (
                    snapshot.strike in wall_strikes
                    and surge_by_strike.get(snapshot.strike, 0.0)
                    >= gamma_policy.minimum_volume_surge_ratio
                )
            )
            and (
                context.equity_context_snapshot_id is None
                or (
                    context.qualified_direction == "BULLISH"
                    and snapshot.contract_type is ContractType.CALL
                )
                or (
                    context.qualified_direction == "BEARISH"
                    and snapshot.contract_type is ContractType.PUT
                )
            )
        ]
        outputs: list[OptionCandidate] = []
        for contract_type in (ContractType.CALL, ContractType.PUT):
            rows = [row for row in eligible if row.contract_type is contract_type]
            rows.sort(
                key=lambda row: (
                    -(wall_strikes[row.strike].gamma_share if row.strike in wall_strikes else 0.0),
                    -(row.day_volume / max(row.open_interest or 0, 1)),
                    -float(row.local_gamma),
                    -(row.day_volume or 0),
                    row.contract_id,
                )
            )
            for row in rows[: policy.maximum_per_side]:
                rank = len(outputs) + 1
                structure = StructureType.LONG_CALL if contract_type is ContractType.CALL else StructureType.LONG_PUT
                leg = _leg(row, 0, OptionSide.BUY)
                wall = wall_strikes.get(row.strike)
                outputs.append(
                    self._candidate(
                        matrix_id,
                        registration,
                        structure,
                        StructureRiskClass.PREMIUM_AT_RISK_DEBIT,
                        rank,
                        (leg,),
                        evaluate_terminal_payoff((leg,)),
                        context,
                        primary_metric_name="volume_oi_ratio",
                        primary_metric_value=row.day_volume / max(row.open_interest or 0, 1),
                        rank_components={
                            "volume_oi_ratio": row.day_volume / max(row.open_interest or 0, 1),
                            "local_gamma": row.local_gamma,
                            "day_volume": row.day_volume,
                            "contract_id": row.contract_id,
                        },
                        primary_evidence={
                            "directional_thesis": "BULLISH" if contract_type is ContractType.CALL else "BEARISH",
                            "moneyness_fraction": abs(float(row.strike / row.spot) - 1),
                            **(
                                {
                                    "gamma_wall_strike": str(wall.strike),
                                    "gamma_wall_share": wall.gamma_share,
                                    "gamma_wall_distance_fraction": wall.distance_to_spot_fraction,
                                    "volume_surge_ratio": surge_by_strike.get(row.strike, 0.0),
                                    **wall_evidence,
                                }
                                if wall is not None
                                else {}
                            ),
                        },
                        capital_at_risk=evaluate_terminal_payoff((leg,)).maximum_loss,
                        reason_codes=self._capability_reasons(context),
                        management_policy={
                            "stop_loss_fraction": policy.stop_loss_fraction,
                            "take_profit_fraction": policy.take_profit_fraction,
                            "trailing_activation_fraction": policy.trailing_activation_fraction,
                            "trailing_distance_fraction": policy.trailing_distance_fraction,
                        },
                    )
                )
        if outputs:
            return tuple(outputs)
        return (
            self._suppressed(
                matrix_id,
                snapshots[0],
                registration,
                ("NO_ZERO_DTE_GAMMA_TRIGGER",),
            ),
        )

    @staticmethod
    def _gamma_wall_gate(
        policy: GammaExposurePolicy,
        gamma_profiles: tuple[ScopedGammaProfile, ...],
    ) -> tuple[tuple[str, ...], tuple[GammaWall, ...], dict[str, object]]:
        scoped = next(
            (item for item in gamma_profiles if item.scope is policy.gamma_wall_scope),
            None,
        )
        if scoped is None:
            return ("GAMMA_PROFILE_UNAVAILABLE",), (), {}
        profile = scoped.profile
        if profile.contributing_contract_count == 0:
            return ("GAMMA_PROFILE_EMPTY",), (), {}
        if (
            policy.required_regime is not None
            and profile.flip.regime_at_spot is not policy.required_regime
        ):
            return ("GAMMA_REGIME_NOT_SQUEEZE_PRONE",), (), {}
        walls = detect_gamma_walls(
            profile,
            minimum_share=policy.minimum_wall_gamma_share,
            maximum_distance_fraction=policy.wall_proximity_fraction,
        )
        if not walls:
            return ("NO_PROXIMATE_GAMMA_WALL",), (), {}
        evidence: dict[str, object] = {
            "gamma_scope": scoped.scope.value,
            "gamma_regime": profile.flip.regime_at_spot.value,
            "gamma_flip_spot": (
                str(profile.flip.flip_spot) if profile.flip.flip_spot is not None else None
            ),
            "dealer_convention": profile.convention.value,
            "gamma_wall_count": len(walls),
        }
        return (), walls, evidence

    def _long_premium(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["DIRECTIONAL_LONG_PREMIUM"]
        policy = self.policy.long_premium
        direction_block = self._direction_block(
            context,
            allowed=frozenset({"BULLISH", "BEARISH"}),
        )
        if direction_block:
            return (
                self._suppressed(matrix_id, snapshots[0], registration, direction_block),
            )
        scored: list[tuple[str, ContractType, float, OptionContractSnapshot, dict[str, object]]] = []
        for row in snapshots:
            if not policy.minimum_dte <= row.calendar_dte <= policy.maximum_dte:
                continue
            absolute_delta = abs(float(row.local_delta))
            if not (
                policy.minimum_absolute_delta
                <= absolute_delta
                <= policy.maximum_absolute_delta
            ):
                continue
            if (row.open_interest or 0) < policy.minimum_open_interest:
                continue
            if (row.day_volume or 0) < policy.minimum_day_volume:
                continue
            if (
                context.equity_context_snapshot_id is not None
                and not (
                    (
                        context.qualified_direction == "BULLISH"
                        and row.contract_type is ContractType.CALL
                    )
                    or (
                        context.qualified_direction == "BEARISH"
                        and row.contract_type is ContractType.PUT
                    )
                )
            ):
                continue
            economics = _long_premium_economics(row)
            if economics is None:
                continue
            required_move, expected_move, ratio = economics
            if ratio > policy.maximum_breakeven_expected_move_ratio:
                continue
            lane = _dte_lane(row.calendar_dte, policy)
            scored.append((
                lane,
                row.contract_type,
                ratio,
                row,
                {
                    "directional_thesis": (
                        "BULLISH" if row.contract_type is ContractType.CALL else "BEARISH"
                    ),
                    "dte_lane": lane,
                    "absolute_delta": absolute_delta,
                    "breakeven": str(row.single_contract_breakeven),
                    "required_move_fraction": required_move,
                    "expected_move_fraction": expected_move,
                    "breakeven_expected_move_ratio": ratio,
                    "iv_context": None,
                    "iv_context_reason": "INSUFFICIENT_COMPLETED_SESSION_HISTORY",
                },
            ))
        if not scored:
            return (
                self._suppressed(
                    matrix_id,
                    snapshots[0],
                    registration,
                    ("NO_LONG_PREMIUM_WITHIN_EXPECTED_MOVE",),
                ),
            )
        scored.sort(
            key=lambda item: (
                item[0],
                item[1].value,
                item[2],
                -(item[3].open_interest or 0),
                -(item[3].day_volume or 0),
                item[3].contract_id,
            )
        )
        outputs: list[OptionCandidate] = []
        emitted: dict[tuple[str, ContractType], int] = defaultdict(int)
        for lane, contract_type, ratio, row, evidence in scored:
            key = (lane, contract_type)
            if emitted[key] >= policy.maximum_candidates_per_lane_side:
                continue
            emitted[key] += 1
            structure = (
                StructureType.LONG_CALL
                if contract_type is ContractType.CALL
                else StructureType.LONG_PUT
            )
            leg = _leg(row, 0, OptionSide.BUY)
            payoff = evaluate_terminal_payoff((leg,))
            outputs.append(
                self._candidate(
                    matrix_id,
                    registration,
                    structure,
                    StructureRiskClass.PREMIUM_AT_RISK_DEBIT,
                    len(outputs) + 1,
                    (leg,),
                    payoff,
                    context,
                    primary_metric_name="breakeven_expected_move_ratio",
                    primary_metric_value=ratio,
                    rank_components={
                        "breakeven_expected_move_ratio": ratio,
                        "absolute_delta": abs(float(row.local_delta)),
                        "open_interest": row.open_interest,
                        "day_volume": row.day_volume,
                        "contract_id": row.contract_id,
                    },
                    primary_evidence=evidence,
                    capital_at_risk=payoff.maximum_loss,
                    reason_codes=self._capability_reasons(context),
                    management_policy={
                        "dte_lane": lane,
                        "maximum_breakeven_expected_move_ratio": (
                            policy.maximum_breakeven_expected_move_ratio
                        ),
                    },
                )
            )
        return tuple(outputs)

    def _debit_spread(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["DIRECTIONAL_DEBIT_SPREAD"]
        policy = self.policy.debit_spread
        direction_block = self._direction_block(
            context,
            allowed=frozenset({"BULLISH", "BEARISH"}),
        )
        if direction_block:
            return (
                self._suppressed(matrix_id, snapshots[0], registration, direction_block),
            )
        by_expiration: dict[
            tuple[date, ContractType], list[OptionContractSnapshot]
        ] = defaultdict(list)
        for row in snapshots:
            if not policy.minimum_dte <= row.calendar_dte <= policy.maximum_dte:
                continue
            if (row.open_interest or 0) < policy.minimum_open_interest:
                continue
            if (row.day_volume or 0) < policy.minimum_day_volume:
                continue
            by_expiration[(row.expiration_date, row.contract_type)].append(row)
        scored: list[
            tuple[str, ContractType, float, float, tuple[CandidateLeg, ...], PayoffSummary, dict[str, object]]
        ] = []
        for (_, contract_type), rows in by_expiration.items():
            if (
                context.equity_context_snapshot_id is not None
                and not (
                    (
                        context.qualified_direction == "BULLISH"
                        and contract_type is ContractType.CALL
                    )
                    or (
                        context.qualified_direction == "BEARISH"
                        and contract_type is ContractType.PUT
                    )
                )
            ):
                continue
            for long_row in rows:
                absolute_delta = abs(float(long_row.local_delta))
                if not (
                    policy.minimum_long_absolute_delta
                    <= absolute_delta
                    <= policy.maximum_long_absolute_delta
                ):
                    continue
                spot = float(long_row.spot)
                if spot <= 0 or long_row.local_iv is None:
                    continue
                if long_row.time_to_expiration_years <= 0:
                    continue
                expected_move = float(long_row.local_iv) * math.sqrt(
                    long_row.time_to_expiration_years
                )
                if expected_move <= 0:
                    continue
                for short_row in rows:
                    if short_row.contract_id == long_row.contract_id:
                        continue
                    # The short wing must be further out of the money than the long leg.
                    if contract_type is ContractType.CALL:
                        if short_row.strike <= long_row.strike:
                            continue
                    elif short_row.strike >= long_row.strike:
                        continue
                    width_fraction = float(
                        abs(short_row.strike - long_row.strike) / long_row.spot
                    )
                    if not (
                        policy.minimum_width_fraction
                        <= width_fraction
                        <= policy.maximum_width_fraction
                    ):
                        continue
                    target_ratio = (
                        abs(float(short_row.strike) - spot) / spot
                    ) / expected_move
                    if target_ratio > policy.maximum_target_expected_move_ratio:
                        continue
                    legs = (
                        _leg(long_row, 0, OptionSide.BUY),
                        _leg(short_row, 1, OptionSide.SELL),
                    )
                    payoff = evaluate_terminal_payoff(legs)
                    if payoff.net_premium >= 0:
                        continue
                    if (
                        not payoff.bounded_maximum_loss
                        or payoff.maximum_loss is None
                        or payoff.maximum_loss <= 0
                        or payoff.maximum_profit is None
                        or payoff.maximum_profit <= 0
                    ):
                        continue
                    return_on_risk = float(payoff.maximum_profit / payoff.maximum_loss)
                    if return_on_risk < policy.minimum_return_on_risk:
                        continue
                    if len(payoff.breakevens) != 1:
                        continue
                    required_move = abs(float(payoff.breakevens[0]) - spot) / spot
                    breakeven_ratio = required_move / expected_move
                    if breakeven_ratio > policy.maximum_breakeven_expected_move_ratio:
                        continue
                    lane = _dte_lane(long_row.calendar_dte, policy)
                    scored.append((
                        lane,
                        contract_type,
                        breakeven_ratio,
                        return_on_risk,
                        legs,
                        payoff,
                        {
                            "directional_thesis": (
                                "BULLISH"
                                if contract_type is ContractType.CALL
                                else "BEARISH"
                            ),
                            "dte_lane": lane,
                            "long_strike": str(long_row.strike),
                            "short_strike": str(short_row.strike),
                            "long_absolute_delta": absolute_delta,
                            "short_absolute_delta": abs(float(short_row.local_delta)),
                            "net_debit_per_contract": str(-payoff.net_premium),
                            "width_fraction": width_fraction,
                            "breakeven": str(payoff.breakevens[0]),
                            "required_move_fraction": required_move,
                            "expected_move_fraction": expected_move,
                            "breakeven_expected_move_ratio": breakeven_ratio,
                            "target_expected_move_ratio": target_ratio,
                            "return_on_risk": return_on_risk,
                            "iv_context": None,
                            "iv_context_reason": "INSUFFICIENT_COMPLETED_SESSION_HISTORY",
                        },
                    ))
        if not scored:
            return (
                self._suppressed(
                    matrix_id,
                    snapshots[0],
                    registration,
                    ("NO_DEBIT_SPREAD_WITHIN_EXPECTED_MOVE",),
                ),
            )
        scored.sort(
            key=lambda item: (
                item[0],
                item[1].value,
                -item[3],
                item[2],
                item[4][0].contract_id,
                item[4][1].contract_id,
            )
        )
        outputs: list[OptionCandidate] = []
        emitted: dict[tuple[str, ContractType], int] = defaultdict(int)
        for lane, contract_type, breakeven_ratio, return_on_risk, legs, payoff, evidence in scored:
            key = (lane, contract_type)
            if emitted[key] >= policy.maximum_candidates_per_lane_side:
                continue
            emitted[key] += 1
            structure = (
                StructureType.CALL_DEBIT_VERTICAL
                if contract_type is ContractType.CALL
                else StructureType.PUT_DEBIT_VERTICAL
            )
            outputs.append(
                self._candidate(
                    matrix_id,
                    registration,
                    structure,
                    StructureRiskClass.PREMIUM_AT_RISK_DEBIT,
                    len(outputs) + 1,
                    legs,
                    payoff,
                    context,
                    primary_metric_name="return_on_risk",
                    primary_metric_value=return_on_risk,
                    rank_components={
                        "return_on_risk": return_on_risk,
                        "breakeven_expected_move_ratio": breakeven_ratio,
                        "target_expected_move_ratio": evidence[
                            "target_expected_move_ratio"
                        ],
                        "width_fraction": evidence["width_fraction"],
                        "ordered_contract_ids": [leg.contract_id for leg in legs],
                    },
                    primary_evidence=evidence,
                    capital_at_risk=payoff.maximum_loss,
                    return_on_risk=return_on_risk,
                    reason_codes=self._capability_reasons(context),
                    management_policy={
                        "dte_lane": lane,
                        "minimum_return_on_risk": policy.minimum_return_on_risk,
                        "maximum_breakeven_expected_move_ratio": (
                            policy.maximum_breakeven_expected_move_ratio
                        ),
                    },
                )
            )
        return tuple(outputs)

    def _spread_and_range(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],        expirations: tuple[OptionExpirationAnalytics, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["SPREAD_RANGE_LOCATOR"]
        by_key = {(row.expiration_date, row.contract_type, row.strike): row for row in snapshots}
        structures: list[tuple[float, tuple[CandidateLeg, ...], StructureType, dict[str, object]]] = []
        policy = self.policy.spreads
        for expiration in expirations:
            walls = json.loads(expiration.wall_clusters_json)
            verticals: dict[ContractType, list[tuple[float, tuple[CandidateLeg, ...], StructureType, dict[str, object]]]] = defaultdict(list)
            for wall in walls:
                contract_type = ContractType(wall["contract_type"])
                member_strikes = tuple(Decimal(value) for value in wall["member_strikes"])
                center = Decimal(wall["center_strike"])
                short_strike = min(member_strikes, key=lambda value: (abs(value - center), value))
                short = by_key.get((expiration.expiration_date, contract_type, short_strike))
                if short is None:
                    continue
                farther = sorted(
                    (
                        row
                        for row in snapshots
                        if row.expiration_date == expiration.expiration_date
                        and row.contract_type is contract_type
                        and (
                            row.strike < short.strike
                            if contract_type is ContractType.PUT
                            else row.strike > short.strike
                        )
                    ),
                    key=lambda row: abs(row.strike - short.strike),
                )[: policy.maximum_wings_per_short_strike]
                for wing in farther:
                    legs = (_leg(short, 0, OptionSide.SELL), _leg(wing, 1, OptionSide.BUY))
                    payoff = evaluate_terminal_payoff(legs)
                    if not payoff.bounded_maximum_loss or payoff.maximum_loss is None or payoff.maximum_loss <= 0 or payoff.net_premium <= 0:
                        continue
                    structure = StructureType.PUT_CREDIT_VERTICAL if contract_type is ContractType.PUT else StructureType.CALL_CREDIT_VERTICAL
                    evidence = {
                        "wall_center": str(center),
                        "wall_strength": wall["maximum_robust_z"],
                        "wall_open_interest": wall["total_open_interest"],
                    }
                    item = (float(wall["maximum_robust_z"]), legs, structure, evidence)
                    verticals[contract_type].append(item)
                    structures.append(item)
            puts = sorted(verticals[ContractType.PUT], key=_structure_sort)
            calls = sorted(verticals[ContractType.CALL], key=_structure_sort)
            if puts and calls:
                put = puts[0]
                call = calls[0]
                if put[1][0].strike < call[1][0].strike:
                    condor_legs = tuple(
                        replace(leg, leg_index=index)
                        for index, leg in enumerate((put[1][1], put[1][0], call[1][0], call[1][1]))
                    )
                    payoff = evaluate_terminal_payoff(condor_legs)
                    if payoff.bounded_maximum_loss and payoff.maximum_loss and payoff.net_premium > 0:
                        structures.append((min(put[0], call[0]), condor_legs, StructureType.IRON_CONDOR, {"put_wall": put[3], "call_wall": call[3]}))
            for wall in walls:
                contract_type = ContractType(wall["contract_type"])
                center = Decimal(wall["center_strike"])
                center_row = by_key.get((expiration.expiration_date, contract_type, center))
                if center_row is None or abs(float(center / center_row.spot) - 1) > policy.maximum_center_distance_fraction:
                    continue
                strikes = sorted(row.strike for row in snapshots if row.expiration_date == expiration.expiration_date and row.contract_type is contract_type)
                widths = sorted({center - strike for strike in strikes if strike < center} & {strike - center for strike in strikes if strike > center})
                if not widths:
                    continue
                lower = by_key[(expiration.expiration_date, contract_type, center - widths[0])]
                upper = by_key[(expiration.expiration_date, contract_type, center + widths[0])]
                legs = (_leg(lower, 0, OptionSide.BUY), _leg(center_row, 1, OptionSide.SELL, ratio=2), _leg(upper, 2, OptionSide.BUY))
                payoff = evaluate_terminal_payoff(legs)
                if payoff.bounded_maximum_loss and payoff.maximum_loss and payoff.net_premium < 0:
                    structure = StructureType.CALL_BUTTERFLY if contract_type is ContractType.CALL else StructureType.PUT_BUTTERFLY
                    structures.append((float(wall["maximum_robust_z"]), legs, structure, {"center": str(center), "width": str(widths[0])}))
        if not structures:
            return (self._suppressed(matrix_id, snapshots[0], registration, ("NO_BOUNDED_LISTED_STRUCTURE",)),)
        direction_block = self._direction_block(
            context,
            allowed=frozenset({"BULLISH", "BEARISH", "NEUTRAL"}),
        )
        if direction_block:
            return (
                self._suppressed(matrix_id, snapshots[0], registration, direction_block),
            )
        if context.equity_context_snapshot_id is not None:
            allowed_structures = {
                "BULLISH": frozenset({StructureType.PUT_CREDIT_VERTICAL}),
                "BEARISH": frozenset({StructureType.CALL_CREDIT_VERTICAL}),
                "NEUTRAL": frozenset({
                    StructureType.IRON_CONDOR,
                    StructureType.CALL_BUTTERFLY,
                    StructureType.PUT_BUTTERFLY,
                }),
            }[context.qualified_direction]
            structures = [
                item for item in structures if item[2] in allowed_structures
            ]
            if not structures:
                return (
                    self._suppressed(
                        matrix_id,
                        snapshots[0],
                        registration,
                        ("NO_STRUCTURE_FOR_QUALIFIED_DIRECTION",),
                    ),
                )
        structures.sort(key=_structure_sort)
        counts: dict[tuple[StructureType, object], int] = defaultdict(int)
        candidates: list[OptionCandidate] = []
        for strength, legs, structure, evidence in structures:
            key = (structure, legs[0].expiration_date)
            if counts[key] >= policy.maximum_per_structure_expiration:
                continue
            counts[key] += 1
            payoff = evaluate_terminal_payoff(legs)
            risk_class = StructureRiskClass.PREMIUM_AT_RISK_DEBIT if payoff.net_premium < 0 else StructureRiskClass.DEFINED_RISK_CREDIT
            candidates.append(
                self._candidate(
                    matrix_id,
                    registration,
                    structure,
                    risk_class,
                    len(candidates) + 1,
                    legs,
                    payoff,
                    context,
                    primary_metric_name="oi_wall_strength",
                    primary_metric_value=strength,
                    rank_components={
                        "wall_strength": strength,
                        "minimum_leg_open_interest": min(leg_value.open_interest or 0 for leg_value in (_snapshot_for_leg(legs, snapshots))),
                        "minimum_leg_volume": min(leg_value.day_volume or 0 for leg_value in (_snapshot_for_leg(legs, snapshots))),
                        "return_on_risk": float(payoff.maximum_profit / payoff.maximum_loss) if payoff.maximum_profit is not None and payoff.maximum_loss else None,
                        "ordered_contract_ids": [leg.contract_id for leg in legs],
                    },
                    primary_evidence=evidence,
                    capital_at_risk=payoff.maximum_loss,
                    return_on_risk=float(payoff.maximum_profit / payoff.maximum_loss) if payoff.maximum_profit is not None and payoff.maximum_loss else None,
                    reason_codes=self._capability_reasons(context),
                )
            )
        return tuple(candidates)

    def _volume_oi(self, matrix_id: UUID, snapshots: tuple[OptionContractSnapshot, ...], context: StrategyContextSnapshot) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["VOLUME_OI_ANOMALY"]
        policy = self.policy.flow
        rows = [row for row in snapshots if row.day_volume is not None and row.open_interest is not None and row.day_volume / max(row.open_interest, 1) >= policy.minimum_volume_oi_ratio]
        rows.sort(key=lambda row: (-(row.day_volume / max(row.open_interest or 0, 1)), -(row.day_volume or 0), -(row.open_interest or 0), row.expiration_date, row.contract_id))
        if not rows:
            return (self._suppressed(matrix_id, snapshots[0], registration, ("NO_VOLUME_OI_ANOMALY",)),)
        return tuple(self._research_candidate(matrix_id, registration, StructureType.VOLUME_OI_ANOMALY, rank, row, context, "volume_oi_ratio", row.day_volume / max(row.open_interest or 0, 1), {"day_volume": row.day_volume, "open_interest": row.open_interest, "contract_id": row.contract_id, "direction": None}) for rank, row in enumerate(rows[: policy.maximum_candidates], start=1))

    def _sweep_like(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        trades: tuple[OptionTradeEvent, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["SWEEP_LIKE_CLUSTER"]
        policy = self.policy.flow
        by_contract = {
            row.contract_id: row
            for row in snapshots
            if row.contract_type is ContractType.CALL and row.strike > row.spot
        }
        grouped: dict[int, list[OptionTradeEvent]] = defaultdict(list)
        for trade in trades:
            if trade.contract_id in by_contract and trade.notional >= policy.minimum_print_notional:
                grouped[trade.contract_id].append(trade)
        clusters: list[tuple[int, Decimal, int, float, OptionContractSnapshot, tuple[OptionTradeEvent, ...]]] = []
        for contract_id, rows in grouped.items():
            rows.sort(key=lambda row: (row.sip_timestamp, row.sequence_number, row.trade_event_id))
            left = 0
            best: tuple[OptionTradeEvent, ...] = ()
            for right, trade in enumerate(rows):
                while (
                    trade.sip_timestamp - rows[left].sip_timestamp
                ).total_seconds() > policy.sweep_window_seconds:
                    left += 1
                window = tuple(rows[left : right + 1])
                exchanges = {row.exchange for row in window if row.exchange is not None}
                if len(window) >= policy.minimum_sweep_prints and len(exchanges) >= policy.minimum_distinct_exchanges:
                    if not best or (
                        len(window),
                        sum(row.notional for row in window),
                        len(exchanges),
                    ) > (
                        len(best),
                        sum(row.notional for row in best),
                        len({row.exchange for row in best if row.exchange is not None}),
                    ):
                        best = window
            if best:
                duration = (best[-1].sip_timestamp - best[0].sip_timestamp).total_seconds()
                clusters.append(
                    (
                        len(best),
                        sum(row.notional for row in best),
                        len({row.exchange for row in best if row.exchange is not None}),
                        duration,
                        by_contract[contract_id],
                        best,
                    )
                )
        clusters.sort(
            key=lambda item: (
                -item[0], -item[1], -item[2], item[3],
                item[5][0].sip_timestamp, item[4].contract_id,
            )
        )
        if not clusters:
            return (
                self._suppressed(
                    matrix_id,
                    snapshots[0],
                    registration,
                    ("NO_QUALIFYING_SWEEP_LIKE_WINDOW" if trades else "TRADE_WINDOW_NOT_AVAILABLE",),
                ),
            )
        return tuple(
            self._research_candidate(
                matrix_id,
                registration,
                StructureType.SWEEP_LIKE_CLUSTER,
                rank,
                snapshot,
                context,
                "qualifying_print_count",
                float(print_count),
                {
                    "window_start": window[0].sip_timestamp.isoformat(),
                    "window_end": window[-1].sip_timestamp.isoformat(),
                    "watermark_time": context.market_data_time.isoformat(),
                    "qualifying_print_count": print_count,
                    "total_notional": str(total_notional),
                    "distinct_exchange_count": exchange_count,
                    "window_duration_seconds": duration,
                    "contract_id": snapshot.contract_id,
                    "contributing_event_keys": [
                        [
                            trade.provider,
                            trade.contract_ticker,
                            trade.sip_timestamp.isoformat(),
                            trade.sequence_number,
                            trade.payload_sha256,
                        ]
                        for trade in window
                    ],
                    "aggressor_side": None,
                    "institutional_owner": None,
                },
            )
            for rank, (print_count, total_notional, exchange_count, duration, snapshot, window)
            in enumerate(clusters[: policy.maximum_candidates], start=1)
        )

    def _smile(self, matrix_id: UUID, snapshots: tuple[OptionContractSnapshot, ...], context: StrategyContextSnapshot) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["VOLATILITY_SMILE_DISTORTION"]
        policy = self.policy.smile
        by_contract = {row.contract_id: row for row in snapshots}
        fits = fit_smile_groups(
            tuple(
                SmileInput(
                    contract_id=row.contract_id,
                    contract_type=row.contract_type,
                    expiration_date=row.expiration_date,
                    strike=row.strike,
                    spot=row.spot,
                    local_iv=float(row.local_iv),
                )
                for row in snapshots
            ),
            minimum_strikes=policy.minimum_strikes,
        )
        outputs: list[tuple[float, OptionContractSnapshot, dict[str, object]]] = []
        for fit in fits:
            for entry in qualifying_distortions(
                fit, minimum_absolute_robust_z=policy.minimum_absolute_robust_z
            ):
                outputs.append((
                    abs(entry.robust_z),
                    by_contract[entry.contract_id],
                    {
                        "expiration_date": fit.expiration_date.isoformat(),
                        "contract_type": fit.contract_type.value,
                        "robust_residual_z": entry.robust_z,
                        "neighboring_consistency": True,
                        "fit_coefficients": coefficient_payload(fit.coefficients),
                        "input_count": fit.input_count,
                    },
                ))
        outputs.sort(key=lambda item: (-item[0], item[1].expiration_date, item[1].contract_type.value, item[1].contract_id))
        if not outputs:
            return (self._suppressed(matrix_id, snapshots[0], registration, ("NO_VALID_SMILE_DISTORTION",)),)
        selected: list[tuple[float, OptionContractSnapshot, dict[str, object]]] = []
        counts: dict[tuple[object, ContractType], int] = defaultdict(int)
        for item in outputs:
            key = (item[1].expiration_date, item[1].contract_type)
            if counts[key] >= policy.maximum_candidates_per_expiration_type:
                continue
            counts[key] += 1
            selected.append(item)
        return tuple(self._research_candidate(matrix_id, registration, StructureType.VOLATILITY_DISTORTION, rank, row, context, "absolute_robust_residual_z", score, evidence) for rank, (score, row, evidence) in enumerate(selected, start=1))

    def _candidate(
        self,
        matrix_id: UUID,
        registration: StrategyRegistration,
        structure_type: StructureType,
        risk_class: StructureRiskClass,
        rank: int,
        legs: tuple[CandidateLeg, ...],
        payoff: PayoffSummary,
        context: StrategyContextSnapshot,
        *,
        primary_metric_name: str,
        primary_metric_value: float | None,
        rank_components: dict[str, object],
        primary_evidence: dict[str, object],
        capital_at_risk: Decimal | None = None,
        collateral_required: Decimal | None = None,
        return_on_collateral: float | None = None,
        return_on_risk: float | None = None,
        reason_codes: tuple[str, ...] = (),
        management_policy: dict[str, object] | None = None,
    ) -> OptionCandidate:
        candidate_id, identity = candidate_identity(matrix_id, registration.strategy_name, self.strategy_version, structure_type, tuple(leg.contract_id for leg in legs), primary_metric_name)
        return OptionCandidate(
            candidate_id=candidate_id,
            identity_sha256=identity,
            matrix_id=matrix_id,
            strategy_name=registration.strategy_name,
            strategy_version=self.strategy_version,
            underlyer=context.underlyer,
            candidate_kind=CandidateKind.SINGLE_CONTRACT if len(legs) == 1 else CandidateKind.MULTI_LEG,
            strategy_archetype=registration.strategy_archetype,
            persona_tags=registration.persona_tags,
            structure_type=structure_type,
            structure_risk_class=risk_class,
            expiration_date=legs[0].expiration_date,
            rank=rank,
            status=CandidateStatus.SELECTED,
            primary_metric_name=primary_metric_name,
            primary_metric_value=float(primary_metric_value) if primary_metric_value is not None else None,
            rank_components=rank_components,
            primary_evidence=primary_evidence,
            legs=legs,
            net_premium=payoff.net_premium,
            collateral_required=collateral_required,
            capital_at_risk=capital_at_risk,
            maximum_profit=payoff.maximum_profit,
            maximum_loss=payoff.maximum_loss,
            return_on_collateral=return_on_collateral,
            return_on_risk=return_on_risk,
            breakevens=payoff.breakevens,
            execution_eligibility=None,
            reason_codes=tuple(dict.fromkeys(reason_codes)),
            management_policy_version=self.strategy_version if management_policy else None,
            management_policy=management_policy or {},
            policy_sha256=self.policy_sha256,
            model_version=legs[0].model_version,
            context_snapshot_id=context.context_snapshot_id,
            iv_context_id=None,
            market_data_time=context.market_data_time,
            observed_time=context.observed_time,
            valid_until=context.market_data_time + timedelta(seconds=900),
        )

    def _research_candidate(self, matrix_id: UUID, registration: StrategyRegistration, structure_type: StructureType, rank: int, row: OptionContractSnapshot, context: StrategyContextSnapshot, metric_name: str, metric_value: float, evidence: dict[str, object]) -> OptionCandidate:
        candidate_id, identity = candidate_identity(matrix_id, registration.strategy_name, self.strategy_version, structure_type, (row.contract_id,), metric_name)
        return OptionCandidate(candidate_id, identity, matrix_id, registration.strategy_name, self.strategy_version, context.underlyer, CandidateKind.RESEARCH_ONLY, registration.strategy_archetype, registration.persona_tags, structure_type, StructureRiskClass.RESEARCH_CONTEXT, row.expiration_date, rank, CandidateStatus.SELECTED, metric_name, float(metric_value), {metric_name: metric_value, "contract_id": row.contract_id}, evidence, (), None, None, None, None, None, None, None, (), None, ("RESEARCH_ONLY", "QUOTE_LIQUIDITY_NOT_AVAILABLE"), None, {}, self.policy_sha256, row.model_version, context.context_snapshot_id, None, context.market_data_time, context.observed_time, context.market_data_time + timedelta(seconds=900))

    def _suppressed(self, matrix_id: UUID, reference: OptionContractSnapshot, registration: StrategyRegistration, reasons: tuple[str, ...]) -> OptionCandidate:
        structure = registration.allowed_structure_types[0]
        candidate_id, identity = candidate_identity(matrix_id, registration.strategy_name, self.strategy_version, structure, (), "SUPPRESSION")
        return OptionCandidate(candidate_id, identity, matrix_id, registration.strategy_name, self.strategy_version, reference.underlyer, CandidateKind.RESEARCH_ONLY, registration.strategy_archetype, registration.persona_tags, structure, StructureRiskClass.RESEARCH_CONTEXT, None, 1, CandidateStatus.SUPPRESSED, None, None, {"suppression_rank": 1}, {"source_contract_count": 0}, (), None, None, None, None, None, None, None, (), None, tuple(dict.fromkeys(reasons)), None, {}, self.policy_sha256, reference.model_version, None, None, reference.market_data_time, reference.first_observed_at, None)

    def _capability_reasons(self, context: StrategyContextSnapshot) -> tuple[str, ...]:
        return self.execution_gate_ledger(context).reason_codes

    def execution_gate_ledger(
        self,
        context: StrategyContextSnapshot,
        candidate_kind: CandidateKind = CandidateKind.MULTI_LEG,
        *,
        equity_direction_required: bool = False,
    ) -> ExecutionGateLedger:
        return evaluate_execution_gates(
            candidate_kind=candidate_kind,
            context_reason_codes=tuple(context.reason_codes),
            equity_reason_codes=tuple(context.equity_reason_codes),
            equity_direction_required=equity_direction_required,
            equity_direction_available=context.qualified_direction is not None,
            quotes_available=self.quotes_available,
            risk_engine_available=self.risk_engine_available,
            read_only=self.read_only,
        )

    @staticmethod
    def _direction_block(
        context: StrategyContextSnapshot,
        *,
        allowed: frozenset[str],
    ) -> tuple[str, ...]:
        if context.equity_context_snapshot_id is None:
            return ()
        if context.equity_context_status == "CONFLICTED":
            return ("EQUITY_DIRECTION_CONFLICT",)
        if context.qualified_direction is None:
            return ("QUALIFIED_EQUITY_DIRECTION_UNAVAILABLE",)
        if context.qualified_direction not in allowed:
            return ("QUALIFIED_EQUITY_DIRECTION_OPPOSES_STRATEGY",)
        return ()


def _leg(snapshot: OptionContractSnapshot, index: int, side: OptionSide, ratio: int = 1) -> CandidateLeg:
    return CandidateLeg(index, snapshot.snapshot_id, snapshot.contract_id, snapshot.contract_ticker, side, ratio, snapshot.shares_per_contract, snapshot.expiration_date, snapshot.strike, snapshot.contract_type, snapshot.spot, snapshot.time_to_expiration_years, snapshot.risk_free_rate, snapshot.dividend_yield, snapshot.model_mark, snapshot.local_iv, snapshot.local_delta, snapshot.local_gamma, snapshot.local_theta_per_day, snapshot.local_vega_per_vol_point, snapshot.local_rho_per_rate_point, snapshot.market_data_time, snapshot.mark_source.value, snapshot.model_version, tuple(flag.value for flag in snapshot.quality_flags), snapshot.valuation_policy_version, snapshot.valuation_policy_sha256)


def _dte_lane(calendar_dte: int, policy) -> str:
    if calendar_dte <= policy.near_lane_maximum_dte:
        return "NEAR"
    if calendar_dte <= policy.short_lane_maximum_dte:
        return "SHORT"
    return "MEDIUM"


def _long_premium_economics(
    row: OptionContractSnapshot,
) -> tuple[float, float, float] | None:
    """Breakeven distance versus the one-sigma move implied over the option's life."""
    if row.single_contract_breakeven is None or row.local_iv is None:
        return None
    spot = float(row.spot)
    if spot <= 0 or row.time_to_expiration_years <= 0:
        return None
    expected_move = float(row.local_iv) * math.sqrt(row.time_to_expiration_years)
    if expected_move <= 0:
        return None
    required_move = abs(float(row.single_contract_breakeven) - spot) / spot
    return required_move, expected_move, required_move / expected_move


def _strike_volume_surge(
    snapshots: tuple[OptionContractSnapshot, ...],
) -> dict[Decimal, float]:
    """Per-strike day volume relative to the median strike, over 0-DTE contracts."""
    totals: dict[Decimal, int] = {}
    for snapshot in snapshots:
        if snapshot.calendar_dte != 0 or snapshot.day_volume is None:
            continue
        totals[snapshot.strike] = totals.get(snapshot.strike, 0) + snapshot.day_volume
    if not totals:
        return {}
    baseline = median(totals.values())
    if baseline <= 0:
        return {strike: 0.0 for strike in totals}
    return {strike: volume / baseline for strike, volume in totals.items()}


def _structure_sort(item: tuple[float, tuple[CandidateLeg, ...], StructureType, dict[str, object]]) -> tuple[object, ...]:
    strength, legs, structure, _ = item
    return (-strength, structure.value, tuple(leg.contract_id for leg in legs))


def _snapshot_for_leg(legs: tuple[CandidateLeg, ...], snapshots: Iterable[OptionContractSnapshot]) -> tuple[OptionContractSnapshot, ...]:
    by_id = {snapshot.contract_id: snapshot for snapshot in snapshots}
    return tuple(by_id[leg.contract_id] for leg in legs)


def _decision_context(market_time, observed_time):
    from options.domain import DecisionContext

    return DecisionContext(market_time, observed_time)