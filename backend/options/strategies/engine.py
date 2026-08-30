from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import timedelta
from decimal import Decimal
from statistics import median
from typing import Iterable
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np

from options.analytics.chain_analysis import ChainHealth
from options.config import StrategyPolicy
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
from .registry import REGISTRY_BY_NAME, STRATEGY_REGISTRY, StrategyRegistration
from .scenarios import build_scenario_grid


@dataclass(frozen=True, slots=True)
class StrategyScanResult:
    candidates: tuple[OptionCandidate, ...]
    scenarios: tuple[ScenarioResult, ...]


class OptionStrategyEngine:
    def __init__(
        self,
        policy: StrategyPolicy,
        policy_sha256: str,
    ) -> None:
        self.policy = policy
        self.policy_sha256 = policy_sha256
        self.strategy_version = policy.strategy_version

    def scan(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        chain_health: ChainHealth,
        expirations: tuple[OptionExpirationAnalytics, ...],
        context: StrategyContextSnapshot,
        trades: tuple[OptionTradeEvent, ...] = (),
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
            return StrategyScanResult(candidates, ())

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
        outputs.extend(self._gamma_squeeze(matrix_id, valid, context))
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
        return StrategyScanResult(candidates, scenarios)

    def _income_wheel(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: StrategyContextSnapshot,
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["INCOME_WHEEL"]
        policy = self.policy.income_wheel
        eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.contract_type is ContractType.PUT
            and policy.minimum_dte <= snapshot.calendar_dte <= policy.maximum_dte
            and snapshot.strike < snapshot.spot
            and snapshot.model_mark is not None
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
                    ("NO_ELIGIBLE_WHEEL_CONTRACT",),
                ),
            )
        results = []
        for rank, snapshot in enumerate(eligible[: policy.maximum_candidates], start=1):
            leg = _leg(snapshot, 0, OptionSide.SELL)
            payoff = evaluate_terminal_payoff((leg,))
            collateral = snapshot.strike * snapshot.shares_per_contract
            reasons = self._capability_reasons(context)
            if snapshot.calendar_dte <= policy.exit_dte:
                reasons = (*reasons, "DTE_AT_OR_BELOW_EXIT_BOUNDARY")
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
    ) -> tuple[OptionCandidate, ...]:
        registration = REGISTRY_BY_NAME["ZERO_DTE_GAMMA_SQUEEZE"]
        policy = self.policy.gamma_squeeze
        eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.calendar_dte == 0
            and abs(float(snapshot.strike / snapshot.spot) - 1) <= policy.maximum_moneyness_fraction
            and snapshot.open_interest is not None
            and snapshot.day_volume is not None
            and snapshot.day_volume / max(snapshot.open_interest, 1) >= policy.minimum_volume_oi_ratio
            and float(snapshot.local_gamma) > policy.minimum_gamma
        ]
        outputs: list[OptionCandidate] = []
        for contract_type in (ContractType.CALL, ContractType.PUT):
            rows = [row for row in eligible if row.contract_type is contract_type]
            rows.sort(
                key=lambda row: (
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

    def _spread_and_range(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        expirations: tuple[OptionExpirationAnalytics, ...],
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
        groups: dict[tuple[object, ContractType], list[OptionContractSnapshot]] = defaultdict(list)
        for row in snapshots:
            groups[(row.expiration_date, row.contract_type)].append(row)
        outputs: list[tuple[float, OptionContractSnapshot, dict[str, object]]] = []
        for (expiration_date, contract_type), rows in sorted(groups.items(), key=lambda item: item[0]):
            rows = sorted(rows, key=lambda row: (row.strike, row.contract_id))
            if len({row.strike for row in rows}) < policy.minimum_strikes or not any(row.strike < row.spot for row in rows) or not any(row.strike > row.spot for row in rows):
                continue
            x = np.asarray([np.log(float(row.strike / row.spot)) for row in rows], dtype=np.float64)
            y = np.asarray([float(row.local_iv) for row in rows], dtype=np.float64)
            coefficients = np.polyfit(x, y, 2)
            residuals = y - np.polyval(coefficients, x)
            residual_median = float(np.median(residuals))
            mad = float(np.median(np.abs(residuals - residual_median)))
            if mad == 0:
                continue
            robust = (residuals - residual_median) / (1.4826 * mad)
            for index in range(1, len(rows) - 1):
                score = float(robust[index])
                neighbors_consistent = np.sign(robust[index - 1]) == np.sign(score) or np.sign(robust[index + 1]) == np.sign(score)
                if abs(score) < policy.minimum_absolute_robust_z or not neighbors_consistent:
                    continue
                row = rows[index]
                outputs.append((abs(score), row, {"expiration_date": expiration_date.isoformat(), "contract_type": contract_type.value, "robust_residual_z": score, "neighboring_consistency": True, "fit_coefficients": coefficients.tolist(), "input_count": len(rows)}))
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

    @staticmethod
    def _capability_reasons(context: StrategyContextSnapshot) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*context.reason_codes, "QUOTE_LIQUIDITY_NOT_AVAILABLE", "PAPER_RISK_ENGINE_NOT_IMPLEMENTED")))


def _leg(snapshot: OptionContractSnapshot, index: int, side: OptionSide, ratio: int = 1) -> CandidateLeg:
    return CandidateLeg(index, snapshot.snapshot_id, snapshot.contract_id, snapshot.contract_ticker, side, ratio, snapshot.shares_per_contract, snapshot.expiration_date, snapshot.strike, snapshot.contract_type, snapshot.spot, snapshot.time_to_expiration_years, snapshot.risk_free_rate, snapshot.dividend_yield, snapshot.model_mark, snapshot.local_iv, snapshot.local_delta, snapshot.local_gamma, snapshot.local_theta_per_day, snapshot.local_vega_per_vol_point, snapshot.local_rho_per_rate_point, snapshot.market_data_time, snapshot.mark_source.value, snapshot.model_version, tuple(flag.value for flag in snapshot.quality_flags))


def _structure_sort(item: tuple[float, tuple[CandidateLeg, ...], StructureType, dict[str, object]]) -> tuple[object, ...]:
    strength, legs, structure, _ = item
    return (-strength, structure.value, tuple(leg.contract_id for leg in legs))


def _snapshot_for_leg(legs: tuple[CandidateLeg, ...], snapshots: Iterable[OptionContractSnapshot]) -> tuple[OptionContractSnapshot, ...]:
    by_id = {snapshot.contract_id: snapshot for snapshot in snapshots}
    return tuple(by_id[leg.contract_id] for leg in legs)


def _decision_context(market_time, observed_time):
    from options.domain import DecisionContext

    return DecisionContext(market_time, observed_time)