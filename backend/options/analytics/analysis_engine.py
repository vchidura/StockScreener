from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from options.config import DeveloperPolicy
from options.domain import DecisionContext, OptionContractSnapshot, OptionExpirationAnalytics

from .chain_analysis import (
    ChainHealth,
    ContractAnalysis,
    ExpirationContractInput,
    analyze_contract,
    analyze_expirations,
    build_chain_health,
)


@dataclass(frozen=True, slots=True)
class UnderlyingAnalysis:
    underlyer: str
    total_day_volume: int
    total_open_interest: int
    expiration_count: int
    valid_atm_term_points: int
    caveats: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OptionAnalysisSnapshot:
    matrix_id: UUID
    context: DecisionContext
    chain_health: ChainHealth
    contracts: tuple[ContractAnalysis, ...]
    expirations: tuple[OptionExpirationAnalytics, ...]
    underlying: UnderlyingAnalysis


class OptionAnalysisEngine:
    def __init__(self, policy: DeveloperPolicy) -> None:
        self.policy = policy

    def analyze(
        self,
        matrix_id: UUID,
        snapshots: tuple[OptionContractSnapshot, ...],
        context: DecisionContext,
        *,
        received_count: int,
        catalog_matched_count: int,
        unknown_reference_count: int,
        reference_drift_failed: bool,
        batch_complete: bool,
    ) -> OptionAnalysisSnapshot:
        if not snapshots:
            raise ValueError("analysis requires at least one normalized snapshot")
        underlyers = {snapshot.underlyer for snapshot in snapshots}
        batch_ids = {snapshot.batch_id for snapshot in snapshots}
        if len(underlyers) != 1 or len(batch_ids) != 1:
            raise ValueError("analysis snapshots must form one underlying batch")
        for snapshot in snapshots:
            snapshot.require_available(context)

        aligned = tuple(snapshot for snapshot in snapshots if snapshot.model_mark is not None)
        converged = tuple(snapshot for snapshot in aligned if snapshot.iv_converged)
        health = build_chain_health(
            received_count=received_count,
            retained_count=len(snapshots),
            catalog_matched_count=catalog_matched_count,
            mark_aligned_count=len(aligned),
            iv_attempt_count=len(aligned),
            iv_converged_count=len(converged),
            unknown_reference_count=unknown_reference_count,
            reference_drift_failed=reference_drift_failed,
            batch_complete=batch_complete,
            minimum_iv_convergence_fraction=float(
                self.policy.model_quality.minimum_iv_success_fraction
            ),
        )
        contract_analyses = tuple(analyze_contract(snapshot) for snapshot in converged)
        expiration_inputs = tuple(
            ExpirationContractInput(
                contract_id=snapshot.contract_id,
                contract_type=snapshot.contract_type,
                expiration_date=snapshot.expiration_date,
                strike=snapshot.strike,
                spot=snapshot.spot,
                time_to_expiration_years=snapshot.time_to_expiration_years,
                risk_free_rate=snapshot.risk_free_rate,
                dividend_yield=snapshot.dividend_yield,
                local_iv=snapshot.local_iv,
                local_delta=snapshot.local_delta,
                day_volume=snapshot.day_volume,
                open_interest=snapshot.open_interest,
            )
            for snapshot in converged
        )
        expiration_analyses = analyze_expirations(
            matrix_id,
            expiration_inputs,
            maximum_delta_interpolation_gap=(
                self.policy.analysis.maximum_delta_interpolation_gap
            ),
            oi_wall_percentile=self.policy.oi_walls.percentile,
            minimum_oi_wall_robust_z=self.policy.oi_walls.minimum_robust_z,
            maximum_oi_wall_clusters=(
                self.policy.oi_walls.maximum_clusters_per_expiration_type
            ),
            allow_zero_mad_wall_fallback=(
                self.policy.oi_walls.allow_zero_mad_fallback
            ),
        )
        caveats = tuple(
            dict.fromkeys(
                reason
                for expiration in expiration_analyses
                for reason in expiration.quality_reasons
            )
        )
        return OptionAnalysisSnapshot(
            matrix_id=matrix_id,
            context=context,
            chain_health=health,
            contracts=contract_analyses,
            expirations=expiration_analyses,
            underlying=UnderlyingAnalysis(
                underlyer=next(iter(underlyers)),
                total_day_volume=sum(snapshot.day_volume or 0 for snapshot in snapshots),
                total_open_interest=sum(
                    snapshot.open_interest or 0 for snapshot in snapshots
                ),
                expiration_count=len(
                    {snapshot.expiration_date for snapshot in snapshots}
                ),
                valid_atm_term_points=sum(
                    expiration.atm_iv is not None for expiration in expiration_analyses
                ),
                caveats=caveats,
            ),
        )