from __future__ import annotations

from typing import Any, Collection
from uuid import UUID

from psycopg2.extras import execute_values

from options.domain import (
    AssetType,
    ContractType,
    DataQualityFlag,
    DecisionContext,
    ExerciseStyle,
    MarkSource,
    OptionContractSnapshot,
)

from .base import ConnectionFactory, PostgresRepository


class OptionSnapshotRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist(
        self,
        snapshots: Collection[OptionContractSnapshot],
        asset_type: AssetType,
        policy_version: str,
        policy_sha256: str,
    ) -> int:
        if not snapshots:
            return 0
        ordered = tuple(snapshots)
        with self._cursor() as cursor:
            partition_probes = {
                (snapshot.first_observed_at.year, snapshot.first_observed_at.month):
                snapshot.first_observed_at
                for snapshot in ordered
            }
            for observed_at in partition_probes.values():
                cursor.execute(
                    "SELECT option_market_data_partitions_ready(%s) AS ready",
                    (observed_at,),
                )
                if not cursor.fetchone()["ready"]:
                    raise RuntimeError("option snapshot partition is missing")

            claimed_rows = execute_values(
                cursor,
                """
                INSERT INTO option_snapshot_fact_keys (
                    snapshot_id, contract_id, provider, market_data_time,
                    normalized_payload_sha256, first_observed_at
                ) VALUES %s
                ON CONFLICT (
                    contract_id, provider, market_data_time, normalized_payload_sha256
                ) DO NOTHING
                RETURNING snapshot_id
                """,
                [
                    (
                        snapshot.snapshot_id,
                        snapshot.contract_id,
                        snapshot.provider,
                        snapshot.market_data_time,
                        snapshot.normalized_payload_sha256,
                        snapshot.first_observed_at,
                    )
                    for snapshot in ordered
                ],
                page_size=1000,
                fetch=True,
            )
            claimed_ids = {row["snapshot_id"] for row in claimed_rows}
            ordered = tuple(
                snapshot for snapshot in ordered if snapshot.snapshot_id in claimed_ids
            )
            if not ordered:
                return 0
            values = [
                (
                    snapshot.snapshot_id,
                    snapshot.contract_id,
                    snapshot.contract_ticker,
                    snapshot.underlyer,
                    asset_type.value,
                    snapshot.provider,
                    snapshot.batch_id,
                    snapshot.contract_type.value,
                    snapshot.expiration_date,
                    snapshot.expiration_cutoff,
                    snapshot.calendar_dte,
                    snapshot.time_to_expiration_years,
                    snapshot.strike,
                    snapshot.shares_per_contract,
                    snapshot.exercise_style.value,
                    snapshot.spot,
                    snapshot.spot_market_data_time,
                    snapshot.bid,
                    snapshot.ask,
                    snapshot.midpoint,
                    snapshot.display_mark,
                    snapshot.model_mark,
                    snapshot.mark_market_data_time,
                    snapshot.mark_source.value,
                    snapshot.day_volume,
                    snapshot.open_interest,
                    snapshot.market_data_time,
                    snapshot.first_observed_at,
                    snapshot.revised_observed_at,
                    snapshot.data_delay_seconds,
                    snapshot.local_iv,
                    snapshot.local_gamma,
                    snapshot.local_delta,
                    snapshot.local_theta_per_day,
                    snapshot.local_vega_per_vol_point,
                    snapshot.local_rho_per_rate_point,
                    snapshot.intrinsic_value,
                    snapshot.extrinsic_value,
                    snapshot.single_contract_breakeven,
                    snapshot.provider_iv,
                    snapshot.provider_gamma,
                    snapshot.risk_free_rate,
                    snapshot.dividend_yield,
                    snapshot.iv_converged,
                    snapshot.iv_solver,
                    snapshot.iv_iteration_count,
                    snapshot.iv_price_error,
                    snapshot.iv_failure_reason,
                    snapshot.model_version,
                    [flag.value for flag in snapshot.quality_flags],
                    snapshot.raw_payload_sha256,
                    snapshot.normalized_payload_sha256,
                    snapshot.revision,
                    policy_version,
                    policy_sha256,
                )
                for snapshot in ordered
            ]
            execute_values(
                cursor,
                """
                INSERT INTO option_chain_snapshots (
                    snapshot_id, contract_id, contract_ticker, underlying, asset_type,
                    provider, batch_id, contract_type, expiration_date,
                    expiration_cutoff, calendar_dte, time_to_expiration_years, strike,
                    shares_per_contract, exercise_style, spot, spot_market_data_time,
                    bid, ask, midpoint, display_mark, model_mark, mark_market_data_time,
                    mark_source, day_volume, open_interest, market_data_time,
                    first_observed_at, revised_observed_at, data_delay_seconds,
                    local_iv, local_gamma, local_delta, local_theta_per_day,
                    local_vega_per_vol_point, local_rho_per_rate_point, intrinsic_value,
                    extrinsic_value, single_contract_breakeven, provider_iv,
                    provider_gamma, risk_free_rate, dividend_yield, iv_converged,
                    iv_solver, iv_iteration_count, iv_price_error, iv_failure_reason,
                    model_version, quality_flags, raw_payload_sha256,
                    normalized_payload_sha256, revision, policy_version, policy_sha256
                ) VALUES %s
                ON CONFLICT (
                    contract_id, provider, market_data_time, normalized_payload_sha256,
                    first_observed_at
                ) DO NOTHING
                """,
                values,
                page_size=1000,
            )
            return cursor.rowcount

    def list_for_batch(
        self,
        batch_id: UUID,
        context: DecisionContext,
    ) -> tuple[OptionContractSnapshot, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (contract_id)
                    snapshot_id, contract_id, contract_ticker, underlying, provider,
                    contract_type, expiration_date, expiration_cutoff, calendar_dte,
                    time_to_expiration_years, strike, shares_per_contract,
                    exercise_style, spot, spot_market_data_time, bid, ask, midpoint,
                    display_mark, model_mark, mark_market_data_time, mark_source,
                    day_volume, open_interest, market_data_time, first_observed_at,
                    revised_observed_at, local_iv, local_gamma, local_delta,
                    local_theta_per_day, local_vega_per_vol_point,
                    local_rho_per_rate_point, intrinsic_value, extrinsic_value,
                    single_contract_breakeven, provider_iv, provider_gamma,
                    risk_free_rate, dividend_yield, iv_converged, iv_solver,
                    iv_iteration_count, iv_price_error, iv_failure_reason, model_version,
                    quality_flags, batch_id, raw_payload_sha256,
                    normalized_payload_sha256, revision
                FROM option_chain_snapshots
                WHERE batch_id = %s
                  AND market_data_time <= %s
                  AND COALESCE(revised_observed_at, first_observed_at) <= %s
                ORDER BY
                    contract_id,
                    market_data_time DESC,
                    COALESCE(revised_observed_at, first_observed_at) DESC,
                    revision DESC,
                    snapshot_id
                """,
                (batch_id, context.market_time, context.observed_time),
            )
            rows = cursor.fetchall()
        return tuple(_snapshot(row) for row in rows)


def _snapshot(row: dict[str, Any]) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        snapshot_id=row["snapshot_id"],
        contract_id=row["contract_id"],
        contract_ticker=row["contract_ticker"],
        underlyer=row["underlying"],
        provider=row["provider"],
        contract_type=ContractType(row["contract_type"]),
        expiration_date=row["expiration_date"],
        expiration_cutoff=row["expiration_cutoff"],
        calendar_dte=row["calendar_dte"],
        time_to_expiration_years=row["time_to_expiration_years"],
        strike=row["strike"],
        shares_per_contract=row["shares_per_contract"],
        exercise_style=ExerciseStyle(row["exercise_style"]),
        spot=row["spot"],
        spot_market_data_time=row["spot_market_data_time"],
        bid=row["bid"],
        ask=row["ask"],
        midpoint=row["midpoint"],
        display_mark=row["display_mark"],
        model_mark=row["model_mark"],
        mark_market_data_time=row["mark_market_data_time"],
        mark_source=MarkSource(row["mark_source"]),
        day_volume=row["day_volume"],
        open_interest=row["open_interest"],
        market_data_time=row["market_data_time"],
        first_observed_at=row["first_observed_at"],
        revised_observed_at=row["revised_observed_at"],
        local_iv=row["local_iv"],
        local_gamma=row["local_gamma"],
        local_delta=row["local_delta"],
        local_theta_per_day=row["local_theta_per_day"],
        local_vega_per_vol_point=row["local_vega_per_vol_point"],
        local_rho_per_rate_point=row["local_rho_per_rate_point"],
        intrinsic_value=row["intrinsic_value"],
        extrinsic_value=row["extrinsic_value"],
        single_contract_breakeven=row["single_contract_breakeven"],
        provider_iv=row["provider_iv"],
        provider_gamma=row["provider_gamma"],
        risk_free_rate=row["risk_free_rate"],
        dividend_yield=row["dividend_yield"],
        iv_converged=row["iv_converged"],
        iv_solver=row["iv_solver"],
        iv_iteration_count=row["iv_iteration_count"],
        iv_price_error=row["iv_price_error"],
        iv_failure_reason=row["iv_failure_reason"],
        model_version=row["model_version"],
        quality_flags=tuple(DataQualityFlag(flag) for flag in row["quality_flags"]),
        batch_id=row["batch_id"],
        raw_payload_sha256=row["raw_payload_sha256"],
        normalized_payload_sha256=row["normalized_payload_sha256"],
        revision=row["revision"],
    )