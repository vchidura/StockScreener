from __future__ import annotations

import hashlib
import json
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, execute_values

from options.strategies.domain import CandidateKind, CandidateStatus, OptionCandidate, OptionSide, StrategyContextSnapshot
from options.strategies.engine import StrategyScanResult
from options.strategies.registry import STRATEGY_REGISTRY
from options.errors import DuplicateFactConflict

from .base import ConnectionFactory, PostgresRepository


class OptionStrategyRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist(
        self,
        context: StrategyContextSnapshot,
        result: StrategyScanResult,
    ) -> int:
        if any(candidate.matrix_id != context.matrix_id for candidate in result.candidates):
            raise ValueError("all candidates must belong to the context matrix")
        evidence_ids = {
            candidate.candidate_id: uuid5(
                NAMESPACE_URL,
                f"option-decision-evidence:{candidate.candidate_id}",
            )
            for candidate in result.candidates
        }
        with self._cursor() as cursor:
            self._persist_registry(cursor, context, result)
            self._persist_context(cursor, context)
            for candidate in result.candidates:
                evidence_id = evidence_ids[candidate.candidate_id]
                self._persist_evidence(cursor, context, candidate, evidence_id)
                self._persist_candidate(cursor, context, candidate, evidence_id)
                self._persist_legs(cursor, candidate)
                if candidate.status is not CandidateStatus.SELECTED:
                    self._persist_suppression(cursor, context, candidate)
                elif candidate.candidate_kind is not CandidateKind.RESEARCH_ONLY:
                    self._persist_signal(cursor, candidate)
                if candidate.status is CandidateStatus.SELECTED:
                    self._persist_research_artifact(cursor, candidate)
            self._persist_scenarios(cursor, result)
        return len(result.candidates)

    @staticmethod
    def _persist_registry(
        cursor,
        context: StrategyContextSnapshot,
        result: StrategyScanResult,
    ) -> None:
        versions = {
            candidate.strategy_name: candidate.strategy_version
            for candidate in result.candidates
        }
        execute_values(
            cursor,
            """
            INSERT INTO option_strategy_registry (
                strategy_name, strategy_version, display_name, strategy_archetype,
                persona_tags, allowed_structure_types, allowed_risk_classes,
                presentation_metadata, policy_sha256, effective_from
            ) VALUES %s
            ON CONFLICT (strategy_name, strategy_version) DO NOTHING
            """,
            [
                (
                    item.strategy_name,
                    versions.get(item.strategy_name, context.policy_version),
                    item.display_name,
                    item.strategy_archetype,
                    list(item.persona_tags),
                    [value.value for value in item.allowed_structure_types],
                    [value.value for value in item.allowed_risk_classes],
                    Json({"description": item.description}),
                    context.policy_sha256,
                    context.observed_time,
                )
                for item in STRATEGY_REGISTRY
            ],
        )
        cursor.execute(
            """
            SELECT strategy_name, strategy_version, policy_sha256
            FROM option_strategy_registry
            WHERE (strategy_name, strategy_version) IN (
                SELECT * FROM UNNEST(%s::TEXT[], %s::TEXT[])
            )
            """,
            (
                [item.strategy_name for item in STRATEGY_REGISTRY],
                [versions.get(item.strategy_name, context.policy_version) for item in STRATEGY_REGISTRY],
            ),
        )
        mismatches = [
            row["strategy_name"]
            for row in cursor.fetchall()
            if row["policy_sha256"] != context.policy_sha256
        ]
        if mismatches:
            raise DuplicateFactConflict(
                "strategy version must change when its policy hash changes: "
                + ", ".join(sorted(mismatches))
            )

    @staticmethod
    def _persist_context(cursor, context: StrategyContextSnapshot) -> None:
        cursor.execute(
            """
            INSERT INTO option_context_snapshots (
                context_snapshot_id, matrix_id, underlying, market_data_time,
                observed_time, status, daily_close, daily_ema_50,
                daily_ema_50_input_bars, daily_window_start, hourly_close,
                hourly_ema_20, hourly_ema_20_input_bars, hourly_window_start,
                trend_state, earnings_blackout_state, fed_blackout_state,
                quote_spread_state, reason_codes, source_bar_keys,
                policy_version, policy_sha256
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL,
                %s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (matrix_id) DO NOTHING
            """,
            (
                context.context_snapshot_id,
                context.matrix_id,
                context.underlyer,
                context.market_data_time,
                context.observed_time,
                context.status.value,
                context.daily_close,
                context.daily_ema_50,
                context.daily_input_bars,
                context.hourly_close,
                context.hourly_ema_20,
                context.hourly_input_bars,
                context.trend_state,
                context.earnings_blackout_state,
                context.fed_blackout_state,
                context.quote_spread_state,
                list(context.reason_codes),
                Json(list(context.source_bar_keys)),
                context.policy_version,
                context.policy_sha256,
            ),
        )

    @staticmethod
    def _persist_evidence(cursor, context, candidate, evidence_id) -> None:
        cursor.execute(
            """
            INSERT INTO option_decision_evidence (
                evidence_id, matrix_id, decision_type, strategy_name,
                strategy_version, normalized_legs, underlying_mark,
                market_data_time, source_observation_time, context_snapshot_id,
                context, rank_components, trigger_values, quality_flags,
                policy_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (evidence_id) DO NOTHING
            """,
            (
                evidence_id,
                candidate.matrix_id,
                "CANDIDATE" if candidate.status is CandidateStatus.SELECTED else "SUPPRESSION",
                candidate.strategy_name,
                candidate.strategy_version,
                Json([
                    {
                        "leg_index": leg.leg_index,
                        "contract_id": leg.contract_id,
                        "side": leg.side.value,
                        "ratio": leg.ratio,
                        "model_mark": str(leg.model_mark),
                    }
                    for leg in candidate.legs
                ]),
                candidate.legs[0].spot if candidate.legs else None,
                candidate.market_data_time,
                candidate.observed_time,
                context.context_snapshot_id,
                Json({
                    "status": context.status.value,
                    "trend_state": context.trend_state,
                    "earnings_blackout_state": context.earnings_blackout_state,
                    "fed_blackout_state": context.fed_blackout_state,
                    "reason_codes": context.reason_codes,
                }),
                Json(dict(candidate.rank_components)),
                Json(dict(candidate.primary_evidence)),
                list(candidate.reason_codes),
                candidate.policy_sha256,
            ),
        )

    @staticmethod
    def _persist_candidate(cursor, context, candidate, evidence_id) -> None:
        cursor.execute(
            """
            INSERT INTO option_strategy_candidates (
                candidate_id, candidate_identity, matrix_id, strategy_name,
                strategy_version, underlying, candidate_kind, strategy_archetype,
                persona_tags, structure_type, structure_risk_class, expiration_date,
                candidate_rank, status, primary_metric_name, primary_metric_value,
                rank_components, primary_evidence, net_premium,
                collateral_required, capital_at_risk, maximum_profit, maximum_loss,
                return_on_collateral, return_on_risk, breakevens,
                execution_eligibility, reason_codes, management_policy_version,
                management_policy, policy_sha256, model_version, iv_context_id,
                context_snapshot_id, decision_evidence_id, market_data_time,
                observed_time, valid_until
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            ) ON CONFLICT (candidate_id) DO NOTHING
            """,
            (
                candidate.candidate_id,
                candidate.identity_sha256,
                candidate.matrix_id,
                candidate.strategy_name,
                candidate.strategy_version,
                candidate.underlyer,
                candidate.candidate_kind.value,
                candidate.strategy_archetype,
                list(candidate.persona_tags),
                candidate.structure_type.value,
                candidate.structure_risk_class.value,
                candidate.expiration_date,
                candidate.rank,
                candidate.status.value,
                candidate.primary_metric_name,
                candidate.primary_metric_value,
                Json(dict(candidate.rank_components)),
                Json(dict(candidate.primary_evidence)),
                candidate.net_premium,
                candidate.collateral_required,
                candidate.capital_at_risk,
                candidate.maximum_profit,
                candidate.maximum_loss,
                candidate.return_on_collateral,
                candidate.return_on_risk,
                list(candidate.breakevens),
                candidate.execution_eligibility.value if candidate.execution_eligibility else None,
                list(candidate.reason_codes),
                candidate.management_policy_version,
                Json(dict(candidate.management_policy)),
                candidate.policy_sha256,
                candidate.model_version,
                candidate.iv_context_id,
                candidate.context_snapshot_id or context.context_snapshot_id,
                evidence_id,
                candidate.market_data_time,
                candidate.observed_time,
                candidate.valid_until,
            ),
        )

    @staticmethod
    def _persist_legs(cursor, candidate: OptionCandidate) -> None:
        if not candidate.legs:
            return
        execute_values(
            cursor,
            """
            INSERT INTO option_candidate_legs (
                candidate_id, leg_index, snapshot_id, contract_id, contract_ticker,
                side, ratio, multiplier, expiration_date, strike, contract_type,
                spot, time_to_expiration_years, risk_free_rate, dividend_yield,
                model_mark, local_iv, local_delta, local_gamma, local_theta_per_day,
                local_vega_per_vol_point, local_rho_per_rate_point,
                source_market_time, mark_source, model_version, quality_flags
            ) VALUES %s
            ON CONFLICT (candidate_id, leg_index) DO NOTHING
            """,
            [
                (
                    candidate.candidate_id, leg.leg_index, leg.snapshot_id,
                    leg.contract_id, leg.contract_ticker, leg.side.value, leg.ratio,
                    leg.multiplier, leg.expiration_date, leg.strike,
                    leg.contract_type.value, leg.spot, leg.time_to_expiration_years,
                    leg.risk_free_rate, leg.dividend_yield, leg.model_mark,
                    leg.local_iv, leg.local_delta, leg.local_gamma,
                    leg.local_theta_per_day, leg.local_vega_per_vol_point,
                    leg.local_rho_per_rate_point, leg.source_market_time,
                    leg.mark_source, leg.model_version, list(leg.quality_flags),
                )
                for leg in candidate.legs
            ],
        )

    @staticmethod
    def _persist_suppression(cursor, context, candidate) -> None:
        cursor.execute(
            """
            INSERT INTO option_signal_suppressions (
                suppression_id, candidate_id, strategy_name, strategy_version,
                decision_time, failed_gate_codes, configuration_version,
                input_provenance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidate_id) DO NOTHING
            """,
            (
                uuid5(NAMESPACE_URL, f"option-suppression:{candidate.candidate_id}"),
                candidate.candidate_id,
                candidate.strategy_name,
                candidate.strategy_version,
                context.market_data_time,
                list(candidate.reason_codes),
                context.policy_version,
                Json({"matrix_id": str(context.matrix_id), "context_snapshot_id": str(context.context_snapshot_id)}),
            ),
        )

    @staticmethod
    def _persist_signal(cursor, candidate: OptionCandidate) -> None:
        idempotency_key = hashlib.sha256(
            f"signal:{candidate.identity_sha256}".encode("ascii")
        ).hexdigest()
        event_id = uuid5(NAMESPACE_URL, f"option-signal:{idempotency_key}")
        action = "SELL" if candidate.net_premium and candidate.net_premium > 0 else "BUY"
        premium_magnitude = abs(candidate.net_premium or 0)
        stop_loss = None
        take_profit = None
        if "stop_loss_multiple" in candidate.management_policy:
            stop_loss = premium_magnitude * _decimal_policy_value(
                candidate.management_policy["stop_loss_multiple"]
            )
        elif "stop_loss_fraction" in candidate.management_policy:
            stop_loss = premium_magnitude * (
                1 - _decimal_policy_value(candidate.management_policy["stop_loss_fraction"])
            )
        if "take_profit_fraction" in candidate.management_policy:
            take_profit = premium_magnitude * (
                1 - _decimal_policy_value(candidate.management_policy["take_profit_fraction"])
                if action == "SELL"
                else 1 + _decimal_policy_value(candidate.management_policy["take_profit_fraction"])
            )
        cursor.execute(
            """
            INSERT INTO option_signal_events (
                event_id, idempotency_key, source_candidate_id, underlying,
                strategy_name, strategy_version, market_data_time, observed_time,
                action, net_premium, stop_loss, take_profit, valid_until,
                confidence, data_quality, execution_eligibility, status,
                blocked_reasons, expected_leg_count, metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, NULL, 'RESEARCH_DELAYED', %s, %s, %s, %s, %s
            ) ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event_id,
                idempotency_key,
                candidate.candidate_id,
                candidate.underlyer,
                candidate.strategy_name,
                candidate.strategy_version,
                candidate.market_data_time,
                candidate.observed_time,
                action,
                candidate.net_premium,
                stop_loss,
                take_profit,
                candidate.valid_until,
                candidate.execution_eligibility.value if candidate.execution_eligibility else None,
                "READY" if candidate.execution_eligibility else "BLOCKED",
                list(candidate.reason_codes),
                len(candidate.legs),
                Json({"structure_type": candidate.structure_type.value}),
            ),
        )
        execute_values(
            cursor,
            """
            INSERT INTO option_signal_legs (
                event_id, leg_index, contract_id, contract_ticker, action, ratio,
                multiplier, model_mark, local_iv, local_gamma, expiration_date, strike
            ) VALUES %s ON CONFLICT (event_id, leg_index) DO NOTHING
            """,
            [
                (
                    event_id, leg.leg_index, leg.contract_id, leg.contract_ticker,
                    leg.side.value, leg.ratio, leg.multiplier, leg.model_mark,
                    leg.local_iv, leg.local_gamma, leg.expiration_date, leg.strike,
                )
                for leg in candidate.legs
            ],
        )
        cursor.execute(
            """
            INSERT INTO option_signal_occurrences (
                occurrence_id, event_id, market_data_time, observed_time,
                source_batch_id, mark_diagnostics, trigger_diagnostics
            ) VALUES (
                %s, %s, %s, %s,
                (SELECT batch_id FROM option_analysis_runs WHERE matrix_id = %s),
                %s, %s
            ) ON CONFLICT (event_id, market_data_time) DO NOTHING
            """,
            (
                uuid5(
                    NAMESPACE_URL,
                    f"option-signal-occurrence:{event_id}:{candidate.market_data_time.isoformat()}",
                ),
                event_id,
                candidate.market_data_time,
                candidate.observed_time,
                candidate.matrix_id,
                Json({
                    "net_premium": str(candidate.net_premium),
                    "leg_marks": [str(leg.model_mark) for leg in candidate.legs],
                }),
                Json(dict(candidate.primary_evidence)),
            ),
        )

    @staticmethod
    def _persist_research_artifact(cursor, candidate: OptionCandidate) -> None:
        if candidate.strategy_name == "SWEEP_LIKE_CLUSTER":
            evidence = candidate.primary_evidence
            cursor.execute(
                """
                INSERT INTO option_flow_windows (
                    flow_window_id, matrix_id, underlying, window_start,
                    window_end, watermark_time, distinct_contract_count,
                    distinct_exchange_count, qualifying_print_count,
                    total_notional, call_notional, otm_call_print_count,
                    detector_version, contributing_event_keys
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, 1, %s, %s,
                    %s, %s, %s, %s, %s
                ) ON CONFLICT (flow_window_id) DO NOTHING
                """,
                (
                    uuid5(NAMESPACE_URL, f"option-flow-window:{candidate.candidate_id}"),
                    candidate.matrix_id,
                    candidate.underlyer,
                    evidence["window_start"],
                    evidence["window_end"],
                    evidence["watermark_time"],
                    evidence["distinct_exchange_count"],
                    evidence["qualifying_print_count"],
                    evidence["total_notional"],
                    evidence["total_notional"],
                    evidence["qualifying_print_count"],
                    candidate.strategy_version,
                    Json(evidence["contributing_event_keys"]),
                ),
            )
        elif candidate.strategy_name == "VOLATILITY_SMILE_DISTORTION":
            evidence = candidate.primary_evidence
            cursor.execute(
                """
                INSERT INTO option_volatility_surfaces (
                    volatility_surface_id, matrix_id, underlying,
                    expiration_date, contract_type, window_start, window_end,
                    input_count, fit_model, fit_version, fit_diagnostics,
                    residual_distribution, coefficients
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    'QUADRATIC_LOG_MONEYNESS', %s, %s, %s, %s
                ) ON CONFLICT (
                    matrix_id, expiration_date, contract_type, fit_version
                ) DO NOTHING
                """,
                (
                    uuid5(
                        NAMESPACE_URL,
                        "option-volatility-surface:"
                        f"{candidate.matrix_id}:{evidence['expiration_date']}:"
                        f"{evidence['contract_type']}:{candidate.strategy_version}",
                    ),
                    candidate.matrix_id,
                    candidate.underlyer,
                    evidence["expiration_date"],
                    evidence["contract_type"],
                    candidate.market_data_time,
                    candidate.market_data_time,
                    evidence["input_count"],
                    candidate.strategy_version,
                    Json({"neighboring_consistency_required": True}),
                    Json({"selected_residual_z": evidence["robust_residual_z"]}),
                    Json(evidence["fit_coefficients"]),
                ),
            )

    @staticmethod
    def _persist_scenarios(cursor, result: StrategyScanResult) -> None:
        if not result.scenarios:
            return
        execute_values(
            cursor,
            """
            INSERT INTO option_scenario_results (
                scenario_result_id, candidate_id, snapshot_id, scenario_key,
                spot_shock_fraction, iv_shock_fraction, time_fraction_remaining,
                repriced_value, profit_loss, delta, gamma, theta_per_day,
                vega_per_vol_point, terminal, assumptions, quality_flags,
                model_version, policy_sha256
            ) VALUES %s ON CONFLICT (scenario_result_id) DO NOTHING
            """,
            [
                (
                    item.scenario_result_id, item.candidate_id, None,
                    item.scenario_key, item.spot_shock_fraction,
                    item.iv_shock_fraction, item.time_fraction_remaining,
                    item.repriced_value, item.profit_loss, item.delta, item.gamma,
                    item.theta_per_day, item.vega_per_vol_point, item.terminal,
                    Json(dict(item.assumptions)), list(item.quality_flags),
                    item.model_version, item.policy_sha256,
                )
                for item in result.scenarios
            ],
        )


def _decimal_policy_value(value):
    from decimal import Decimal

    return Decimal(str(value))