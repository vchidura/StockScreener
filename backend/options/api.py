from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from psycopg2 import Error as DatabaseError

from database import get_db_cursor
from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE
from equity.domain import DecisionWatermark
from equity.repositories import EquityEvidenceRepository, EquityReferenceRepository
from options.calendar import OptionExchangeCalendar
from options.alert_plans import AlertManagementPolicy, preview_indicative_alert_plan
from options.alert_orchestration import DRY_RUN_VERSION, dry_run_exposure_keys, finalize_alert_dry_run
from options.alert_qualification import (
    RETAINED_EVIDENCE_VERSION,
    alert_qualification_policy, qualification_policy_sha256,
    qualify_option_alert, retained_alert_evidence, retained_event_horizon,
)
from options.config import load_option_runtime_configuration
from options.discovery import CONTRACT_FILTERS, EligibleChainQuery, contract_filter_sql
from options.outcome_contracts import PACKAGE_ASSESSMENT_POLICY, OptionPackageAssessment
from options.repositories.alert_publications import OptionAlertPublicationRepository
from options.strategies.domain import StructureType
from options.strategies.engine import OptionStrategyEngine
from options.strategies.registry import (
    STRUCTURE_DISCOVERY_CATEGORIES,
    build_discovery_catalog,
)
from options.stock_behavior_gates import (
    STOCK_BEHAVIOR_GATE_POLICY,
    StockBehaviorGateAssessment,
    evaluate_option_stock_behavior,
)
from security_types import is_earnings_applicable_security_type
from options.worker import OptionWorkerSettings


DATA_TIER_LABEL = "15-MINUTE DELAYED RESEARCH DATA"
CANDIDATE_RESEARCH_EVIDENCE_VERSION = "option_candidate_research_evidence_v1"
router = APIRouter(prefix="/api/options", tags=["options"])



class OptionsEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    available: bool
    reason: str | None = None
    data_tier: str = DATA_TIER_LABEL
    generated_at: datetime
    as_of: datetime | None = None
    observed_at: datetime | None = None
    policy_sha256: str | None = None
    model_version: str | None = None
    data: Any = None


class OptionAlertManagementRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    policy_version: str = Field(min_length=1, max_length=80, pattern=r"\S")
    strategy_name: Literal["DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"]
    stop_loss_fraction: Decimal = Field(gt=0, lt=1)
    take_profit_fraction: Decimal = Field(gt=0)
    maximum_hold_seconds: int = Field(strict=True, ge=1, le=366 * 86400)
    minimum_exit_dte: int = Field(default=1, strict=True, ge=1)


class OptionAlertPlanTerms(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    entry_deadline: AwareDatetime | None = None
    exit_deadline: AwareDatetime | None = None
    entry_limit: Decimal | None = Field(default=None, gt=0)


class OptionAlertPreviewRequest(OptionAlertPlanTerms):
    management_policy: OptionAlertManagementRequest | None = None


class OptionAlertDryRunCandidate(OptionAlertPlanTerms):
    candidate_id: UUID


class OptionAlertDryRunStrategy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_name: Literal["INCOME_WHEEL", "SPREAD_RANGE_LOCATOR", "ZERO_DTE_GAMMA_SQUEEZE",
                           "DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"]
    management_source: Literal["ORIGINAL", "EXPLICIT"]
    management_policy: OptionAlertManagementRequest | None = None

    @model_validator(mode="after")
    def validate_management_source(self):
        if (self.management_source == "EXPLICIT") != (self.management_policy is not None):
            raise ValueError("management_source must match the supplied policy")
        if self.management_policy is not None and self.management_policy.strategy_name != self.strategy_name:
            raise ValueError("management policy must match its strategy rule")
        return self


class OptionAlertDryRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_version: str = Field(min_length=1, max_length=80, pattern=r"\S")
    strategies: list[OptionAlertDryRunStrategy] = Field(min_length=1, max_length=5)
    candidates: list[OptionAlertDryRunCandidate] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def validate_distinct_inputs(self):
        if len({row.strategy_name for row in self.strategies}) != len(self.strategies):
            raise ValueError("strategy rules must be distinct")
        if len({row.candidate_id for row in self.candidates}) != len(self.candidates):
            raise ValueError("candidate IDs must be distinct")
        return self


def _configuration():
    return load_option_runtime_configuration()


def _envelope(
    *,
    available: bool,
    reason: str | None = None,
    as_of: datetime | None = None,
    observed_at: datetime | None = None,
    policy_sha256: str | None = None,
    model_version: str | None = None,
    data: Any = None,
) -> OptionsEnvelope:
    configuration = _configuration()
    return OptionsEnvelope(
        available=available,
        reason=reason,
        generated_at=datetime.now(timezone.utc),
        as_of=as_of,
        observed_at=observed_at,
        policy_sha256=policy_sha256 or configuration.policy_sha256,
        model_version=model_version,
        data=data,
    )


def _schema_available(cursor) -> bool:
    cursor.execute("SELECT to_regclass('public.option_ingestion_runs') IS NOT NULL AS ready")
    row = cursor.fetchone()
    return bool(row and row["ready"])


def _strategy_schema_available(cursor) -> bool:
    cursor.execute(
        "SELECT to_regclass('public.option_strategy_candidates') IS NOT NULL AS ready"
    )
    row = cursor.fetchone()
    return bool(row and row["ready"])


def _event_window_state(
    event_type: str,
    coverage: list[dict[str, Any]],
    events: list[dict[str, Any]],
    *,
    not_applicable: bool = False,
) -> str:
    if not_applicable:
        return "NOT_APPLICABLE"
    if not any(row["event_type"] == event_type for row in coverage):
        return "UNAVAILABLE"
    if any(
        row["event_type"] == event_type and row["status"] != "CANCELED"
        for row in events
    ):
        return "BLOCKED"
    return "CLEAR"


def _event_asset_type(
    symbol: str,
    observed_at: datetime,
    fallback_etfs: tuple[str, ...],
) -> str:
    reference = EquityReferenceRepository().get_security_as_of(
        symbol,
        DecisionWatermark(observed_at, observed_at),
    )
    if reference is not None and reference.security_type:
        return (
            "ETF"
            if not is_earnings_applicable_security_type(reference.security_type)
            else "STOCK"
        )
    return "ETF" if symbol in fallback_etfs else "STOCK"










@router.post("/eligible-chain/query", response_model=OptionsEnvelope)
def option_eligible_chain(request: EligibleChainQuery) -> OptionsEnvelope:
    configuration = _configuration()
    requested = [request.underlyer] if request.underlyer else list(configuration.settings.underlyers)
    if any(ticker not in configuration.settings.underlyers for ticker in requested):
        return _envelope(available=False, reason="UNDERLYING_NOT_CONFIGURED", data={"rows": [], "total": 0, "coverage": []})
    where_sql, filter_params = contract_filter_sql(request)
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(available=False, reason="MARKET_DATA_SCHEMA_UNAVAILABLE", data={"rows": [], "total": 0, "coverage": []})
        cursor.execute(
            f"""
            WITH latest AS MATERIALIZED (
                SELECT DISTINCT ON (ingestion.underlying)
                    ingestion.underlying, ingestion.batch_id, analysis.matrix_id,
                    analysis.market_time, analysis.observed_time,
                    ingestion.received_row_count, ingestion.retained_row_count
                FROM option_ingestion_runs AS ingestion
                JOIN option_analysis_runs AS analysis USING (batch_id)
                WHERE ingestion.underlying = ANY(%s::text[])
                  AND ingestion.configuration_sha256 = %s
                  AND ingestion.policy_sha256 = %s AND analysis.policy_sha256 = %s
                  AND ingestion.status = 'COMPLETE' AND analysis.status = 'COMPLETE'
                  AND ingestion.completed_at <= NOW() AND analysis.observed_time <= NOW()
                  AND analysis.market_time <= NOW()
                  AND (%s::date IS NULL OR (analysis.market_time AT TIME ZONE 'America/New_York')::date = %s::date)
                ORDER BY ingestion.underlying, analysis.market_time DESC,
                         analysis.observed_time DESC, analysis.matrix_id DESC
            ), eligible AS MATERIALIZED (
                SELECT snapshot.*, latest.matrix_id,
                       ABS(snapshot.local_delta) AS absolute_delta,
                       snapshot.model_mark AS mark,
                       snapshot.day_volume::double precision / NULLIF(snapshot.open_interest, 0) AS volume_open_interest_ratio,
                       CASE snapshot.contract_type WHEN 'CALL' THEN (snapshot.strike - snapshot.spot) / snapshot.spot
                           ELSE (snapshot.spot - snapshot.strike) / snapshot.spot END AS otm_fraction
                FROM latest
                JOIN option_chain_snapshots AS snapshot USING (batch_id)
                WHERE snapshot.model_mark > 0 AND snapshot.spot > 0
                  AND snapshot.iv_converged AND snapshot.local_iv IS NOT NULL
                  AND snapshot.local_delta IS NOT NULL AND snapshot.local_gamma IS NOT NULL
                  AND snapshot.local_theta_per_day IS NOT NULL
                  AND snapshot.local_vega_per_vol_point IS NOT NULL
                  AND snapshot.local_rho_per_rate_point IS NOT NULL
                  AND snapshot.market_data_time <= latest.market_time
                  AND snapshot.first_observed_at <= latest.observed_time
                  AND COALESCE(snapshot.revised_observed_at, snapshot.first_observed_at) <= latest.observed_time
                  AND snapshot.expiration_cutoff > latest.market_time
            ), filtered AS MATERIALIZED (
                SELECT * FROM eligible WHERE {where_sql}
            ), page AS (
                SELECT * FROM filtered
                ORDER BY {request.sort} {'DESC' if request.descending else 'ASC'} NULLS LAST,
                         underlying, expiration_date, strike, contract_type, contract_id
                LIMIT %s OFFSET %s
            )
            SELECT (SELECT COUNT(*) FROM filtered) AS total,
                   COALESCE((SELECT jsonb_agg(to_jsonb(page) ORDER BY
                       {request.sort} {'DESC' if request.descending else 'ASC'} NULLS LAST,
                       underlying, expiration_date, strike, contract_type, contract_id
                   ) FROM page), '[]'::jsonb) AS rows,
                   COALESCE((SELECT jsonb_agg(jsonb_build_object(
                       'underlying', latest.underlying, 'batch_id', latest.batch_id,
                       'matrix_id', latest.matrix_id, 'market_time', latest.market_time,
                       'observed_time', latest.observed_time, 'received', latest.received_row_count,
                       'retained', latest.retained_row_count,
                       'eligible', (SELECT COUNT(*) FROM eligible WHERE eligible.batch_id = latest.batch_id)
                   ) ORDER BY latest.underlying) FROM latest), '[]'::jsonb) AS coverage
            """,
            (requested, configuration.configuration_sha256, configuration.policy_sha256,
             configuration.policy_sha256, request.session_date, request.session_date,
             *filter_params, request.limit, request.offset),
        )
        result = dict(cursor.fetchone())
    coverage = result["coverage"]
    covered = {row["underlying"] for row in coverage}
    missing = [ticker for ticker in requested if ticker not in covered]
    return _envelope(
        available=bool(coverage), reason=None if coverage else "NO_ACTIVE_POLICY_ELIGIBLE_CHAIN",
        as_of=max((datetime.fromisoformat(row["market_time"]) for row in coverage), default=None),
        observed_at=max((datetime.fromisoformat(row["observed_time"]) for row in coverage), default=None),
        data={
            **result, "requested_underlyers": requested, "missing_underlyers": missing,
            "coverage_status": "MISSING" if not coverage else "PARTIAL" if missing else "COVERED",
            "selection_basis": "LATEST_COMPLETE_MATRIX_PER_UNDERLYING",
            "serving_mode": "CURRENT_POLICY", "session_date": request.session_date,
            "limit": request.limit, "offset": request.offset,
            "filters": [item.model_dump() for item in request.filters],
            "quote_liquidity": "NOT_AVAILABLE", "execution_mode": "READ_ONLY_RESEARCH",
        },
    )






@router.get("/flow", response_model=OptionsEnvelope)
def option_flow(
    underlyer: str | None = None,
    session_date: date | None = None,
) -> OptionsEnvelope:
    configuration = _configuration()
    requested = underlyer.strip().upper() if underlyer else "SPY"
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(
                available=False,
                reason="MIGRATION_015_NOT_APPLIED",
                data={"underlyers": [], "selected": requested},
            )
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (run.underlying)
                    run.underlying, run.asset_type, run.batch_id,
                    run.scheduled_cycle, run.market_data_time,
                    run.first_observed_at, run.received_row_count,
                    run.retained_row_count, analysis.matrix_id,
                    analysis.model_version, analysis.policy_sha256,
                    run.configuration_sha256,
                    (
                        analysis.policy_sha256 = %s
                        AND run.configuration_sha256 = %s
                    ) AS current_policy
                FROM option_ingestion_runs AS run
                JOIN option_analysis_runs AS analysis USING (batch_id)
                WHERE run.status = 'COMPLETE'
                  AND analysis.status = 'COMPLETE'
                  AND run.retained_row_count > 0
                                    AND (
                                            %s::date IS NULL
                                            OR (run.market_data_time AT TIME ZONE 'America/New_York')::date
                                                    = %s::date
                                    )
                ORDER BY run.underlying, current_policy DESC,
                         run.scheduled_cycle DESC,
                         run.completed_at DESC, analysis.observed_time DESC
            )
            SELECT latest.underlying, latest.asset_type, latest.batch_id,
                   latest.matrix_id, latest.scheduled_cycle,
                   latest.market_data_time, latest.first_observed_at,
                   latest.received_row_count, latest.retained_row_count,
                   latest.model_version, latest.policy_sha256,
                   latest.configuration_sha256, latest.current_policy,
                   MAX(snapshot.spot) AS spot,
                   COUNT(*) AS contract_count,
                   COUNT(DISTINCT snapshot.expiration_date) AS expiration_count,
                   COUNT(DISTINCT snapshot.strike) AS strike_count,
                   COUNT(*) FILTER (WHERE snapshot.contract_type = 'CALL')
                       AS call_contract_count,
                   COUNT(*) FILTER (WHERE snapshot.contract_type = 'PUT')
                       AS put_contract_count,
                   COALESCE(SUM(snapshot.day_volume) FILTER (
                       WHERE snapshot.contract_type = 'CALL'
                   ), 0) AS call_volume,
                   COALESCE(SUM(snapshot.day_volume) FILTER (
                       WHERE snapshot.contract_type = 'PUT'
                   ), 0) AS put_volume,
                   COALESCE(SUM(snapshot.open_interest) FILTER (
                       WHERE snapshot.contract_type = 'CALL'
                   ), 0) AS call_open_interest,
                   COALESCE(SUM(snapshot.open_interest) FILTER (
                       WHERE snapshot.contract_type = 'PUT'
                   ), 0) AS put_open_interest,
                   COALESCE(SUM(
                       snapshot.day_volume
                       * COALESCE(snapshot.model_mark, snapshot.display_mark)
                       * snapshot.shares_per_contract
                   ) FILTER (WHERE snapshot.contract_type = 'CALL'), 0)
                       AS call_premium_activity,
                   COALESCE(SUM(
                       snapshot.day_volume
                       * COALESCE(snapshot.model_mark, snapshot.display_mark)
                       * snapshot.shares_per_contract
                   ) FILTER (WHERE snapshot.contract_type = 'PUT'), 0)
                       AS put_premium_activity,
                   COUNT(*) FILTER (
                       WHERE snapshot.day_volume IS NOT NULL
                         AND COALESCE(snapshot.model_mark, snapshot.display_mark) IS NOT NULL
                   ) AS premium_activity_contract_count
            FROM latest
            JOIN option_chain_snapshots AS snapshot USING (batch_id)
            GROUP BY latest.underlying, latest.asset_type, latest.batch_id,
                     latest.matrix_id, latest.scheduled_cycle,
                     latest.market_data_time, latest.first_observed_at,
                     latest.received_row_count, latest.retained_row_count,
                     latest.model_version, latest.policy_sha256,
                     latest.configuration_sha256, latest.current_policy
            ORDER BY latest.underlying
            """,
            (
                configuration.policy_sha256,
                configuration.configuration_sha256,
                session_date,
                session_date,
            ),
        )
        summaries = [dict(row) for row in cursor.fetchall()]
        if summaries and requested not in {row["underlying"] for row in summaries}:
            requested = summaries[0]["underlying"]
        selected_summary = next(
            (row for row in summaries if row["underlying"] == requested), None
        )

        oi_by_underlying: dict[str, dict[str, object]] = {}
        cursor.execute(
            """
            WITH ranked_sessions AS (
                SELECT underlying, settlement_session,
                       DENSE_RANK() OVER (
                           PARTITION BY underlying
                           ORDER BY settlement_session DESC
                       ) AS session_rank
                FROM (
                    SELECT DISTINCT underlying, settlement_session
                    FROM option_daily_contract_facts
                    WHERE open_interest IS NOT NULL
                      AND open_interest_observed_session <= COALESCE(
                          %s::date,
                          (NOW() AT TIME ZONE 'America/New_York')::date
                      )
                ) AS sessions
            ), current_rows AS (
                SELECT fact.underlying, fact.contract_id,
                       fact.settlement_session,
                       COALESCE(
                           fact.open_interest_revised_value,
                           fact.open_interest
                       ) AS open_interest
                FROM option_daily_contract_facts AS fact
                JOIN ranked_sessions AS ranked
                  ON ranked.underlying = fact.underlying
                 AND ranked.settlement_session = fact.settlement_session
                 AND ranked.session_rank = 1
                WHERE fact.open_interest IS NOT NULL
            ), prior_rows AS (
                SELECT fact.underlying, fact.contract_id,
                       fact.settlement_session,
                       COALESCE(
                           fact.open_interest_revised_value,
                           fact.open_interest
                       ) AS open_interest
                FROM option_daily_contract_facts AS fact
                JOIN ranked_sessions AS ranked
                  ON ranked.underlying = fact.underlying
                 AND ranked.settlement_session = fact.settlement_session
                 AND ranked.session_rank = 2
                WHERE fact.open_interest IS NOT NULL
            ), paired AS (
                SELECT current.underlying, current.contract_id,
                       current.settlement_session AS settlement_session,
                       prior.settlement_session AS prior_settlement_session,
                       catalog.contract_type,
                       current.open_interest - prior.open_interest AS oi_change
                FROM current_rows AS current
                JOIN prior_rows AS prior
                  ON prior.underlying = current.underlying
                 AND prior.contract_id = current.contract_id
                                JOIN LATERAL (
                                        SELECT version.contract_type
                                        FROM option_contract_catalog_versions AS version
                                        WHERE version.contract_id = current.contract_id
                                            AND version.contract_type IS NOT NULL
                                            AND version.valid_from
                                                    < current.settlement_session + INTERVAL '1 day'
                                            AND (
                                                    version.valid_to IS NULL
                                                    OR version.valid_to >= current.settlement_session
                                            )
                                        ORDER BY version.valid_from DESC,
                                                         version.catalog_version_id DESC
                                        LIMIT 1
                                ) AS catalog ON TRUE
            ), current_counts AS (
                SELECT underlying, COUNT(*) AS current_contract_count
                FROM current_rows
                GROUP BY underlying
            )
            SELECT paired.underlying,
                   MAX(paired.settlement_session) AS settlement_session,
                   MAX(paired.prior_settlement_session) AS prior_settlement_session,
                   COUNT(*) AS matched_contract_count,
                   current_counts.current_contract_count,
                   COALESCE(SUM(paired.oi_change) FILTER (
                       WHERE paired.contract_type = 'CALL'
                   ), 0) AS call_open_interest_change,
                   COALESCE(SUM(paired.oi_change) FILTER (
                       WHERE paired.contract_type = 'PUT'
                   ), 0) AS put_open_interest_change
            FROM paired
            JOIN current_counts USING (underlying)
            GROUP BY paired.underlying, current_counts.current_contract_count
            ORDER BY paired.underlying
            """,
            (session_date,),
        )
        for row in cursor.fetchall():
            item = dict(row)
            for field in (
                "matched_contract_count",
                "current_contract_count",
                "call_open_interest_change",
                "put_open_interest_change",
            ):
                item[field] = int(item[field] or 0)
            current_count = item["current_contract_count"] or 0
            item["matched_coverage_fraction"] = (
                item["matched_contract_count"] / current_count
                if current_count
                else 0.0
            )
            item["total_open_interest_change"] = (
                item["call_open_interest_change"]
                + item["put_open_interest_change"]
            )
            oi_by_underlying[item["underlying"]] = item

        for summary in summaries:
            for field in (
                "call_volume",
                "put_volume",
                "call_open_interest",
                "put_open_interest",
            ):
                summary[field] = int(summary[field] or 0)
            total_volume = summary["call_volume"] + summary["put_volume"]
            total_oi = summary["call_open_interest"] + summary["put_open_interest"]
            summary["total_volume"] = total_volume
            summary["total_open_interest"] = total_oi
            summary["total_premium_activity"] = (
                summary["call_premium_activity"]
                + summary["put_premium_activity"]
            )
            summary["put_call_volume_ratio"] = (
                summary["put_volume"] / summary["call_volume"]
                if summary["call_volume"]
                else None
            )
            summary["put_call_open_interest_ratio"] = (
                summary["put_open_interest"] / summary["call_open_interest"]
                if summary["call_open_interest"]
                else None
            )
            summary["retention_fraction"] = (
                summary["retained_row_count"] / summary["received_row_count"]
                if summary["received_row_count"]
                else 0.0
            )
            summary["open_interest_change"] = oi_by_underlying.get(
                summary["underlying"]
            )

        expirations: list[dict[str, object]] = []
        strikes: list[dict[str, object]] = []
        top_contracts: list[dict[str, object]] = []
        if selected_summary is not None:
            batch_id = selected_summary["batch_id"]
            cursor.execute(
                """
                SELECT expiration_date, MIN(calendar_dte) AS calendar_dte,
                       COUNT(*) AS contract_count,
                       COALESCE(SUM(day_volume) FILTER (
                           WHERE contract_type = 'CALL'
                       ), 0) AS call_volume,
                       COALESCE(SUM(day_volume) FILTER (
                           WHERE contract_type = 'PUT'
                       ), 0) AS put_volume,
                       COALESCE(SUM(open_interest) FILTER (
                           WHERE contract_type = 'CALL'
                       ), 0) AS call_open_interest,
                       COALESCE(SUM(open_interest) FILTER (
                           WHERE contract_type = 'PUT'
                       ), 0) AS put_open_interest,
                       COALESCE(SUM(
                           day_volume * COALESCE(model_mark, display_mark)
                           * shares_per_contract
                       ) FILTER (WHERE contract_type = 'CALL'), 0)
                           AS call_premium_activity,
                       COALESCE(SUM(
                           day_volume * COALESCE(model_mark, display_mark)
                           * shares_per_contract
                       ) FILTER (WHERE contract_type = 'PUT'), 0)
                           AS put_premium_activity
                FROM option_chain_snapshots
                WHERE batch_id = %s
                GROUP BY expiration_date
                ORDER BY expiration_date
                """,
                (batch_id,),
            )
            expirations = [dict(row) for row in cursor.fetchall()]
            for expiration in expirations:
                for field in (
                    "call_volume",
                    "put_volume",
                    "call_open_interest",
                    "put_open_interest",
                ):
                    expiration[field] = int(expiration[field] or 0)
            cursor.execute(
                """
                SELECT expiration_date, strike, COUNT(*) AS contract_count,
                       COALESCE(SUM(day_volume) FILTER (
                           WHERE contract_type = 'CALL'
                       ), 0) AS call_volume,
                       COALESCE(SUM(day_volume) FILTER (
                           WHERE contract_type = 'PUT'
                       ), 0) AS put_volume,
                       COALESCE(SUM(open_interest) FILTER (
                           WHERE contract_type = 'CALL'
                       ), 0) AS call_open_interest,
                       COALESCE(SUM(open_interest) FILTER (
                           WHERE contract_type = 'PUT'
                       ), 0) AS put_open_interest
                FROM option_chain_snapshots
                WHERE batch_id = %s
                GROUP BY expiration_date, strike
                ORDER BY expiration_date, strike
                """,
                (batch_id,),
            )
            strikes = [dict(row) for row in cursor.fetchall()]
            for strike in strikes:
                for field in (
                    "call_volume",
                    "put_volume",
                    "call_open_interest",
                    "put_open_interest",
                ):
                    strike[field] = int(strike[field] or 0)
            cursor.execute(
                """
                SELECT contract_id, contract_ticker, contract_type,
                       expiration_date, calendar_dte, strike, spot,
                       day_volume, open_interest, model_mark, display_mark,
                       local_iv, local_delta,
                       CASE
                           WHEN open_interest IS NOT NULL
                           THEN day_volume::DOUBLE PRECISION
                                / GREATEST(open_interest, 1)
                           ELSE NULL
                       END AS volume_open_interest_ratio,
                       day_volume * COALESCE(model_mark, display_mark)
                           * shares_per_contract AS premium_activity,
                       ABS(strike / spot - 1) AS moneyness_fraction
                FROM option_chain_snapshots
                WHERE batch_id = %s
                  AND day_volume IS NOT NULL
                ORDER BY premium_activity DESC NULLS LAST,
                         day_volume DESC, contract_id
                LIMIT 24
                """,
                (batch_id,),
            )
            top_contracts = [dict(row) for row in cursor.fetchall()]

    newest_market = max(
        (row["market_data_time"] for row in summaries if row["market_data_time"]),
        default=None,
    )
    newest_observed = max(
        (row["first_observed_at"] for row in summaries if row["first_observed_at"]),
        default=None,
    )
    return _envelope(
        available=bool(summaries),
        reason=None if summaries else "NO_COMPLETE_MATRIX",
        as_of=newest_market,
        observed_at=newest_observed,
        model_version=(selected_summary or {}).get("model_version"),
        data={
            "serving_mode": (
                "CURRENT_POLICY"
                if selected_summary and selected_summary["current_policy"]
                else "HISTORICAL_PREVIOUS_POLICY"
            ),
            "active_policy_sha256": configuration.policy_sha256,
            "active_configuration_sha256": configuration.configuration_sha256,
            "underlyers": summaries,
            "selected": requested,
            "session_date": (
                session_date.isoformat()
                if session_date is not None
                else (
                    OptionExchangeCalendar()
                    .session_for_market_time(newest_market)
                    .isoformat()
                    if newest_market is not None
                    else None
                )
            ),
            "selected_summary": selected_summary,
            "expirations": expirations,
            "strikes": strikes,
            "top_contracts": top_contracts,
            "directional_flow_available": False,
            "quote_liquidity": "NOT_AVAILABLE",
            "trade_tape_scope": "EXCLUDED_FROM_TOTALS_WATCHLIST_BIASED",
            "definitions": {
                "volume": (
                    "Cumulative provider day volume summed across retained "
                    "standard contracts in the latest complete matrix."
                ),
                "open_interest": (
                    "Latest provider open interest, representing the prior "
                    "completed settlement."
                ),
                "premium_activity": (
                    "Estimated activity = day volume x latest aligned mark x "
                    "contract multiplier. It is not transacted premium or net flow."
                ),
                "open_interest_change": (
                    "Change across contracts present in both latest captured "
                    "settlements. It identifies positioning change, not trade direction."
                ),
                "coverage": (
                    "Totals cover the configured DTE, strike corridor, standard-contract "
                    "and liquidity-retained matrix rather than the provider's full chain."
                ),
            },
        },
    )






@router.get("/alerts/datasets", response_model=OptionsEnvelope)
def option_detector_datasets() -> OptionsEnvelope:
    from zoneinfo import ZoneInfo
    from options.repositories.alert_review_sources import OptionAlertReviewSourceRepository, configured_detector_launch

    now = datetime.now(timezone.utc)
    try:
        data = OptionAlertReviewSourceRepository().dataset_index(as_of=now)
        launch = configured_detector_launch()
        configured = launch.dataset_id if launch else None
        configured_session = launch.effective_from.astimezone(ZoneInfo("America/New_York")).date().isoformat() if launch else None
        launch_effective = bool(launch and launch.effective_from <= now)
        retained_session = data["sessions"][-1] if data["sessions"] else None
        retained_dataset = data["session_datasets"].get(retained_session) if retained_session else None
        current = configured if launch_effective else retained_dataset
        data["current_dataset_id"] = current
        data["datasets"] = sorted(set(data["datasets"]) | ({current} if current else set()))
        data["default_dataset_id"] = current or (data["datasets"][0] if len(data["datasets"]) == 1 else None)
        if launch_effective and current and configured_session:
            data["dataset_sessions"].setdefault(current, [])
            data["dataset_sessions"][current] = sorted(set(data["dataset_sessions"][current]) | {configured_session})
            data["sessions"] = sorted(set(data["sessions"]) | {configured_session})
            data["session_datasets"][configured_session] = current
        data["current_session_date"] = data["sessions"][-1] if data["sessions"] else None
        data["configured_dataset_id"] = configured
        data["configured_effective_from"] = launch.effective_from if launch else None
    except (OSError, ValueError, DatabaseError):
        return _envelope(available=False, reason="DETECTOR_DATASET_INDEX_UNAVAILABLE", data={})
    return _envelope(available=True, as_of=now, data=data)


@router.get("/alerts/schedule", response_model=OptionsEnvelope)
def option_detector_schedule() -> OptionsEnvelope:
    from zoneinfo import ZoneInfo
    from options.repositories.alert_review_sources import OptionAlertReviewSourceRepository, configured_detector_launch, detector_alert_schedule, detector_attempt_status

    now = datetime.now(timezone.utc)
    try:
        launch = configured_detector_launch()
        if launch is None:
            return _envelope(available=False, reason="DETECTOR_LAUNCH_NOT_CONFIGURED", data={})
        completed = OptionAlertReviewSourceRepository().window_completions(dataset_id=launch.dataset_id,
            session_date=now.astimezone(ZoneInfo("America/New_York")).date(), as_of=now)
        data = detector_alert_schedule(as_of=now, effective_from=launch.effective_from,
            settings=OptionWorkerSettings.from_environment(), completed=completed)
        data["dataset_id"] = launch.dataset_id
        data["evaluation_attempts"] = detector_attempt_status(launch=launch, as_of=now, completed=completed)
        if any(row["status"] in ("FAILED", "INCOMPLETE", "UNVERIFIED") for row in data["evaluation_attempts"]["attempts"]):
            data["warning"] = True
    except (OSError, ValueError, DatabaseError):
        return _envelope(available=False, reason="DETECTOR_SCHEDULE_UNAVAILABLE", data={})
    return _envelope(available=True, as_of=now, data=data)


@router.get("/alerts/evaluations", response_model=OptionsEnvelope)
def option_alert_evaluations(
    session_date: date | None = None,
    dataset_id: str | None = Query(default=None, min_length=1, max_length=80),
    detector: Literal["O1", "O2", "O3", "S1", "S2"] | None = None,
    selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"] | None = None,
    underlyer: str | None = None,
    sort_by: Literal["triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"] = "run",
    sort_order: Literal["asc", "desc"] = "asc",
    limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0),
) -> OptionsEnvelope:
    from options.analytics.behavior_review import build_detector_evaluation_review
    from options.repositories.alert_review_sources import configured_detector_dataset

    now = datetime.now(timezone.utc)
    try:
        if dataset_id is None:
            dataset_id = configured_detector_dataset()
        data = build_detector_evaluation_review(as_of=now, session_date=session_date, dataset_id=dataset_id,
            detector=detector, selection_status=selection_status, limit=limit, offset=offset,
            underlyer=underlyer, sort_by=sort_by, sort_order=sort_order)
    except (OSError, ValueError, DatabaseError):
        return _envelope(available=False, reason="DETECTOR_EVALUATION_UNAVAILABLE", data={})
    return _envelope(available=True, as_of=now, data=data)


@router.get("/alerts/detector-runs", response_model=OptionsEnvelope)
def option_detector_alerts(
    dataset_id: str = Query(min_length=1, max_length=80),
    scope: Literal["LATEST", "HISTORY"] = "LATEST", session_date: date | None = None,
    session_rollup: bool = False,
    detector: Literal["O1", "O2", "O3", "S1", "S2"] | None = None,
    underlyer: str | None = None,
    sort_by: Literal["triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"] = "run",
    sort_order: Literal["asc", "desc"] = "asc",
    limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0),
) -> OptionsEnvelope:
    from options.analytics.behavior_review import build_detector_alert_review
    from options.repositories.alert_review_sources import (
        OptionAlertReviewSourceRepository, configured_detector_launch, detector_attempt_status,
    )

    now = datetime.now(timezone.utc)
    try:
        configuration = _configuration()
        source_repository = OptionAlertReviewSourceRepository()
        launch = configured_detector_launch()
        attempts = detector_attempt_status(launch=launch, as_of=now)["attempts"] if launch is not None and launch.dataset_id == dataset_id else ()
        history_dataset_ids = ()
        if session_rollup:
            if scope != "HISTORY" or session_date is None:
                raise ValueError("detector session rollup requires one history session")
            index = source_repository.dataset_index(as_of=now)
            history_dataset_ids = tuple(sorted(dataset for dataset, sessions in index["dataset_sessions"].items()
                if session_date.isoformat() in sessions))
            if dataset_id not in history_dataset_ids:
                history_dataset_ids = (*history_dataset_ids, dataset_id)
        data = build_detector_alert_review(dataset_id=dataset_id, as_of=now, scope=scope,
            session_date=session_date, detector=detector, limit=limit, offset=offset,
            underlyer=underlyer, sort_by=sort_by, sort_order=sort_order,
            source_repository=source_repository, history_dataset_ids=history_dataset_ids,
            valuation_policy=configuration.valuation_policy, attempts=attempts)
    except (OSError, ValueError, DatabaseError):
        return _envelope(available=False, reason="DETECTOR_ALERTS_UNAVAILABLE", data={})
    return _envelope(available=True, as_of=now, data=data)


@router.get("/candidates", response_model=OptionsEnvelope)
def option_candidates(    underlyer: str | None = None,
    persona: Literal["INCOME", "DEFINED_RISK_INCOME", "MOMENTUM", "NEUTRAL_VOL"] | None = None,
    status: Literal["SELECTED", "SUPPRESSED", "REJECTED"] | None = None,
    strategy: str | None = None,
    risk_class: Literal[
        "RESEARCH_CONTEXT",
        "CASH_SECURED",
        "DEFINED_RISK_CREDIT",
        "PREMIUM_AT_RISK_DEBIT",
    ] | None = None,
    expiration: str | None = None,
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    category: Literal["INCOME", "DEFINED_RISK_INCOME", "MOMENTUM", "NEUTRAL_VOL"] | None = None,
    session_date: date | None = None,
    structured_only: bool = False,
    minimum_dte: int | None = None,
    maximum_dte: int | None = None,
    maximum_capital: float | None = None,
    sort: Literal["DEFAULT", "CAPITAL_ASC", "DTE_ASC"] = "DEFAULT",
) -> OptionsEnvelope:
    if (
        minimum_dte is not None and not 0 <= minimum_dte <= 365
        or maximum_dte is not None and not 0 <= maximum_dte <= 365
        or minimum_dte is not None and maximum_dte is not None and minimum_dte > maximum_dte
        or maximum_capital is not None and not 0 <= maximum_capital < float("inf")
    ):
        return _envelope(available=False, reason="INVALID_PACKAGE_FILTER", data={"rows": [], "total": 0, "limit": limit, "offset": offset})
    configuration = _configuration()
    clauses = [
        "candidate.policy_sha256 = %s",
        "candidate.market_data_time <= NOW()",
        "candidate.observed_time <= NOW()",
        """NOT EXISTS (
            SELECT 1
            FROM option_candidate_legs AS causal_leg
            WHERE causal_leg.candidate_id = candidate.candidate_id
              AND (
                  causal_leg.source_market_time > NOW() OR
                  causal_leg.quote_time > NOW() OR
                  causal_leg.underlying_quote_time > NOW()
              )
        )""",
    ]
    params: list[object] = [configuration.strategy_policy_sha256]
    if category:
        clauses.append("candidate.structure_type = ANY(%s::text[])")
        params.append([structure.value for structure, categories in STRUCTURE_DISCOVERY_CATEGORIES.items() if category in categories])
    if structured_only:
        clauses.extend([
            "candidate.candidate_kind <> 'RESEARCH_ONLY'",
            """(SELECT COUNT(*) FROM option_candidate_legs AS complete_leg
                WHERE complete_leg.candidate_id = candidate.candidate_id
            ) = CASE candidate.structure_type
                WHEN 'CASH_SECURED_PUT' THEN 1 WHEN 'LONG_CALL' THEN 1 WHEN 'LONG_PUT' THEN 1
                WHEN 'CALL_DEBIT_VERTICAL' THEN 2 WHEN 'PUT_DEBIT_VERTICAL' THEN 2
                WHEN 'CALL_CREDIT_VERTICAL' THEN 2 WHEN 'PUT_CREDIT_VERTICAL' THEN 2
                WHEN 'IRON_CONDOR' THEN 4 WHEN 'CALL_BUTTERFLY' THEN 3 WHEN 'PUT_BUTTERFLY' THEN 3
                ELSE -1 END""",
            """NOT EXISTS (SELECT 1 FROM option_candidate_legs AS invalid_leg
                WHERE invalid_leg.candidate_id = candidate.candidate_id
                  AND (invalid_leg.model_mark IS NULL OR invalid_leg.model_mark <= 0
                       OR invalid_leg.ratio <= 0 OR invalid_leg.multiplier <= 0))""",
        ])
    if minimum_dte is not None or maximum_dte is not None:
        clauses.append("""EXISTS (SELECT 1 FROM option_candidate_legs AS present_leg
            WHERE present_leg.candidate_id = candidate.candidate_id)""")
        clauses.append("""NOT EXISTS (SELECT 1 FROM option_candidate_legs AS dated_leg
            WHERE dated_leg.candidate_id = candidate.candidate_id
              AND (dated_leg.expiration_date - (candidate.market_data_time AT TIME ZONE 'America/New_York')::date)
                  NOT BETWEEN %s AND %s)""")
        params.extend([minimum_dte if minimum_dte is not None else 0, maximum_dte if maximum_dte is not None else 365])
    if maximum_capital is not None:
        clauses.append("candidate.capital_at_risk >= 0 AND candidate.capital_at_risk <= %s")
        params.append(maximum_capital)
    session_clause = "AND (candidate.market_data_time AT TIME ZONE 'America/New_York')::date = %s" if session_date else ""
    completion_clause = "AND analysis.status = 'COMPLETE'" if structured_only else ""
    latest_params = (
        configuration.policy_sha256, configuration.configuration_sha256,
        configuration.strategy_policy_sha256, *((session_date,) if session_date else ()),
    )
    package_order = {
        "DEFAULT": "",
        "CAPITAL_ASC": "candidate.capital_at_risk ASC NULLS LAST,",
        "DTE_ASC": "(candidate.expiration_date - (candidate.market_data_time AT TIME ZONE 'America/New_York')::date) ASC NULLS LAST,",
    }[sort]
    if underlyer:
        clauses.append("candidate.underlying = %s")
        params.append(underlyer.strip().upper())
    if persona:
        clauses.append("%s = ANY(candidate.persona_tags)")
        params.append(persona)
    if strategy:
        clauses.append("candidate.strategy_name = %s")
        params.append(strategy.strip().upper())
    if risk_class:
        clauses.append("candidate.structure_risk_class = %s")
        params.append(risk_class)
    if expiration:
        clauses.append("candidate.expiration_date = %s::date")
        params.append(expiration)
    summary_where_sql = " AND ".join(clauses)
    filtered_clauses = list(clauses)
    filtered_params = list(params)
    if status:
        filtered_clauses.append("candidate.status = %s")
        filtered_params.append(status)
    where_sql = " AND ".join(filtered_clauses)
    with get_db_cursor() as cursor:
        if not _strategy_schema_available(cursor):
            return _envelope(
                available=False,
                reason="MIGRATION_016_NOT_APPLIED",
                policy_sha256=configuration.strategy_policy_sha256,
                data={
                    "rows": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "status_counts": {
                        "selected": 0,
                        "suppressed": 0,
                        "rejected": 0,
                    },
                },
            )
        cursor.execute(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (candidate.underlying)
                    candidate.underlying, candidate.matrix_id,
                    (
                        analysis.policy_sha256 = %s
                        AND ingestion.configuration_sha256 = %s
                    ) AS current_policy
                FROM option_strategy_candidates AS candidate
                JOIN option_analysis_runs AS analysis USING (matrix_id)
                JOIN option_ingestion_runs AS ingestion USING (batch_id)
                WHERE candidate.policy_sha256 = %s
                  AND candidate.market_data_time <= NOW()
                  AND candidate.observed_time <= NOW()
                                    {session_clause}
                  {completion_clause}
                ORDER BY candidate.underlying, current_policy DESC,
                         candidate.market_data_time DESC,
                         candidate.observed_time DESC
            )
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE candidate.status = 'SELECTED') AS selected,
                COUNT(*) FILTER (WHERE candidate.status = 'SUPPRESSED') AS suppressed,
                COUNT(*) FILTER (WHERE candidate.status = 'REJECTED') AS rejected
            FROM option_strategy_candidates AS candidate
            JOIN latest USING (underlying, matrix_id)
            WHERE {summary_where_sql}
            """,
            (
                *latest_params,
                *params,
            ),
        )
        status_counts = dict(cursor.fetchone())
        total = status_counts[status.lower()] if status else status_counts["total"]
        cursor.execute(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (candidate.underlying)
                    candidate.underlying, candidate.matrix_id,
                    (
                        analysis.policy_sha256 = %s
                        AND ingestion.configuration_sha256 = %s
                    ) AS current_policy
                FROM option_strategy_candidates AS candidate
                JOIN option_analysis_runs AS analysis USING (matrix_id)
                JOIN option_ingestion_runs AS ingestion USING (batch_id)
                WHERE candidate.policy_sha256 = %s
                  AND candidate.market_data_time <= NOW()
                  AND candidate.observed_time <= NOW()
                                    {session_clause}
                  {completion_clause}
                ORDER BY candidate.underlying, current_policy DESC,
                         candidate.market_data_time DESC,
                         candidate.observed_time DESC
            )
                 SELECT candidate.*, latest.current_policy, registry.display_name,
                     candidate.expiration_date - (candidate.market_data_time AT TIME ZONE 'America/New_York')::date AS calendar_dte,
                   registry.presentation_metadata,
                     source_contract.contract_id AS source_contract_id,
                     source_contract.contract_ticker AS source_contract_ticker,
                   COALESCE(
                       jsonb_agg(
                           jsonb_build_object(
                               'leg_index', leg.leg_index,
                               'contract_id', leg.contract_id,
                               'contract_ticker', leg.contract_ticker,
                               'side', leg.side,
                               'ratio', leg.ratio,
                               'multiplier', leg.multiplier,
                               'expiration_date', leg.expiration_date,
                               'strike', leg.strike,
                               'contract_type', leg.contract_type,
                               'model_mark', leg.model_mark,
                               'local_iv', leg.local_iv,
                               'local_delta', leg.local_delta,
                               'local_gamma', leg.local_gamma,
                               'local_theta_per_day', leg.local_theta_per_day,
                               'local_vega_per_vol_point', leg.local_vega_per_vol_point,
                               'local_rho_per_rate_point', leg.local_rho_per_rate_point,
                               'spot', leg.spot,
                               'day_volume', entry_snapshot.day_volume,
                               'open_interest', entry_snapshot.open_interest,
                               'source_market_time', leg.source_market_time,
                               'mark_source', leg.mark_source,
                               'valuation_policy_version', leg.valuation_policy_version,
                               'valuation_policy_sha256', leg.valuation_policy_sha256,
                               'quality_flags', leg.quality_flags,
                               'quote_bid', leg.quote_bid,
                               'quote_ask', leg.quote_ask,
                               'quote_midpoint', leg.quote_midpoint,
                               'quote_spread_midpoint', leg.quote_spread_midpoint
                           ) ORDER BY leg.leg_index
                       ) FILTER (WHERE leg.candidate_id IS NOT NULL),
                       '[]'::jsonb
                   ) AS legs
            FROM option_strategy_candidates AS candidate
            JOIN latest USING (underlying, matrix_id)
            JOIN option_strategy_registry AS registry
              ON registry.strategy_name = candidate.strategy_name
             AND registry.strategy_version = candidate.strategy_version
            LEFT JOIN option_candidate_legs AS leg
              ON leg.candidate_id = candidate.candidate_id
                        LEFT JOIN option_chain_snapshots AS entry_snapshot
                            ON entry_snapshot.snapshot_id = leg.snapshot_id
                         AND entry_snapshot.contract_id = leg.contract_id
                         AND entry_snapshot.first_observed_at <= candidate.observed_time
                         AND entry_snapshot.market_data_time <= candidate.market_data_time
                        LEFT JOIN option_contract_catalog AS source_contract
                            ON source_contract.contract_id =
                                 (candidate.rank_components->>'contract_id')::BIGINT
            WHERE {where_sql}
            GROUP BY candidate.candidate_id, latest.current_policy,
                     registry.strategy_name, registry.strategy_version,
                     source_contract.contract_id
            ORDER BY
                CASE candidate.status
                    WHEN 'SELECTED' THEN 0 WHEN 'SUPPRESSED' THEN 1 ELSE 2
                END,
                {package_order}
                candidate.strategy_archetype,
                candidate.underlying,
                candidate.candidate_rank,
                candidate.candidate_id
            LIMIT %s OFFSET %s
            """,
            (
                *latest_params,
                *filtered_params,
                limit,
                offset,
            ),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    serving_mode = (
        "CURRENT_POLICY"
        if rows and all(row["current_policy"] for row in rows)
        else "HISTORICAL_PREVIOUS_POLICY"
    )
    for row in rows:
        row.pop("current_policy", None)
    newest_market = max((row["market_data_time"] for row in rows), default=None)
    newest_observed = max((row["observed_time"] for row in rows), default=None)
    return _envelope(
        available=bool(rows),
        reason=None if rows else "NO_STRATEGY_RESULTS",
        as_of=newest_market,
        observed_at=newest_observed,
        policy_sha256=configuration.strategy_policy_sha256,
        model_version=rows[0]["model_version"] if rows else None,
        data={
            "serving_mode": serving_mode,
            "active_policy_sha256": configuration.policy_sha256,
            "title": "Weekly Research Candidates",
            "selection_basis": "LATEST_COMPLETE_MATRIX_PER_UNDERLYING" if structured_only else "LATEST_MATRIX_PER_UNDERLYING",
            "session_date": session_date.isoformat() if session_date else None,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
            "status_counts": {
                "selected": status_counts["selected"],
                "suppressed": status_counts["suppressed"],
                "rejected": status_counts["rejected"],
            },
            "quote_liquidity": "NOT_AVAILABLE",
            "execution_mode": "READ_ONLY_RESEARCH",
        },
    )




@router.get("/discovery-catalog")
def option_discovery_catalog() -> dict[str, object]:
    return {**build_discovery_catalog(), "contract_filters": CONTRACT_FILTERS}


@router.get("/alert-qualification/policy")
def option_alert_qualification_policy() -> dict[str, object]:
    return {**alert_qualification_policy(), "policy_sha256": qualification_policy_sha256()}


def _retained_alert_detail(candidate_id: UUID, holding_until: datetime | None = None):
    original = option_candidate_detail(candidate_id)
    if not original.available:
        return original, None
    detail = dict(original.data)
    candidate = detail["candidate"]
    with get_db_cursor() as cursor:
        cursor.execute(
            """
                 SELECT snapshot.*, ingestion.asset_type AS underlying_asset_type,
                     ingestion.first_observed_at AS underlying_asset_type_observed_at
            FROM option_chain_snapshots AS snapshot
                 JOIN option_ingestion_runs AS ingestion USING (batch_id)
            WHERE snapshot.snapshot_id IN (
                SELECT snapshot_id FROM option_candidate_legs WHERE candidate_id = %s
            ) OR (
                snapshot.contract_id = %s
                AND snapshot.batch_id = (SELECT batch_id FROM option_analysis_runs WHERE matrix_id = %s)
            )
            ORDER BY snapshot.contract_id, snapshot.snapshot_id
            """,
            (str(candidate_id), candidate.get("source_contract_id"), candidate["matrix_id"]),
        )
        detail["source_snapshots"] = [dict(row) for row in cursor.fetchall()]
        if detail["source_snapshots"]:
            market_session = candidate["market_data_time"].astimezone(ZoneInfo("America/New_York")).date()
            settlement = OptionExchangeCalendar().previous_session(market_session)
            cursor.execute(
                """
                SELECT contract_id, underlying, settlement_session, open_interest,
                       open_interest_source, open_interest_observed_at, open_interest_observed_session,
                       open_interest_revision_count, open_interest_revised_value,
                       open_interest_revised_observed_at
                FROM option_daily_contract_facts
                WHERE underlying = %s AND contract_id = ANY(%s)
                  AND settlement_session = %s AND open_interest_observed_at <= %s
                ORDER BY contract_id
                """,
                (candidate["underlying"], [row["contract_id"] for row in detail["source_snapshots"]], settlement, candidate["observed_time"]),
            )
            detail["open_interest_evidence"] = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT gamma_profile_id, matrix_id, underlying, scope,
                       market_data_time, first_observed_at, gamma_policy_version,
                       gamma_policy_sha256, shares_per_contract, dealer_convention,
                       regime_at_spot, contributing_contract_count, eligible_contract_count,
                       coverage_fraction, quality_reasons
                FROM option_gamma_profiles
                WHERE matrix_id = %s AND underlying = %s AND first_observed_at <= %s
                ORDER BY scope, gamma_policy_sha256
                """,
                (candidate["matrix_id"], candidate["underlying"], candidate["observed_time"]),
            )
            detail["gamma_evidence"] = [dict(row) for row in cursor.fetchall()]
        if holding_until is not None:
            sources = sorted({row["source"] for row in detail.get("event_coverage_evidence", ()) if row.get("source")})
            if sources:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (source, source_key) *
                        FROM option_event_calendar_coverage
                        WHERE source = ANY(%s) AND first_observed_at <= %s
                        ORDER BY source, source_key, first_observed_at DESC, coverage_id
                    )
                    SELECT * FROM latest WHERE affected_underlying = %s OR affected_underlying IS NULL
                    ORDER BY event_type, window_start, coverage_id
                    """,
                    (sources, candidate["observed_time"], candidate["underlying"]),
                )
                detail["holding_event_coverage"] = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (source, source_key) *
                        FROM option_market_events
                        WHERE source = ANY(%s) AND first_observed_at <= %s
                          AND COALESCE(revised_observed_at, first_observed_at) <= %s
                        ORDER BY source, source_key, COALESCE(revised_observed_at, first_observed_at) DESC, market_event_id
                    )
                    SELECT * FROM latest WHERE affected_underlying = %s OR affected_underlying IS NULL
                    ORDER BY event_type, scheduled_time, market_event_id
                    """,
                    (sources, candidate["observed_time"], candidate["observed_time"], candidate["underlying"]),
                )
                detail["holding_event_evidence"] = [dict(row) for row in cursor.fetchall()]
    return original, detail


@router.get("/alert-qualification/{candidate_id}", response_model=OptionsEnvelope)
def option_alert_qualification(candidate_id: UUID, holding_until: datetime | None = None) -> OptionsEnvelope:
    decision_at = datetime.now(timezone.utc)
    if holding_until is not None and (holding_until.utcoffset() is None or holding_until <= decision_at or holding_until - decision_at > timedelta(days=366)):
        raise HTTPException(status_code=422, detail="holding_until must be timezone-aware, after assessment time and within 366 days")
    original, detail = _retained_alert_detail(candidate_id, holding_until)
    if detail is None:
        return original
    candidate = detail["candidate"]
    evidence = retained_alert_evidence(detail, decision_at, **({"holding_until": holding_until} if holding_until is not None else {}))
    assessment = qualify_option_alert(candidate["strategy_name"], StructureType(candidate["structure_type"]), evidence, decision_at)
    return _envelope(
        available=True, as_of=original.as_of, observed_at=original.observed_at,
        policy_sha256=original.policy_sha256, model_version=original.model_version,
        data={"candidate_id": str(candidate_id), "assessment": assessment,
              "evidence_adapter_version": RETAINED_EVIDENCE_VERSION,
              "event_horizon": retained_event_horizon(detail, decision_at, holding_until) if holding_until is not None else None,
              "source_basis": "RETAINED_CANDIDATE_ASSESSED_NOW", "persisted": False,
              "original_execution_eligibility": candidate.get("execution_eligibility")},
    )


@router.post("/alerts/preview/{candidate_id}", response_model=OptionsEnvelope)
def option_alert_publication_preview(candidate_id: UUID, request: OptionAlertPreviewRequest) -> OptionsEnvelope:
    decision_at = datetime.now(timezone.utc)
    holding_until = request.exit_deadline
    if holding_until is not None and not decision_at < holding_until <= decision_at + timedelta(days=366):
        holding_until = None
    original, detail = _retained_alert_detail(candidate_id, holding_until)
    if detail is None:
        return original
    management = AlertManagementPolicy(**request.management_policy.model_dump()) if request.management_policy else None
    preview = preview_indicative_alert_plan(
        detail, decision_at=decision_at, entry_deadline=request.entry_deadline,
        exit_deadline=request.exit_deadline, entry_limit=request.entry_limit, management_policy=management,
    )
    return _envelope(
        available=True, as_of=original.as_of, observed_at=original.observed_at,
        policy_sha256=original.policy_sha256, model_version=original.model_version, data=preview,
    )


def _assess_stock_behavior_for_dry_run(
    source: dict[str, Any], candidate: dict[str, Any], decision_at: datetime,
    target_holding_until: datetime | None, entry_deadline: datetime | None = None,
) -> dict[str, Any]:
    arguments = dict(
        candidate_id=UUID(str(source["candidate_id"])),
        matrix_id=UUID(str(source["matrix_id"])),
        underlyer=source["underlying"],
        strategy_name=source["strategy_name"],
        structure_type=StructureType(source["structure_type"]),
        directional_thesis=(candidate.get("primary_evidence") or {}).get(
            "directional_thesis"
        ),
        candidate_identity_sha256=source.get("candidate_identity_sha256"),
        option_strategy_version=source.get("strategy_version"),
        option_strategy_policy_sha256=source.get("strategy_policy_sha256"),
        option_configuration_sha256=source.get("configuration_sha256"),
        option_market_policy_sha256=source.get("market_policy_sha256"),
        option_analysis_policy_sha256=source.get("analysis_policy_sha256"),
        option_market_time=source.get("option_market_time"),
        option_observed_at=source.get("candidate_observed_at"),
        stock_market_cutoff=source["scheduled_cycle"],
        decision_at=decision_at,
        entry_deadline=entry_deadline,
        target_holding_until=target_holding_until,
    )
    scoped = evaluate_option_stock_behavior(None, **arguments)
    if not scoped.applicable:
        return scoped.model_dump(mode="json")
    context = DecisionWatermark(source["scheduled_cycle"], decision_at)
    try:
        security = EquityReferenceRepository().get_security_as_of(
            source["underlying"], context,
        )
        snapshot = (
            EquityEvidenceRepository().get_behavior_as_of(
                security.security_id, context,
                profile=OPTIONS_SWING_PROFILE.name,
                definition_sha256=DEFINITION_SHA256,
                policy_sha256=OPTIONS_SWING_PROFILE.sha256,
            )
            if security is not None else None
        )
    except DatabaseError:
        return evaluate_option_stock_behavior(
            None, unavailable_reason="STOCK_BEHAVIOR_READ_FAILED", **arguments,
        ).model_dump(mode="json")
    except ValueError:
        return evaluate_option_stock_behavior(
            None, unavailable_reason="STOCK_BEHAVIOR_CONTRACT_REJECTED", **arguments,
        ).model_dump(mode="json")
    return evaluate_option_stock_behavior(
        snapshot,
        unavailable_reason=(
            "STOCK_SECURITY_IDENTITY_UNAVAILABLE" if security is None
            else "STOCK_BEHAVIOR_SNAPSHOT_UNAVAILABLE"
        ),
        **arguments,
    ).model_dump(mode="json")


@router.post("/alerts/dry-run", response_model=OptionsEnvelope)
def option_alert_publications_dry_run(request: OptionAlertDryRunRequest) -> OptionsEnvelope:
    decision_at = datetime.now(timezone.utc)
    configuration = _configuration()
    settings = OptionWorkerSettings.from_environment()
    calendar = OptionExchangeCalendar()
    engine = OptionStrategyEngine(
        configuration.strategy_policy, configuration.strategy_policy_sha256,
        configuration.gamma_policy, configuration.gamma_policy_sha256,
    )

    def delayed_slot(checked_at):
        return calendar.latest_delayed_slot(
            checked_at, interval=timedelta(seconds=settings.slot_seconds),
            provider_delay=timedelta(seconds=settings.provider_delay_seconds),
            publication_grace=timedelta(seconds=settings.publication_grace_seconds),
        )

    expected_slot = delayed_slot(decision_at)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        if not _strategy_schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_016_NOT_APPLIED",
                             data={"version": DRY_RUN_VERSION, "persisted": False, "publication_permission": False})
        cursor.execute(
            """
                    SELECT candidate.candidate_id, candidate.candidate_identity AS candidate_identity_sha256,
                         candidate.matrix_id, candidate.strategy_name,
                     candidate.structure_type,
                   candidate.strategy_version, candidate.policy_sha256 AS strategy_policy_sha256,
                         candidate.market_data_time AS option_market_time,
                         candidate.observed_time AS candidate_observed_at, ingestion.underlying,
                   ingestion.configuration_sha256, ingestion.policy_sha256 AS market_policy_sha256,
                   analysis.policy_sha256 AS analysis_policy_sha256, ingestion.scheduled_cycle,
                   ingestion.status AS ingestion_status, analysis.status AS analysis_status,
                   ingestion.completed_at AS ingestion_completed_at, analysis.completed_at AS analysis_completed_at
            FROM option_strategy_candidates AS candidate
            JOIN option_analysis_runs AS analysis USING (matrix_id)
            JOIN option_ingestion_runs AS ingestion USING (batch_id)
            WHERE candidate.candidate_id = ANY(%s::uuid[])
            """, ([str(row.candidate_id) for row in request.candidates],),
        )
        sources = {str(row["candidate_id"]): dict(row) for row in cursor.fetchall()}
    rules = {rule.strategy_name: rule for rule in request.strategies}
    rows = []
    for item in request.candidates:
        candidate_id = str(item.candidate_id)
        source = sources.get(candidate_id)
        row = {
            "candidate_id": candidate_id, "status": "BLOCKED", "source": source,
            "preview": None, "stock_behavior_assessment": None, "blockers": [],
        }
        rows.append(row)
        if source is None:
            row["blockers"].append({"code": "CANDIDATE_SOURCE_UNAVAILABLE"})
            continue
        if source["strategy_name"] not in rules:
            row["blockers"].append({"code": "STRATEGY_NOT_ALLOWED"})
        if (
            source["configuration_sha256"] != configuration.configuration_sha256
            or source["market_policy_sha256"] != configuration.policy_sha256
            or source["analysis_policy_sha256"] != configuration.policy_sha256
            or source["strategy_policy_sha256"] != engine.policy_sha256
            or source["strategy_version"] != engine.strategy_version
            or source["underlying"] not in configuration.settings.underlyers
        ):
            row["blockers"].append({"code": "ACTIVE_POLICY_OR_UNIVERSE_MISMATCH"})
        if expected_slot is None or source["scheduled_cycle"] != expected_slot:
            row["blockers"].append({"code": "SOURCE_SLOT_NOT_CURRENT"})
        if source["ingestion_status"] != "COMPLETE" or source["analysis_status"] != "COMPLETE" or any(
            source[field] is None or source[field] > decision_at
            for field in ("candidate_observed_at", "ingestion_completed_at", "analysis_completed_at")
        ):
            row["blockers"].append({"code": "SOURCE_NOT_COMPLETE_AT_ASSESSMENT"})
        if row["blockers"]:
            continue
        holding_until = item.exit_deadline
        if holding_until is not None and not decision_at < holding_until <= decision_at + timedelta(days=366):
            holding_until = None
        _, detail = _retained_alert_detail(item.candidate_id, holding_until)
        if detail is None:
            row["blockers"].append({"code": "CANDIDATE_EVIDENCE_UNAVAILABLE"})
            continue
        candidate = detail["candidate"]
        if str(candidate["matrix_id"]) != str(source["matrix_id"]) or candidate["policy_sha256"] != source["strategy_policy_sha256"]:
            row["blockers"].append({"code": "CANDIDATE_SOURCE_CHANGED"})
            continue
        row["stock_behavior_assessment"] = _assess_stock_behavior_for_dry_run(
            source, candidate, decision_at, holding_until,
            entry_deadline=item.entry_deadline,
        )
        rule = rules[source["strategy_name"]]
        management = AlertManagementPolicy(**rule.management_policy.model_dump()) if rule.management_policy else None
        preview = preview_indicative_alert_plan(detail, decision_at=decision_at,
            entry_deadline=item.entry_deadline, exit_deadline=item.exit_deadline,
            entry_limit=item.entry_limit, management_policy=management)
        row.update(status=preview["status"], preview=preview, blockers=list(preview["blockers"]))
    exposure_keys = dry_run_exposure_keys(rows)
    publication_state = {"available": None, "checked_at": None, "reason": "NO_VALID_PLAN_EXPOSURES", "rows": []}
    if exposure_keys:
        try:
            publication_state = OptionAlertPublicationRepository().exposure_state(exposure_keys)
        except DatabaseError:
            publication_state = {"available": False, "checked_at": None, "reason": "PUBLICATION_STATE_READ_FAILED", "rows": []}
    completed_at = datetime.now(timezone.utc)
    result = finalize_alert_dry_run(rows, assessed_at=decision_at, completed_at=completed_at,
        expected_slot=expected_slot, final_slot=delayed_slot(completed_at), publication_state=publication_state,
        policy={"version": DRY_RUN_VERSION, "policy_version": request.policy_version,
                "configuration_sha256": configuration.configuration_sha256,
                "market_policy_sha256": configuration.policy_sha256,
                "strategy_policy_sha256": engine.policy_sha256, "strategy_version": engine.strategy_version,
                "stock_behavior_gate_policy_version": STOCK_BEHAVIOR_GATE_POLICY.version,
                "stock_behavior_gate_policy_sha256": STOCK_BEHAVIOR_GATE_POLICY.sha256,
                "stock_behavior_effect": "ASSESSMENT_ONLY_NO_SELECTION_OR_PUBLICATION_BLOCK",
                "worker_slot_policy": {"slot_seconds": settings.slot_seconds,
                                       "provider_delay_seconds": settings.provider_delay_seconds,
                                       "publication_grace_seconds": settings.publication_grace_seconds},
                "strategies": [rule.model_dump(mode="json") for rule in sorted(request.strategies, key=lambda rule: rule.strategy_name)]})
    return _envelope(available=True, as_of=expected_slot, data=result)


def _candidate_research_evidence(cursor, candidate: dict[str, Any]) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT to_regclass('public.option_stock_behavior_assessments') IS NOT NULL
                   AS stock_behavior_ready,
               to_regclass('public.option_package_assessments') IS NOT NULL
                                     AND (SELECT COUNT(*) = 2 FROM pg_attribute
                                                WHERE attrelid = to_regclass('public.option_package_assessments')
                                                    AND attname IN ('assessment_policy_version', 'assessment_policy_sha256')
                                                    AND NOT attisdropped)
                   AS package_assessment_ready
        """
    )
    schema = cursor.fetchone()

    def unavailable(reason: str) -> dict[str, Any]:
        return {"availability": "UNAVAILABLE", "reason": reason, "assessment": None}

    def persisted_assessment(row, contract, policy, policy_prefix: str) -> dict[str, Any]:
        try:
            assessment = json.loads(row["payload_text"])
        except (TypeError, ValueError):
            return unavailable("PERSISTED_ASSESSMENT_PAYLOAD_INVALID")
        if (
            not isinstance(assessment, dict)
            or assessment.get("schema_version") != contract.model_fields["schema_version"].default
        ):
            return unavailable("PERSISTED_ASSESSMENT_VERSION_UNSUPPORTED")
        version_field = f"{policy_prefix}_version"
        hash_field = f"{policy_prefix}_sha256"
        if (
            assessment.get(version_field) != policy.version
            or assessment.get(hash_field) != policy.sha256
            or row.get(version_field) != policy.version
            or row.get(hash_field) != policy.sha256
        ):
            return unavailable("PERSISTED_ASSESSMENT_POLICY_UNSUPPORTED")
        try:
            validated = contract.model_validate(assessment)
        except (TypeError, ValueError):
            return unavailable("PERSISTED_ASSESSMENT_PAYLOAD_INVALID")
        if (
            validated.sha256 != row["payload_sha256"]
            or validated.canonical_json() != row["payload_text"]
        ):
            return unavailable("PERSISTED_ASSESSMENT_HASH_MISMATCH")
        if (
            str(validated.candidate_id) != str(candidate["candidate_id"])
            or validated.candidate_identity_sha256 != candidate.get("candidate_identity")
        ):
            return unavailable("PERSISTED_ASSESSMENT_CANDIDATE_MISMATCH")
        if isinstance(validated, StockBehaviorGateAssessment):
            expected = {
                "matrix_id": candidate.get("matrix_id"),
                "underlyer": candidate.get("underlying"),
                "strategy_name": candidate.get("strategy_name"),
                "structure_type": candidate.get("structure_type"),
                "option_strategy_version": candidate.get("strategy_version"),
                "option_strategy_policy_sha256": candidate.get("policy_sha256"),
                "option_market_time": candidate.get("market_data_time"),
                "option_observed_at": candidate.get("observed_time"),
                "entry_deadline": candidate.get("valid_until"),
            }
            stored = {
                "disposition": validated.disposition,
                "decision_at": validated.decision_at,
                "stock_snapshot_id": validated.stock_snapshot_id,
            }
            assessed_at = validated.decision_at
            if validated.stock_snapshot_id is not None and (
                validated.stock_profile != OPTIONS_SWING_PROFILE.name
                or validated.stock_definition_sha256 != DEFINITION_SHA256
                or validated.stock_policy_sha256 != OPTIONS_SWING_PROFILE.sha256
            ):
                return unavailable("PERSISTED_ASSESSMENT_POLICY_UNSUPPORTED")
        else:
            expected = {"option_matrix_id": candidate.get("matrix_id")}
            stored = {
                "assessment_status": validated.status,
                "assessed_at": validated.assessed_at,
                "package_terms_sha256": validated.package_terms_sha256,
                "valuation_policy_sha256": validated.valuation_policy_sha256,
            }
            assessed_at = validated.assessed_at
            if validated.package is not None:
                package = validated.package
                package_expected = {
                    "candidate_id": validated.candidate_id,
                    "candidate_identity_sha256": validated.candidate_identity_sha256,
                    "option_matrix_id": validated.option_matrix_id,
                    "underlyer": candidate.get("underlying"),
                    "strategy_name": candidate.get("strategy_name"),
                    "strategy_version": candidate.get("strategy_version"),
                    "strategy_policy_sha256": candidate.get("policy_sha256"),
                    "structure": candidate.get("structure_type"),
                    "market_time": candidate.get("market_data_time"),
                    "observed_at": candidate.get("observed_time"),
                    "valid_until": candidate.get("valid_until"),
                }
                if any(getattr(package, key) != value for key, value in package_expected.items()) or any(
                    leg.valuation_policy_sha256 != validated.valuation_policy_sha256
                    for leg in package.ordered_legs
                ):
                    return unavailable("PERSISTED_ASSESSMENT_CANDIDATE_MISMATCH")
        if any(getattr(validated, key) != value for key, value in expected.items()):
            return unavailable("PERSISTED_ASSESSMENT_CANDIDATE_MISMATCH")
        if any(row.get(key) != value for key, value in stored.items()):
            return unavailable("PERSISTED_ASSESSMENT_COLUMN_MISMATCH")
        observed_at = candidate.get("observed_time")
        if observed_at is None or not observed_at <= assessed_at <= row["recorded_at"]:
            return unavailable("PERSISTED_ASSESSMENT_CLOCK_MISMATCH")
        return {
            "availability": "AVAILABLE",
            "reason": None,
            "payload_sha256": row["payload_sha256"],
            "recorded_at": row["recorded_at"],
            "assessment": assessment,
        }

    stock_behavior = unavailable("STOCK_BEHAVIOR_ASSESSMENT_NOT_RECORDED")
    if not schema["stock_behavior_ready"]:
        stock_behavior = unavailable("STOCK_BEHAVIOR_ASSESSMENT_SCHEMA_UNAVAILABLE")
    else:
        cursor.execute(
            """
            SELECT payload_text, payload_sha256, disposition, decision_at,
                   recorded_at, detector_policy_version, detector_policy_sha256,
                   stock_snapshot_id
            FROM option_stock_behavior_assessments
            WHERE candidate_id = %s AND recorded_at <= NOW()
                            AND detector_policy_version = %s AND detector_policy_sha256 = %s
            ORDER BY decision_at DESC, recorded_at DESC, assessment_id
            LIMIT 1
            """,
                        (str(candidate["candidate_id"]), STOCK_BEHAVIOR_GATE_POLICY.version,
                         STOCK_BEHAVIOR_GATE_POLICY.sha256),
        )
        row = cursor.fetchone()
        if row:
            stock_behavior = persisted_assessment(
                row, StockBehaviorGateAssessment, STOCK_BEHAVIOR_GATE_POLICY,
                "detector_policy",
            )

    package_assessment = unavailable("PACKAGE_ASSESSMENT_NOT_RECORDED")
    if not schema["package_assessment_ready"]:
        package_assessment = unavailable("PACKAGE_ASSESSMENT_SCHEMA_UNAVAILABLE")
    else:
        cursor.execute(
            """
            SELECT payload_text, payload_sha256, package_terms_sha256,
                   assessment_status, assessed_at, recorded_at,
                     valuation_policy_sha256, assessment_policy_version,
                     assessment_policy_sha256
            FROM option_package_assessments
            WHERE candidate_id = %s AND recorded_at <= NOW()
                AND assessment_policy_version = %s AND assessment_policy_sha256 = %s
            ORDER BY assessed_at DESC, recorded_at DESC, assessment_id
            LIMIT 1
            """,
                 (str(candidate["candidate_id"]), PACKAGE_ASSESSMENT_POLICY.version,
                  PACKAGE_ASSESSMENT_POLICY.sha256),
        )
        row = cursor.fetchone()
        if row:
            package_assessment = persisted_assessment(
                row, OptionPackageAssessment, PACKAGE_ASSESSMENT_POLICY,
                "assessment_policy",
            )

    structure = StructureType(candidate["structure_type"])
    return {
        "version": CANDIDATE_RESEARCH_EVIDENCE_VERSION,
        "detector": {
            "id": candidate["strategy_name"],
            "version": candidate["strategy_version"],
            "display_name": candidate["display_name"],
            "output_kind": (
                "OBSERVATION" if candidate["candidate_kind"] == "RESEARCH_ONLY"
                else "STRUCTURED_PACKAGE"
            ),
        },
        "category_ids": list(STRUCTURE_DISCOVERY_CATEGORIES[structure]),
        "structure": candidate["structure_type"],
        "stock_behavior": stock_behavior,
        "package_assessment": package_assessment,
        "probability": {
            "status": "UNAVAILABLE",
            "value": None,
            "reason": "NO_QUALIFIED_CALIBRATION_REPORT",
            "target": None,
            "outcome_basis": None,
            "report_sha256": None,
        },
        "source_basis": "PERSISTED_EVIDENCE_ONLY",
        "calculation_performed": False,
        "publication_permission": False,
        "execution_permission": False,
    }


@router.get("/candidates/{candidate_id}", response_model=OptionsEnvelope)
def option_candidate_detail(candidate_id: UUID) -> OptionsEnvelope:
    configuration = _configuration()
    with get_db_cursor() as cursor:
        if not _strategy_schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_016_NOT_APPLIED", data={})
        cursor.execute(
            """
            SELECT candidate.*, registry.display_name,
                   registry.presentation_metadata,
                     source_contract.contract_id AS source_contract_id,
                     source_contract.contract_ticker AS source_contract_ticker,
                   context.status AS context_status,
                   context.trend_state,
                   context.earnings_blackout_state,
                   context.fed_blackout_state,
                   context.quote_spread_state,
                   context.reason_codes AS context_reason_codes,
                   evidence.normalized_legs,
                   evidence.context AS decision_context,
                   evidence.rank_components AS evidence_rank_components,
                   evidence.trigger_values,
                   evidence.quality_flags AS evidence_quality_flags,
                   signal.event_id AS signal_id,
                   signal.status AS signal_status,
                   signal.blocked_reasons AS signal_blocked_reasons
            FROM option_strategy_candidates AS candidate
            JOIN option_strategy_registry AS registry
              ON registry.strategy_name = candidate.strategy_name
             AND registry.strategy_version = candidate.strategy_version
            LEFT JOIN option_context_snapshots AS context
              ON context.context_snapshot_id = candidate.context_snapshot_id
            JOIN option_decision_evidence AS evidence
              ON evidence.evidence_id = candidate.decision_evidence_id
                        LEFT JOIN option_signal_occurrences AS signal_occurrence
                            ON signal_occurrence.source_candidate_id = candidate.candidate_id
                        LEFT JOIN option_signal_events AS signal
                            ON signal.event_id = signal_occurrence.event_id
                        LEFT JOIN option_contract_catalog AS source_contract
                            ON source_contract.contract_id =
                                 (candidate.rank_components->>'contract_id')::BIGINT
                        WHERE candidate.candidate_id = %s
                            AND candidate.market_data_time <= NOW()
                            AND candidate.observed_time <= NOW()
                            AND NOT EXISTS (
                                    SELECT 1
                                    FROM option_candidate_legs AS causal_leg
                                    WHERE causal_leg.candidate_id = candidate.candidate_id
                                        AND (
                                                causal_leg.source_market_time > NOW() OR
                                                causal_leg.quote_time > NOW() OR
                                                causal_leg.underlying_quote_time > NOW()
                                        )
                            )
            """,
            (str(candidate_id),),
        )
        candidate = cursor.fetchone()
        if not candidate:
            return _envelope(
                available=False,
                reason="CANDIDATE_NOT_FOUND",
                policy_sha256=configuration.strategy_policy_sha256,
                data={},
            )
        cursor.execute(
            """
            SELECT *
            FROM option_candidate_legs
            WHERE candidate_id = %s
            ORDER BY leg_index
            """,
            (str(candidate_id),),
        )
        legs = cursor.fetchall()
        cursor.execute(
            """
            SELECT *
            FROM option_scenario_results
            WHERE candidate_id = %s
            ORDER BY time_fraction_remaining DESC,
                     spot_shock_fraction, iv_shock_fraction
            """,
            (str(candidate_id),),
        )
        scenarios = cursor.fetchall()
        cursor.execute(
            """
            SELECT ledger_version, gate_name, verdict, blocking,
                   reason_codes, evidence, evaluated_at
            FROM option_candidate_execution_gates
            WHERE candidate_id = %s
            ORDER BY gate_name
            """,
            (str(candidate_id),),
        )
        execution_gates = cursor.fetchall()
        cursor.execute(
            """
            SELECT event.market_event_id, event.event_type,
                   event.affected_underlying, event.scheduled_time,
                   event.source, event.source_key, event.announcement_time,
                   event.source_observed_at, event.first_observed_at,
                   event.confidence, event.status, event.payload_sha256
            FROM option_context_market_event_evidence AS link
            JOIN option_market_events AS event USING (market_event_id)
            WHERE link.context_snapshot_id = %s
            ORDER BY event.event_type, event.scheduled_time,
                     event.source, event.source_key, event.market_event_id
            """,
            (candidate["context_snapshot_id"],),
        )
        market_event_evidence = cursor.fetchall()
        cursor.execute(
            """
            SELECT coverage.coverage_id, coverage.event_type,
                   coverage.affected_underlying, coverage.window_start,
                   coverage.window_end, coverage.source, coverage.source_key,
                   coverage.source_observed_at, coverage.first_observed_at,
                   coverage.payload_sha256
            FROM option_context_event_coverage_evidence AS link
            JOIN option_event_calendar_coverage AS coverage USING (coverage_id)
            WHERE link.context_snapshot_id = %s
            ORDER BY coverage.event_type, coverage.affected_underlying,
                     coverage.window_start, coverage.source, coverage.source_key,
                     coverage.coverage_id
            """,
            (candidate["context_snapshot_id"],),
        )
        event_coverage_evidence = cursor.fetchall()
        research_evidence = _candidate_research_evidence(cursor, candidate)
    return _envelope(
        available=True,
        as_of=candidate["market_data_time"],
        observed_at=candidate["observed_time"],
        policy_sha256=candidate["policy_sha256"],
        model_version=candidate["model_version"],
        data={
            "candidate": candidate,
            "legs": legs,
            "scenarios": scenarios,
            "execution_gates": execution_gates,
            "market_event_evidence": market_event_evidence,
            "event_coverage_evidence": event_coverage_evidence,
            "research_evidence": research_evidence,
            "quote_liquidity": "NOT_AVAILABLE",
            "execution_mode": "READ_ONLY_RESEARCH",
        },
    )








@router.get("/calendar/{underlyer}", response_model=OptionsEnvelope)
def option_event_calendar_reference(
    underlyer: str,
    days_forward: int = Query(default=30, ge=3, le=120),
) -> OptionsEnvelope:
    configuration = _configuration()
    symbol = underlyer.strip().upper()
    source = configuration.settings.event_calendar_provider
    maximum_age = timedelta(
        seconds=configuration.settings.event_calendar_max_age_seconds
    )
    now = datetime.now(timezone.utc)
    decision_start = now - timedelta(days=1)
    decision_end = now + timedelta(hours=72)
    upcoming_end = now + timedelta(days=days_forward)
    asset_type = _event_asset_type(
        symbol,
        now,
        configuration.settings.fixed_etf_underlyers,
    )
    empty = {
        "underlyer": symbol,
        "asset_type": asset_type,
        "configured_source": source,
        "decision_window_start": decision_start,
        "decision_window_end": decision_end,
        "earnings_state": "NOT_APPLICABLE" if asset_type == "ETF" else "UNAVAILABLE",
        "fed_state": "UNAVAILABLE",
        "events": [],
        "coverage": [],
    }
    if source is None:
        return _envelope(
            available=False,
            reason="EVENT_CALENDAR_UNCONFIGURED",
            data=empty,
        )
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (source, source_key) *
                FROM option_event_calendar_coverage
                WHERE source = %s
                  AND first_observed_at <= %s
                                    AND first_observed_at >= %s
                ORDER BY source, source_key, first_observed_at DESC
            )
            SELECT coverage_id, event_type, affected_underlying,
                   window_start, window_end, source, source_key,
                   source_observed_at, first_observed_at, payload_sha256
            FROM latest
            WHERE window_start <= %s
              AND window_end >= %s
              AND (
                  (event_type = 'EARNINGS' AND affected_underlying = %s)
                  OR (event_type = 'FED_RATE_DECISION'
                      AND affected_underlying IS NULL)
              )
            ORDER BY event_type, affected_underlying, source_key
            """,
            (source, now, now - maximum_age, decision_start, decision_end, symbol),
        )
        coverage = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (source, source_key) *
                FROM option_market_events
                WHERE source = %s
                  AND first_observed_at <= %s
                  AND COALESCE(revised_observed_at, first_observed_at) <= %s
                ORDER BY source, source_key,
                         COALESCE(revised_observed_at, first_observed_at) DESC
            )
            SELECT market_event_id, event_type, affected_underlying,
                   scheduled_time, source, source_key, announcement_time,
                   source_observed_at, first_observed_at,
                   confidence, status, payload_sha256
            FROM latest
            WHERE scheduled_time BETWEEN %s AND %s
              AND (affected_underlying IS NULL OR affected_underlying = %s)
            ORDER BY scheduled_time, event_type, source_key
            """,
            (source, now, now, decision_start, upcoming_end, symbol),
        )
        events = [dict(row) for row in cursor.fetchall()]
    decision_events = [
        row for row in events if row["scheduled_time"] <= decision_end
    ]
    data = {
        **empty,
        "earnings_state": _event_window_state(
            "EARNINGS", coverage, decision_events,
            not_applicable=asset_type == "ETF",
        ),
        "fed_state": _event_window_state(
            "FED_RATE_DECISION", coverage, decision_events
        ),
        "events": events,
        "coverage": coverage,
    }
    available = data["fed_state"] != "UNAVAILABLE" and (
        asset_type == "ETF" or data["earnings_state"] != "UNAVAILABLE"
    )
    return _envelope(
        available=available,
        reason=None if available else "EVENT_CALENDAR_COVERAGE_INCOMPLETE",
        observed_at=max(
            [row["first_observed_at"] for row in events + coverage],
            default=None,
        ),
        data=data,
    )


@router.get("/quotes/current", response_model=OptionsEnvelope)
def option_current_quotes(
    contract_id: list[int] = Query(min_length=1, max_length=8),
) -> OptionsEnvelope:
    contract_ids = tuple(dict.fromkeys(contract_id))
    if any(value <= 0 for value in contract_ids):
        return _envelope(available=False, reason="INVALID_CONTRACT_ID", data={"quotes": []})
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT to_regclass('public.option_quote_current') IS NOT NULL AS ready
            """
        )
        if not cursor.fetchone()["ready"]:
            return _envelope(available=False, reason="ADVANCED_QUOTE_SCHEMA_UNAVAILABLE", data={"quotes": []})
        cursor.execute(
            """
            SELECT quote.contract_id, quote.contract_ticker, quote.quote_time,
                   quote.received_at, quote.bid, quote.ask, quote.bid_size,
                   quote.ask_size, quote.updated_at
            FROM option_quote_current AS quote
            WHERE quote.provider = 'polygon_advanced'
              AND quote.contract_id = ANY(%s::bigint[])
              AND quote.quote_time <= NOW()
              AND quote.received_at <= NOW()
            ORDER BY quote.contract_id
            """,
            (list(contract_ids),),
        )
        quotes = [dict(row) for row in cursor.fetchall()]
    found = {row["contract_id"] for row in quotes}
    return _envelope(
        available=bool(quotes),
        reason=None if quotes else "ADVANCED_QUOTES_UNAVAILABLE",
        as_of=max((row["quote_time"] for row in quotes), default=None),
        observed_at=max((row["received_at"] for row in quotes), default=None),
        data={
            "quotes": quotes,
            "requested_contract_ids": list(contract_ids),
            "missing_contract_ids": [value for value in contract_ids if value not in found],
            "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
            "provider_fetch_performed": False,
            "execution_permission": False,
        },
    )