from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from database import get_db_cursor
from options.config import load_option_runtime_configuration


DATA_TIER_LABEL = "15-MINUTE DELAYED RESEARCH DATA"
router = APIRouter(prefix="/api/options", tags=["options-research"])


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


@router.get("/health", response_model=OptionsEnvelope)
def option_health() -> OptionsEnvelope:
    configuration = _configuration()
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(
                available=False,
                reason="MIGRATION_015_NOT_APPLIED",
                data={
                    "read_only": True,
                    "candidate_workbench": {
                        "available": False,
                        "reason": "MIGRATION_016_NOT_APPLIED",
                        "candidate_count": 0,
                    },
                },
            )
        strategy_ready = _strategy_schema_available(cursor)
        candidate_count = 0
        if strategy_ready:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM option_strategy_candidates WHERE policy_sha256 = %s",
                (configuration.strategy_policy_sha256,),
            )
            candidate_count = cursor.fetchone()["count"]
        cursor.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (underlying)
                    underlying, status, scheduled_cycle, market_data_time,
                    first_observed_at, completed_at, received_row_count,
                    retained_row_count, unknown_reference_count, error_category,
                    failure_reason
                FROM option_ingestion_runs
                ORDER BY underlying, scheduled_cycle DESC, started_at DESC
            ), work AS (
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('PENDING', 'RETRY')) AS pending,
                    COUNT(*) FILTER (WHERE status = 'CLAIMED') AS claimed,
                    MIN(
                        CASE WHEN status IN ('PENDING', 'RETRY')
                             THEN created_at END
                    ) AS oldest_pending_at
                FROM option_work_items
            ), leader AS (
                SELECT instance_id, status, last_heartbeat_at
                FROM option_scheduler_instances
                ORDER BY last_heartbeat_at DESC
                LIMIT 1
            )
            SELECT
                COALESCE(jsonb_agg(to_jsonb(latest) ORDER BY latest.underlying)
                    FILTER (WHERE latest.underlying IS NOT NULL), '[]'::jsonb) AS underlyings,
                to_jsonb(work) AS work,
                to_jsonb(leader) AS leader,
                option_market_data_partitions_ready(NOW()) AS partitions_ready
            FROM work
            LEFT JOIN latest ON TRUE
            LEFT JOIN leader ON TRUE
            GROUP BY work.*, leader.*
            """
        )
        row = cursor.fetchone()
    latest = row["underlyings"] or []
    work = row["work"] or {}
    oldest_pending_at = work.pop("oldest_pending_at", None)
    if isinstance(oldest_pending_at, str):
        oldest_pending_at = datetime.fromisoformat(oldest_pending_at)
    work["oldest_pending_seconds"] = (
        max(
            0.0,
            (datetime.now(timezone.utc) - oldest_pending_at).total_seconds(),
        )
        if oldest_pending_at
        else None
    )
    newest_market = max(
        (item["market_data_time"] for item in latest if item.get("market_data_time")),
        default=None,
    )
    newest_observed = max(
        (item["first_observed_at"] for item in latest if item.get("first_observed_at")),
        default=None,
    )
    return _envelope(
        available=bool(latest),
        reason=None if latest else "NO_INGESTION_RUNS",
        as_of=newest_market,
        observed_at=newest_observed,
        data={
            "read_only": True,
            "schema_ready": True,
            "partitions_ready": row["partitions_ready"],
            "archive_enabled": configuration.settings.raw_archive_enabled,
            "risk_free_rate": str(configuration.settings.risk_free_rate),
            "risk_free_rate_source": configuration.settings.risk_free_rate_source,
            "default_dividend_yield": str(
                configuration.settings.default_dividend_yield
            ),
            "underlyings": latest,
            "work": work,
            "leader": row["leader"],
            "candidate_workbench": {
                "available": strategy_ready and candidate_count > 0,
                "reason": (
                    None
                    if strategy_ready and candidate_count > 0
                    else "NO_STRATEGY_RESULTS"
                    if strategy_ready
                    else "MIGRATION_016_NOT_APPLIED"
                ),
                "candidate_count": candidate_count,
                "strategy_policy_sha256": configuration.strategy_policy_sha256,
            },
        },
    )


@router.get("/universe", response_model=OptionsEnvelope)
def option_universe() -> OptionsEnvelope:
    configuration = _configuration()
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_015_NOT_APPLIED", data=[])
        cursor.execute(
            """
            SELECT DISTINCT ON (member.ticker)
                member.ticker, member.asset_type, member.effective_from,
                member.member_rank, member.score, member.activated_at,
                member.deactivated_at, member.first_observed_at,
                run.run_id, run.mode, run.status AS run_status,
                run.completeness_fraction, run.as_of_session
            FROM option_universe_members AS member
            JOIN option_universe_runs AS run ON run.run_id = member.source_run_id
            WHERE member.deactivated_at IS NULL
              AND run.status IN ('COMPLETE', 'DEGRADED')
            ORDER BY member.ticker, member.effective_from DESC,
                     member.first_observed_at DESC, member.member_id DESC
            """
        )
        members = cursor.fetchall()
    persisted_by_ticker = {member["ticker"]: dict(member) for member in members}
    merged = []
    for ticker in configuration.settings.underlyers:
        persisted = persisted_by_ticker.get(ticker)
        if persisted:
            merged.append(persisted)
        else:
            merged.append(
                {
                    "ticker": ticker,
                    "asset_type": (
                        "ETF"
                        if ticker in configuration.settings.fixed_etf_underlyers
                        else "STOCK"
                    ),
                    "state": "CONFIGURED_PENDING_FIRST_RUN",
                }
            )
    if not members:
        return _envelope(
            available=False,
            reason="NO_COMPLETED_UNIVERSE_RUN",
            data=merged,
        )
    return _envelope(
        available=True,
        as_of=max(member["activated_at"] for member in members),
        observed_at=max(member["first_observed_at"] for member in members),
        data=merged,
    )


@router.get("/chain/{underlyer}", response_model=OptionsEnvelope)
def option_chain(
    underlyer: str,
    expiration: str | None = None,
    contract_type: Literal["CALL", "PUT"] | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> OptionsEnvelope:
    ticker = underlyer.strip().upper()
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_015_NOT_APPLIED", data=[])
        cursor.execute(
            """
            SELECT batch_id, scheduled_cycle, market_data_time, first_observed_at,
                   retained_row_count, received_row_count, unknown_reference_count,
                   completed_at
            FROM option_ingestion_runs
            WHERE underlying = %s AND status = 'COMPLETE'
              AND retained_row_count > 0
            ORDER BY scheduled_cycle DESC, completed_at DESC
            LIMIT 1
            """,
            (ticker,),
        )
        batch = cursor.fetchone()
        if not batch:
            return _envelope(
                available=False,
                reason="NO_COMPLETE_MATRIX",
                data={"underlyer": ticker, "rows": [], "total": 0},
            )
        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM option_chain_snapshots
            WHERE batch_id = %s
              AND (%s IS NULL OR expiration_date = %s::date)
              AND (%s IS NULL OR contract_type = %s)
            """,
            (batch["batch_id"], expiration, expiration, contract_type, contract_type),
        )
        total = cursor.fetchone()["total"]
        cursor.execute(
            """
            SELECT
                snapshot_id, contract_id, contract_ticker, contract_type,
                expiration_date, expiration_cutoff, calendar_dte, strike, spot,
                display_mark, model_mark, mark_source, day_volume, open_interest,
                market_data_time, first_observed_at, data_delay_seconds,
                local_iv, local_delta, local_gamma, local_theta_per_day,
                local_vega_per_vol_point, local_rho_per_rate_point,
                intrinsic_value, extrinsic_value, single_contract_breakeven,
                provider_iv, provider_gamma, iv_converged, iv_solver,
                iv_failure_reason, model_version, quality_flags
            FROM option_chain_snapshots
            WHERE batch_id = %s
              AND (%s IS NULL OR expiration_date = %s::date)
              AND (%s IS NULL OR contract_type = %s)
            ORDER BY expiration_date, strike, contract_type, contract_id
            LIMIT %s OFFSET %s
            """,
            (
                batch["batch_id"],
                expiration,
                expiration,
                contract_type,
                contract_type,
                limit,
                offset,
            ),
        )
        rows = cursor.fetchall()
        cursor.execute(
            """
            SELECT status, quality_reasons, iv_convergence_fraction, matrix_id
            FROM option_analysis_runs
            WHERE batch_id = %s
            LIMIT 1
            """,
            (batch["batch_id"],),
        )
        analysis = cursor.fetchone()
    return _envelope(
        available=True,
        as_of=batch["market_data_time"] or batch["scheduled_cycle"],
        observed_at=batch["first_observed_at"] or batch["completed_at"],
        model_version=rows[0]["model_version"] if rows else None,
        data={
            "underlyer": ticker,
            "batch": batch,
            "analysis": analysis,
            "total": total,
            "limit": limit,
            "offset": offset,
            "quote_liquidity": "NOT_AVAILABLE",
            "rows": rows,
        },
    )


@router.get("/analysis/{underlyer}", response_model=OptionsEnvelope)
def option_analysis(underlyer: str) -> OptionsEnvelope:
    ticker = underlyer.strip().upper()
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_015_NOT_APPLIED", data={})
        cursor.execute(
            """
            SELECT *
            FROM option_analysis_runs
            WHERE underlying = %s AND status <> 'RUNNING'
            ORDER BY market_time DESC, observed_time DESC
            LIMIT 1
            """,
            (ticker,),
        )
        analysis = cursor.fetchone()
        if not analysis:
            return _envelope(
                available=False,
                reason="NO_ANALYSIS",
                data={"underlyer": ticker, "expirations": []},
            )
        cursor.execute(
            """
            SELECT *
            FROM option_expiration_analytics
            WHERE matrix_id = %s
            ORDER BY expiration_date
            """,
            (analysis["matrix_id"],),
        )
        expirations = cursor.fetchall()
    return _envelope(
        available=True,
        as_of=analysis["market_time"],
        observed_at=analysis["observed_time"],
        policy_sha256=analysis["policy_sha256"],
        model_version=analysis["model_version"],
        data={
            "underlyer": ticker,
            "analysis": analysis,
            "expirations": expirations,
            "quote_liquidity": "NOT_AVAILABLE",
        },
    )


@router.get("/data-quality", response_model=OptionsEnvelope)
def option_data_quality(
    limit: int = Query(default=50, ge=1, le=200),
) -> OptionsEnvelope:
    with get_db_cursor() as cursor:
        if not _schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_015_NOT_APPLIED", data={})
        cursor.execute(
            """
            SELECT
                batch_id, underlying, asset_type, scheduled_cycle, status,
                page_count, terminal_page_received, received_row_count,
                catalog_row_count, retained_row_count, rejected_counts,
                unknown_reference_count, latency_ms, retry_count, error_category,
                failure_reason, market_data_time, first_observed_at, completed_at
            FROM option_ingestion_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        runs = cursor.fetchall()
        cursor.execute(
            """
            SELECT stage, status, COUNT(*) AS count,
                   MIN(created_at) AS oldest_created_at
            FROM option_work_items
            GROUP BY stage, status
            ORDER BY stage, status
            """
        )
        work = cursor.fetchall()
        cursor.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM option_contract_discoveries
            GROUP BY state
            ORDER BY state
            """
        )
        discoveries = cursor.fetchall()
        cursor.execute(
            """
            SELECT backfill_status, COUNT(*) AS count
            FROM option_trade_watchlist
            GROUP BY backfill_status
            ORDER BY backfill_status
            """
        )
        backfills = cursor.fetchall()
    newest = runs[0] if runs else None
    return _envelope(
        available=bool(runs),
        reason=None if runs else "NO_INGESTION_RUNS",
        as_of=newest["market_data_time"] if newest else None,
        observed_at=newest["first_observed_at"] if newest else None,
        data={
            "runs": runs,
            "work": work,
            "new_series": discoveries,
            "trade_backfills": backfills,
        },
    )


@router.get("/candidates", response_model=OptionsEnvelope)
def option_candidates(
    underlyer: str | None = None,
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
) -> OptionsEnvelope:
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
                SELECT DISTINCT ON (underlying)
                    underlying, matrix_id
                FROM option_strategy_candidates
                WHERE policy_sha256 = %s
                                    AND market_data_time <= NOW()
                                    AND observed_time <= NOW()
                ORDER BY underlying, market_data_time DESC, observed_time DESC
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
            (configuration.strategy_policy_sha256, *params),
        )
        status_counts = dict(cursor.fetchone())
        total = status_counts[status.lower()] if status else status_counts["total"]
        cursor.execute(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (underlying)
                    underlying, matrix_id
                FROM option_strategy_candidates
                WHERE policy_sha256 = %s
                                    AND market_data_time <= NOW()
                                    AND observed_time <= NOW()
                ORDER BY underlying, market_data_time DESC, observed_time DESC
            )
            SELECT candidate.*, registry.display_name,
                   registry.presentation_metadata,
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
                               'source_market_time', leg.source_market_time,
                               'mark_source', leg.mark_source,
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
            WHERE {where_sql}
            GROUP BY candidate.candidate_id, registry.strategy_name,
                     registry.strategy_version
            ORDER BY
                CASE candidate.status
                    WHEN 'SELECTED' THEN 0 WHEN 'SUPPRESSED' THEN 1 ELSE 2
                END,
                candidate.strategy_archetype,
                candidate.underlying,
                candidate.candidate_rank,
                candidate.candidate_id
            LIMIT %s OFFSET %s
            """,
            (configuration.strategy_policy_sha256, *filtered_params, limit, offset),
        )
        rows = cursor.fetchall()
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
            "title": "Weekly Research Candidates",
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
            LEFT JOIN option_signal_events AS signal
              ON signal.source_candidate_id = candidate.candidate_id
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
            "quote_liquidity": "NOT_AVAILABLE",
            "execution_mode": "READ_ONLY_RESEARCH",
        },
    )


@router.get("/scenarios/{candidate_id}", response_model=OptionsEnvelope)
def option_candidate_scenarios(candidate_id: UUID) -> OptionsEnvelope:
    with get_db_cursor() as cursor:
        if not _strategy_schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_016_NOT_APPLIED", data=[])
        cursor.execute(
            """
            SELECT scenario.*,
                   candidate.market_data_time,
                   candidate.observed_time,
                   candidate.policy_sha256
            FROM option_scenario_results AS scenario
            JOIN option_strategy_candidates AS candidate USING (candidate_id)
            WHERE scenario.candidate_id = %s
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
            ORDER BY scenario.time_fraction_remaining DESC,
                     scenario.spot_shock_fraction,
                     scenario.iv_shock_fraction
            """,
            (str(candidate_id),),
        )
        rows = cursor.fetchall()
    first = rows[0] if rows else None
    return _envelope(
        available=bool(rows),
        reason=None if rows else "NO_SCENARIO_RESULTS",
        as_of=first["market_data_time"] if first else None,
        observed_at=first["observed_time"] if first else None,
        policy_sha256=first["policy_sha256"] if first else None,
        data=rows,
    )


@router.get("/signals", response_model=OptionsEnvelope)
def option_signals(
    underlyer: str | None = None,
    status: Literal["PENDING", "READY", "BLOCKED", "EXPIRED"] | None = None,
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> OptionsEnvelope:
    clauses = ["market_data_time <= NOW()", "observed_time <= NOW()"]
    params: list[object] = []
    if underlyer:
        clauses.append("underlying = %s")
        params.append(underlyer.strip().upper())
    if status:
        clauses.append("status = %s")
        params.append(status)
    with get_db_cursor() as cursor:
        if not _strategy_schema_available(cursor):
            return _envelope(available=False, reason="MIGRATION_016_NOT_APPLIED", data=[])
        cursor.execute(
            f"""
            SELECT *
            FROM option_signal_events
            WHERE {' AND '.join(clauses)}
            ORDER BY market_data_time DESC, strategy_name, event_id
            LIMIT %s OFFSET %s
            """,
            (*params, limit, offset),
        )
        rows = cursor.fetchall()
    first = rows[0] if rows else None
    return _envelope(
        available=bool(rows),
        reason=None if rows else "NO_SIGNAL_EVENTS",
        as_of=first["market_data_time"] if first else None,
        observed_at=first["observed_time"] if first else None,
        data=rows,
    )