"""PostgreSQL persistence for equity materialization facts."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Callable, ContextManager, Iterator, Mapping, Sequence
from uuid import UUID

from psycopg2.extras import Json, RealDictCursor, execute_values, register_uuid

from .domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    ContextStatus,
    DecisionWatermark,
    EquityBarRevision,
    EquityContextSnapshot,
    EquityCorporateAction,
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    FundamentalReport,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from .outcomes import OutcomePolicy, ResearchOutcome
from .qualification import QualificationRevision


register_uuid()
ConnectionFactory = Callable[[], ContextManager[Any]]


def default_connection_factory() -> ContextManager[Any]:
    from database import get_db_connection

    return get_db_connection()


class _Repository:
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or default_connection_factory

    @contextmanager
    def _cursor(self, *, dict_rows: bool = True) -> Iterator[Any]:
        with self._connection_factory() as connection:
            cursor = connection.cursor(cursor_factory=RealDictCursor if dict_rows else None)
            try:
                yield cursor
                connection.commit()
            except Exception:
                if not connection.closed:
                    connection.rollback()
                raise
            finally:
                if not cursor.closed:
                    cursor.close()


class EquityReferenceRepository(_Repository):
    def list_historical_sector_candidates(
        self,
        policy_version: str,
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH first_membership AS (
                    SELECT DISTINCT ON (member.security_id, member.ticker)
                        member.security_id, member.ticker, member.effective_from
                    FROM equity_universe_members AS member
                    JOIN equity_universe_runs AS run
                      ON run.universe_run_id = member.universe_run_id
                    WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
                      AND run.policy_version = %s
                    ORDER BY member.security_id, member.ticker,
                             member.effective_from ASC
                )
                SELECT security_id, ticker, effective_from
                FROM first_membership AS member
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM equity_security_reference_revisions AS reference
                    WHERE reference.security_id = member.security_id
                      AND reference.ticker = member.ticker
                      AND reference.effective_from <= member.effective_from
                      AND reference.sector IS NOT NULL
                )
                ORDER BY ticker, effective_from
                """,
                (policy_version,),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def persist_security_revisions(
        self, revisions: Sequence[SecurityReferenceRevision]
    ) -> int:
        if not revisions:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_security_reference_revisions (
                    security_revision_id, security_id, ticker, active, company_name,
                    security_type, cik, composite_figi, share_class_figi,
                    primary_exchange, sic_code, sic_description, sector, industry,
                    list_date, delisted_date, weighted_shares, free_float,
                    free_float_percent, market_cap, source, effective_from,
                    observed_at, payload_sha256, raw_payload
                ) VALUES %s
                ON CONFLICT (security_id, source, effective_from, payload_sha256)
                DO NOTHING
                RETURNING security_revision_id
                """,
                [
                    (
                        row.security_revision_id, row.security_id, row.ticker, row.active,
                        row.company_name, row.security_type, row.cik, row.composite_figi,
                        row.share_class_figi, row.primary_exchange, row.sic_code,
                        row.sic_description, row.sector, row.industry, row.list_date,
                        row.delisted_date, row.weighted_shares, row.free_float,
                        row.free_float_percent, row.market_cap, row.source,
                        row.effective_from, row.observed_at, row.payload_sha256,
                        Json(__import__("json").loads(row.raw_payload_json)),
                    )
                    for row in revisions
                ],
                fetch=True,
            )
            return len(inserted)

    def update_selected_ticker_projection(
        self, revisions: Sequence[SecurityReferenceRevision]
    ) -> None:
        if not revisions:
            return
        with self._cursor() as cursor:
            cursor.executemany(
                """
                UPDATE selected_tickers
                SET company_name = %s,
                    security_id = %s,
                    cik = %s,
                    composite_figi = %s,
                    share_class_figi = %s,
                    sic_code = COALESCE(%s, sic_code),
                    industry = COALESCE(%s, industry),
                    sector = COALESCE(%s, sector),
                    market_cap = COALESCE(%s, market_cap),
                    market_cap_group = COALESCE(%s, market_cap_group),
                    float_shares = COALESCE(%s, float_shares),
                    weighted_shares_outstanding = %s,
                    exchange = COALESCE(%s, exchange),
                    metadata_source = %s,
                    metadata_observed_at = %s,
                    metadata_updated = %s
                WHERE ticker = %s
                """,
                [
                    (
                        row.company_name, row.security_id, row.cik,
                        row.composite_figi, row.share_class_figi, row.sic_code,
                        row.sic_description, row.sector, row.market_cap,
                        _market_cap_group(row.market_cap), row.free_float,
                        row.weighted_shares, row.primary_exchange, row.source,
                        row.observed_at, row.observed_at, row.ticker,
                    )
                    for row in revisions
                ],
            )

    def persist_fundamental_reports(
        self, reports: Sequence[FundamentalReport]
    ) -> int:
        if not reports:
            return 0
        import json

        typed_columns = (
            "revenue", "gross_profit", "operating_income", "ebitda",
            "pretax_income", "interest_expense", "income_taxes", "net_income",
            "basic_eps", "diluted_eps", "basic_weighted_shares",
            "diluted_weighted_shares", "research_and_development",
            "selling_general_admin", "depreciation_amortization",
            "cash_and_equivalents", "short_term_investments", "current_assets",
            "current_liabilities", "total_assets", "current_debt",
            "long_term_debt", "total_liabilities", "total_equity",
            "operating_cash_flow", "capital_expenditures", "free_cash_flow",
            "dividends", "investing_cash_flow", "financing_cash_flow",
        )
        with self._cursor() as cursor:
            values = []
            for report in reports:
                metrics = json.loads(report.metrics_json)
                values.append((
                    report.fundamental_report_id, report.security_id,
                    report.security_revision_id, report.cik, report.accession_number,
                    report.form_type, report.timeframe, report.fiscal_year,
                    report.fiscal_quarter, report.period_end, report.filing_date,
                    report.availability_time, report.observed_at,
                    *(metrics.get(column) for column in typed_columns),
                    report.source, report.source_key, report.payload_sha256,
                    Json(json.loads(report.raw_payload_json)), list(report.quality_codes),
                ))
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_fundamental_reports (
                    fundamental_report_id, security_id, security_revision_id, cik,
                    accession_number, form_type, timeframe, fiscal_year,
                    fiscal_quarter, period_end, filing_date, availability_time,
                    observed_at, revenue, gross_profit, operating_income, ebitda,
                    pretax_income, interest_expense, income_taxes, net_income,
                    basic_eps, diluted_eps, basic_weighted_shares,
                    diluted_weighted_shares, research_and_development,
                    selling_general_admin, depreciation_amortization,
                    cash_and_equivalents, short_term_investments, current_assets,
                    current_liabilities, total_assets, current_debt, long_term_debt,
                    total_liabilities, total_equity, operating_cash_flow,
                    capital_expenditures, free_cash_flow, dividends,
                    investing_cash_flow, financing_cash_flow, source, source_key,
                    payload_sha256, raw_payload, quality_codes
                ) VALUES %s
                ON CONFLICT (source, source_key, payload_sha256) DO NOTHING
                RETURNING fundamental_report_id
                """,
                values,
                fetch=True,
            )
            return len(inserted)

    def get_security_as_of(
        self, ticker: str, context: DecisionWatermark
    ) -> SecurityReferenceRevision | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM equity_security_reference_revisions
                WHERE ticker = %s
                  AND effective_from <= %s
                  AND observed_at <= %s
                ORDER BY effective_from DESC, observed_at DESC, created_at DESC
                LIMIT 1
                """,
                (ticker.upper(), context.market_time, context.observed_time),
            )
            row = cursor.fetchone()
        return _security_from_row(row) if row else None

    def get_security_revision(
        self,
        security_revision_id: UUID,
    ) -> SecurityReferenceRevision | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM equity_security_reference_revisions
                WHERE security_revision_id = %s
                """,
                (security_revision_id,),
            )
            row = cursor.fetchone()
        return _security_from_row(row) if row else None

    def list_securities_as_of(
        self,
        tickers: Sequence[str],
        context: DecisionWatermark,
    ) -> tuple[SecurityReferenceRevision, ...]:
        if not tickers:
            return ()
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (ticker) *
                FROM equity_security_reference_revisions
                WHERE ticker = ANY(%s)
                  AND effective_from <= %s
                  AND observed_at <= %s
                ORDER BY ticker, effective_from DESC, observed_at DESC, created_at DESC
                """,
                (
                    [ticker.upper() for ticker in tickers],
                    context.market_time,
                    context.observed_time,
                ),
            )
            return tuple(_security_from_row(row) for row in cursor.fetchall())

    def list_fundamentals_as_of(
        self,
        security_id: UUID,
        context: DecisionWatermark,
        *,
        timeframe: str | None = None,
        limit: int = 8,
    ) -> tuple[dict[str, Any], ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM equity_fundamental_reports
                WHERE security_id = %s
                  AND availability_time <= %s
                  AND observed_at <= %s
                  AND (%s IS NULL OR timeframe = %s)
                ORDER BY availability_time DESC, observed_at DESC, period_end DESC
                LIMIT %s
                """,
                (
                    security_id, context.market_time, context.observed_time,
                    timeframe, timeframe, limit,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())


class EquityCorporateActionRepository(_Repository):
    def persist(self, actions: Sequence[EquityCorporateAction]) -> int:
        if not actions:
            return 0
        import json

        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_corporate_actions (
                    corporate_action_id, security_id, ticker, action_type,
                    effective_date, declaration_date, ex_date, record_date,
                    pay_date, cash_amount, split_from, split_to, new_ticker,
                    source, source_key, first_observed_at, revised_observed_at,
                    payload_sha256, raw_payload, availability_mode,
                    replay_available_at
                ) VALUES %s
                ON CONFLICT (corporate_action_id) DO NOTHING
                RETURNING corporate_action_id
                """,
                [
                    (
                        row.corporate_action_id, row.security_id, row.ticker,
                        row.action_type, row.effective_date, row.declaration_date,
                        row.ex_date, row.record_date, row.pay_date, row.cash_amount,
                        row.split_from, row.split_to, row.new_ticker, row.source,
                        row.source_key, row.first_observed_at,
                        row.revised_observed_at, row.payload_sha256,
                        Json(json.loads(row.raw_payload_json)),
                        row.availability_mode.value, row.replay_available_at,
                    )
                    for row in actions
                ],
                fetch=True,
            )
            return len(inserted)

    def list_for_replay(
        self,
        tickers: Sequence[str],
        context: DecisionWatermark,
        *,
        start_date,
    ) -> tuple[dict[str, Any], ...]:
        if not tickers:
            return ()
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM equity_corporate_actions
                WHERE ticker = ANY(%s)
                  AND effective_date BETWEEN %s AND %s
                  AND COALESCE(replay_available_at, first_observed_at) <= %s
                ORDER BY effective_date, ticker, action_type, source_key
                """,
                (
                    [ticker.upper() for ticker in tickers], start_date,
                    context.market_time.date(), context.observed_time,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())


class EquityBarRepository(_Repository):
    def persist(self, bars: Sequence[EquityBarRevision]) -> int:
        if not bars:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_bar_revisions (
                    bar_revision_id, security_id, ticker, interval, session_date, session_scope,
                    bar_start, bar_end, open_price, high_price, low_price,
                    close_price, volume, vwap, transaction_count, source_kind,
                    availability_mode, is_final, provider_published_at,
                    system_observed_at, replay_available_at, ingestion_segment_id,
                    adjusted, payload_sha256, source_bar_revision_ids,
                    supersedes_bar_revision_id, reconciliation_status, quality_codes
                ) VALUES %s
                ON CONFLICT (
                    ticker, interval, bar_start, session_scope, adjusted,
                    source_kind, availability_mode, payload_sha256
                )
                DO NOTHING
                RETURNING bar_revision_id
                """,
                [
                    (
                        row.bar_revision_id, row.security_id, row.ticker, row.interval,
                        row.session_date, row.session_scope.value,
                        row.bar_start, row.bar_end, row.open_price,
                        row.high_price, row.low_price, row.close_price, row.volume,
                        row.vwap, row.transaction_count, row.source_kind.value,
                        row.availability_mode.value, row.is_final,
                        row.provider_published_at, row.system_observed_at,
                        row.replay_available_at, row.ingestion_segment_id, row.adjusted,
                        row.payload_sha256, list(row.source_bar_revision_ids),
                        row.supersedes_bar_revision_id, row.reconciliation_status,
                        list(row.quality_codes),
                    )
                    for row in bars
                ],
                fetch=True,
            )
            return len(inserted)

    def list_final_as_of(
        self,
        ticker: str,
        interval: str,
        context: DecisionWatermark,
        *,
        limit: int,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ) -> tuple[EquityBarRevision, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH visible AS (
                    SELECT DISTINCT ON (ticker, interval, bar_start)
                        *
                    FROM equity_bar_revisions
                    WHERE ticker = %s
                      AND interval = %s
                                            AND session_scope = %s
                                            AND adjusted = %s
                      AND is_final = TRUE
                      AND NOT (
                          availability_mode = 'HISTORICAL_RECONSTRUCTED'
                          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                      )
                      AND bar_end <= %s
                      AND COALESCE(replay_available_at, system_observed_at) <= %s
                    ORDER BY ticker, interval, bar_start,
                            CASE
                               WHEN source_kind = 'RECONCILED' THEN 0
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'NATIVE_REST' THEN 1
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'REALTIME_STREAM' THEN 2
                               WHEN interval IN ('1h', '1d', '1wk', '1mo')
                                   AND source_kind = 'DERIVED' THEN 1
                               WHEN source_kind = 'NATIVE_REST' THEN 2
                               WHEN source_kind = 'DERIVED' THEN 3
                               ELSE 4
                            END,
                             CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                             COALESCE(replay_available_at, system_observed_at) DESC,
                             created_at DESC
                )
                SELECT * FROM visible
                ORDER BY bar_start DESC
                LIMIT %s
                """,
                (
                    ticker.upper(), interval, session_scope.value, adjusted,
                    context.market_time,
                    context.observed_time, limit,
                ),
            )
            rows = tuple(cursor.fetchall())
        return tuple(reversed([_bar_from_row(row) for row in rows]))

    def list_final_after(
        self,
        ticker: str,
        interval: str,
        *,
        after,
        available_by,
        limit: int,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
        historical_reconstructed_only: bool = False,
    ) -> tuple[EquityBarRevision, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH visible AS (
                    SELECT DISTINCT ON (ticker, interval, bar_start) *
                    FROM equity_bar_revisions
                    WHERE ticker = %s
                      AND interval = %s
                                            AND session_scope = %s
                                            AND adjusted = %s
                      AND is_final = TRUE
                      AND (
                          %s = FALSE OR (
                              availability_mode = 'HISTORICAL_RECONSTRUCTED'
                              AND quality_codes @>
                                  ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
                          )
                      )
                      AND NOT (
                          availability_mode = 'HISTORICAL_RECONSTRUCTED'
                          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                      )
                      AND bar_start > %s
                      AND COALESCE(replay_available_at, system_observed_at) <= %s
                    ORDER BY ticker, interval, bar_start,
                            CASE
                               WHEN source_kind = 'RECONCILED' THEN 0
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'NATIVE_REST' THEN 1
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'REALTIME_STREAM' THEN 2
                               WHEN interval IN ('1h', '1d', '1wk', '1mo')
                                   AND source_kind = 'DERIVED' THEN 1
                               WHEN source_kind = 'NATIVE_REST' THEN 2
                               WHEN source_kind = 'DERIVED' THEN 3
                               ELSE 4
                            END,
                             CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                             COALESCE(replay_available_at, system_observed_at) DESC,
                             created_at DESC
                )
                SELECT * FROM visible ORDER BY bar_start ASC LIMIT %s
                """,
                (
                    ticker.upper(), interval, session_scope.value, adjusted,
                    historical_reconstructed_only,
                    after, available_by, limit,
                ),
            )
            return tuple(_bar_from_row(row) for row in cursor.fetchall())

    def list_final_for_tickers_as_of(
        self,
        tickers: Sequence[str],
        interval: str,
        context: DecisionWatermark,
        *,
        limit_per_ticker: int | None,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ) -> dict[str, tuple[EquityBarRevision, ...]]:
        if not tickers:
            return {}
        if limit_per_ticker is not None and limit_per_ticker <= 0:
            raise ValueError("limit_per_ticker must be positive")
        normalized = tuple(dict.fromkeys(ticker.upper() for ticker in tickers))
        limit_clause = (
            "WHERE recency_rank <= %s" if limit_per_ticker is not None else ""
        )
        with self._cursor() as cursor:
            cursor.execute(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (ticker, interval, bar_start) *
                    FROM equity_bar_revisions
                    WHERE ticker = ANY(%s)
                      AND interval = %s
                      AND session_scope = %s
                      AND adjusted = %s
                      AND is_final = TRUE
                      AND NOT (
                          availability_mode = 'HISTORICAL_RECONSTRUCTED'
                          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                      )
                      AND bar_end <= %s
                      AND COALESCE(replay_available_at, system_observed_at) <= %s
                    ORDER BY ticker, interval, bar_start,
                             CASE
                                 WHEN source_kind = 'RECONCILED' THEN 0
                                 WHEN interval IN ('5m', '15m', '30m')
                                      AND source_kind = 'NATIVE_REST' THEN 1
                                 WHEN interval IN ('5m', '15m', '30m')
                                      AND source_kind = 'REALTIME_STREAM' THEN 2
                                 WHEN interval IN ('1h', '1d', '1wk', '1mo')
                                      AND source_kind = 'DERIVED' THEN 1
                                 WHEN source_kind = 'NATIVE_REST' THEN 2
                                 WHEN source_kind = 'DERIVED' THEN 3
                                 ELSE 4
                             END,
                             CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                             COALESCE(replay_available_at, system_observed_at) DESC,
                             created_at DESC
                ), ranked AS (
                    SELECT visible.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY ticker ORDER BY bar_start DESC
                           ) AS recency_rank
                    FROM visible
                )
                SELECT *
                FROM ranked
                {limit_clause}
                ORDER BY ticker, bar_start
                """,
                (
                    list(normalized), interval, session_scope.value, adjusted,
                    context.market_time, context.observed_time,
                    *((limit_per_ticker,) if limit_per_ticker is not None else ()),
                ),
            )
            rows = cursor.fetchall()
        grouped = {ticker: [] for ticker in normalized}
        for row in rows:
            grouped[row["ticker"]].append(_bar_from_row(row))
        return {ticker: tuple(values) for ticker, values in grouped.items()}

    def daily_session_bars(
        self,
        tickers: Sequence[str],
        session_date,
        *,
        observed_by,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ) -> dict[str, EquityBarRevision]:
        if not tickers:
            return {}
        normalized = tuple(dict.fromkeys(ticker.upper() for ticker in tickers))
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (ticker) *
                FROM equity_bar_revisions
                WHERE ticker = ANY(%s)
                  AND interval = '1d'
                  AND session_date = %s
                  AND session_scope = %s
                  AND adjusted = %s
                  AND is_final = TRUE
                  AND NOT (
                      availability_mode = 'HISTORICAL_RECONSTRUCTED'
                      AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                  )
                  AND COALESCE(replay_available_at, system_observed_at) <= %s
                ORDER BY ticker,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             ELSE 3
                         END,
                         CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
                """,
                (
                    list(normalized), session_date, session_scope.value,
                    adjusted, observed_by,
                ),
            )
            rows = cursor.fetchall()
        return {row["ticker"]: _bar_from_row(row) for row in rows}

    def list_source_at_market_time(
        self,
        tickers: Sequence[str],
        interval: str,
        market_time,
        *,
        source_kind: BarSourceKind,
        observed_by,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ) -> dict[str, EquityBarRevision]:
        if not tickers:
            return {}
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (ticker) *
                FROM equity_bar_revisions
                WHERE ticker = ANY(%s)
                  AND interval = %s
                  AND bar_end = %s
                  AND source_kind = %s
                  AND session_scope = %s
                  AND adjusted = %s
                  AND is_final = TRUE
                  AND NOT (
                      availability_mode = 'HISTORICAL_RECONSTRUCTED'
                      AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                  )
                  AND COALESCE(replay_available_at, system_observed_at) <= %s
                ORDER BY ticker,
                         CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
                """,
                (
                    [ticker.upper() for ticker in tickers], interval, market_time,
                    source_kind.value, session_scope.value, adjusted, observed_by,
                ),
            )
            rows = cursor.fetchall()
        return {row["ticker"]: _bar_from_row(row) for row in rows}

    def list_pending_reconciliation(
        self,
        interval: str,
        *,
        available_by,
        limit: int = 5000,
    ) -> tuple[EquityBarRevision, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                                SELECT *
                                FROM (
                                        SELECT DISTINCT ON (stream.ticker, stream.interval, stream.bar_start)
                                                stream.*
                                        FROM equity_bar_revisions AS stream
                                        WHERE stream.interval = %s
                                            AND stream.source_kind = 'REALTIME_STREAM'
                                            AND stream.availability_mode = 'LIVE_OBSERVED'
                                            AND stream.reconciliation_status = 'PENDING'
                                            AND stream.system_observed_at <= %s
                                            AND NOT EXISTS (
                                                    SELECT 1
                                                    FROM equity_bar_revisions AS reconciliation
                                                    WHERE reconciliation.source_kind = 'RECONCILED'
                                                        AND stream.bar_revision_id = ANY(
                                                                reconciliation.source_bar_revision_ids
                                                        )
                                            )
                                        ORDER BY stream.ticker, stream.interval, stream.bar_start,
                                                         stream.system_observed_at DESC, stream.created_at DESC
                                ) AS pending
                                ORDER BY pending.bar_end, pending.ticker
                LIMIT %s
                """,
                (interval, available_by, limit),
            )
            return tuple(_bar_from_row(row) for row in cursor.fetchall())

    def latest_common_market_time(
        self,
        tickers: Sequence[str],
        interval: str,
        *,
        observed_by,
        minimum_coverage: float = 0.90,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ):
        if not tickers:
            return None
        if not 0 < minimum_coverage <= 1:
            raise ValueError("minimum_coverage must be in (0, 1]")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH visible AS (
                    SELECT DISTINCT ON (ticker, interval, bar_start)
                        ticker, bar_end
                    FROM equity_bar_revisions
                    WHERE ticker = ANY(%s)
                      AND interval = %s
                                            AND session_scope = %s
                                            AND adjusted = %s
                      AND is_final = TRUE
                      AND NOT (
                          availability_mode = 'HISTORICAL_RECONSTRUCTED'
                          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                      )
                      AND COALESCE(replay_available_at, system_observed_at) <= %s
                    ORDER BY ticker, interval, bar_start,
                            CASE
                               WHEN source_kind = 'RECONCILED' THEN 0
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'NATIVE_REST' THEN 1
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'REALTIME_STREAM' THEN 2
                               WHEN interval IN ('1h', '1d', '1wk', '1mo')
                                   AND source_kind = 'DERIVED' THEN 1
                               WHEN source_kind = 'NATIVE_REST' THEN 2
                               WHEN source_kind = 'DERIVED' THEN 3
                               ELSE 4
                            END,
                             CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                             COALESCE(replay_available_at, system_observed_at) DESC,
                             created_at DESC
                )
                SELECT bar_end
                FROM visible
                GROUP BY bar_end
                HAVING COUNT(DISTINCT ticker) >= CEIL(%s * %s)
                ORDER BY bar_end DESC
                LIMIT 1
                """,
                (
                    [ticker.upper() for ticker in tickers], interval,
                    session_scope.value, adjusted, observed_by,
                    len(tickers), minimum_coverage,
                ),
            )
            row = cursor.fetchone()
        return row["bar_end"] if row else None

    def common_market_times(
        self,
        tickers: Sequence[str],
        interval: str,
        *,
        start,
        end,
        available_by,
        minimum_coverage: float = 0.90,
        limit: int = 10000,
        session_scope: BarSessionScope = BarSessionScope.RTH,
        adjusted: bool = False,
    ) -> tuple[Any, ...]:
        if not tickers:
            return ()
        if not 0 < minimum_coverage <= 1:
            raise ValueError("minimum_coverage must be in (0, 1]")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH visible AS (
                    SELECT DISTINCT ON (ticker, interval, bar_start)
                        ticker, bar_end
                    FROM equity_bar_revisions
                    WHERE ticker = ANY(%s)
                      AND interval = %s
                                            AND session_scope = %s
                                            AND adjusted = %s
                      AND is_final = TRUE
                      AND NOT (
                          availability_mode = 'HISTORICAL_RECONSTRUCTED'
                          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
                      )
                      AND bar_end BETWEEN %s AND %s
                      AND COALESCE(replay_available_at, system_observed_at) <= %s
                    ORDER BY ticker, interval, bar_start,
                            CASE
                               WHEN source_kind = 'RECONCILED' THEN 0
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'NATIVE_REST' THEN 1
                               WHEN interval IN ('5m', '15m', '30m')
                                   AND source_kind = 'REALTIME_STREAM' THEN 2
                               WHEN interval IN ('1h', '1d', '1wk', '1mo')
                                   AND source_kind = 'DERIVED' THEN 1
                               WHEN source_kind = 'NATIVE_REST' THEN 2
                               WHEN source_kind = 'DERIVED' THEN 3
                               ELSE 4
                            END,
                             CASE WHEN availability_mode = 'LIVE_OBSERVED' THEN 0 ELSE 1 END,
                             COALESCE(replay_available_at, system_observed_at) DESC,
                             created_at DESC
                )
                SELECT bar_end
                FROM visible
                GROUP BY bar_end
                HAVING COUNT(DISTINCT ticker) >= CEIL(%s * %s)
                ORDER BY bar_end ASC
                LIMIT %s
                """,
                (
                    [ticker.upper() for ticker in tickers], interval,
                    session_scope.value, adjusted, start, end, available_by,
                    len(tickers), minimum_coverage, limit,
                ),
            )
            return tuple(row["bar_end"] for row in cursor.fetchall())

    def publish_canonical_cohort(
        self,
        *,
        publication_id: UUID,
        business_key: str,
        interval: str,
        market_time,
        observed_at,
        session_scope: BarSessionScope,
        adjusted: bool,
        selection_policy_version: str,
        selection_policy_sha256: str,
        input_sha256: str,
        output_sha256: str,
        members: Sequence[SecurityReferenceRevision],
        selected: Mapping[str, EquityBarRevision],
        failure_reasons: Mapping[str, str] | None = None,
        minimum_coverage: float = 0.95,
    ) -> dict[str, Any]:
        if not 0 < minimum_coverage <= 1:
            raise ValueError("minimum_coverage must be in (0, 1]")
        failures = {
            ticker.upper(): reason for ticker, reason in (failure_reasons or {}).items()
        }
        member_by_ticker = {row.ticker: row for row in members}
        if len(member_by_ticker) != len(members):
            raise ValueError("publication members must have unique tickers")
        if set(selected) - set(member_by_ticker):
            raise ValueError("selected bars must belong to publication members")
        if set(failures) - set(member_by_ticker):
            raise ValueError("failed tickers must belong to publication members")

        member_values = []
        selected_bars = []
        for ticker, member in member_by_ticker.items():
            bar = selected.get(ticker)
            if bar is not None:
                if (
                    not bar.is_final
                    or bar.ticker != ticker
                    or bar.interval != interval
                    or bar.bar_end != market_time
                    or bar.session_scope is not session_scope
                    or bar.adjusted != adjusted
                ):
                    raise ValueError(f"selected bar does not match cohort: {ticker}")
                available_at = bar.replay_available_at or bar.system_observed_at
                if available_at > observed_at:
                    raise ValueError(f"selected bar is not visible at publication: {ticker}")
                status = "SELECTED"
                revision_id = bar.bar_revision_id
                reason_codes = ()
                selected_bars.append(bar)
            elif ticker in failures:
                status = "FAILED"
                revision_id = None
                reason_codes = (failures[ticker],)
            else:
                status = "MISSING"
                revision_id = None
                reason_codes = ("FINAL_BAR_UNAVAILABLE",)
            member_values.append((
                publication_id, member.security_id, ticker, status,
                revision_id, list(reason_codes),
            ))

        expected_count = len(members)
        selected_count = len(selected_bars)
        failed_count = len(failures)
        missing_count = expected_count - selected_count - failed_count
        coverage = selected_count / expected_count if expected_count else 0.0
        publication_status = (
            "COMPLETE" if selected_count == expected_count
            else "DEGRADED" if coverage >= minimum_coverage
            else "FAILED"
        )
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_bar_publications (
                    publication_id, business_key, interval, market_time, observed_at,
                    session_scope, adjusted, selection_policy_version,
                    selection_policy_sha256, expected_members, selected_members,
                    missing_members, failed_members, status, input_sha256,
                    output_sha256, completed_at, published_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    NOW(), CASE WHEN %s IN ('COMPLETE', 'DEGRADED') THEN NOW() END
                )
                ON CONFLICT (business_key) DO NOTHING
                RETURNING *
                """,
                (
                    publication_id, business_key, interval, market_time, observed_at,
                    session_scope.value, adjusted, selection_policy_version,
                    selection_policy_sha256, expected_count, selected_count,
                    missing_count, failed_count, publication_status, input_sha256,
                    output_sha256, publication_status,
                ),
            )
            publication = cursor.fetchone()
            if publication is None:
                cursor.execute(
                    "SELECT * FROM equity_bar_publications WHERE business_key = %s",
                    (business_key,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    raise RuntimeError("canonical bar publication conflict was not readable")
                return dict(existing)

            execute_values(
                cursor,
                """
                INSERT INTO equity_bar_publication_members (
                    publication_id, security_id, ticker, status,
                    selected_bar_revision_id, reason_codes
                ) VALUES %s
                ON CONFLICT (publication_id, ticker) DO NOTHING
                """,
                member_values,
            )
            if publication_status in ("COMPLETE", "DEGRADED") and selected_bars:
                published_at = publication["published_at"]
                execute_values(
                    cursor,
                    """
                    INSERT INTO equity_current_bar_projection (
                        ticker, interval, bar_start, bar_end, session_scope, adjusted,
                        selected_bar_revision_id, publication_id,
                        selection_policy_version, selection_policy_sha256,
                        observed_at, published_at
                    ) VALUES %s
                    ON CONFLICT (ticker, interval, bar_start, session_scope, adjusted)
                    DO UPDATE SET
                        bar_end = EXCLUDED.bar_end,
                        selected_bar_revision_id = EXCLUDED.selected_bar_revision_id,
                        publication_id = EXCLUDED.publication_id,
                        selection_policy_version = EXCLUDED.selection_policy_version,
                        selection_policy_sha256 = EXCLUDED.selection_policy_sha256,
                        observed_at = EXCLUDED.observed_at,
                        published_at = EXCLUDED.published_at
                    WHERE equity_current_bar_projection.observed_at <= EXCLUDED.observed_at
                    """,
                    [
                        (
                            bar.ticker, bar.interval, bar.bar_start, bar.bar_end,
                            bar.session_scope.value, bar.adjusted, bar.bar_revision_id,
                            publication_id, selection_policy_version,
                            selection_policy_sha256, observed_at, published_at,
                        )
                        for bar in selected_bars
                    ],
                )
            return dict(publication)


class EquityIngestionRepository(_Repository):
    def fail_stale_segments(
        self,
        *,
        stale_after: timedelta,
        reason: str = "INGESTION_SEGMENT_STALE",
    ) -> tuple[dict[str, Any], ...]:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH stale_segments AS (
                    SELECT ingestion_segment_id
                    FROM equity_ingestion_segments
                    WHERE status = 'WRITING'
                      AND created_at < NOW() - %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE equity_ingestion_segments AS segment
                SET status = 'FAILED',
                    gap_details = segment.gap_details || jsonb_build_object(
                        'recovery_reason', %s
                    ),
                    completed_at = NOW()
                FROM stale_segments
                WHERE segment.ingestion_segment_id = stale_segments.ingestion_segment_id
                RETURNING segment.ingestion_segment_id, segment.dataset, segment.interval
                """,
                (stale_after, reason),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def start_segment(
        self,
        *,
        ingestion_segment_id: UUID,
        provider: str,
        provider_mode: str,
        dataset: str,
        interval: str | None,
        requested_from,
        requested_to,
        observed_at,
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_ingestion_segments (
                    ingestion_segment_id, provider, provider_mode, dataset,
                    interval, requested_from, requested_to, observed_at, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'WRITING')
                ON CONFLICT (ingestion_segment_id) DO NOTHING
                """,
                (
                    ingestion_segment_id, provider, provider_mode, dataset,
                    interval, requested_from, requested_to, observed_at,
                ),
            )

    def complete_segment(
        self,
        ingestion_segment_id: UUID,
        *,
        status: str,
        requested_from=None,
        market_watermark,
        record_count: int,
        byte_count: int = 0,
        checksum_sha256: str | None = None,
        archive_uri: str | None = None,
        gap_details: dict[str, Any] | None = None,
        completed_at,
    ) -> None:
        if status not in ("COMPLETE", "DEGRADED", "FAILED", "QUARANTINED"):
            raise ValueError("invalid terminal segment status")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE equity_ingestion_segments
                SET requested_from = COALESCE(%s, requested_from),
                    market_watermark = %s,
                    status = %s,
                    record_count = %s,
                    byte_count = %s,
                    checksum_sha256 = %s,
                    archive_uri = %s,
                    gap_details = %s,
                    completed_at = %s
                WHERE ingestion_segment_id = %s
                """,
                (
                    requested_from, market_watermark, status, record_count, byte_count,
                    checksum_sha256, archive_uri, Json(gap_details or {}),
                    completed_at, ingestion_segment_id,
                ),
            )


class EquityEvidenceRepository(_Repository):
    def persist(self, evidence: Sequence[EquityEvidence]) -> int:
        if not evidence:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_evidence (
                    evidence_id, evidence_key, lifecycle_key, evidence_type,
                    evidence_role, security_id, ticker, interval, direction,
                    lifecycle_status, strength, market_time, observed_at, valid_until,
                    source_name, source_version, payload_schema_version,
                    analysis_run_id, latest_bar_revision_id, security_revision_id,
                    fundamental_report_ids, source_revision_ids, quality_state,
                    quality_codes, qualification_revision_id, payload, payload_sha256
                ) VALUES %s
                ON CONFLICT (evidence_key) DO NOTHING
                RETURNING evidence_id
                """,
                [
                    (
                        row.evidence_id, row.evidence_key, row.lifecycle_key,
                        row.evidence_type.value, row.evidence_role.value,
                        row.security_id, row.ticker, row.interval, row.direction,
                        row.lifecycle_status.value, row.strength, row.market_time,
                        row.observed_at, row.valid_until, row.source_name,
                        row.source_version, row.payload_schema_version,
                        row.analysis_run_id, row.latest_bar_revision_id,
                        row.security_revision_id, list(row.fundamental_report_ids),
                        list(row.source_revision_ids), row.quality_state.value,
                        list(row.quality_codes), row.qualification_revision_id,
                        Json(__import__("json").loads(row.payload_json)),
                        row.payload_sha256,
                    )
                    for row in evidence
                ],
                fetch=True,
            )
            return len(inserted)

    def list_as_of(
        self,
        ticker: str,
        context: DecisionWatermark,
        *,
        evidence_types: Sequence[EvidenceType] = (),
        intervals: Sequence[str] = (),
    ) -> tuple[EquityEvidence, ...]:
        type_values = [value.value for value in evidence_types]
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (COALESCE(lifecycle_key, evidence_key)) *
                FROM equity_evidence
                WHERE ticker = %s
                  AND market_time <= %s
                  AND observed_at <= %s
                  AND (valid_until IS NULL OR valid_until > %s)
                  AND (cardinality(%s::TEXT[]) = 0 OR evidence_type = ANY(%s::TEXT[]))
                  AND (cardinality(%s::TEXT[]) = 0 OR interval = ANY(%s::TEXT[]))
                ORDER BY COALESCE(lifecycle_key, evidence_key),
                         market_time DESC, observed_at DESC, created_at DESC
                """,
                (
                    ticker.upper(), context.market_time, context.observed_time,
                    context.market_time, type_values, type_values,
                    list(intervals), list(intervals),
                ),
            )
            return tuple(_evidence_from_row(row) for row in cursor.fetchall())

    def persist_context(
        self,
        snapshot: EquityContextSnapshot,
        evidence_links: Sequence[tuple[UUID, EvidenceRole]],
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_context_snapshots (
                    equity_context_snapshot_id, security_id, ticker, strategy_horizon,
                    market_time, observed_at, valid_until, status, universe_run_id,
                    security_revision_id, fundamental_snapshot_id, regime_state,
                    ema_direction, qualified_direction, direction_qualification_id,
                    direction_evidence_id, direction_horizon, direction_valid_until,
                    trigger_state, trigger_valid_until, range_forecast_id,
                    range_lower, range_upper, range_valid_until, market_cap,
                    shares_outstanding, free_float, dividend_yield, enterprise_value,
                    ebitda, operating_income, free_cash_flow, risk_levels,
                    conflict_state, stale_components, reason_codes, summary,
                    context_policy_version, context_policy_sha256
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) ON CONFLICT (
                    ticker, strategy_horizon, market_time, observed_at,
                    context_policy_sha256
                ) DO NOTHING
                """,
                _context_values(snapshot),
            )
            if evidence_links:
                execute_values(
                    cursor,
                    """
                    INSERT INTO equity_context_evidence (
                        equity_context_snapshot_id, evidence_id, evidence_role, ordinal
                    ) VALUES %s
                    ON CONFLICT (equity_context_snapshot_id, evidence_id) DO NOTHING
                    """,
                    [
                        (snapshot.equity_context_snapshot_id, evidence_id, role.value, index)
                        for index, (evidence_id, role) in enumerate(evidence_links)
                    ],
                )

    def get_context_as_of(
        self,
        ticker: str,
        strategy_horizon: str,
        context: DecisionWatermark,
        *,
        policy_sha256: str | None = None,
    ) -> EquityContextSnapshot | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM equity_context_snapshots
                WHERE ticker = %s
                  AND strategy_horizon = %s
                  AND market_time <= %s
                  AND observed_at <= %s
                  AND (valid_until IS NULL OR valid_until > %s)
                  AND (%s IS NULL OR context_policy_sha256 = %s)
                ORDER BY market_time DESC, observed_at DESC, created_at DESC
                LIMIT 1
                """,
                (
                    ticker.upper(), strategy_horizon, context.market_time,
                    context.observed_time, context.market_time,
                    policy_sha256, policy_sha256,
                ),
            )
            row = cursor.fetchone()
        return _context_from_row(row) if row else None

    def robust_qualification_ids_as_of(
        self,
        context: DecisionWatermark,
    ) -> frozenset[UUID]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (
                    source_name, source_version, interval, direction,
                    horizon_key, outcome_policy_key
                ) qualification_revision_id, qualification_state
                FROM equity_qualification_revisions
                WHERE effective_from <= %s
                  AND (effective_to IS NULL OR effective_to > %s)
                                    AND COALESCE(metrics->>'research_scope', 'EQUITY_SIGNAL') =
                                            'EQUITY_SIGNAL'
                ORDER BY source_name, source_version, interval, direction,
                         horizon_key, outcome_policy_key, effective_from DESC,
                         created_at DESC
                """,
                (context.observed_time, context.observed_time),
            )
            rows = cursor.fetchall()
        return frozenset(
            row["qualification_revision_id"]
            for row in rows
            if row["qualification_state"] == "ROBUST_PASS"
        )

    def robust_qualifications_as_of(
        self,
        context: DecisionWatermark,
        *,
        interval: str,
        horizon_key: str,
    ) -> dict[tuple[str, str, str | None, int | None], UUID]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (
                    source_name, source_version, interval, direction
                ) qualification_revision_id, source_name, source_version,
                  interval, direction, qualification_state
                FROM equity_qualification_revisions
                WHERE effective_from <= %s
                  AND (effective_to IS NULL OR effective_to > %s)
                                                                        AND COALESCE(
                                                                                        metrics->>'research_scope',
                                                                                        'EQUITY_SIGNAL'
                                                                                ) = 'EQUITY_SIGNAL'
                                    AND interval = %s
                                    AND horizon_key = %s
                                    AND outcome_policy_key =
                                            source_name || ':' || source_version || ':' || interval ||
                                            ':SIGNED:SECTOR_PRIMARY'
                ORDER BY source_name, source_version, interval, direction,
                         effective_from DESC, created_at DESC
                """,
                (
                    context.observed_time, context.observed_time,
                    interval, horizon_key,
                ),
            )
            rows = cursor.fetchall()
        return {
            (
                row["source_name"], row["source_version"],
                row["interval"], row["direction"],
            ): row["qualification_revision_id"]
            for row in rows
            if row["qualification_state"] == "ROBUST_PASS"
        }

    def upsert_current_projection(
        self,
        *,
        ticker: str,
        interval_key: str,
        projection_type: str,
        source_name: str,
        market_time,
        observed_at,
        published_at,
        payload: dict[str, Any],
        evidence_id: UUID | None = None,
        equity_context_snapshot_id: UUID | None = None,
        analysis_run_id: UUID | None = None,
    ) -> None:
        if evidence_id is None and equity_context_snapshot_id is None:
            raise ValueError("a projection requires evidence or context identity")
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_current_projection (
                    ticker, interval_key, projection_type, source_name,
                    evidence_id, equity_context_snapshot_id, analysis_run_id,
                    market_time, observed_at, published_at, payload
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ticker, interval_key, projection_type, source_name)
                DO UPDATE SET
                    evidence_id = EXCLUDED.evidence_id,
                    equity_context_snapshot_id = EXCLUDED.equity_context_snapshot_id,
                    analysis_run_id = EXCLUDED.analysis_run_id,
                    market_time = EXCLUDED.market_time,
                    observed_at = EXCLUDED.observed_at,
                    published_at = EXCLUDED.published_at,
                    payload = EXCLUDED.payload
                WHERE equity_current_projection.market_time <= EXCLUDED.market_time
                  AND equity_current_projection.observed_at <= EXCLUDED.observed_at
                """,
                (
                    ticker.upper(), interval_key, projection_type, source_name,
                    evidence_id, equity_context_snapshot_id, analysis_run_id,
                    market_time, observed_at, published_at, Json(payload),
                ),
            )


class EquityUniverseRepository(_Repository):
    def persist_complete_run(
        self,
        *,
        universe_run_id: UUID,
        source: str,
        mode: str,
        effective_from,
        observed_at,
        policy_version: str,
        policy_sha256: str,
        members: Sequence[SecurityReferenceRevision],
        configuration: dict[str, Any],
        availability_mode: BarAvailabilityMode = BarAvailabilityMode.LIVE_OBSERVED,
        replay_available_at=None,
        source_request_sha256: str | None = None,
        member_metadata: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        if (
            availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            and replay_available_at is None
        ):
            raise ValueError("reconstructed universe requires replay_available_at")
        if (
            availability_mode is BarAvailabilityMode.LIVE_OBSERVED
            and replay_available_at is not None
        ):
            raise ValueError("live universe cannot set replay_available_at")
        metadata = member_metadata or {}
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_universe_runs (
                    universe_run_id, source, mode, effective_from, observed_at,
                    expected_members, admitted_members, status, policy_version,
                    policy_sha256, configuration, completed_at,
                    availability_mode, replay_available_at, source_request_sha256
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'COMPLETE',%s,%s,%s,NOW(),%s,%s,%s)
                ON CONFLICT (universe_run_id) DO NOTHING
                """,
                (
                    universe_run_id, source, mode, effective_from, observed_at,
                    len(members), len(members), policy_version, policy_sha256,
                    Json(configuration), availability_mode.value,
                    replay_available_at, source_request_sha256,
                ),
            )
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_universe_members (
                    universe_run_id, security_id, security_revision_id, ticker,
                    member_rank, score, effective_from, first_observed_at, reasons
                ) VALUES %s
                ON CONFLICT (universe_run_id, security_id) DO NOTHING
                """,
                [
                    (
                        universe_run_id, row.security_id, row.security_revision_id,
                        row.ticker, rank, metadata.get(row.ticker, {}).get("score"),
                        effective_from, observed_at,
                        list(metadata.get(row.ticker, {}).get("reasons", ())),
                    )
                    for rank, row in enumerate(members, start=1)
                ],
            )

    def get_latest_as_of(
        self,
        context: DecisionWatermark,
    ) -> dict[str, Any] | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM equity_universe_runs
                WHERE effective_from <= %s
                  AND observed_at <= %s
                                    AND availability_mode = 'LIVE_OBSERVED'
                  AND status IN ('COMPLETE', 'DEGRADED')
                ORDER BY effective_from DESC, observed_at DESC
                LIMIT 1
                """,
                (context.market_time, context.observed_time),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def get_latest_for_replay(
        self,
        context: DecisionWatermark,
    ) -> dict[str, Any] | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM equity_universe_runs
                WHERE effective_from <= %s
                  AND COALESCE(replay_available_at, observed_at) <= %s
                  AND status IN ('COMPLETE', 'DEGRADED')
                ORDER BY effective_from DESC,
                         COALESCE(replay_available_at, observed_at) DESC,
                         observed_at DESC
                LIMIT 1
                """,
                (context.market_time, context.observed_time),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def get_reconstructed_session(
        self,
        *,
        policy_sha256: str,
        effective_from,
    ) -> dict[str, Any] | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM equity_universe_runs
                WHERE policy_sha256 = %s
                  AND effective_from = %s
                  AND availability_mode = 'HISTORICAL_RECONSTRUCTED'
                  AND status IN ('COMPLETE', 'DEGRADED')
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (policy_sha256, effective_from),
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def member_tickers(self, universe_run_id: UUID) -> frozenset[str]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT ticker FROM equity_universe_members
                WHERE universe_run_id = %s
                """,
                (universe_run_id,),
            )
            return frozenset(row["ticker"] for row in cursor.fetchall())

    def members_for_replay(
        self,
        universe_run_id: UUID,
        tickers: Sequence[str] = (),
    ) -> tuple[SecurityReferenceRevision, ...]:
        normalized_tickers = sorted({ticker.upper() for ticker in tickers})
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT reference.*
                FROM equity_universe_members member
                                JOIN LATERAL (
                                        SELECT candidate.*
                                        FROM equity_security_reference_revisions AS candidate
                                        WHERE candidate.security_id = member.security_id
                                            AND candidate.ticker = member.ticker
                                            AND candidate.effective_from <= member.effective_from
                                        ORDER BY (candidate.sector IS NOT NULL) DESC,
                                                         candidate.effective_from DESC,
                                                         candidate.observed_at DESC
                                        LIMIT 1
                                ) AS reference ON TRUE
                WHERE member.universe_run_id = %s
                                  AND (cardinality(%s::TEXT[]) = 0
                                       OR member.ticker = ANY(%s::TEXT[]))
                ORDER BY member.member_rank, member.ticker
                """,
                                (universe_run_id, normalized_tickers, normalized_tickers),
            )
            return tuple(_security_from_row(row) for row in cursor.fetchall())


class EquityAnalysisRepository(_Repository):
    def latest_published_market_times(
        self,
        intervals: Sequence[str],
        *,
        run_purpose: str = "ORIGINAL",
    ) -> dict[str, Any]:
        normalized = tuple(dict.fromkeys(intervals))
        if not normalized:
            return {}
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (interval) interval, market_time
                FROM equity_analysis_runs
                WHERE interval = ANY(%s)
                  AND run_purpose = %s
                  AND status IN ('COMPLETE', 'DEGRADED')
                  AND published_at IS NOT NULL
                ORDER BY interval, market_time DESC, observed_at DESC
                """,
                (list(normalized), run_purpose),
            )
            return {
                row["interval"]: row["market_time"] for row in cursor.fetchall()
            }

    def fail_stale_runs(
        self,
        *,
        stale_after: timedelta,
        reason: str = "ANALYSIS_RUN_LEASE_EXPIRED",
    ) -> tuple[dict[str, Any], ...]:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT analysis_run_id
                FROM equity_analysis_runs
                WHERE status = 'RUNNING'
                  AND created_at < NOW() - %s
                FOR UPDATE SKIP LOCKED
                """,
                (stale_after,),
            )
            stale_run_ids = tuple(row["analysis_run_id"] for row in cursor.fetchall())
            if not stale_run_ids:
                return ()
            cursor.execute(
                """
                UPDATE equity_analysis_members AS member
                SET status = 'FAILED',
                    failure_reason = %s,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    completed_at = NOW()
                WHERE member.analysis_run_id = ANY(%s)
                  AND member.status IN ('PENDING', 'CLAIMED')
                """,
                (reason, list(stale_run_ids)),
            )
            cursor.execute(
                """
                UPDATE equity_analysis_runs AS run
                SET completed_members = counts.complete,
                    no_match_members = counts.no_match,
                    insufficient_members = counts.insufficient,
                    failed_members = counts.failed,
                    status = 'FAILED',
                    completed_at = NOW(),
                    published_at = NULL
                FROM (
                    SELECT
                        analysis_run_id,
                        COUNT(*) FILTER (WHERE status = 'COMPLETE') AS complete,
                        COUNT(*) FILTER (WHERE status = 'NO_MATCH') AS no_match,
                        COUNT(*) FILTER (WHERE status = 'INSUFFICIENT_DATA') AS insufficient,
                        COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
                    FROM equity_analysis_members
                    GROUP BY analysis_run_id
                ) AS counts
                WHERE run.analysis_run_id = counts.analysis_run_id
                                    AND run.analysis_run_id = ANY(%s)
                RETURNING run.analysis_run_id, run.business_key, run.failed_members
                """,
                                (list(stale_run_ids),),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def start_run(
        self,
        *,
        analysis_run_id: UUID,
        business_key: str,
        run_purpose: str,
        interval: str,
        market_time,
        observed_at,
        universe_run_id: UUID,
        model_bundle_version: str,
        model_bundle_sha256: str,
        input_sha256: str,
        members: Sequence[SecurityReferenceRevision],
    ) -> dict[str, Any]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_analysis_runs (
                    analysis_run_id, business_key, run_purpose, interval,
                    market_time, observed_at, universe_run_id,
                    model_bundle_version, model_bundle_sha256, expected_members,
                    input_sha256, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING')
                ON CONFLICT (business_key) DO NOTHING
                RETURNING *
                """,
                (
                    analysis_run_id, business_key, run_purpose, interval,
                    market_time, observed_at, universe_run_id,
                    model_bundle_version, model_bundle_sha256, len(members),
                    input_sha256,
                ),
            )
            inserted = cursor.fetchone()
            if inserted is None:
                cursor.execute(
                    "SELECT * FROM equity_analysis_runs WHERE business_key = %s",
                    (business_key,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    raise RuntimeError("analysis run conflict was not readable")
                run = dict(existing)
                run["was_created"] = False
                return run
            run = dict(inserted)
            run["was_created"] = True
            execute_values(
                cursor,
                """
                INSERT INTO equity_analysis_members (
                    analysis_run_id, security_id, ticker, status
                ) VALUES %s
                ON CONFLICT (analysis_run_id, security_id) DO NOTHING
                """,
                [
                    (analysis_run_id, row.security_id, row.ticker, "PENDING")
                    for row in members
                ],
            )
            return run

    def complete_member(
        self,
        analysis_run_id: UUID,
        security_id: UUID,
        *,
        status: str,
        latest_bar_revision_id: UUID | None,
        source_bar_count: int,
        evidence_count: int,
        failure_reason: str | None = None,
    ) -> None:
        if status not in ("COMPLETE", "NO_MATCH", "INSUFFICIENT_DATA", "FAILED"):
            raise ValueError("invalid terminal member status")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE equity_analysis_members
                SET status = %s,
                    latest_bar_revision_id = %s,
                    source_bar_count = %s,
                    evidence_count = %s,
                    failure_reason = %s,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    completed_at = NOW()
                WHERE analysis_run_id = %s AND security_id = %s
                """,
                (
                    status, latest_bar_revision_id, source_bar_count,
                    evidence_count, failure_reason, analysis_run_id, security_id,
                ),
            )

    def publish_run(
        self,
        analysis_run_id: UUID,
        *,
        output_sha256: str | None = None,
        projections: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH counts AS (
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'COMPLETE') AS complete,
                        COUNT(*) FILTER (WHERE status = 'NO_MATCH') AS no_match,
                        COUNT(*) FILTER (WHERE status = 'INSUFFICIENT_DATA') AS insufficient,
                        COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
                        COUNT(*) FILTER (WHERE status IN ('PENDING', 'CLAIMED')) AS unresolved,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'NO_MATCH')) AS usable
                    FROM equity_analysis_members
                    WHERE analysis_run_id = %s
                ), updated AS (
                    UPDATE equity_analysis_runs AS run
                    SET completed_members = counts.complete,
                        no_match_members = counts.no_match,
                        insufficient_members = counts.insufficient,
                        failed_members = counts.failed,
                        output_sha256 = %s,
                        status = CASE
                            WHEN counts.total = 0 OR counts.unresolved > 0
                                OR counts.usable::double precision / counts.total < 0.90
                                THEN 'FAILED'
                            WHEN counts.failed > 0
                                OR counts.usable::double precision / counts.total < 0.95
                                THEN 'DEGRADED'
                            ELSE 'COMPLETE'
                        END,
                        completed_at = NOW(),
                        published_at = CASE
                            WHEN counts.total > 0 AND counts.unresolved = 0
                                AND counts.usable::double precision / counts.total >= 0.90
                                THEN NOW()
                            ELSE NULL
                        END
                    FROM counts
                    WHERE run.analysis_run_id = %s
                    RETURNING run.*
                ) SELECT * FROM updated
                """,
                (analysis_run_id, output_sha256, analysis_run_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("analysis run was not found")
            run = dict(row)
            if (
                run.get("run_purpose") == "ORIGINAL"
                and run["status"] in ("COMPLETE", "DEGRADED")
                and run["published_at"] is not None
            ):
                cursor.execute(
                    """
                    DELETE FROM equity_current_projection AS projection
                    WHERE projection.interval_key = %s
                      AND projection.ticker IN (
                          SELECT member.ticker
                          FROM equity_analysis_members AS member
                          WHERE member.analysis_run_id = %s
                      )
                    """,
                    (run["interval"], analysis_run_id),
                )
                if projections:
                    cursor.executemany(
                        """
                        INSERT INTO equity_current_projection (
                            ticker, interval_key, projection_type, source_name,
                            evidence_id, equity_context_snapshot_id, analysis_run_id,
                            market_time, observed_at, published_at, payload
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (ticker, interval_key, projection_type, source_name)
                        DO UPDATE SET
                            evidence_id = EXCLUDED.evidence_id,
                            equity_context_snapshot_id = EXCLUDED.equity_context_snapshot_id,
                            analysis_run_id = EXCLUDED.analysis_run_id,
                            market_time = EXCLUDED.market_time,
                            observed_at = EXCLUDED.observed_at,
                            published_at = EXCLUDED.published_at,
                            payload = EXCLUDED.payload
                        WHERE equity_current_projection.market_time <= EXCLUDED.market_time
                          AND equity_current_projection.observed_at <= EXCLUDED.observed_at
                        """,
                        [
                            (
                                projection["ticker"].upper(),
                                projection["interval_key"],
                                projection["projection_type"],
                                projection["source_name"],
                                projection.get("evidence_id"),
                                projection.get("equity_context_snapshot_id"),
                                analysis_run_id,
                                projection["market_time"],
                                projection["observed_at"],
                                run["published_at"],
                                Json(projection["payload"]),
                            )
                            for projection in projections
                        ],
                    )
            return run


class EquityOutcomeRepository(_Repository):
    def persist_policy(self, policy: OutcomePolicy) -> None:
        import json

        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO equity_outcome_policies (
                    outcome_policy_id, policy_key, policy_version, evidence_type,
                    source_name, source_version, interval, direction_contract,
                    eligibility_transition, entry_model, horizons, cost_model,
                    benchmark_policy, ambiguity_policy, success_definition,
                    missingness_policy, independence_policy, effective_from,
                    effective_to, policy_sha256
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) ON CONFLICT (policy_key, policy_version) DO NOTHING
                """,
                (
                    policy.outcome_policy_id, policy.policy_key, policy.policy_version,
                    policy.evidence_type, policy.source_name, policy.source_version,
                    policy.interval, policy.direction_contract,
                    policy.eligibility_transition, policy.entry_model,
                    Json(json.loads(policy.horizons_json)),
                    Json(json.loads(policy.cost_model_json)),
                    Json(json.loads(policy.benchmark_policy_json)),
                    policy.ambiguity_policy,
                    Json(json.loads(policy.success_definition_json)),
                    Json(json.loads(policy.missingness_policy_json)),
                    Json(json.loads(policy.independence_policy_json)),
                    policy.effective_from, policy.effective_to, policy.policy_sha256,
                ),
            )

    def persist_outcomes(self, outcomes: Sequence[ResearchOutcome]) -> int:
        if not outcomes:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_research_outcomes (
                    outcome_id, subject_evidence_id, outcome_policy_id, horizon_key,
                    outcome_revision, entry_status, signal_time,
                    confirmation_bar_id, confirmation_bar_end, entry_bar_id,
                    entry_time, entry_price, exit_bar_id, exit_time, exit_price,
                    gross_return, signed_return, estimated_cost, net_return,
                    market_benchmark_ticker, market_return,
                    sector_benchmark_ticker, sector_return,
                    net_alpha, sector_net_alpha,
                    mae_pct, mfe_pct, mae_r, mfe_r, stop_hit, target_hit, first_hit,
                    outcome_category, is_stale, outcome_available_at, quality_codes,
                    path_bar_ids, benchmark_bar_ids, supersedes_outcome_id
                ) VALUES %s
                ON CONFLICT (
                    subject_evidence_id, outcome_policy_id, horizon_key, outcome_revision
                ) DO NOTHING
                RETURNING outcome_id
                """,
                [
                    (
                        row.outcome_id, row.subject_evidence_id, row.outcome_policy_id,
                        row.horizon_key, row.outcome_revision, row.entry_status,
                        row.signal_time, row.confirmation_bar_id,
                        row.confirmation_bar_end, row.entry_bar_id, row.entry_time,
                        row.entry_price, row.exit_bar_id, row.exit_time, row.exit_price,
                        row.gross_return, row.signed_return, row.estimated_cost,
                        row.net_return, row.market_benchmark_ticker,
                        row.market_return, row.sector_benchmark_ticker,
                        row.sector_return,
                        row.net_alpha, row.sector_net_alpha, row.mae_pct, row.mfe_pct,
                        row.mae_r, row.mfe_r, row.stop_hit, row.target_hit,
                        row.first_hit, row.outcome_category, row.is_stale,
                        row.outcome_available_at, list(row.quality_codes),
                        list(row.path_bar_ids), list(row.benchmark_bar_ids),
                        row.supersedes_outcome_id,
                    )
                    for row in outcomes
                ],
                fetch=True,
            )
            return len(inserted)

    def list_pending_directional_subjects(
        self,
        policy: OutcomePolicy,
        horizon_key: str,
        *,
        available_by,
        signal_observed_through=None,
        prospective_only: bool = False,
        subject_evidence_ids: Sequence[UUID] = (),
        limit: int = 5000,
    ) -> tuple[EquityEvidence, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        subject_cte = (
            "WITH requested AS ("
            "SELECT evidence_id, ordinal FROM "
            "UNNEST(%s::UUID[]) WITH ORDINALITY "
            "AS requested(evidence_id, ordinal))"
            if subject_evidence_ids else ""
        )
        subject_join = (
            "JOIN requested ON requested.evidence_id = evidence.evidence_id"
            if subject_evidence_ids else ""
        )
        order_by = (
            "requested.ordinal"
            if subject_evidence_ids else
            "evidence.observed_at ASC, evidence.evidence_id ASC"
        )
        parameters = []
        if subject_evidence_ids:
            parameters.append(list(subject_evidence_ids))
        parameters.extend((
            policy.evidence_type, policy.source_name, policy.source_version,
            policy.interval, policy.effective_from,
            policy.effective_to, policy.effective_to, available_by,
            prospective_only,
            signal_observed_through, signal_observed_through,
        ))
        parameters.extend((policy.outcome_policy_id, horizon_key, limit))
        with self._cursor() as cursor:
            cursor.execute(
                f"""
                {subject_cte}
                SELECT evidence.*
                FROM equity_evidence AS evidence
                {subject_join}
                WHERE evidence.evidence_type = %s
                  AND evidence.direction IN (-1, 1)
                  AND evidence.source_name = %s
                  AND evidence.source_version = %s
                  AND evidence.interval = %s
                  AND evidence.observed_at >= %s
                  AND (%s IS NULL OR evidence.observed_at < %s)
                  AND evidence.observed_at <= %s
                  AND (
                      %s = FALSE OR EXISTS (
                          SELECT 1
                          FROM equity_analysis_runs AS analysis
                          WHERE analysis.analysis_run_id = evidence.analysis_run_id
                            AND analysis.run_purpose = 'ORIGINAL'
                      )
                  )
                  AND (%s IS NULL OR evidence.observed_at <= %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM equity_research_outcomes AS outcome
                      WHERE outcome.subject_evidence_id = evidence.evidence_id
                        AND outcome.outcome_policy_id = %s
                        AND outcome.horizon_key = %s
                    AND outcome.is_stale = FALSE
                  )
                ORDER BY {order_by}
                LIMIT %s
                """,
                tuple(parameters),
            )
            return tuple(_evidence_from_row(row) for row in cursor.fetchall())

    def outcome_revision_context(
        self,
        subject_evidence_ids: Sequence[UUID],
        policy: OutcomePolicy,
        horizon_key: str,
    ) -> dict[UUID, tuple[int, UUID | None]]:
        if not subject_evidence_ids:
            return {}
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (subject_evidence_id)
                       subject_evidence_id, outcome_id, outcome_revision
                FROM equity_research_outcomes
                WHERE subject_evidence_id = ANY(%s::UUID[])
                  AND outcome_policy_id = %s
                  AND horizon_key = %s
                ORDER BY subject_evidence_id, outcome_revision DESC,
                         created_at DESC
                """,
                (list(subject_evidence_ids), policy.outcome_policy_id, horizon_key),
            )
            return {
                row["subject_evidence_id"]: (
                    int(row["outcome_revision"]) + 1,
                    row["outcome_id"],
                )
                for row in cursor.fetchall()
            }

    def mark_outcomes_stale(
        self,
        subject_evidence_ids: Sequence[UUID],
        reason: str,
    ) -> int:
        if not subject_evidence_ids:
            return 0
        if not reason.strip():
            raise ValueError("stale outcome reason cannot be blank")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE equity_research_outcomes
                SET is_stale = TRUE,
                    quality_codes = CASE
                        WHEN %s = ANY(quality_codes) THEN quality_codes
                        ELSE array_append(quality_codes, %s)
                    END
                WHERE subject_evidence_id = ANY(%s::UUID[])
                  AND is_stale = FALSE
                """,
                (reason, reason, list(subject_evidence_ids)),
            )
            return cursor.rowcount

    def qualification_observations(
        self,
        *,
        available_by,
        interval: str | None = None,
        source_names: Sequence[str] = (),
        subject_evidence_ids: Sequence[UUID] = (),
        outcome_policy_keys: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                                WITH latest AS (
                                        SELECT DISTINCT ON (
                                            outcome.subject_evidence_id,
                                            outcome.outcome_policy_id,
                                            outcome.horizon_key
                                        ) evidence.ticker, evidence.source_name,
                                            evidence.source_version, evidence.interval,
                                            evidence.direction, outcome.horizon_key,
                                            (policy.horizons ->> outcome.horizon_key)::INTEGER AS horizon_bars,
                                            policy.policy_key, evidence.observed_at AS signal_time,
                                            outcome.outcome_id,
                                            outcome.subject_evidence_id,
                                            outcome.net_return, outcome.net_alpha,
                                            outcome.sector_net_alpha,
                                            COALESCE(
                                                policy.benchmark_policy->>'primary',
                                                'MARKET'
                                            ) AS primary_benchmark,
                                            outcome.mae_pct, outcome.mfe_pct,
                                            outcome.stop_hit, outcome.target_hit,
                                            outcome.first_hit,
                                            (evidence.payload ? 'stop_price'
                                             AND evidence.payload ? 'target_price') AS has_bracket
                                        FROM equity_research_outcomes AS outcome
                                        JOIN equity_evidence AS evidence
                                            ON evidence.evidence_id = outcome.subject_evidence_id
                                        JOIN equity_outcome_policies AS policy
                                            ON policy.outcome_policy_id = outcome.outcome_policy_id
                                        WHERE outcome.entry_status = 'ENTERED'
                                            AND outcome.is_stale = FALSE
                                            AND outcome.outcome_available_at <= %s
                                            AND (%s IS NULL OR evidence.interval = %s)
                                            AND (cardinality(%s::UUID[]) = 0
                                                     OR outcome.subject_evidence_id = ANY(%s::UUID[]))
                                            AND (cardinality(%s::TEXT[]) = 0
                                                     OR evidence.source_name = ANY(%s::TEXT[]))
                                            AND (cardinality(%s::TEXT[]) = 0
                                                     OR policy.policy_key = ANY(%s::TEXT[]))
                                        ORDER BY outcome.subject_evidence_id,
                                            outcome.outcome_policy_id, outcome.horizon_key,
                                            outcome.outcome_revision DESC,
                                            policy.created_at DESC, outcome.created_at DESC
                                )
                                SELECT * FROM latest ORDER BY signal_time, ticker
                """,
                (
                    available_by, interval, interval,
                    list(subject_evidence_ids), list(subject_evidence_ids),
                    list(source_names), list(source_names),
                    list(outcome_policy_keys), list(outcome_policy_keys),
                ),
            )
            return [dict(row) for row in cursor.fetchall()]

    def persist_qualification_revisions(
        self,
        revisions: Sequence[QualificationRevision],
    ) -> int:
        if not revisions:
            return 0
        import json

        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO equity_qualification_revisions (
                    qualification_revision_id, source_name, source_version,
                    interval, direction, horizon_key, outcome_policy_key,
                    evaluation_version, qualification_state, effective_from,
                    sample_size, independent_periods, mean_net_alpha,
                    alpha_t_stat, alpha_fdr_q, calibrated_probability,
                    probability_ci_low, probability_ci_high, brier_score,
                    brier_skill_score, expected_calibration_error,
                    report_identity, metrics
                ) VALUES %s
                ON CONFLICT (
                    source_name, source_version, interval, direction, horizon_key,
                    outcome_policy_key, evaluation_version, effective_from
                ) DO NOTHING
                RETURNING qualification_revision_id
                """,
                [
                    (
                        row.qualification_revision_id, row.source_name,
                        row.source_version, row.interval, row.direction,
                        row.horizon_key, row.outcome_policy_key,
                        row.evaluation_version, row.qualification_state,
                        row.effective_from, row.sample_size,
                        row.independent_periods, row.mean_net_alpha,
                        row.alpha_t_stat, row.alpha_fdr_q,
                        row.calibrated_probability, row.probability_ci_low,
                        row.probability_ci_high, row.brier_score,
                        row.brier_skill_score, row.expected_calibration_error,
                        row.report_identity,
                        Json(json.loads(row.metrics_json)),
                    )
                    for row in revisions
                ],
                fetch=True,
            )
            return len(inserted)


def _security_from_row(row: dict[str, Any]) -> SecurityReferenceRevision:
    import json

    return SecurityReferenceRevision(
        security_revision_id=row["security_revision_id"],
        security_id=row["security_id"],
        ticker=row["ticker"],
        active=row["active"],
        company_name=row.get("company_name"),
        security_type=row.get("security_type"),
        cik=row.get("cik"),
        composite_figi=row.get("composite_figi"),
        share_class_figi=row.get("share_class_figi"),
        primary_exchange=row.get("primary_exchange"),
        sic_code=row.get("sic_code"),
        sic_description=row.get("sic_description"),
        sector=row.get("sector"),
        industry=row.get("industry"),
        list_date=row.get("list_date"),
        delisted_date=row.get("delisted_date"),
        weighted_shares=row.get("weighted_shares"),
        free_float=row.get("free_float"),
        free_float_percent=row.get("free_float_percent"),
        market_cap=row.get("market_cap"),
        source=row["source"],
        effective_from=row["effective_from"],
        observed_at=row["observed_at"],
        payload_sha256=row["payload_sha256"],
        raw_payload_json=json.dumps(row.get("raw_payload") or {}, sort_keys=True),
    )


def _bar_from_row(row: dict[str, Any]) -> EquityBarRevision:
    return EquityBarRevision(
        bar_revision_id=row["bar_revision_id"], security_id=row["security_id"],
        ticker=row["ticker"], interval=row["interval"], session_date=row["session_date"],
        bar_start=row["bar_start"], bar_end=row["bar_end"],
        open_price=row["open_price"], high_price=row["high_price"],
        low_price=row["low_price"], close_price=row["close_price"],
        volume=row["volume"], vwap=row.get("vwap"),
        transaction_count=row.get("transaction_count"),
        source_kind=BarSourceKind(row["source_kind"]),
        availability_mode=BarAvailabilityMode(row["availability_mode"]),
        is_final=row["is_final"], system_observed_at=row["system_observed_at"],
        replay_available_at=row.get("replay_available_at"), adjusted=row["adjusted"],
        payload_sha256=row["payload_sha256"],
        quality_codes=tuple(row.get("quality_codes") or ()),
        provider_published_at=row.get("provider_published_at"),
        ingestion_segment_id=row.get("ingestion_segment_id"),
        source_bar_revision_ids=tuple(row.get("source_bar_revision_ids") or ()),
        supersedes_bar_revision_id=row.get("supersedes_bar_revision_id"),
        reconciliation_status=row.get("reconciliation_status"),
        session_scope=BarSessionScope(row.get("session_scope") or "RTH"),
    )


def _evidence_from_row(row: dict[str, Any]) -> EquityEvidence:
    import json

    return EquityEvidence(
        evidence_id=row["evidence_id"], evidence_key=row["evidence_key"],
        lifecycle_key=row.get("lifecycle_key"),
        evidence_type=EvidenceType(row["evidence_type"]),
        evidence_role=EvidenceRole(row["evidence_role"]), security_id=row["security_id"],
        ticker=row["ticker"], interval=row.get("interval"), direction=row.get("direction"),
        lifecycle_status=LifecycleStatus(row["lifecycle_status"]),
        strength=row.get("strength"), market_time=row["market_time"],
        observed_at=row["observed_at"], valid_until=row.get("valid_until"),
        source_name=row["source_name"], source_version=row["source_version"],
        payload_schema_version=row["payload_schema_version"],
        analysis_run_id=row.get("analysis_run_id"),
        latest_bar_revision_id=row.get("latest_bar_revision_id"),
        security_revision_id=row.get("security_revision_id"),
        fundamental_report_ids=tuple(row.get("fundamental_report_ids") or ()),
        source_revision_ids=tuple(row.get("source_revision_ids") or ()),
        quality_state=QualityState(row["quality_state"]),
        quality_codes=tuple(row.get("quality_codes") or ()),
        qualification_revision_id=row.get("qualification_revision_id"),
        payload_json=json.dumps(row.get("payload") or {}, sort_keys=True),
        payload_sha256=row["payload_sha256"],
    )


def _context_values(row: EquityContextSnapshot) -> tuple[Any, ...]:
    import json

    return (
        row.equity_context_snapshot_id, row.security_id, row.ticker,
        row.strategy_horizon, row.market_time, row.observed_at, row.valid_until,
        row.status.value, row.universe_run_id, row.security_revision_id,
        row.fundamental_snapshot_id, row.regime_state, row.ema_direction,
        row.qualified_direction, row.direction_qualification_id,
        row.direction_evidence_id, row.direction_horizon, row.direction_valid_until,
        row.trigger_state, row.trigger_valid_until, row.range_forecast_id,
        row.range_lower, row.range_upper, row.range_valid_until, row.market_cap,
        row.shares_outstanding, row.free_float, row.dividend_yield,
        row.enterprise_value, row.ebitda, row.operating_income, row.free_cash_flow,
        Json(json.loads(row.risk_levels_json)), Json(json.loads(row.conflict_state_json)),
        Json(json.loads(row.stale_components_json)), list(row.reason_codes),
        Json(json.loads(row.summary_json)), row.context_policy_version,
        row.context_policy_sha256,
    )


def _context_from_row(row: dict[str, Any]) -> EquityContextSnapshot:
    import json

    return EquityContextSnapshot(
        equity_context_snapshot_id=row["equity_context_snapshot_id"],
        security_id=row["security_id"], ticker=row["ticker"],
        strategy_horizon=row["strategy_horizon"], market_time=row["market_time"],
        observed_at=row["observed_at"], valid_until=row.get("valid_until"),
        status=ContextStatus(row["status"]), universe_run_id=row.get("universe_run_id"),
        security_revision_id=row.get("security_revision_id"),
        fundamental_snapshot_id=row.get("fundamental_snapshot_id"),
        regime_state=row.get("regime_state"), ema_direction=row.get("ema_direction"),
        qualified_direction=row.get("qualified_direction"),
        direction_qualification_id=row.get("direction_qualification_id"),
        direction_evidence_id=row.get("direction_evidence_id"),
        direction_horizon=row.get("direction_horizon"),
        direction_valid_until=row.get("direction_valid_until"),
        trigger_state=row.get("trigger_state"),
        trigger_valid_until=row.get("trigger_valid_until"),
        range_forecast_id=row.get("range_forecast_id"),
        range_lower=row.get("range_lower"), range_upper=row.get("range_upper"),
        range_valid_until=row.get("range_valid_until"), market_cap=row.get("market_cap"),
        shares_outstanding=row.get("shares_outstanding"), free_float=row.get("free_float"),
        dividend_yield=row.get("dividend_yield"),
        enterprise_value=row.get("enterprise_value"), ebitda=row.get("ebitda"),
        operating_income=row.get("operating_income"),
        free_cash_flow=row.get("free_cash_flow"),
        risk_levels_json=json.dumps(row.get("risk_levels") or {}, sort_keys=True),
        conflict_state_json=json.dumps(row.get("conflict_state") or {}, sort_keys=True),
        stale_components_json=json.dumps(row.get("stale_components") or [], sort_keys=True),
        reason_codes=tuple(row.get("reason_codes") or ()),
        summary_json=json.dumps(row.get("summary") or {}, sort_keys=True),
        context_policy_version=row["context_policy_version"],
        context_policy_sha256=row["context_policy_sha256"],
    )


def _market_cap_group(value) -> str | None:
    if value is None:
        return None
    amount = float(value)
    if amount >= 200_000_000_000:
        return "Mega"
    if amount >= 10_000_000_000:
        return "Large"
    if amount >= 2_000_000_000:
        return "Mid"
    if amount >= 300_000_000:
        return "Small"
    return "Micro"