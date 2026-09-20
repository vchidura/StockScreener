from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any, Sequence
from uuid import UUID

from psycopg2.extras import Json, execute_values

from options.calendar import OptionExchangeCalendar
from options.domain import MarkSource
from options.outcomes import OptionDecayOutcome, OptionOutcomeLeg
from options.strategies.domain import OptionSide

from .base import ConnectionFactory, PostgresRepository


OPTION_OUTCOME_RETENTION_DAYS = 60


class OptionOutcomeRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def retained_plan_marks(self, candidate_ids, *, available_by, valuation_policy_sha256):
        ids = sorted({UUID(str(value)) for value in candidate_ids}, key=str)
        if not 1 <= len(ids) <= 200 or available_by.tzinfo is None:
            raise ValueError("history marks require 1-200 candidates and an aware cutoff")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SELECT to_regclass('public.option_signal_current_marks') IS NOT NULL AS ready")
            if not cursor.fetchone()["ready"]:
                return {"ready": False, "rows": {}}
            cursor.execute("""
                SELECT mark.*, candidate.candidate_identity, candidate.matrix_id,
                       COALESCE(evidence.legs, '[]'::jsonb) AS legs
                FROM option_signal_current_marks AS mark
                JOIN option_strategy_candidates AS candidate USING(candidate_id)
                LEFT JOIN LATERAL (
                    SELECT jsonb_agg(jsonb_build_object(
                        'contract_id', snapshot.contract_id, 'contract_ticker', snapshot.contract_ticker,
                        'side', leg.side, 'ratio', leg.ratio, 'multiplier', leg.multiplier,
                        'strike', snapshot.strike, 'expiration_date', snapshot.expiration_date,
                        'contract_type', snapshot.contract_type, 'entry_mark', leg.model_mark,
                        'entry_valuation_policy_sha256', leg.valuation_policy_sha256,
                        'snapshot_multiplier', snapshot.shares_per_contract,
                        'entry_mark_source', leg.mark_source, 'exit_mark', snapshot.model_mark,
                        'exit_mark_source', snapshot.mark_source, 'valuation_policy_sha256', snapshot.valuation_policy_sha256,
                        'snapshot_id', snapshot.snapshot_id, 'batch_id', snapshot.batch_id,
                        'source_market_time', snapshot.mark_market_data_time,
                        'source_observed_time', snapshot.first_observed_at,
                        'revised_observed_at', snapshot.revised_observed_at
                    ) ORDER BY leg.leg_index) AS legs
                    FROM option_candidate_legs AS leg
                    JOIN option_chain_snapshots AS snapshot
                      ON snapshot.contract_id=leg.contract_id AND snapshot.snapshot_id=ANY(mark.source_snapshot_ids)
                     AND snapshot.batch_id=mark.source_batch_id
                     AND snapshot.first_observed_at>=candidate.observed_time AND snapshot.first_observed_at<=mark.observed_time
                    WHERE leg.candidate_id=mark.candidate_id
                ) AS evidence ON TRUE
                WHERE mark.candidate_id=ANY(%s::uuid[]) AND mark.valuation_policy_sha256=%s
                  AND mark.observed_time<=%s AND mark.updated_at<=%s
                ORDER BY mark.candidate_id
            """, (ids, valuation_policy_sha256, available_by, available_by))
            return {"ready": True, "rows": {str(row["candidate_id"]): dict(row) for row in cursor.fetchall()}}

    def persist_decay_outcomes(
        self,
        outcomes: Sequence[OptionDecayOutcome],
    ) -> int:
        if not outcomes:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO option_signal_decay_outcomes (
                    outcome_id, event_id, candidate_id, measurement_type,
                    market_time, observed_time, mark, net_return,
                    availability_flag, quality_flags, entry_net_premium,
                    exit_net_premium, gross_pnl, estimated_cost, net_pnl,
                    capital_at_risk, valuation_policy_version,
                    valuation_policy_sha256, source_snapshot_ids, source_batch_id
                ) VALUES %s
                ON CONFLICT (
                    candidate_id, measurement_type, valuation_policy_sha256
                ) DO NOTHING
                RETURNING outcome_id
                """,
                [
                    (
                        row.outcome_id, row.event_id, row.candidate_id,
                        row.measurement_type, row.market_time, row.observed_time,
                        row.exit_net_premium, row.net_return,
                        row.availability_flag, list(row.quality_flags),
                        row.entry_net_premium, row.exit_net_premium,
                        row.gross_pnl, row.estimated_cost, row.net_pnl,
                        row.capital_at_risk, row.valuation_policy_version,
                        row.valuation_policy_sha256,
                        list(row.source_snapshot_ids), row.source_batch_id,
                    )
                    for row in outcomes
                ],
                fetch=True,
            )
            return len(inserted)

    def persist_current_marks(
        self,
        outcomes: Sequence[OptionDecayOutcome],
    ) -> int:
        if not outcomes:
            return 0
        with self._cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO option_signal_current_marks (
                    candidate_id, event_id, market_time, observed_time, mark,
                    net_return, availability_flag, quality_flags, entry_net_premium,
                    exit_net_premium, gross_pnl, estimated_cost, net_pnl,
                    capital_at_risk, valuation_policy_version,
                    valuation_policy_sha256, source_snapshot_ids, source_batch_id
                ) VALUES %s
                ON CONFLICT (candidate_id, valuation_policy_sha256) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    market_time = EXCLUDED.market_time,
                    observed_time = EXCLUDED.observed_time,
                    mark = EXCLUDED.mark,
                    net_return = EXCLUDED.net_return,
                    availability_flag = EXCLUDED.availability_flag,
                    quality_flags = EXCLUDED.quality_flags,
                    entry_net_premium = EXCLUDED.entry_net_premium,
                    exit_net_premium = EXCLUDED.exit_net_premium,
                    gross_pnl = EXCLUDED.gross_pnl,
                    estimated_cost = EXCLUDED.estimated_cost,
                    net_pnl = EXCLUDED.net_pnl,
                    capital_at_risk = EXCLUDED.capital_at_risk,
                    valuation_policy_version = EXCLUDED.valuation_policy_version,
                    source_snapshot_ids = EXCLUDED.source_snapshot_ids,
                    source_batch_id = EXCLUDED.source_batch_id,
                    updated_at = NOW()
                WHERE option_signal_current_marks.market_time < EXCLUDED.market_time
                   OR (
                       option_signal_current_marks.market_time = EXCLUDED.market_time
                       AND option_signal_current_marks.observed_time < EXCLUDED.observed_time
                   )
                """,
                [
                    (
                        row.candidate_id, row.event_id, row.market_time,
                        row.observed_time, row.exit_net_premium, row.net_return,
                        row.availability_flag, list(row.quality_flags),
                        row.entry_net_premium, row.exit_net_premium, row.gross_pnl,
                        row.estimated_cost, row.net_pnl, row.capital_at_risk,
                        row.valuation_policy_version, row.valuation_policy_sha256,
                        list(row.source_snapshot_ids), row.source_batch_id,
                    )
                    for row in outcomes
                ],
            )
            return cursor.rowcount

    def current_marks_available(self) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass('public.option_signal_current_marks') IS NOT NULL AS ready"
            )
            row = cursor.fetchone()
        return bool(row and row["ready"])

    def delete_non_causal_current_marks(self) -> int:
        with self._cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM option_signal_current_marks AS current_mark
                USING option_strategy_candidates AS candidate
                WHERE candidate.candidate_id = current_mark.candidate_id
                  AND current_mark.market_time <= candidate.market_data_time
                """
            )
            return cursor.rowcount

    def retained_leg_bounds(
        self,
        underlyer: str,
        *,
        available_by,
        retention_days: int = OPTION_OUTCOME_RETENTION_DAYS,
    ) -> dict[str, Any] | None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(leg.strike) AS minimum_strike,
                       MAX(leg.strike) AS maximum_strike,
                       MAX(leg.expiration_date) AS expiration_through,
                       COUNT(DISTINCT leg.contract_id) AS contract_count
                FROM option_strategy_candidates AS candidate
                JOIN option_candidate_legs AS leg USING (candidate_id)
                WHERE candidate.underlying = %s
                  AND candidate.status = 'SELECTED'
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
                  AND candidate.capital_at_risk > 0
                  AND candidate.market_data_time <= %s
                  AND candidate.observed_time <= %s
                  AND candidate.market_data_time >
                      %s - (%s * INTERVAL '1 day')
                  AND leg.expiration_date >=
                      (%s AT TIME ZONE 'America/New_York')::DATE
                """,
                (
                    underlyer.upper(), available_by, available_by,
                    available_by, retention_days, available_by,
                ),
            )
            row = cursor.fetchone()
        if not row or not row["contract_count"]:
            return None
        return dict(row)

    def list_pending_candidates(
        self,
        *,
        valuation_policy_sha256: str,
        available_by,
        retention_days: int = OPTION_OUTCOME_RETENTION_DAYS,
        limit: int = 1000,
        include_incomplete_packages: bool = False,
        availability_policy_sha256: str | None = None,
        calendar: OptionExchangeCalendar | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if retention_days <= 0 or limit <= 0:
            raise ValueError("retention_days and limit must be positive")
        session_clocks = []
        if include_incomplete_packages:
            if availability_policy_sha256 is None:
                raise ValueError("terminal outcome scheduling requires an exact availability policy")
            exchange = calendar or OptionExchangeCalendar()
            for session in exchange.trailing_sessions_before(
                available_by.date() + timedelta(days=1), retention_days + 1,
            ):
                if session >= (available_by - timedelta(days=retention_days)).date():
                    session_clocks.append({
                        "session_date": session.isoformat(),
                        "session_close": exchange.session_close(session).isoformat(),
                        "next_open": exchange.next_session_open(session).isoformat(),
                    })
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH session_clocks AS (
                    SELECT * FROM jsonb_to_recordset(%s::jsonb) AS clocks(
                        session_date date, session_close timestamptz, next_open timestamptz
                    )
                )
                SELECT candidate.candidate_id, candidate.candidate_identity,
                       signal.event_id,
                       candidate.market_data_time, candidate.capital_at_risk,
                       ARRAY(
                           SELECT required_leg.contract_id
                           FROM option_candidate_legs AS required_leg
                           WHERE required_leg.candidate_id = candidate.candidate_id
                           ORDER BY required_leg.leg_index
                       ) AS required_contract_ids,
                       COALESCE(
                           ARRAY_AGG(DISTINCT outcome.measurement_type) FILTER (
                               WHERE outcome.outcome_id IS NOT NULL
                           ), ARRAY[]::TEXT[]
                       ) AS completed_measurements,
                       COALESCE(
                           ARRAY_AGG(DISTINCT unavailable.measurement_type) FILTER (
                               WHERE unavailable.evidence_id IS NOT NULL
                           ), ARRAY[]::TEXT[]
                       ) AS unavailable_measurements
                FROM option_strategy_candidates AS candidate
                                LEFT JOIN session_clocks AS session
                                    ON session.session_date = (candidate.market_data_time AT TIME ZONE 'UTC')::date
                                LEFT JOIN option_signal_occurrences AS signal_occurrence
                                    ON signal_occurrence.source_candidate_id = candidate.candidate_id
                                LEFT JOIN option_signal_events AS signal
                                    ON signal.event_id = signal_occurrence.event_id
                LEFT JOIN option_signal_decay_outcomes AS outcome
                  ON outcome.candidate_id = candidate.candidate_id
                 AND outcome.valuation_policy_sha256 = %s
                                LEFT JOIN option_outcome_unavailable_evidence AS unavailable
                                    ON unavailable.candidate_id = candidate.candidate_id
                                 AND unavailable.valuation_policy_sha256 = %s
                                 AND (%s::text IS NULL OR unavailable.availability_policy_sha256 = %s)
                WHERE candidate.status = 'SELECTED'
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
                  AND candidate.capital_at_risk > 0
                  AND candidate.market_data_time <= %s
                  AND candidate.market_data_time > %s - (%s * INTERVAL '1 day')
                                    AND NOT EXISTS (
                                            SELECT 1
                                            FROM option_candidate_legs AS entry_leg
                                            WHERE entry_leg.candidate_id = candidate.candidate_id
                                                AND entry_leg.valuation_policy_sha256 IS DISTINCT FROM %s
                                    )
                                    AND (NOT %s OR EXISTS (
                                            SELECT 1 FROM (VALUES
                                                    ('15MIN', candidate.market_data_time + INTERVAL '15 minutes'),
                                                    ('30MIN', candidate.market_data_time + INTERVAL '30 minutes'),
                                                    ('60MIN', candidate.market_data_time + INTERVAL '60 minutes'),
                                                    ('CLOSE', CASE WHEN session.session_close > candidate.market_data_time
                                                                                 THEN session.session_close END),
                                                    ('NEXT_OPEN', session.next_open)
                                            ) AS due(measurement_type, checkpoint_at)
                                            WHERE due.checkpoint_at <= %s
                                                AND NOT EXISTS (
                                                        SELECT 1 FROM option_signal_decay_outcomes AS completed
                                                        WHERE completed.candidate_id = candidate.candidate_id
                                                            AND completed.measurement_type = due.measurement_type
                                                            AND completed.valuation_policy_sha256 = %s
                                                )
                                                AND NOT EXISTS (
                                                        SELECT 1 FROM option_outcome_unavailable_evidence AS terminal
                                                        WHERE terminal.candidate_id = candidate.candidate_id
                                                            AND terminal.measurement_type = due.measurement_type
                                                            AND terminal.valuation_policy_sha256 = %s
                                                            AND terminal.availability_policy_sha256 = %s
                                                )
                                    ))
                                    AND (
                                        %s
                                        OR EXISTS (
                                            SELECT 1
                                            FROM option_chain_snapshots AS snapshot
                                            JOIN option_candidate_legs AS observed_leg
                                              ON observed_leg.contract_id = snapshot.contract_id
                                            WHERE observed_leg.candidate_id = candidate.candidate_id
                                                                                            AND snapshot.underlying = candidate.underlying
                                                                                            AND snapshot.market_data_time >= candidate.market_data_time
                                              AND snapshot.model_mark IS NOT NULL
                                              AND snapshot.valuation_policy_sha256 = %s
                                              AND snapshot.first_observed_at <= %s
                                              AND snapshot.mark_market_data_time >=
                                                  candidate.market_data_time + INTERVAL '15 minutes'
                                            GROUP BY snapshot.batch_id
                                            HAVING COUNT(DISTINCT snapshot.contract_id) = (
                                                SELECT COUNT(*)
                                                FROM option_candidate_legs AS required_leg
                                                WHERE required_leg.candidate_id = candidate.candidate_id
                                            )
                                        )
                                    )
                GROUP BY candidate.candidate_id, signal.event_id
                                 HAVING %s OR COUNT(DISTINCT outcome.measurement_type)
                                         + COUNT(DISTINCT unavailable.measurement_type) < 5
                 ORDER BY CASE WHEN %s THEN candidate.market_data_time END ASC,
                        CASE WHEN NOT %s THEN COUNT(DISTINCT outcome.measurement_type)
                            + COUNT(DISTINCT unavailable.measurement_type) END,
                        CASE WHEN NOT %s THEN candidate.market_data_time END DESC,
                        candidate.candidate_id
                LIMIT %s
                """,
                (
                    Json(session_clocks),
                    valuation_policy_sha256, valuation_policy_sha256,
                    availability_policy_sha256, availability_policy_sha256,
                    available_by, available_by, retention_days,
                    valuation_policy_sha256, include_incomplete_packages, available_by,
                    valuation_policy_sha256, valuation_policy_sha256, availability_policy_sha256,
                    include_incomplete_packages,
                    valuation_policy_sha256, available_by,
                    include_incomplete_packages, include_incomplete_packages,
                    include_incomplete_packages, include_incomplete_packages, limit,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def list_current_candidates(
        self,
        *,
        valuation_policy_sha256: str,
        available_by,
        retention_days: int = OPTION_OUTCOME_RETENTION_DAYS,
        limit: int = 1000,
    ) -> tuple[dict[str, Any], ...]:
        if retention_days <= 0 or limit <= 0:
            raise ValueError("retention_days and limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                                WITH candidate_queue AS (
                                        SELECT candidate.candidate_id, signal.event_id,
                                                     candidate.market_data_time,
                                                     candidate.capital_at_risk,
                                                     current_mark.candidate_id IS NULL AS mark_missing,
                                                     ROW_NUMBER() OVER (
                                                             PARTITION BY current_mark.candidate_id IS NULL
                                                             ORDER BY
                                                                     CASE WHEN current_mark.candidate_id IS NULL
                                                                             THEN candidate.market_data_time END DESC,
                                                                     current_mark.market_time NULLS FIRST,
                                                                     candidate.market_data_time DESC,
                                                                     candidate.candidate_id
                                                     ) AS queue_rank
                                        FROM option_strategy_candidates AS candidate
                                        LEFT JOIN option_signal_occurrences AS signal_occurrence
                                            ON signal_occurrence.source_candidate_id = candidate.candidate_id
                                        LEFT JOIN option_signal_events AS signal
                                            ON signal.event_id = signal_occurrence.event_id
                                        LEFT JOIN option_signal_current_marks AS current_mark
                                            ON current_mark.candidate_id = candidate.candidate_id
                                         AND current_mark.valuation_policy_sha256 = %s
                                        WHERE candidate.status = 'SELECTED'
                                            AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
                                            AND candidate.capital_at_risk > 0
                                            AND candidate.market_data_time <= %s
                                            AND candidate.market_data_time >
                                                    %s - (%s * INTERVAL '1 day')
                                            AND NOT EXISTS (
                                                    SELECT 1
                                                    FROM option_candidate_legs AS entry_leg
                                                    WHERE entry_leg.candidate_id = candidate.candidate_id
                                                        AND entry_leg.valuation_policy_sha256
                                                            IS DISTINCT FROM %s
                                            )
                                            AND EXISTS (
                                                    SELECT 1 FROM option_candidate_legs AS leg
                                                    WHERE leg.candidate_id = candidate.candidate_id
                                                        AND leg.expiration_date >=
                                                                (%s AT TIME ZONE 'America/New_York')::DATE
                                            )
                                            AND EXISTS (
                                                    SELECT 1
                                                    FROM option_chain_snapshots AS snapshot
                                                    JOIN option_candidate_legs AS observed_leg
                                                        ON observed_leg.contract_id = snapshot.contract_id
                                                    WHERE observed_leg.candidate_id = candidate.candidate_id
                                                        AND snapshot.model_mark IS NOT NULL
                                                        AND snapshot.valuation_policy_sha256 = %s
                                                        AND snapshot.first_observed_at <= %s
                                                        AND snapshot.mark_market_data_time >
                                                                candidate.market_data_time
                                                    GROUP BY snapshot.batch_id
                                                    HAVING COUNT(DISTINCT snapshot.contract_id) = (
                                                            SELECT COUNT(*)
                                                            FROM option_candidate_legs AS required_leg
                                                            WHERE required_leg.candidate_id = candidate.candidate_id
                                                    )
                                            )
                                )
                                SELECT candidate_id, event_id, market_data_time, capital_at_risk
                                FROM candidate_queue
                                WHERE queue_rank <= %s
                                ORDER BY mark_missing DESC, queue_rank
                """,
                (
                    valuation_policy_sha256, available_by, available_by,
                    retention_days, valuation_policy_sha256, available_by,
                    valuation_policy_sha256, available_by, limit,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def checkpoint_legs(
        self,
        candidate_id: UUID,
        *,
        checkpoint_time,
        available_by,
        valuation_policy_sha256: str,
        maximum_mark_lag: timedelta = timedelta(minutes=15),
    ) -> tuple[OptionOutcomeLeg, ...]:
        if maximum_mark_lag <= timedelta(0):
            raise ValueError("maximum_mark_lag must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH candidate_legs AS (
                    SELECT candidate_id, contract_id, side, ratio, multiplier,
                              model_mark AS entry_mark,
                              mark_source AS entry_mark_source,
                              valuation_policy_sha256 AS entry_valuation_policy_sha256
                    FROM option_candidate_legs
                    WHERE candidate_id = %s
                         AND valuation_policy_sha256 = %s
                ), eligible_batches AS (
                    SELECT snapshot.batch_id,
                           MAX(snapshot.mark_market_data_time) AS market_time,
                           MAX(snapshot.first_observed_at) AS observed_time
                    FROM option_chain_snapshots AS snapshot
                    JOIN candidate_legs AS leg USING (contract_id)
                    WHERE snapshot.model_mark IS NOT NULL
                                            AND snapshot.valuation_policy_sha256 = %s
                      AND snapshot.mark_market_data_time >= %s
                      AND snapshot.mark_market_data_time <= %s + %s
                      AND snapshot.first_observed_at <= %s
                    GROUP BY snapshot.batch_id
                    HAVING COUNT(DISTINCT snapshot.contract_id) =
                           (SELECT COUNT(*) FROM candidate_legs)
                    ORDER BY market_time, observed_time, snapshot.batch_id
                    LIMIT 1
                ), selected AS (
                    SELECT DISTINCT ON (leg.contract_id)
                           leg.contract_id, leg.side, leg.ratio, leg.multiplier,
                           leg.entry_mark, snapshot.model_mark AS exit_mark,
                           leg.entry_mark_source,
                           snapshot.mark_source AS exit_mark_source,
                           leg.entry_valuation_policy_sha256,
                           snapshot.valuation_policy_sha256
                               AS exit_valuation_policy_sha256,
                           snapshot.snapshot_id, snapshot.batch_id,
                           snapshot.mark_market_data_time,
                           snapshot.first_observed_at, snapshot.revision
                    FROM candidate_legs AS leg
                    JOIN option_chain_snapshots AS snapshot USING (contract_id)
                    JOIN eligible_batches AS batch USING (batch_id)
                    WHERE snapshot.first_observed_at <= %s
                    ORDER BY leg.contract_id, snapshot.first_observed_at DESC,
                             snapshot.revision DESC, snapshot.snapshot_id
                )
                SELECT * FROM selected ORDER BY contract_id
                """,
                (
                    candidate_id, valuation_policy_sha256,
                    valuation_policy_sha256, checkpoint_time, checkpoint_time,
                    maximum_mark_lag, available_by, available_by,
                ),
            )
            rows = cursor.fetchall()
        return tuple(
            OptionOutcomeLeg(
                contract_id=int(row["contract_id"]),
                side=OptionSide(row["side"]),
                ratio=int(row["ratio"]),
                multiplier=int(row["multiplier"]),
                entry_mark=Decimal(row["entry_mark"]),
                exit_mark=Decimal(row["exit_mark"]),
                source_snapshot_id=row["snapshot_id"],
                source_batch_id=row["batch_id"],
                source_market_time=row["mark_market_data_time"],
                source_observed_time=row["first_observed_at"],
                entry_mark_source=MarkSource(row["entry_mark_source"]),
                exit_mark_source=MarkSource(row["exit_mark_source"]),
                entry_valuation_policy_sha256=row["entry_valuation_policy_sha256"],
                exit_valuation_policy_sha256=row["exit_valuation_policy_sha256"],
            )
            for row in rows
        )

    def current_mark_legs(
        self,
        candidate_id: UUID,
        *,
        available_by,
        valuation_policy_sha256: str,
    ) -> tuple[OptionOutcomeLeg, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH candidate_legs AS (
                    SELECT leg.candidate_id, leg.contract_id, leg.side, leg.ratio,
                           leg.multiplier, leg.model_mark AS entry_mark,
                           leg.mark_source AS entry_mark_source,
                           leg.valuation_policy_sha256 AS entry_valuation_policy_sha256,
                           candidate.market_data_time AS entry_market_time
                    FROM option_candidate_legs AS leg
                    JOIN option_strategy_candidates AS candidate
                      ON candidate.candidate_id = leg.candidate_id
                    WHERE leg.candidate_id = %s
                                            AND leg.valuation_policy_sha256 = %s
                ), latest_batch AS (
                    SELECT snapshot.batch_id,
                           MAX(snapshot.mark_market_data_time) AS market_time,
                           MAX(snapshot.first_observed_at) AS observed_time
                    FROM option_chain_snapshots AS snapshot
                    JOIN candidate_legs AS leg USING (contract_id)
                    WHERE snapshot.model_mark IS NOT NULL
                                            AND snapshot.valuation_policy_sha256 = %s
                      AND snapshot.first_observed_at <= %s
                      AND snapshot.mark_market_data_time > leg.entry_market_time
                    GROUP BY snapshot.batch_id
                    HAVING COUNT(DISTINCT snapshot.contract_id) =
                           (SELECT COUNT(*) FROM candidate_legs)
                    ORDER BY market_time DESC, observed_time DESC, snapshot.batch_id DESC
                    LIMIT 1
                )
                SELECT DISTINCT ON (leg.contract_id)
                       leg.contract_id, leg.side, leg.ratio, leg.multiplier,
                       leg.entry_mark, snapshot.model_mark AS exit_mark,
                       leg.entry_mark_source,
                       snapshot.mark_source AS exit_mark_source,
                       leg.entry_valuation_policy_sha256,
                       snapshot.valuation_policy_sha256
                           AS exit_valuation_policy_sha256,
                       snapshot.snapshot_id, snapshot.batch_id,
                       snapshot.mark_market_data_time, snapshot.first_observed_at,
                       snapshot.revision
                FROM candidate_legs AS leg
                JOIN option_chain_snapshots AS snapshot USING (contract_id)
                JOIN latest_batch AS batch USING (batch_id)
                WHERE snapshot.first_observed_at <= %s
                ORDER BY leg.contract_id, snapshot.first_observed_at DESC,
                         snapshot.revision DESC, snapshot.snapshot_id
                """,
                (
                    candidate_id, valuation_policy_sha256,
                    valuation_policy_sha256, available_by, available_by,
                ),
            )
            rows = cursor.fetchall()
        return tuple(
            OptionOutcomeLeg(
                contract_id=int(row["contract_id"]),
                side=OptionSide(row["side"]), ratio=int(row["ratio"]),
                multiplier=int(row["multiplier"]),
                entry_mark=Decimal(row["entry_mark"]),
                exit_mark=Decimal(row["exit_mark"]),
                source_snapshot_id=row["snapshot_id"], source_batch_id=row["batch_id"],
                source_market_time=row["mark_market_data_time"],
                source_observed_time=row["first_observed_at"],
                entry_mark_source=MarkSource(row["entry_mark_source"]),
                exit_mark_source=MarkSource(row["exit_mark_source"]),
                entry_valuation_policy_sha256=row["entry_valuation_policy_sha256"],
                exit_valuation_policy_sha256=row["exit_valuation_policy_sha256"],
            )
            for row in rows
        )