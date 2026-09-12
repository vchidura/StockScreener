from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, execute_values

from options.strategies.gates import GATE_LEDGER_VERSION
from .base import ConnectionFactory, PostgresRepository


BOARD_SELECTOR_VERSION = "option_board_selector_v1"
BOARD_SELECTOR_POLICY = {
    "candidate_status": "SELECTED",
    "causal_cutoff": "PUBLICATION_OBSERVED_TIME",
    "matrix_scope": "COMPLETE_CONFIGURED_UNIVERSE_SCHEDULED_CYCLE",
    "maximum_position_per_lane": 3,
    "prior_contract_exclusion": "GLOBAL_ACTIVE_STRATEGY_POLICY_FIRST_SELECTED_MATRIX",
    "rank_partition": ["underlying", "strategy_name", "candidate_kind"],
    "rank_order": ["candidate_rank", "candidate_id"],
}
BOARD_SELECTOR_SHA256 = hashlib.sha256(
    json.dumps(
        BOARD_SELECTOR_POLICY,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
).hexdigest()


@dataclass(frozen=True, slots=True)
class BoardPublicationResult:
    status: str
    publication_id: UUID | None
    expected_underlying_count: int
    covered_underlying_count: int
    member_count: int
    selection_evidence: Mapping[str, object]


def rank_board_members(
    rows: Sequence[Mapping[str, Any]],
    *,
    maximum_position: int = 3,
) -> tuple[dict[str, Any], ...]:
    if maximum_position <= 0:
        raise ValueError("maximum_position must be positive")
    eligible = [row for row in rows if not row["prior_contract_excluded"]]
    eligible.sort(
        key=lambda row: (
            row["underlying"],
            row["strategy_name"],
            row["candidate_kind"],
            int(row["candidate_rank"]),
            str(row["candidate_id"]),
        )
    )
    positions: dict[tuple[str, str, str], int] = defaultdict(int)
    members: list[dict[str, Any]] = []
    for row in eligible:
        lane = (
            row["underlying"], row["strategy_name"], row["candidate_kind"]
        )
        positions[lane] += 1
        if positions[lane] > maximum_position:
            continue
        member = dict(row)
        member["board_position"] = positions[lane]
        members.append(member)
    return tuple(members)


class OptionBoardPublicationRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def publish_complete_cycle(
        self,
        *,
        scheduled_cycle: datetime,
        as_of_session: date,
        expected_underlyers: tuple[str, ...],
        strategy_policy_sha256: str,
        configuration_sha256: str,
        published_at: datetime,
    ) -> BoardPublicationResult:
        if not expected_underlyers:
            raise ValueError("board publication requires expected underlyers")
        if len(set(expected_underlyers)) != len(expected_underlyers):
            raise ValueError("expected underlyers must be unique")
        scheduled_cycle = _utc(scheduled_cycle, "scheduled_cycle")
        published_at = _utc(published_at, "published_at")
        expected = tuple(value.strip().upper() for value in expected_underlyers)
        publication_id = uuid5(
            NAMESPACE_URL,
            (
                f"option-board:{scheduled_cycle.isoformat()}:"
                f"{strategy_policy_sha256}:{configuration_sha256}:"
                f"{BOARD_SELECTOR_SHA256}"
            ),
        )
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '60s'")
            cursor.execute(
                """
                SELECT publication_id, covered_underlying_count,
                       selection_evidence,
                       (SELECT COUNT(*) FROM option_board_members AS member
                        WHERE member.publication_id = publication.publication_id)
                           AS member_count
                FROM option_board_publications AS publication
                WHERE publication.publication_id = %s
                """,
                (publication_id,),
            )
            existing = cursor.fetchone()
            if existing:
                return BoardPublicationResult(
                    "ALREADY_PUBLISHED",
                    existing["publication_id"],
                    len(expected),
                    existing["covered_underlying_count"],
                    existing["member_count"],
                    dict(existing["selection_evidence"] or {}),
                )

            cursor.execute(
                """
                WITH cycle_matrices AS (
                    SELECT DISTINCT ON (analysis.underlying)
                           analysis.matrix_id, analysis.underlying,
                           analysis.market_time, analysis.observed_time
                    FROM option_analysis_runs AS analysis
                    JOIN option_ingestion_runs AS run USING (batch_id)
                    WHERE run.scheduled_cycle = %s
                                            AND run.configuration_sha256 = %s
                      AND analysis.underlying = ANY(%s)
                      AND analysis.status = 'COMPLETE'
                      AND EXISTS (
                          SELECT 1
                          FROM option_strategy_candidates AS candidate
                          WHERE candidate.matrix_id = analysis.matrix_id
                            AND candidate.policy_sha256 = %s
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM option_strategy_candidates AS candidate
                          WHERE candidate.matrix_id = analysis.matrix_id
                            AND candidate.policy_sha256 = %s
                            AND (
                                SELECT COUNT(*)
                                FROM option_candidate_execution_gates AS gate
                                WHERE gate.candidate_id = candidate.candidate_id
                                                                    AND gate.ledger_version = %s
                            ) <> 6
                      )
                    ORDER BY analysis.underlying,
                             analysis.observed_time DESC, analysis.matrix_id
                )
                SELECT matrix_id, underlying, market_time, observed_time
                FROM cycle_matrices
                ORDER BY underlying
                """,
                (
                    scheduled_cycle,
                    configuration_sha256,
                    list(expected),
                    strategy_policy_sha256,
                    strategy_policy_sha256,
                    GATE_LEDGER_VERSION,
                ),
            )
            matrices = [dict(row) for row in cursor.fetchall()]
            covered = {row["underlying"] for row in matrices}
            if covered != set(expected):
                return BoardPublicationResult(
                    "INCOMPLETE_UNIVERSE",
                    None,
                    len(expected),
                    len(covered),
                    0,
                    {
                        "missing_underlyers": sorted(set(expected) - covered),
                        "covered_underlyers": sorted(covered),
                    },
                )

            matrix_ids = tuple(row["matrix_id"] for row in matrices)
            selector_started = time.perf_counter()
            cursor.execute(
                """
                SELECT candidate.candidate_id, candidate.matrix_id,
                       candidate.underlying, candidate.strategy_name,
                      candidate.candidate_kind, candidate.candidate_rank,
                      candidate.market_data_time, candidate.observed_time,
                       ARRAY(
                           SELECT leg.contract_id
                           FROM option_candidate_legs AS leg
                           WHERE leg.candidate_id = candidate.candidate_id
                           UNION
                           SELECT (candidate.rank_components->>'contract_id')::BIGINT
                           WHERE candidate.rank_components->>'contract_id' IS NOT NULL
                           ORDER BY 1
                       ) AS contract_ids
                FROM option_strategy_candidates AS candidate
                WHERE candidate.matrix_id = ANY(%s)
                  AND candidate.policy_sha256 = %s
                  AND candidate.status = 'SELECTED'
                  AND candidate.market_data_time <= %s
                  AND candidate.observed_time <= %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM option_candidate_legs AS causal_leg
                      WHERE causal_leg.candidate_id = candidate.candidate_id
                        AND (
                            causal_leg.source_market_time > %s
                            OR causal_leg.quote_time > %s
                            OR causal_leg.underlying_quote_time > %s
                        )
                  )
                ORDER BY candidate.underlying, candidate.strategy_name,
                         candidate.candidate_kind, candidate.candidate_rank,
                         candidate.candidate_id
                """,
                (
                    list(matrix_ids),
                    strategy_policy_sha256,
                    published_at,
                    published_at,
                    published_at,
                    published_at,
                    published_at,
                ),
            )
            current_rows = [dict(row) for row in cursor.fetchall()]
            requested_contract_ids = sorted(
                {
                    int(contract_id)
                    for row in current_rows
                    for contract_id in row["contract_ids"]
                }
            )
            first_selected: dict[int, dict[str, Any]] = {}
            if requested_contract_ids:
                cursor.execute(
                    """
                    WITH requested AS (
                        SELECT UNNEST(%s::BIGINT[]) AS contract_id
                    ), appearances AS (
                        SELECT leg.contract_id, candidate.matrix_id,
                               candidate.market_data_time,
                               candidate.observed_time
                        FROM requested
                        JOIN option_candidate_legs AS leg USING (contract_id)
                        JOIN option_strategy_candidates AS candidate
                          USING (candidate_id)
                        WHERE candidate.policy_sha256 = %s
                          AND candidate.status = 'SELECTED'
                          AND candidate.market_data_time <= %s
                          AND candidate.observed_time <= %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM option_candidate_legs AS causal_leg
                              WHERE causal_leg.candidate_id = candidate.candidate_id
                                AND (
                                    causal_leg.source_market_time > %s
                                    OR causal_leg.quote_time > %s
                                    OR causal_leg.underlying_quote_time > %s
                                )
                          )
                        UNION ALL
                        SELECT requested.contract_id, candidate.matrix_id,
                               candidate.market_data_time,
                               candidate.observed_time
                        FROM requested
                        JOIN option_strategy_candidates AS candidate
                          ON (candidate.rank_components->>'contract_id')::BIGINT
                             = requested.contract_id
                        WHERE candidate.policy_sha256 = %s
                          AND candidate.status = 'SELECTED'
                          AND candidate.rank_components->>'contract_id' IS NOT NULL
                          AND candidate.market_data_time <= %s
                          AND candidate.observed_time <= %s
                    )
                    SELECT DISTINCT ON (contract_id)
                           contract_id, matrix_id, market_data_time, observed_time
                    FROM appearances
                    ORDER BY contract_id, market_data_time, observed_time, matrix_id
                    """,
                    (
                        requested_contract_ids,
                        strategy_policy_sha256,
                        published_at,
                        published_at,
                        published_at,
                        published_at,
                        published_at,
                        strategy_policy_sha256,
                        published_at,
                        published_at,
                    ),
                )
                first_selected = {
                    int(row["contract_id"]): dict(row)
                    for row in cursor.fetchall()
                }
            selected_rows = []
            for row in current_rows:
                row["prior_contract_excluded"] = any(
                    (first := first_selected.get(int(contract_id))) is not None
                    and first["matrix_id"] != row["matrix_id"]
                    and (
                        first["market_data_time"] < row["market_data_time"]
                        or (
                            first["market_data_time"] == row["market_data_time"]
                            and first["observed_time"] < row["observed_time"]
                        )
                    )
                    for contract_id in row["contract_ids"]
                )
                selected_rows.append(row)
            selected_rows = tuple(selected_rows)
            members = rank_board_members(
                selected_rows,
                maximum_position=BOARD_SELECTOR_POLICY[
                    "maximum_position_per_lane"
                ],
            )
            survivor_count = sum(
                not row["prior_contract_excluded"] for row in selected_rows
            )
            excluded_count = len(selected_rows) - survivor_count
            by_strategy: dict[str, dict[str, int]] = defaultdict(
                lambda: {"input": 0, "excluded": 0, "surviving": 0, "members": 0}
            )
            for row in selected_rows:
                bucket = by_strategy[row["strategy_name"]]
                bucket["input"] += 1
                if row["prior_contract_excluded"]:
                    bucket["excluded"] += 1
                else:
                    bucket["surviving"] += 1
            for row in members:
                by_strategy[row["strategy_name"]]["members"] += 1
            selection_evidence = {
                "selector_policy": BOARD_SELECTOR_POLICY,
                "input_selected_candidates": len(selected_rows),
                "prior_contract_excluded_candidates": excluded_count,
                "surviving_candidates": survivor_count,
                "persisted_members": len(members),
                "selector_query_ms": round(
                    (time.perf_counter() - selector_started) * 1000, 3
                ),
                "by_strategy": dict(sorted(by_strategy.items())),
            }
            market_data_time = max(row["market_time"] for row in matrices)
            observed_time = max(row["observed_time"] for row in matrices)
            if published_at < observed_time:
                raise ValueError("published_at cannot precede source observation")
            cursor.execute(
                """
                INSERT INTO option_board_publications (
                    publication_id, scheduled_cycle, as_of_session, status,
                    selector_version, selector_sha256, strategy_policy_sha256,
                    configuration_sha256, expected_underlying_count,
                    covered_underlying_count, source_matrix_ids,
                    market_data_time, observed_time, selection_evidence,
                    published_at
                ) VALUES (
                    %s, %s, %s, 'COMPLETE', %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    publication_id,
                    scheduled_cycle,
                    as_of_session,
                    BOARD_SELECTOR_VERSION,
                    BOARD_SELECTOR_SHA256,
                    strategy_policy_sha256,
                    configuration_sha256,
                    len(expected),
                    len(matrices),
                    list(matrix_ids),
                    market_data_time,
                    observed_time,
                    Json(selection_evidence),
                    published_at,
                ),
            )
            if members:
                execute_values(
                    cursor,
                    """
                    INSERT INTO option_board_members (
                        publication_id, candidate_id, underlying,
                        strategy_name, candidate_kind, board_position,
                        raw_candidate_rank, source_matrix_id,
                        selection_evidence
                    ) VALUES %s
                    """,
                    [
                        (
                            publication_id,
                            row["candidate_id"],
                            row["underlying"],
                            row["strategy_name"],
                            row["candidate_kind"],
                            row["board_position"],
                            row["candidate_rank"],
                            row["matrix_id"],
                            Json(
                                {
                                    "contract_ids": list(row["contract_ids"]),
                                    "prior_contract_excluded": False,
                                    "selector_sha256": BOARD_SELECTOR_SHA256,
                                }
                            ),
                        )
                        for row in members
                    ],
                )
            return BoardPublicationResult(
                "PUBLISHED",
                publication_id,
                len(expected),
                len(matrices),
                len(members),
                selection_evidence,
            )

    def latest_complete(
        self,
        strategy_policy_sha256: str,
        configuration_sha256: str,
    ) -> dict[str, Any] | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM option_board_publications
                WHERE status = 'COMPLETE'
                  AND strategy_policy_sha256 = %s
                  AND configuration_sha256 = %s
                  AND selector_sha256 = %s
                ORDER BY scheduled_cycle DESC, published_at DESC
                LIMIT 1
                """,
                (
                    strategy_policy_sha256,
                    configuration_sha256,
                    BOARD_SELECTOR_SHA256,
                ),
            )
            row = cursor.fetchone()
        return dict(row) if row else None


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)