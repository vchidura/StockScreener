from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from uuid import UUID


class BoardSelectorVariant(str, Enum):
    LIFETIME_GLOBAL = "LIFETIME_GLOBAL"
    SAME_SESSION_GLOBAL = "SAME_SESSION_GLOBAL"
    PREVIOUS_MATRIX_GLOBAL = "PREVIOUS_MATRIX_GLOBAL"
    SAME_STRATEGY_GRACE_1 = "SAME_STRATEGY_GRACE_1"
    SAME_STRATEGY_GRACE_2 = "SAME_STRATEGY_GRACE_2"
    NO_EXCLUSION = "NO_EXCLUSION"


@dataclass(frozen=True, slots=True)
class BoardCandidateObservation:
    candidate_id: UUID
    matrix_id: UUID
    underlying: str
    strategy_name: str
    candidate_kind: str
    candidate_rank: int
    market_data_time: datetime
    observed_time: datetime
    session_date: date
    contract_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.underlying or not self.strategy_name or not self.candidate_kind:
            raise ValueError("candidate selector keys cannot be blank")
        if self.candidate_rank < 1:
            raise ValueError("candidate rank must be positive")
        if not self.contract_ids or any(value <= 0 for value in self.contract_ids):
            raise ValueError("candidate contract ids must be positive and non-empty")


@dataclass(frozen=True, slots=True)
class BoardShadowMember:
    variant: BoardSelectorVariant
    candidate_id: UUID
    matrix_id: UUID
    underlying: str
    strategy_name: str
    candidate_kind: str
    raw_candidate_rank: int
    board_position: int
    market_data_time: datetime
    session_date: date
    contract_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class BoardShadowResult:
    variant: BoardSelectorVariant
    input_candidates: int
    excluded_candidates: int
    surviving_candidates: int
    members: tuple[BoardShadowMember, ...]


def select_board_shadow(
    observations: tuple[BoardCandidateObservation, ...],
    variant: BoardSelectorVariant,
    *,
    maximum_position: int = 3,
    evaluation_start: datetime | None = None,
) -> BoardShadowResult:
    if maximum_position < 1:
        raise ValueError("maximum_position must be positive")
    ordered = sorted(
        observations,
        key=lambda row: (
            row.underlying,
            row.market_data_time,
            row.observed_time,
            str(row.matrix_id),
            row.strategy_name,
            row.candidate_kind,
            row.candidate_rank,
            str(row.candidate_id),
        ),
    )
    by_matrix: dict[tuple[str, UUID], list[BoardCandidateObservation]] = {}
    matrix_order: list[tuple[str, UUID]] = []
    for observation in ordered:
        key = (observation.underlying, observation.matrix_id)
        if key not in by_matrix:
            by_matrix[key] = []
            matrix_order.append(key)
        by_matrix[key].append(observation)

    lifetime_seen: set[int] = set()
    session_seen: dict[tuple[str, date], set[int]] = defaultdict(set)
    previous_matrix_contracts: dict[str, set[int]] = defaultdict(set)
    strategy_last_seen: dict[tuple[str, str, str, int], int] = {}
    matrix_indexes: dict[str, int] = defaultdict(int)
    evaluated_inputs = 0
    excluded = 0
    members: list[BoardShadowMember] = []

    for matrix_key in matrix_order:
        underlying, matrix_id = matrix_key
        matrix_rows = by_matrix[matrix_key]
        matrix_index = matrix_indexes[underlying]
        matrix_indexes[underlying] += 1
        eligible: list[BoardCandidateObservation] = []
        for row in matrix_rows:
            in_evaluation = (
                evaluation_start is None
                or row.market_data_time >= evaluation_start
            )
            if in_evaluation:
                evaluated_inputs += 1
            contract_set = set(row.contract_ids)
            is_excluded = _excluded(
                row,
                contract_set,
                variant,
                matrix_index=matrix_index,
                lifetime_seen=lifetime_seen,
                session_seen=session_seen[(underlying, row.session_date)],
                previous_matrix_contracts=previous_matrix_contracts[underlying],
                strategy_last_seen=strategy_last_seen,
            )
            if is_excluded and in_evaluation:
                excluded += 1
            else:
                eligible.append(row)

        eligible.sort(
            key=lambda row: (
                row.underlying,
                row.strategy_name,
                row.candidate_kind,
                row.candidate_rank,
                str(row.candidate_id),
            )
        )
        lane_positions: dict[tuple[str, str, str], int] = defaultdict(int)
        for row in eligible:
            lane = (row.underlying, row.strategy_name, row.candidate_kind)
            lane_positions[lane] += 1
            if lane_positions[lane] > maximum_position:
                continue
            if evaluation_start is not None and row.market_data_time < evaluation_start:
                continue
            members.append(
                BoardShadowMember(
                    variant=variant,
                    candidate_id=row.candidate_id,
                    matrix_id=row.matrix_id,
                    underlying=row.underlying,
                    strategy_name=row.strategy_name,
                    candidate_kind=row.candidate_kind,
                    raw_candidate_rank=row.candidate_rank,
                    board_position=lane_positions[lane],
                    market_data_time=row.market_data_time,
                    session_date=row.session_date,
                    contract_ids=row.contract_ids,
                )
            )

        current_contracts = {
            contract_id for row in matrix_rows for contract_id in row.contract_ids
        }
        lifetime_seen.update(current_contracts)
        session_seen[(underlying, matrix_rows[0].session_date)].update(
            current_contracts
        )
        previous_matrix_contracts[underlying] = current_contracts
        for row in matrix_rows:
            for contract_id in row.contract_ids:
                strategy_last_seen[
                    (
                        row.underlying,
                        row.strategy_name,
                        row.candidate_kind,
                        contract_id,
                    )
                ] = matrix_index

    return BoardShadowResult(
        variant=variant,
        input_candidates=evaluated_inputs,
        excluded_candidates=excluded,
        surviving_candidates=evaluated_inputs - excluded,
        members=tuple(members),
    )


def _excluded(
    row: BoardCandidateObservation,
    contract_ids: set[int],
    variant: BoardSelectorVariant,
    *,
    matrix_index: int,
    lifetime_seen: set[int],
    session_seen: set[int],
    previous_matrix_contracts: set[int],
    strategy_last_seen: dict[tuple[str, str, str, int], int],
) -> bool:
    if variant is BoardSelectorVariant.NO_EXCLUSION:
        return False
    if variant is BoardSelectorVariant.LIFETIME_GLOBAL:
        return bool(contract_ids & lifetime_seen)
    if variant is BoardSelectorVariant.SAME_SESSION_GLOBAL:
        return bool(contract_ids & session_seen)
    if variant is BoardSelectorVariant.PREVIOUS_MATRIX_GLOBAL:
        return bool(contract_ids & previous_matrix_contracts)
    grace = (
        1
        if variant is BoardSelectorVariant.SAME_STRATEGY_GRACE_1
        else 2
    )
    return any(
        matrix_index
        - strategy_last_seen.get(
            (row.underlying, row.strategy_name, row.candidate_kind, contract_id),
            -10_000,
        )
        <= grace + 1
        for contract_id in contract_ids
    )