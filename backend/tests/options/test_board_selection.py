from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from options.analytics.board_selection import (
    BoardCandidateObservation,
    BoardSelectorVariant,
    select_board_shadow,
)


UTC = timezone.utc
BASE = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)


def _row(index, matrix, contracts, *, rank=1, strategy="S", session=date(2026, 9, 8)):
    return BoardCandidateObservation(
        candidate_id=UUID(f"00000000-0000-0000-0000-{index:012d}"),
        matrix_id=UUID(f"10000000-0000-0000-0000-{matrix:012d}"),
        underlying="SPY",
        strategy_name=strategy,
        candidate_kind="MULTI_LEG",
        candidate_rank=rank,
        market_data_time=BASE + timedelta(minutes=15 * matrix),
        observed_time=BASE + timedelta(minutes=15 * matrix + 1),
        session_date=session,
        contract_ids=tuple(contracts),
    )


def test_lifetime_global_excludes_any_reused_contract_forever():
    observations = (
        _row(1, 1, (10, 11)),
        _row(2, 2, (11, 12)),
        _row(3, 3, (13, 14)),
    )

    result = select_board_shadow(observations, BoardSelectorVariant.LIFETIME_GLOBAL)

    assert result.excluded_candidates == 1
    assert [member.candidate_id for member in result.members] == [
        observations[0].candidate_id,
        observations[2].candidate_id,
    ]


def test_same_session_resets_contract_history_next_session():
    observations = (
        _row(1, 1, (10,)),
        _row(2, 2, (10,), session=date(2026, 9, 9)),
    )

    result = select_board_shadow(
        observations, BoardSelectorVariant.SAME_SESSION_GLOBAL
    )

    assert result.excluded_candidates == 0


def test_previous_matrix_allows_contract_after_one_absent_matrix():
    observations = (
        _row(1, 1, (10,)),
        _row(2, 2, (20,)),
        _row(3, 3, (10,)),
    )

    result = select_board_shadow(
        observations, BoardSelectorVariant.PREVIOUS_MATRIX_GLOBAL
    )

    assert result.excluded_candidates == 0


def test_same_strategy_grace_one_bridges_one_missing_slot():
    observations = (
        _row(1, 1, (10,)),
        _row(2, 2, (20,)),
        _row(3, 3, (10,)),
        _row(4, 4, (30,)),
        _row(5, 5, (40,)),
        _row(6, 6, (10,)),
    )

    result = select_board_shadow(
        observations, BoardSelectorVariant.SAME_STRATEGY_GRACE_1
    )

    selected = {member.candidate_id for member in result.members}
    assert observations[2].candidate_id not in selected
    assert observations[5].candidate_id in selected


def test_ranking_is_recomputed_per_matrix_lane_after_exclusion():
    observations = (
        _row(1, 1, (10,), rank=1),
        _row(2, 2, (10,), rank=1),
        _row(3, 2, (11,), rank=2),
        _row(4, 2, (12,), rank=3),
        _row(5, 2, (13,), rank=4),
        _row(6, 2, (14,), rank=5),
    )

    result = select_board_shadow(observations, BoardSelectorVariant.LIFETIME_GLOBAL)
    second_matrix = [
        member for member in result.members if member.matrix_id == observations[1].matrix_id
    ]

    assert [member.raw_candidate_rank for member in second_matrix] == [2, 3, 4]
    assert [member.board_position for member in second_matrix] == [1, 2, 3]


def test_evaluation_window_uses_prior_rows_as_warmup_without_reporting_them():
    observations = (
        _row(1, 1, (10,)),
        _row(2, 2, (10,)),
        _row(3, 2, (20,), rank=2),
    )

    result = select_board_shadow(
        observations,
        BoardSelectorVariant.LIFETIME_GLOBAL,
        evaluation_start=observations[1].market_data_time,
    )

    assert result.input_candidates == 2
    assert result.excluded_candidates == 1
    assert [member.candidate_id for member in result.members] == [
        observations[2].candidate_id
    ]