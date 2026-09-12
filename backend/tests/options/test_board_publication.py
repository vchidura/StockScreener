from uuid import UUID

from options.repositories.board import (
    BOARD_SELECTOR_SHA256,
    BOARD_SELECTOR_VERSION,
    rank_board_members,
)
from options.repositories.board import OptionBoardPublicationRepository
from options.strategies.gates import GATE_LEDGER_VERSION
import inspect


def _row(
    candidate_id: str,
    rank: int,
    *,
    strategy: str = "SPREAD_RANGE_LOCATOR",
    kind: str = "MULTI_LEG",
    excluded: bool = False,
):
    return {
        "candidate_id": UUID(candidate_id),
        "matrix_id": UUID("00000000-0000-0000-0000-000000000099"),
        "underlying": "SPY",
        "strategy_name": strategy,
        "candidate_kind": kind,
        "candidate_rank": rank,
        "contract_ids": [rank],
        "prior_contract_excluded": excluded,
    }


def test_board_selector_identity_is_pinned():
    assert BOARD_SELECTOR_VERSION == "option_board_selector_v1"
    assert BOARD_SELECTOR_SHA256 == (
        "cd15ba59bf664060221bdb83961516bace81cef28615b48f606af69e0a9f6f30"
    )


def test_board_publication_requires_active_gate_ledger_version():
    source = inspect.getsource(OptionBoardPublicationRepository.publish_complete_cycle)

    assert "gate.ledger_version = %s" in source
    assert GATE_LEDGER_VERSION == "gate_ledger_v2"


def test_board_reranks_after_prior_contract_exclusion_and_caps_each_lane():
    rows = (
        _row("00000000-0000-0000-0000-000000000001", 1, excluded=True),
        _row("00000000-0000-0000-0000-000000000004", 2),
        _row("00000000-0000-0000-0000-000000000003", 2),
        _row("00000000-0000-0000-0000-000000000005", 3),
        _row("00000000-0000-0000-0000-000000000006", 4),
        _row(
            "00000000-0000-0000-0000-000000000007",
            1,
            strategy="VOLUME_OI_ANOMALY",
            kind="RESEARCH_ONLY",
        ),
    )

    members = rank_board_members(rows)

    assert [str(row["candidate_id"])[-1] for row in members] == ["3", "4", "5", "7"]
    assert [row["board_position"] for row in members] == [1, 2, 3, 1]
    assert all(not row["prior_contract_excluded"] for row in members)