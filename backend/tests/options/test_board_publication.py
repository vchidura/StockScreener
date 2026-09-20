from uuid import UUID

from options.repositories.board import (
    BOARD_SELECTOR_SHA256,
    BOARD_SELECTOR_VERSION,
    rank_board_members,
)
from options.repositories.board import OptionBoardPublicationRepository
from options.strategies.gates import GATE_LEDGER_VERSION
import inspect
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from types import SimpleNamespace
import os

import pytest

from options.analytics.alert_selection import baseline_package_identity, select_baseline_alerts


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


ALERT_TIME = datetime(2026, 9, 18, 15, tzinfo=timezone.utc)


def _alert(index, *, strategy="DIRECTIONAL_LONG_PREMIUM", underlying="SPY", rank=1):
    row = {
        **_row(str(UUID(int=index)), rank, strategy=strategy, kind="SINGLE_CONTRACT"),
        "underlying": underlying, "status": "SELECTED", "strategy_version": "phase2_v4",
        "policy_sha256": "a" * 64, "structure_type": "LONG_CALL", "net_premium": "-200",
        "gate_ledger_complete": True, "baseline_gates_pass": True, "market_data_time": ALERT_TIME - timedelta(minutes=2),
        "observed_time": ALERT_TIME - timedelta(minutes=1),
        "valid_until": ALERT_TIME + timedelta(minutes=10),
        "expires_at": ALERT_TIME + timedelta(days=10),
        "legs": [{"contract_id": index, "side": "BUY", "ratio": 1, "multiplier": 100,
                  "model_mark": "2", "source_market_time": ALERT_TIME - timedelta(minutes=2)}],
    }
    if strategy == "DIRECTIONAL_DEBIT_SPREAD":
        row.update(structure_type="CALL_DEBIT_VERTICAL", candidate_kind="MULTI_LEG", net_premium="-100")
        row["legs"].append({**row["legs"][0], "contract_id": index + 10000, "side": "SELL", "model_mark": "1"})
    return row


def test_baseline_cap_fair_allocation_lanes_and_order_independence():
    rows = [_alert(index + 1 + model_index * 100, underlying=f"T{index}", strategy=model)
            for model_index, model in enumerate(("DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"))
            for index in range(60)]
    result = select_baseline_alerts(rows, {}, decision_at=ALERT_TIME)
    assert len(result["members"]) == 50
    assert {model: counts["new_alerts"] for model, counts in result["evidence"]["by_model"].items()} == {
        "DIRECTIONAL_LONG_PREMIUM": 25, "DIRECTIONAL_DEBIT_SPREAD": 25,
    }
    assert result == select_baseline_alerts(list(reversed(rows)), {}, decision_at=ALERT_TIME)
    assert len(select_baseline_alerts([_alert(1), _alert(2, rank=2)], {}, decision_at=ALERT_TIME)["members"]) == 1


def test_baseline_repeats_are_exact_and_do_not_consume_new_alert_lane():
    first, repeat, new = _alert(1), _alert(2), _alert(3, rank=2)
    repeat["legs"] = first["legs"]
    identity = baseline_package_identity(first)
    prior = {identity: {"candidate_id": first["candidate_id"], "expires_at": first["expires_at"]}}
    result = select_baseline_alerts([repeat, repeat, new], prior, decision_at=ALERT_TIME)
    assert [row["candidate_id"] for row in result["members"]] == [new["candidate_id"]]
    assert len(result["observations"]) == 1
    assert result["observations"][0]["first_candidate_id"] == str(first["candidate_id"])
    assert baseline_package_identity({**repeat, "strategy_version": "next"}) != identity
    assert baseline_package_identity({**repeat, "policy_sha256": "b" * 64}) != identity


def test_baseline_fail_closed_and_no_minimum_quota():
    rows = [
        {**_alert(1), "valid_until": ALERT_TIME},
        {**_alert(2), "expires_at": ALERT_TIME},
        {**_alert(3), "gate_ledger_complete": False},
        {**_alert(4), "net_premium": "-201"},
        {**_alert(5), "legs": []},
        _alert(6, strategy="VOLUME_OI_ANOMALY"),
        {**_alert(7), "baseline_gates_pass": False},
    ]
    result = select_baseline_alerts(rows, {}, decision_at=ALERT_TIME)
    assert result["members"] == [] and result["observations"] == []
    assert sum(result["evidence"]["rejections"].values()) == len(rows)
    assert select_baseline_alerts([], {}, decision_at=ALERT_TIME)["members"] == []


class _BaselineCursor:
    def __init__(self, *, existing=None, latest=None, matrices=(), rows=(), prior=(), timely=True):
        self.singles = iter([existing, {"latest_cycle": latest}])
        self.batches = iter([matrices, rows, prior])
        self.commands = []
        self.timely = timely

    def execute(self, query, args=None):
        self.commands.append((query, args))

    def fetchone(self):
        if "AS decision_at" in self.commands[-1][0]:
            return {"decision_at": ALERT_TIME}
        if "AS timely" in self.commands[-1][0]:
            return {"timely": self.timely}
        return next(self.singles)

    def fetchall(self):
        return next(self.batches)


def _publish_baseline(cursor):
    repository = OptionBoardPublicationRepository()

    @contextmanager
    def connection():
        yield cursor

    repository._cursor = connection
    configuration = SimpleNamespace(settings=SimpleNamespace(underlyers=("SPY",)),
        strategy_policy_sha256="a" * 64, configuration_sha256="b" * 64,
        policy_sha256="c" * 64, strategy_policy=SimpleNamespace(strategy_version="phase2_v4"))
    return repository.publish_baseline_cycle(configuration=configuration,
        scheduled_cycle=ALERT_TIME - timedelta(minutes=5), published_at=ALERT_TIME,
        effective_from=ALERT_TIME - timedelta(minutes=10),
        calendar=SimpleNamespace(session_for_slot=lambda clock: clock.date(),
            expiration_cutoff=lambda day: ALERT_TIME + timedelta(days=10)))


def test_baseline_repository_idempotence_incomplete_and_out_of_order_do_not_write():
    for cursor, status in [
        (_BaselineCursor(existing={"covered_underlying_count": 1, "selection_evidence": {"new_alerts": 0}}), "ALREADY_PUBLISHED"),
        (_BaselineCursor(latest=ALERT_TIME), "OUT_OF_ORDER"),
        (_BaselineCursor(), "INCOMPLETE_UNIVERSE"),
    ]:
        assert _publish_baseline(cursor).status == status
        assert not any("INSERT" in query for query, _ in cursor.commands)
        assert any("pg_advisory_xact_lock" in query for query, _ in cursor.commands)


def test_baseline_repository_persists_zero_and_only_final_members(monkeypatch):
    captured = []
    monkeypatch.setattr("options.repositories.board.execute_values", lambda cursor, query, values: captured.extend(values))
    matrix = {"matrix_id": UUID(int=99), "underlying": "SPY", "market_time": ALERT_TIME - timedelta(minutes=2),
        "observed_time": ALERT_TIME - timedelta(minutes=1)}
    for candidates, expected in [([], 0), ([_alert(1), _alert(2, rank=2)], 1)]:
        cursor = _BaselineCursor(matrices=[matrix], rows=[{**row, "first_expiration": ALERT_TIME.date()} for row in candidates])
        result = _publish_baseline(cursor)
        assert result.status == "PUBLISHED" and result.member_count == expected
        assert sum("INSERT INTO option_board_publications" in query for query, _ in cursor.commands) == 1
    assert len(captured) == 1
    assert captured[0][1] == UUID(int=1)
    assert not any("option_signal_events" in query for query, _ in cursor.commands)
    for query, args in cursor.commands:
        if args is not None:
            assert query.count("%s") == len(args)
    late = _BaselineCursor(matrices=[matrix], rows=[{**_alert(1), "first_expiration": ALERT_TIME.date()}], timely=False)
    with pytest.raises(ValueError, match="elapsed during persistence"):
        _publish_baseline(late)


def test_baseline_package_identity_tracks_terms_not_leg_order_or_missing_runs():
    first = _alert(1, strategy="DIRECTIONAL_DEBIT_SPREAD")
    identity = baseline_package_identity(first)
    assert baseline_package_identity({**first, "legs": list(reversed(first["legs"]))}) == identity
    for field, value in [("contract_id", 999), ("side", "SELL"), ("ratio", 2), ("multiplier", 10)]:
        changed = {**first, "legs": [{**first["legs"][0], field: value}, first["legs"][1]]}
        assert baseline_package_identity(changed) != identity
    prior = {identity: {"candidate_id": first["candidate_id"], "expires_at": first["expires_at"]}}
    assert select_baseline_alerts([], prior, decision_at=ALERT_TIME)["observations"] == []
    later = {**first, "candidate_id": UUID(int=2), "valid_until": ALERT_TIME + timedelta(days=2)}
    result = select_baseline_alerts([later], prior, decision_at=ALERT_TIME + timedelta(days=1))
    assert result["members"] == [] and len(result["observations"]) == 1


@pytest.mark.skipif(os.getenv("OPTION_ALERT_READONLY_TESTS") != "1", reason="explicit read-only PostgreSQL check required")
def test_baseline_publication_queries_compile_live_read_only(monkeypatch):
    from database import get_db_cursor

    monkeypatch.setattr("options.repositories.board.execute_values", lambda *args: None)
    matrix = {"matrix_id": UUID(int=99), "underlying": "SPY", "market_time": ALERT_TIME - timedelta(minutes=2),
        "observed_time": ALERT_TIME - timedelta(minutes=1)}
    captured = _BaselineCursor(matrices=[matrix], rows=[{**_alert(1), "first_expiration": ALERT_TIME.date()}])
    _publish_baseline(captured)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        for query, args in captured.commands:
            if query.lstrip().startswith("SELECT") and "pg_advisory" not in query:
                cursor.execute("EXPLAIN " + query, args)
                assert cursor.fetchall()