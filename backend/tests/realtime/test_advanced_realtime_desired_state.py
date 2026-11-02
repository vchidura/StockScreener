from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from realtime.advanced_realtime_desired_state import (
    DEFAULT_OPTION_UNDERLYINGS,
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    build_desired_state_revision,
    default_advanced_realtime_desired_state,
    effective_next_session,
)
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from realtime import advanced_realtime_desired_state_repository as repository
from scripts import manage_advanced_realtime_desired_state as command


UTC = timezone.utc
SOURCE_COHORT = UUID("11111111-1111-4111-8111-111111111111")


def test_default_desired_state_preserves_target9_and_exact_qqq_spy_scope():
    change = default_advanced_realtime_desired_state(SOURCE_COHORT)

    equities = tuple(
        row.ticker
        for row in change.members
        if row.kind is DesiredMemberKind.EQUITY
    )
    underlyings = tuple(
        row.ticker
        for row in change.members
        if row.kind is DesiredMemberKind.OPTION_UNDERLYING
    )
    assert equities == EQUITY_VALIDATION_UNIVERSE.equity_tickers
    assert underlyings == DEFAULT_OPTION_UNDERLYINGS
    assert change.source_slot_version == 1


def test_effective_session_is_upcoming_before_open_and_next_during_or_after_market():
    assert effective_next_session(
        datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
    ) == date(2026, 10, 30)
    assert effective_next_session(
        datetime(2026, 10, 30, 15, 0, tzinfo=UTC)
    ) == date(2026, 11, 2)
    assert effective_next_session(
        datetime(2026, 10, 30, 21, 0, tzinfo=UTC)
    ) == date(2026, 11, 2)
    assert effective_next_session(
        datetime(2026, 10, 31, 16, 0, tzinfo=UTC)
    ) == date(2026, 11, 2)


def test_internal_rollover_rejects_a_non_session_target():
    change = default_advanced_realtime_desired_state(SOURCE_COHORT)

    with pytest.raises(ValueError, match="XNYS session"):
        repository.record_desired_state_rollover_revision_cursor(
            object(),
            change,
            effective_session_date=date(2026, 7, 3),
            recorded_at=datetime(2026, 7, 3, 15, 0, tzinfo=UTC),
        )


def test_effective_snapshot_selection_is_bounded_when_pointer_is_next_session(
    monkeypatch,
):
    active_session = date(2026, 10, 30)
    active_revision_id = UUID(int=2)
    revision = SimpleNamespace(
        revision_id=active_revision_id,
        effective_session_date=active_session,
    )
    snapshot = SimpleNamespace(
        revision_id=active_revision_id,
        session_date=active_session,
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (date(2026, 11, 2),),
        (str(active_revision_id),),
    )
    load_revision = MagicMock(return_value=revision)
    load_snapshot = MagicMock(return_value=snapshot)
    load_preparations = MagicMock(return_value=())
    monkeypatch.setattr(
        repository,
        "load_desired_state_revision_cursor",
        load_revision,
    )
    monkeypatch.setattr(
        repository,
        "_load_desired_snapshot_cursor",
        load_snapshot,
    )
    monkeypatch.setattr(
        repository,
        "load_desired_member_preparations_cursor",
        load_preparations,
    )

    result = (
        repository.load_effective_desired_state_reconciliation_cursor(
            cursor,
            session_date=active_session,
        )
    )

    assert result.state == "READY"
    assert result.snapshot == snapshot
    assert cursor.execute.call_count == 2
    assert "LIMIT 1" in cursor.execute.call_args_list[1].args[0]
    load_revision.assert_called_once_with(cursor, active_revision_id)
    load_snapshot.assert_called_once_with(cursor, active_revision_id)


def test_desired_state_accepts_automatic_or_stored_options_membership():
    with pytest.raises(ValueError, match="invalid syntax"):
        DesiredMemberIntent("bad symbol!", DesiredMemberKind.EQUITY)
    with pytest.raises(ValueError, match="requires an enabled Equity"):
        DesiredStateChange(
            source_cohort_id=SOURCE_COHORT,
            source_slot_version=1,
            members=(
                DesiredMemberIntent(
                    "SPY",
                    DesiredMemberKind.OPTION_UNDERLYING,
                ),
            ),
        )
    automatic = DesiredStateChange(
        source_cohort_id=SOURCE_COHORT,
        members=(
            DesiredMemberIntent("SPY", DesiredMemberKind.EQUITY),
            DesiredMemberIntent(
                "SPY",
                DesiredMemberKind.OPTION_UNDERLYING,
            ),
        ),
    )
    stored = DesiredStateChange(
        source_cohort_id=SOURCE_COHORT,
        source_slot_version=1,
        members=automatic.members,
    )

    assert automatic.source_slot_version is None
    assert stored.source_slot_version == 1


def test_same_desired_revision_identity_is_replayable_across_record_times():
    change = default_advanced_realtime_desired_state(SOURCE_COHORT)
    security_ids = {
        ticker: uuid4()
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }
    first = build_desired_state_revision(
        change,
        effective_session_date=date(2026, 11, 2),
        recorded_at=datetime(2026, 10, 30, 21, 0, tzinfo=UTC),
        security_ids=security_ids,
        selection_policy_reference="rt.slot_set_versions:source:1",
        selection_policy_sha256="a" * 64,
    )
    replay = build_desired_state_revision(
        change,
        effective_session_date=date(2026, 11, 2),
        recorded_at=datetime(2026, 10, 31, 16, 0, tzinfo=UTC),
        security_ids=security_ids,
        selection_policy_reference="rt.slot_set_versions:source:1",
        selection_policy_sha256="a" * 64,
    )

    assert replay.revision_id == first.revision_id
    assert replay.revision_sha256 == first.revision_sha256
    assert replay.recorded_at != first.recorded_at


def test_management_command_assumes_hardened_app_role():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value

    command._set_app_role(connection)

    cursor.execute.assert_called_once_with(
        "SET ROLE stocks_realtime_v2_app"
    )


def test_maintenance_command_is_provider_free_and_reports_zero_effects(monkeypatch):
    args = command.parse_args([
        "apply",
        "--source-cohort-id",
        str(SOURCE_COHORT),
        "--observed-at",
        "2026-10-30T21:00:00Z",
    ])
    revision = SimpleNamespace(
        revision_id=uuid4(),
        revision_sha256="a" * 64,
        effective_session_date=date(2026, 11, 2),
    )
    monkeypatch.setattr(
        command,
        "apply_desired_state",
        lambda connection, change, observed_at: SimpleNamespace(
            state="READY",
            revision=revision,
            preparations=(),
            snapshot=None,
            reason_codes=(),
        ),
    )

    payload = command.execute(args, object())

    assert payload["provider_effects"] == 0
    assert payload["production_effects"] == 0
    assert payload["publication_effects"] == 0
    assert payload["effective_session_date"] == "2026-11-02"


def test_maintenance_command_defaults_all_timestamps_to_current_aware_utc(
    monkeypatch,
):
    fixed = datetime(2026, 10, 31, 19, 25, tzinfo=UTC)
    revision_id = uuid4()
    revision = SimpleNamespace(
        revision_id=revision_id,
        revision_sha256="b" * 64,
        effective_session_date=date(2026, 11, 2),
    )
    result = SimpleNamespace(
        state="WARMING",
        revision=revision,
        preparations=(),
        snapshot=None,
        reason_codes=("MEMBER_WARMING:XLK:SECTOR_ETF",),
    )
    observed = []
    checked = []
    materialized = []
    monkeypatch.setattr(command, "_now_utc", lambda: fixed)
    monkeypatch.setattr(
        command,
        "apply_desired_state",
        lambda connection, change, observed_at: (
            observed.append(observed_at) or result
        ),
    )
    monkeypatch.setattr(
        command,
        "update_desired_member_preparation",
        lambda connection, revision_id, **values: (
            checked.append(values["checked_at"])
            or SimpleNamespace(
                revision_id=revision_id,
                ticker=values["ticker"],
                kind=values["kind"],
                status=values["status"],
            )
        ),
    )
    monkeypatch.setattr(
        command,
        "reconcile_desired_state",
        lambda connection, revision_id, materialized_at: (
            materialized.append(materialized_at) or result
        ),
    )

    command.execute(
        command.parse_args([
            "apply",
            "--source-cohort-id",
            str(SOURCE_COHORT),
        ]),
        object(),
    )
    command.execute(
        command.parse_args([
            "readiness",
            "--revision-id",
            str(revision_id),
            "--ticker",
            "XLK",
            "--kind",
            "SECTOR_ETF",
            "--status",
            "WARMING",
        ]),
        object(),
    )
    command.execute(
        command.parse_args([
            "reconcile",
            "--revision-id",
            str(revision_id),
        ]),
        object(),
    )

    assert observed == checked == materialized == [fixed]
    assert fixed.tzinfo is UTC


class _SetReadCursor:
    def __init__(self, rows):
        self.rows = rows
        self.execute_count = 0

    def execute(self, statement, parameters):
        self.execute_count += 1

    def fetchall(self):
        return self.rows


def test_c2_preparation_uses_bounded_statements_for_400_names(monkeypatch):
    tickers = tuple(f"T{index:03d}" for index in range(400))
    security_ids = {
        ticker: uuid5(NAMESPACE_URL, f"c2-bounded:{ticker}")
        for ticker in tickers
    }
    members = tuple(
        DesiredMemberIntent(ticker, DesiredMemberKind.EQUITY)
        for ticker in tickers
    )
    observed_at = datetime(2026, 10, 30, 21, 0, tzinfo=UTC)
    identity_cursor = _SetReadCursor([
        (
            ticker,
            str(security_ids[ticker]),
            "STOCK",
            "CS",
            index + 1,
        )
        for index, ticker in enumerate(tickers)
    ])

    loaded_security_ids, loaded_instrument_ids = (
        repository._stored_security_identities_cursor(
            identity_cursor,
            members,
            observed_at=observed_at,
        )
    )

    assert identity_cursor.execute_count == 1
    assert loaded_security_ids == security_ids
    assert len(loaded_instrument_ids) == 400
    instrument_cursor = _SetReadCursor([
        (str(security_ids[ticker]), index + 1)
        for index, ticker in enumerate(tickers)
    ])
    assert len(repository._equity_instrument_ids_cursor(
        instrument_cursor,
        security_ids,
    )) == 400
    assert instrument_cursor.execute_count == 1

    change = DesiredStateChange(
        source_cohort_id=SOURCE_COHORT,
        members=members,
    )
    revision = build_desired_state_revision(
        change,
        effective_session_date=date(2026, 11, 2),
        recorded_at=observed_at,
        security_ids=security_ids,
        selection_policy_reference="stored-equity-policy",
        selection_policy_sha256="c" * 64,
    )
    batches = []
    monkeypatch.setattr(
        repository,
        "execute_values",
        lambda cursor, statement, values, page_size: batches.append(
            (statement, tuple(values), page_size)
        ),
    )

    persist_cursor = MagicMock()
    persist_cursor.fetchall.return_value = [
        (
            str(security_ids[ticker]),
            str(uuid5(NAMESPACE_URL, f"c2-bounded-revision:{ticker}")),
        )
        for ticker in tickers
    ]
    repository._persist_desired_members_cursor(
        persist_cursor,
        revision,
        security_ids=security_ids,
        instrument_ids={
            ticker: index + 1
            for index, ticker in enumerate(tickers)
        },
        source_base=set(security_ids.values()),
        source_underlyings=set(),
        ready_option_securities=set(),
        checked_at=observed_at,
    )

    assert len(batches) == 5
    assert [len(values) for _statement, values, _page_size in batches] == [
        400,
        400,
        400,
        400,
        400,
    ]
    assert {page_size for _statement, _values, page_size in batches} == {
        1_000
    }
