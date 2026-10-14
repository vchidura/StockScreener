from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.contracts import MarketChannel
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.option_slot_set_repository import (
    activate_slot_set_cursor,
    load_active_slot_set_cursor,
    load_slot_set_version_cursor,
    persist_and_activate_slot_set,
    persist_and_activate_slot_set_cursor,
    persist_slot_set_version_cursor,
)
from tests.realtime.slot_set_fakes import FakeSlotSetStore, SlotSetCursor

UTC = timezone.utc
NOW = datetime(2026, 10, 5, 13, 0, tzinfo=UTC)
COHORT = uuid5(NAMESPACE_URL, "slot-set-repository-cohort")
UNDERLYING = "00000000-0000-0000-0000-0000000000aa"
POLICY = "a" * 64
T, Q = MarketChannel.TRADE, MarketChannel.QUOTE


def _store(contracts=20, *, kind="MARKET_OPTIONS", underlyings=(UNDERLYING,)):
    store = FakeSlotSetStore()
    store.register_cohort(COHORT, kind, underlyings)
    for index in range(1, contracts + 1):
        store.register_option(index, 10_000 + index, 1_000 + index, UNDERLYING)
    return store, SlotSetCursor(store)


def _members(ids, purpose=SlotSetPurpose.PRIMARY):
    return tuple(SlotSetMember(index, channel, purpose, 100, 1_000 + index) for index in ids for channel in (T, Q))


def _slot_set(ids=range(1, 6), *, version=1, kind=SlotSetKind.PRE_SESSION, effective_at=NOW, members=None):
    return SlotSetVersion(
        COHORT, version, kind, effective_at, POLICY,
        members if members is not None else _members(ids))


def _refresh(version=2, ids=range(3, 9), **changes):
    return _slot_set(ids, version=version, kind=SlotSetKind.REFRESH, **changes)


def _inserts(store):
    return [row for row in store.statements if row.startswith("INSERT")]


def test_persist_writes_version_and_members_then_reads_them_back_verified():
    store, cursor = _store()
    slot_set = _slot_set()

    stored = persist_slot_set_version_cursor(cursor, slot_set)

    assert stored == slot_set
    key = (str(COHORT), 1)
    assert store.versions[key] == ("PRE_SESSION", NOW, POLICY, slot_set.member_sha256, 5, 0)
    assert len(store.members[key]) == 10
    assert load_slot_set_version_cursor(cursor, COHORT, 1) == slot_set
    assert load_slot_set_version_cursor(cursor, COHORT, 2) is None
    assert store.current == {}


def test_identical_replay_is_a_no_op_and_a_divergent_replay_raises():
    store, cursor = _store()
    persist_slot_set_version_cursor(cursor, _slot_set())
    writes = len(_inserts(store))

    assert persist_slot_set_version_cursor(cursor, _slot_set()) == _slot_set()
    assert len(_inserts(store)) == writes

    for divergent in (
        _slot_set(range(1, 7)),
        _slot_set(members=tuple(replace(row, priority=101) for row in _members(range(1, 6)))),
        _slot_set(members=_members(range(1, 6), SlotSetPurpose.RESERVE)),
        _slot_set(effective_at=NOW + timedelta(minutes=1)),
        replace(_slot_set(), policy_sha256="b" * 64),
    ):
        with pytest.raises(ValueError, match="divergent replay"):
            persist_slot_set_version_cursor(cursor, divergent)
    assert len(_inserts(store)) == writes


def test_versions_are_contiguous_from_one():
    store, cursor = _store()
    with pytest.raises(ValueError, match="contiguous"):
        persist_slot_set_version_cursor(cursor, _refresh(version=2))
    persist_slot_set_version_cursor(cursor, _slot_set())
    with pytest.raises(ValueError, match="contiguous"):
        persist_slot_set_version_cursor(cursor, _refresh(version=3))
    assert persist_slot_set_version_cursor(cursor, _refresh(version=2)).kind is SlotSetKind.REFRESH
    assert persist_slot_set_version_cursor(cursor, _refresh(version=3, ids=range(2, 6))).version == 3


def test_member_identity_failures_write_nothing():
    cases = {
        "catalog version does not belong": lambda s: s.catalog.__setitem__(1_001, (99_999, UNDERLYING)),
        "outside the cohort option universe": lambda s: s.catalog.__setitem__(1_001, (10_001, "other-security")),
        "catalog version is absent": lambda s: s.catalog.pop(1_001),
        "does not belong to its instrument": lambda s: s.instruments.__setitem__(1, ("EQUITY", None)),
    }
    for message, tamper in cases.items():
        store, cursor = _store()
        tamper(store)
        with pytest.raises(ValueError, match=message):
            persist_slot_set_version_cursor(cursor, _slot_set())
        assert _inserts(store) == [], message
    store, cursor = _store(kind="EQUITY_VALIDATION")
    with pytest.raises(ValueError, match="MARKET_OPTIONS"):
        persist_slot_set_version_cursor(cursor, _slot_set())
    store, cursor = _store(underlyings=())
    with pytest.raises(ValueError, match="no option underlyings"):
        persist_slot_set_version_cursor(cursor, _slot_set())
    store, cursor = _store()
    del store.cohorts[str(COHORT)]
    with pytest.raises(ValueError, match="cohort is absent"):
        persist_slot_set_version_cursor(cursor, _slot_set())
    assert _inserts(store) == []


def test_a_thousand_contracts_persist_in_chunks_and_round_trip():
    store, cursor = _store(contracts=1_000)
    ids = range(1, 1_001)
    slot_set = SlotSetVersion(COHORT, 1, SlotSetKind.PRE_SESSION, NOW, POLICY,
                              _members(range(1, 601)) + _members(range(601, 1_001), SlotSetPurpose.RESERVE))

    persist_slot_set_version_cursor(cursor, slot_set)

    member_inserts = [row for row in store.statements if row.startswith("INSERT INTO rt.slot_set_members")]
    assert len(member_inserts) == 8 and all(row.count("(%s,") <= 250 for row in member_inserts)
    stored = load_slot_set_version_cursor(cursor, COHORT, 1)
    assert stored == slot_set and (stored.primary_contract_count, stored.reserve_contract_count) == (600, 400)
    assert len(stored.members) == 2_000 and set(ids) == {row.instrument_id for row in stored.members}


def test_the_provider_cap_is_rejected_before_any_sql():
    store, cursor = _store(contracts=1_001)
    with pytest.raises(ValueError, match="provider cap"):
        _slot_set(members=_members(range(1, 1_002)))
    assert store.statements == []


def test_stored_hash_and_counts_are_recomputed_on_load():
    store, cursor = _store()
    persist_slot_set_version_cursor(cursor, _slot_set())
    key = (str(COHORT), 1)
    store.members[key][0] = (1, "Q", "PRIMARY", 7, 1_001)
    with pytest.raises(ValueError, match="hash or counts"):
        load_slot_set_version_cursor(cursor, COHORT, 1)
    store.members[key][0] = (1, "Q", "PRIMARY", 100, 1_001)
    assert load_slot_set_version_cursor(cursor, COHORT, 1) == _slot_set()
    kind, effective_at, policy, member_sha, primary, reserve = store.versions[key]
    store.versions[key] = (kind, effective_at, policy, member_sha, primary + 1, reserve)
    with pytest.raises(ValueError, match="hash or counts"):
        load_slot_set_version_cursor(cursor, COHORT, 1)
    store.versions[key] = (kind, effective_at, policy, "0" * 64, primary, reserve)
    with pytest.raises(ValueError, match="hash or counts"):
        load_slot_set_version_cursor(cursor, COHORT, 1)


def test_activation_is_make_before_break_and_reads_back_the_active_set():
    store, cursor = _store()
    assert load_active_slot_set_cursor(cursor, COHORT) is None
    assert persist_and_activate_slot_set_cursor(cursor, _slot_set()) is True
    first = load_active_slot_set_cursor(cursor, COHORT)
    assert (first.slot_set, first.activated_at) == (_slot_set(), NOW)

    persist_slot_set_version_cursor(cursor, _refresh())
    assert load_active_slot_set_cursor(cursor, COHORT).slot_set.version == 1

    later = NOW + timedelta(minutes=5)
    assert activate_slot_set_cursor(cursor, COHORT, 2, activated_at=later) is True
    second = load_active_slot_set_cursor(cursor, COHORT)
    assert (second.slot_set, second.activated_at) == (_refresh(), later)
    assert activate_slot_set_cursor(cursor, COHORT, 2, activated_at=later) is False


def test_the_pointer_never_moves_backwards_or_to_an_unpersisted_version():
    store, cursor = _store()
    persist_and_activate_slot_set_cursor(cursor, _slot_set())
    persist_and_activate_slot_set_cursor(cursor, _refresh(), activated_at=NOW + timedelta(minutes=5))

    with pytest.raises(ValueError, match="backwards"):
        activate_slot_set_cursor(cursor, COHORT, 1, activated_at=NOW + timedelta(minutes=10))
    with pytest.raises(ValueError, match="backwards"):
        activate_slot_set_cursor(cursor, COHORT, 2, activated_at=NOW + timedelta(minutes=10))
    with pytest.raises(ValueError, match="not persisted"):
        activate_slot_set_cursor(cursor, COHORT, 3, activated_at=NOW + timedelta(minutes=10))
    assert store.current[str(COHORT)] == (2, NOW + timedelta(minutes=5))

    persist_slot_set_version_cursor(cursor, _refresh(version=3, ids=range(4, 9)))
    with pytest.raises(ValueError, match="precedes the current activation"):
        activate_slot_set_cursor(cursor, COHORT, 3, activated_at=NOW + timedelta(minutes=1))
    with pytest.raises(ValueError, match="cannot precede its effective time"):
        activate_slot_set_cursor(cursor, COHORT, 3, activated_at=NOW - timedelta(minutes=1))
    assert store.current[str(COHORT)][0] == 2


def test_a_failed_persist_leaves_the_active_pointer_untouched():
    store, cursor = _store()
    persist_and_activate_slot_set_cursor(cursor, _slot_set())
    store.catalog[1_003] = (99_999, UNDERLYING)

    with pytest.raises(ValueError, match="does not belong"):
        persist_and_activate_slot_set_cursor(cursor, _refresh(), activated_at=NOW + timedelta(minutes=5))

    assert store.current[str(COHORT)] == (1, NOW)
    assert (str(COHORT), 2) not in store.versions


def test_replaying_a_superseded_version_is_a_no_op_only_when_requested():
    store, cursor = _store()
    persist_and_activate_slot_set_cursor(cursor, _slot_set())
    persist_and_activate_slot_set_cursor(cursor, _refresh(), activated_at=NOW + timedelta(minutes=5))

    with pytest.raises(ValueError, match="backwards"):
        persist_and_activate_slot_set_cursor(cursor, _slot_set())
    assert persist_and_activate_slot_set_cursor(cursor, _slot_set(), allow_superseded=True) is False
    assert store.current[str(COHORT)] == (2, NOW + timedelta(minutes=5))
    with pytest.raises(ValueError, match="divergent replay"):
        persist_and_activate_slot_set_cursor(cursor, _slot_set(range(1, 7)), allow_superseded=True)
    assert store.current[str(COHORT)] == (2, NOW + timedelta(minutes=5))


def test_connection_wrapper_commits_once_or_rolls_back():
    store, cursor = _store()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    assert persist_and_activate_slot_set(connection, _slot_set()) is True
    assert (connection.commit.call_count, connection.rollback.call_count) == (1, 0)

    store.catalog[1_003] = (99_999, UNDERLYING)
    with pytest.raises(ValueError):
        persist_and_activate_slot_set(connection, _refresh())
    assert (connection.commit.call_count, connection.rollback.call_count) == (1, 1)
    with pytest.raises(TypeError, match="validated SlotSetVersion"):
        persist_slot_set_version_cursor(cursor, object())
