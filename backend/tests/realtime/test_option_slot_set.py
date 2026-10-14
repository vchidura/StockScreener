from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    SubscriptionPurpose,
    UniverseManifest,
)
from realtime.option_slot_set import (
    MAXIMUM_SLOT_SET_CONTRACTS,
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
    equity_manifest_members,
    option_manifest_members,
    option_universe_manifest_sha256,
    pre_session_slot_set,
    slot_members_from_manifest,
    slot_set_member_sha256,
)

UTC = timezone.utc
NOW = datetime(2026, 10, 5, 13, 0, tzinfo=UTC)
COHORT = uuid5(NAMESPACE_URL, "slot-set-test-cohort")
POLICY = "a" * 64
T, Q = MarketChannel.TRADE, MarketChannel.QUOTE


def _member(instrument_id, channel=Q, purpose=SlotSetPurpose.PRIMARY, priority=100, catalog=None):
    return SlotSetMember(instrument_id, channel, purpose, priority, catalog or instrument_id + 1_000)


def _slot_set(members, *, version=1, kind=SlotSetKind.PRE_SESSION, **overrides):
    values = dict(cohort_id=COHORT, version=version, kind=kind, effective_at=NOW, policy_sha256=POLICY,
                  members=tuple(members))
    values.update(overrides)
    return SlotSetVersion(**values)


def _contracts(count, purpose=SlotSetPurpose.PRIMARY, start=1):
    return [_member(start + index, channel, purpose) for index in range(count) for channel in (T, Q)]


def test_member_hash_is_order_independent_and_tracks_every_stored_column():
    members = [_member(1), _member(1, T), _member(2), _member(2, T)]
    base = slot_set_member_sha256(members)
    assert base == slot_set_member_sha256(reversed(members))
    assert base == _slot_set(members).member_sha256
    for changed in (
        replace(members[0], priority=101),
        replace(members[0], purpose=SlotSetPurpose.RESERVE),
        replace(members[0], catalog_version_id=members[0].catalog_version_id + 1),
        replace(members[0], instrument_id=9),
    ):
        assert slot_set_member_sha256([changed, *members[1:]]) != base
    assert slot_set_member_sha256(members[:-1]) != base


def test_members_are_stored_in_primary_key_order_and_counts_are_per_contract():
    slot_set = _slot_set(_contracts(3) + _contracts(2, SlotSetPurpose.RESERVE, start=10)
                         + _contracts(1, SlotSetPurpose.RETAINED_ALERT, start=20))
    assert [row.sort_key for row in slot_set.members] == sorted(row.sort_key for row in slot_set.members)
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count, slot_set.contract_count) == (3, 2, 6)
    assert slot_set.effective_at == NOW and len(slot_set.members) == 12


def test_effective_time_is_normalised_to_utc_and_must_be_aware():
    eastern = timezone(timedelta(hours=-4))
    assert _slot_set(_contracts(1), effective_at=NOW.astimezone(eastern)).effective_at == NOW
    with pytest.raises(ValueError, match="timezone-aware"):
        _slot_set(_contracts(1), effective_at=NOW.replace(tzinfo=None))


def test_provider_cap_is_enforced_in_python_per_contract_not_per_row():
    assert _slot_set(_contracts(MAXIMUM_SLOT_SET_CONTRACTS)).contract_count == 1_000
    split = _contracts(600) + _contracts(400, SlotSetPurpose.RESERVE, start=601)
    assert (_slot_set(split).primary_contract_count, _slot_set(split).reserve_contract_count) == (600, 400)
    with pytest.raises(ValueError, match="1000 contract provider cap"):
        _slot_set(_contracts(MAXIMUM_SLOT_SET_CONTRACTS + 1))
    retained = _contracts(600) + _contracts(400, SlotSetPurpose.RESERVE, start=601) \
        + _contracts(1, SlotSetPurpose.RETAINED_ALERT, start=2_000)
    with pytest.raises(ValueError, match="provider cap"):
        _slot_set(retained)


@pytest.mark.parametrize("changes, message", [
    (dict(version=2, kind=SlotSetKind.PRE_SESSION), "PRE_SESSION slot set is version 1"),
    (dict(version=1, kind=SlotSetKind.REFRESH), "REFRESH slot set follows"),
    (dict(version=0), "positive integer"),
    (dict(policy_sha256="G" * 64), "policy_sha256"),
    (dict(members=()), "typed members"),
    (dict(cohort_id="not-a-uuid"), "cohort_id"),
])
def test_version_rules_fail_closed(changes, message):
    with pytest.raises(ValueError, match=message):
        _slot_set(**{**dict(members=_contracts(1)), **changes})


def test_member_rules_fail_closed():
    with pytest.raises(ValueError, match="channel must be T or Q"):
        SlotSetMember(1, MarketChannel.MINUTE_AGGREGATE, SlotSetPurpose.PRIMARY, 100, 5)
    with pytest.raises(ValueError, match="priority"):
        SlotSetMember(1, Q, SlotSetPurpose.PRIMARY, 0, 5)
    with pytest.raises(ValueError, match="catalog_version_id"):
        SlotSetMember(1, Q, SlotSetPurpose.PRIMARY, 100, 0)
    with pytest.raises(ValueError, match="duplicate"):
        _slot_set([_member(1), _member(1)])
    with pytest.raises(ValueError, match="one purpose"):
        _slot_set([_member(1, T), _member(1, Q, SlotSetPurpose.RESERVE)])


def _manifest(options=(("O:AAPL261218C00100000", "AAPL"), ("O:MSFT261218P00200000", "MSFT")), *, policy="b" * 64,
              tickers=("AAPL", "MSFT"), purposes=None):
    universe = UniverseManifest("slot-set-test-universe", tickers, tickers)
    equity = tuple(SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
                   for ticker in tickers for channel in (T, Q, MarketChannel.MINUTE_AGGREGATE))
    purposes = purposes or {}
    members = tuple(
        SubscriptionMember(AssetClass.OPTION, symbol, underlying, channel,
                           purpose=purposes.get(symbol, SubscriptionPurpose.PRIMARY),
                           catalog_version_id=500 + index)
        for index, (symbol, underlying) in enumerate(options) for channel in (T, Q))
    return SubscriptionManifest(
        "slot-set-test-manifest", NOW.date(), universe, "c" * 64, policy, equity + members,
        maximum_option_quote_contracts=10)


def test_manifest_identity_covers_the_equity_universe_but_not_the_option_contracts():
    first = _manifest()
    other_contracts = _manifest((("O:AAPL261218C00105000", "AAPL"), ("O:MSFT261218P00205000", "MSFT")))
    assert first.sha256 != other_contracts.sha256
    assert option_universe_manifest_sha256(first) == option_universe_manifest_sha256(other_contracts)
    assert option_universe_manifest_sha256(first) != option_universe_manifest_sha256(_manifest(policy="d" * 64))
    smaller = _manifest((("O:AAPL261218C00100000", "AAPL"),), tickers=("AAPL",))
    assert option_universe_manifest_sha256(first) != option_universe_manifest_sha256(smaller)
    assert len(equity_manifest_members(first)) == 6 and len(option_manifest_members(first)) == 4


def test_pre_session_slot_set_derives_primary_members_with_catalog_identity():
    manifest = _manifest()
    ids = {"O:AAPL261218C00100000": 11, "O:MSFT261218P00200000": 12}
    slot_set = pre_session_slot_set(manifest, cohort_id=COHORT, instrument_ids=ids, effective_at=NOW)
    assert (slot_set.version, slot_set.kind) == (1, SlotSetKind.PRE_SESSION)
    assert slot_set.policy_sha256 == manifest.allocation_policy_sha256
    assert (slot_set.primary_contract_count, slot_set.reserve_contract_count) == (2, 0)
    assert [(row.instrument_id, row.channel.value, row.catalog_version_id) for row in slot_set.members] == [
        (11, "Q", 500), (12, "Q", 501), (11, "T", 500), (12, "T", 501)]
    assert all(row.purpose is SlotSetPurpose.PRIMARY for row in slot_set.members)
    overridden = pre_session_slot_set(manifest, cohort_id=COHORT, instrument_ids=ids, effective_at=NOW,
                                      policy_sha256="e" * 64)
    assert overridden.policy_sha256 == "e" * 64 and overridden.member_sha256 == slot_set.member_sha256


def test_derivation_requires_exact_instrument_identities_and_maps_purposes():
    manifest = _manifest()
    with pytest.raises(ValueError, match="do not match"):
        slot_members_from_manifest(manifest, {"O:AAPL261218C00100000": 11})
    with pytest.raises(ValueError, match="do not match"):
        slot_members_from_manifest(manifest, {"O:AAPL261218C00100000": 11, "O:MSFT261218P00200000": 12, "O:X": 13})
    shared = slot_members_from_manifest(manifest, {"O:AAPL261218C00100000": 11, "O:MSFT261218P00200000": 11})
    with pytest.raises(ValueError, match="duplicate"):
        _slot_set(shared)
    retained = _manifest(purposes={"O:MSFT261218P00200000": SubscriptionPurpose.RETAINED_ALERT})
    derived = pre_session_slot_set(
        retained, cohort_id=COHORT, instrument_ids={"O:AAPL261218C00100000": 11, "O:MSFT261218P00200000": 12},
        effective_at=NOW)
    assert {row.instrument_id: row.purpose for row in derived.members} == {
        11: SlotSetPurpose.PRIMARY, 12: SlotSetPurpose.RETAINED_ALERT}
    assert (derived.primary_contract_count, derived.reserve_contract_count) == (1, 0)
