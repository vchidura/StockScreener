from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from advanced_options_reference_support import reference_state
from realtime.acquisition_ledger import ProviderRequestLane
from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
)
from realtime.advanced_options_control_plane import (
    ADVANCED_OPTIONS_CHANNELS,
    ADVANCED_OPTIONS_COHORT_KIND,
    ADVANCED_OPTIONS_REST_LANES,
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    OPTIONS_PORTAL_READY,
    PRESERVED_STAGE_CATALOGS,
    AdvancedOptionsFeedScope,
    AdvancedOptionsOwnerNames,
    AdvancedOptionsPolicyPins,
    AdvancedOptionsReceiptInputs,
    AdvancedOptionsRuntimeAuthorization,
    AdvancedOptionsSocketBudget,
    build_advanced_options_control_plane_plan,
)
from realtime.contracts import MarketChannel, MarketFeed
from realtime.option_session_manifest import BoundedOptionManifestPolicy
from realtime.option_slot_set import SlotSetMember, SlotSetPurpose
from realtime.preflight import (
    DetectorFamily,
    ProductEntitlementProfile,
    ProviderTier,
)
from realtime.provider_budget import ProviderBudgetScope


UTC = timezone.utc
SESSION = date(2026, 10, 16)
CHECKED = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
PLANNED = datetime(2026, 10, 16, 13, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
EXPIRES = datetime(2026, 10, 16, 23, 0, tzinfo=UTC)
JOURNAL = UUID("11111111-1111-1111-1111-111111111111")
STREAM = UUID("22222222-2222-2222-2222-222222222222")
EQUITY_RECEIPT = UUID("33333333-3333-3333-3333-333333333333")


def _equity_plan():
    return build_advanced_equity_control_plane_plan(
        session_date=SESSION,
        security_ids={
            "AAPL": UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            "SPY": UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        },
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-equity-v2-20261016",
        checked_at=CHECKED,
        expires_at=EXPIRES,
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )


def _manifest_policy(**changes):
    values = dict(
        version="advanced-options-p1-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=1,
        minimum_dte=1,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.20"),
        minimum_day_volume=20,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=2,
        maximum_option_quote_contracts=3,
        reserved_option_quote_contracts=1,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O2,
            DetectorFamily.O3,
        ),
    )
    values.update(changes)
    return BoundedOptionManifestPolicy(**values)


def _pins(**changes):
    values = dict(
        condition_semantics_sha256="1" * 64,
        flow_policy_sha256="2" * 64,
        valuation_policy_sha256="3" * 64,
        model_bundle_sha256="4" * 64,
        catalog_sha256="5" * 64,
        slot_set_policy_sha256="6" * 64,
    )
    values.update(changes)
    return AdvancedOptionsPolicyPins(**values)


def _slot_members(*, changed_priority=False):
    values = []
    for instrument_id, purpose in (
        (101, SlotSetPurpose.PRIMARY),
        (102, SlotSetPurpose.RESERVE),
    ):
        for channel in ADVANCED_OPTIONS_CHANNELS:
            values.append(SlotSetMember(
                instrument_id=instrument_id,
                channel=channel,
                purpose=purpose,
                priority=101 if changed_priority and instrument_id == 101 else 100,
                catalog_version_id=instrument_id + 1_000,
            ))
    return tuple(values)


def _provider_budget(**changes):
    values = dict(
        account_alias="MASSIVE_ADVANCED",
        session_date=SESSION,
        authorization_reference="advanced-options-p1-20261016",
        provider_authorization_reference="stocks-options-advanced",
        expected_cost="no incremental charge expected",
        request_ceiling=50,
        decoded_byte_ceiling=50_000_000,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=EXPIRES,
    )
    values.update(changes)
    return ProviderBudgetScope(**values)


def _receipt_inputs(**changes):
    state = changes.pop("reference_state", reference_state())
    values = dict(
        cutoff=CUTOFF,
        reference_state=state,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_sha256="7" * 64,
        discovery_batches_sha256="8" * 64,
        open_interest_sha256=state.open_interest_sha256,
        consumer_checkpoints_sha256="a" * 64,
        unresolved_gaps_sha256="b" * 64,
        market_state_sha256="c" * 64,
        source_clocks_sha256="d" * 64,
        detector_outcomes_sha256="e" * 64,
        projection_sha256="f" * 64,
        build_id="advanced-options-p1-build",
    )
    values.update(changes)
    return AdvancedOptionsReceiptInputs(**values)


def _plan(**changes):
    values = dict(
        equity_plan=_equity_plan(),
        entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_ADVANCED,
        ),
        option_underlyings=("SPY",),
        options_entitlement_evidence_sha256="0" * 64,
        options_authorization_reference="advanced-options-p1-20261016",
        checked_at=CHECKED,
        expires_at=EXPIRES,
        planned_at=PLANNED,
        manifest_policy=_manifest_policy(),
        slot_members=_slot_members(),
        slot_effective_at=PLANNED,
        feed_scope=AdvancedOptionsFeedScope(),
        pins=_pins(),
        socket_budget=AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=3,
            reconnects=2,
            decoded_messages=1_000_000,
            decoded_bytes=100_000_000,
        ),
        provider_budget=_provider_budget(),
        owners=AdvancedOptionsOwnerNames(),
        journal_generation=JOURNAL,
        stream_session_ids=(STREAM,),
        receipt_inputs=_receipt_inputs(),
    )
    values.update(changes)
    return build_advanced_options_control_plane_plan(**values)


def test_plan_is_exact_combined_advanced_scope_without_runtime_authority():
    plan = _plan()

    assert plan.entitlement.advanced_platform_ready
    assert plan.binding.stocks_tier is ProviderTier.MASSIVE_ADVANCED
    assert plan.binding.options_tier is ProviderTier.MASSIVE_ADVANCED
    assert DetectorFamily.O2 not in plan.binding.disabled_detectors
    assert plan.cohort_kind == ADVANCED_OPTIONS_COHORT_KIND
    assert plan.cohort_id != _equity_plan().cohort_id
    assert plan.universe.equity_tickers == ("AAPL", "SPY")
    assert plan.universe.option_underlyings == ("SPY",)
    assert plan.feed_scope.feed is MarketFeed.OPTIONS
    assert plan.feed_scope.channels == (
        MarketChannel.TRADE,
        MarketChannel.QUOTE,
    )
    assert plan.capability_only
    assert not plan.runtime_authorization.any_authorized
    assert not plan.runtime_enabled
    assert not plan.options_cycle_bound
    assert plan.stage_catalog_versions == PRESERVED_STAGE_CATALOGS


def test_plan_reuses_exact_slot_policy_budget_and_owner_contracts():
    plan = _plan()

    assert plan.slot_set.cohort_id == plan.cohort_id
    assert plan.slot_set.primary_contract_count == 1
    assert plan.slot_set.reserve_contract_count == 1
    assert plan.slot_set.contract_count == 2
    assert plan.slot_set.contract_count <= 1_000
    assert plan.slot_set.policy_sha256 == plan.pins.slot_set_policy_sha256
    assert plan.manifest_policy.primary_option_quote_contracts == 2
    assert plan.manifest_policy.reserved_option_quote_contracts == 1
    assert plan.provider_budget.lanes == frozenset({
        ProviderRequestLane.OPTIONS_CHAIN,
        ProviderRequestLane.OPTIONS_ALIGNMENT,
        ProviderRequestLane.REFERENCE,
    })
    assert plan.socket_budget.sha256 != plan.provider_budget.sha256
    assert plan.owners == AdvancedOptionsOwnerNames()


def test_receipts_have_product_specific_deterministic_identities():
    first = _plan()
    second = _plan()

    assert first.plan_id == second.plan_id
    assert first.plan_sha256 == second.plan_sha256
    assert first.receipts.ids == second.receipts.ids
    assert tuple(
        receipt.spec.receipt_kind
        for receipt in (
            first.receipts.options_input_ready,
            first.receipts.options_cycle,
            first.receipts.options_portal_ready,
        )
    ) == (OPTIONS_INPUT_READY, OPTIONS_CYCLE, OPTIONS_PORTAL_READY)
    assert all(
        not receipt.spec.advance_release
        for receipt in (
            first.receipts.options_input_ready,
            first.receipts.options_cycle,
            first.receipts.options_portal_ready,
        )
    )


@pytest.mark.parametrize(
    "changes",
    (
        {"options_entitlement_evidence_sha256": "f" * 64},
        {"slot_members": _slot_members(changed_priority=True)},
        {"pins": _pins(model_bundle_sha256="f" * 64)},
        {
            "socket_budget": AdvancedOptionsSocketBudget(
                stream_sessions=1,
                connection_attempts=3,
                reconnects=2,
                decoded_messages=1_000_001,
                decoded_bytes=100_000_000,
            )
        },
        {"provider_budget": _provider_budget(request_ceiling=51)},
        {
            "journal_generation": UUID(
                "44444444-4444-4444-4444-444444444444"
            )
        },
        {"receipt_inputs": _receipt_inputs(market_state_sha256="0" * 64)},
        {
            "receipt_inputs": _receipt_inputs(
                reference_state=reference_state(generation=8)
            )
        },
    ),
)
def test_every_relevant_valid_change_changes_plan_and_receipts(changes):
    original = _plan()
    changed = _plan(**changes)

    assert changed.plan_id != original.plan_id
    assert changed.plan_sha256 != original.plan_sha256
    assert changed.receipts.ids != original.receipts.ids


def test_wrong_products_feed_channels_and_owner_names_fail_explicitly():
    with pytest.raises(ValueError, match="Stocks Advanced plus Options Advanced"):
        _plan(entitlement=ProductEntitlementProfile(
            ProviderTier.MASSIVE_ADVANCED,
            ProviderTier.MASSIVE_DEVELOPER,
        ))
    with pytest.raises(ValueError, match="OPTIONS feed"):
        AdvancedOptionsFeedScope(feed=MarketFeed.STOCKS)
    with pytest.raises(ValueError, match="exact T/Q"):
        AdvancedOptionsFeedScope(channels=(MarketChannel.TRADE,))
    with pytest.raises(ValueError, match="canonical"):
        AdvancedOptionsOwnerNames(flow="second-flow-writer")


def test_options_input_receipt_binds_tier_a_reference_state():
    state = reference_state()
    plan = _plan(receipt_inputs=_receipt_inputs(reference_state=state))

    assert plan.receipts.options_input_ready.spec.reference_generation == (
        state.reference_generation
    )
    assert plan.receipts.options_input_ready.spec.input_sha256
    assert _plan(
        receipt_inputs=_receipt_inputs(
            reference_state=reference_state(generation=8)
        )
    ).receipts.options_input_ready.receipt_sha256 != (
        plan.receipts.options_input_ready.receipt_sha256
    )


def test_stale_mismatched_unpinned_and_incomplete_inputs_fail_explicitly():
    with pytest.raises(ValueError, match="stale or out of scope"):
        _plan(expires_at=CUTOFF)
    with pytest.raises(ValueError, match="provider-ledger scope"):
        _plan(provider_budget=_provider_budget(
            session_date=SESSION + timedelta(days=1)
        ))
    with pytest.raises(ValueError, match="SHA-256"):
        _pins(condition_semantics_sha256="")
    with pytest.raises(ValueError, match="reserve slots are incomplete"):
        _plan(slot_members=tuple(
            member
            for member in _slot_members()
            if member.purpose is SlotSetPurpose.PRIMARY
        ))
    with pytest.raises(ValueError, match="exact stream sessions"):
        _plan(stream_session_ids=())
    with pytest.raises(ValueError, match="Tier A reference state changed"):
        _plan(option_underlyings=("AAPL", "SPY"))


def test_over_cap_or_unpaired_slot_members_fail_through_slot_owner():
    over_cap = tuple(
        SlotSetMember(
            instrument_id=instrument_id,
            channel=channel,
            purpose=SlotSetPurpose.PRIMARY,
            priority=100,
            catalog_version_id=instrument_id + 10_000,
        )
        for instrument_id in range(1, 1_002)
        for channel in ADVANCED_OPTIONS_CHANNELS
    )
    with pytest.raises(ValueError, match="1000 contract provider cap"):
        _plan(slot_members=over_cap)
    with pytest.raises(ValueError, match="exact T/Q"):
        _plan(slot_members=_slot_members()[:-1])


@pytest.mark.parametrize(
    "field",
    (
        "acquisition",
        "o2_activation",
        "publication",
        "options_alerts_production",
        "strategy_activation",
        "execution",
        "delivery",
    ),
)
def test_capability_never_implies_runtime_authorization(field):
    authorization = replace(
        AdvancedOptionsRuntimeAuthorization(),
        **{field: True},
    )
    with pytest.raises(ValueError, match="does not authorize"):
        _plan(runtime_authorization=authorization)


def test_source_flow_lineage_has_no_retry_or_process_identity():
    plan = _plan()

    assert set(plan.source_flow.lineage_identity) == {
        "contract",
        "journal_generation",
        "stream_session_ids",
        "owner_name",
        "condition_semantics_sha256",
        "flow_policy_sha256",
    }
    assert "retry" not in plan.source_flow.source_flow_sha256
    assert "process" not in plan.source_flow.source_flow_sha256


def test_builder_has_zero_socket_filesystem_or_process_effects(monkeypatch):
    def reject_effect(*_args, **_kwargs):
        raise AssertionError("pure control-plane construction attempted an effect")

    monkeypatch.setattr("builtins.open", reject_effect)
    monkeypatch.setattr("socket.socket", reject_effect)
    monkeypatch.setattr("subprocess.Popen", reject_effect)

    assert _plan().capability_only


def test_contracts_are_immutable():
    plan = _plan()

    with pytest.raises(FrozenInstanceError):
        plan.runtime_enabled = True
    with pytest.raises(FrozenInstanceError):
        plan.source_flow.owner_name = "other"
