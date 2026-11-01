from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from realtime.acquisition_ledger import ProviderRequestLane
from realtime.advanced_options_acquisition_runtime import (
    acquisition_binding_from_payload,
    acquisition_binding_payload,
    build_acquisition_binding,
)
from realtime.advanced_options_selection_c6 import (
    C6CapacityError,
    C6ConnectionCapacity,
    C6ConsumerState,
    C6JournalRecord,
    C6ReadinessState,
    C6SelectionMember,
    C6UnderlyingReadiness,
    FutureDetectorActivation,
    FutureRequirementState,
    OptionSide,
    SourceTimestampUnit,
    build_c6_selection_plan,
    c6_consumer_state_payload,
    c6_event_span_milliseconds,
    c6_journal_record_from_payload,
    c6_journal_record_payload,
    c6_selection_plan_from_payload,
    c6_selection_plan_payload,
    load_advanced_options_selection_c6_policy,
    load_strategy_policy_identity,
    resolve_selection_requirements,
)
from realtime.advanced_realtime_session_scheduler import (
    AdvancedSessionProviderContract,
    SchedulerOptionsCapture,
)
from realtime.contracts import canonical_sha256
from realtime.provider_budget import ProviderBudgetScope
from tests.realtime.test_advanced_options_acquisition_runtime import (
    _authority,
)


UTC = timezone.utc
SESSION_DATE = date(2026, 11, 2)
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
DESIRED_REVISION_ID = UUID(
    "11111111-2222-3333-4444-555555555555"
)
ROOT = Path(__file__).resolve().parents[3]
STRATEGY_POLICY_PATH = (
    ROOT
    / "backend"
    / "options"
    / "policies"
    / "strategy_o3_credit_v2.json"
)


def _symbols(count: int) -> tuple[str, ...]:
    values = []
    index = 0
    while len(values) < count:
        number = index
        letters = ""
        while True:
            letters = chr(ord("A") + number % 26) + letters
            number = number // 26 - 1
            if number < 0:
                break
        values.append(letters)
        index += 1
    return tuple(values)


def _ticker(
    underlying: str,
    side: OptionSide,
    *,
    expiration: str = "261218",
    strike: int = 100_000,
) -> str:
    marker = "C" if side is OptionSide.CALL else "P"
    return f"O:{underlying}{expiration}{marker}{strike:08d}"


def _members(
    underlyings: tuple[str, ...],
    *,
    expiration: date = date(2026, 12, 18),
    expiration_code: str = "261218",
    flow_underlyings: frozenset[str] = frozenset(),
) -> tuple[C6SelectionMember, ...]:
    rows = []
    contract_id = 1
    for underlying in underlyings:
        for side in (OptionSide.CALL, OptionSide.PUT):
            ticker = _ticker(
                underlying,
                side,
                expiration=expiration_code,
            )
            rows.append(C6SelectionMember(
                contract_id=contract_id,
                contract_ticker=ticker,
                underlying=underlying,
                expiration=expiration,
                strike=Decimal("100"),
                option_side=side,
                multiplier=Decimal("100"),
                deliverable="STANDARD",
                catalog_version_id=10_000 + contract_id,
                catalog_payload_sha256=canonical_sha256({
                    "catalog": ticker,
                }),
                matching_strategies=(
                    "DIRECTIONAL_LONG_PREMIUM",
                ),
                contract_roles=(
                    "CALL_ANCHOR"
                    if side is OptionSide.CALL
                    else "PUT_ANCHOR",
                ),
                selection_reasons=(
                    "CONFIGURED_STRATEGY_COVERAGE",
                ),
                evidence_references=(
                    canonical_sha256({"evidence": ticker}),
                ),
                flow_eligible=underlying in flow_underlyings,
            ))
            contract_id += 1
    return tuple(rows)


def _readiness(
    underlyings: tuple[str, ...],
    *,
    overrides: dict[str, C6ReadinessState] | None = None,
) -> tuple[C6UnderlyingReadiness, ...]:
    overrides = overrides or {}
    return tuple(
        C6UnderlyingReadiness(
            underlying=underlying,
            catalog=overrides.get(
                underlying,
                C6ReadinessState.AVAILABLE,
            ),
            open_interest=overrides.get(
                underlying,
                C6ReadinessState.AVAILABLE,
            ),
            history=overrides.get(
                underlying,
                C6ReadinessState.AVAILABLE,
            ),
            feed_states=(
                ("OPTION_QUOTE", C6ReadinessState.AVAILABLE),
                ("OPTION_TRADE", C6ReadinessState.AVAILABLE),
            ),
            reason_codes=(
                ()
                if underlying not in overrides
                else ("UNDERLYING_NOT_READY",)
            ),
            observed_at=NOW,
            source_as_of=NOW - timedelta(minutes=1),
        )
        for underlying in underlyings
    )


def _capacity(
    *,
    limit: int = 1,
    active_options_connections: int = 0,
    active_quote_contract_budget: int = 900,
    maximum_contracts_per_shard: int = 10,
) -> C6ConnectionCapacity:
    return C6ConnectionCapacity(
        account_alias="MASSIVE_ADVANCED",
        provider_cluster="OPTIONS",
        configured_cluster_connection_limit=limit,
        provider_cluster_connection_limit=limit,
        active_cluster_connections=active_options_connections,
        proposed_physical_connections=1,
        quote_contract_ceiling=1000,
        active_quote_contract_budget=active_quote_contract_budget,
        quote_slot_reserve=(
            1000 - active_quote_contract_budget
        ),
        maximum_contracts_per_logical_shard=(
            maximum_contracts_per_shard
        ),
    )


def _plan(
    underlyings: tuple[str, ...],
    *,
    limit: int,
    maximum_contracts_per_shard: int = 10,
    previous_shards=(),
    readiness=None,
    members=None,
):
    policy = load_advanced_options_selection_c6_policy()
    strategy = load_strategy_policy_identity(
        STRATEGY_POLICY_PATH
    )
    selected = members or _members(
        underlyings,
        flow_underlyings=frozenset(
            set(underlyings) & {"QQQ", "SPY"}
        ),
    )
    return build_c6_selection_plan(
        session_date=SESSION_DATE,
        desired_revision_id=DESIRED_REVISION_ID,
        desired_snapshot_sha256=canonical_sha256({
            "desired": underlyings,
        }),
        selection_policy=policy,
        members=selected,
        readiness=readiness or _readiness(underlyings),
        capacity=_capacity(
            limit=limit,
            maximum_contracts_per_shard=(
                maximum_contracts_per_shard
            ),
        ),
        strategy_policies=(strategy,),
        catalog_identity=canonical_sha256({
            "catalog": underlyings,
        }),
        reference_identity=canonical_sha256({
            "reference": underlyings,
        }),
        source_as_of=NOW,
        previous_shards=previous_shards,
    )


def test_policy_loader_resolves_frozen_identity_and_default_zero_work():
    policy = load_advanced_options_selection_c6_policy()
    strategy = load_strategy_policy_identity(
        STRATEGY_POLICY_PATH
    )
    resolved = resolve_selection_requirements(
        policy,
        strategy_policy=strategy,
        enabled_strategies=(
            "INCOME_WHEEL",
            "DIRECTIONAL_DEBIT_SPREAD",
        ),
    )

    assert policy.flow_policy_sha256 == (
        "a09fa7207c4cff7e028e54ae42ff87400ea1670247906bd2b4b"
        "4c3ebbd3d2833"
    )
    assert len(policy.future_detectors) == 7
    assert resolved.active_future_feeds == ()
    assert {
        row.state for row in resolved.future_requirements
    } == {FutureRequirementState.DISABLED}
    assert all(
        not row.capture_enabled and not row.detector_enabled
        for row in resolved.future_requirements
    )


def test_capture_only_is_separate_from_detector_enablement():
    policy = load_advanced_options_selection_c6_policy()
    strategy = load_strategy_policy_identity(
        STRATEGY_POLICY_PATH
    )
    resolved = resolve_selection_requirements(
        policy,
        strategy_policy=strategy,
        enabled_strategies=(),
        future_activations=(
            FutureDetectorActivation(
                "SILENT_ACCUMULATION",
                capture_enabled=True,
                resolved_fields=frozenset(
                    policy.required_resolved_fields
                ),
            ),
        ),
    )
    requirement = next(
        row
        for row in resolved.future_requirements
        if row.detector_id == "SILENT_ACCUMULATION"
    )

    assert requirement.state is FutureRequirementState.CAPTURE_ONLY
    assert requirement.capture_enabled
    assert not requirement.detector_enabled
    assert "OPTION_TRADE" in requirement.feeds


@pytest.mark.parametrize(
    ("count", "maximum_contracts", "expected_shards"),
    (
        (1, 2, 1),
        (9, 6, 3),
        (50, 10, 10),
    ),
)
def test_representative_memberships_shard_without_truncation(
    count,
    maximum_contracts,
    expected_shards,
):
    underlyings = _symbols(count)
    plan = _plan(
        underlyings,
        limit=1,
        maximum_contracts_per_shard=maximum_contracts,
    )
    rehydrated = c6_selection_plan_from_payload(
        c6_selection_plan_payload(plan)
    )

    assert len(plan.members) == count * 2
    assert len(plan.shards) == expected_shards
    assert sum(
        len(row.contract_tickers) for row in plan.shards
    ) == count * 2
    assert {
        row.physical_subscription_id for row in plan.shards
    } == {plan.physical_subscription.subscription_id}
    assert plan.capacity.required_cluster_connections == 1
    assert plan.capacity.logical_shard_count == expected_shards
    assert rehydrated == plan


def test_exact_four_remains_default_supported_fixture():
    plan = _plan(
        ("QQQ", "SPY"),
        limit=3,
        maximum_contracts_per_shard=4,
    )

    assert len(plan.members) == 4
    assert plan.flow_underlyings == ("QQQ", "SPY")
    assert len(plan.shards) == 1
    assert plan.product_ready(("QQQ", "SPY"))


def test_nine_underlying_acquisition_and_scheduler_contracts_are_configured():
    underlyings = _symbols(9)
    tickers = tuple(
        _ticker(underlying, side)
        for underlying in underlyings
        for side in (OptionSide.CALL, OptionSide.PUT)
    )
    binding = build_acquisition_binding(
        source_sha256="1" * 64,
        backend_image_id=f"sha256:{'2' * 64}",
        authority=_authority(),
        contract_tickers=tickers,
        connection_attempt_ceiling=3,
        decoded_message_ceiling=100_000,
        decoded_byte_ceiling=10_000_000,
    )
    restored = acquisition_binding_from_payload(
        acquisition_binding_payload(binding)
    )
    scheduler_contract = AdvancedSessionProviderContract(
        budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=SESSION_DATE,
            authorization_reference="c6-nine-underlying",
            provider_authorization_reference="provider-free",
            expected_cost="provider-free fixture",
            request_ceiling=1,
            decoded_byte_ceiling=1_000_000,
            lanes=frozenset({
                ProviderRequestLane.OPTIONS_ALIGNMENT,
            }),
            authorized_until=NOW + timedelta(hours=8),
        ),
        response_byte_ceiling=1_000_000,
        contract_tickers=tickers,
    )
    capture = SchedulerOptionsCapture(
        evidence_sha256="3" * 64,
        reviewed_at=NOW,
        contract_tickers=tickers,
        request_count=1,
        pagination_requests=0,
    )

    assert restored.contract_tickers == tickers
    assert scheduler_contract.contract_tickers == tickers
    assert capture.contract_tickers == tickers
    assert len(capture.contract_tickers) == 18


def test_add_remove_preserves_shard_identity_and_old_evidence():
    original = _plan(
        ("A", "B"),
        limit=3,
        maximum_contracts_per_shard=6,
    )
    original_document = c6_selection_plan_payload(original)
    added = _plan(
        ("A", "B", "C"),
        limit=3,
        maximum_contracts_per_shard=6,
        previous_shards=original.shards,
    )
    removed = _plan(
        ("A", "C"),
        limit=3,
        maximum_contracts_per_shard=6,
        previous_shards=added.shards,
    )

    assert added.shards[0].shard_id == original.shards[0].shard_id
    assert removed.shards[0].shard_id == original.shards[0].shard_id
    assert c6_selection_plan_payload(original) == original_document
    assert "B" in original.broad_underlyings
    assert "B" not in removed.broad_underlyings


def test_warming_or_failed_underlying_does_not_hide_ready_members():
    underlyings = ("A", "B", "C")
    plan = _plan(
        underlyings,
        limit=3,
        readiness=_readiness(
            underlyings,
            overrides={
                "B": C6ReadinessState.WARMING,
                "C": C6ReadinessState.FAILED,
            },
        ),
    )

    assert plan.ready_underlyings == ("A",)
    assert plan.product_ready(("A",))
    assert not plan.product_ready(
        underlyings,
        complete_membership_required=True,
    )


def test_second_options_connection_is_rejected_without_truncation():
    underlyings = ("A",)
    members = _members(underlyings)
    with pytest.raises(C6CapacityError) as raised:
        build_c6_selection_plan(
            session_date=SESSION_DATE,
            desired_revision_id=DESIRED_REVISION_ID,
            desired_snapshot_sha256="1" * 64,
            selection_policy=(
                load_advanced_options_selection_c6_policy()
            ),
            members=members,
            readiness=_readiness(underlyings),
            capacity=_capacity(
                limit=1,
                active_options_connections=1,
                maximum_contracts_per_shard=2,
            ),
            strategy_policies=(
                load_strategy_policy_identity(
                    STRATEGY_POLICY_PATH
                ),
            ),
            catalog_identity="catalog-v1",
            reference_identity="reference-v1",
            source_as_of=NOW,
        )

    decision = raised.value.decision
    assert not decision.accepted
    assert decision.required_cluster_connections == 2
    assert decision.reason_codes == (
        "OPTIONS_CLUSTER_CONNECTION_CAPACITY_EXCEEDED",
    )
    assert len(members) == 2


def test_quote_budget_rejects_without_silent_contract_truncation():
    underlyings = ("A",)
    members = _members(underlyings)
    with pytest.raises(C6CapacityError) as raised:
        build_c6_selection_plan(
            session_date=SESSION_DATE,
            desired_revision_id=DESIRED_REVISION_ID,
            desired_snapshot_sha256="1" * 64,
            selection_policy=(
                load_advanced_options_selection_c6_policy()
            ),
            members=members,
            readiness=_readiness(underlyings),
            capacity=_capacity(
                active_quote_contract_budget=1,
                maximum_contracts_per_shard=2,
            ),
            strategy_policies=(
                load_strategy_policy_identity(
                    STRATEGY_POLICY_PATH
                ),
            ),
            catalog_identity="catalog-v1",
            reference_identity="reference-v1",
            source_as_of=NOW,
        )

    assert raised.value.decision.reason_codes == (
        "OPTIONS_ACTIVE_QUOTE_BUDGET_EXCEEDED",
    )
    assert len(members) == 2


def test_long_dated_pairs_and_expiry_neighborhood_rehydrate():
    underlyings = ("SPY",)
    near = _members(
        underlyings,
        expiration=date(2027, 3, 19),
        expiration_code="270319",
    )
    far = tuple(
        C6SelectionMember(
            contract_id=row.contract_id + 10,
            contract_ticker=_ticker(
                row.underlying,
                row.option_side,
                expiration="280121",
                strike=110_000,
            ),
            underlying=row.underlying,
            expiration=date(2028, 1, 21),
            strike=Decimal("110"),
            option_side=row.option_side,
            multiplier=row.multiplier,
            deliverable=row.deliverable,
            catalog_version_id=row.catalog_version_id + 10,
            catalog_payload_sha256=canonical_sha256({
                "far": row.contract_ticker,
            }),
            matching_strategies=("SILENT_ACCUMULATION",),
            contract_roles=("PAIRED_CONTRACT",),
            selection_reasons=(
                "FUTURE_CAPTURE_LONG_DATED_PAIR",
            ),
            evidence_references=("future-capture-v1",),
            flow_eligible=False,
        )
        for row in near
    )
    plan = _plan(
        underlyings,
        limit=3,
        maximum_contracts_per_shard=4,
        members=(*near, *far),
    )
    restored = c6_selection_plan_from_payload(
        c6_selection_plan_payload(plan)
    )

    assert {
        row.expiration for row in restored.members
    } == {date(2027, 3, 19), date(2028, 1, 21)}
    assert {
        (row.expiration, row.option_side)
        for row in restored.members
    } == {
        (date(2027, 3, 19), OptionSide.CALL),
        (date(2027, 3, 19), OptionSide.PUT),
        (date(2028, 1, 21), OptionSide.CALL),
        (date(2028, 1, 21), OptionSide.PUT),
    }
    assert restored.flow_underlyings == ()


@pytest.mark.parametrize("span_ms", (49, 50, 51))
def test_event_time_precision_and_provenance_survive_replay(span_ms):
    records = tuple(
        C6JournalRecord(
            source_kind="OPTION_TRADE",
            source_schema="massive.options.v3",
            instrument_key="O:SPY261218C00100000",
            raw_timestamp_value=1_000 + offset,
            raw_timestamp_unit=SourceTimestampUnit.MILLISECOND,
            received_at=NOW + timedelta(milliseconds=offset + 3),
            source_sequence=f"sequence-{offset}",
            trade_id=f"trade-{offset}",
            conditions=("209", "37"),
            exchange="CBOE",
            correction_code=(
                None if offset == 0 else "CORRECTED"
            ),
            cancellation=False,
            payload={"price": "1.25", "size": 10},
        )
        for offset in (0, span_ms)
    )
    replayed = tuple(
        c6_journal_record_from_payload(
            c6_journal_record_payload(row)
        )
        for row in records
    )

    assert replayed == records
    assert c6_event_span_milliseconds(replayed) == Decimal(span_ms)
    assert replayed[1].correction_code == "CORRECTED"
    assert replayed[1].conditions == ("209", "37")
    assert replayed[1].source_schema == "massive.options.v3"


def test_synthetic_consumers_attach_with_independent_offsets_and_watermarks():
    primary = C6ConsumerState(
        consumer_name="flow-v1",
        state_version="flow-v1-state",
        committed_offset=500,
        event_watermark_ns=1_000_000_000,
        history_start=NOW - timedelta(days=3),
        history_end=NOW,
        missing_feed_states=(),
        updated_at=NOW,
    )
    synthetic = C6ConsumerState(
        consumer_name="synthetic-future-consumer",
        state_version="synthetic-v1",
        committed_offset=49,
        event_watermark_ns=49_000_000,
        history_start=None,
        history_end=None,
        missing_feed_states=("INDEX_OBSERVATION_UNAVAILABLE",),
        updated_at=NOW,
    )

    assert primary.committed_offset == 500
    assert synthetic.committed_offset == 49
    assert primary.event_watermark_ns != synthetic.event_watermark_ns
    assert c6_consumer_state_payload(synthetic)[
        "missing_feed_states"
    ] == ["INDEX_OBSERVATION_UNAVAILABLE"]
