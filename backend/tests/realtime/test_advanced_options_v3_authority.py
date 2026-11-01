from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5
from unittest.mock import MagicMock

import pytest

from realtime.advanced_options_control_plane import (
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsSocketBudget,
)
from realtime.advanced_options_v3_authority import (
    AdvancedOptionsAuthorityBundle,
    OpeningAdmissionStatus,
    OpeningOpenInterestEvidence,
    OpeningReferenceEvidence,
    TargetWindowAgePolicy,
    advanced_options_authority_bundle_payload,
    advanced_options_authority_preflight_cursor,
    advanced_options_authority_status_cursor,
    _opening_admission,
    build_opening_subscription_readiness,
    build_prospective_opening_admission,
    build_session_acquisition_authorization,
    build_window_cycle_authorization,
    late_canary_opening_admission,
    prospective_opening_admission_from_payload,
    prospective_opening_admission_payload,
)
from realtime.contracts import MarketChannel, UniverseManifest, canonical_sha256
from realtime.option_pre_session_producer import OptionPreSessionCollection
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from scripts.prepare_advanced_options_v3_pre_session import _select
from realtime.preflight import ProviderTier
from realtime.provider_budget import ProviderBudgetScope
from tests.realtime.test_option_pre_session_producer import (
    _packages,
    _producer,
    _request,
)
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _advanced_policy,
    _candidates,
    _scope,
)


OPENING = MARKET_TIME + timedelta(minutes=30)


def _pre_session_result():
    universe = UniverseManifest(
        "prospective-opening-qqq-spy-v1",
        ("QQQ", "SPY"),
        ("QQQ", "SPY"),
    )
    policy = _advanced_policy()
    scope = _scope(ProviderTier.MASSIVE_ADVANCED, universe)
    candidates = _candidates(
        universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    packages = _packages(scope, policy, candidates)
    collection = OptionPreSessionCollection(
        packages,
        2,
        sum(row.evidence.response_bytes for row in packages),
    )
    producer, _, _ = _producer(collection)
    return producer.run_once(_request(scope, policy))


def _reference(result):
    included = {
        row.contract_id
        for row in result.plan.dispositions
        if row.status.value == "INCLUDED"
    }
    members = tuple(
        OpeningOpenInterestEvidence(
            contract_id=row.contract_id,
            contract_ticker=row.contract_ticker,
            underlyer=row.underlyer,
            open_interest=row.open_interest,
            known_at=MARKET_TIME + timedelta(seconds=3),
            member_sha256=canonical_sha256({
                "contract_id": row.contract_id,
                "open_interest": row.open_interest,
            }),
        )
        for row in result.snapshots
        if row.contract_id in included and row.open_interest is not None
    )
    return OpeningReferenceEvidence(
        state_id=uuid5(NAMESPACE_URL, "prospective-opening-reference"),
        state_sha256="1" * 64,
        reference_generation=7,
        session_date=SESSION_DATE,
        known_at=MARKET_TIME + timedelta(seconds=5),
        valid_until=OPENING + timedelta(hours=1),
        reference_policy_sha256="2" * 64,
        contract_scope_sha256="3" * 64,
        open_interest_sha256=canonical_sha256(
            tuple(row.member_sha256 for row in members)
        ),
        open_interest=members,
    )


def _admission():
    result = _pre_session_result()
    return build_prospective_opening_admission(
        result,
        _reference(result),
        prepared_at=MARKET_TIME + timedelta(minutes=10),
        capture_boundary=OPENING,
    )


def test_configured_admission_round_trips_asymmetric_missing_oi():
    base = _admission()
    contracts = tuple(
        (
            replace(
                row,
                open_interest=None,
                open_interest_member_sha256=None,
            )
            if row.underlyer == "QQQ"
            else row
        )
        for row in base.contracts
        if row.contract_type == "CALL"
    )
    admission = _opening_admission(
        status=OpeningAdmissionStatus.READY,
        session_date=base.session_date,
        prepared_at=base.prepared_at,
        capture_boundary=base.capture_boundary,
        pre_session_selection_id=base.pre_session_selection_id,
        pre_session_selection_sha256=(
            base.pre_session_selection_sha256
        ),
        source_evidence_sha256=base.source_evidence_sha256,
        reference_evidence_sha256=(
            base.reference_evidence_sha256
        ),
        contracts=contracts,
        policy_sha256="9" * 64,
    )

    restored = prospective_opening_admission_from_payload(
        prospective_opening_admission_payload(admission)
    )

    assert restored == admission
    assert {
        row.contract_type for row in restored.contracts
    } == {"CALL"}
    assert next(
        row
        for row in restored.contracts
        if row.underlyer == "QQQ"
    ).open_interest is None


def _session_authority(cohort_id=None, *, decoded_messages=100_000):
    admission = _admission()
    cohort = cohort_id or uuid5(
        NAMESPACE_URL,
        "prospective-opening-cohort",
    )
    instruments = {
        row.contract_ticker: index + 1000
        for index, row in enumerate(admission.contracts)
    }
    members = tuple(
        SlotSetMember(
            instrument_id=instruments[row.contract_ticker],
            channel=channel,
            purpose=SlotSetPurpose.PRIMARY,
            priority=index + 1,
            catalog_version_id=row.catalog_version_id,
        )
        for index, row in enumerate(admission.contracts)
        for channel in (MarketChannel.QUOTE, MarketChannel.TRADE)
    )
    slot_set = SlotSetVersion(
        cohort_id=cohort,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=OPENING - timedelta(minutes=10),
        policy_sha256=admission.policy_sha256,
        members=members,
    )
    readiness = build_opening_subscription_readiness(
        admission,
        slot_set,
        instrument_id_by_contract_ticker=instruments,
        journal_generation=uuid5(
            NAMESPACE_URL,
            f"prospective-opening-journal:{cohort}",
        ),
        journal_identity_sha256="4" * 64,
        ready_at=OPENING - timedelta(minutes=5),
    )
    reference = "V3-PROSPECTIVE-OPENING-TEST"
    stop = OPENING + timedelta(minutes=30)
    provider_budget = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=SESSION_DATE,
        authorization_reference=reference,
        provider_authorization_reference="provider-test",
        expected_cost="bounded-test",
        request_ceiling=2,
        decoded_byte_ceiling=100_000_000,
        lanes=ADVANCED_OPTIONS_REST_LANES,
        authorized_until=stop,
    )
    session = build_session_acquisition_authorization(
        admission,
        readiness,
        provider_budget,
        AdvancedOptionsSocketBudget(
            stream_sessions=1,
            connection_attempts=2,
            reconnects=1,
            decoded_messages=decoded_messages,
            decoded_bytes=100_000_000,
        ),
        authorization_reference=reference,
        prepared_at=OPENING - timedelta(minutes=2),
        acquisition_start=OPENING - timedelta(minutes=1),
        acquisition_stop=stop,
    )
    return admission, readiness, session


def _authority_bundle(
    *,
    cohort_id: UUID | None = None,
    target_window_id: UUID | None = None,
    equity_receipt_id: UUID | None = None,
    options_age_seconds: int = 120,
    decoded_messages: int = 100_000,
    equity_receipt_created_at=None,
    publication_authorization_reference=None,
):
    admission, readiness, session = _session_authority(
        cohort_id,
        decoded_messages=decoded_messages,
    )
    equity_cutoff = OPENING + timedelta(minutes=10)
    options_cutoff = equity_cutoff + timedelta(seconds=options_age_seconds)
    cycle = build_window_cycle_authorization(
        session,
        authorization_reference="V3-WINDOW-CYCLE-TEST",
        target_window_id=target_window_id or uuid5(
            NAMESPACE_URL,
            f"prospective-window:{session.cohort_id}",
        ),
        target_cohort_id=session.cohort_id,
        prepared_at=(
            equity_cutoff
            if options_age_seconds == 0
            else equity_cutoff + timedelta(seconds=1)
        ),
        history_binding_sha256="5" * 64,
        equity_binding_sha256="6" * 64,
        equity_receipt_id=equity_receipt_id or uuid5(
            NAMESPACE_URL,
            f"prospective-equity-receipt:{session.cohort_id}",
        ),
        equity_cutoff=equity_cutoff,
        equity_receipt_created_at=(
            equity_cutoff
            if equity_receipt_created_at is None
            else equity_receipt_created_at
        ),
        options_cutoff=options_cutoff,
        age_policy=TargetWindowAgePolicy(
            "advanced-options-e-o-age-v1",
            300,
        ),
        evidence_sha256="7" * 64,
        build_id="advanced-options-v3-authority-test-v1",
        publication_authorization_reference=(
            publication_authorization_reference
        ),
    )
    return AdvancedOptionsAuthorityBundle(
        session=session,
        admission=admission,
        readiness=readiness,
        cycle=cycle,
    )


def test_prospective_opening_admission_uses_only_frozen_pre_session_inputs():
    admission = _admission()

    assert admission.status is OpeningAdmissionStatus.READY
    assert [row.underlyer for row in admission.contracts] == [
        "QQQ",
        "QQQ",
        "SPY",
        "SPY",
    ]
    assert {
        (row.underlyer, row.contract_type)
        for row in admission.contracts
    } == {
        ("QQQ", "CALL"),
        ("QQQ", "PUT"),
        ("SPY", "CALL"),
        ("SPY", "PUT"),
    }
    assert all(row.open_interest >= 100 for row in admission.contracts)
    assert all(row.day_volume >= 20 for row in admission.contracts)
    assert admission.prepared_at < admission.capture_boundary
    assert admission.opening_capture_ready is True
    assert admission.opening_coverage_claimed is False
    assert admission.accumulation_900_second_coverage_claimed is False


def test_prospective_opening_admission_payload_round_trips_exactly():
    admission = _admission()
    payload = prospective_opening_admission_payload(admission)

    assert prospective_opening_admission_from_payload(payload) == admission

    changed = {
        **payload,
        "prepared_at": payload["capture_boundary"],
    }
    with pytest.raises(
        ValueError,
        match="clocks or claims are invalid",
    ):
        prospective_opening_admission_from_payload(changed)


def test_pre_session_snapshot_selection_keeps_call_and_put_per_underlyer():
    def candidate(underlyer, option_type, ticker, moneyness):
        return SimpleNamespace(
            underlyer=underlyer,
            option_type=option_type,
            contract_ticker=ticker,
            moneyness_fraction=moneyness,
            open_interest=100,
            day_volume=20,
            relative_spread=moneyness,
        )

    selected = _select(tuple(
        candidate(
            underlyer,
            option_type,
            f"O:{underlyer}{option_type}{index}",
            moneyness,
        )
        for underlyer in ("QQQ", "SPY")
        for option_type in ("C", "P")
        for index, moneyness in enumerate((0.02, 0.01))
    ))

    assert len(selected) == 4
    assert all(row.moneyness_fraction == 0.01 for row in selected)
    assert {
        (row.underlyer, row.option_type)
        for row in selected
    } == {
        ("QQQ", "C"),
        ("QQQ", "P"),
        ("SPY", "C"),
        ("SPY", "P"),
    }


def test_prospective_opening_admission_refuses_missing_ranking_inputs():
    result = _pre_session_result()
    unavailable = build_prospective_opening_admission(
        replace(result, catalog=(), snapshots=()),
        _reference(result),
        prepared_at=MARKET_TIME + timedelta(minutes=10),
        capture_boundary=OPENING,
    )

    assert unavailable.status is OpeningAdmissionStatus.UNAVAILABLE
    assert unavailable.reason_codes == (
        "PRE_SESSION_RANKING_INPUTS_UNAVAILABLE",
    )
    assert unavailable.contracts == ()
    assert unavailable.opening_capture_ready is False


def test_post_e_activity_selection_is_explicitly_late_canary_only():
    admission = late_canary_opening_admission(
        session_date=SESSION_DATE,
        prepared_at=OPENING - timedelta(seconds=1),
        capture_boundary=OPENING,
        activity_selection_sha256="8" * 64,
    )

    assert admission.status is OpeningAdmissionStatus.UNAVAILABLE
    assert "POST_E_ACTIVITY_RANKED_LATE_CANARY" in admission.reason_codes
    assert admission.opening_coverage_claimed is False
    assert admission.accumulation_900_second_coverage_claimed is False


def test_session_authority_binds_ready_tq_slots_and_zero_serving_authority():
    _, readiness, authority = _session_authority()
    payload = advanced_options_authority_bundle_payload(
        AdvancedOptionsAuthorityBundle(
            session=authority,
            admission=_admission(),
            readiness=readiness,
        )
    )
    granted = payload["session_acquisition"]["authority"]

    assert readiness.ready_at < readiness.capture_boundary
    assert all(row[4] == ("Q", "T") for row in readiness.slots)
    assert granted["provider_request"] is True
    assert granted["provider_socket"] is True
    for denied in (
        "options_cycle",
        "projection",
        "publication",
        "pointer",
        "dispatch",
        "delivery",
        "broker",
        "trading",
    ):
        assert granted[denied] is False


@pytest.mark.parametrize("options_age_seconds", [0, 1, 300])
def test_window_cycle_authority_accepts_exact_e_o_age_boundaries(
    options_age_seconds,
):
    bundle = _authority_bundle(options_age_seconds=options_age_seconds)
    payload = advanced_options_authority_bundle_payload(bundle)
    granted = payload["window_cycle"]["authority"]

    assert granted["provider_request"] is False
    assert granted["provider_socket"] is False
    assert granted["target_window_write"] is True
    assert granted["target_window_stage"] is True
    assert granted["publication"] is False


def test_window_cycle_authority_refuses_301_seconds():
    with pytest.raises(ValueError, match="H/E/O clocks"):
        _authority_bundle(options_age_seconds=301)


def test_window_cycle_authority_refuses_pre_receipt_preparation():
    with pytest.raises(ValueError, match="H/E/O clocks"):
        _authority_bundle(
            equity_receipt_created_at=(
                OPENING
                + timedelta(minutes=10, seconds=2)
            ),
        )


def test_window_cycle_authority_does_not_grant_publication_by_reference():
    bundle = _authority_bundle(
        publication_authorization_reference="SEPARATE-PUBLICATION-TEST",
    )
    granted = advanced_options_authority_bundle_payload(
        bundle
    )["window_cycle"]["authority"]

    assert granted["publication"] is False
    assert granted["pointer"] is False


def test_window_cycle_authority_recomputes_age_policy_identity():
    bundle = _authority_bundle()
    with pytest.raises(ValueError, match="age policy identity changed"):
        replace(bundle.cycle, age_policy_sha256="f" * 64)


def test_authority_status_preflight_is_read_only_and_carries_cleanup_ids():
    bundle = _authority_bundle()
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    status = advanced_options_authority_status_cursor(cursor, bundle)

    sql = cursor.execute.call_args.args[0]
    assert sql.startswith("SELECT")
    assert "INSERT" not in sql
    assert status["session_acquisition"]["state"] == "PREPARED_UNCONSUMED"
    assert status["window_cycle"]["state"] == "PREPARED_UNCONSUMED"
    assert status["cleanup"]["immutable_session_authorization_id"] == str(
        bundle.session.authorization_id
    )
    assert status["cleanup"]["immutable_window_cycle_authorization_id"] == str(
        bundle.cycle.authorization_id
    )

    cursor.reset_mock()
    cursor.fetchall.return_value = []
    preflight = advanced_options_authority_preflight_cursor(
        cursor,
        bundle,
        checked_at=bundle.session.acquisition_start,
    )
    assert preflight["authority_receipts_created"] == 0
    assert all(
        call.args[0].startswith("SELECT")
        for call in cursor.execute.call_args_list
    )
