from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_producers import _producer_plan
from realtime.advanced_options_input import (
    MAX_ADVANCED_OPTIONS_INPUT_BYTES,
    build_advanced_options_input_closure,
    build_advanced_options_input_gate_plan,
    decode_advanced_options_input_closure,
    encode_advanced_options_input_closure,
    reconstruct_options_input_ready_receipt,
)
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.option_cycle_input import (
    OptionCycleInput,
    OptionCycleUnderlying,
)
from realtime.orchestration_repository import OPTIONS_INPUT_READY
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
SECURITY = UUID("11111111-2222-3333-4444-555555555555")
BATCH = UUID("66666666-7777-8888-9999-aaaaaaaaaaaa")
EQUITY_RECEIPT = UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
WINDOW = UUID("12121212-3434-5656-7878-909090909090")
EQUITY_WINDOW = UUID("abababab-cdcd-efef-1212-343434343434")


def _cycle_input(control=None):
    control = control or _control_plane()
    underlying = OptionCycleUnderlying(
        underlyer="SPY",
        underlying_security_id=SECURITY,
        discovery_batch_id=BATCH,
        discovery_evidence_sha256="1" * 64,
        payload_chain_sha256="2" * 64,
        coverage_sha256="3" * 64,
        request_filter_sha256="4" * 64,
        page_count=2,
        row_count=100,
        response_bytes=10_000,
        expiration_from=date(2026, 10, 23),
        expiration_through=date(2026, 12, 18),
        strike_min=Decimal("400"),
        strike_max=Decimal("600"),
    )
    return OptionCycleInput(
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        scheduled_cycle=NOW,
        profile_id=control.profile_id,
        profile_sha256=control.profile_sha256,
        universe_sha256=control.universe.sha256,
        policy_sha256=control.manifest_policy.sha256,
        configuration_sha256="5" * 64,
        code_sha256="6" * 64,
        cohort_id=control.cohort_id,
        underlyings=(underlying,),
        slot_set_version=control.slot_set.version,
        slot_set_member_sha256=control.slot_set.member_sha256,
    )


def _closure(**changes):
    control = _control_plane()
    state = changes.pop("reference_state", reference_state())
    rolling = changes.pop(
        "rolling_state",
        rolling_state(
            cohort_id=control.cohort_id,
            journal_generation=control.source_flow.journal_generation,
        ),
    )
    values = dict(
        control_plane=control,
        producer_plan=_producer_plan(),
        cycle_input=_cycle_input(control),
        reference_state=state,
        rolling_state=rolling,
        reference_policy_sha256=state.reference_policy_sha256,
        oi_facts_sha256=state.open_interest_sha256,
        market_state_checkpoint_sha256=(
            rolling.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=rolling.flow_checkpoint_sha256,
        unresolved_gaps_sha256="b" * 64,
        market_state_heads_sha256="c" * 64,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_sha256="d" * 64,
    )
    values.update(changes)
    return build_advanced_options_input_closure(**values)


def test_compact_closure_round_trip_is_deterministic_and_contains_no_pages():
    closure = _closure()
    text, digest = encode_advanced_options_input_closure(closure)
    decoded = decode_advanced_options_input_closure(text, digest)

    assert decoded == closure
    assert decoded.sha256 == digest
    assert len(text.encode("ascii")) <= MAX_ADVANCED_OPTIONS_INPUT_BYTES
    assert "payload_text" not in text
    assert "raw_page" not in text
    assert "base64" not in text


def test_every_live_input_change_changes_closure_identity():
    baseline = _closure()
    changed = (
        _closure(reference_state=reference_state(generation=8)),
        _closure(rolling_state=rolling_state(
            cohort_id=_control_plane().cohort_id,
            journal_generation=(
                _control_plane().source_flow.journal_generation
            ),
            committed_offset=12,
            committed_at=NOW + timedelta(seconds=1),
        )),
        _closure(unresolved_gaps_sha256="e" * 64),
        _closure(market_state_heads_sha256="e" * 64),
        _closure(equity_input_sha256="e" * 64),
    )

    assert len({baseline.sha256, *(row.sha256 for row in changed)}) == 6


def test_closure_binds_exact_tier_a_state_and_rejects_drift():
    state = reference_state()
    closure = _closure(reference_state=state)

    assert closure.reference_state_id == state.state_id
    assert closure.reference_state_sha256 == state.state_sha256
    assert closure.reference_generation == state.reference_generation
    assert closure.reference_known_at == state.known_at
    assert closure.reference_valid_until == state.valid_until
    with pytest.raises(ValueError, match="Tier A reference state changed"):
        _closure(
            reference_state=state,
            oi_facts_sha256="f" * 64,
        )


def test_closure_binds_exact_tier_b_state_and_rejects_drift():
    control = _control_plane()
    rolling = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=control.source_flow.journal_generation,
    )
    closure = _closure(rolling_state=rolling)

    assert closure.rolling_state_id == rolling.state_id
    assert closure.rolling_state_sha256 == rolling.state_sha256
    assert closure.rolling_owner_epoch == rolling.owner_epoch
    assert closure.rolling_committed_offset == rolling.committed_offset
    with pytest.raises(ValueError, match="Tier B rolling state changed"):
        _closure(
            rolling_state=rolling,
            flow_checkpoint_sha256="f" * 64,
        )


def test_input_gate_reuses_existing_jobs_and_exact_barrier_membership():
    closure = _closure()
    first = build_advanced_options_input_gate_plan(
        closure,
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p4a-build",
        available_at=NOW,
    )
    retry = build_advanced_options_input_gate_plan(
        decode_advanced_options_input_closure(
            first.closure_text,
            first.closure_sha256,
        ),
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p4a-build",
        available_at=NOW,
    )

    assert retry == first
    assert first.jobs[0].job_kind == "OPTIONS_INPUT_MEMBER"
    assert first.jobs[0].bundle_sha256 == first.closure_sha256
    assert {row.member_key for row in first.barrier_members} == {
        "UNDERLYER:SPY",
        "EQUITY_INPUT_READY",
    }
    assert first.barrier_members[-1].job_id is None


def test_options_input_ready_reconstructs_only_from_complete_exact_barrier():
    gate = build_advanced_options_input_gate_plan(
        _closure(),
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p4a-build",
        available_at=NOW,
    )
    members = tuple(row.member_key for row in gate.barrier_members)
    verdict = JobBarrierVerdict(
        gate.barrier_id,
        "COMPLETE",
        len(members),
        members,
        (),
        (),
    )

    receipt = reconstruct_options_input_ready_receipt(
        gate,
        verdict,
        cutoff=NOW,
        reference_generation=7,
        build_id="p4a-build",
    )
    assert receipt.receipt_kind == OPTIONS_INPUT_READY
    assert receipt.input_sha256 == gate.closure_sha256
    assert not receipt.advance_release
    with pytest.raises(ValueError, match="barrier is incomplete"):
        reconstruct_options_input_ready_receipt(
            gate,
            replace(verdict, state="OPEN", open_members=(members[0],)),
            cutoff=NOW,
            reference_generation=7,
            build_id="p4a-build",
        )
    with pytest.raises(ValueError, match="barrier is incomplete"):
        reconstruct_options_input_ready_receipt(
            gate,
            verdict,
            cutoff=NOW,
            reference_generation=8,
            build_id="p4a-build",
        )


def test_scope_mismatch_and_noncanonical_retry_fail_closed():
    control = _control_plane()
    with pytest.raises(ValueError, match="changed control scope"):
        _closure(cycle_input=replace(
            _cycle_input(control),
            cohort_id=UUID(int=1),
        ))
    text, digest = encode_advanced_options_input_closure(_closure())
    with pytest.raises(ValueError, match="digest mismatch"):
        decode_advanced_options_input_closure(text, "f" * 64)
    with pytest.raises(ValueError, match="instrument membership"):
        build_advanced_options_input_gate_plan(
            _closure(),
            window_id=WINDOW,
            equity_window_id=EQUITY_WINDOW,
            instrument_ids={"AAPL": 202},
            build_id="p4a-build",
            available_at=NOW,
        )
