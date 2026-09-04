import sys
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    BatchStatus,
    DecisionContext,
    DurableWorkItem,
    NewSeriesState,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
    WorkStage,
    WorkStatus,
    is_new_series_transition_allowed,
    reference_drift_failed,
)
from options.errors import ContextViolation
from options.strategies.domain import StructureType, signal_identity_sha256


UTC = timezone.utc
HASH = "a" * 64


def _page(batch_id, page_number=1, terminal=True):
    return RawBatchPage(
        batch_id=batch_id,
        page_number=page_number,
        row_count=2,
        response_bytes=b"payload",
        payload_sha256=HASH,
        received_at=datetime(2026, 8, 28, 20, 16, tzinfo=UTC),
        terminal=terminal,
        validation_status=PageValidationStatus.VALID,
        request_filter_sha256=HASH,
    )


def test_decision_context_enforces_market_and_observation_time():
    context = DecisionContext(
        market_time=datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        observed_time=datetime(2026, 8, 28, 20, 15, tzinfo=UTC),
    )

    context.require_available(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        datetime(2026, 8, 28, 20, 15, tzinfo=UTC),
    )
    with pytest.raises(ContextViolation):
        context.require_available(
            datetime(2026, 8, 28, 20, 1, tzinfo=UTC),
            datetime(2026, 8, 28, 20, 15, tzinfo=UTC),
        )
    with pytest.raises(FrozenInstanceError):
        context.market_time = datetime.now(UTC)


def test_complete_batch_requires_valid_contiguous_terminal_page_chain():
    batch_id = uuid4()
    started = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)

    complete = RawOptionBatch(
        batch_id=batch_id,
        provider="polygon",
        underlyer="SPY",
        scheduled_cycle=started,
        request_filter_sha256=HASH,
        policy_sha256=HASH,
        status=BatchStatus.COMPLETE,
        pages=(_page(batch_id),),
        started_at=started,
        completed_at=started + timedelta(seconds=1),
    )
    assert complete.complete is True
    assert complete.row_count == 2

    with pytest.raises(ValueError, match="terminal page"):
        RawOptionBatch(
            batch_id=batch_id,
            provider="polygon",
            underlyer="SPY",
            scheduled_cycle=started,
            request_filter_sha256=HASH,
            policy_sha256=HASH,
            status=BatchStatus.COMPLETE,
            pages=(_page(batch_id, terminal=False),),
            started_at=started,
            completed_at=started + timedelta(seconds=1),
        )


def test_claimed_work_requires_complete_lease_state():
    now = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)

    item = DurableWorkItem(
        work_id=uuid4(),
        stage=WorkStage.NORMALIZE,
        subject_id=str(uuid4()),
        business_key="polygon:SPY:2026-08-28T20:15:00Z",
        status=WorkStatus.CLAIMED,
        attempt_count=1,
        maximum_attempts=5,
        created_at=now,
        next_attempt_at=now,
        lease_owner="worker-1",
        lease_expires_at=now + timedelta(minutes=2),
    )
    assert item.lease_owner == "worker-1"

    with pytest.raises(ValueError, match="lease owner"):
        DurableWorkItem(
            work_id=uuid4(),
            stage=WorkStage.NORMALIZE,
            subject_id=str(uuid4()),
            business_key="missing-lease",
            status=WorkStatus.CLAIMED,
            attempt_count=1,
            maximum_attempts=5,
            created_at=now,
            next_attempt_at=now,
        )


def test_money_fields_reject_binary_float():
    from options.domain import OptionTradeEvent

    now = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)
    with pytest.raises(TypeError, match="price must be Decimal"):
        OptionTradeEvent(
            trade_event_id=uuid4(),
            provider="polygon",
            contract_id=1,
            contract_ticker="O:SPY260828C00600000",
            underlyer="SPY",
            sip_timestamp=now,
            sequence_number=1,
            participant_timestamp=None,
            first_observed_at=now,
            revised_observed_at=None,
            exchange=1,
            conditions=(),
            correction=None,
            price=1.25,
            size=1,
            shares_per_contract=100,
            notional=Decimal("125"),
            payload_sha256=HASH,
            raw_batch_id=uuid4(),
        )


def test_new_series_transitions_and_reference_drift_boundaries():
    assert is_new_series_transition_allowed(
        NewSeriesState.UNKNOWN_REFERENCE, NewSeriesState.REFERENCE_PENDING
    )
    assert not is_new_series_transition_allowed(
        NewSeriesState.UNKNOWN_REFERENCE, NewSeriesState.WATCHLIST_ACTIVE
    )
    assert reference_drift_failed(
        2,
        100,
        maximum_unknown_references=20,
        maximum_unknown_reference_fraction=Decimal("0.01"),
    )
    assert not reference_drift_failed(
        1,
        100,
        maximum_unknown_references=20,
        maximum_unknown_reference_fraction=Decimal("0.01"),
    )
    assert reference_drift_failed(
        21,
        10_000,
        maximum_unknown_references=20,
        maximum_unknown_reference_fraction=Decimal("0.01"),
    )


def test_signal_identity_tracks_the_package_not_the_analysis_matrix():
    package = signal_identity_sha256(
        "SPY",
        "INCOME_WHEEL",
        "1.0",
        HASH,
        StructureType.CASH_SECURED_PUT,
        ((101, "SELL", 1, 100),),
    )

    assert package == signal_identity_sha256(
        "SPY",
        "INCOME_WHEEL",
        "1.0",
        HASH,
        StructureType.CASH_SECURED_PUT,
        ((101, "SELL", 1, 100),),
    )
    assert package != signal_identity_sha256(
        "SPY",
        "INCOME_WHEEL",
        "1.0",
        HASH,
        StructureType.CASH_SECURED_PUT,
        ((102, "SELL", 1, 100),),
    )