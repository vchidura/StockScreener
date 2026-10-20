from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

from realtime.advanced_equity_detector_consumer import (
    AdvancedEquityDetectorConsumerConfig,
    AdvancedEquityDetectorDecision,
    claim_advanced_equity_detector_lease_cursor,
    plan_advanced_equity_detector_slice_cursor,
    persist_advanced_equity_detector_slice_cursor,
)
from realtime.advanced_equity_detector_shadow import (
    detector_shadow_input_sha256,
)
from realtime.detector_adapters import StableDetectorAdapterResult
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 13, 36, tzinfo=UTC)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")
OWNER = UUID("44444444-4444-4444-4444-444444444444")
REVISION = UUID("55555555-5555-5555-5555-555555555555")
SECURITY = UUID("66666666-6666-6666-6666-666666666666")


def _config(owner_epoch=1, *, interval="1m"):
    return AdvancedEquityDetectorConsumerConfig(
        cohort_id=COHORT,
        journal_generation=GENERATION,
        instrument_id=1,
        shard_id=0,
        shard_map_sha256="d" * 64,
        owner_id=OWNER,
        owner_epoch=owner_epoch,
        lease_resource_key="advanced-equity-detector-shadow:0",
        detector_id="ADVANCED_EQUITY_BREAKOUT",
        detector_version="advanced-equity-breakout-shadow-v1",
        code_sha256="b" * 64,
        policy_sha256="c" * 64,
        requirements={"interval": "1m"},
        output_kind="SIGNAL",
        readiness_epoch=1,
        interval=interval,
    )


def _row(*, interval="1m", end=NOW, offset=7):
    retained = {
        "ticker": "SPY",
        "interval": interval,
        "bar_start": (end - timedelta(minutes=1)).isoformat(),
        "bar_end": end.isoformat(),
        "open": "600.00000000",
        "high": "601.00000000",
        "low": "599.00000000",
        "close": "600.50000000",
        "volume": "1000.00000000",
        "vwap": "600.40000000",
        "transaction_count": 100,
        "quality_codes": [],
        "payload_sha256": "a" * 64,
    }
    return (
        REVISION,
        COHORT,
        SECURITY,
        1,
        "SPY",
        interval,
        end - timedelta(minutes=1),
        end,
        end,
        "a" * 64,
        GENERATION,
        SESSION,
        offset,
        retained,
    )


def _evaluator(record, inputs):
    payload = {
        "ticker": record.ticker,
        "disposition": "DETECTED",
    }
    return AdvancedEquityDetectorDecision(
        adapter=StableDetectorAdapterResult(
            adapter_id="advanced-equity-breakout-shadow",
            cohort_id=record.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            input_sha256=detector_shadow_input_sha256(inputs),
            status="EXECUTED",
            output=payload,
        ),
        trigger_identity=f"{record.ticker}:{record.interval}:{record.bar_end.isoformat()}",
        valid_until=None,
        disposition="DETECTED",
        direction=1,
        reason_codes=(),
        payload_schema="advanced_equity_breakout_shadow_signal_v1",
        payload=payload,
        state_key=f"{record.ticker}:ADVANCED_EQUITY_BREAKOUT",
        state_revision=record.source_offset,
        state_payload={"last_offset": record.source_offset},
    )


def test_claim_lease_preserves_epoch_for_same_owner():
    cursor = MagicMock()
    cursor.fetchone.return_value = (4,)

    epoch = claim_advanced_equity_detector_lease_cursor(
        cursor,
        resource_key="advanced-equity-detector-shadow:0",
        owner_id=OWNER,
        checked_at=NOW,
        lease_seconds=120,
    )

    assert epoch == 4
    statement = cursor.execute.call_args.args[0]
    assert "owner_epoch=CASE" in statement
    assert "lease_until<=%s" in statement


def test_plan_and_persist_one_provider_free_detector_slice(monkeypatch):
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = (_row(),)

    planned = plan_advanced_equity_detector_slice_cursor(
        cursor,
        _config(),
        evaluator=_evaluator,
        clock=lambda: NOW,
    )

    assert planned.start_offset == 0
    assert planned.committed_offset == 7
    assert len(planned.batches) == 1
    assert planned.batches[0].checkpoint.committed_offset == 7

    persisted = MagicMock()
    monkeypatch.setattr(
        "realtime.advanced_equity_detector_consumer."
        "persist_advanced_equity_detector_shadow_cursor",
        persisted,
    )
    persist_advanced_equity_detector_slice_cursor(cursor, planned)
    persisted.assert_called_once_with(cursor, planned.batches[0])


def test_empty_slice_preserves_checkpoint_offset():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        1,
        7,
        "d" * 64,
        b'{"trigger_at":"2026-10-20T13:36:00+00:00"}',
    )
    cursor.fetchall.return_value = ()

    planned = plan_advanced_equity_detector_slice_cursor(
        cursor,
        _config(),
        evaluator=_evaluator,
        clock=lambda: NOW,
    )

    assert planned.start_offset == planned.committed_offset == 7
    assert planned.batches == ()


def test_derived_5m_consumer_does_not_skip_an_incomplete_cutoff():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        1,
        100,
        "d" * 64,
        b'{"trigger_at":"2026-10-20T13:35:00+00:00"}',
    )
    cursor.fetchall.return_value = (
        _row(
            interval="5m",
            end=datetime(2026, 10, 20, 13, 45, tzinfo=UTC),
            offset=110,
        ),
    )

    planned = plan_advanced_equity_detector_slice_cursor(
        cursor,
        _config(interval="5m"),
        evaluator=_evaluator,
        clock=lambda: NOW,
    )

    assert planned.start_offset == planned.committed_offset == 100
    assert planned.records == planned.batches == ()
    source_query = cursor.execute.call_args_list[1].args[0]
    assert "receipt.receipt_kind='INPUT_READY'" in source_query
    assert "revision.instrument_id=%s" in source_query
