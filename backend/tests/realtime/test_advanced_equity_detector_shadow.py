from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_equity_detector_shadow import (
    CONTRACT_VERSION,
    PRODUCTION_AUTHORITY,
    STATE_SCHEMA,
    build_advanced_equity_detector_shadow_batch,
    detector_shadow_input_sha256,
    persist_advanced_equity_detector_shadow_cursor,
)
from realtime.detector_adapters import StableDetectorAdapterResult
from realtime.detector_evidence import RealtimeDetectorInput
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 13, 35, tzinfo=UTC)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")
OWNER = UUID("44444444-4444-4444-4444-444444444444")


def _input(*, offset=7, generation=GENERATION):
    return RealtimeDetectorInput(
        input_role="BAR_EVENT",
        input_key="SPY:1m:2026-10-20T13:35:00Z",
        source_kind="ADVANCED_EQUITY_JOURNAL_EVENT",
        input_sha256="a" * 64,
        available_at=NOW,
        received_at=NOW,
        retained_input={
            "ticker": "SPY",
            "interval": "1m",
            "close": "600.00",
        },
        journal_generation=generation,
        stream_session_id=SESSION,
        source_offset=offset,
    )


def _batch(*, publication=False, execution=False, offset=7, committed=7):
    inputs = (_input(offset=offset),)
    adapter = StableDetectorAdapterResult(
        adapter_id="advanced-equity-breakout-shadow",
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        input_sha256=detector_shadow_input_sha256(inputs),
        status="EXECUTED",
        output={"ticker": "SPY", "disposition": "DETECTED"},
        publication_permission=publication,
        execution_permission=execution,
    )
    return build_advanced_equity_detector_shadow_batch(
        adapter=adapter,
        detector_id="ADVANCED_EQUITY_BREAKOUT",
        detector_version="advanced-equity-breakout-shadow-v1",
        code_sha256="b" * 64,
        policy_sha256="c" * 64,
        requirements={"interval": "1m", "minimum_bars": 1},
        output_kind="SIGNAL",
        instrument_id=1,
        trigger_identity="SPY:1m:2026-10-20T13:35:00Z",
        trigger_at=NOW,
        decision_at=NOW,
        valid_until=None,
        disposition="DETECTED",
        direction=1,
        reason_codes=(),
        readiness_epoch=1,
        payload_schema="advanced_equity_breakout_shadow_signal_v1",
        payload={"ticker": "SPY", "disposition": "DETECTED"},
        inputs=inputs,
        state_key="SPY:ADVANCED_EQUITY_BREAKOUT",
        state_revision=1,
        state_payload={"episode": "SPY:2026-10-20T13:35:00Z"},
        lease_resource_key="advanced-equity-detector-shadow:0",
        owner_id=OWNER,
        journal_generation=GENERATION,
        shard_id=0,
        shard_map_sha256="d" * 64,
        owner_epoch=1,
        committed_offset=committed,
        committed_at=NOW,
    )


def test_shadow_batch_is_deterministic_and_has_no_production_authority():
    first = _batch()
    repeated = _batch()

    assert first == repeated
    assert first.state.state_schema == STATE_SCHEMA
    assert first.state.state_payload["contract_version"] == CONTRACT_VERSION
    assert first.checkpoint.committed_offset == 7
    assert not any(PRODUCTION_AUTHORITY.values())


@pytest.mark.parametrize(
    ("publication", "execution"),
    ((True, False), (False, True)),
)
def test_shadow_rejects_publication_or_execution_authority(
    publication,
    execution,
):
    with pytest.raises(
        ValueError,
        match="cannot carry publication or execution authority",
    ):
        _batch(publication=publication, execution=execution)


def test_shadow_rejects_input_past_checkpoint():
    with pytest.raises(ValueError, match="exceeds its journal checkpoint"):
        _batch(offset=8, committed=7)


def test_shadow_rejects_adapter_payload_drift():
    inputs = (_input(),)
    adapter = StableDetectorAdapterResult(
        adapter_id="advanced-equity-breakout-shadow",
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        input_sha256=detector_shadow_input_sha256(inputs),
        status="EXECUTED",
        output={"ticker": "QQQ", "disposition": "DETECTED"},
    )

    with pytest.raises(ValueError, match="differs from its adapter result"):
        build_advanced_equity_detector_shadow_batch(
            adapter=adapter,
            detector_id="ADVANCED_EQUITY_BREAKOUT",
            detector_version="advanced-equity-breakout-shadow-v1",
            code_sha256="b" * 64,
            policy_sha256="c" * 64,
            requirements={"interval": "1m", "minimum_bars": 1},
            output_kind="SIGNAL",
            instrument_id=1,
            trigger_identity="SPY:1m:2026-10-20T13:35:00Z",
            trigger_at=NOW,
            decision_at=NOW,
            valid_until=None,
            disposition="DETECTED",
            direction=1,
            reason_codes=(),
            readiness_epoch=1,
            payload_schema="advanced_equity_breakout_shadow_signal_v1",
            payload={"ticker": "SPY", "disposition": "DETECTED"},
            inputs=inputs,
            state_key="SPY:ADVANCED_EQUITY_BREAKOUT",
            state_revision=1,
            state_payload={"episode": "SPY:2026-10-20T13:35:00Z"},
            lease_resource_key="advanced-equity-detector-shadow:0",
            owner_id=OWNER,
            journal_generation=GENERATION,
            shard_id=0,
            shard_map_sha256="d" * 64,
            owner_epoch=1,
            committed_offset=7,
            committed_at=NOW,
        )


def test_shadow_persistence_structurally_disables_alerts(monkeypatch):
    persisted = MagicMock()
    monkeypatch.setattr(
        "realtime.advanced_equity_detector_shadow."
        "persist_detector_evaluation_batch_cursor",
        persisted,
    )
    cursor = MagicMock()
    batch = _batch()

    persist_advanced_equity_detector_shadow_cursor(cursor, batch)

    persisted.assert_called_once_with(
        cursor,
        lease_resource_key=batch.lease_resource_key,
        owner_id=OWNER,
        checkpoint=batch.checkpoint,
        versions=(batch.version,),
        states=(batch.state,),
        events=(batch.event,),
        alerts=(),
    )
