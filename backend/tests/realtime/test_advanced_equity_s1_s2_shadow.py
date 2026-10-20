from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_equity_detector_consumer import (
    AdvancedEquityDetectorSourceRecord,
)
from realtime.advanced_equity_s1_s2_shadow import (
    build_s1_s2_consumer_config,
    s1_s2_evaluator_cursor,
    s1_s2_requirements,
)
from tests.options.test_stock_bar_detection import (
    OPEN,
    SECURITY,
    bar,
    daily_history,
)


UTC = timezone.utc
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")


def _row(revision):
    return (
        revision.bar_revision_id,
        revision.security_id,
        revision.ticker,
        revision.interval,
        revision.session_date,
        revision.bar_start,
        revision.bar_end,
        revision.open_price,
        revision.high_price,
        revision.low_price,
        revision.close_price,
        revision.volume,
        revision.vwap,
        revision.transaction_count,
        revision.source_kind.value,
        revision.availability_mode.value,
        revision.is_final,
        revision.system_observed_at,
        revision.replay_available_at,
        "RAW_UNADJUSTED",
        revision.payload_sha256,
        revision.quality_codes,
        revision.provider_published_at,
        revision.ingestion_segment_id,
        revision.source_bar_revision_ids,
        revision.supersedes_bar_revision_id,
        revision.session_scope.value,
        revision.system_observed_at,
    )


def _record(revision, offset):
    retained = {
        "ticker": revision.ticker,
        "interval": revision.interval,
        "payload_sha256": revision.payload_sha256,
    }
    return AdvancedEquityDetectorSourceRecord(
        bar_revision_id=revision.bar_revision_id,
        cohort_id=COHORT,
        security_id=SECURITY,
        instrument_id=1,
        ticker=revision.ticker,
        interval=revision.interval,
        bar_start=revision.bar_start,
        bar_end=revision.bar_end,
        observed_at=revision.system_observed_at,
        payload_sha256=revision.payload_sha256,
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        source_offset=offset,
        retained_bar=retained,
    )


@pytest.mark.parametrize(
    ("detector_id", "intraday"),
    (
        (
            "S1",
            (
                bar("5m", OPEN, 5, 100),
                bar("5m", OPEN + timedelta(minutes=5), 5, 101.1),
            ),
        ),
        (
            "S2",
            (
                bar("5m", OPEN, 5, 101.2),
                bar("5m", OPEN + timedelta(minutes=5), 5, 100.6),
                bar("5m", OPEN + timedelta(minutes=10), 5, 101.1),
            ),
        ),
    ),
)
def test_existing_s1_s2_models_bind_to_advanced_5m_shadow(
    detector_id,
    intraday,
):
    cursor = MagicMock()
    cursor.fetchall.side_effect = (
        tuple(_row(value) for value in reversed(daily_history())),
        tuple(_row(value) for value in reversed(intraday)),
    )
    source = _record(intraday[-1], offset=len(intraday) * 5)
    evaluator = s1_s2_evaluator_cursor(
        cursor,
        detector_id=detector_id,
        session_close=datetime(2026, 9, 28, 20, tzinfo=UTC),
        decision_at=lambda: intraday[-1].bar_end + timedelta(minutes=15),
    )

    decision = evaluator(source, source.detector_inputs())

    assert decision.disposition == "DETECTED"
    assert decision.direction in {-1, 1}
    assert decision.payload["detector_id"] == detector_id
    assert decision.adapter.output == decision.payload
    assert len(decision.additional_inputs) == (
        15 + len(intraday) - 1
    )
    assert s1_s2_requirements(detector_id)["trigger_interval"] == "5m"
    config = build_s1_s2_consumer_config(
        detector_id=detector_id,
        cohort_id=COHORT,
        journal_generation=GENERATION,
        instrument_id=1,
        shard_map_sha256="d" * 64,
        owner_id=UUID("44444444-4444-4444-4444-444444444444"),
        owner_epoch=1,
        lease_resource_key=f"advanced-equity-{detector_id.lower()}:0",
        code_sha256="b" * 64,
        readiness_epoch=1,
    )
    assert config.interval == "5m"
    assert config.detector_id == f"ADVANCED_EQUITY_{detector_id}"
