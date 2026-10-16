from dataclasses import replace
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.contracts import canonical_sha256
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bulk_backfill import (
    RETAINED_BACKFILL_ENDPOINT,
    RetainedEquityBulkSource,
    build_equity_bulk_backfill_plan,
)
from realtime.stage_runners_ingest import EquityAcquisitionRequest
from tests.realtime.test_equity_bar_adapter import COHORT, NOW, _developer_revision


def _facts(count):
    result = []
    for index in range(count):
        revision = _developer_revision()
        start = revision.bar_start + timedelta(minutes=index)
        payload = canonical_sha256(("bulk", index))
        revision = replace(
            revision,
            bar_revision_id=uuid5(NAMESPACE_URL, f"bulk-bar:{index}"),
            bar_start=start,
            bar_end=start + timedelta(minutes=1),
            system_observed_at=max(NOW, start + timedelta(minutes=1)),
            payload_sha256=payload,
        )
        result.append(adapt_developer_equity_bar(
            revision,
            cohort_id=COHORT,
            instrument_id=1,
        ))
    return tuple(result)


def test_bulk_plan_chunks_one_ticker_interval_without_changing_bar_lineage():
    facts = _facts(2_501)
    plan = build_equity_bulk_backfill_plan(
        facts,
        cohort_id=COHORT,
        tickers=("SPY",),
    )

    assert [len(chunk.facts) for chunk in plan.chunks] == [1_000, 1_000, 501]
    assert plan.fact_count == 2_501
    assert len({chunk.provider_batch_id for chunk in plan.chunks}) == 3
    assert [
        fact.revision.ingestion_segment_id
        for chunk in plan.chunks
        for fact in chunk.facts
    ] == [fact.revision.ingestion_segment_id for fact in facts]


def test_retained_chunk_records_internal_lineage_but_zero_external_requests():
    plan = build_equity_bulk_backfill_plan(
        _facts(2),
        cohort_id=COHORT,
        tickers=("SPY",),
    )
    chunk = plan.chunks[0]
    source = RetainedEquityBulkSource(chunk, cohort_id=COHORT, recorded_at=NOW)
    request = EquityAcquisitionRequest(
        COHORT,
        universe=type("Universe", (), {
            "equity_tickers": ("SPY",),
            "option_underlyings": (),
        })(),
        requested_at=NOW,
    )

    result = source.collect(request)

    assert result.provider_requests == 0 and result.response_bytes == 0
    assert len(result.requests) == 1
    assert result.requests[0].reservation.endpoint_class == RETAINED_BACKFILL_ENDPOINT
    assert result.batches[0].request_id == result.requests[0].reservation.request_id


def test_bulk_plan_rejects_partial_or_duplicate_scope():
    facts = _facts(2)
    with pytest.raises(ValueError, match="exact ticker"):
        build_equity_bulk_backfill_plan(
            facts,
            cohort_id=COHORT,
            tickers=("SPY", "MSFT"),
        )
    with pytest.raises(ValueError, match="duplicate"):
        build_equity_bulk_backfill_plan(
            (*facts, facts[0]),
            cohort_id=COHORT,
            tickers=("SPY",),
        )
