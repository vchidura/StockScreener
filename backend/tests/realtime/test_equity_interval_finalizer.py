from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

import realtime.equity_interval_finalizer as finalizer
from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from realtime.database_access import RealtimeDatabaseRole
from realtime.materialization_jobs import BarrierOutcome, ClaimedMaterializationJob
from realtime.preflight import ProviderTier
from realtime.worker_database import RealtimeProcessDatabase


UTC = timezone.utc
COHORT = UUID("11111111-1111-1111-1111-111111111111")
WINDOW = UUID("22222222-2222-2222-2222-222222222222")
BARRIER = UUID("33333333-3333-3333-3333-333333333333")
POLICY = "a" * 64
START = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
END = START + timedelta(minutes=5)
DEADLINE = END + timedelta(minutes=2)


class Database(RealtimeProcessDatabase):
    def __init__(self):
        self._process_plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)
        self.cursor = MagicMock()

    def transaction(self, **options):
        return nullcontext(self.cursor)


def security(ticker="AAPL"):
    return SecurityReferenceRevision(
        security_revision_id=uuid4(),
        security_id=uuid4(),
        ticker=ticker,
        active=True,
        company_name=ticker,
        security_type="CS",
        cik=None,
        composite_figi=None,
        share_class_figi=None,
        primary_exchange="XNAS",
        sic_code=None,
        sic_description=None,
        sector=None,
        industry=None,
        list_date=None,
        delisted_date=None,
        weighted_shares=None,
        free_float=None,
        free_float_percent=None,
        market_cap=None,
        source="TEST",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC),
        payload_sha256="b" * 64,
        raw_payload_json="{}",
    )


def minute_bars(item, *, observed_offset_seconds=0):
    rows = []
    for index in range(5):
        start = START + timedelta(minutes=index)
        end = start + timedelta(minutes=1)
        rows.append(
            EquityBarRevision(
                bar_revision_id=uuid4(),
                security_id=item.security_id,
                ticker=item.ticker,
                interval="1m",
                session_date=date(2026, 8, 28),
                bar_start=start,
                bar_end=end,
                open_price=Decimal(100 + index),
                high_price=Decimal(102 + index),
                low_price=Decimal(99 + index),
                close_price=Decimal(101 + index),
                volume=Decimal("1000"),
                vwap=Decimal("100.5"),
                transaction_count=10,
                source_kind=BarSourceKind.NATIVE_REST,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                is_final=True,
                system_observed_at=end + timedelta(seconds=observed_offset_seconds),
                replay_available_at=None,
                adjusted=False,
                payload_sha256=f"{index + 1:064x}",
                ingestion_segment_id=uuid4(),
                session_scope=BarSessionScope.RTH,
            )
        )
    return tuple(rows)


def window(provider_tier=ProviderTier.MASSIVE_DEVELOPER):
    return finalizer.FiveMinuteFinalizerWindow(
        cohort_id=COHORT,
        provider_tier=provider_tier,
        window_id=WINDOW,
        window_start=START,
        window_end=END,
        deadline=DEADLINE,
    )


def input_for(instrument_id=7, *, bars=None, checked_at=DEADLINE):
    item = security("AAPL" if instrument_id == 7 else "MSFT")
    rows = minute_bars(item) if bars is None else bars
    return finalizer.build_five_minute_finalizer_input(
        window(),
        instrument_id=instrument_id,
        security=item,
        source_bars=rows,
        checked_at=checked_at,
        policy_sha256=POLICY,
        build_id="build-1",
    )


def claimed(prepared_input):
    job = finalizer.build_five_minute_finalizer_job(
        prepared_input,
        policy_sha256=POLICY,
        build_id="build-1",
    )
    return ClaimedMaterializationJob(
        job, uuid4(), 1, DEADLINE + timedelta(minutes=1), 1
    )


def test_finalizer_input_classifies_complete_gap_and_late_without_inventing_bars():
    item = security()
    complete = finalizer.build_five_minute_finalizer_input(
        window(),
        instrument_id=7,
        security=item,
        source_bars=minute_bars(item),
        checked_at=DEADLINE,
        policy_sha256=POLICY,
        build_id="build-1",
    )
    late_rows = minute_bars(item)
    late_rows = (*late_rows[:-1], replace(late_rows[-1], system_observed_at=DEADLINE + timedelta(seconds=1)))
    late = finalizer.build_five_minute_finalizer_input(
        window(),
        instrument_id=7,
        security=item,
        source_bars=late_rows,
        checked_at=DEADLINE + timedelta(seconds=1),
        policy_sha256=POLICY,
        build_id="build-1",
    )
    gap = finalizer.build_five_minute_finalizer_input(
        window(),
        instrument_id=7,
        security=item,
        source_bars=minute_bars(item)[:-1],
        checked_at=DEADLINE,
        policy_sha256=POLICY,
        build_id="build-1",
    )

    assert complete.outcome is finalizer.FiveMinuteFinalizerOutcome.COMPLETE
    assert late.outcome is finalizer.FiveMinuteFinalizerOutcome.LATE
    assert gap.outcome is finalizer.FiveMinuteFinalizerOutcome.GAP
    assert len({complete.input_sha256, late.input_sha256, gap.input_sha256}) == 3
    with pytest.raises(finalizer.FiveMinuteFinalizerInputPending):
        finalizer.build_five_minute_finalizer_input(
            window(),
            instrument_id=7,
            security=item,
            source_bars=minute_bars(item)[:-1],
            checked_at=DEADLINE - timedelta(seconds=1),
            policy_sha256=POLICY,
            build_id="build-1",
        )


def test_advanced_finalizer_distinguishes_sparse_no_trade_from_gap():
    item = security()
    advanced = window(ProviderTier.MASSIVE_ADVANCED)
    sparse = finalizer.build_five_minute_finalizer_input(
        advanced,
        instrument_id=7,
        security=item,
        source_bars=minute_bars(item)[::2],
        checked_at=END + timedelta(seconds=5),
        policy_sha256=POLICY,
        build_id="build-1",
        gap_free_coverage=True,
    )
    no_trade = finalizer.build_five_minute_finalizer_input(
        advanced,
        instrument_id=7,
        security=item,
        source_bars=(),
        checked_at=END + timedelta(seconds=5),
        policy_sha256=POLICY,
        build_id="build-1",
        gap_free_coverage=True,
    )

    assert sparse.outcome is finalizer.FiveMinuteFinalizerOutcome.COMPLETE
    assert no_trade.outcome is finalizer.FiveMinuteFinalizerOutcome.NO_TRADE
    assert sparse.gap_free_coverage and no_trade.gap_free_coverage


def test_enqueuer_uses_one_logical_job_per_instrument_window_and_exact_barrier(monkeypatch):
    first = input_for(7)
    second = input_for(8)
    monkeypatch.setattr(finalizer, "_load_finalizer_window_cursor", MagicMock(return_value=window()))
    monkeypatch.setattr(
        finalizer,
        "_load_finalizer_member_input_cursor",
        MagicMock(side_effect=(first, second)),
    )
    enqueue = MagicMock(side_effect=lambda cursor, job: job.job_id)
    barrier = MagicMock()
    monkeypatch.setattr(finalizer, "enqueue_materialization_job_cursor", enqueue)
    monkeypatch.setattr(finalizer, "open_job_barrier_cursor", barrier)

    result = finalizer.enqueue_five_minute_finalizer_jobs_cursor(
        MagicMock(),
        cohort_id=COHORT,
        window_id=WINDOW,
        instrument_ids=(7, 8),
        checked_at=DEADLINE,
        policy_sha256=POLICY,
        build_id="build-1",
        barrier_id=BARRIER,
    )

    assert [row.job.instrument_id for row in result] == [7, 8]
    assert len({row.job.job_id for row in result}) == 2
    assert all(
        row.job.primary_window_id == WINDOW
        and row.job.interval == "5m"
        and row.job.bundle_sha256 == row.input.input_sha256
        for row in result
    )
    members = barrier.call_args.kwargs["members"]
    assert [member.member_key for member in members] == [
        "instrument:7:5m",
        "instrument:8:5m",
    ]
    assert [member.expected_input_sha256 for member in members] == [
        first.input_sha256,
        second.input_sha256,
    ]
    assert [member.job_id for member in members] == [
        result[0].job.job_id,
        result[1].job.job_id,
    ]


@pytest.mark.parametrize(
    ("outcome", "expected_count", "barrier_outcome", "watermark_state", "reason_codes"),
    [
        (finalizer.FiveMinuteFinalizerOutcome.COMPLETE, 1, BarrierOutcome.COMPLETE, "COMPLETE", ()),
        (finalizer.FiveMinuteFinalizerOutcome.LATE, 1, BarrierOutcome.COMPLETE, "COMPLETE", ("FINALIZER_LATE",)),
        (finalizer.FiveMinuteFinalizerOutcome.GAP, 0, BarrierOutcome.MISSING, "GAP", ("FINALIZER_GAP",)),
        (
            finalizer.FiveMinuteFinalizerOutcome.NO_TRADE,
            0,
            BarrierOutcome.COMPLETE,
            "COMPLETE",
            ("FINALIZER_NO_TRADE",),
        ),
    ],
)
def test_prepare_persists_bar_and_watermark_as_one_worker_output(
    monkeypatch, outcome, expected_count, barrier_outcome, watermark_state, reason_codes
):
    prepared_input = input_for()
    if outcome is finalizer.FiveMinuteFinalizerOutcome.LATE:
        prepared_input = replace(prepared_input, outcome=outcome)
    elif outcome is finalizer.FiveMinuteFinalizerOutcome.GAP:
        prepared_input = replace(prepared_input, outcome=outcome, source_bars=prepared_input.source_bars[:-1])
    elif outcome is finalizer.FiveMinuteFinalizerOutcome.NO_TRADE:
        prepared_input = replace(
            prepared_input,
            window=window(ProviderTier.MASSIVE_ADVANCED),
            outcome=outcome,
            source_bars=(),
            gap_free_coverage=True,
        )
    job = claimed(prepared_input)
    database = Database()
    monkeypatch.setattr(
        finalizer,
        "_load_finalizer_window_cursor",
        MagicMock(return_value=prepared_input.window),
    )
    monkeypatch.setattr(
        finalizer,
        "_load_finalizer_member_input_cursor",
        MagicMock(return_value=prepared_input),
    )
    persist = MagicMock()
    watermark = MagicMock(return_value=True)
    monkeypatch.setattr(finalizer, "persist_realtime_equity_bars_cursor", persist)
    monkeypatch.setattr(finalizer, "advance_acquisition_watermark_cursor", watermark)

    output = finalizer.prepare_five_minute_finalizer_output(database, job)
    output.persist_output(database.cursor)

    assert output.output_count == expected_count
    assert output.input_sha256 == prepared_input.input_sha256
    assert output.barrier_outcome is barrier_outcome
    assert persist.call_count == expected_count
    saved_watermark = watermark.call_args.args[1]
    assert saved_watermark.terminal_state.value == watermark_state
    assert saved_watermark.reason_codes == reason_codes
    assert saved_watermark.last_examined_bar_end == END
    assert saved_watermark.last_complete_bar_end == (
        None
        if outcome is finalizer.FiveMinuteFinalizerOutcome.GAP
        else END
    )


def test_advanced_sparse_finalizer_derives_full_window_without_invented_minutes(
    monkeypatch,
):
    item = security()
    prepared_input = finalizer.build_five_minute_finalizer_input(
        window(ProviderTier.MASSIVE_ADVANCED),
        instrument_id=7,
        security=item,
        source_bars=minute_bars(item)[::2],
        checked_at=END + timedelta(seconds=5),
        policy_sha256=POLICY,
        build_id="build-1",
        gap_free_coverage=True,
    )
    job = claimed(prepared_input)
    database = Database()
    database.cursor.fetchone.return_value = None
    monkeypatch.setattr(
        finalizer,
        "_load_finalizer_window_cursor",
        MagicMock(return_value=prepared_input.window),
    )
    monkeypatch.setattr(
        finalizer,
        "_load_finalizer_member_input_cursor",
        MagicMock(return_value=prepared_input),
    )
    persist = MagicMock()
    monkeypatch.setattr(
        finalizer,
        "persist_realtime_equity_bars_cursor",
        persist,
    )
    monkeypatch.setattr(
        finalizer,
        "advance_acquisition_watermark_cursor",
        MagicMock(return_value=True),
    )

    output = finalizer.prepare_five_minute_finalizer_output(database, job)
    output.persist_output(database.cursor)

    fact = persist.call_args.args[1][0]
    assert fact.revision.bar_start == START
    assert fact.revision.bar_end == END
    assert fact.revision.volume == Decimal("3000")
    assert len(fact.revision.source_bar_revision_ids) == 3
    assert "ADVANCED_NO_TRADE_MINUTES" in fact.revision.quality_codes


def test_finalizer_runner_reuses_bounded_generic_worker_claims(monkeypatch):
    database = Database()
    config = finalizer.EquityWorkerConfig(
        cohort_id=COHORT,
        job_kind=finalizer.EQUITY_INTERVAL_FINALIZE_JOB,
        lease_owner=uuid4(),
        claim_limit=17,
    )
    run = MagicMock(return_value="status")
    monkeypatch.setattr(finalizer, "run_equity_worker_once", run)

    assert finalizer.run_five_minute_finalizer_once(
        database, config, checked_at=DEADLINE
    ) == "status"
    assert run.call_args.args[:2] == (database, config)
    assert run.call_args.kwargs["checked_at"] == DEADLINE
