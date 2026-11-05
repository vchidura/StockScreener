"""Consumers stop at each Options cutoff until that window ends, on a real journal
that keeps growing past the barrier."""
from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from test_advanced_options_control_plane import _plan as _control_plane
from realtime import advanced_options_p8_roles as roles_module
from realtime import advanced_options_p8_window as window_module
from realtime.advanced_options_acquisition_runtime import (
    ACQUISITION_STATUS_SCHEMA,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
    build_advanced_options_journal_registration,
)
from realtime.advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
)
from realtime.advanced_options_resident_barrier import (
    ResidentCompletionMember,
)
from realtime.advanced_options_rolling_producer import (
    RollingProducerStatus,
    assemble_advanced_options_rolling_state_cursor,
    _checkpoints,
    _market_state,
    _volumes,
)
from realtime.advanced_options_rolling_state import (
    AdvancedOptionsOrderWindowState,
)
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import AssetClass, MarketFeed, RawMessageEnvelope
from realtime.journal import RawJournalReader
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION
from realtime.option_source_flow import CANONICAL_OPTION_FLOW_OWNER
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5
from tests.realtime import test_advanced_options_rolling_producer as producer

UTC = timezone.utc
CUTOFF = datetime(2026, 10, 23, 18, 20, tzinfo=UTC)
OPTION = "O:SPY261120C00450000"


def _ms(at):
    return int(at.timestamp() * 1000)


def _frame(*times):
    rows = []
    for index, at in enumerate(times):
        rows.append({
            "ev": "Q",
            "sym": OPTION,
            "bp": 4.9,
            "ap": 5.0,
            "bs": 10,
            "as": 12,
            "t": _ms(at),
            "q": index + 1,
        })
    return json.dumps(rows).encode("ascii")


class _Journal:
    """A journal the feed can keep appending to, read through the capped source."""

    def __init__(self, tmp_path):
        self.control = _control_plane()
        self.stream = self.control.source_flow.stream_session_ids[0]
        self.path = tmp_path / "mounted" / "options.sqlite"
        self.message_number = 0
        self.source = None

    def append(self, *frames, received_at=None):
        buffer = RawMessageBuffer(
            BufferLimits(1000, 4 * 1024 * 1024, 60_000_000_000),
            monotonic_ns=time.monotonic_ns,
        )
        for times in frames:
            buffer.put_nowait(RawMessageEnvelope(
                self.stream,
                self.message_number,
                MarketFeed.OPTIONS,
                max(times) if received_at is None else received_at,
                time.monotonic_ns(),
                _frame(*times),
            ))
            self.message_number += 1
        pump = AdvancedOptionsJournalPump(
            buffer,
            self.path,
            self.control.source_flow.journal_generation,
            {
                OPTION: InstrumentAdmission(
                    101,
                    OPTION,
                    "SPY",
                    AssetClass.OPTION,
                    0,
                ),
            },
            maximum_batch_messages=4,
            maximum_batch_bytes=64 * 1024,
        )
        pump.start()
        pump.request_stop()
        progress = pump.join(10)
        registration = replace(
            build_advanced_options_journal_registration(
                control_plane=self.control,
                root=self.path.parent,
                path=self.path,
                stream_session_id=self.stream,
                owner_epoch=1,
                normalizer_version=NORMALIZER_VERSION,
                shard_ids=(0,),
                shard_map_sha256="f" * 64,
            ),
            status="WRITING",
            raw_committed_offset=progress.last_raw_offset,
            normalized_committed_offset=progress.last_event_offset,
        )
        if self.source is None:
            self.source = AdvancedOptionsJournalSource(
                root=self.path.parent,
                registration=registration,
                source_flow=self.control.source_flow,
            )
        else:
            self.source.registration = registration
        return progress

    @property
    def reader(self):
        return RawJournalReader(self.path)

    def consume(self):
        """What market state and flow can read from the start under the cap."""
        return (
            tuple(self.source.read_normalized_after(0, limit=10_000)),
            tuple(self.source.read_after(0, limit=10_000)),
        )


@pytest.fixture
def journal(tmp_path):
    return _Journal(tmp_path)


def _at(seconds):
    return CUTOFF + timedelta(seconds=seconds)


class _HoldProvider:
    """The cycle rows, as the hold's query reads them."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = 0

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        provider = self

        class Cursor:
            def execute(self, statement, _parameters):
                assert "cycle_authorizations" in statement
                provider.queries += 1

            def fetchall(self):
                return provider.rows

        yield SimpleNamespace(cursor=Cursor())


def _controller(monkeypatch, journal, rows, *, refresh_seconds=0):
    authorization_id = uuid4()
    monkeypatch.setattr(
        roles_module,
        "session_acquisition_authorization_from_payload",
        lambda _value: SimpleNamespace(authorization_id=authorization_id),
    )
    provider = _HoldProvider(rows)
    controller = roles_module._ResidentConsumerHoldController(
        provider,
        {"v3_authorities": {"session_acquisition": {}}},
        journal.source,
        refresh_seconds=refresh_seconds,
    )
    return controller, provider


def _cycle(cutoff, *, state="RUNNING", deadline=None):
    return (
        cutoff - timedelta(minutes=5),
        str(uuid4()),
        cutoff,
        deadline or datetime.now(UTC) + timedelta(hours=1),
        state,
    )


# --- the barrier ------------------------------------------------------------------


def test_barrier_ends_at_the_cutoff_with_later_events_in_the_journal(journal):
    journal.append(
        (_at(-2), _at(-2)),
        (_at(-1),),
        (_at(0),),
        (_at(1),),
        (_at(2), _at(2)),
    )

    assert journal.reader.offsets_at(CUTOFF) == (3, 4)


def test_barrier_never_takes_in_an_event_past_the_cutoff(journal):
    # A quote stamped past the cutoff arrives ahead of a print stamped before it.
    journal.append(
        (_at(-1),),
        (_at(0.005),),
        (_at(-0.001),),
        (_at(1),),
    )

    assert journal.reader.offsets_at(CUTOFF) == (1, 1)


def test_barrier_stops_before_a_frame_that_spans_the_cutoff(journal):
    journal.append(
        (_at(-1),),
        (_at(-0.001), _at(0.001)),
        (_at(1),),
    )

    assert journal.reader.offsets_at(CUTOFF) == (1, 1)


def test_barrier_does_not_count_an_event_stamped_far_past_its_frame(journal):
    journal.append((_at(-1),))
    journal.append((_at(3600),), received_at=_at(-0.5))
    journal.append((_at(0),), (_at(1),))

    # The bad stamp sits behind the barrier, and the first credible later event
    # still ends it.
    assert journal.reader.offsets_at(CUTOFF) == (3, 3)
    journal.append((_at(20),))
    assert journal.reader.offsets_at(_at(10)) == (4, 4)
    # With nothing credible later yet, the barrier is the newest event.
    assert journal.reader.offsets_at(_at(25)) == (5, 5)


def test_barrier_counts_an_event_stamped_within_the_limit(journal):
    journal.append((_at(-1),))
    journal.append((_at(50),), received_at=_at(-0.5))
    journal.append((_at(1),))

    assert journal.reader.offsets_at(CUTOFF) == (1, 1)


def test_barrier_skips_a_bad_stamp_that_has_come_into_reach(journal):
    # The feed has reached the bad stamp's time, so only its frame can show that
    # it does not count. Later events make the barrier what it would have been.
    journal.append((_at(-1),))
    journal.append((_at(120),), received_at=_at(-0.5))
    journal.append((_at(0),), (_at(1),), (_at(100),))

    assert journal.reader.offsets_at(CUTOFF) == (3, 3)


def test_barrier_does_not_move_when_a_bad_stamp_arrives_later(journal):
    journal.append((_at(-1),), (_at(1),))
    fixed = journal.reader.offsets_at(CUTOFF)

    journal.append((_at(3600),), received_at=_at(2))

    assert fixed == (1, 1)
    assert journal.reader.offsets_at(CUTOFF) == fixed


def test_barrier_finds_far_future_stamps_out_through_the_index(journal):
    from contextlib import closing

    from realtime.journal import (
        _FIRST_EVENT_AFTER_SQL,
        _connect,
    )

    journal.append((_at(-1),), (_at(1),))
    with closing(
        _connect(journal.path, timeout_seconds=5, read_only=True)
    ) as connection:
        plan = " ".join(
            str(tuple(row))
            for row in connection.execute(
                f"EXPLAIN QUERY PLAN {_FIRST_EVENT_AFTER_SQL}",
                (1, 2),
            ).fetchall()
        )

    assert "COVERING INDEX idx_normalized_event_instrument_time" in plan
    assert "raw_messages" not in plan


def test_barrier_does_not_move_when_a_late_event_arrives(journal):
    journal.append((_at(-1),), (_at(1),))
    fixed = journal.reader.offsets_at(CUTOFF)

    journal.append((_at(-0.5),), (_at(2),))

    assert fixed == (1, 1)
    assert journal.reader.offsets_at(CUTOFF) == fixed


def test_barrier_follows_the_feed_until_the_cutoff_arrives(journal):
    journal.append((_at(-2),))
    assert journal.reader.offsets_at(CUTOFF) == (1, 1)

    journal.append((_at(-1),))
    assert journal.reader.offsets_at(CUTOFF) == (2, 2)


def test_barrier_is_zero_when_nothing_precedes_the_cutoff(journal):
    journal.append((_at(1),))

    assert journal.reader.offsets_at(CUTOFF) == (0, 0)


def test_barrier_with_a_fractional_cutoff_keeps_its_own_millisecond(journal):
    exact = CUTOFF + timedelta(milliseconds=123)
    journal.append((exact,), (exact + timedelta(milliseconds=1),))

    assert journal.reader.offsets_at(
        exact + timedelta(microseconds=456)
    ) == (1, 1)


# --- the hold ---------------------------------------------------------------------


def test_consumers_hold_at_the_barrier_while_the_feed_keeps_writing(
    monkeypatch,
    journal,
):
    journal.append((_at(-1),), (_at(0),), (_at(1),), (_at(2),))
    controller, _provider = _controller(
        monkeypatch,
        journal,
        (_cycle(CUTOFF),),
    )

    hold = controller.apply(journal.source)
    normalized, raw = journal.consume()

    assert (hold.raw_offset, hold.normalized_offset) == (2, 2)
    assert [row.event_offset for row in normalized] == [1, 2]
    assert [row.raw_offset for row in raw] == [1, 2]
    assert max(row.event_at for row in normalized) <= CUTOFF
    assert journal.source.last_offset() == 2

    journal.append((_at(3),))
    controller.apply(journal.source)

    assert [row.event_offset for row in journal.consume()[0]] == [1, 2]


def test_consumers_run_up_to_the_cutoff_before_it_arrives(
    monkeypatch,
    journal,
):
    journal.append((_at(-3),), (_at(-2),))
    controller, _provider = _controller(
        monkeypatch,
        journal,
        (_cycle(CUTOFF),),
    )

    controller.apply(journal.source)
    assert len(journal.consume()[0]) == 2

    journal.append((_at(-1),))
    controller.apply(journal.source)
    assert len(journal.consume()[0]) == 3


def test_release_on_terminal_window_catches_up_and_holds_at_the_next(
    monkeypatch,
    journal,
):
    next_cutoff = _at(300)
    journal.append((_at(-1),), (_at(1),), (_at(299),), (_at(301),))
    first = _cycle(CUTOFF)
    second = _cycle(next_cutoff, state="PENDING")
    controller, provider = _controller(
        monkeypatch,
        journal,
        (first, second),
    )

    controller.apply(journal.source)
    assert [row.event_offset for row in journal.consume()[0]] == [1]

    provider.rows = ((*first[:4], "COMPLETE"), second)
    hold = controller.apply(journal.source)

    assert hold.cutoff == next_cutoff
    assert [row.event_offset for row in journal.consume()[0]] == [1, 2, 3]


def test_release_when_a_stuck_window_passes_its_deadline(monkeypatch, journal):
    journal.append((_at(-1),), (_at(1),), (_at(2),))
    controller, _provider = _controller(
        monkeypatch,
        journal,
        (_cycle(CUTOFF, deadline=datetime.now(UTC) - timedelta(seconds=1)),),
    )

    assert controller.apply(journal.source) is None
    assert len(journal.consume()[0]) == 3


def test_no_open_target_runs_freely(monkeypatch, journal):
    journal.append((_at(-1),), (_at(1),), (_at(2),))
    controller, _provider = _controller(monkeypatch, journal, ())

    assert controller.apply(journal.source) is None
    assert len(journal.consume()[0]) == 3
    assert journal.source.last_offset() == 3


def test_the_cap_comes_off_when_the_hold_ends(monkeypatch, journal):
    journal.append((_at(-1),), (_at(1),))
    first = _cycle(CUTOFF)
    controller, provider = _controller(monkeypatch, journal, (first,))

    controller.apply(journal.source)
    assert len(journal.consume()[0]) == 1

    provider.rows = ()
    controller.apply(journal.source)
    assert len(journal.consume()[0]) == 2


def test_hold_lookup_is_cached_between_refreshes(monkeypatch, journal):
    journal.append((_at(-1),), (_at(1),))
    controller, provider = _controller(
        monkeypatch,
        journal,
        (_cycle(CUTOFF),),
        refresh_seconds=60,
    )

    first = controller.apply(journal.source)
    second = controller.apply(journal.source)

    assert provider.queries == 1
    assert first is second


def test_hold_refreshes_about_once_a_second_by_default():
    assert (
        roles_module._ResidentConsumerHoldController.__init__.__kwdefaults__[
            "refresh_seconds"
        ]
        == 1.0
    )


def test_old_targets_are_not_rechecked_in_the_snapshot(monkeypatch, journal):
    """A window nothing will run again is left out of the list every role scans."""
    journal.append((_at(-1),), (_at(1),))
    statements = []

    class Cursor:
        def execute(self, statement, _parameters):
            statements.append(statement)

        def fetchall(self):
            return ()

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=Cursor())

    monkeypatch.setattr(
        roles_module,
        "session_acquisition_authorization_from_payload",
        lambda _value: SimpleNamespace(authorization_id=UUID(int=1)),
    )

    assert roles_module._snapshot_v3_resident_windows(
        SimpleNamespace(transaction=transaction),
        object(),
        bundle={
            "orchestration": {"catalog_version": STAGE_CATALOG_VERSION_V5},
            "v3_authorities": {"session_acquisition": {}},
        },
        member=ResidentCompletionMember.INGRESS,
        owner_epoch=1,
        reader=journal.reader,
        raw_offset=2,
        normalized_offset=2,
    ) == 0
    assert "window_run.state IN ('PENDING','RUNNING')" in statements[0]
    assert "window_run.deadline>%s" in statements[0]


def test_flow_writes_the_barrier_checkpoint_once_it_stands_still():
    flushes = []
    consumer = SimpleNamespace(
        fed_offset=7,
        checkpointed_offset=4,
        flush=lambda **kwargs: flushes.append(kwargs),
    )
    hold = SimpleNamespace(raw_offset=7, cutoff=CUTOFF)

    roles_module._checkpoint_flow_at_hold(
        consumer,
        hold,
        checked_at=_at(-1),
    )
    assert flushes == []

    roles_module._checkpoint_flow_at_hold(consumer, hold, checked_at=_at(1))
    assert flushes == [{"reason": "HOLD_BARRIER", "full": True}]

    flushes.clear()
    consumer.checkpointed_offset = 7
    roles_module._checkpoint_flow_at_hold(consumer, hold, checked_at=_at(2))
    consumer.checkpointed_offset, consumer.fed_offset = 4, 6
    roles_module._checkpoint_flow_at_hold(consumer, hold, checked_at=_at(2))
    roles_module._checkpoint_flow_at_hold(consumer, None, checked_at=_at(2))
    assert flushes == []


# --- the producer at the barrier --------------------------------------------------


class _RoutedCursor:
    def __init__(self, routes):
        self.routes = routes
        self.rows = ()

    def execute(self, statement, _parameters):
        for name, rows in self.routes.items():
            if f"FROM {name}" in statement:
                self.rows = rows
                return
        self.rows = ()

    def fetchall(self):
        return self.rows


def _held_inputs(plan):
    """The stored state with consumers exactly at the barrier, after the cutoff,
    while ingress keeps moving and the journal holds events past the barrier."""
    binding = producer._equity_binding()
    option_rows, contracts, equities = producer._separate_head_inputs(
        plan,
        binding,
    )
    selected = plan.enrollment.activity_selection.contracts[0]
    contracts = ({
        **contracts[0],
        "contract_ticker": selected.contract_ticker,
    },)
    generation = str(plan.source_flow.journal_generation)
    session = str(plan.source_flow.stream_session_ids[0])
    known = plan.known_at
    heads = tuple(
        {
            **row,
            "journal_generation": generation,
            "session_id": session,
            "source_offset": plan.market_state_committed_offset,
            # An option event from before the cutoff that arrived just after it.
            "received_at": (
                row["received_at"]
                if row["instrument_id"] == equities[0]["instrument_id"]
                else known + timedelta(milliseconds=40)
            ),
        }
        for row in option_rows
    )
    checkpoints = tuple(
        {
            "consumer_name": owner,
            "journal_generation": generation,
            "shard_id": 0,
            "shard_map_sha256": shard_map,
            "owner_epoch": epoch,
            "committed_offset": offset,
            "checkpoint_id": str(uuid4()),
            "state_hash": "c" * 64,
            "committed_at": known + timedelta(milliseconds=300),
        }
        for owner, shard_map, epoch, offset in (
            (
                CANONICAL_MARKET_STATE_OWNER,
                plan.market_state_shard_map_sha256,
                plan.owner_epoch + 2,
                plan.market_state_committed_offset,
            ),
            (
                CANONICAL_OPTION_FLOW_OWNER,
                plan.flow_shard_map_sha256,
                plan.owner_epoch + 1,
                plan.flow_committed_offset,
            ),
        )
    )
    routes = {
        "rt.stream_sessions": ({
            "session_id": session,
            "manifest_id": str(plan.control_plane.manifest_id),
            "owner_epoch": plan.owner_epoch,
            "status": "LIVE",
            "journal_generation": generation,
            "last_raw_offset": plan.flow_committed_offset + 500,
            "last_event_offset": plan.market_state_committed_offset + 500,
            "event_time_watermark": known + timedelta(seconds=90),
            "heartbeat_at": known + timedelta(seconds=90),
            "started_at": known - timedelta(hours=1),
            "subscribed_at": known - timedelta(hours=1),
            "ended_at": None,
        },),
        "rt.consumer_checkpoints": checkpoints,
        "rt.quote_current": heads,
        "rt.option_volume_checkpoints": ({
            "cohort_id": str(plan.control_plane.cohort_id),
            "session_date": plan.control_plane.session_date,
            "contract_ticker": selected.contract_ticker,
            # Every print counts here, but flow keeps only those over its
            # premium filter, so this matches no flow event.
            "cumulative_volume": 7_999,
            "last_source_seq": 41_000,
            "last_event_time": known - timedelta(milliseconds=5),
            "baseline_kind": "SESSION_COMPLETE",
            "snapshot_volume": None,
            "snapshot_watermark_seq": None,
        },),
    }
    return routes, contracts, equities


@pytest.fixture
def assembled(monkeypatch):
    plan = producer._plan(
        market_state_committed_offset=9,
        flow_committed_offset=11,
    )
    routes, contracts, equities = _held_inputs(plan)
    from realtime import advanced_options_rolling_producer as rolling

    monkeypatch.setattr(
        rolling,
        "_active_scope",
        lambda *_args: (contracts, equities),
    )
    monkeypatch.setattr(rolling, "_reference", lambda *_args: None)
    monkeypatch.setattr(rolling, "_source_flow", lambda *_args: None)
    monkeypatch.setattr(
        rolling,
        "_order_windows",
        lambda _cursor, value, _evidence: AdvancedOptionsOrderWindowState(
            owner_epoch=value.owner_epoch,
            committed_offset=value.flow_committed_offset,
            checkpoint_sha256="b" * 64,
            windows_seconds=rolling.CANONICAL_ORDER_WINDOWS_SECONDS,
            members=(),
            maximum_members=value.maximum_cluster_members,
        ),
    )
    return plan, routes


def _reasons(result):
    return result.reason_codes


def test_preflight_is_ready_when_held_exactly_at_the_barrier(assembled):
    plan, routes = assembled

    result = assemble_advanced_options_rolling_state_cursor(
        _RoutedCursor(routes),
        plan,
    )

    assert _reasons(result) == ()
    assert result.status is RollingProducerStatus.READY
    assert result.state.committed_offset == plan.flow_committed_offset
    assert result.state.committed_at == plan.known_at


def test_preflight_is_repeatable_while_ingress_moves(assembled):
    plan, routes = assembled
    first = assemble_advanced_options_rolling_state_cursor(
        _RoutedCursor(routes),
        plan,
    )
    moved = dict(routes)
    moved["rt.stream_sessions"] = ({
        **routes["rt.stream_sessions"][0],
        "last_raw_offset": 9_999,
        "last_event_offset": 9_999,
        "heartbeat_at": plan.known_at + timedelta(seconds=120),
        "event_time_watermark": plan.known_at + timedelta(seconds=120),
    },)

    second = assemble_advanced_options_rolling_state_cursor(
        _RoutedCursor(moved),
        plan,
    )

    assert second.state == first.state


@pytest.mark.parametrize(
    "change,reason",
    (
        (
            lambda routes, plan: routes.update({
                "rt.consumer_checkpoints": tuple(
                    {
                        **row,
                        "committed_offset": row["committed_offset"] + 1,
                    }
                    for row in routes["rt.consumer_checkpoints"]
                ),
            }),
            "CONSUMER_CHECKPOINT_OFFSET_MISMATCH",
        ),
        (
            lambda routes, plan: routes.update({
                "rt.quote_current": tuple(
                    {
                        **row,
                        "event_at": plan.known_at + timedelta(seconds=1),
                    }
                    for row in routes["rt.quote_current"]
                ),
            }),
            "CANONICAL_CURRENT_HEAD_AHEAD_OF_CHECKPOINT",
        ),
        (
            lambda routes, plan: routes.update({
                "rt.quote_current": tuple(
                    {
                        **row,
                        "source_offset": (
                            plan.market_state_committed_offset + 1
                        ),
                    }
                    for row in routes["rt.quote_current"]
                ),
            }),
            "CANONICAL_CURRENT_HEAD_AHEAD_OF_CHECKPOINT",
        ),
        (
            lambda routes, plan: routes.update({
                "rt.option_volume_checkpoints": tuple(
                    {
                        **row,
                        "last_event_time": (
                            plan.known_at + timedelta(seconds=1)
                        ),
                    }
                    for row in routes["rt.option_volume_checkpoints"]
                ),
            }),
            "OPTION_VOLUME_CHECKPOINT_WATERMARK_MISMATCH",
        ),
    ),
)
def test_data_past_the_cutoff_blocks_the_window_without_ending_the_role(
    assembled,
    change,
    reason,
):
    plan, routes = assembled
    routes = dict(routes)
    change(routes, plan)

    result = assemble_advanced_options_rolling_state_cursor(
        _RoutedCursor(routes),
        plan,
    )

    assert result.status is RollingProducerStatus.BLOCKED
    assert reason in _reasons(result)
    assert result.state is None


def _option_heads():
    binding = producer._equity_binding()
    plan = producer._plan(
        equity_live_head_binding=binding,
        market_state_committed_offset=10,
    )
    rows, contracts, equities = producer._separate_head_inputs(plan, binding)
    return plan, rows, contracts, equities


def test_quiet_contract_with_no_print_is_not_a_blocked_window():
    plan, rows, contracts, equities = _option_heads()
    evidence = []
    without_option_trade = tuple(
        row
        for row in rows
        if not (
            row["kind"] == "T"
            and row["instrument_id"] == contracts[0]["instrument_id"]
        )
    )

    assert _market_state(
        producer._HeadCursor(without_option_trade),
        plan,
        contracts,
        equities,
        evidence,
    ) is not None
    assert evidence[-1].availability.value == "READY"


def test_option_with_no_quote_still_blocks():
    plan, rows, contracts, equities = _option_heads()
    evidence = []
    without_option_quote = tuple(
        row
        for row in rows
        if not (
            row["kind"] == "Q"
            and row["instrument_id"] == contracts[0]["instrument_id"]
        )
    )

    assert _market_state(
        producer._HeadCursor(without_option_quote),
        plan,
        contracts,
        equities,
        evidence,
    ) is None
    assert "CANONICAL_CURRENT_HEADS_INCOMPLETE" in evidence[-1].reason_codes


def test_excluded_last_print_on_an_option_does_not_block():
    plan, rows, contracts, equities = _option_heads()
    evidence = []
    excluded = tuple(
        {
            **row,
            "eligibility": "EXCLUDED",
            "reason_codes": ["CANCELED"],
        }
        if (
            row["kind"] == "T"
            and row["instrument_id"] == contracts[0]["instrument_id"]
        )
        else row
        for row in rows
    )

    assert _market_state(
        producer._HeadCursor(excluded),
        plan,
        contracts,
        equities,
        evidence,
    ) is not None


def test_equity_heads_keep_their_received_clock_check():
    plan, rows, contracts, equities = _option_heads()
    evidence = []
    late = tuple(
        {
            **row,
            "received_at": plan.equity_live_head_binding.observed_at
            + timedelta(seconds=1),
        }
        if row["instrument_id"] == equities[0]["instrument_id"]
        else row
        for row in rows
    )

    assert _market_state(
        producer._HeadCursor(late),
        plan,
        contracts,
        equities,
        evidence,
    ) is None
    assert "CANONICAL_CURRENT_HEAD_AHEAD_OF_CHECKPOINT" in (
        evidence[-1].reason_codes
    )


def test_volume_is_not_compared_with_the_last_retained_flow_event():
    plan = producer._plan()
    selected = plan.enrollment.activity_selection.contracts
    contracts = tuple(
        {"contract_ticker": row.contract_ticker} for row in selected
    )
    rows = tuple(
        {
            "cohort_id": str(plan.control_plane.cohort_id),
            "session_date": plan.control_plane.session_date,
            "contract_ticker": row["contract_ticker"],
            "cumulative_volume": 7_999,
            "last_source_seq": 41_000,
            "last_event_time": plan.known_at - timedelta(seconds=1),
            "baseline_kind": "SESSION_COMPLETE",
            "snapshot_volume": None,
            "snapshot_watermark_seq": None,
        }
        for row in contracts
    )
    evidence = []

    assert _volumes(
        producer._HeadCursor(rows),
        plan,
        contracts,
        evidence,
    ) is not None
    assert evidence[-1].availability.value == "READY"


def test_checkpoint_written_after_the_cutoff_is_still_ready():
    plan = producer._plan()
    generation = str(plan.source_flow.journal_generation)

    def row(owner, shard_map, offset):
        return {
            "consumer_name": owner,
            "journal_generation": generation,
            "shard_id": 0,
            "shard_map_sha256": shard_map,
            "owner_epoch": plan.owner_epoch + 4,
            "committed_offset": offset,
            "checkpoint_id": str(uuid4()),
            "state_hash": "c" * 64,
            "committed_at": plan.known_at + timedelta(seconds=2),
        }

    evidence = []

    assert _checkpoints(
        producer._HeadCursor((
            row(
                CANONICAL_MARKET_STATE_OWNER,
                plan.market_state_shard_map_sha256,
                plan.market_state_committed_offset,
            ),
            row(
                CANONICAL_OPTION_FLOW_OWNER,
                plan.flow_shard_map_sha256,
                plan.flow_committed_offset,
            ),
        )),
        plan,
        evidence,
    ) is not None
    assert {item.availability.value for item in evidence} == {"READY"}


# --- a window with no qualifying print --------------------------------------------


def test_quiet_session_takes_the_flow_policy_from_the_engine(monkeypatch):
    class Cursor:
        row = None

        def execute(self, _statement, _parameters):
            return None

        def fetchone(self):
            return Cursor.row

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=Cursor())

    provider = SimpleNamespace(transaction=transaction)
    control = SimpleNamespace(cohort_id=UUID(int=1))
    source_flow = SimpleNamespace(
        source_flow_id=UUID(int=2),
        journal_generation=UUID(int=3),
    )
    seen = []

    def config(environment, settings, cohort_id, generation, *, source_flow):
        seen.append((cohort_id, generation, source_flow))
        return SimpleNamespace(policy_sha256=lambda: "e" * 64)

    monkeypatch.setattr(window_module, "_engine_config", config)
    environment = SimpleNamespace(options_flow_settings=object())

    assert window_module._flow_runtime_policy_sha256(
        provider,
        environment,
        control,
        source_flow,
    ) == "e" * 64
    assert seen == [(UUID(int=1), UUID(int=3), source_flow)]

    Cursor.row = ("d" * 64,)
    assert window_module._flow_runtime_policy_sha256(
        provider,
        environment,
        control,
        source_flow,
    ) == "d" * 64


def test_package_mark_accepts_a_quote_received_just_after_the_cutoff(
    monkeypatch,
):
    received = {}

    def materialize(_package, quotes, *, checked_at, policy):
        received.update(quotes)
        return SimpleNamespace(mark="mark", reason_codes=())

    monkeypatch.setattr(
        window_module,
        "materialize_realtime_option_package_mark",
        materialize,
    )
    generation = UUID(int=1)
    mark = SimpleNamespace(
        instrument_id=7,
        journal_generation=generation,
        stream_session_id=UUID(int=2),
        source_offset=5,
        market_time=CUTOFF - timedelta(milliseconds=2),
        observed_time=CUTOFF + timedelta(milliseconds=30),
        bid=window_module.Decimal("1"),
        ask=window_module.Decimal("2"),
        bid_size=1,
        ask_size=1,
        payload_sha256="a" * 64,
    )
    package = SimpleNamespace(
        legs=(SimpleNamespace(instrument_id=7, contract_ticker=OPTION),),
    )

    assert window_module._package_mark(
        package,
        (mark,),
        object(),
        CUTOFF,
    ) == "mark"
    assert received[7].received_at == CUTOFF
    assert received[7].event_at == mark.market_time


# --- a stop is not the end of a session -------------------------------------------


def _authority(stop):
    return SimpleNamespace(
        authorization_id=UUID(int=9),
        acquisition_stop=stop,
        cohort_id=UUID(int=4),
        journal_generation=UUID(int=5),
    )


def _bundle():
    return {
        "orchestration": {"catalog_version": STAGE_CATALOG_VERSION_V5},
        "v3_authorities": {"session_acquisition": {}},
    }


def _session_stop(monkeypatch, *, seconds_to_stop):
    authority = _authority(
        datetime.now(UTC) + timedelta(seconds=seconds_to_stop)
    )
    monkeypatch.setattr(
        roles_module,
        "session_acquisition_authorization_from_payload",
        lambda _value: authority,
    )
    return authority


def test_a_stop_before_the_session_end_pauses_consumers(monkeypatch):
    _session_stop(monkeypatch, seconds_to_stop=600)
    stop = threading.Event()
    writing = SimpleNamespace(status="WRITING")

    assert not roles_module._stop_pauses_session(_bundle(), stop, writing)

    stop.set()

    assert roles_module._stop_pauses_session(_bundle(), stop, writing)
    assert roles_module._stop_pauses_session(_bundle(), stop, None)


@pytest.mark.parametrize("status", ("DRAINING", "DRAINED"))
def test_a_sealing_journal_still_drains_on_stop(monkeypatch, status):
    _session_stop(monkeypatch, seconds_to_stop=600)
    stop = threading.Event()
    stop.set()

    assert not roles_module._stop_pauses_session(
        _bundle(),
        stop,
        SimpleNamespace(status=status),
    )


def test_a_stop_at_the_session_end_drains_as_before(monkeypatch):
    _session_stop(monkeypatch, seconds_to_stop=-1)
    stop = threading.Event()
    stop.set()

    assert not roles_module._stop_pauses_session(
        _bundle(),
        stop,
        SimpleNamespace(status="WRITING"),
    )


def test_older_stage_catalogs_keep_their_stop_behaviour():
    stop = threading.Event()
    stop.set()

    assert not roles_module._stop_pauses_session(
        {"orchestration": {"catalog_version": "stages_v4"}},
        stop,
        None,
    )


def _observer(monkeypatch, tmp_path, status, *, seconds_to_stop=600):
    """The ingress role over a status file, with its database effects recorded."""
    authority = _session_stop(monkeypatch, seconds_to_stop=seconds_to_stop)
    generation = authority.journal_generation
    contract = SimpleNamespace(contract_ticker=OPTION)
    binding = SimpleNamespace(
        window_id=UUID(int=11),
        enrollment=SimpleNamespace(
            control_plane=SimpleNamespace(
                cohort_id=authority.cohort_id,
                manifest_id=UUID(int=6),
                plan_id=UUID(int=7),
                session_date=CUTOFF.date(),
                source_flow=SimpleNamespace(journal_generation=generation),
            ),
            activity_selection=SimpleNamespace(contracts=(contract,)),
        ),
    )
    acquisition = SimpleNamespace(
        authority=authority,
        contract_tickers=(OPTION,),
        binding_sha256="a" * 64,
        owner_id=UUID(int=8),
        stream_session_id=UUID(int=10),
    )
    journal_path = (
        tmp_path / "options-acquisition" / str(generation) / "options.sqlite"
    )
    journal_path.parent.mkdir(parents=True)
    journal_path.write_bytes(b"journal")
    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({
        "schema": ACQUISITION_STATUS_SCHEMA,
        "binding_sha256": acquisition.binding_sha256,
        "owner_id": str(acquisition.owner_id),
        "journal_generation": str(generation),
        "connection_attempts": 2,
        "decoded_messages": 20,
        "decoded_bytes": 200,
        "last_raw_offset": 9,
        "last_event_offset": 12,
        "last_market_event_at_utc": CUTOFF.isoformat(),
        **status,
    }), encoding="utf-8")
    registration = SimpleNamespace(
        status="WRITING",
        owner_epoch=1,
        journal_generation=generation,
        shard_ids=(0,),
        shard_map_sha256="a" * 64,
        raw_committed_offset=0,
        normalized_committed_offset=0,
    )
    stop = threading.Event()
    effects = {"sealed": [], "ended_at": [], "offsets": []}

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=object())

    for name, value in {
        "_selection_plan": lambda _state: None,
        "_lease": lambda *_args: (UUID(int=8), 1),
        "_journal_root": lambda _environment: tmp_path,
        "_shard_map": lambda *_args: SimpleNamespace(
            shard_count=1,
            sha256="a" * 64,
        ),
        "_registration": lambda *_args: registration,
        "RawJournalReader": lambda _path: SimpleNamespace(
            read_after=lambda *_args, **_kwargs: (),
            latest_event_at=lambda **_kwargs: None,
        ),
        "_record_v3_stream_session_cursor": (
            lambda _cursor, _acquisition, **kwargs: effects["ended_at"].append(
                kwargs["ended_at"]
            )
        ),
        "update_advanced_journal_offsets_cursor": (
            lambda *_args, **kwargs: effects["offsets"].append(kwargs)
        ),
        "seal_advanced_journal_cursor": (
            lambda *_args, **kwargs: effects["sealed"].append(kwargs)
        ),
        "_snapshot_v3_resident_windows": lambda *_args, **_kwargs: 0,
    }.items():
        monkeypatch.setattr(roles_module, name, value)
    run = lambda: roles_module._run_v3_acquisition_observer(  # noqa: E731
        package={},
        bundle=_bundle(),
        binding=binding,
        provider=SimpleNamespace(transaction=transaction),
        environment={
            "REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH": str(status_path),
        },
        stop_event=stop,
        session_state=SimpleNamespace(
            load_acquisition_binding=lambda: acquisition,
        ),
    )
    return run, stop, effects


def _stop_soon(stop, seconds=0.3):
    timer = threading.Timer(seconds, stop.set)
    timer.start()
    return timer


def test_paused_acquisition_leaves_the_journal_open(monkeypatch, tmp_path):
    run, stop, effects = _observer(
        monkeypatch,
        tmp_path,
        {"state": "PAUSED", "socket_state": "STOPPED"},
    )
    _stop_soon(stop)

    evidence = run()

    assert evidence["status"] == "PAUSED"
    assert evidence["kind"] == "INGRESS"
    assert evidence["raw_end_offset"] == 9
    assert evidence["connection_attempts"] == 2
    assert effects["sealed"] == []
    assert effects["ended_at"]
    assert set(effects["ended_at"]) == {None}


def test_ingress_stopped_while_capturing_leaves_the_journal_open(
    monkeypatch,
    tmp_path,
):
    run, stop, effects = _observer(
        monkeypatch,
        tmp_path,
        {"state": "CAPTURING", "socket_state": "LIVE"},
    )
    _stop_soon(stop)

    assert run()["status"] == "PAUSED"
    assert effects["sealed"] == []


def test_ingress_stopped_before_any_data_is_not_a_failure(
    monkeypatch,
    tmp_path,
):
    run, stop, effects = _observer(
        monkeypatch,
        tmp_path,
        {"state": "CONNECTING", "socket_state": "CONNECTING"},
    )
    stop.set()

    assert run()["status"] == "PAUSED"
    assert effects["sealed"] == []


def test_real_end_of_session_still_seals(monkeypatch, tmp_path):
    run, _stop, effects = _observer(
        monkeypatch,
        tmp_path,
        {"state": "COMPLETE", "socket_state": "STOPPED"},
    )

    evidence = run()

    assert evidence["status"] == "COMPLETE"
    assert len(effects["sealed"]) == 1
    assert effects["sealed"][0]["raw_committed_offset"] == 9
    assert effects["sealed"][0]["normalized_committed_offset"] == 12
    assert effects["ended_at"][-1] == CUTOFF


def test_stop_at_the_authority_end_still_seals(monkeypatch, tmp_path):
    run, stop, effects = _observer(
        monkeypatch,
        tmp_path,
        {"state": "CAPTURING", "socket_state": "LIVE"},
        seconds_to_stop=-1,
    )
    _stop_soon(stop)

    assert run()["status"] == "COMPLETE"
    assert len(effects["sealed"]) == 1
