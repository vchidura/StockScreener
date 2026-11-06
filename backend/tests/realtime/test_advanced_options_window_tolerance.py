"""A window survives what one contract, one short gap or one retry does to it."""
from __future__ import annotations

import inspect
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from advanced_options_rolling_support import rolling_state
from realtime import advanced_options_mark_producer as mark_module
from realtime import advanced_options_p8_production as production
from realtime import advanced_options_p8_roles as roles_module
from realtime.advanced_options_mark_producer import (
    AdvancedOptionsMarkEvidence,
    AdvancedOptionsMarkPreflight,
    AdvancedOptionsMarkPreflightBlocked,
    AdvancedOptionsMarkStatus,
    assemble_advanced_options_marks_cursor,
)
from realtime.advanced_options_native_loader import _packages_for_events
from realtime import advanced_options_native_loader as native_loader
from realtime.advanced_options_postgres import (
    PostgresAdvancedOptionsCanonicalRuntimePort as Port,
)
from realtime.advanced_options_rolling_producer import (
    MAXIMUM_TOLERATED_GAP_SECONDS,
    AdvancedOptionsRollingProducerEvidence,
    AdvancedOptionsRollingProducerPreflight,
    RollingProducerStatus,
    _gaps,
    _market_state,
    advanced_options_gap_blocks,
    advanced_options_unresolved_gaps_sha256,
)
from realtime.advanced_options_rolling_repository import (
    decode_advanced_options_rolling_state,
    persist_advanced_options_rolling_state_cursor,
)
from realtime.advanced_options_rolling_state import (
    MAXIMUM_EXCLUDED_CONTRACT_SHARE,
    AdvancedOptionsWindowRetryDiverged,
    RollingAvailability,
    build_advanced_options_rolling_state,
)
from realtime.contracts import canonical_json, canonical_sha256
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5
from tests.realtime import test_advanced_options_mark_producer as marks
from tests.realtime import test_advanced_options_rolling_producer as producer
from test_advanced_options_consumer_hold import _option_heads

UTC = timezone.utc


# --- 1. gaps ----------------------------------------------------------------------


class _RowsCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement, _parameters=None):
        return None

    def fetchall(self):
        return self.rows


def _gap(plan, *, seconds=1.4, ends_before=1.0, status="UNRESOLVED", open_ended=False):
    gap_end = plan.known_at - timedelta(seconds=ends_before)
    return {
        "gap_id": str(uuid4()),
        "session_id": str(plan.source_flow.stream_session_ids[0]),
        "instrument_id": None,
        "channel": None,
        "gap_start": gap_end - timedelta(seconds=seconds),
        "gap_end": None if open_ended else gap_end,
        "journal_generation": str(plan.source_flow.journal_generation),
        "last_durable_offset": 5,
        "reason": "GATEWAY_SESSION_CHANGED",
        "recovery_status": status,
    }


def test_short_ended_gap_stays_in_the_digest_and_does_not_block():
    plan = producer._plan()
    rows = (_gap(plan),)
    evidence = []

    digest = _gaps(_RowsCursor(rows), plan, evidence)

    assert digest == advanced_options_unresolved_gaps_sha256(rows)
    assert digest != canonical_sha256(())
    assert evidence[-1].availability is RollingAvailability.READY
    assert evidence[-1].reason_codes == ("TOLERATED_STREAM_GAPS",)
    assert evidence[-1].evidence_sha256 == canonical_sha256(
        tuple(tuple(row.values()) for row in rows)
    )
    # A tolerated gap is not a reason the window is blocked.
    assert AdvancedOptionsRollingProducerPreflight(
        RollingProducerStatus.READY,
        plan.plan_id,
        "a" * 64,
        tuple(evidence),
        None,
    ).reason_codes == ()


def test_no_gap_keeps_the_empty_digest():
    plan = producer._plan()
    evidence = []

    assert _gaps(_RowsCursor(()), plan, evidence) == canonical_sha256(())
    assert evidence[-1].reason_codes == ()


@pytest.mark.parametrize(
    "changes",
    (
        {"open_ended": True},
        {"status": "WARMING"},
        {"seconds": MAXIMUM_TOLERATED_GAP_SECONDS + 1},
        {"ends_before": -1.0},
        {"ends_before": 0.0},
    ),
    ids=("open", "warming", "too-long", "ends-after-barrier", "ends-at-barrier"),
)
def test_gap_still_blocks(changes):
    plan = producer._plan()
    evidence = []
    rows = (_gap(plan), _gap(plan, **changes))

    assert _gaps(_RowsCursor(rows), plan, evidence) is None
    assert evidence[-1].availability is RollingAvailability.UNAVAILABLE
    assert evidence[-1].reason_codes == ("UNRESOLVED_OR_WARMING_STREAM_GAPS",)


def _accepted_gap(**changes):
    return {
        "gap_start": datetime(2026, 11, 5, 15, 18, 12, 825000, tzinfo=UTC),
        "gap_end": datetime(2026, 11, 5, 15, 33, 53, 400000, tzinfo=UTC),
        "reason": "GATEWAY_SESSION_CHANGED",
        "recovery_status": "UNRESOLVED",
        **changes,
    }


def test_the_accepted_nov_5_gap_stops_blocking_once_it_is_over():
    assert not advanced_options_gap_blocks(
        _accepted_gap(), datetime(2026, 11, 5, 15, 35, tzinfo=UTC)
    )
    assert advanced_options_gap_blocks(
        _accepted_gap(), datetime(2026, 11, 5, 15, 30, tzinfo=UTC)
    )


@pytest.mark.parametrize(
    "changes",
    (
        {
            "gap_start": datetime(2026, 11, 5, 16, 0, tzinfo=UTC),
            "gap_end": datetime(2026, 11, 5, 16, 10, tzinfo=UTC),
        },
        {
            "gap_start": datetime(2026, 11, 6, 15, 18, 12, 825000, tzinfo=UTC),
            "gap_end": datetime(2026, 11, 6, 15, 33, 53, 400000, tzinfo=UTC),
        },
        {"gap_end": datetime(2026, 11, 5, 15, 34, tzinfo=UTC)},
        {"reason": "OTHER"},
        {"recovery_status": "WARMING"},
        {"gap_end": None},
    ),
    ids=("later-that-day", "next-day", "runs-longer", "other-reason", "warming", "open"),
)
def test_no_other_long_gap_is_accepted(changes):
    assert advanced_options_gap_blocks(
        _accepted_gap(**changes), datetime(2026, 11, 7, tzinfo=UTC)
    )


def test_gap_of_exactly_the_limit_is_tolerated():
    plan = producer._plan()
    rows = (_gap(plan, seconds=MAXIMUM_TOLERATED_GAP_SECONDS),)

    assert _gaps(_RowsCursor(rows), plan, []) is not None


def test_postgres_gap_digest_agrees_with_the_rolling_binding():
    plan = producer._plan()
    rows = (_gap(plan), _gap(plan, seconds=3))
    control = SimpleNamespace(source_flow=plan.source_flow)

    assert Port._unresolved_gaps_sha256(
        _RowsCursor(tuple(tuple(row.values()) for row in rows)),
        control,
    ) == advanced_options_unresolved_gaps_sha256(rows)


def _rolling(**changes):
    state = rolling_state(cohort_id=uuid4(), journal_generation=uuid4())
    values = {
        name: getattr(state, name)
        for name in (
            "cohort_id",
            "session_date",
            "journal_generation",
            "owner_epoch",
            "committed_offset",
            "committed_at",
            "market_state_checkpoint_sha256",
            "flow_checkpoint_sha256",
            "underlying_ticks",
            "contract_volumes",
            "order_windows",
        )
    }
    return state, build_advanced_options_rolling_state(**values, **changes)


def test_degraded_state_round_trips_and_a_clean_state_keeps_its_identity():
    state, clean = _rolling()
    _, degraded = _rolling(
        degraded_reason_codes=("TOLERATED_STREAM_GAPS",),
        excluded_contracts=(("7:O:SPY", ("OPTION_CONTRACTS_EXCLUDED",)),),
    )

    assert "degraded_reason_codes" not in clean.identity
    assert clean.state_sha256 == state.state_sha256
    assert degraded.state_sha256 != clean.state_sha256
    import json

    document = json.loads(canonical_json(degraded.identity))
    decoded = decode_advanced_options_rolling_state(
        state_id=degraded.state_id,
        state_sha256=degraded.state_sha256,
        document=document,
    )
    assert decoded.degraded_reason_codes == ("TOLERATED_STREAM_GAPS",)
    assert decoded.excluded_contracts == degraded.excluded_contracts


# --- 2. one bad contract ----------------------------------------------------------


def _five_contracts():
    plan, rows, contracts, equities = _option_heads()
    option_id = contracts[0]["instrument_id"]
    option_rows = tuple(row for row in rows if row["instrument_id"] == option_id)
    equity_rows = tuple(row for row in rows if row["instrument_id"] != option_id)
    ids = tuple(option_id + index for index in range(5))
    return (
        plan,
        equity_rows,
        tuple(
            {**row, "instrument_id": instrument_id}
            for instrument_id in ids
            for row in option_rows
        ),
        tuple(
            {**contracts[0], "instrument_id": instrument_id}
            for instrument_id in ids
        ),
        equities,
        ids,
    )


def _spoil(rows, instrument_id, **changes):
    return tuple(
        {**row, **changes}
        if row["instrument_id"] == instrument_id and row["kind"] == "Q"
        else row
        for row in rows
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"quality": "CROSSED"}, "CANONICAL_CURRENT_HEAD_INVALID"),
        ({"reason_codes": ["ONE_SIDED"]}, "CANONICAL_CURRENT_HEAD_INVALID"),
        ({"bid": None}, "CANONICAL_CURRENT_HEAD_INVALID"),
        ({"ask": None}, "CANONICAL_CURRENT_HEAD_INVALID"),
    ),
)
def test_one_bad_quote_leaves_out_one_contract(changes, reason):
    plan, equity_rows, option_rows, contracts, equities, ids = _five_contracts()
    evidence = []

    result = _market_state(
        producer._HeadCursor((*equity_rows, *_spoil(option_rows, ids[2], **changes))),
        plan,
        contracts,
        equities,
        evidence,
    )

    assert result is not None
    excluded = [row for row in evidence if row.member_kind == "EXCLUDED_CONTRACT"]
    assert [row.member_key for row in excluded] == [str(ids[2])]
    assert reason in excluded[0].reason_codes
    assert excluded[0].availability is RollingAvailability.READY
    assert evidence[-1].availability is RollingAvailability.READY


def test_missing_quote_and_old_session_head_each_leave_out_one_contract():
    plan, equity_rows, option_rows, contracts, equities, ids = _five_contracts()
    for rows, reason in (
        (
            tuple(
                row
                for row in option_rows
                if not (row["instrument_id"] == ids[0] and row["kind"] == "Q")
            ),
            "CANONICAL_CURRENT_HEADS_INCOMPLETE",
        ),
        (
            tuple(
                {**row, "journal_generation": str(uuid4())}
                if row["instrument_id"] == ids[0]
                else row
                for row in option_rows
            ),
            "CANONICAL_CURRENT_HEAD_OTHER_SESSION",
        ),
    ):
        evidence = []
        assert _market_state(
            producer._HeadCursor((*equity_rows, *rows)),
            plan,
            contracts,
            equities,
            evidence,
        ) is not None
        excluded = [
            row for row in evidence if row.member_kind == "EXCLUDED_CONTRACT"
        ]
        assert [row.member_key for row in excluded] == [str(ids[0])]
        assert reason in excluded[0].reason_codes


def test_last_session_print_is_a_quiet_contract_not_a_left_out_one():
    plan, equity_rows, option_rows, contracts, equities, _ids = _five_contracts()
    evidence = []
    old_prints = tuple(
        {
            **row,
            "journal_generation": str(uuid4()),
            "source_offset": plan.market_state_committed_offset + 99,
        }
        if row["kind"] == "T"
        else row
        for row in option_rows
    )

    assert _market_state(
        producer._HeadCursor((*equity_rows, *old_prints)),
        plan,
        contracts,
        equities,
        evidence,
    ) is not None
    assert not [
        row for row in evidence if row.member_kind == "EXCLUDED_CONTRACT"
    ]


def test_far_future_head_leaves_out_one_contract():
    plan, equity_rows, option_rows, contracts, equities, ids = _five_contracts()
    evidence = []
    future = plan.known_at + timedelta(hours=4)

    assert _market_state(
        producer._HeadCursor((
            *equity_rows,
            *_spoil(option_rows, ids[1], event_at=future),
        )),
        plan,
        contracts,
        equities,
        evidence,
    ) is not None
    assert evidence[-1].availability is RollingAvailability.READY


def test_more_than_the_limit_out_of_an_underlying_blocks():
    plan, equity_rows, option_rows, contracts, equities, ids = _five_contracts()
    assert MAXIMUM_EXCLUDED_CONTRACT_SHARE * len(ids) == 2.5
    evidence = []
    rows = _spoil(
        _spoil(
            _spoil(option_rows, ids[0], quality="CROSSED"),
            ids[1],
            quality="CROSSED",
        ),
        ids[2],
        quality="CROSSED",
    )

    assert _market_state(
        producer._HeadCursor((*equity_rows, *rows)),
        plan,
        contracts,
        equities,
        evidence,
    ) is None
    assert "OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT" in evidence[-1].reason_codes
    assert "CANONICAL_CURRENT_HEAD_INVALID" in evidence[-1].reason_codes


def test_equity_heads_stay_strict():
    plan, equity_rows, option_rows, contracts, equities, _ids = _five_contracts()
    evidence = []
    spoiled = tuple(
        {**row, "quality": "CROSSED"} if row["kind"] == "Q" else row
        for row in equity_rows
    )

    assert _market_state(
        producer._HeadCursor((*spoiled, *option_rows)),
        plan,
        contracts,
        equities,
        evidence,
    ) is None
    assert "CANONICAL_CURRENT_HEAD_INVALID" in evidence[-1].reason_codes


def test_ready_evidence_refuses_reasons_outside_what_a_window_tolerates():
    with pytest.raises(ValueError, match="availability is invalid"):
        AdvancedOptionsRollingProducerEvidence(
            "MARKET_STATE_HEADS",
            "key",
            RollingAvailability.READY,
            "a" * 64,
            reason_codes=("ANYTHING",),
        )


def _mark_rows(plan, **quote):
    now = marks.NOW
    rows = marks._quote_rows(
        plan,
        event_at=now - timedelta(milliseconds=2),
        received_at=now - timedelta(milliseconds=1),
    )
    return tuple({**row, **quote} for row in rows)


def _replace_row(rows, index, **changes):
    return tuple(
        {**row, **changes} if position == index else row
        for position, row in enumerate(rows)
    )


class _FiveContractPlan:
    """The fixture's plan with five contracts on its one underlying. A real plan
    binds its selection's identity, so its validation is stubbed for these."""

    def __init__(self, plan):
        self._plan = plan
        base = plan.enrollment.activity_selection.contracts[0]
        self.enrollment = SimpleNamespace(
            activity_selection=SimpleNamespace(
                contracts=tuple(
                    SimpleNamespace(
                        instrument_id=base.instrument_id + index,
                        catalog_version_id=base.catalog_version_id + index,
                        contract_id=base.contract_id + index,
                        contract_ticker=f"{base.contract_ticker}{index}",
                        underlyer=base.underlyer,
                    )
                    for index in range(5)
                ),
            ),
        )

    def __getattr__(self, name):
        return getattr(self._plan, name)


@pytest.fixture
def five_contract_marks(monkeypatch):
    monkeypatch.setattr(mark_module, "_validate_plan", lambda _plan: {})
    plan = _FiveContractPlan(marks._plan())
    rows = _mark_rows(plan)
    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(rows),
        plan,
    )
    assert result.status is AdvancedOptionsMarkStatus.READY
    assert len(result.marks) == 5
    return plan, rows


def test_one_bad_quote_costs_that_contract_alone_its_mark(five_contract_marks):
    plan, rows = five_contract_marks

    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(_replace_row(rows, 2, quality="CROSSED")),
        plan,
    )

    assert result.status is AdvancedOptionsMarkStatus.READY
    assert len(result.marks) == 4
    assert rows[2]["instrument_id"] not in {
        row.instrument_id for row in result.marks
    }
    named = [
        row for row in result.evidence
        if row.status is AdvancedOptionsMarkStatus.BLOCKED
    ]
    assert [row.contract_ticker for row in named] == [
        rows[2]["contract_ticker"]
    ]
    assert result.reason_codes == ()


def test_quiet_contracts_lose_their_marks_and_the_window_continues(
    five_contract_marks,
):
    plan, rows = five_contract_marks
    stale = marks.NOW - timedelta(seconds=30)
    rows = tuple(
        {**row, "event_at": stale, "received_at": stale} for row in rows
    )

    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(rows),
        plan,
    )

    assert result.status is AdvancedOptionsMarkStatus.READY
    assert result.marks == ()
    assert all(
        "OPTION_NBBO_STALE_OR_NONCAUSAL" in row.reason_codes
        for row in result.evidence
    )
    assert result.reason_codes == ()


def test_quotes_across_contracts_need_not_share_a_moment(five_contract_marks):
    plan, rows = five_contract_marks

    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(_replace_row(
            rows,
            0,
            event_at=marks.NOW - timedelta(milliseconds=900),
        )),
        plan,
    )

    assert result.status is AdvancedOptionsMarkStatus.READY
    assert len(result.marks) == 5


def test_broken_quotes_beyond_the_limit_block_marks(five_contract_marks):
    plan, rows = five_contract_marks
    rows = _replace_row(
        _replace_row(
            _replace_row(rows, 0, quality="CROSSED"),
            1,
            bid=None,
        ),
        2,
        quality="CROSSED",
    )

    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(rows),
        plan,
    )

    assert result.status is AdvancedOptionsMarkStatus.BLOCKED
    assert result.marks == ()
    assert "OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT" in result.reason_codes


def test_mark_head_offset_is_compared_with_the_normalized_barrier():
    raw_only = marks._plan()
    rows = tuple(
        {**row, "source_offset": raw_only.rolling_state.committed_offset + 50}
        for row in _mark_rows(raw_only)
    )
    assert assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(rows),
        raw_only,
    ).status is AdvancedOptionsMarkStatus.BLOCKED

    from dataclasses import replace

    from realtime.advanced_options_mark_producer import (
        build_advanced_options_mark_producer_plan,
    )

    plan = build_advanced_options_mark_producer_plan(
        enrollment=raw_only.enrollment,
        reference_state=raw_only.reference_state,
        rolling_state=raw_only.rolling_state,
        source_flow=raw_only.source_flow,
        cutoff=raw_only.cutoff,
        producer_build_id=raw_only.producer_build_id,
        producer_build_sha256=raw_only.producer_build_sha256,
        valuation_policy=raw_only.valuation_policy,
        market_state_committed_offset=(
            raw_only.rolling_state.committed_offset + 50
        ),
    )
    assert replace(plan).plan_sha256 != raw_only.plan_sha256
    result = assemble_advanced_options_marks_cursor(
        marks._QuoteCursor(rows),
        plan,
    )
    assert result.status is AdvancedOptionsMarkStatus.READY
    assert len(result.marks) == len(rows)


def test_ready_marks_with_a_left_out_contract_give_no_blocking_reason():
    preflight = AdvancedOptionsMarkPreflight(
        status=AdvancedOptionsMarkStatus.READY,
        plan_id=uuid4(),
        preflight_sha256="7" * 64,
        marks_sha256="8" * 64,
        marks=(),
        evidence=(
            AdvancedOptionsMarkEvidence(
                1,
                "O:SPY",
                AdvancedOptionsMarkStatus.BLOCKED,
                "9" * 64,
                ("OPTION_NBBO_ABSENT",),
            ),
        ),
    )

    assert preflight.reason_codes == ()
    assert Port._persisted_mark_reasons_cursor(None, ()) == ()


def test_package_on_a_contract_with_no_mark_is_not_selected(monkeypatch):
    event_id = uuid4()
    candidate_id = uuid4()
    package = SimpleNamespace(
        candidate_id=candidate_id,
        legs=(SimpleNamespace(instrument_id=7),),
    )
    monkeypatch.setattr(
        native_loader,
        "load_realtime_option_packages_cursor",
        lambda *_args, **_kwargs: (package,),
    )
    rows = (
        {"event_id": str(event_id), "candidate_id": str(candidate_id)},
    )

    assert _packages_for_events(
        _RowsCursor(rows),
        cohort_id=uuid4(),
        event_ids=(event_id,),
        unmarked_instrument_ids=(7,),
    ) == {}
    assert _packages_for_events(
        _RowsCursor(rows),
        cohort_id=uuid4(),
        event_ids=(event_id,),
        unmarked_instrument_ids=(8,),
    ) == {event_id: package}


# --- 4. retry of a running window -------------------------------------------------


class _WindowCursor:
    def __init__(self, row):
        self.row = row

    def execute(self, _statement, _parameters=None):
        return None

    def fetchone(self):
        return self.row


def _fenced(*, state, owner, lock_window):
    cutoff = datetime(2026, 11, 5, 15, 0, tzinfo=UTC)
    session_date = cutoff.date()
    cohort_id = uuid4()
    plan_id = uuid4()
    control = SimpleNamespace(
        cohort_id=cohort_id,
        session_date=session_date,
        stored_identity=None,
        binding=SimpleNamespace(
            options_tier=SimpleNamespace(value="MASSIVE_ADVANCED"),
        ),
    )
    plan = SimpleNamespace(
        window_id=uuid4(),
        service_plan_id=plan_id,
        control_plane=control,
        cutoff=cutoff,
        enrollment=SimpleNamespace(enrollment_sha256="a" * 64),
    )
    row = (
        plan_id,
        cohort_id,
        session_date,
        "stages_v5",
        ["OPTIONS_INPUT_READY"],
        "a" * 64,
        cutoff + timedelta(hours=1),
        plan.window_id,
        plan_id,
        cohort_id,
        "MASSIVE_ADVANCED",
        "key",
        cutoff,
        cutoff,
        cutoff + timedelta(hours=1),
        state,
        owner,
    )
    port = SimpleNamespace(_scope=SimpleNamespace(input_ready_plan=plan))
    return Port._input_ready_context_cursor(
        port,
        _WindowCursor(row),
        lock_window=lock_window,
    )


@pytest.mark.parametrize(
    ("state", "owner"),
    (("RUNNING", str(uuid4())), ("PENDING", str(uuid4())), ("PENDING", None)),
    ids=("running", "released", "fresh"),
)
def test_preflight_reads_a_window_an_earlier_attempt_left(state, owner):
    context = _fenced(state=state, owner=owner, lock_window=False)

    assert context.window.state == state


@pytest.mark.parametrize(
    ("state", "owner"),
    (("RUNNING", str(uuid4())), ("PENDING", str(uuid4()))),
    ids=("running", "released"),
)
def test_a_write_to_the_window_keeps_the_strict_fence(state, owner):
    with pytest.raises(ValueError, match="window fence changed"):
        _fenced(state=state, owner=owner, lock_window=True)


def test_preflight_still_refuses_a_finished_window():
    with pytest.raises(ValueError, match="window fence changed"):
        _fenced(state="COMPLETE", owner=None, lock_window=False)


def _window_role(monkeypatch, serial):
    package = {
        "orchestration": {"catalog_version": STAGE_CATALOG_VERSION_V5},
    }
    bundle = {"orchestration": dict(package["orchestration"])}
    monkeypatch.setattr(
        roles_module,
        "validate_advanced_options_v3_runtime_identity",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        roles_module,
        "_v3_resident_dependencies",
        lambda *_args, **_kwargs: {"FLOW": {"status": "COMPLETE"}},
    )
    monkeypatch.setattr(roles_module, "_BLOCKED_WINDOW_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(production, "run_v3_serial_window", serial)
    binding = SimpleNamespace(
        window_id=UUID("11111111-2222-3333-4444-555555555555"),
    )
    monkeypatch.setattr(
        production,
        "load_v3_pending_window_bindings",
        lambda **_kwargs: (binding,),
    )
    return lambda stop_event: roles_module._run_window(
        package=package,
        bundle=bundle,
        binding=binding,
        provider=object(),
        pool=object(),
        environment={},
        stop_event=stop_event,
    )


def test_retry_that_cannot_match_durable_rows_ends_as_a_failed_attempt(monkeypatch):
    stopped = threading.Event()
    attempts = []
    failed = []

    def serial(**_kwargs):
        attempts.append(1)
        raise AdvancedOptionsWindowRetryDiverged(
            "persisted rolling checkpoint conflicts"
        )

    def fail(**kwargs):
        failed.append(kwargs)
        stopped.set()
        return {"status": "FINISHED", "window_state": "FAILED"}

    monkeypatch.setattr(production, "fail_v3_window_attempt", fail)

    result = _window_role(monkeypatch, serial)(stopped)

    assert result["status"] == "COMPLETE"
    assert len(attempts) == 1
    assert failed[0]["reason"] == "persisted rolling checkpoint conflicts"
    assert next(iter(result["windows"].values()))["window_state"] == "FAILED"


def test_blocked_marks_defer_the_window_and_do_not_end_the_role(monkeypatch):
    stopped = threading.Event()
    blocked = AdvancedOptionsMarkPreflightBlocked(
        AdvancedOptionsMarkPreflight(
            status=AdvancedOptionsMarkStatus.BLOCKED,
            plan_id=uuid4(),
            preflight_sha256="7" * 64,
            marks_sha256="8" * 64,
            marks=(),
            evidence=(
                AdvancedOptionsMarkEvidence(
                    1,
                    "O:SPY",
                    AdvancedOptionsMarkStatus.BLOCKED,
                    "9" * 64,
                    ("OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT",),
                ),
            ),
        )
    )

    def serial(**_kwargs):
        stopped.set()
        raise blocked

    result = _window_role(monkeypatch, serial)(stopped)

    assert next(iter(result["windows"].values())) == {
        "status": "DEFERRED",
        "reason_codes": ["OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT"],
    }


def test_window_still_leased_waits_like_a_blocked_one(monkeypatch):
    stopped = threading.Event()
    waits = []
    real_wait = stopped.wait

    class _Stop:
        def is_set(self):
            return stopped.is_set()

        def wait(self, seconds):
            waits.append(seconds)
            stopped.set()
            return real_wait(0)

    result = _window_role(
        monkeypatch,
        lambda **_kwargs: {"status": "NOT_CLAIMED"},
    )(_Stop())

    assert waits == [0.01]
    assert result["status"] == "COMPLETE"


def test_failed_attempt_recovers_orphans_and_finishes_the_window(monkeypatch):
    calls = []

    class _Repository:
        def __init__(self, _provider):
            pass

        def claim_window(self, window_id, owner_id, now, lease_seconds):
            calls.append(("claim", window_id))
            return "claim"

        def recover_orphans(self, claim, now):
            calls.append(("recover", claim))

        def finish_window(self, claim, state, now):
            calls.append(("finish", state))

    monkeypatch.setattr(production, "PostgresOrchestrationRepository", _Repository)
    window_id = uuid4()

    result = production.fail_v3_window_attempt(
        provider=object(),
        window_id=window_id,
        reason="persisted rolling checkpoint conflicts",
        checked_at=datetime(2026, 11, 5, 15, 0, tzinfo=UTC),
    )

    assert result["window_state"] == "FAILED"
    assert calls == [
        ("claim", window_id),
        ("recover", "claim"),
        ("finish", "FAILED"),
    ]


def test_failed_attempt_waits_for_a_live_lease(monkeypatch):
    class _Repository:
        def __init__(self, _provider):
            pass

        def claim_window(self, *_args):
            return None

    monkeypatch.setattr(production, "PostgresOrchestrationRepository", _Repository)

    assert production.fail_v3_window_attempt(
        provider=object(),
        window_id=uuid4(),
        reason="x",
        checked_at=datetime(2026, 11, 5, 15, 0, tzinfo=UTC),
    ) == {"status": "NOT_CLAIMED", "persisted": False}


def test_rolling_checkpoint_conflict_is_a_diverged_retry():
    state, degraded = _rolling(degraded_reason_codes=("TOLERATED_STREAM_GAPS",))

    class _Cursor:
        def execute(self, _statement, _parameters=None):
            return None

        def fetchone(self):
            return (uuid4(), state.state_sha256, {})

    with pytest.raises(AdvancedOptionsWindowRetryDiverged, match="conflicts"):
        persist_advanced_options_rolling_state_cursor(_Cursor(), degraded)


def test_pending_loader_skips_a_window_under_a_live_lease(monkeypatch):
    statements = []

    class _Cursor:
        def execute(self, statement, parameters=None):
            statements.append((statement, parameters))

        def fetchall(self):
            return ()

    class _Provider:
        def transaction(self, *_args, **_kwargs):
            class _Unit:
                cursor = _Cursor()

                def __enter__(self):
                    return self

                def __exit__(self, *_exc):
                    return False

            return _Unit()

    monkeypatch.setattr(
        production,
        "session_acquisition_authorization_from_payload",
        lambda _payload: SimpleNamespace(authorization_id=uuid4()),
    )

    assert production.load_v3_pending_window_bindings(
        provider=_Provider(),
        binding=SimpleNamespace(
            enrollment=SimpleNamespace(
                control_plane=SimpleNamespace(cohort_id=uuid4()),
            ),
        ),
        bundle={"v3_authorities": {"session_acquisition": {}}},
        available_at=datetime(2026, 11, 5, 15, 0, tzinfo=UTC),
    ) == ()
    statement, parameters = statements[0]
    assert "window_run.lease_until<=%s" in statement
    assert statement.count("%s") == len(parameters)


def test_a_blocked_native_preflight_is_a_retryable_refusal_not_a_crash():
    from types import SimpleNamespace

    from realtime.advanced_options_native_loader import (
        AdvancedOptionsNativePreflightBlocked,
    )

    error = AdvancedOptionsNativePreflightBlocked(
        SimpleNamespace(reason_codes=("OS1_QQQ_E_CONTEXT_INCOMPLETE",))
    )

    assert isinstance(error, ValueError)
    assert error.result.reason_codes == ("OS1_QQQ_E_CONTEXT_INCOMPLETE",)
    assert "OS1_QQQ_E_CONTEXT_INCOMPLETE" in str(error)


def test_window_attempts_log_once_per_change_of_outcome(capsys):
    from realtime.advanced_options_p8_roles import _log_window_attempt

    logged = {}
    waiting = {"status": "DEFERRED", "reason_codes": ["WAITING"]}
    _log_window_attempt(logged, "window", waiting, 0.0)
    _log_window_attempt(logged, "window", waiting, 5.0)
    _log_window_attempt(logged, "window", {"status": "COMPLETE", "window_state": "COMPLETE"}, 9.0)

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 2
    assert '"status": "DEFERRED"' in lines[0]
    assert '"window_state": "COMPLETE"' in lines[1]


def test_order_windows_read_only_clusters_still_in_range():
    # A session's clusters grow all day. Nothing downstream reads older members, and
    # reading them all tripped the member bound 40 minutes into the Nov 5 rehearsal.
    from realtime import advanced_options_rolling_producer as rolling

    source = inspect.getsource(rolling._order_windows)

    assert source.count("window_end>=%s-make_interval(secs=>") == 2
    assert source.count("latest.window_end>=%s-make_interval(secs=>latest.window_seconds)") == 1


def test_reference_stage_takes_the_state_the_real_producer_returns():
    from types import SimpleNamespace
    from uuid import uuid4

    from realtime.advanced_options_loaders import AdvancedOptionsCanonicalLoaders

    state = SimpleNamespace(
        state_id=uuid4(),
        state_sha256="a" * 64,
        reference_generation=7,
    )
    loaders = AdvancedOptionsCanonicalLoaders.__new__(
        AdvancedOptionsCanonicalLoaders
    )
    loaders._port = SimpleNamespace(produce_reference_state=lambda: state)

    result = loaders.reference(None)

    assert result.state_id == state.state_id
    assert result.reference_generation == 7


class _FreezeCursor:
    def __init__(self, stored, frozen, first_cycle):
        self._rows = {
            "FROM rt.advanced_options_reference_states": stored,
            "SELECT state_id FROM rt.advanced_options_reference_session_freezes": frozen,
            "SELECT first_cycle_id": first_cycle,
        }
        self.row = None
        self.statements = []

    def execute(self, statement, _values=None):
        self.statements.append(statement)
        self.row = next(
            (row for key, row in self._rows.items() if key in statement),
            None,
        )

    def fetchone(self):
        return self.row


def _freeze(state_id, frozen_id, first_cycle):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from datetime import date
    from uuid import uuid4

    from realtime.advanced_options_reference_repository import (
        PostgresAdvancedOptionsReferenceStateRepository,
    )

    cursor = _FreezeCursor((state_id, "a" * 64), (frozen_id,), (first_cycle,))
    provider = MagicMock()
    provider.transaction.return_value.__enter__.return_value = SimpleNamespace(cursor=cursor)
    repository = PostgresAdvancedOptionsReferenceStateRepository.__new__(
        PostgresAdvancedOptionsReferenceStateRepository
    )
    repository._provider = provider
    state = SimpleNamespace(
        reference_generation=9,
        state_id=state_id,
        state_sha256="a" * 64,
        session_date=date(2026, 11, 5),
    )
    repository.freeze_for_cycle(
        state,
        cycle_id=uuid4(),
        consumed_at=datetime(2026, 11, 5, 15, tzinfo=timezone.utc),
    )
    return cursor


def test_a_resident_cycle_passes_the_opening_freeze_without_binding():
    from uuid import NAMESPACE_URL, uuid4, uuid5

    opening = uuid5(NAMESPACE_URL, "stockscreener:c8-reference-freeze:2026-11-05")
    cursor = _freeze(uuid4(), uuid4(), opening)

    assert not any("cycle_bindings" in row for row in cursor.statements)


def test_any_other_frozen_state_still_refuses():
    from uuid import uuid4

    with pytest.raises(ValueError, match="frozen"):
        _freeze(uuid4(), uuid4(), uuid4())
