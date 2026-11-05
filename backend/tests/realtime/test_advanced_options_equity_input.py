from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from realtime.advanced_options_control_plane import (
    build_advanced_options_equity_input_binding,
)
from realtime.advanced_options_equity_input import (
    load_advanced_options_equity_context_cursor,
    initial_advanced_options_equity_context_requirements,
    validate_advanced_options_equity_dependencies_cursor,
)
from options.calendar import OptionExchangeCalendar
from realtime.equity_history import EquityHistoryRead, equity_history_coordinates
from realtime.contracts import UniverseManifest
from tests.realtime.test_advanced_options_control_plane import (
    _enrollment_plan,
)


class _Cursor:
    def __init__(self, *, omit_qqq_context=False, stored_after_end=None, receipt_known_by=None):
        self.query = ""
        self.parameters = ()
        self.omit_qqq_context = omit_qqq_context
        self.stored_after_end = stored_after_end
        self.receipt_known_by = receipt_known_by
        self.connection = self

    @contextmanager
    def cursor(self, **_kwargs):
        yield self

    def execute(self, query, parameters):
        self.query = query
        self.parameters = parameters

    def fetchone(self):
        if "FROM rt.cycle_receipts" in self.query:
            return (
                None
                if self.receipt_known_by is None
                else {"known_by": self.receipt_known_by}
            )
        raise AssertionError(self.query)

    def fetchall(self):
        if "revision.bar_revision_id::text" in self.query:
            if (
                self.omit_qqq_context
                and self.parameters[1]
                == "44444444-4444-4444-8444-444444444444"
            ):
                return []
            cutoff = self.parameters[-3]
            known_by = self.parameters[-2]
            count = self.parameters[-1]
            interval = self.parameters[2]
            step = (
                timedelta(days=1)
                if interval == "1d"
                else timedelta(minutes=5)
            )
            start = 1 if interval == "1d" else 0
            late = self.stored_after_end or timedelta(0)
            rows = [
                {
                    "bar_revision_id": str(uuid4()),
                    "bar_end": cutoff - step * (index + start),
                    "observed_at": cutoff - step * (index + start) + late,
                    "payload_sha256": f"{index:064x}",
                }
                for index in range(count + 1)
            ]
            return [row for row in rows if row["observed_at"] <= known_by][:count]
        raise AssertionError(self.query)


@pytest.fixture(autouse=True)
def history_resolver(monkeypatch):
    def resolve(cursor, **scope):
        calendar = OptionExchangeCalendar("XNYS")
        if cursor.omit_qqq_context and scope["ticker"] == "QQQ":
            return EquityHistoryRead((), None)
        return EquityHistoryRead(tuple(
            {
                "cohort_id": scope["cohort_id"],
                "bar_revision_id": uuid5(
                    NAMESPACE_URL, f"{scope['cohort_id']}:{scope['ticker']}:{session}",
                ),
                "session_date": session,
                "bar_start": equity_history_coordinates(calendar, "1d", session)[0][0],
                "bar_end": equity_history_coordinates(calendar, "1d", session)[0][1],
                "observed_at": scope["checked_at"],
                "availability_mode": "HISTORICAL_RECONSTRUCTED",
                "payload_sha256": f"{index:064x}",
            }
            for index, session in enumerate(scope["sessions"])
        ), None)

    monkeypatch.setattr(
        "realtime.advanced_options_equity_input.load_equity_history_cursor", resolve,
    )
    return resolve


def _dependencies():
    enrollment = _enrollment_plan()
    equity = replace(
        enrollment.equity_plan,
        cohort_id=UUID("11111111-1111-4111-8111-111111111111"),
        cohort_sha256="8" * 64,
        manifest_id=UUID("22222222-2222-4222-8222-222222222222"),
        profile_id=UUID("33333333-3333-4333-8333-333333333333"),
        universe=UniverseManifest(
            "p8-equity-input-test",
            ("QQQ", "SPY"),
            (),
        ),
        security_ids=(
            ("QQQ", UUID("44444444-4444-4444-8444-444444444444")),
            ("SPY", UUID("55555555-5555-4555-8555-555555555555")),
        ),
    )
    options_cutoff = enrollment.control_plane.receipts.options_input_ready.spec.cutoff
    equity_cutoff = options_cutoff - timedelta(minutes=2)
    binding = build_advanced_options_equity_input_binding(
        target_options_window_id=uuid4(),
        target_cohort_id=enrollment.control_plane.cohort_id,
        source_cohort_id=equity.cohort_id,
        source_cohort_sha256=equity.cohort_sha256,
        source_manifest_id=equity.manifest_id,
        source_manifest_sha256=equity.manifest.sha256,
        source_profile_id=equity.profile_id,
        source_profile_sha256=equity.profile_sha256,
        equity_window_id=uuid4(),
        equity_receipt_id=uuid4(),
        equity_cutoff=equity_cutoff,
        reference_generation=1,
        universe_sha256=equity.universe.sha256,
        input_sha256="9" * 64,
        build_id="advanced-equity-input-test-v1",
        maximum_age_seconds=300,
        authorization_reference="p8-equity-input-test",
    )
    return equity, binding


def test_two_underlying_dependencies_are_exact_and_ready():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )
    assert result["status"] == "READY"
    assert result["lane_context"]["O1"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["member_count"] == 6
    assert result["lane_context"]["OS1"]["status"] == "AVAILABLE"
    assert result["lane_context"]["OS1"]["member_count"] == 34
    assert result["lane_context"]["OS2"]["status"] == "AVAILABLE"
    assert result["lane_context"]["OS2"]["member_count"] == 36


def test_bars_stored_after_they_end_satisfy_the_lanes_through_the_equity_receipt():
    # Real bars are always stored after they end: 15 s in the sim, 5 s or more on live.
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(
            stored_after_end=timedelta(seconds=15),
            receipt_known_by=binding.equity_cutoff + timedelta(seconds=16),
        ),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )

    for lane in ("O3", "OS1", "OS2"):
        assert result["lane_context"][lane]["status"] == "AVAILABLE"


def test_bars_the_equity_window_had_not_accepted_still_block():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(
            stored_after_end=timedelta(seconds=15),
            receipt_known_by=binding.equity_cutoff + timedelta(seconds=5),
        ),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )

    assert result["lane_context"]["O3"]["status"] == "BLOCKED"


def test_continuous_dependencies_can_reuse_the_same_equity_history_source():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(), equity_plan=equity, equity_input=binding,
        history_source_cohort_id=equity.cohort_id,
        underlyings=("QQQ", "SPY"), allow_shared_history_source=True,
    )
    assert result["status"] == "READY"
    assert all(
        row["status"] == "AVAILABLE"
        for row in result["lane_context"].values()
    )


def test_missing_qqq_context_blocks_only_declared_lanes_before_provider_work():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(omit_qqq_context=True),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )

    assert result["status"] == "READY"
    assert result["lane_context"]["O1"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["status"] == "BLOCKED"
    assert result["lane_context"]["O3"]["reason_codes"] == (
        "O3_QQQ_E_CONTEXT_INCOMPLETE",
    )
    assert result["lane_context"]["OS1"]["status"] == "BLOCKED"
    assert result["lane_context"]["OS2"]["status"] == "BLOCKED"


def test_native_context_assembly_preserves_mixed_history_sources(monkeypatch, history_resolver):
    equity, binding = _dependencies()
    repaired, retained = uuid4(), uuid4()
    expected = {}

    def mixed(cursor, **scope):
        assert scope["cohort_id"] == repaired
        assert scope["checked_at"] == binding.equity_cutoff
        history = history_resolver(cursor, **scope)
        rows = tuple(
            dict(row, cohort_id=retained if index < 12 else repaired)
            for index, row in enumerate(history.rows)
        )
        expected[scope["ticker"]] = rows
        return EquityHistoryRead(rows, "a" * 64)

    monkeypatch.setattr(
        "realtime.advanced_options_equity_input.load_equity_history_cursor", mixed,
    )
    contexts = load_advanced_options_equity_context_cursor(
        _Cursor(), equity_plan=equity, history_source_cohort_id=repaired,
        equity_cutoff=binding.equity_cutoff,
        requirements=initial_advanced_options_equity_context_requirements(),
        underlyings=("QQQ", "SPY"),
    )
    for context in contexts:
        assert context.available
        if context.lane.value in {"OS1", "OS2"}:
            historical = tuple(
                member for member in context.members
                if member.source_cohort_id != equity.cohort_id
            )
            assert {
                (member.source_cohort_id, member.bar_revision_id, member.payload_sha256)
                for member in historical
            } == {
                (row["cohort_id"], row["bar_revision_id"], row["payload_sha256"])
                for rows in expected.values() for row in rows
            }
    readiness = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(), equity_plan=equity, equity_input=binding,
        history_source_cohort_id=repaired, underlyings=("QQQ", "SPY"),
    )
    assert all(value["status"] == "AVAILABLE" for value in readiness["lane_context"].values())


@pytest.mark.parametrize("invalid", ["missing_session", "availability", "coordinates"])
def test_native_history_context_rejects_invalid_daily_coverage(
    monkeypatch, history_resolver, invalid,
):
    equity, binding = _dependencies()

    def read(cursor, **scope):
        history = history_resolver(cursor, **scope)
        rows = list(history.rows)
        if invalid == "missing_session":
            rows.pop()
        elif invalid == "availability":
            rows[-1] = dict(rows[-1], availability_mode="UNKNOWN")
        else:
            rows[-1] = dict(rows[-1], bar_end=rows[-1]["bar_end"] - timedelta(minutes=1))
        return EquityHistoryRead(tuple(rows), history.binding_sha256)

    monkeypatch.setattr(
        "realtime.advanced_options_equity_input.load_equity_history_cursor", read,
    )
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(), equity_plan=equity, equity_input=binding,
        history_source_cohort_id=uuid4(), underlyings=("SPY",),
    )
    assert result["lane_context"]["O3"]["status"] == "AVAILABLE"
    assert result["lane_context"]["OS1"]["status"] == "BLOCKED"
    assert result["lane_context"]["OS2"]["status"] == "BLOCKED"


def test_undeclared_stock_first_context_is_blocked_without_attempting_atr_assembly():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(), equity_plan=equity, equity_input=binding,
        history_source_cohort_id=uuid4(), underlyings=("SPY",),
        requirements=(initial_advanced_options_equity_context_requirements()[0],),
    )
    assert result["lane_context"]["O3"]["status"] == "AVAILABLE"
    for lane in ("OS1", "OS2"):
        assert result["lane_context"][lane]["status"] == "BLOCKED"
        assert result["lane_context"][lane]["reason_codes"] == (
            f"{lane}_E_CONTEXT_REQUIREMENTS_INCOMPLETE",
        )
