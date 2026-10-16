"""known_by in the screening input: consumers accept a bar recorded after the market cutoff but by known_by.

`RealtimeScreeningInput.cutoff` is the market boundary; `known_by` (None means the cutoff) bounds when a bar must
have been recorded. Every recording-clock check of the consumers compares against `inputs.recorded_by`; bar_end and
market times keep comparing against the cutoff. The existing suites are the oracle for the default (None).
"""
from __future__ import annotations

from dataclasses import fields, replace
from datetime import timedelta, timezone

import pytest

import realtime.ticker_projection as producer
from realtime.contracts import canonical_sha256
from realtime.dashboard_projection import produce_realtime_dashboard
from realtime.lineage_head import inputs_sha256
from realtime.screening_delta_loader import ScreeningInputLoader
from realtime.screening_projection import RealtimeScreeningInput, produce_realtime_screening
from realtime.stock_alert_projection import alert_domain_inputs
from realtime.ticker_projection import produce_realtime_ticker_workspace
from tests.realtime.test_dashboard_vertical_slice import dashboard_fixture
from tests.realtime.test_equity_validation_history import OBSERVED_AT
from tests.realtime.test_screening_delta_loader import FakeCursor, FakeDatabase
from tests.realtime.test_screening_vertical_slice import screening_fixture
from tests.realtime.test_stock_alert_vertical_slice import timely_fixture
from tests.realtime.test_ticker_vertical_slice import ticker_fixture

SECOND = timedelta(seconds=1)
KNOWN_BY = OBSERVED_AT + timedelta(minutes=16, seconds=30)  # a window's release_at + completion budget
LATE = OBSERVED_AT + timedelta(minutes=15, seconds=20)  # recorded after the cutoff, within known_by
CLOCKS = ("observed_at", "created_at", "replay_available_at", "provider_published_at")


def with_clock(inputs, index, **changes):
    bars = [dict(row) for row in inputs.bars]
    bars[index].update(changes)
    return replace(inputs, bars=tuple(bars))


# ---- the input ----------------------------------------------------------------------------------------------

def test_known_by_is_not_a_field_so_no_hash_or_identity_can_depend_on_it():
    inputs = screening_fixture()
    assert inputs.known_by is None and inputs.recorded_by == inputs.cutoff
    assert "known_by" not in {field.name for field in fields(inputs)}
    late = replace(inputs, known_by=KNOWN_BY)
    assert late.known_by == KNOWN_BY and late.recorded_by == KNOWN_BY and late.cutoff == inputs.cutoff
    assert late == inputs and "known_by" not in repr(late)
    assert canonical_sha256(late) == canonical_sha256(inputs) and inputs_sha256(late) == inputs_sha256(inputs)
    # replace() carries it through the consumers' `replace(inputs, bars=...)`, and clears it on request.
    assert replace(late, bars=late.bars[:3]).known_by == KNOWN_BY
    assert replace(late, known_by=None).known_by is None


def test_known_by_is_normalised_validated_and_never_before_the_cutoff():
    inputs = screening_fixture()
    assert replace(inputs, known_by=KNOWN_BY.astimezone(timezone(timedelta(hours=-4)))).known_by == KNOWN_BY
    with pytest.raises(ValueError, match="known_by must be timezone-aware"):
        replace(inputs, known_by=KNOWN_BY.replace(tzinfo=None))
    with pytest.raises(ValueError, match="known_by must not precede the cutoff"):
        replace(inputs, known_by=OBSERVED_AT - SECOND)
    assert replace(inputs, known_by=OBSERVED_AT).recorded_by == OBSERVED_AT


# ---- the consumers --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("clock", ["observed_at", "replay_available_at"])
def test_screening_checks_the_recording_clocks_against_recorded_by(clock):
    inputs = screening_fixture()
    late = with_clock(inputs, 0, **{clock: LATE})
    with pytest.raises(ValueError, match="availability drift"):
        produce_realtime_screening(late)
    assert produce_realtime_screening(replace(late, known_by=KNOWN_BY)).rows
    with pytest.raises(ValueError, match="availability drift"):
        produce_realtime_screening(replace(with_clock(inputs, 0, **{clock: KNOWN_BY + SECOND}), known_by=KNOWN_BY))
    # bar_end is a market time: it keeps comparing against the cutoff, whatever known_by is.
    market = with_clock(inputs, 0, bar_end=OBSERVED_AT + SECOND)
    with pytest.raises(ValueError, match="availability drift"):
        produce_realtime_screening(replace(market, known_by=KNOWN_BY))


@pytest.mark.parametrize("clock", CLOCKS)
def test_the_dashboard_checks_the_recording_clocks_against_recorded_by(clock):
    inputs = dashboard_fixture()
    late = with_clock(inputs, 0, **{clock: LATE})
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        produce_realtime_dashboard(late)
    assert produce_realtime_dashboard(replace(late, known_by=KNOWN_BY)).generated_at == inputs.cutoff
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        produce_realtime_dashboard(replace(with_clock(inputs, 0, **{clock: KNOWN_BY + SECOND}), known_by=KNOWN_BY))
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        produce_realtime_dashboard(replace(with_clock(inputs, 0, bar_end=OBSERVED_AT + SECOND), known_by=KNOWN_BY))


def restricted(inputs, tickers):
    members = tuple(row for row in inputs.members if row["ticker"] in tickers)
    ids = {row["security_id"] for row in members}
    return replace(inputs, members=members, bars=tuple(row for row in inputs.bars if row["ticker"] in tickers),
                   actions=tuple(row for row in inputs.actions if row["security_id"] in ids),
                   coverage=tuple(row for row in inputs.coverage if row["security_id"] in ids))


@pytest.mark.parametrize("clock", CLOCKS)
def test_the_alert_domain_checks_the_recording_clocks_against_recorded_by(clock):
    inputs = restricted(timely_fixture(), ("AAPL", "MSFT"))
    index = next(
        i for i, row in enumerate(inputs.bars)
        if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
    )
    late = with_clock(inputs, index, **{clock: LATE})
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        alert_domain_inputs(late)
    bars, _ = alert_domain_inputs(replace(late, known_by=KNOWN_BY))
    assert len(bars) == len(alert_domain_inputs(inputs)[0])
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        alert_domain_inputs(replace(with_clock(inputs, index, **{clock: KNOWN_BY + SECOND}), known_by=KNOWN_BY))
    with pytest.raises(ValueError, match="beyond cutoff|availability drift"):
        alert_domain_inputs(replace(with_clock(inputs, index, bar_end=OBSERVED_AT + SECOND), known_by=KNOWN_BY))


def one_ticker(ticker="AAPL"):
    return restricted(ticker_fixture(), (ticker,))


@pytest.mark.parametrize("clock", CLOCKS)
def test_the_ticker_workspace_checks_the_recording_clocks_against_recorded_by(clock):
    inputs = one_ticker()
    index = next(i for i, row in enumerate(inputs.bars) if row["interval"] == "1wk")
    late = with_clock(inputs, index, **{clock: LATE})
    with pytest.raises(ValueError, match="availability drift"):
        produce_realtime_ticker_workspace(late)
    snapshots = produce_realtime_ticker_workspace(replace(late, known_by=KNOWN_BY))
    assert snapshots and all(snapshot.generated_at == inputs.cutoff for snapshot in snapshots)
    with pytest.raises(ValueError, match="availability drift"):
        produce_realtime_ticker_workspace(
            replace(with_clock(inputs, index, **{clock: KNOWN_BY + SECOND}), known_by=KNOWN_BY))
    with pytest.raises(ValueError, match="availability drift|drift"):
        produce_realtime_ticker_workspace(
            replace(with_clock(inputs, index, bar_end=OBSERVED_AT + SECOND), known_by=KNOWN_BY))


def test_the_recovery_history_uses_known_by_for_recorded_bars_and_the_cutoff_for_market_ends(monkeypatch):
    inputs = ticker_fixture()
    index = next(i for i, row in enumerate(inputs.bars) if row["interval"] == "1wk")
    late = with_clock(inputs, index, observed_at=LATE)
    with pytest.raises(ValueError, match="known-by"):
        producer.project_realtime_ticker_evidence_history(late)
    monkeypatch.setattr(producer, "materialize_realtime_equity_evidence", lambda **scope: scope)
    assert producer.project_realtime_ticker_evidence_history(replace(late, known_by=KNOWN_BY))
    with pytest.raises(ValueError, match="known-by"):
        producer.project_realtime_ticker_evidence_history(
            replace(with_clock(inputs, index, observed_at=KNOWN_BY + SECOND), known_by=KNOWN_BY))
    with pytest.raises(ValueError, match="known-by"):
        producer.project_realtime_ticker_evidence_history(
            replace(with_clock(inputs, index, bar_end=OBSERVED_AT + SECOND), known_by=KNOWN_BY))


# ---- the loader feeding the projection ----------------------------------------------------------------------------

class FixtureCursor(FakeCursor):
    def _actions(self, sql, params):
        return [dict(row) for row in self.db.fixture.actions]

    def _coverage(self, sql, params):
        return [dict(row) for row in self.db.fixture.coverage]


class FixtureDatabase(FakeDatabase):
    """The ticker fixture's members, actions, coverage and bars behind the fake cursor (the visibility rules)."""

    def __init__(self, inputs, *, late_session=False):
        super().__init__(tickers=tuple(member["ticker"] for member in inputs.members), tier=inputs.provider_tier.value)
        self.fixture, self.cohort_id, self.generation = inputs, inputs.cohort_id, inputs.source_generation
        self.manifest_sha256 = inputs.source_manifest_sha256
        for bar in inputs.bars:
            row = dict(
                bar, bar_revision_id=str(bar["bar_revision_id"]),
                source_revision_ids=[str(source) for source in bar["source_revision_ids"]],
                created_at=bar["observed_at"], journal_generation=None, stream_session_id=None, source_offset=None,
                _committed=True,
            )
            if late_session and bar["session_date"] == inputs.session:
                # The current session's bars are fetched and recorded at the window's release, after the cutoff.
                row.update(observed_at=LATE, created_at=LATE + SECOND,
                           replay_available_at=LATE if bar["replay_available_at"] is not None else None,
                           provider_published_at=LATE if bar["provider_published_at"] is not None else None)
            self.rows.append(row)

    @property
    def members(self):
        return [dict(member) for member in self.fixture.members]

    def cursor(self):
        return FixtureCursor(self)


def two_tickers():
    return restricted(ticker_fixture(), ("AAPL", "MSFT"))


PROVENANCE_KEYS = ("evidence", "generation", "observed_at", "analysis_run_id")


def market_content(value):
    """A payload without its recording provenance: evidence ids and hashes, the screening generation, analysis
    run ids and observed_at stamps are content-addressed over the bars' recording clocks (a bar recorded later
    is a different piece of evidence); everything that is market content must not move."""
    if isinstance(value, dict):
        return {key: market_content(item) for key, item in value.items()
                if not any(name in key for name in PROVENANCE_KEYS)}
    if isinstance(value, (list, tuple)):
        return [market_content(item) for item in value]
    return value


def snapshot_identity(snapshots):
    """Per snapshot: its market content and the hash of its source manifest (lineage heads, never a clock)."""
    return {(snapshot.snapshot_type, snapshot.snapshot_key): (canonical_sha256(market_content(snapshot.payload)),
                                                              canonical_sha256(snapshot.source_manifest))
            for snapshot in snapshots}


def test_inputs_loaded_with_known_by_project_without_drift_and_equal_to_the_single_clock_projection():
    base = two_tickers()
    control = FixtureDatabase(base)
    single = ScreeningInputLoader().load(control.cursor(), **control.scope(base.cutoff))
    assert single.known_by is None and len(single.bars) == len(base.bars)
    expected = snapshot_identity(produce_realtime_ticker_workspace(single))

    late = FixtureDatabase(base, late_session=True)
    # With the single clock the current session's bars (recorded after the cutoff) are not visible at all.
    stale = ScreeningInputLoader().load(late.cursor(), **late.scope(base.cutoff))
    assert len(stale.bars) < len(base.bars)
    loader = ScreeningInputLoader()
    cursor = late.cursor()
    loaded = loader.load(cursor, **late.scope(base.cutoff, known_by=KNOWN_BY))
    assert loaded.known_by == KNOWN_BY and loaded.cutoff == base.cutoff and len(loaded.bars) == len(base.bars)
    assert loader.last_report.mode == "full" and "winners_state" in cursor.kinds
    assert any(row["observed_at"] > base.cutoff for row in loaded.bars)
    snapshots = produce_realtime_ticker_workspace(loaded)  # no availability drift
    assert snapshot_identity(snapshots) == expected  # same market content, same source manifest hashes
    # The next load of the same window takes the delta path and is the same input.
    again = loader.load(late.cursor(), **late.scope(base.cutoff, known_by=KNOWN_BY))
    assert loader.last_report.mode == "delta" and loader.last_report.bar_rows_read == 0
    assert again == loaded and again.known_by == KNOWN_BY
    assert snapshot_identity(produce_realtime_ticker_workspace(again)) == expected
