"""V2-9: incremental Ticker Workspace state and the bounded lineage-head manifest."""
from dataclasses import replace
from datetime import timedelta
from functools import lru_cache
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.contracts import canonical_json, canonical_sha256
from realtime.lineage_head import BOUNDED_MANIFEST_BYTES, bar_lineage_head
from realtime.ticker_projection import (
    _DerivationVerifier,
    TickerProjectionState,
    _fact,
    _security,
    project_realtime_ticker_workspace,
)
from realtime.preflight import ProviderTier
from tests.realtime.test_ticker_vertical_slice import ticker_fixture

TICKERS = ("AAPL", "MSFT")


@lru_cache(maxsize=1)
def two_names():
    inputs = ticker_fixture()
    members = tuple(member for member in inputs.members if member["ticker"] in TICKERS)
    identities = {str(member["security_id"]) for member in members}
    return replace(
        inputs, members=members,
        bars=tuple(row for row in inputs.bars if row["ticker"] in TICKERS),
        actions=tuple(row for row in inputs.actions if str(row["security_id"]) in identities),
        coverage=tuple(row for row in inputs.coverage if str(row["security_id"]) in identities),
    )


def _revised(row, **changes):
    return dict(
        row, bar_revision_id=uuid5(NAMESPACE_URL, f"revised:{row['bar_revision_id']}"),
        payload_sha256=canonical_sha256(("revised", row["payload_sha256"])), **changes,
    )


def _revise_monthly(inputs, ticker):
    return replace(inputs, bars=tuple(
        _revised(row, close=row["close"] + 0.01) if row["ticker"] == ticker and row["interval"] == "1mo" else row
        for row in inputs.bars
    ))


def _identity(projection):
    return [(snapshot.snapshot_key, snapshot.snapshot_id, snapshot.source_manifest_sha256,
             snapshot.payload_sha256) for snapshot in projection.snapshots] + [
        canonical_sha256(projection.summary_charts), projection.screening_generation.generation_id]


def _later(inputs):
    return replace(inputs, cutoff=inputs.cutoff + timedelta(minutes=5))


@pytest.fixture(scope="module")
def primed():
    """One cold projection with a long-lived state, shared read-only by the tests below."""
    state = TickerProjectionState()
    projection = project_realtime_ticker_workspace(two_names(), state=state)
    return state, projection, dict(state.stats)


def test_the_manifest_is_a_bounded_lineage_head_not_embedded_bars(primed):
    _, projection, _ = primed
    rows = {ticker: [row for row in two_names().bars if row["ticker"] == ticker] for ticker in TICKERS}
    for snapshot in projection.snapshots:
        manifest = snapshot.source_manifest
        assert len(canonical_json(manifest).encode()) <= BOUNDED_MANIFEST_BYTES
        assert "bars" not in manifest
        assert manifest["bar_lineage"] == bar_lineage_head(rows[snapshot.snapshot_key])
        assert manifest["bar_lineage"]["bars"] == len(rows[snapshot.snapshot_key])
        for name in ("actions", "coverage", "market_events", "market_event_coverage",
                     "fundamental_reports", "fundamental_coverage"):
            assert set(manifest[name]) == {"count", "sha256"}


def test_advanced_rest_fact_preserves_explicit_repair_lineage():
    row = dict(
        next(
            row
            for row in two_names().bars
            if row["source_kind"] == "NATIVE_REST"
        ),
        provider_tier="MASSIVE_ADVANCED",
        quality_codes=("EXPLICIT_ADVANCED_REST_REPAIR_V1",),
    )

    fact = _fact(row, two_names())

    assert fact.revision.quality_codes == (
        "EXPLICIT_ADVANCED_REST_REPAIR_V1",
    )


def test_derivation_verifier_separates_bound_history_from_advanced_bars():
    inputs = two_names()
    rows = [
        row
        for row in inputs.bars
        if row["ticker"] == "AAPL"
        and row["interval"] == "5m"
        and row["adjustment_basis"] == "RAW_UNADJUSTED"
    ]
    sessions = sorted({row["session_date"] for row in rows})[-2:]
    source_by_session = {}
    for session in sessions:
        source_by_session[session] = tuple(
            _fact(
                (
                    dict(
                        row,
                        cohort_id=UUID(int=9),
                        provider_tier=ProviderTier.MASSIVE_ADVANCED.value,
                        quality_codes=(
                            "EXPLICIT_ADVANCED_REST_REPAIR_V1",
                        ),
                    )
                    if session == sessions[-1]
                    else row
                ),
                inputs,
            )
            for row in rows
            if row["session_date"] == session
        )
    member = next(
        row for row in inputs.members if row["ticker"] == "AAPL"
    )
    verifier = _DerivationVerifier(
        TickerProjectionState(),
        _security(member),
        inputs,
        UUID(int=10),
    )

    verifier._prefetch("1d", source_by_session, sessions)

    assert all(verifier._derived[("1d", session)] for session in sessions)
    assert verifier.verify_mixed_whole(
        "1d",
        tuple(
            fact
            for session in sessions
            for fact in source_by_session[session]
        ),
        (),
        {
            fact.revision.source_kind
            for facts in source_by_session.values()
            for fact in facts
        },
    ) == 2


def test_a_long_lived_state_never_changes_the_result(primed):
    state, first, cold_stats = primed
    stateless = project_realtime_ticker_workspace(two_names())
    assert _identity(first) == _identity(stateless)
    assert cold_stats["fact_built"] == len(two_names().bars) and cold_stats.get("fact_reused", 0) == 0

    later = _later(two_names())
    warm = project_realtime_ticker_workspace(later, state=state)
    assert _identity(warm) == _identity(project_realtime_ticker_workspace(later))
    assert state.stats["fact_built"] == 0 and state.stats["fact_reused"] == len(later.bars)
    assert state.stats["materialization_built"] == 0 and state.stats["materialization_reused"] == 24
    assert state.stats["lineage_session_built"] == 0 and state.stats["derived_windows_built"] == 0
    # Same bars, later cutoff: the manifest head is unchanged except for the cutoff-bound input digest.
    for before, after in zip(first.snapshots, warm.snapshots):
        assert before.source_manifest["bar_lineage"] == after.source_manifest["bar_lineage"]
        assert before.source_manifest["input_sha256"] != after.source_manifest["input_sha256"]


def test_a_revised_interval_rebuilds_only_that_tickers_interval(primed):
    state, first, _ = primed
    state_for_run = TickerProjectionState()
    project_realtime_ticker_workspace(two_names(), state=state_for_run)
    revised = _revise_monthly(two_names(), "AAPL")
    projection = project_realtime_ticker_workspace(revised, state=state_for_run)
    stats = state_for_run.stats
    # AAPL's 1mo setup and 1mo pattern; every other interval and every MSFT interval is reused.
    assert stats["materialization_built"] == 2 and stats["materialization_reused"] == 22
    assert stats["fact_built"] == sum(1 for row in revised.bars if row["ticker"] == "AAPL" and row["interval"] == "1mo")
    by_key = {snapshot.snapshot_key: snapshot for snapshot in projection.snapshots}
    original = {snapshot.snapshot_key: snapshot for snapshot in first.snapshots}
    assert by_key["MSFT"].snapshot_id == original["MSFT"].snapshot_id
    assert by_key["MSFT"].source_manifest["input_sha256"] == original["MSFT"].source_manifest["input_sha256"]
    assert by_key["AAPL"].source_manifest["input_sha256"] != original["AAPL"].source_manifest["input_sha256"]
    assert by_key["AAPL"].payload["chart_refs"] == original["AAPL"].payload["chart_refs"]
    assert (by_key["AAPL"].source_manifest["bar_lineage"]["groups"]["1mo:ADJUSTED"]["chain_sha256"]
            != original["AAPL"].source_manifest["bar_lineage"]["groups"]["1mo:ADJUSTED"]["chain_sha256"])
    assert (by_key["AAPL"].source_manifest["bar_lineage"]["groups"]["1d:ADJUSTED"]
            == original["AAPL"].source_manifest["bar_lineage"]["groups"]["1d:ADJUSTED"])
    assert _identity(projection) == _identity(project_realtime_ticker_workspace(revised))


def test_a_changed_derived_bar_rederives_only_its_own_session(primed):
    state = TickerProjectionState()
    project_realtime_ticker_workspace(two_names(), state=state)
    inputs = two_names()
    target = max((row for row in inputs.bars if row["ticker"] == "AAPL" and row["interval"] == "30m"),
                 key=lambda row: row["bar_start"])
    same_values_new_revision = replace(inputs, bars=tuple(
        _revised(row) if row is target else row for row in inputs.bars
    ))
    projection = project_realtime_ticker_workspace(same_values_new_revision, state=state)
    assert state.stats["lineage_session_built"] == 1 and state.stats["derived_windows_built"] == 0
    assert _identity(projection) == _identity(project_realtime_ticker_workspace(same_values_new_revision))


def test_warm_state_still_rejects_a_drifted_derived_aggregate_and_survives_the_failure():
    state = TickerProjectionState()
    inputs = two_names()
    project_realtime_ticker_workspace(inputs, state=state)
    retained = len(state)
    target = min((row for row in inputs.bars if row["ticker"] == "MSFT" and row["interval"] == "30m"),
                 key=lambda row: row["bar_start"])
    drifted = replace(inputs, bars=tuple(
        _revised(row, close=row["close"] + 1, high=row["high"] + 1) if row is target else row
        for row in inputs.bars
    ))
    with pytest.raises(ValueError, match="derived interval lineage/aggregate drift: 30m"):
        project_realtime_ticker_workspace(drifted, state=state)
    assert len(state) == retained
    project_realtime_ticker_workspace(inputs, state=state)
    assert state.stats["fact_built"] == 0 and state.stats["materialization_built"] == 0


def test_state_holds_only_the_latest_cycles_working_set():
    state = TickerProjectionState()
    inputs = two_names()
    project_realtime_ticker_workspace(inputs, state=state)
    steady = len(state)
    project_realtime_ticker_workspace(_later(inputs), state=state)
    assert len(state) == steady
    project_realtime_ticker_workspace(_revise_monthly(inputs, "AAPL"), state=state)
    assert len(state) == steady
