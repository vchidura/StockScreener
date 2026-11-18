"""R96 (Nov 17): the provider re-sent a finalized minute (CVX and INTC 14:49) with corrected values after the 5m bar
14:45-14:50 had been derived from the first revision. The window load lists the winning (corrected) minute and, through
the stored 5m bar's source closure, the superseded one: two final revisions of one bar key. The ticker workspace used to
feed both to the 1m -> 5m re-derivation ("duplicate canonical source window") and skip the ticker every window.

Charts and lineage heads take the winner of each bar key; the stored 5m bar is still re-derived from exactly the
minutes it names (the superseded one), so a corrupted 5m still fails, and a second revision no bar names still fails."""
from dataclasses import replace
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.contracts import canonical_sha256
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
from realtime.lineage_head import bar_group_head
from realtime.ticker_projection import project_realtime_ticker_workspace
from tests.realtime.test_ticker_vertical_slice import advanced_closing_fixture

ONE = "1m:RAW_UNADJUSTED"
FIVE = "5m:RAW_UNADJUSTED"


def _corrected_minute_inputs():
    """The closing fixture with one stream minute corrected after its 5m bar was derived. The winner keeps its place
    among the winners and the superseded revision follows them, as the source closure of `load_full_bars` and of the
    delta loader returns it."""
    inputs = advanced_closing_fixture()
    five = sorted(
        (row for row in inputs.bars
         if row["session_date"] == inputs.session and row["interval"] == "5m" and row["source_kind"] == "DERIVED"),
        key=lambda row: row["bar_start"],
    )
    stored_five = five[len(five) // 2]
    superseded = next(row for row in inputs.bars if row["bar_revision_id"] == stored_five["source_revision_ids"][-1])
    assert superseded["interval"] == "1m" and superseded["source_kind"] == "REALTIME_STREAM"
    corrected = dict(
        superseded,
        bar_revision_id=uuid5(NAMESPACE_URL, f"r96-corrected-minute:{superseded['bar_revision_id']}"),
        payload_sha256=canonical_sha256(("r96-corrected-minute", superseded["payload_sha256"])),
        close=superseded["close"] - 0.05, volume=superseded["volume"] + 8755.0,
        observed_at=superseded["observed_at"] + timedelta(seconds=61),
    )
    winners = tuple(corrected if row is superseded else row for row in inputs.bars)
    return inputs, replace(inputs, bars=(*winners, superseded)), stored_five, superseded, corrected


def _workspace(inputs):
    projection = project_realtime_ticker_workspace(inputs)
    assert not projection.unavailable_members
    (snapshot,) = projection.snapshots
    return snapshot


def test_a_corrected_minute_charts_the_winner_and_keeps_the_5m_verified_against_its_named_minute():
    original, inputs, stored_five, superseded, corrected = _corrected_minute_inputs()
    snapshot = _workspace(inputs)
    baseline = _workspace(original)

    payload, groups = snapshot.payload, snapshot.source_manifest["bar_lineage"]["groups"]
    assert "1m" in payload["chart_refs"] and "chart:1m" not in payload["missing_producers"]
    plan = build_realtime_equity_portal_history_plan(inputs.session, equity_ticker_count=len(inputs.members))
    winners = sorted(
        (row for row in inputs.bars[:-1]
         if row["interval"] == "1m" and row["session_date"] in plan.one_minute_sessions),
        key=lambda row: row["bar_start"],
    )
    assert corrected in winners and superseded not in winners
    # The 1m chart is one bar per minute, the corrected one: what rt.bar_current composes for the reader.
    assert groups[ONE] == bar_group_head(winners)
    assert groups[ONE]["count"] == baseline.source_manifest["bar_lineage"]["groups"][ONE]["count"]
    assert groups[ONE]["chain_sha256"] != baseline.source_manifest["bar_lineage"]["groups"][ONE]["chain_sha256"]
    # The stored 5m bar (derived from the first revision) is unchanged and was verified against it.
    assert groups[FIVE] == baseline.source_manifest["bar_lineage"]["groups"][FIVE]
    assert superseded["bar_revision_id"] in stored_five["source_revision_ids"]


@pytest.mark.parametrize("corruption", ["five_aggregate", "superseded_minute"])
def test_the_5m_bar_naming_a_superseded_minute_is_still_verified_against_it(corruption):
    _original, inputs, stored_five, superseded, _corrected = _corrected_minute_inputs()
    # The valid cycle's screening, so the workspace's own 5m verifier (not the screening's) meets the corruption.
    screening = project_realtime_ticker_workspace(inputs).screening_generation
    if corruption == "five_aggregate":
        rows = tuple(dict(row, volume=row["volume"] + 1) if row is stored_five else row for row in inputs.bars)
    else:  # the minute the 5m bar names no longer adds up to it
        rows = tuple(dict(row, volume=row["volume"] + 1) if row is superseded else row for row in inputs.bars)
    with pytest.raises(ValueError, match="lineage/aggregate drift: 5m"):
        project_realtime_ticker_workspace(replace(inputs, bars=rows), screening_override=screening)


def test_a_second_revision_no_bar_names_still_fails_the_ticker():
    inputs = advanced_closing_fixture()
    minute = next(row for row in inputs.bars if row["session_date"] == inputs.session and row["interval"] == "1m")
    stray = dict(
        minute, bar_revision_id=uuid5(NAMESPACE_URL, f"r96-stray-minute:{minute['bar_revision_id']}"),
        payload_sha256=canonical_sha256(("r96-stray-minute", minute["payload_sha256"])), volume=minute["volume"] + 1,
    )
    with pytest.raises(ValueError, match="duplicate canonical source window: SPY"):
        project_realtime_ticker_workspace(replace(inputs, bars=(*inputs.bars, stray)))
