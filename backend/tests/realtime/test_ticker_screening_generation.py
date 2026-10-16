"""The screening generation the Ticker Workspace names is persisted by the compute stage (offline, fake store).

The workspaces embed `context.cohort.generation`, the digest of the screening the projection built from its
own bar subset. The reader's default current mode, the dashboard and the screening readers resolve the cohort
through rt.screening_current -> rt.screening_generations, which no other V2 stage writes.
"""
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import exchange_calendars
import pytest

from equity.screening_hourly import hourly_slots
from realtime.contracts import canonical_sha256
from realtime.database_access import RealtimeDatabaseRole
from realtime.portal_compatibility_repository import RealtimeScreeningRepository
from realtime.portal_projection_repository import persist_screening_generation_cursor
from realtime.screening_projection import produce_realtime_screening
import realtime.ticker_repository as ticker_repository
from realtime.ticker_repository import compute_realtime_ticker_cycle
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_screening_vertical_slice import ScreeningStore, current_bars_of
from tests.realtime.test_ticker_vertical_slice import ticker_fixture, ticker_projection


def generation_of(snapshot):
    return snapshot.payload["context"]["cohort"]["generation"]


def run_compute(monkeypatch, projection, store):
    compute = object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    inputs = ticker_fixture()

    @contextmanager
    def transaction(database, **options):
        yield store

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(ticker_repository, "load_realtime_screening_input", lambda cursor, **scope: inputs)
    monkeypatch.setattr(ticker_repository, "_load_ticker_calendar_inputs", lambda cursor, members, cutoff: ((), ()))
    monkeypatch.setattr(ticker_repository, "_load_ticker_fundamental_inputs", lambda cursor, members, cutoff: ((), ()))
    monkeypatch.setattr(ticker_repository, "project_realtime_ticker_workspace", lambda inputs, **context: projection)
    monkeypatch.setattr(ticker_repository, "produce_realtime_ticker_workspace_summary",
                        lambda inputs, snapshots, charts: MagicMock())
    monkeypatch.setattr(ticker_repository, "produce_realtime_pattern_watch_publication",
                        lambda inputs, snapshots: MagicMock())
    monkeypatch.setattr(ticker_repository, "_verify_ticker_evidence_parity", lambda projection: None)
    monkeypatch.setattr(ticker_repository, "persist_realtime_equity_materialization_batch_cursor",
                        lambda cursor, writes: None)
    monkeypatch.setattr(
        ticker_repository, "persist_screening_generation_cursor",
        lambda cursor, generation: persist_screening_generation_cursor(cursor, generation, values_executor=store.values))
    return lambda: compute_realtime_ticker_cycle(compute, cohort_id=inputs.cohort_id)


def test_the_projection_exposes_the_complete_generation_its_workspaces_name():
    projection, inputs = ticker_projection(), ticker_fixture()
    generation = projection.screening_generation
    assert (generation.status, generation.cohort_id, generation.provider_tier, generation.session_date) == (
        "COMPLETE", inputs.cohort_id, inputs.provider_tier, inputs.session)
    assert generation.generated_at == inputs.cutoff
    assert generation.expected_members == len(generation.rows) == len(inputs.members) == 9
    assert sorted(row.ticker for row in generation.rows) == sorted(member["ticker"] for member in inputs.members)
    assert {generation_of(snapshot) for snapshot in projection.snapshots} == {generation.manifest["generation"]}
    # What is hashed is exactly what is stored, and the generation never enters it beyond its digest.
    assert all(canonical_sha256(snapshot.payload) == snapshot.payload_sha256 for snapshot in projection.snapshots)


def test_compute_persists_the_generation_once_and_a_replay_changes_nothing(monkeypatch):
    projection, inputs, store = ticker_projection(), ticker_fixture(), ScreeningStore()
    cycle = run_compute(monkeypatch, projection, store)()
    generation = projection.screening_generation
    assert list(store.generations) == [str(generation.generation_id)] and store.current == str(generation.generation_id)
    assert len(store.rows[str(generation.generation_id)]) == 9
    # The cycle no longer carries the (large) generation, and every workspace is exactly what was projected.
    assert cycle.projection.screening_generation is None
    assert cycle.projection.snapshots == projection.snapshots
    run_compute(monkeypatch, projection, store)()
    assert list(store.generations) == [str(generation.generation_id)] and store.current == str(generation.generation_id)
    # The generation the compute stage stored passes the screening reader's identity, hash and (against the
    # fixture's bars, as `rt.bar_current` would hold them) bar-lineage checks.
    stored = RealtimeScreeningRepository(store.cursor, bar_loader=current_bars_of(inputs))._load(
        store, str(inputs.cohort_id), inputs.cutoff)
    assert stored["generation"] == generation.manifest["generation"] == generation_of(projection.snapshots[0])
    assert [row["ticker"] for row in stored["rows"]] == sorted(row.ticker for row in generation.rows)


def test_a_projection_without_a_generation_persists_no_screening(monkeypatch):
    projection, store = replace(ticker_projection(), screening_generation=None), ScreeningStore()
    cycle = run_compute(monkeypatch, projection, store)()
    assert store.generations == {} and store.current is None
    assert cycle.projection.snapshots == projection.snapshots


@pytest.mark.parametrize("defect", ["cohort", "status", "later", "digest", "rows", "members"])
def test_compute_refuses_a_generation_that_is_not_the_one_the_workspaces_name(monkeypatch, defect):
    generation = ticker_projection().screening_generation
    tampered = {
        "cohort": dict(cohort_id=uuid4()), "status": dict(status="DEGRADED"),
        "later": dict(generated_at=generation.generated_at + timedelta(seconds=1)),
        "digest": dict(manifest=dict(generation.manifest, generation="0" * 64)),
        "rows": dict(rows=generation.rows[:-1]), "members": dict(expected_members=generation.expected_members + 1),
    }[defect]
    projection = replace(ticker_projection(), screening_generation=replace(generation, **tampered))
    store = ScreeningStore()
    with pytest.raises(ValueError, match="does not match the cycle's workspaces"):
        run_compute(monkeypatch, projection, store)()
    assert store.generations == {} and store.current is None


def test_the_app_role_holds_every_privilege_the_persist_statements_use(monkeypatch):
    from realtime.relation_registry import grants

    table_grants = grants()
    for relation in ("rt.screening_generations", "rt.screening_rows", "rt.screening_current"):
        assert {"SELECT", "INSERT"} <= {name for name, roles in table_grants[relation].items() if "app" in roles}
    assert "app" in table_grants["rt.screening_current"]["UPDATE"]
    store = ScreeningStore()
    run_compute(monkeypatch, ticker_projection(), store)()
    assert store.statements and all(
        statement.startswith(("INSERT INTO rt.screening_", "SELECT ")) for statement in store.statements)


def daily_only_bars(inputs, *, ancestors):
    """The bars a daily-only screening input holds (`screening_history_window`: 283 adjusted sessions, the last
    20 raw daily sessions, the final session's raw hourly bars), with or without their causal ancestors."""
    calendar = exchange_calendars.get_calendar("XNYS")
    session = inputs.session
    start = calendar.session_offset(str(session), -282).date()
    nominal = calendar.session_offset(str(session), -19).date()
    slots = hourly_slots(calendar, calendar.session_close(str(session)).to_pydatetime())
    chosen = {
        str(row["bar_revision_id"]): row for row in inputs.bars
        if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED" and start <= row["session_date"] <= session
        or row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED" and nominal <= row["session_date"] <= session
        or row["interval"] == "1h" and row["adjustment_basis"] == "RAW_UNADJUSTED"
        and row["bar_start"] >= slots[0][0] and row["bar_end"] <= slots[-1][1]
    }
    if ancestors:
        indexed = {str(row["bar_revision_id"]): row for row in inputs.bars}
        pending = list(chosen)
        while pending:
            for source in indexed[pending.pop()]["source_revision_ids"]:
                if str(source) not in chosen:
                    chosen[str(source)] = indexed[str(source)]
                    pending.append(str(source))
    return replace(inputs, bars=tuple(chosen.values()))


def test_the_workspace_screening_rows_equal_a_daily_only_screening_over_the_same_sessions():
    """The projection screens a reduced bar subset (and its causal ancestors). Its ROWS are those of a
    daily-only input over the same sessions; only the lineage (hence the manifest digest and the generation id)
    differs. With the derived raw daily/hourly bars of the V2 bar set a daily-only input cannot be screened at
    all: its causal sources are not loaded."""
    inputs, generation = ticker_fixture(), ticker_projection().screening_generation
    with pytest.raises(ValueError, match="Derived screening causal source is missing"):
        produce_realtime_screening(daily_only_bars(inputs, ancestors=False))
    daily = produce_realtime_screening(daily_only_bars(inputs, ancestors=True))
    assert [(row.security_id, row.payload_sha256) for row in daily.rows] == [
        (row.security_id, row.payload_sha256) for row in generation.rows]
    assert daily.manifest["generation"] != generation.manifest["generation"]
