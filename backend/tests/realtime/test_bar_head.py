"""G102 A4 (design section 4): bars keyed by what they are, in `rt.bar_head`.

Unit level: the writer's head upsert, the selection order, the backfill and the head reads. The read boundary keeps
the bar tables inside `equity_bar_repository.py`; modules not yet converted are listed and the list may only shrink.
Semantics on a real database are in `test_bar_head_v2_db.py`.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from unittest.mock import MagicMock
from uuid import UUID

from realtime import equity_bar_repository as repository
from realtime.equity_bar_repository import (
    BAR_SELECTION_POLICY,
    backfill_bar_head_cursor,
    load_bar_groups_cursor,
    load_bar_heads_cursor,
    load_bar_range_cursor,
    persist_realtime_equity_bars_cursor,
)
from tests.realtime.test_equity_bar_repository import _batch_executor, _cursor, _fact, _values_executor

UTC = timezone.utc
START = datetime(2026, 11, 6, 14, 30, tzinfo=UTC)
SECURITY = UUID(int=7)


def test_the_writer_writes_the_head_and_not_the_cohort_table():
    fact = _fact()
    cursor = _cursor(fact)
    persist_realtime_equity_bars_cursor(cursor, (fact,), values_executor=_values_executor, batch_executor=_batch_executor)
    statement, (row,) = cursor.recorded_head_statement, cursor.recorded_head_rows
    assert statement.startswith("INSERT INTO rt.bar_head (instrument_id,interval,bar_start,session_scope,"
                                "adjustment_basis,bar_revision_id,")
    assert "ON CONFLICT (instrument_id,interval,bar_start,session_scope,adjustment_basis) DO UPDATE" in statement
    assert "WHERE rt.bar_head.bar_end=EXCLUDED.bar_end AND" in statement
    revision = fact.revision
    assert row[:6] == (1, revision.interval, revision.bar_start, revision.session_scope.value,
                       "ADJUSTED" if revision.adjusted else "RAW_UNADJUSTED", str(revision.bar_revision_id))
    assert row[-2:] == (BAR_SELECTION_POLICY, str(fact.cohort_id))
    assert row[10] == revision.system_observed_at and row[19] == revision.payload_sha256
    # rt.bar_current is no longer written (the target design runs Monday).
    assert not isinstance(getattr(cursor, "recorded_current_rows", None), tuple)


def test_the_selection_order_is_precedence_then_observation_then_tier():
    """The A4 rehearsal on a copy of live: putting the tier first overrode the precedence table (AAPL weekly bars)."""
    order = repository._selection_order("r")
    precedence = order.index("CASE WHEN r.adjustment_basis")
    observed = order.index("r.observed_at DESC")
    tier = order.index("CASE WHEN r.provider_tier='MASSIVE_ADVANCED'")
    assert precedence < observed < tier < order.index("r.created_at DESC") < order.index("r.bar_revision_id")
    replaces = repository._head_replaces("n", "o")
    assert replaces.index("CASE WHEN n.adjustment_basis") < replaces.index("o.observed_at < n.observed_at") \
        < replaces.index("CASE WHEN n.provider_tier")


def test_the_backfill_chooses_across_cohorts_and_keeps_existing_rows():
    cursor = MagicMock()
    cursor.rowcount = 42
    assert backfill_bar_head_cursor(cursor, instrument_id=9) == 42
    statement, params = cursor.execute.call_args.args
    assert statement.startswith("INSERT INTO rt.bar_head") and statement.endswith("ON CONFLICT DO NOTHING")
    assert "SELECT DISTINCT ON (revision.instrument_id,revision.interval,revision.bar_start," in statement
    assert "FROM rt.bar_current AS cur JOIN rt.bar_revisions AS revision" in statement
    assert repository._selection_order("revision") in statement
    assert params == (BAR_SELECTION_POLICY, 9)


def _row(key_text, start, revision=1):
    return dict(key_text=key_text, instrument_id=3, interval="5m", adjustment_basis="RAW_UNADJUSTED",
                bar_revision_id=UUID(int=revision), bar_start=start, bar_end=start + timedelta(minutes=5),
                session_date=start.date(), observed_at=start + timedelta(minutes=6), source_kind="NATIVE_REST",
                provider_tier="MASSIVE_ADVANCED", open_price=1, high_price=1, low_price=1, close_price=1, volume=1,
                vwap=None, transaction_count=None, payload_sha256="a" * 64)


def test_head_reads_map_rows_to_their_targets_and_check_the_instrument():
    cursor = MagicMock()
    cursor.fetchall.return_value = [_row(f"{SECURITY}|5m:RAW_UNADJUSTED", START)]
    result = load_bar_heads_cursor(cursor, targets=(
        (3, SECURITY, "AAPL", "5m:RAW_UNADJUSTED", START, START + timedelta(hours=1)),
        (3, SECURITY, "AAPL", "1d:ADJUSTED", START, START + timedelta(days=1)),
    ))
    assert [row["bar_start"] for row in result[(str(SECURITY), "5m:RAW_UNADJUSTED")]] == [START]
    assert result[(str(SECURITY), "1d:ADJUSTED")] == []
    statement, params = cursor.execute.call_args.args
    assert "JOIN rt.instruments AS instrument ON instrument.instrument_id=target.instrument_id " \
           "AND instrument.security_id=target.security_id" in statement
    assert "FROM rt.bar_head" not in statement and "JOIN rt.bar_head AS head" in statement
    assert "UNION ALL" not in statement and params["as_of"] is None
    assert "cur.cohort_id" not in statement and "cohort" not in statement


def test_a_rerun_reads_keys_corrected_after_its_cutoff_from_the_revisions():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    as_of = START + timedelta(hours=2)
    load_bar_groups_cursor(cursor, instrument_id=3, security_id=SECURITY,
                           groups={"5m:RAW_UNADJUSTED": (START, START + timedelta(hours=1))}, as_of=as_of)
    statement, params = cursor.execute.call_args.args
    assert params["as_of"] == as_of
    assert "WHERE candidate.observed_at<=%(as_of)s" in statement
    assert "UNION ALL" in statement and "CROSS JOIN LATERAL" in statement
    assert "revision.observed_at<=%(as_of)s ORDER BY " + repository._selection_order("revision") + " LIMIT 1" \
        in statement
    assert "WHERE candidate.observed_at>%(as_of)s" in statement


def test_range_reads_group_by_instrument():
    cursor = MagicMock()
    rows = [_row("3", START), _row("3", START + timedelta(minutes=5), revision=2)]
    cursor.fetchall.return_value = rows
    result = load_bar_range_cursor(cursor, instrument_ids=(3, 4), interval="5m", basis="RAW_UNADJUSTED",
                                   start=START, end=START + timedelta(hours=1))
    assert [row["bar_revision_id"] for row in result[3]] == [UUID(int=1), UUID(int=2)] and result[4] == []
    assert cursor.execute.call_args.args[1]["ids"] == [3, 4]


# ---- the read boundary (design 4.5) ------------------------------------------------------------------------------------

BACKEND = Path(__file__).resolve().parents[2]
BAR_TABLES = re.compile(r"\brt\.bar_(?:current|revisions|catalog|head)\b")
OWNER = "realtime/equity_bar_repository.py"
# Not SQL readers: grants, retention metadata and the statistics step.
METADATA = {"realtime/relation_registry.py", "realtime/security.py", "realtime/equity_live024_compatibility.py",
            "realtime/statistics_maintenance.py"}
# Runtime modules that still query the bar tables directly (Nov 7). Each conversion removes its entry; nothing may
# be added.
NOT_YET_CONVERTED = {
    "realtime/advanced_equity_detector_consumer.py", "realtime/advanced_equity_preparation.py",
    "realtime/advanced_gap_repair.py", "realtime/advanced_journal_replay.py",
    "realtime/advanced_options_equity_input.py", "realtime/advanced_options_native_loader.py",
    "realtime/advanced_options_os1_os2_shadow.py", "realtime/advanced_options_p8_production.py",
    "realtime/advanced_options_projection.py", "realtime/advanced_options_reference_producer.py",
    "realtime/advanced_options_v3_history.py", "realtime/advanced_session_recovery.py",
    "realtime/advanced_stream_gap_repair.py", "realtime/canonical_stock_alert_repository.py",
    "realtime/detector_evidence_repository.py", "realtime/equity_bar_drain.py", "realtime/equity_history.py",
    "realtime/equity_interval_derivation_worker.py", "realtime/equity_interval_finalizer.py",
    "realtime/equity_portal_projection_worker.py", "realtime/equity_recent_repair.py",
    "realtime/equity_v2_backfill_source.py", "realtime/equity_validation_history.py", "realtime/forward_bindings.py",
    "realtime/gap_repair_plan.py", "realtime/lineage_head.py", "realtime/portal_compatibility_repository.py",
    "realtime/screening_delta_loader.py", "realtime/screening_projection.py", "realtime/screening_references.py",
    "realtime/screening_repository.py", "realtime/session_close_coverage.py", "realtime/stage_runners_advanced_equity.py",
    "realtime/stage_runners_equity.py", "realtime/stage_runners_ingest.py", "realtime/stock_alert_repository.py",
    "realtime/ticker_references.py", "realtime/ticker_repository.py",
}


def _modules_naming_bar_tables():
    found = set()
    for path in (BACKEND / "realtime").rglob("*.py"):
        relative = path.relative_to(BACKEND).as_posix()
        if BAR_TABLES.search(path.read_text(encoding="utf-8")):
            found.add(relative)
    return found


def test_only_the_bar_repository_and_not_yet_converted_modules_name_the_bar_tables():
    offenders = _modules_naming_bar_tables() - {OWNER} - METADATA
    assert offenders - NOT_YET_CONVERTED == set(), "a new module queries the bar tables; use equity_bar_repository"


def test_the_not_yet_converted_list_only_shrinks():
    converted = NOT_YET_CONVERTED - _modules_naming_bar_tables()
    assert converted == set(), f"remove converted modules from NOT_YET_CONVERTED: {sorted(converted)}"
