from __future__ import annotations

import re
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2 import errors

from option_flow_support import (
    CALL,
    CANCEL,
    JOURNAL_GENERATION,
    MULTI_LEG,
    OTHER,
    PUT,
    UNKNOWN_CODE,
    advanced_contract_scope,
    advanced_policy,
    check_cluster_rules,
    check_event_rules,
    config,
    policy,
    run,
    semantics,
    T0,
    quote,
    trade,
)
from realtime.migrations import V2_MIGRATIONS_DIR
from realtime.option_flow.engine import OptionFlowEngine
from realtime.option_flow.labels import PriorOpenInterest
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.volume import BaselineKind, VolumeConfig
from realtime.option_flow.writer import (
    CHECKPOINT_COLUMNS,
    CLUSTER_MEMBER_COLUMNS,
    CLUSTER_COLUMNS,
    EVENT_PROFILE_COLUMNS,
    EVENT_COLUMNS,
    OptionFlowReplayDivergence,
    OptionFlowSchemaViolation,
    OptionFlowWriteError,
    OptionFlowWriter,
    checkpoint_values,
    cluster_member_values,
    cluster_values,
    copy_text,
    event_values,
    event_profile_values,
    write_condition_reference,
)
from realtime.option_source_flow import build_option_source_flow_identity
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

MIGRATION = Path(V2_MIGRATIONS_DIR) / "008_realtime_options_flow.sql"


class FakeCursor:
    def __init__(self, connection):
        self._connection = connection
        self.rowcount = 0
        self._rows = []

    def execute(self, statement, parameters=None):
        self._connection.log.append(("execute", statement))
        failure = self._connection.fail_on
        if failure is not None and failure[0] in statement:
            raise failure[1]
        if statement.startswith("INSERT INTO"):
            stale = self._connection.stale_checkpoints if "option_volume_checkpoints" in statement else 0
            self.rowcount = self._connection.copied - stale
        elif "IS DISTINCT FROM" in statement:
            self._rows = [row for table, rows in self._connection.divergent.items() if table in statement
                          for row in rows]

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def copy_expert(self, statement, file):
        payload = file.read()
        self._connection.log.append(("copy", statement, payload))
        self._connection.copied = payload.count("\n")

    def close(self):
        pass


class FakeConnection:
    autocommit = False

    def __init__(self, fail_on=None, divergent=None, stale_checkpoints=0):
        self.log = []
        self.fail_on = fail_on
        self.divergent = divergent or {}
        self.stale_checkpoints = stale_checkpoints
        self.copied = 0
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1
        self.log.append(("commit",))

    def rollback(self):
        self.rollbacks += 1
        self.log.append(("rollback",))


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def _output(count: int, *, offset: int = 10, shard: int = 0) -> EngineOutput:
    template = run(_scenario_engine(), [quote(seq=1, time_ms=T0 - 100), trade(seq=1, price=5.0, size=100)]).events[0]
    out = EngineOutput()
    for index in range(count):
        out.events.append(replace(template, event_id=uuid4(), source_seq=1000 + index))
    out.offsets[shard] = offset
    return out


def _scenario_engine(prior=None, **overrides) -> OptionFlowEngine:
    values = dict(volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE))
    values.update(overrides)
    return OptionFlowEngine(config(**values), prior)


def test_copy_text_uses_the_postgres_text_format():
    stamp = datetime(2026, 10, 12, 14, 30, 0, 123000, tzinfo=timezone.utc)
    row = (None, True, False, 7, Decimal("1E+2"), Decimal("5.50"), (9001, 9003), (), date(2026, 10, 12), stamp,
           UUID(int=5), "a\tb\nc\\d\re")
    line = copy_text([row])
    assert line == ("\\N\tt\tf\t7\t100\t5.50\t{9001,9003}\t{}\t2026-10-12\t2026-10-12T14:30:00.123000+00:00\t"
                    f"{UUID(int=5)}\ta\\tb\\nc\\\\d\\re\n")


def test_copy_text_writes_text_arrays_with_quoting_and_both_escape_layers():
    assert copy_text([(("VOLUME_EXCEEDS_PRIOR_OI", "RATIO_AT_OR_ABOVE_THRESHOLD"),)]) == (
        '{"VOLUME_EXCEEDS_PRIOR_OI","RATIO_AT_OR_ABOVE_THRESHOLD"}\n')
    # Array layer: backslash and quote are escaped; COPY layer: backslash is escaped again.
    assert copy_text([(('a"b', "c\\d", "x,y"),)]) == '{"a\\\\"b","c\\\\\\\\d","x,y"}\n'
    assert copy_text([((),)]) == "{}\n"


def test_writer_columns_match_the_schema_text():
    sql = MIGRATION.read_text(encoding="utf-8")
    forward = "\n".join(
        (
            Path(V2_MIGRATIONS_DIR)
            / name
        ).read_text(encoding="utf-8")
        for name in (
            "025_realtime_advanced_options_control_plane.sql",
            "026_advanced_options_flow_evidence.sql",
            "027_advanced_options_provider_references.sql",
        )
    )

    def columns(table):
        block = sql.split(f"CREATE TABLE {table} (")[1].split("\n)")[0]
        found = {}
        for line in block.splitlines():
            match = re.match(r"^    ([a-z_0-9]+) (?!KEY|\()\S", line)
            if match and not line.strip().startswith(("PRIMARY", "UNIQUE", "CHECK")):
                found[match.group(1)] = "NOT NULL" in line and "DEFAULT" not in line
        for alteration in forward.split(f"ALTER TABLE {table}")[1:]:
            statement = alteration.split(";", 1)[0]
            for name in re.findall(r"ADD COLUMN ([a-z_0-9]+) ", statement):
                found[name] = False
        return found

    for table, written in (("rt.option_flow_events", EVENT_COLUMNS), ("rt.option_flow_clusters", CLUSTER_COLUMNS),
                           ("rt.option_volume_checkpoints", CHECKPOINT_COLUMNS)):
        schema = columns(table)
        assert {"cohort_id", "session_date"} <= set(schema) and len(schema) >= 9, table
        assert set(written) <= set(schema), table
        assert {name for name, required in schema.items() if required} <= set(written), table


def test_row_tuples_line_up_with_their_column_lists():
    prior = {CALL: PriorOpenInterest(100, date(2026, 10, 9))}
    out = run(_scenario_engine(prior, policy=policy(sweep_window_seconds=5), cluster_windows_seconds=(5,)),
              [quote(seq=1, time_ms=T0 - 100)] + [trade(seq=index, time_ms=T0 + index * 50, price=5.0, size=100,
                                                      exchange=300 + index) for index in range(1, 5)])
    assert out.events and out.clusters and out.checkpoints
    assert {len(event_values(item)) for item in out.events} == {len(EVENT_COLUMNS)}
    assert {len(cluster_values(item)) for item in out.clusters} == {len(CLUSTER_COLUMNS)}
    assert {len(checkpoint_values(item)) for item in out.checkpoints} == {len(CHECKPOINT_COLUMNS)}


def test_advanced_rows_normalize_profiles_and_cluster_member_identity():
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    base = config(
        shard_count=1,
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=JOURNAL_GENERATION,
        stream_session_ids=(UUID(int=77),),
        condition_semantics_sha256=base.semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=flow_policy.sha256,
        contract_scope_sha256=scope.sha256,
    )
    engine = OptionFlowEngine(replace(base, source_flow=source_flow))
    messages = [quote(seq=1, time_ms=T0 - 100)]
    messages += [
        trade(
            seq=index + 1,
            time_ms=T0 + index * 80,
            price=2.0,
            size=50,
            exchange=302 + index % 3,
        )
        for index in range(50)
    ]
    out = run(engine, messages)
    profiles = [
        values
        for event in out.events
        for values in event_profile_values(event)
    ]
    members = [
        values
        for cluster in out.clusters
        for values in cluster_member_values(cluster)
    ]

    assert out.events and out.clusters and profiles and members
    assert {len(row) for row in profiles} == {len(EVENT_PROFILE_COLUMNS)}
    assert {len(row) for row in members} == {len(CLUSTER_MEMBER_COLUMNS)}
    assert {row[3] for row in profiles} == {
        "ACCUMULATION",
        "SWEEP",
    }
    assert {row[3] for row in members} == {
        "ACCUMULATION",
        "SWEEP",
    }
    event_keys = {
        (event.event_id, event.event_time, profile.profile)
        for event in out.events
        for profile in event.retention_thresholds
    }
    assert {
        (row[4], row[5], row[3]) for row in members
    } <= event_keys

    connection = FakeConnection()
    writer = OptionFlowWriter(connection)
    writer.add(out)
    writer.flush()
    statements = [
        entry[1]
        for entry in connection.log
        if entry[0] == "execute"
    ]
    assert any(
        statement.startswith(
            "INSERT INTO rt.option_flow_event_profiles"
        )
        for statement in statements
    )
    assert any(
        statement.startswith(
            "INSERT INTO rt.option_flow_cluster_members"
        )
        for statement in statements
    )


def test_writer_flushes_at_1000_rows_with_copy_into_a_staging_table_then_on_conflict():
    connection, clock = FakeConnection(), Clock()
    writer = OptionFlowWriter(connection, clock=clock)
    assert writer.add(_output(999)) is None and connection.log == []
    report = writer.add(_output(1, offset=11))
    assert report is not None and report.reason == "ROWS" and report.events_inserted == 1000
    statements = [entry[1] for entry in connection.log if entry[0] == "execute"]
    assert statements[0].startswith("CREATE TEMP TABLE _stage_option_flow_events (LIKE rt.option_flow_events")
    assert "ON COMMIT DROP" in statements[0]
    assert statements[1].startswith("INSERT INTO rt.option_flow_events") and "ON CONFLICT DO NOTHING" in statements[1]
    copy_entry = next(entry for entry in connection.log if entry[0] == "copy")
    assert copy_entry[1].startswith("COPY _stage_option_flow_events (event_id, event_time") and copy_entry[1].endswith("FROM STDIN")
    assert copy_entry[2].count("\n") == 1000
    assert connection.log[-1] == ("commit",) and connection.commits == 1
    assert writer.pending_rows == 0 and writer.committed_offsets == {(None, 0): 11}


def test_writer_flushes_after_one_second_even_with_few_rows():
    connection, clock = FakeConnection(), Clock()
    writer = OptionFlowWriter(connection, clock=clock)
    writer.add(_output(3))
    clock.now += 0.9
    assert writer.poll() is None and connection.commits == 0
    clock.now += 0.2
    report = writer.poll()
    assert report is not None and report.reason == "INTERVAL" and report.events_inserted == 3
    assert writer.poll() is None


def test_checkpoints_are_deduplicated_to_the_latest_and_upserted():
    connection = FakeConnection()
    writer = OptionFlowWriter(connection, flush_rows=10)
    first = run(_scenario_engine(), [trade(seq=1, price=1.0, size=10)]).checkpoints
    second = run(_scenario_engine(), [trade(seq=1, price=1.0, size=10), trade(seq=2, price=1.0, size=10)]).checkpoints
    for batch in (first, second):
        out = EngineOutput()
        out.checkpoints.extend(batch)
        writer.add(out)
    assert writer.pending_rows == 1
    report = writer.flush()
    assert report.checkpoints_written == 1
    insert = next(entry[1] for entry in connection.log if entry[0] == "execute" and entry[1].startswith("INSERT")
                  and "option_volume_checkpoints AS t (" in entry[1])
    assert "ON CONFLICT (cohort_id, session_date, contract_ticker) DO UPDATE SET" in insert
    assert insert.endswith("WHERE EXCLUDED.last_source_seq >= t.last_source_seq")
    copy = next(entry for entry in connection.log if entry[0] == "copy")
    assert copy[2].split("\t")[3] == "20"


def test_a_checkpoint_older_than_the_stored_one_is_ignored_and_counted():
    connection = FakeConnection(stale_checkpoints=1)
    writer = OptionFlowWriter(connection)
    out = EngineOutput()
    out.checkpoints.extend(run(_scenario_engine(), [trade(seq=1, price=1.0, size=10)]).checkpoints)
    writer.add(out)
    report = writer.flush()
    assert (report.checkpoints_written, report.checkpoints_stale_ignored) == (0, 1)


def test_schema_violation_raises_a_typed_error_keeps_the_rows_and_rolls_back():
    failure = errors.CheckViolation("new row violates check constraint")
    connection = FakeConnection(fail_on=("INSERT INTO rt.option_flow_events", failure))
    writer = OptionFlowWriter(connection)
    writer.add(_output(2))
    with pytest.raises(OptionFlowSchemaViolation) as raised:
        writer.flush()
    assert raised.value.table == "rt.option_flow_events" and isinstance(raised.value.__cause__, psycopg2.Error)
    assert connection.rollbacks == 1 and connection.commits == 0
    assert writer.pending_rows == 2 and writer.committed_offsets == {}


def test_other_database_errors_are_typed_but_not_schema_violations_and_programming_errors_are():
    connection = FakeConnection(fail_on=("INSERT INTO rt.option_flow_events", errors.OperationalError("connection lost")))
    writer = OptionFlowWriter(connection)
    writer.add(_output(1))
    with pytest.raises(OptionFlowWriteError) as raised:
        writer.flush()
    assert not isinstance(raised.value, OptionFlowSchemaViolation)
    connection = FakeConnection(fail_on=("INSERT INTO rt.option_flow_events", errors.UndefinedTable("no partition")))
    writer = OptionFlowWriter(connection)
    writer.add(_output(1))
    with pytest.raises(OptionFlowSchemaViolation):
        writer.flush()


def test_non_database_failure_rolls_back_and_propagates():
    connection = FakeConnection(fail_on=("INSERT INTO rt.option_flow_events", RuntimeError("boom")))
    writer = OptionFlowWriter(connection)
    writer.add(_output(1))
    with pytest.raises(RuntimeError):
        writer.flush()
    assert connection.rollbacks == 1 and writer.pending_rows == 1


def test_in_transaction_hook_runs_before_the_commit_with_the_offsets():
    connection, seen = FakeConnection(), []

    def hook(cursor, report):
        seen.append((len(connection.log), dict(report.committed_offsets), connection.commits))

    writer = OptionFlowWriter(connection, in_transaction=hook)
    writer.add(_output(1, offset=77), journal_generation=JOURNAL_GENERATION)
    report = writer.flush()
    assert seen == [(seen[0][0], {(JOURNAL_GENERATION, 0): 77}, 0)]
    assert report.committed_offsets == {(JOURNAL_GENERATION, 0): 77} and connection.commits == 1


def test_offsets_alone_are_committed_when_no_row_passed_the_filter():
    connection = FakeConnection()
    writer = OptionFlowWriter(connection)
    out = EngineOutput()
    out.offsets[0] = 500
    writer.add(out)
    report = writer.close()
    assert report is not None and report.committed_offsets == {(None, 0): 500} and connection.commits == 1
    assert not any(entry[0] == "copy" for entry in connection.log)


def test_autocommit_connection_is_refused():
    connection = FakeConnection()
    connection.autocommit = True
    with pytest.raises(ValueError):
        OptionFlowWriter(connection)


def test_a_retracted_cluster_is_written_as_a_zero_count_revision():
    options = dict(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1),
                   cluster_windows_seconds=(5,), shard_count=1)
    out = run(_scenario_engine(**options), [quote(seq=1, time_ms=T0 - 100), trade(seq=1, price=5.0, size=100),
                                           trade(seq=2, price=5.0, size=100, conditions=(CANCEL,))])
    retracted = [row for row in out.clusters if row.status == "RETRACTED"]
    assert retracted
    for row in retracted:
        values = dict(zip(CLUSTER_COLUMNS, cluster_values(row)))
        assert (values["status"], values["print_count"], values["total_size"], values["total_premium"],
                values["distinct_exchanges"]) == ("RETRACTED", 0, 0, 0, 0)
    connection = FakeConnection()
    writer = OptionFlowWriter(connection)
    writer.add(out)
    assert writer.flush().clusters_inserted == len(out.clusters)
    copy = next(entry for entry in connection.log if entry[0] == "copy" and "option_flow_clusters" in entry[1])
    assert copy[2].count("RETRACTED") == len(retracted)


def test_cancel_revision_row_carries_its_revision_and_supersedes_link():
    out = run(_scenario_engine(), [trade(seq=1, price=5.0, size=100),
                                   trade(seq=1, price=5.0, size=100, conditions=(CANCEL,))])
    original, revision = out.events
    original_values = dict(zip(EVENT_COLUMNS, event_values(original)))
    revision_values = dict(zip(EVENT_COLUMNS, event_values(revision)))
    assert (original_values["revision"], original_values["supersedes_event_id"]) == (1, None)
    assert revision_values["revision"] == 2 and revision_values["supersedes_event_id"] == original.event_id
    assert (revision_values["source_seq"], revision_values["event_time"]) == (
        original_values["source_seq"], original_values["event_time"])
    assert revision_values["cumulative_volume_before"] is None and revision_values["cumulative_volume_after"] is None


def test_an_identical_replay_passes_the_divergence_check_without_error():
    connection = FakeConnection()
    writer = OptionFlowWriter(connection)
    writer.add(_output(2))
    report = writer.flush()
    compare = [entry[1] for entry in connection.log if entry[0] == "execute" and "IS DISTINCT FROM" in entry[1]]
    assert len(compare) == 3 and report.events_inserted == 2
    assert all("created_at" not in statement for statement in compare)
    assert any("t.event_id = s.event_id AND t.event_time = s.event_time" in statement for statement in compare)
    assert any("t.lineage_version = 1" in statement for statement in compare)
    assert any("t.source_flow_id = s.source_flow_id" in statement for statement in compare)


def test_a_divergent_replay_raises_with_identities_only_and_rolls_back():
    stamp = datetime(2026, 10, 12, 14, 30, tzinfo=timezone.utc)
    connection = FakeConnection(
        divergent={"rt.option_flow_events": [(None, CALL, 1000, stamp, 1)]}
    )
    writer = OptionFlowWriter(connection)
    writer.add(_output(2))
    with pytest.raises(OptionFlowReplayDivergence) as raised:
        writer.flush()
    error = raised.value
    assert isinstance(error, OptionFlowWriteError) and error.table == "rt.option_flow_events"
    assert error.identities == ((None, CALL, 1000, stamp, 1),)
    text = str(error)
    assert CALL in text and "1000" in text and "5.0" not in text and "AT_OR_ABOVE_ASK" not in text
    assert connection.rollbacks == 1 and connection.commits == 0 and writer.pending_rows == 2


def test_a_divergent_cluster_replay_is_reported_by_cluster_id():
    options = dict(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1),
                   cluster_windows_seconds=(5,), shard_count=1)
    out = run(_scenario_engine(**options), [quote(seq=1, time_ms=T0 - 100), trade(seq=1, price=5.0, size=100)])
    row = out.clusters[0]
    connection = FakeConnection(divergent={"rt.option_flow_clusters": [(row.cluster_id, row.revision, row.session_date)]})
    writer = OptionFlowWriter(connection)
    writer.add(out)
    pending = writer.pending_rows
    with pytest.raises(OptionFlowReplayDivergence) as raised:
        writer.flush()
    assert raised.value.table == "rt.option_flow_clusters" and str(row.cluster_id) in str(raised.value)
    assert connection.rollbacks == 1 and writer.pending_rows == pending


# ---------------------------------------------------------------------------------------------
# DB-backed: needs STOCKS_V2_DB_TESTS=1 and pytest run WITHOUT the offline I/O guard plugin.
# ---------------------------------------------------------------------------------------------


IWM = "O:IWM261218C00200000"
DIA = "O:DIA261218C00400000"


def _live_scenario(base_ms: int):
    """A stream that exercises every persisted state, timed around the real clock so the weekly
    partitions created by the baseline cover it."""
    messages = [quote(seq=1, time_ms=base_ms - 100, bid=4.9, ask=5.0),
                quote(symbol=PUT, seq=1, time_ms=base_ms - 100, bid=2.0, ask=2.1),
                quote(symbol=OTHER, seq=1, time_ms=base_ms - 100)]
    messages += [trade(seq=seq, time_ms=base_ms + seq, price=1.0, size=10) for seq in range(1, 4)]
    messages += [trade(seq=10 + index, time_ms=base_ms + 100 + index * 100, price=5.0, size=100, exchange=302 + index)
                 for index in range(3)]
    messages.append(trade(seq=20, time_ms=base_ms + 600, price=5.0, size=150, exchange=305))
    messages.append(trade(seq=21, time_ms=base_ms + 700, price=5.0, size=150, exchange=305, conditions=(CANCEL,)))
    messages.append(trade(seq=22, time_ms=base_ms + 800, price=5.0, size=100, conditions=(MULTI_LEG,)))
    messages.append(trade(seq=23, time_ms=base_ms + 900, price=5.0, size=100, conditions=(UNKNOWN_CODE,)))
    messages.append(trade(seq=24, time_ms=base_ms + 950, price=4.95, size=102, exchange=306))
    messages += [trade(symbol=PUT, seq=1, time_ms=base_ms + 1000, price=2.1, size=300, exchange=310)]
    messages += [trade(symbol=OTHER, seq=1 + index, time_ms=base_ms + 1100 + index * 50, price=5.0, size=100,
                       exchange=311 + index) for index in range(3)]
    # A cancel that echoes the original's sequence and time: it must become revision 2 of that print.
    messages.append(trade(symbol=IWM, seq=30, time_ms=base_ms + 1300, price=5.0, size=100, exchange=330))
    messages.append(trade(symbol=IWM, seq=30, time_ms=base_ms + 1300, price=5.0, size=100, exchange=330,
                          conditions=(CANCEL,)))
    # Three venues build a cluster, then every print is canceled one by one: revisions 1-3, then RETRACTED.
    burst = [(40 + index, base_ms + 1400 + index * 50, 320 + index) for index in range(3)]
    messages += [trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange)
                 for seq, time_ms, exchange in burst]
    messages += [trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange,
                       conditions=(CANCEL,)) for seq, time_ms, exchange in burst]
    return messages


def _live_output(cohort: UUID, base_ms: int) -> EngineOutput:
    prior = {CALL: PriorOpenInterest(300, (datetime.fromtimestamp(base_ms / 1000, timezone.utc) - timedelta(days=3)).date()),
             PUT: PriorOpenInterest(100, (datetime.fromtimestamp(base_ms / 1000, timezone.utc) - timedelta(days=3)).date())}
    engine = OptionFlowEngine(
        config(cohort_id=cohort, shard_count=2, policy=policy(sweep_window_seconds=5), cluster_windows_seconds=(5,),
               quote_universe=frozenset({CALL, PUT}),
               volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE)), prior)
    out = EngineOutput()
    for offset, message in enumerate(_live_scenario(base_ms), start=1):
        out.extend(engine.feed(message, offset))
    engine.submit_reconcile(OTHER, 400, 0, base_ms + 1500)
    out.extend(engine.process())
    engine.submit_time(base_ms + 60_000)
    out.extend(engine.process())
    out.extend(engine.flush(final=True))
    return out


def _count(cursor, table: str, cohort: UUID) -> int:
    cursor.execute(f"SELECT count(*) FROM {table} WHERE cohort_id = %s", (str(cohort),))
    return cursor.fetchone()[0]


@requires_v2_database
def test_writer_against_the_disposable_baseline_database():
    base_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    with disposable_v2_database("test_realtime_v2_d1") as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cohort = insert_cohort(cursor, datetime.now(timezone.utc))
            connection.commit()

            pinned = semantics()
            assert write_condition_reference(connection, pinned) == (13, 12)
            assert write_condition_reference(connection, pinned) == (0, 0)

            out = _live_output(cohort, base_ms)
            for event in out.events:
                check_event_rules(event)
            for row in out.clusters:
                check_cluster_rules(row)
            kinds = {event.trade_treatment for event in out.events}
            assert {"INCLUDED", "CANCELED", "UNKNOWN"} <= kinds
            assert {"PROVISIONAL", "FINAL", "RETRACTED"} == {row.status for row in out.clusters}
            assert any(event.volume_exceeds_prior_oi for event in out.events)
            assert any(event.opening_inference == "LIKELY_OPENING" for event in out.events)
            assert {"SESSION_COMPLETE", "SNAPSHOT_RECONCILED"} <= {row.baseline_kind for row in out.checkpoints}
            assert any(event.side_class == "INSIDE_SPREAD" and event.spread_position == Decimal("0.5000")
                       for event in out.events)
            assert any(event.side_class == "AGGRESSION_UNAVAILABLE_NO_QUOTE" for event in out.events)
            assert "AGGRESSION_UNAVAILABLE_NO_QUOTE" in {row.side_class for row in out.clusters}

            hooked = []

            def hook(cursor, report):
                cursor.execute("SELECT count(*) FROM rt.option_flow_events WHERE cohort_id = %s", (str(cohort),))
                hooked.append(cursor.fetchone()[0])

            writer = OptionFlowWriter(connection, in_transaction=hook)
            writer.add(out, journal_generation=JOURNAL_GENERATION)
            report = writer.flush()
            assert report.events_inserted == len(out.events) and report.events_skipped == 0
            assert report.clusters_inserted == len(out.clusters) and report.clusters_skipped == 0
            distinct_checkpoints = {(row.session_date, row.contract_ticker) for row in out.checkpoints}
            assert report.checkpoints_written == len(distinct_checkpoints)
            assert hooked == [len(out.events)]
            assert writer.committed_offsets == {(JOURNAL_GENERATION, shard): offset for shard, offset in out.offsets.items()}

            with connection.cursor() as cursor:
                assert _count(cursor, "rt.option_flow_events", cohort) == len(out.events)
                assert _count(cursor, "rt.option_flow_clusters", cohort) == len(out.clusters)
                assert _count(cursor, "rt.option_volume_checkpoints", cohort) == len(distinct_checkpoints)
                cancel_rows = sum(1 for event in out.events if event.trade_treatment == "CANCELED")
                assert cancel_rows >= 5
                cursor.execute(
                    "SELECT count(*) FROM rt.option_flow_events c JOIN rt.option_flow_events o "
                    "ON c.supersedes_event_id = o.event_id WHERE c.cohort_id = %s AND c.trade_treatment = 'CANCELED' "
                    "AND c.revision = o.revision + 1", (str(cohort),))
                assert cursor.fetchone()[0] == cancel_rows
                # The cancel that echoes the original's sequence and time is revision 2 of the same print.
                cursor.execute(
                    "SELECT revision, trade_treatment, supersedes_event_id IS NOT NULL, event_time, source_seq "
                    "FROM rt.option_flow_events WHERE cohort_id = %s AND contract_ticker = %s AND source_seq = 30 "
                    "ORDER BY revision", (str(cohort), IWM))
                echoed = cursor.fetchall()
                assert [row[:3] for row in echoed] == [(1, "INCLUDED", False), (2, "CANCELED", True)]
                assert echoed[0][3] == echoed[1][3] and echoed[0][4] == echoed[1][4] == 30
                # A cluster whose prints were all canceled ends in a RETRACTED revision with zero counts.
                cursor.execute(
                    "SELECT cluster_id, revision, status, print_count, total_size, total_premium, distinct_exchanges "
                    "FROM rt.option_flow_clusters WHERE cohort_id = %s AND contract_ticker = %s "
                    "ORDER BY cluster_id, revision", (str(cohort), DIA))
                history = {}
                for cluster_id, revision, status, count, size, premium, venues in cursor.fetchall():
                    history.setdefault(cluster_id, []).append((revision, status, count, size, premium, venues))
                full = [rows for rows in history.values() if len(rows) == 4]
                assert full, "a window holding all three prints must have a four-revision history"
                for rows in full:
                    assert [row[:3] for row in rows] == [
                        (1, "PROVISIONAL", 3), (2, "PROVISIONAL", 2), (3, "PROVISIONAL", 1), (4, "RETRACTED", 0)]
                    assert rows[3][3:] == (0, 0, 0)
                    assert rows[0][5] == 3 and rows[2][5] == 1
                    assert all(row[1] != "FINAL" for row in rows)
                cursor.execute(
                    "SELECT side_class, side_confidence, quote_time <= event_time FROM rt.option_flow_events "
                    "WHERE cohort_id = %s AND contract_ticker = %s AND source_seq = 10", (str(cohort), CALL))
                assert cursor.fetchone() == ("AT_OR_ABOVE_ASK", "HIGH", True)
                cursor.execute(
                    "SELECT side_class, spread_position, opening_inference, opening_score, opening_reasons "
                    "FROM rt.option_flow_events WHERE cohort_id = %s AND contract_ticker = %s AND source_seq = 24",
                    (str(cohort), CALL))
                inside = cursor.fetchone()
                assert inside[:2] == ("INSIDE_SPREAD", Decimal("0.50000"))
                cursor.execute(
                    "SELECT side_class, side_confidence, spread_position, opening_inference, opening_score, "
                    "opening_reasons FROM rt.option_flow_events WHERE cohort_id = %s AND contract_ticker = %s "
                    "AND source_seq = 1", (str(cohort), OTHER))
                unlisted = cursor.fetchone()
                assert unlisted == ("AGGRESSION_UNAVAILABLE_NO_QUOTE", "NONE", None, "NOT_ASSESSED", None, [])
                cursor.execute(
                    "SELECT opening_score, opening_reasons FROM rt.option_flow_events WHERE cohort_id = %s "
                    "AND opening_inference = 'LIKELY_OPENING'", (str(cohort),))
                likely = cursor.fetchall()
                assert likely and all(0 <= score <= 1 and reasons[0] == "VOLUME_EXCEEDS_PRIOR_OI"
                                      and reasons[-1] == "HEURISTIC_UNCALIBRATED_V1" for score, reasons in likely)
                cursor.execute(
                    "SELECT count(*) FROM rt.option_flow_events WHERE cohort_id = %s AND opening_inference <> 'NOT_ASSESSED' "
                    "AND (opening_score IS NULL OR NOT ('HEURISTIC_UNCALIBRATED_V1' = ANY(opening_reasons)))",
                    (str(cohort),))
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT count(*) FROM rt.option_flow_clusters WHERE cohort_id = %s "
                    "AND side_class = 'AGGRESSION_UNAVAILABLE_NO_QUOTE'", (str(cohort),))
                assert cursor.fetchone()[0] > 0
                cursor.execute(
                    "SELECT volume_exceeds_prior_oi, opening_inference, cumulative_volume_after, prior_open_interest "
                    "FROM rt.option_flow_events WHERE cohort_id = %s AND contract_ticker = %s "
                    "AND volume_exceeds_prior_oi", (str(cohort), CALL))
                flagged = cursor.fetchall()
                assert flagged and all(row[0] and row[2] > row[3] for row in flagged)
                cursor.execute(
                    "SELECT baseline_kind, snapshot_volume, snapshot_watermark_seq, cumulative_volume "
                    "FROM rt.option_volume_checkpoints WHERE cohort_id = %s AND contract_ticker = %s",
                    (str(cohort), OTHER))
                assert cursor.fetchone() == ("SNAPSHOT_RECONCILED", 400, 0, 700)
                cursor.execute(
                    "SELECT revision, status FROM rt.option_flow_clusters WHERE cohort_id = %s "
                    "AND side_class = 'AT_OR_ABOVE_ASK' ORDER BY cluster_id, revision", (str(cohort),))
                assert {row[1] for row in cursor.fetchall()} == {"PROVISIONAL", "FINAL"}
            connection.rollback()

            # A replay of the same input is idempotent: no new events or cluster revisions.
            replay = OptionFlowWriter(connection)
            replay.add(_live_output(cohort, base_ms), journal_generation=JOURNAL_GENERATION)
            again = replay.flush()
            assert again.events_inserted == 0 and again.events_skipped == len(out.events)
            assert again.clusters_inserted == 0 and again.clusters_skipped == len(out.clusters)
            with connection.cursor() as cursor:
                assert _count(cursor, "rt.option_flow_events", cohort) == len(out.events)
                assert _count(cursor, "rt.option_flow_clusters", cohort) == len(out.clusters)
                assert _count(cursor, "rt.option_volume_checkpoints", cohort) == len(distinct_checkpoints)
            connection.rollback()

            # A divergent replay is never silently accepted: the same identity with different values.
            changed = replace(out.events[0], size=out.events[0].size + 1)
            same_key_new_id = replace(out.events[0], event_id=uuid4())
            for variant in (changed, same_key_new_id):
                divergent = EngineOutput()
                divergent.events.append(variant)
                guard = OptionFlowWriter(connection)
                guard.add(divergent)
                with pytest.raises(OptionFlowReplayDivergence) as raised:
                    guard.flush()
                assert raised.value.table == "rt.option_flow_events" and guard.pending_rows == 1
                assert raised.value.identities == ((
                    out.events[0].source_flow_id,
                    out.events[0].contract_ticker,
                    out.events[0].source_seq,
                    out.events[0].event_time,
                    out.events[0].revision,
                ),)
            revised = next(row for row in out.clusters if row.status == "FINAL")
            divergent = EngineOutput()
            divergent.clusters.append(replace(revised, total_size=revised.total_size + 1))
            guard = OptionFlowWriter(connection)
            guard.add(divergent)
            with pytest.raises(OptionFlowReplayDivergence) as raised:
                guard.flush()
            assert raised.value.table == "rt.option_flow_clusters"
            assert raised.value.identities == ((str(revised.cluster_id), revised.revision, revised.session_date),)
            with connection.cursor() as cursor:
                assert _count(cursor, "rt.option_flow_events", cohort) == len(out.events)
                assert _count(cursor, "rt.option_flow_clusters", cohort) == len(out.clusters)
            connection.rollback()

            # A checkpoint older than the stored one is ignored, not written over it.
            newest = [row for row in out.checkpoints if row.contract_ticker == CALL][-1]
            stale = EngineOutput()
            stale.checkpoints.append(replace(newest, last_source_seq=newest.last_source_seq - 1,
                                             cumulative_volume=newest.cumulative_volume + 7))
            ignored = OptionFlowWriter(connection)
            ignored.add(stale)
            stale_report = ignored.flush()
            assert (stale_report.checkpoints_written, stale_report.checkpoints_stale_ignored) == (0, 1)
            with connection.cursor() as cursor:
                cursor.execute("SELECT cumulative_volume, last_source_seq FROM rt.option_volume_checkpoints "
                               "WHERE cohort_id = %s AND contract_ticker = %s", (str(cohort), CALL))
                assert cursor.fetchone() == (newest.cumulative_volume, newest.last_source_seq)
            connection.rollback()

            # 1,000 rows trigger a flush without an explicit call.
            bulk = OptionFlowWriter(connection)
            template = out.events[0]
            heavy = EngineOutput()
            for index in range(1005):
                heavy.events.append(replace(template, event_id=uuid4(), source_seq=50_000 + index, supersedes_event_id=None))
            triggered = bulk.add(heavy)
            assert triggered is not None and triggered.reason == "ROWS" and triggered.events_inserted == 1005

            # A row that breaks a CHECK raises a typed error; the connection stays usable.
            broken = replace(template, event_id=uuid4(), source_seq=70_000,
                             quote_time=template.event_time + timedelta(seconds=1))
            poison = EngineOutput()
            poison.events.append(broken)
            guarded = OptionFlowWriter(connection)
            guarded.add(poison)
            with pytest.raises(OptionFlowSchemaViolation) as violation:
                guarded.flush()
            assert violation.value.sqlstate == "23514" and violation.value.table == "rt.option_flow_events"
            assert guarded.pending_rows == 1
            # A revision above 1 must point at the row it supersedes (and revision 1 must not).
            orphan = EngineOutput()
            orphan.events.append(replace(template, event_id=uuid4(), source_seq=70_001, revision=2,
                                         supersedes_event_id=None))
            unlinked = OptionFlowWriter(connection)
            unlinked.add(orphan)
            with pytest.raises(OptionFlowSchemaViolation) as orphaned:
                unlinked.flush()
            assert orphaned.value.sqlstate == "23514"
            with connection.cursor() as cursor:
                assert _count(cursor, "rt.option_flow_events", cohort) == len(out.events) + 1005
            connection.rollback()
        finally:
            pool.putconn(connection)
