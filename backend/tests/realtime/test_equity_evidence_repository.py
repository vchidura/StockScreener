from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from realtime.equity_evidence_repository import (
    EvidenceRef,
    RealtimeEquityMaterializationWrite,
    RealtimeFeatureDefinition,
    _prepare_realtime_equity_materialization,
    persist_realtime_equity_materialization,
    persist_realtime_equity_materialization_batch_cursor,
    persist_realtime_equity_materialization_cursor,
    resolve_current_evidence_ref_cursor,
    resolve_evidence_ref_as_of_cursor,
    resolve_release_evidence_refs_cursor,
    load_release_evidence_bodies_cursor,
    verify_realtime_equity_materializations_persisted_cursor,
)
from realtime.equity_materialization_adapter import materialize_realtime_equity_evidence
from tests.realtime.test_equity_materialization_adapter import _facts, _security


DURABLE = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
LATER = DURABLE + timedelta(days=40)


def _definition(version="equity_features_v1"):
    return RealtimeFeatureDefinition(
        bundle_sha256="b" * 64,
        bundle_version=version,
        feature_schema_version="1.0",
        dependency_spec={"bars": {"interval": "1m", "final": True}},
        policy_sha256="c" * 64,
        code_sha256="d" * 64,
    )


def _result(offset=0):
    facts = _facts()
    return materialize_realtime_equity_evidence(
        analysis_run_id=uuid4(),
        security=_security(),
        bars=facts,
        observed_at=facts[-1].revision.bar_end.astimezone(timezone.utc) + timedelta(seconds=offset),
    )


def _setup_result():
    template = _facts()[0]
    start = template.revision.bar_start - timedelta(minutes=30 * 59)
    facts = tuple(
        replace(
            template,
            revision=replace(
                template.revision,
                bar_revision_id=uuid4(),
                interval="30m",
                session_date=(start + timedelta(minutes=30 * index)).date(),
                bar_start=start + timedelta(minutes=30 * index),
                bar_end=start + timedelta(minutes=30 * (index + 1)),
                system_observed_at=start + timedelta(minutes=30 * (index + 1)),
                payload_sha256=f"{index + 1:064x}",
            ),
        )
        for index in range(60)
    )
    return materialize_realtime_equity_evidence(
        analysis_run_id=uuid4(),
        security=_security(),
        bars=facts,
        observed_at=facts[-1].revision.system_observed_at,
    )


class EvidenceStore:
    """In-memory model of the V2 evidence catalogs and partitioned bodies.

    Catalog inserts keep the first durable_at (ON CONFLICT DO NOTHING); bodies are stored under the
    durable_at the writer supplies, so a writer that ignored the catalog would create a second body.
    """

    def __init__(self, durable=DURABLE):
        self.durable = durable
        self.definitions = {}
        self.window_catalog = {}
        self.window_bodies = {}
        self.evidence_catalog = {}
        self.evidence_keys = set()
        self.evidence_bodies = {}
        self.dependencies = {}
        self.snapshots = {}
        self.statements = []
        self.page_sizes = set()
        self.current_rows = []
        self.feature_current_rows = []
        self.last = None
        self.cursor = MagicMock()
        self.cursor.execute.side_effect = self._execute
        self.cursor.fetchall.side_effect = self._fetchall

    def _execute(self, statement, parameters=None):
        self.statements.append(statement)
        self.last = (statement, parameters)

    def batch_executor(self, cursor, statement, rows, *, page_size):
        self.page_sizes.add(page_size)
        self.statements.append(statement)
        self.current_rows.extend(rows)

    def values_executor(self, cursor, statement, rows, *, page_size):
        self.page_sizes.add(page_size)
        self.statements.append(statement)
        rows = tuple(rows)
        if "INSERT INTO rt.feature_definitions" in statement:
            for row in rows:
                self.definitions.setdefault(row[0], (row[0], row[1], row[2], row[4], row[5]))
        elif "INSERT INTO rt.evidence_source_window_catalog" in statement:
            for cohort, window_id, sha, interval, market_time in rows:
                self.window_catalog.setdefault(window_id, (sha, interval, market_time, self.durable))
        elif "INSERT INTO rt.equity_evidence_source_windows" in statement:
            assert all(len(row) == 12 and row[-1] == self.window_catalog[row[1]][3] for row in rows)
            for row in rows:
                self.window_bodies.setdefault((row[1], row[-1]), row[:-1])
        elif "INSERT INTO rt.evidence_catalog" in statement:
            for cohort, evidence_id, key, *_ in rows:
                if evidence_id not in self.evidence_catalog and key not in self.evidence_keys:
                    self.evidence_catalog[evidence_id] = (key, self.durable)
                    self.evidence_keys.add(key)
        elif "INSERT INTO rt.equity_evidence_dependencies" in statement:
            for cohort, evidence_id, order, dependency_id, durable in rows:
                self.dependencies.setdefault((evidence_id, order, durable), dependency_id)
        elif "INSERT INTO rt.equity_evidence (" in statement:
            assert statement.count("durable_at") == 1
            for row in rows:
                self.evidence_bodies.setdefault(
                    (row[2], row[-1]), (row[2], row[3], row[29], row[31], row[32]),
                )
        elif "INSERT INTO rt.feature_snapshots" in statement:
            for row in rows:
                self.snapshots.setdefault(
                    row[0], ((row[0], row[1], row[2], row[3], row[4], row[5], row[10]), row[-1]),
                )
        elif "INSERT INTO rt.feature_current" in statement:
            self.feature_current_rows.extend(rows)
        elif "INSERT INTO rt.equity_evidence_current" in statement:
            self.current_rows.extend(rows)
        else:
            raise AssertionError(statement)

    def _fetchall(self):
        statement, parameters = self.last
        if "FROM rt.feature_definitions" in statement:
            return tuple(self.definitions[key] for key in parameters[0] if key in self.definitions)
        if "FROM rt.evidence_source_window_catalog AS catalog JOIN" in statement:
            return tuple(
                self.window_bodies[(window_id, self.window_catalog[window_id][3])]
                for window_id in parameters[1]
                if window_id in self.window_catalog
                and (window_id, self.window_catalog[window_id][3]) in self.window_bodies
            )
        if statement.startswith("SELECT source_window_id, source_window_sha256"):
            return tuple(
                (window_id, *self.window_catalog[window_id])
                for window_id in parameters[1] if window_id in self.window_catalog
            )
        if "FROM rt.equity_evidence_source_windows WHERE" in statement:
            return tuple(
                body for (window_id, durable), body in self.window_bodies.items()
                if window_id in parameters[1] and durable in parameters[2]
            )
        if "FROM rt.evidence_catalog AS catalog JOIN rt.equity_evidence AS" in statement:
            return tuple(
                self.evidence_bodies[(evidence_id, self.evidence_catalog[evidence_id][1])]
                for evidence_id in parameters[1]
                if evidence_id in self.evidence_catalog
                and (evidence_id, self.evidence_catalog[evidence_id][1]) in self.evidence_bodies
            )
        if statement.startswith("SELECT evidence_id, evidence_key, durable_at"):
            return tuple(
                (evidence_id, *self.evidence_catalog[evidence_id])
                for evidence_id in parameters[1] if evidence_id in self.evidence_catalog
            )
        if "FROM rt.equity_evidence WHERE" in statement:
            return tuple(
                body for (evidence_id, durable), body in self.evidence_bodies.items()
                if evidence_id in parameters[1] and durable in parameters[2]
            )
        if "JOIN rt.equity_evidence_dependencies AS dep" in statement:
            return tuple(
                (evidence_id, order, dependency_id)
                for (evidence_id, order, durable), dependency_id in self.dependencies.items()
                if evidence_id in self.evidence_catalog
                and self.evidence_catalog[evidence_id][1] == durable
                and evidence_id in parameters[1]
            )
        if "FROM rt.equity_evidence_dependencies WHERE" in statement:
            return tuple(
                (evidence_id, order, dependency_id)
                for (evidence_id, order, durable), dependency_id in self.dependencies.items()
                if evidence_id in parameters[1] and durable in parameters[2]
            )
        if "FROM rt.feature_snapshots" in statement:
            return tuple(
                self.snapshots[key][0] for key in parameters[0] if key in self.snapshots
            )
        raise AssertionError(statement)

    def connection(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = self.cursor
        return connection

    def persist(self, result, definition, *, generation=7):
        connection = self.connection()
        snapshot_id = persist_realtime_equity_materialization(
            connection, result, definition, generation=generation,
            values_executor=self.values_executor, batch_executor=self.batch_executor,
        )
        return snapshot_id, connection

    def persist_batch(self, writes):
        return persist_realtime_equity_materialization_batch_cursor(
            self.cursor, writes, values_executor=self.values_executor,
        )

    def index(self, fragment):
        return next(i for i, statement in enumerate(self.statements) if fragment in statement)


def test_single_write_inserts_catalogs_first_and_bodies_under_the_catalog_durable_at():
    result, definition, store = _result(), _definition(), EvidenceStore()

    snapshot_id, connection = store.persist(result, definition)

    prepared = _prepare_realtime_equity_materialization(result, definition, generation=7)
    assert snapshot_id == prepared.snapshot_id
    assert store.index("INSERT INTO rt.evidence_catalog") < store.index("INSERT INTO rt.equity_evidence (")
    assert store.index("INSERT INTO rt.evidence_source_window_catalog") < store.index(
        "INSERT INTO rt.equity_evidence_source_windows"
    )
    assert store.index("INSERT INTO rt.equity_evidence (") < store.index("INSERT INTO rt.feature_snapshots")
    assert len(store.evidence_bodies) == len(result.materialization.evidence)
    assert {durable for _, durable in store.evidence_bodies} == {DURABLE}
    assert {durable for _, durable in store.window_bodies} == {DURABLE}
    assert {durable for _, durable in store.snapshots.values()} == {DURABLE}
    assert store.page_sizes == {100}
    assert not any("INSERT INTO rt.equity_evidence_inputs" in statement for statement in store.statements)
    assert not any("created_at" in statement for statement in store.statements)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_feature_snapshot_and_discovery_pointers_are_written_after_identity_checks():
    result, definition, store = _result(), _definition(), EvidenceStore()
    feature = next(row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT")

    store.persist(result, definition)

    assert {row[4] for row in store.current_rows} == {result.interval}
    assert {row[5] for row in store.current_rows} == {"FEATURE_SNAPSHOT"}
    assert all(row[6] == "equity_features_v1" for row in store.current_rows)
    assert any(row[8] == str(feature.evidence_id) for row in store.current_rows)
    statements = " ".join(call.args[0] for call in store.cursor.execute.call_args_list)
    assert "rt.feature_current.generation < EXCLUDED.generation" in statements
    pointer = next(s for s in store.statements if "INSERT INTO rt.equity_evidence_current" in s)
    assert "rt.equity_evidence_current.market_time < EXCLUDED.market_time" in pointer


def test_replay_after_the_next_month_started_never_adds_a_second_body():
    result, definition, store = _result(), _definition(), EvidenceStore()
    first_snapshot, _ = store.persist(result, definition)
    bodies = dict(store.evidence_bodies)
    windows = dict(store.window_bodies)
    snapshots = dict(store.snapshots)

    store.durable = LATER
    second_snapshot, _ = store.persist(result, definition)

    assert second_snapshot == first_snapshot
    assert store.evidence_bodies == bodies
    assert store.window_bodies == windows
    assert store.snapshots == snapshots
    assert {durable for _, durable in store.evidence_bodies} == {DURABLE}


def test_identity_conflict_rolls_back_before_current_updates():
    result, definition, store = _result(), _definition(), EvidenceStore()
    feature = next(row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT")
    store.evidence_catalog[str(feature.evidence_id)] = ("another-key", DURABLE)
    store.evidence_keys.add("another-key")
    connection = store.connection()
    current_executor = MagicMock()

    with pytest.raises(ValueError, match="evidence identity conflict"):
        persist_realtime_equity_materialization(
            connection, result, definition, generation=7,
            values_executor=store.values_executor, batch_executor=current_executor,
        )

    current_executor.assert_not_called()
    assert not any("INSERT INTO rt.equity_evidence (" in statement for statement in store.statements)
    assert not any("INSERT INTO rt.feature_snapshots" in statement for statement in store.statements)
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_evidence_key_reuse_under_another_id_is_an_identity_conflict():
    result, definition, store = _result(), _definition(), EvidenceStore()
    feature = next(row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT")
    store.evidence_catalog[str(uuid4())] = (feature.evidence_key, DURABLE)
    store.evidence_keys.add(feature.evidence_key)

    with pytest.raises(ValueError, match="evidence identity conflict"):
        store.persist(result, definition)


def test_setup_evidence_persists_ordered_dependencies_in_the_parents_partition():
    result, definition, store = _setup_result(), _definition(), EvidenceStore()

    store.persist(result, definition)

    setup = next(row for row in result.materialization.evidence if row.evidence_type.value == "TRADE_SETUP")
    stored = sorted(
        (order, dependency) for (evidence_id, order, durable), dependency in store.dependencies.items()
        if evidence_id == str(setup.evidence_id)
    )
    assert [dependency for _, dependency in stored] == [str(value) for value in setup.source_revision_ids]
    assert {durable for (_, _, durable) in store.dependencies} == {DURABLE}
    dependency_statement = next(s for s in store.statements if "INSERT INTO rt.equity_evidence_dependencies" in s)
    assert "durable_at" in dependency_statement


def test_setup_dependency_conflict_rolls_back_before_current_updates():
    result, definition, store = _setup_result(), _definition(), EvidenceStore()
    store.persist(result, definition)
    key = next(iter(store.dependencies))
    store.dependencies[key] = str(uuid4())
    connection = store.connection()
    current_executor = MagicMock()

    with pytest.raises(ValueError, match="dependency conflict"):
        persist_realtime_equity_materialization(
            connection, result, definition, generation=7,
            values_executor=store.values_executor, batch_executor=current_executor,
        )

    current_executor.assert_not_called()
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_snapshot_conflict_for_an_existing_snapshot_id_is_rejected():
    result, definition, store = _result(), _definition(), EvidenceStore()
    store.persist(result, definition)
    snapshot_id = next(iter(store.snapshots))
    row, durable = store.snapshots[snapshot_id]
    store.snapshots[snapshot_id] = ((*row[:6], "0" * 64), durable)

    with pytest.raises(ValueError, match="feature snapshot identity conflict"):
        store.persist(result, definition)


def test_materialization_batch_uses_set_based_relation_writes():
    result, definition, store = _setup_result(), _definition(), EvidenceStore()
    prepared = _prepare_realtime_equity_materialization(result, definition, generation=7)

    receipt = store.persist_batch((RealtimeEquityMaterializationWrite(result, definition, 7),))

    assert receipt.persisted_snapshot_ids == (prepared.snapshot_id,)
    assert receipt.skipped_source_window_ids == ()
    assert store.page_sizes == {500}
    inserts = [s for s in store.statements if s.startswith("INSERT INTO")]
    assert sum("INSERT INTO rt.feature_definitions" in s for s in inserts) == 1
    assert sum("INSERT INTO rt.evidence_catalog" in s for s in inserts) == 1
    assert sum("INSERT INTO rt.equity_evidence (" in s for s in inserts) == 1
    assert sum("INSERT INTO rt.feature_snapshots" in s for s in inserts) == 1
    assert sum("INSERT INTO rt.feature_current" in s for s in inserts) == 1
    assert sum("INSERT INTO rt.equity_evidence_current" in s for s in inserts) == 1
    assert len(store.evidence_bodies) == len(result.materialization.evidence)


def test_pattern_batch_keeps_technical_discovery_pointers_unchanged():
    result, definition, store = _setup_result(), _definition(), EvidenceStore()

    receipt = store.persist_batch((
        RealtimeEquityMaterializationWrite(result, definition, 7, update_evidence_current=False),
    ))

    assert receipt.persisted_count == 1
    assert any("INSERT INTO rt.feature_current" in s for s in store.statements)
    assert not any("INSERT INTO rt.equity_evidence_current" in s for s in store.statements)


def test_historical_batch_retains_all_snapshots_and_upserts_only_latest_feature_current():
    definition, store = _definition(), EvidenceStore()
    results = tuple(_result(offset) for offset in range(3))
    writes = tuple(
        RealtimeEquityMaterializationWrite(result, definition, generation=7 + offset,
                                           update_evidence_current=False)
        for offset, result in enumerate(results)
    )

    receipt = store.persist_batch(tuple(reversed(writes)))

    assert receipt.persisted_count == 3
    assert len(store.snapshots) == 3
    assert len(store.feature_current_rows) == 1
    assert store.feature_current_rows[0][4] == 9
    assert not any("INSERT INTO rt.equity_evidence_current" in s for s in store.statements)


@pytest.mark.parametrize("update_evidence_current", [True, False])
def test_materialization_batch_skips_exact_unchanged_closure(update_evidence_current):
    result, definition, store = _setup_result(), _definition(), EvidenceStore()
    write = RealtimeEquityMaterializationWrite(
        result, definition, 8, update_evidence_current=update_evidence_current,
    )
    store.persist_batch((write,))
    store.durable = LATER
    store.statements.clear()
    store.cursor.execute.reset_mock()

    receipt = store.persist_batch((write,))

    prepared = _prepare_realtime_equity_materialization(
        result, definition, generation=8, update_evidence_current=update_evidence_current,
    )
    assert receipt.persisted_snapshot_ids == ()
    assert receipt.skipped_source_window_ids == (prepared.source_window_id,)
    assert not any(s.startswith("INSERT INTO") for s in store.statements)
    assert store.cursor.execute.call_count == 3
    assert {durable for _, durable in store.evidence_bodies} == {DURABLE}


def test_batch_reports_a_source_window_conflict_without_writing():
    result, definition, store = _setup_result(), _definition(), EvidenceStore()
    write = RealtimeEquityMaterializationWrite(result, definition, 8)
    store.persist_batch((write,))
    window_id, durable = next(iter(store.window_bodies))
    body = store.window_bodies[(window_id, durable)]
    store.window_bodies[(window_id, durable)] = (*body[:10], "0" * 64)

    with pytest.raises(ValueError, match="source window conflict"):
        store.persist_batch((write,))


def test_feature_definition_must_match_materialized_feature_version():
    with pytest.raises(ValueError, match="version does not match"):
        persist_realtime_equity_materialization(
            MagicMock(), _result(), _definition("equity_features_v2"), generation=1,
        )


class _VerifyCursor:
    """Answers the verifier's SELECTs from a list of stored evidence rows."""

    def __init__(self, rows):
        self.rows, self.statements, self._page = list(rows), [], []

    def execute(self, statement, parameters):
        self.statements.append(statement)
        _, ids = parameters
        wanted = set(ids)
        self._page = [row for row in self.rows
                      if str(row["evidence_id"] if isinstance(row, dict) else row[0]) in wanted]

    def fetchall(self):
        return self._page


def _stored_evidence(prepared):
    return [(evidence_id, *identity) for evidence_id, identity in prepared.expected_evidence.items()]


def test_verification_accepts_exactly_the_persisted_identities_and_reads_only():
    result, definition = _setup_result(), _definition()
    write = RealtimeEquityMaterializationWrite(result, definition, 7)
    prepared = _prepare_realtime_equity_materialization(result, definition, generation=7)
    cursor = _VerifyCursor(_stored_evidence(prepared))
    assert verify_realtime_equity_materializations_persisted_cursor(cursor, (write,)) == len(
        prepared.expected_evidence)
    assert cursor.statements and all(statement.startswith("SELECT") for statement in cursor.statements)
    dict_rows = [dict(zip(("evidence_id", "evidence_key", "payload_sha256", "source_window_id",
                           "source_window_role"), row)) for row in _stored_evidence(prepared)]
    assert verify_realtime_equity_materializations_persisted_cursor(_VerifyCursor(dict_rows), (write,)) == len(
        prepared.expected_evidence)
    assert verify_realtime_equity_materializations_persisted_cursor(_VerifyCursor([]), ()) == 0


@pytest.mark.parametrize("defect", ("missing", "payload", "key", "window", "role"))
def test_verification_fails_closed_on_any_missing_or_different_evidence_row(defect):
    result, definition = _setup_result(), _definition()
    write = RealtimeEquityMaterializationWrite(result, definition, 7)
    stored = _stored_evidence(_prepare_realtime_equity_materialization(result, definition, generation=7))
    row = list(stored[0])
    if defect == "missing":
        stored.pop(0)
    elif defect == "payload":
        row[2] = "0" * 64
        stored[0] = tuple(row)
    elif defect == "key":
        row[1] = row[1] + "-other"
        stored[0] = tuple(row)
    elif defect == "window":
        row[3] = str(uuid4())
        stored[0] = tuple(row)
    elif defect == "role":
        row[4] = "PRIMARY" if row[4] != "PRIMARY" else None
        stored[0] = tuple(row)
    with pytest.raises(ValueError, match="not persisted as the materializations require"):
        verify_realtime_equity_materializations_persisted_cursor(_VerifyCursor(stored), (write,))


def test_verification_reads_in_bounded_chunks(monkeypatch):
    import realtime.equity_evidence_repository as repository

    result, definition = _setup_result(), _definition()
    prepared = _prepare_realtime_equity_materialization(result, definition, generation=7)
    cursor = _VerifyCursor(_stored_evidence(prepared))
    monkeypatch.setattr(repository, "_VERIFY_EVIDENCE_CHUNK", 2)
    verify_realtime_equity_materializations_persisted_cursor(
        cursor, (RealtimeEquityMaterializationWrite(result, definition, 7),))
    assert len(cursor.statements) == -(-len(prepared.expected_evidence) // 2) > 1


def test_equity_evidence_cursor_api_leaves_transaction_to_worker():
    result, definition, store = _result(), _definition(), EvidenceStore()

    snapshot_id = persist_realtime_equity_materialization_cursor(
        store.cursor, result, definition, generation=7,
        values_executor=store.values_executor, batch_executor=store.batch_executor,
    )

    assert snapshot_id == _prepare_realtime_equity_materialization(
        result, definition, generation=7,
    ).snapshot_id
    assert len(store.evidence_bodies) == len(result.materialization.evidence)


def test_current_evidence_ref_resolves_typed_pointer_through_the_catalog():
    result = _result()
    feature = next(row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT")
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        str(result.cohort_id), str(feature.evidence_id), feature.evidence_type.value,
        feature.market_time, feature.payload_sha256,
    )

    reference = resolve_current_evidence_ref_cursor(
        cursor,
        cohort_id=result.cohort_id,
        security_id=feature.security_id,
        interval=result.interval,
        evidence_type=feature.evidence_type,
        model_variant=feature.source_version,
    )

    assert isinstance(reference, EvidenceRef)
    assert reference.evidence_id == feature.evidence_id
    statement, parameters = cursor.execute.call_args.args
    assert "FROM rt.equity_evidence_current AS current" in statement
    assert "JOIN rt.evidence_catalog AS catalog" in statement
    assert "evidence.durable_at=catalog.durable_at" in statement
    assert "evidence.payload_sha256=current.payload_sha256" in statement
    assert parameters[-1] == feature.source_version


def test_as_of_evidence_ref_uses_market_and_knowledge_time():
    result = _setup_result()
    setup = next(row for row in result.materialization.evidence if row.evidence_type.value == "TRADE_SETUP")
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        str(result.cohort_id), str(setup.evidence_id), setup.evidence_type.value,
        setup.market_time, setup.payload_sha256,
    )

    reference = resolve_evidence_ref_as_of_cursor(
        cursor,
        cohort_id=result.cohort_id,
        security_id=setup.security_id,
        interval=result.interval,
        evidence_type=setup.evidence_type,
        model_variant=setup.source_version,
        market_as_of=setup.market_time,
        known_by=setup.observed_at,
    )

    assert reference is not None and reference.evidence_id == setup.evidence_id
    statement, parameters = cursor.execute.call_args.args
    assert "market_time<=%s AND observed_at<=%s" in statement
    assert "ORDER BY market_time DESC, observed_at DESC" in statement
    assert parameters[-2:] == (setup.market_time, setup.observed_at)


def test_release_evidence_refs_bound_knowledge_by_durable_arrival():
    result = _result()
    feature = next(row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT")
    reference = EvidenceRef(
        result.cohort_id, feature.evidence_id, feature.evidence_type,
        feature.market_time, feature.payload_sha256,
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = ((
        str(result.cohort_id), str(feature.evidence_id), feature.evidence_type.value,
        feature.market_time, feature.payload_sha256,
    ),)

    selected = resolve_release_evidence_refs_cursor(
        cursor, (reference,), security_id=feature.security_id, known_by=feature.observed_at,
    )

    assert selected == {feature.evidence_id: reference}
    statement = cursor.execute.call_args.args[0]
    assert "catalog.durable_at<=%s" in statement and "created_at" not in statement
    assert "evidence.durable_at=catalog.durable_at" in statement
    cursor.fetchall.return_value = ()
    with pytest.raises(ValueError, match="missing or its immutable identity"):
        resolve_release_evidence_refs_cursor(
            cursor, (reference,), security_id=feature.security_id, known_by=feature.observed_at,
        )


def _stored_body_row(result, row):
    """One `rt.equity_evidence` row joined to its source window, as the V2 tables return it: the
    `source_revision_ids` column is empty and the lineage is the window role or the dependency rows."""
    primary, confirmation = tuple(result.bar_revision_ids), tuple(result.confirmation_bar_revision_ids)
    sources = tuple(row.source_revision_ids)
    role = "PRIMARY" if sources and sources == primary else (
        "CONFIRMATION" if sources and sources == confirmation else None)
    return dict(
        evidence_id=row.evidence_id, evidence_type=row.evidence_type.value, evidence_role=row.evidence_role.value,
        market_time=row.market_time, observed_at=row.observed_at, valid_until=row.valid_until,
        source_name=row.source_name, source_version=row.source_version,
        payload_schema_version=row.payload_schema_version, analysis_run_id=row.analysis_run_id,
        latest_bar_revision_id=row.latest_bar_revision_id, security_revision_id=row.security_revision_id,
        fundamental_report_ids=list(row.fundamental_report_ids), quality_state=row.quality_state.value,
        quality_codes=list(row.quality_codes), payload=json.loads(row.payload_json),
        payload_sha256=row.payload_sha256, source_window_role=role,
        primary_bar_revision_ids=list(primary), confirmation_bar_revision_ids=list(confirmation),
    ), role, sources


def _body_cursor(result, *, change=None, dependency_rows=None):
    rows, edges = [], []
    for evidence in result.materialization.evidence:
        stored, role, sources = _stored_body_row(result, evidence)
        if change is not None:
            stored = change(evidence, stored)
        if stored is not None:
            rows.append(stored)
        if role is None:
            edges.extend(dict(evidence_id=evidence.evidence_id, dependency_order=order, dependency_evidence_id=value)
                         for order, value in enumerate(sources))
    cursor = MagicMock()
    cursor.fetchall.side_effect = [rows, edges if dependency_rows is None else dependency_rows]
    expected = {row.evidence_id: (row.evidence_type.value, row.payload_sha256)
                for row in result.materialization.evidence}
    return cursor, expected


def _load_bodies(cursor, expected, result):
    return load_release_evidence_bodies_cursor(
        cursor, expected, cohort_id=result.cohort_id, security_id=result.materialization.evidence[0].security_id,
        provider_tier=result.provider_tier.value, known_by=LATER,
    )


def test_release_evidence_bodies_rebuild_the_source_lineage_the_table_leaves_empty():
    result = _setup_result()
    cursor, expected = _body_cursor(result)

    bodies = _load_bodies(cursor, expected, result)

    assert bodies.keys() == expected.keys()
    roles = set()
    for evidence in result.materialization.evidence:
        assert bodies[evidence.evidence_id]["source_revision_ids"] == tuple(evidence.source_revision_ids)
        assert bodies[evidence.evidence_id]["payload"] == json.loads(evidence.payload_json)
        roles.add(_stored_body_row(result, evidence)[1])
    assert {"PRIMARY", None} <= roles
    statement, parameters = cursor.execute.call_args_list[0].args
    assert "evidence.durable_at=catalog.durable_at" in statement
    assert "source_window.durable_at=window_catalog.durable_at" in statement
    assert "evidence.observed_at<=%s AND catalog.durable_at<=%s" in statement
    assert parameters[-2:] == (LATER, LATER)
    assert "equity_evidence_dependencies" in cursor.execute.call_args_list[1].args[0]


def test_release_evidence_bodies_without_pinned_rows_read_nothing():
    cursor = MagicMock()
    assert load_release_evidence_bodies_cursor(
        cursor, {}, cohort_id=uuid4(), security_id=uuid4(), provider_tier="MASSIVE_DEVELOPER", known_by=LATER,
    ) == {}
    cursor.execute.assert_not_called()


@pytest.mark.parametrize("defect", ["missing", "type", "pinned_hash", "stored_hash", "payload", "extra"])
def test_release_evidence_bodies_fail_closed_on_any_identity_or_hash_change(defect):
    result = _setup_result()
    target = result.materialization.evidence[1].evidence_id

    def change(evidence, stored):
        if evidence.evidence_id != target:
            return stored
        if defect == "missing":
            return None
        if defect == "type":
            return dict(stored, evidence_type="PATTERN_OBSERVATION")
        if defect == "stored_hash":
            return dict(stored, payload_sha256="0" * 64)
        if defect == "payload":
            return dict(stored, payload=dict(stored["payload"], tampered=True))
        return stored

    cursor, expected = _body_cursor(result, change=change)
    if defect == "pinned_hash":
        expected[target] = (expected[target][0], "0" * 64)
    if defect == "extra":
        expected[uuid4()] = ("TRADE_SETUP", "1" * 64)
    with pytest.raises(ValueError, match="missing or its immutable identity"):
        _load_bodies(cursor, expected, result)


def test_release_evidence_bodies_reject_a_dependency_chain_with_a_gap():
    result = _setup_result()
    gapped = [
        dict(evidence_id=row.evidence_id, dependency_order=order * 2, dependency_evidence_id=value)
        for row in result.materialization.evidence if _stored_body_row(result, row)[1] is None
        for order, value in enumerate(row.source_revision_ids)
    ]
    assert any(edge["dependency_order"] > 0 for edge in gapped)
    cursor, expected = _body_cursor(result, dependency_rows=gapped)
    with pytest.raises(ValueError, match="missing or its immutable identity"):
        _load_bodies(cursor, expected, result)
