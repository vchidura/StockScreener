from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from psycopg2 import errors
from psycopg2.extras import execute_values

from realtime.session_close_coverage import (
    audit_session_close_cursor,
    five_minute_session,
    persist_session_close_coverage_cursor,
    session_close_audit_id,
)
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
DATABASE = f"test_realtime_v2_close_{uuid4().hex[:12]}"
UTC = timezone.utc


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as connection_pool:
        yield connection_pool


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def _seed_session(pool, session_date, tickers):
    session = five_minute_session(session_date)
    observed_at = session.closed_at + timedelta(minutes=20)
    profile, manifest, cohort = uuid4(), uuid4(), uuid4()
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rt.ensure_partitions('rt.bar_revisions', %s)",
                (session_date,),
            )
            cursor.execute(
                "INSERT INTO rt.capability_profiles ("
                "profile_id, config_sha256, schema_sha256, account_alias, "
                "capabilities_sha256, universe_sha256, checked_at, expires_at, "
                "status, profile_sha256, provider_tier, security_policy_sha256, "
                "preflight_mode, provider_requests, action_authorized"
                ") VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,"
                "'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
                (
                    str(profile),
                    _digest(),
                    _digest(),
                    _digest(),
                    _digest(),
                    session.opened_at - timedelta(hours=1),
                    session.closed_at + timedelta(hours=1),
                    _digest(),
                    _digest(),
                ),
            )
            cursor.execute(
                "INSERT INTO rt.subscription_manifests ("
                "manifest_id, manifest_sha256, universe_sha256, session_date, "
                "effective_at, profile_id, manifest_kind, stock_count, "
                "option_underlying_count, member_count, allocation_policy_sha256"
                ") VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s)",
                (
                    str(manifest),
                    _digest(),
                    _digest(),
                    session_date - timedelta(days=2),
                    session.opened_at - timedelta(days=2),
                    str(profile),
                    len(tickers),
                    len(tickers),
                    _digest(),
                ),
            )
            cursor.execute(
                "INSERT INTO rt.cohorts ("
                "cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
                "model_bundle_sha256, valuation_sha256, effective_at, mode"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
                (
                    str(cohort),
                    _digest(),
                    str(manifest),
                    _digest(),
                    _digest(),
                    _digest(),
                    session.opened_at - timedelta(days=2),
                ),
            )
            members = []
            securities = {}
            for ticker in tickers:
                security, revision = uuid4(), uuid4()
                cursor.execute(
                    "INSERT INTO ref.securities "
                    "(security_id, asset_type, source_origin, created_at) "
                    "VALUES (%s,'STOCK','fixture',%s)",
                    (str(security), session.opened_at - timedelta(days=1)),
                )
                cursor.execute(
                    "INSERT INTO ref.security_revisions ("
                    "revision_id, security_id, ticker, calendar_id, effective_from, "
                    "observed_at, payload_sha256, active"
                    ") VALUES (%s,%s,%s,'XNYS',%s,%s,%s,true)",
                    (
                        str(revision),
                        str(security),
                        ticker,
                        session.opened_at - timedelta(days=1),
                        session.opened_at - timedelta(days=1),
                        _digest(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.instruments (asset_class, security_id) "
                    "VALUES ('EQUITY',%s) RETURNING instrument_id",
                    (str(security),),
                )
                instrument = int(cursor.fetchone()[0])
                cursor.execute(
                    "INSERT INTO rt.subscription_members ("
                    "manifest_id, instrument_id, channel, priority, purpose"
                    ") VALUES (%s,%s,'AM',1,'PRIMARY')",
                    (str(manifest), instrument),
                )
                members.append((instrument, ticker))
                securities[instrument] = security

            catalog_values = []
            current_values = []
            revision_values = []
            for instrument, ticker in members:
                for bar_start in session.bar_starts:
                    revision = uuid4()
                    bar_end = bar_start + timedelta(minutes=5)
                    catalog_values.append(
                        (
                            str(revision),
                            str(cohort),
                            "MASSIVE_DEVELOPER",
                            instrument,
                            "5m",
                            bar_start,
                            bar_end,
                            observed_at,
                            session_date,
                        )
                    )
                    current_values.append(
                        (
                            str(cohort),
                            "MASSIVE_DEVELOPER",
                            instrument,
                            "5m",
                            bar_start,
                            "RTH",
                            "RAW_UNADJUSTED",
                            "NATIVE_REST",
                            str(revision),
                            bar_end,
                            observed_at,
                            observed_at,
                        )
                    )
                    revision_values.append((
                        str(revision), str(cohort), "MASSIVE_DEVELOPER",
                        str(securities[instrument]), instrument, ticker, "5m",
                        session_date, "RTH", "RAW_UNADJUSTED", bar_start, bar_end,
                        100, 101, 99, 100, 1000, "NATIVE_REST", "LIVE_OBSERVED",
                        True, observed_at, str(uuid4()), _digest(), observed_at,
                    ))
            execute_values(
                cursor,
                "INSERT INTO rt.bar_catalog ("
                "bar_revision_id, cohort_id, provider_tier, instrument_id, "
                "interval, bar_start, bar_end, observed_at, session_date"
                ") VALUES %s",
                catalog_values,
            )
            execute_values(
                cursor,
                "INSERT INTO rt.bar_revisions ("
                "bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, "
                "ticker, interval, session_date, session_scope, adjustment_basis, "
                "bar_start, bar_end, open_price, high_price, low_price, close_price, "
                "volume, source_kind, availability_mode, is_final, observed_at, "
                "ingestion_segment_id, payload_sha256, created_at) VALUES %s",
                revision_values,
            )
            execute_values(
                cursor,
                "INSERT INTO rt.bar_current ("
                "cohort_id, provider_tier, instrument_id, interval, bar_start, "
                "session_scope, adjustment_basis, source_kind, bar_revision_id, "
                "bar_end, observed_at, updated_at"
                ") VALUES %s",
                current_values,
            )
        connection.commit()
    finally:
        pool.putconn(connection)
    return cohort, members, session, observed_at


@pytest.fixture
def half_day(pool):
    return _seed_session(pool, date(2026, 11, 27), ("AAPL", "MSFT"))


def _audit(pool, cohort: UUID, session_date: date, known_by: datetime):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return audit_session_close_cursor(
                cursor,
                cohort_id=cohort,
                provider_tier="MASSIVE_DEVELOPER",
                session_date=session_date,
                known_by=known_by,
            )
    finally:
        connection.rollback()
        pool.putconn(connection)


def _persist(pool, coverage):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            identity = persist_session_close_coverage_cursor(cursor, coverage)
        connection.commit()
        return identity
    except Exception:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


def _revise_first_bar(
    pool, half_day, revised_at, *, source_kind="NATIVE_REST", make_current=True,
    created_at=None, replay_available_at=None, provider_published_at=None,
):
    cohort, members, session, _ = half_day
    revision_id = uuid4()
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT bar_revision_id FROM rt.bar_current WHERE cohort_id=%s "
                "AND instrument_id=%s AND interval='5m' AND bar_start=%s",
                (str(cohort), members[0][0], session.bar_starts[0]),
            )
            original_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO rt.bar_catalog (bar_revision_id, cohort_id, provider_tier, "
                "instrument_id, interval, bar_start, bar_end, observed_at, session_date) "
                "SELECT %s, cohort_id, provider_tier, instrument_id, interval, "
                "bar_start, bar_end, %s, session_date FROM rt.bar_catalog "
                "WHERE bar_revision_id=%s",
                (str(revision_id), revised_at, original_id),
            )
            cursor.execute(
                "INSERT INTO rt.bar_revisions ("
                "bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, "
                "ticker, interval, session_date, session_scope, adjustment_basis, "
                "bar_start, bar_end, open_price, high_price, low_price, close_price, "
                "volume, source_kind, availability_mode, is_final, observed_at, "
                "ingestion_segment_id, source_revision_ids, payload_sha256, created_at, "
                "replay_available_at, provider_published_at) "
                "SELECT %s, cohort_id, provider_tier, security_id, instrument_id, "
                "ticker, interval, session_date, session_scope, adjustment_basis, "
                "bar_start, bar_end, open_price, high_price, low_price, close_price, "
                "volume, %s, availability_mode, is_final, %s, ingestion_segment_id, "
                "CASE WHEN %s='NATIVE_REST' THEN ARRAY[]::uuid[] "
                "ELSE ARRAY[bar_revision_id] END, %s, %s, %s, %s "
                "FROM rt.bar_revisions WHERE bar_revision_id=%s",
                (
                    str(revision_id), source_kind, revised_at, source_kind, _digest(),
                    created_at or revised_at, replay_available_at,
                    provider_published_at, original_id,
                ),
            )
            if make_current:
                cursor.execute(
                    "UPDATE rt.bar_current SET bar_revision_id=%s, observed_at=%s, "
                    "source_kind=%s, updated_at=%s WHERE cohort_id=%s "
                    "AND instrument_id=%s AND interval='5m' AND bar_start=%s",
                    (
                        str(revision_id), revised_at, source_kind,
                        created_at or revised_at, str(cohort),
                        members[0][0], session.bar_starts[0],
                    ),
                )
        connection.commit()
    finally:
        pool.putconn(connection)
    return revision_id


def test_half_day_complete_coverage_is_read_only_and_exact(pool, half_day):
    cohort, members, session, observed_at = half_day

    result = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    )

    assert result.status == "COMPLETE"
    assert result.instrument_count == len(members) == 2
    assert result.manifest_session_date == date(2026, 11, 25)
    assert result.session.expected_per_instrument == 42
    assert result.expected_members == result.complete_members == 84
    assert result.missing_members == 0
    assert result.missing_ranges == ()
    assert result.query_seconds < 5
    replay = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=2)
    )
    assert replay.known_by != result.known_by
    assert replay.input_sha256 == result.input_sha256
    assert replay.coverage_sha256 == result.coverage_sha256
    assert session_close_audit_id(replay) == session_close_audit_id(result)

    persisted = _persist(pool, result)
    replayed = _persist(pool, replay)
    identity = persisted.audit_id
    assert identity == session_close_audit_id(result)
    assert persisted.pointer_advanced is True
    assert replayed.audit_id == identity
    assert replayed.pointer_advanced is False

    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM rt.session_close_audits WHERE audit_id=%s",
                (str(identity),),
            )
            assert cursor.fetchone()[0] == 1
            cursor.execute(
                "SELECT audit_id::text, status FROM rt.session_close_current "
                "WHERE cohort_id=%s AND provider_tier='MASSIVE_DEVELOPER' "
                "AND session_date=%s",
                (str(cohort), session.session_date),
            )
            assert cursor.fetchone() == (str(identity), "COMPLETE")
        connection.rollback()
    finally:
        pool.putconn(connection)

    _revise_first_bar(
        pool, half_day, observed_at + timedelta(minutes=2, seconds=30)
    )
    revised = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=3)
    )
    assert revised.complete_members == result.complete_members
    assert revised.input_sha256 != result.input_sha256
    assert revised.coverage_sha256 != result.coverage_sha256
    revised_persistence = _persist(pool, revised)
    assert revised_persistence.pointer_advanced is True
    assert revised_persistence.current_status == "COMPLETE"
    assert revised_persistence.current_audit_id == revised_persistence.audit_id


def test_half_day_one_missing_slot_is_one_exact_repair_range(pool, half_day):
    cohort, members, session, observed_at = half_day
    _persist(pool, _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    ))
    missing_start = session.bar_starts[-1]
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM rt.bar_current "
                "WHERE cohort_id=%s AND instrument_id=%s AND interval='5m' "
                "AND bar_start=%s",
                (str(cohort), members[0][0], missing_start),
            )
        connection.commit()
    finally:
        pool.putconn(connection)

    result = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=4)
    )

    assert result.status == "INCOMPLETE"
    assert result.expected_members == 84
    assert result.complete_members == 83
    assert result.missing_members == 1
    assert result.missing_range_count == 1
    missing = result.missing_ranges[0]
    assert (missing.ticker, missing.requested_from, missing.requested_to) == (
        "AAPL",
        missing_start,
        session.closed_at,
    )
    persisted = _persist(pool, result)
    identity = persisted.audit_id
    assert persisted.pointer_advanced is False
    assert persisted.current_status == "COMPLETE"
    assert persisted.current_audit_id != identity

    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT audit_id::text, status FROM rt.session_close_current "
                "WHERE cohort_id=%s AND provider_tier='MASSIVE_DEVELOPER' "
                "AND session_date=%s",
                (str(cohort), session.session_date),
            )
            current_audit_id, current_status = cursor.fetchone()
            assert current_status == "COMPLETE"
            assert current_audit_id == str(persisted.current_audit_id)
            cursor.execute(
                "SELECT ticker, requested_from, requested_to, missing_members "
                "FROM rt.session_close_anomalies WHERE audit_id=%s",
                (str(identity),),
            )
            assert cursor.fetchone() == (
                "AAPL",
                missing_start,
                session.closed_at,
                1,
            )
        connection.rollback()
    finally:
        pool.putconn(connection)

    connection = pool.getconn()
    try:
        with pytest.raises(errors.RaiseException, match="cannot regress"):
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE rt.session_close_current SET audit_id=%s, known_by=%s, "
                    "status='INCOMPLETE' WHERE cohort_id=%s "
                    "AND provider_tier='MASSIVE_DEVELOPER' AND session_date=%s",
                    (
                        str(identity),
                        result.known_by,
                        str(cohort),
                        session.session_date,
                    ),
                )
    finally:
        connection.rollback()
        pool.putconn(connection)


def test_earlier_known_by_replays_after_current_revision_changes(pool, half_day):
    cohort, _, session, observed_at = half_day
    known_by = observed_at + timedelta(minutes=1)
    original = _audit(pool, cohort, session.session_date, known_by)
    _revise_first_bar(pool, half_day, observed_at + timedelta(minutes=2))

    replay = _audit(pool, cohort, session.session_date, known_by)

    assert replay.status == "COMPLETE"
    assert replay.input_sha256 == original.input_sha256
    assert session_close_audit_id(replay) == session_close_audit_id(original)


def test_incomplete_audit_advances_only_after_inputs_are_observed(pool, half_day):
    cohort, _, session, observed_at = half_day
    incomplete = _audit(
        pool, cohort, session.session_date, observed_at - timedelta(minutes=1)
    )

    assert incomplete.status == "INCOMPLETE"
    assert incomplete.complete_members == 0
    assert incomplete.missing_members == 84
    assert incomplete.missing_range_count == 2
    assert _persist(pool, incomplete).current_status == "INCOMPLETE"

    complete = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    )
    persisted = _persist(pool, complete)

    assert persisted.pointer_advanced is True
    assert persisted.current_status == "COMPLETE"
    assert _persist(pool, incomplete).current_audit_id == persisted.audit_id


def test_late_created_revision_cannot_rewrite_an_earlier_inspection(pool, half_day):
    cohort, _, session, observed_at = half_day
    known_by = observed_at + timedelta(minutes=1)
    original = _audit(pool, cohort, session.session_date, known_by)
    _revise_first_bar(
        pool, half_day, observed_at + timedelta(seconds=30),
        created_at=observed_at + timedelta(minutes=2),
    )

    replay = _audit(pool, cohort, session.session_date, known_by)
    revised = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=3)
    )

    assert replay.input_sha256 == original.input_sha256
    assert revised.status == "COMPLETE"
    assert revised.input_sha256 != original.input_sha256


@pytest.mark.parametrize(
    "clock", ("observed_at", "created_at", "replay_available_at", "provider_published_at")
)
def test_historical_selection_excludes_revisions_not_yet_visible(pool, half_day, clock):
    cohort, _, session, observed_at = half_day
    known_by = observed_at + timedelta(minutes=3)
    original = _audit(pool, cohort, session.session_date, known_by)
    future = observed_at + timedelta(minutes=5)
    clocks = {} if clock == "observed_at" else {clock: future}
    _revise_first_bar(
        pool, half_day,
        future if clock == "observed_at" else observed_at + timedelta(minutes=1),
        source_kind="RECONCILED", make_current=False, **clocks,
    )
    _revise_first_bar(pool, half_day, observed_at + timedelta(minutes=4))

    replay = _audit(pool, cohort, session.session_date, known_by)

    assert replay.status == "COMPLETE"
    assert replay.input_sha256 == original.input_sha256


def test_historical_selection_preserves_source_precedence(pool, half_day):
    cohort, _, session, observed_at = half_day
    _revise_first_bar(
        pool, half_day, observed_at + timedelta(minutes=1), source_kind="RECONCILED"
    )
    _revise_first_bar(
        pool, half_day, observed_at + timedelta(minutes=2), make_current=False
    )
    known_by = observed_at + timedelta(minutes=3)
    original = _audit(pool, cohort, session.session_date, known_by)
    _revise_first_bar(
        pool, half_day, observed_at + timedelta(minutes=4), source_kind="RECONCILED"
    )

    replay = _audit(pool, cohort, session.session_date, known_by)

    assert replay.status == "COMPLETE"
    assert replay.input_sha256 == original.input_sha256


def test_pointer_rejects_stale_audits_in_database_and_repository(pool, half_day):
    cohort, _, session, observed_at = half_day
    original = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    )
    original_id = _persist(pool, original).audit_id
    _revise_first_bar(pool, half_day, observed_at + timedelta(minutes=2))
    revised = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=3)
    )
    revised_id = _persist(pool, revised).audit_id

    stale = _persist(pool, original)
    assert stale.pointer_advanced is False
    assert stale.current_audit_id == revised_id

    connection = pool.getconn()
    try:
        with pytest.raises(errors.RaiseException, match="cannot move backward"):
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE rt.session_close_current SET audit_id=%s, known_by=%s "
                    "WHERE cohort_id=%s AND provider_tier='MASSIVE_DEVELOPER' "
                    "AND session_date=%s",
                    (str(original_id), original.known_by, str(cohort), session.session_date),
                )
    finally:
        connection.rollback()
        pool.putconn(connection)


def test_pointer_scope_cannot_be_changed(pool, half_day):
    cohort, _, session, observed_at = half_day
    _persist(pool, _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    ))
    connection = pool.getconn()
    try:
        with pytest.raises(errors.RaiseException, match="scope cannot change"):
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE rt.session_close_current SET session_date=session_date+1 "
                    "WHERE cohort_id=%s",
                    (str(cohort),),
                )
    finally:
        connection.rollback()
        pool.putconn(connection)


@pytest.mark.parametrize("instrument_count,seconds", ((9, 1), (400, 5)))
def test_regular_session_current_coverage_meets_exact_counts_and_query_budget(
    pool, instrument_count, seconds
):
    cohort, _, session, observed_at = _seed_session(
        pool, date(2026, 10, 15),
        tuple(f"TEST{index:03}" for index in range(instrument_count)),
    )

    result = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    )

    assert result.status == "COMPLETE"
    assert result.expected_members == result.complete_members == instrument_count * 78
    assert result.missing_members == 0
    assert result.query_seconds < seconds


@pytest.mark.parametrize("changed_inputs", (False, True))
def test_concurrent_persistence_preserves_replay_and_latest_pointer(
    pool, half_day, changed_inputs
):
    cohort, _, session, observed_at = half_day
    original = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=1)
    )
    if changed_inputs:
        _revise_first_bar(pool, half_day, observed_at + timedelta(minutes=2))
    revised = _audit(
        pool, cohort, session.session_date, observed_at + timedelta(minutes=3)
    )
    barrier = Barrier(2)

    def persist(coverage):
        barrier.wait(timeout=10)
        return _persist(pool, coverage)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(persist, coverage) for coverage in (original, revised)]
        results = [future.result(timeout=10) for future in futures]

    assert {row.audit_id for row in results} == {
        session_close_audit_id(original), session_close_audit_id(revised)
    }
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT audit_id::text FROM rt.session_close_current WHERE cohort_id=%s "
                "AND provider_tier='MASSIVE_DEVELOPER' AND session_date=%s",
                (str(cohort), session.session_date),
            )
            assert cursor.fetchone()[0] == str(session_close_audit_id(revised))
            cursor.execute(
                "SELECT count(*) FROM rt.session_close_audits WHERE cohort_id=%s",
                (str(cohort),),
            )
            assert cursor.fetchone()[0] == (2 if changed_inputs else 1)
    finally:
        connection.rollback()
        pool.putconn(connection)
