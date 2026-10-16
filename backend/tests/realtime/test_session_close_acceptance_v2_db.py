"""Opt-in isolated measurements, not a live API/worker isolation certificate."""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock
from time import perf_counter, sleep
from uuid import uuid4

from psycopg2.pool import ThreadedConnectionPool

from realtime.database_access_v2 import API_ROLE, APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2, harden_database_v2
from tests.realtime.session_close_acceptance_probe import METRICS_MARKER
from tests.realtime.test_equity_bar_adapter import _developer_revision
from tests.realtime.test_session_close_coverage_v2_db import _digest, _seed_session
from v2_database import _connect, _settings, disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database
ROOT = Path(__file__).resolve().parents[3]
PROBE = Path(__file__).with_name("session_close_acceptance_probe.py")


@contextmanager
def _hardened_database():
    suffix = "close_perf_" + uuid4().hex[:10]
    database, roles = "test_realtime_v2_" + suffix, build_role_names_v2("test_rt_" + suffix)
    settings = _settings()
    owns_roles = False
    try:
        with disposable_v2_database(database) as pool:
            with closing(_connect(settings, database)) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s)",
                        (list(roles.all_names),),
                    )
                    assert cursor.fetchone()[0] == 0, "refusing existing disposable roles"
                connection.rollback()
                owns_roles = True
                created, audit = harden_database_v2(connection, roles)
                assert len(created) == 3
                assert all(status.startswith("PASS") for _, status in audit), audit
            yield database, roles.prefix, pool, settings
    finally:
        if owns_roles:
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, roles)


def _snapshot(pool):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT (SELECT count(*) FROM rt.bar_catalog), "
                "(SELECT count(*) FROM rt.bar_revisions), "
                "(SELECT md5(string_agg(bar_revision_id::text || observed_at::text || "
                "updated_at::text, ',' ORDER BY bar_revision_id)) FROM rt.bar_current), "
                "(SELECT count(*) FROM rt.provider_request_ledger), "
                "(SELECT count(*) FROM rt.portal_releases), "
                "(SELECT count(*) FROM rt.session_close_audits), "
                "(SELECT count(*) FROM rt.session_close_anomalies), "
                "(SELECT count(*) FROM rt.session_close_current)"
            )
            return cursor.fetchone()
    finally:
        connection.rollback()
        pool.putconn(connection)


def _probe(database, prefix, fixture, label, *, persist=False, refuse=False, known_by=None):
    cohort, members, session, observed_at = fixture
    command = [
        sys.executable, "-B", str(PROBE), "--database", database,
        "--confirm-database-name", database, "--role-prefix", prefix,
        "--cohort-id", str(cohort), "--session-date", session.session_date.isoformat(),
        "--known-by", (known_by or observed_at + timedelta(minutes=1)).isoformat(),
    ]
    if persist:
        command.extend(("--persist", "--execute"))
    if refuse:
        command.append("--expect-truncated-refusal")
    started = perf_counter()
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=60)
    wall_seconds = perf_counter() - started
    assert result.returncode == 0, result.stderr[-6000:]
    payload, metrics_json = result.stdout.rsplit("\n" + METRICS_MARKER, 1)
    report, metrics = json.loads(payload), json.loads(metrics_json)
    assert report["provider_requests"] == 0
    assert metrics["peak_working_set_bytes"] > 0
    summary = {
        "scenario": label, "instruments": len(members),
        "invocation_wall_seconds": wall_seconds, **metrics,
        **{key: report[key] for key in (
            "status", "expected_members", "complete_members", "missing_members",
            "missing_range_count", "missing_ranges_truncated", "input_sha256",
            "outcome", "audit_id", "current_audit_id", "current_status", "pointer_advanced",
        )},
    }
    return summary


def _checked_probe(pool, *args, **kwargs):
    before = _snapshot(pool)
    summary = _probe(*args, **kwargs)
    after = _snapshot(pool)
    assert before[:5] == after[:5], "audit changed bars, heads, provider ledger or releases"
    summary["evidence_row_delta"] = [
        int(new - old) for old, new in zip(before[5:], after[5:])
    ]
    if not kwargs.get("persist") or kwargs.get("refuse"):
        assert summary["evidence_row_delta"] == [0, 0, 0]
    return summary


def _revise_all_heads(pool, fixture):
    cohort, _, _, observed_at = fixture
    revised_at = observed_at + timedelta(minutes=2)
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.bar_catalog (bar_revision_id, cohort_id, provider_tier, "
                "instrument_id, interval, bar_start, bar_end, observed_at, session_date) "
                "SELECT gen_random_uuid(), cur.cohort_id, cur.provider_tier, "
                "cur.instrument_id, cur.interval, cur.bar_start, cur.bar_end, %s, cat.session_date "
                "FROM rt.bar_current cur JOIN rt.bar_catalog cat "
                "ON cat.bar_revision_id=cur.bar_revision_id WHERE cur.cohort_id=%s",
                (revised_at, str(cohort)),
            )
            cursor.execute(
                "INSERT INTO rt.bar_revisions ("
                "bar_revision_id, cohort_id, provider_tier, security_id, instrument_id, "
                "ticker, interval, session_date, session_scope, adjustment_basis, bar_start, "
                "bar_end, open_price, high_price, low_price, close_price, volume, source_kind, "
                "availability_mode, is_final, observed_at, ingestion_segment_id, payload_sha256, created_at) "
                "SELECT cat.bar_revision_id, old.cohort_id, old.provider_tier, old.security_id, "
                "old.instrument_id, old.ticker, old.interval, old.session_date, old.session_scope, "
                "old.adjustment_basis, old.bar_start, old.bar_end, old.open_price, old.high_price, "
                "old.low_price, old.close_price, old.volume, old.source_kind, old.availability_mode, "
                "old.is_final, cat.observed_at, old.ingestion_segment_id, "
                "repeat(md5(cat.bar_revision_id::text),2), cat.observed_at "
                "FROM rt.bar_catalog cat JOIN rt.bar_current cur "
                "ON cur.cohort_id=cat.cohort_id AND cur.instrument_id=cat.instrument_id "
                "AND cur.interval=cat.interval AND cur.bar_start=cat.bar_start "
                "JOIN rt.bar_revisions old ON old.bar_revision_id=cur.bar_revision_id "
                "AND old.observed_at=cur.observed_at "
                "WHERE cat.cohort_id=%s AND cat.observed_at=%s",
                (str(cohort), revised_at),
            )
            cursor.execute(
                "UPDATE rt.bar_current cur SET bar_revision_id=cat.bar_revision_id, "
                "observed_at=cat.observed_at, updated_at=cat.observed_at "
                "FROM rt.bar_catalog cat WHERE cat.cohort_id=cur.cohort_id "
                "AND cat.instrument_id=cur.instrument_id AND cat.interval=cur.interval "
                "AND cat.bar_start=cur.bar_start AND cat.cohort_id=%s AND cat.observed_at=%s",
                (str(cohort), revised_at),
            )
        connection.commit()
    finally:
        pool.putconn(connection)
    return revised_at


def _fragment_heads(pool, fixture):
    cohort, members, session, _ = fixture
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM rt.bar_current WHERE cohort_id=%s "
                "AND mod((extract(epoch FROM (bar_start-%s::timestamptz))/300)::integer,2)=1",
                (str(cohort), session.opened_at),
            )
            assert cursor.rowcount == len(members) * 39
        connection.commit()
    finally:
        pool.putconn(connection)


def _percentile(values, percentile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _contention(database, prefix, settings, audit_fixture, writer_fixture):
    cohort, members, session, _ = writer_fixture
    with closing(_connect(settings, database)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT security_id FROM rt.instruments WHERE instrument_id=%s",
                (members[0][0],),
            )
            security = cursor.fetchone()[0]
    pools = {
        role: ThreadedConnectionPool(
            1, 1, dbname=database, user=settings["POSTGRES_ADMIN_USER"],
            password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
            port=settings["DB_PORT"], options="-c timezone=UTC",
        )
        for role in (API_ROLE, APP_ROLE)
    }
    config = V2AccessConfig(security_mode="hardened", role_prefix=prefix, allow_disposable=True)
    providers = {role: V2ConnectionProvider({role: pool}, config) for role, pool in pools.items()}
    stop, ready = Event(), {role: Event() for role in pools}
    lock, phase = Lock(), ["before"]
    samples = {name: {role: [] for role in pools} for name in ("before", "during", "after")}
    phase_seconds = {}
    base = _developer_revision()

    def loop(role):
        ready[role].set()
        while not stop.is_set():
            with lock:
                name = phase[0]
            started = perf_counter()
            with providers[role].transaction(role, read_only=role == API_ROLE) as unit:
                if role == API_ROLE:
                    unit.cursor.execute(
                        "SELECT bar_revision_id, bar_start, bar_end FROM rt.bar_current "
                        "WHERE cohort_id=%s AND instrument_id=%s AND interval='5m' "
                        "AND session_scope='RTH' AND adjustment_basis='RAW_UNADJUSTED' "
                        "ORDER BY bar_start DESC LIMIT 20",
                        (str(cohort), members[0][0]),
                    )
                    assert len(unit.cursor.fetchall()) == 20
                else:
                    revision = replace(
                        base, bar_revision_id=uuid4(), security_id=security,
                        ticker=members[0][1], interval="5m", session_date=session.session_date,
                        bar_start=session.bar_starts[-1], bar_end=session.closed_at,
                        system_observed_at=datetime.now(timezone.utc),
                        ingestion_segment_id=uuid4(), payload_sha256=_digest(),
                    )
                    fact = adapt_developer_equity_bar(
                        revision, cohort_id=cohort, instrument_id=members[0][0]
                    )
                    persist_realtime_equity_bars_cursor(unit.cursor, (fact,))
            with lock:
                samples[name][role].append(perf_counter() - started)
            stop.wait(0.01 if role == API_ROLE else 0.02)

    try:
        for provider in providers.values():
            assert provider.verify() == database
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(loop, role) for role in pools]
            try:
                assert all(event.wait(timeout=5) for event in ready.values())
                started = perf_counter()
                sleep(2)
                phase_seconds["before"] = perf_counter() - started
                with lock:
                    phase[0] = "during"
                started = perf_counter()
                for index in range(3):
                    audit = _probe(
                        database, prefix, audit_fixture, f"contention-audit-{index + 1}"
                    )
                    assert audit["status"] == "COMPLETE"
                    assert audit["historical_queries"] == 0
                phase_seconds["during"] = perf_counter() - started
                with lock:
                    phase[0] = "after"
                started = perf_counter()
                sleep(2)
                phase_seconds["after"] = perf_counter() - started
            finally:
                stop.set()
                for future in futures:
                    future.result(timeout=10)
    finally:
        for pool in pools.values():
            pool.closeall()
    measurements = {}
    for name, roles in samples.items():
        measurements[name] = {}
        for role, values in roles.items():
            assert len(values) >= 20, (name, role, len(values))
            measurements[name][role] = {
                "operations": len(values), "p50_ms": _percentile(values, 0.5) * 1000,
                "p95_ms": _percentile(values, 0.95) * 1000, "maximum_ms": max(values) * 1000,
                "operations_per_second": len(values) / phase_seconds[name],
            }
    return {
        "kind": "hardened DB reader and real immutable bar-writer proxy, not HTTP/market-lag",
        "measurements": measurements,
        "reader_during_vs_before_p95": (
            measurements["during"][API_ROLE]["p95_ms"] / measurements["before"][API_ROLE]["p95_ms"]
        ),
        "writer_during_vs_before_p95": (
            measurements["during"][APP_ROLE]["p95_ms"] / measurements["before"][APP_ROLE]["p95_ms"]
        ),
        "verdict": "MEASURED_NOT_GATED_NO_APPROVED_REGRESSION_BUDGET",
    }


def test_isolated_session_close_acceptance(tmp_path):
    cases = []
    with _hardened_database() as (database, prefix, pool, settings):
        fixtures = {
            count: _seed_session(
                pool, date(2026, 10, 15), tuple(f"TEST{index:03}" for index in range(count))
            )
            for count in (9, 400)
        }
        for count, fixture in fixtures.items():
            preview = _checked_probe(pool, database, prefix, fixture, f"current-{count}")
            assert preview["complete_members"] == count * 78
            assert preview["historical_queries"] == 0
            assert preview["coverage_seconds"] < (1 if count == 9 else 5)
            cases.append(preview)
            persisted = _checked_probe(
                pool, database, prefix, fixture, f"persist-{count}", persist=True
            )
            assert persisted["pointer_advanced"]
            assert persisted["evidence_row_delta"] == [1, 0, 1]
            cases.append(persisted)
            replay = _checked_probe(
                pool, database, prefix, fixture, f"replay-{count}", persist=True
            )
            assert replay["audit_id"] == persisted["audit_id"]
            assert replay["evidence_row_delta"] == [0, 0, 0]
            assert not replay["pointer_advanced"]
            cases.append(replay)
        writer_fixture = _seed_session(pool, date(2026, 10, 15), ("WRITER",))
        contention = _contention(database, prefix, settings, fixtures[400], writer_fixture)
        revised_at = _revise_all_heads(pool, fixtures[400])
        historical = _checked_probe(
            pool, database, prefix, fixtures[400], "historical-400-all-heads-revised"
        )
        assert historical["input_sha256"] == cases[3]["input_sha256"]
        assert historical["historical_queries"] == 1
        cases.append(historical)
        _fragment_heads(pool, fixtures[400])
        known_by = revised_at + timedelta(minutes=1)
        fragmented = _checked_probe(
            pool, database, prefix, fixtures[400], "fragmented-400", known_by=known_by
        )
        assert fragmented["missing_members"] == fragmented["missing_range_count"] == 15_600
        assert fragmented["missing_ranges_truncated"]
        assert fragmented["historical_queries"] == 0
        cases.append(fragmented)
        refused = _checked_probe(
            pool, database, prefix, fixtures[400], "fragmented-400-persist-refused",
            persist=True, refuse=True, known_by=known_by,
        )
        assert refused["outcome"] == "REFUSED_TRUNCATED_EVIDENCE"
        cases.append(refused)
        _fragment_heads(pool, fixtures[9])
        persisted = _checked_probe(
            pool, database, prefix, fixtures[9], "fragmented-9-persist", persist=True
        )
        assert persisted["missing_range_count"] == 351
        assert persisted["evidence_row_delta"] == [1, 351, 0]
        assert persisted["current_status"] == "COMPLETE"
        cases.append(persisted)
        replay = _checked_probe(
            pool, database, prefix, fixtures[9], "fragmented-9-replay", persist=True
        )
        assert replay["audit_id"] == persisted["audit_id"]
        assert replay["evidence_row_delta"] == [0, 0, 0]
        cases.append(replay)
        assert _snapshot(pool)[3:5] == (0, 0)
    report = {
        "version": "isolated-session-close-acceptance-v1",
        "verdict": "ISOLATED_CHECKS_PASSED_NOT_ZERO_OVERHEAD_CERTIFICATION",
        "cases": cases, "contention": contention, "cleanup_completed": True,
        "limitations": [
            "Fresh Python processes against a seeded, warm-cache disposable database.",
            "Same audited runtime provider, coverage and persistence functions; not the production CLI front door.",
            "Production target gates unchanged. NOLOGIN roles simulated with SET LOCAL ROLE.",
            "No live HTTP API, market-ingestion lag, retention operation or cadence enabled.",
            "Peak working set includes fresh-process imports; allocator/DB-server peaks are not isolated.",
            "Contention is one before/during/after run on a shared machine, not a causal latency certificate.",
        ],
    }
    destination = Path(os.environ.get(
        "STOCKS_CLOSE_ACCEPTANCE_REPORT", str(tmp_path / "close-acceptance.json")
    ))
    assert not destination.exists(), "refusing to overwrite acceptance evidence"
    destination.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    for case in cases:
        print(
            f"{case['scenario']}: wall={case['invocation_wall_seconds']:.3f}s "
            f"coverage={case['coverage_seconds']:.3f}s "
            f"peak={case['peak_working_set_bytes'] / 2**20:.1f}MiB "
            f"historical={case['historical_queries']} "
            f"evidence_delta={case['evidence_row_delta']}"
        )
    print("CONTENTION " + json.dumps(contention, sort_keys=True))
    print(f"ACCEPTANCE_REPORT {destination}")
