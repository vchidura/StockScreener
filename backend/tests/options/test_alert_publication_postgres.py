from __future__ import annotations

import copy
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2 import sql

from test_execution_gates import retained_wheel_detail
from options.alert_plans import freeze_indicative_alert_plan, _canonical
from options.repositories.alert_publications import (
    AlertPublicationEvent, OptionAlertPublicationRepository, publication_exposure_key,
)


BACKEND_DIR = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(os.getenv("OPTION_ALERT_POSTGRES_TESTS") != "1", reason="explicit isolated PostgreSQL integration opt-in required")


def connection(*, admin=False, schema="public"):
    load_dotenv(BACKEND_DIR / ".env")
    user_key, password_key = ("POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD") if admin else ("DB_USER", "DB_PASSWORD")
    if not all(os.getenv(key) for key in ("DB_NAME", "DB_HOST", user_key, password_key)):
        raise RuntimeError("configured PostgreSQL credentials are required for isolated tests")
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], host=os.environ["DB_HOST"], port=os.getenv("DB_PORT", "5432"),
        user=os.environ[user_key], password=os.environ[password_key], connect_timeout=10,
        application_name="option-alert-isolated-integration",
        options=f"-c search_path={schema} -c timezone=UTC -c statement_timeout=15000 -c lock_timeout=10000",
    )


@pytest.fixture(scope="module")
def isolated_schema():
    schema = "option_alert_test_" + uuid4().hex
    migration = (BACKEND_DIR / "migrations" / "043_option_alert_publications.sql").read_text(encoding="utf-8")
    isolated_migration = migration.replace("public.", f'"{schema}".')
    with closing(connection(admin=True)) as admin:
        try:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
                cursor.execute("SELECT current_schema()")
                assert cursor.fetchone()[0] == schema
                cursor.execute(sql.SQL("""CREATE TABLE {}.option_analysis_runs (matrix_id uuid PRIMARY KEY,
                    batch_id uuid,underlying text,market_time timestamptz,observed_time timestamptz,
                    status text,policy_sha256 char(64),completed_at timestamptz,created_at timestamptz)""").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""CREATE TABLE {}.option_strategy_candidates (
                    candidate_id uuid PRIMARY KEY, matrix_id uuid, candidate_identity char(64),
                    observed_time timestamptz, valid_until timestamptz,policy_sha256 char(64),status text)""").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""CREATE TABLE {}.option_ingestion_runs (batch_id uuid PRIMARY KEY,
                    scheduled_cycle timestamptz,configuration_sha256 char(64),policy_sha256 char(64),status text,completed_at timestamptz)""").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""CREATE TABLE {}.option_work_items (subject_id text,stage text,
                    business_key text,status text,completed_at timestamptz)""").format(sql.Identifier(schema)))
                board = (BACKEND_DIR / "migrations/026_option_board_publications.sql").read_text(encoding="utf-8")
                cursor.execute(board)
                cursor.execute(isolated_migration)
                cursor.execute(isolated_migration)
                evaluations = (BACKEND_DIR / "migrations" / "053_option_detector_evaluations.sql").read_text(encoding="utf-8")
                evaluations = evaluations.replace("public.", f'"{schema}".')
                cursor.execute(evaluations)
                cursor.execute(evaluations)
                for statement in (
                    "GRANT USAGE ON SCHEMA {} TO {}",
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}",
                    "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}",
                    "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {} TO {}",
                ):
                    cursor.execute(sql.SQL(statement).format(sql.Identifier(schema), sql.Identifier(os.environ["DB_USER"])))
            admin.commit()
            yield schema
        finally:
            admin.rollback()
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            admin.commit()
            with admin.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)", (schema,))
                assert cursor.fetchone()[0] is False


def repository(schema):
    @contextmanager
    def factory():
        with closing(connection(schema=schema)) as current:
            yield current
    return OptionAlertPublicationRepository(factory)


def evaluation_repository(schema):
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    @contextmanager
    def factory():
        with closing(connection(schema=schema)) as current:
            yield current
    return OptionAlertEvaluationRepository(factory)


def make_evaluation_records(schema, model="O1", *, technical=False):
    from options.analytics.alert_selection import build_selection_evidence, build_surface_evidence, select_dual_origin_packages
    from options.dual_origin import qualify_dual_origin_package
    from options.surface_detection import assess_surface_first
    from test_dual_origin import qualification_inputs, surface_inputs, technical_qualification_inputs

    dataset = "isolated-" + uuid4().hex
    if model == "O2":
        source, stock, inputs = surface_inputs()
        observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
        records = build_surface_evidence([observation], dataset_id=dataset, selected_at=inputs["decision_at"])
        candidate = None
    else:
        inputs = technical_qualification_inputs(model=model) if technical else qualification_inputs(model=model)
        package, plan = qualify_dual_origin_package(**inputs)
        selection = select_dual_origin_packages([package], {}, decision_at=package.decision_at,
            scheduled_cycle=package.scheduled_cycle, expected_underlyers=("AAPL",), completed_matrices={"AAPL": package.matrix_id})
        records = build_selection_evidence(selection, {plan.sha256: plan}, dataset_id=dataset, selected_at=package.decision_at)
        candidate = inputs["candidate"]
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("INSERT INTO option_analysis_runs (matrix_id) VALUES (%s)", (records[0].matrix_id,))
            if candidate is not None:
                cursor.execute("""INSERT INTO option_strategy_candidates
                    (candidate_id,matrix_id,candidate_identity,observed_time,valid_until) VALUES (%s,%s,%s,%s,%s)""",
                    (candidate.candidate_id, candidate.matrix_id, candidate.identity_sha256, candidate.observed_time, candidate.valid_until))
        current.commit()
    return records


def seed_completed_run(schema, run):
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            for symbol, matrix_id in run.source_matrices:
                batch_id = uuid4()
                cursor.execute("""INSERT INTO option_ingestion_runs VALUES (%s,%s,%s,%s,'COMPLETE',%s)""",
                    (batch_id, run.scheduled_cycle, run.configuration_sha256, run.market_policy_sha256, run.selected_at))
                cursor.execute("""INSERT INTO option_analysis_runs
                    (matrix_id,batch_id,underlying,market_time,observed_time,status,policy_sha256,completed_at,created_at)
                    VALUES (%s,%s,%s,%s,%s,'COMPLETE',%s,%s,%s) ON CONFLICT(matrix_id) DO UPDATE SET
                    batch_id=EXCLUDED.batch_id,underlying=EXCLUDED.underlying,market_time=EXCLUDED.market_time,
                    observed_time=EXCLUDED.observed_time,status=EXCLUDED.status,policy_sha256=EXCLUDED.policy_sha256,
                    completed_at=EXCLUDED.completed_at,created_at=EXCLUDED.created_at""",
                    (matrix_id, batch_id, symbol, run.market_time, run.observed_time, run.market_policy_sha256, run.selected_at, run.selected_at))
                cursor.execute("INSERT INTO option_work_items VALUES (%s,'STRATEGY',%s,'COMPLETED',%s)",
                    (str(matrix_id), f"strategy:{matrix_id}:{run.strategy_version}", run.selected_at))
            cursor.execute("UPDATE option_strategy_candidates SET policy_sha256=%s,status='SELECTED' WHERE matrix_id=ANY(%s::uuid[])",
                (run.strategy_policy_sha256, [matrix for _, matrix in run.source_matrices]))
        current.commit()


@pytest.mark.parametrize("forward", [False, True])
def test_detector_evaluation_zero_run_roundtrip_and_expired_batch_rollback(isolated_schema, forward):
    from options.analytics.alert_selection import build_detector_run
    from test_dual_origin import detector_run_inputs

    inputs = detector_run_inputs()
    if forward:
        inputs["configuration"].strategy_policy.forward_admission = True
        inputs["matrices"][0]["market_time"] = inputs["scheduled_cycle"] + timedelta(minutes=5)
    inputs["dataset_id"] = "isolated-zero-" + uuid4().hex
    run = build_detector_run(**inputs)
    seed_completed_run(isolated_schema, run)
    repo = evaluation_repository(isolated_schema)
    assert repo.persist_completed_run(run, ())["status"] == "RECORDED"
    assert repo.persist_completed_run(run, ())["status"] == "ALREADY_RECORDED"
    assert repo.completed_runs(dataset_id=run.dataset_id, as_of=datetime.now(timezone.utc)) == (run,)
    assert repo.completed_run(dataset_id=run.dataset_id, scheduled_cycle=run.scheduled_cycle,
        as_of=datetime.now(timezone.utc)) == (run, ())
    from options.analytics.behavior_review import build_detector_evaluation_review
    review = build_detector_evaluation_review(repository=repo, dataset_id=run.dataset_id, as_of=datetime.now(timezone.utc))
    assert review["storage_ready"] and review["total"] == 0
    assert review["session_date"] == run.scheduled_cycle.date().isoformat()
    assert review["completed_runs"][0]["selection_counts"]["SELECTED"] == 0
    records = make_evaluation_records(isolated_schema)
    expired = build_detector_run(**detector_run_inputs(records))
    seed_completed_run(isolated_schema, expired)
    with pytest.raises(ValueError, match="elapsed during"):
        repo.persist_completed_run(expired, records)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM option_board_publications WHERE publication_id=%s", (expired.run_id,))
            assert cursor.fetchone()[0] == 0
            cursor.execute("SELECT COUNT(*) FROM option_detector_evaluations WHERE run_id=%s", (expired.run_id,))
            assert cursor.fetchone()[0] == 0


@pytest.mark.parametrize("model", ["O1", "O2", "S1", "S2"])
def test_detector_evaluation_postgres_roundtrip_and_immutability(isolated_schema, model):
    records = make_evaluation_records(isolated_schema, model)
    repo = evaluation_repository(isolated_schema)
    assert repo.persist_run(records) == 1
    assert repo.persist_run(records) == 0
    record = records[0]
    result = repo.review_inputs(as_of=datetime.now(timezone.utc), dataset_id=record.dataset_id)
    assert result["ready"] and result["records"] == list(records)
    with closing(connection(schema=isolated_schema)) as current:
        for query in ("UPDATE option_detector_evaluations SET selected_at=selected_at WHERE evaluation_id=%s",
                      "DELETE FROM option_detector_evaluations WHERE evaluation_id=%s"):
            with pytest.raises(psycopg2.Error, match="immutable"):
                with current.cursor() as cursor:
                    cursor.execute(query, (record.evaluation_id,))
            current.rollback()
    with closing(connection(admin=True, schema=isolated_schema)) as current:
        with pytest.raises(psycopg2.Error, match="immutable"):
            with current.cursor() as cursor:
                cursor.execute("TRUNCATE option_detector_evaluations")
        current.rollback()


@pytest.mark.parametrize("model", ["O1", "S1", "S2"])
def test_detector_evaluation_technical_plans_roundtrip_without_schema_changes(isolated_schema, model):
    records = make_evaluation_records(isolated_schema, model, technical=True)
    repo = evaluation_repository(isolated_schema)
    assert repo.persist_run(records) == 1
    assert repo.persist_run(records) == 0
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            cutoff = cursor.fetchone()[0]
    result = repo.review_inputs(dataset_id=records[0].dataset_id, as_of=cutoff)
    assert result["records"] == list(records)
    assert records[0].package.schema_version == "dual_origin_qualified_package_v3"
    assert json.loads(records[0].plan_payload_text)["version"] == "dual_origin_indicative_plan_v2"


def test_detector_evaluation_complete_observation_concurrency_and_scope(isolated_schema):
    from options.analytics.alert_selection import DetectorRunEvidence, build_detector_run
    from test_dual_origin import detector_run_inputs

    records = make_evaluation_records(isolated_schema, "O2")
    run = build_detector_run(**detector_run_inputs(records))
    seed_completed_run(isolated_schema, run)
    barrier = Barrier(2)

    def persist():
        barrier.wait(timeout=5)
        return evaluation_repository(isolated_schema).persist_completed_run(run, records)["status"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(persist) for _ in range(2)]
        assert sorted(future.result(timeout=10) for future in futures) == ["ALREADY_RECORDED", "RECORDED"]
    repo = evaluation_repository(isolated_schema)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            cutoff = cursor.fetchone()[0]
    review = repo.review_inputs(dataset_id=run.dataset_id, as_of=cutoff)
    assert review["records"] == list(records) and review["runs"] == [run]
    assert repo.completed_run(dataset_id=run.dataset_id, scheduled_cycle=run.scheduled_cycle, as_of=cutoff) == (run, records)
    assert repo.completed_runs(dataset_id=run.dataset_id, as_of=run.selected_at) == ()
    changed = DetectorRunEvidence.model_validate({**run.model_dump(), "rejections": (("DIFFERENT", 1),)})
    with pytest.raises(ValueError, match="retry differs"):
        repo.persist_completed_run(changed, records)
    for changes in (
        dict(scheduled_cycle=run.scheduled_cycle - timedelta(days=1), market_time=run.market_time - timedelta(days=1)),
        dict(scheduled_cycle=run.scheduled_cycle + timedelta(minutes=15), selected_at=run.selected_at + timedelta(minutes=15),
            configuration_sha256="d" * 64),
        dict(scheduled_cycle=run.scheduled_cycle + timedelta(minutes=15), selected_at=run.selected_at + timedelta(minutes=15)),
    ):
        changed = DetectorRunEvidence.model_validate({**run.model_dump(), **changes, "record_sha256s": (),
            "selection_counts": tuple((status, 0) for status, _ in run.selection_counts)})
        with pytest.raises(ValueError, match="scope|out-of-order|predates"):
            repo.persist_completed_run(changed, ())


def test_detector_evaluation_reader_counts_only_completed_repeat_hits(isolated_schema):
    from psycopg2.extras import Json
    from uuid import NAMESPACE_URL, uuid5
    from options.analytics.alert_selection import DetectorSelectionEvidence, build_detector_run
    from options.analytics.behavior_review import build_detector_alert_review
    from test_dual_origin import detector_run_inputs

    original = make_evaluation_records(isolated_schema)[0]
    repo = evaluation_repository(isolated_schema)
    retained = []
    for index, status in enumerate(("SELECTED", "REPEAT", "NOT_SELECTED", "REPEAT")):
        cycle = original.scheduled_cycle + timedelta(seconds=index)
        package = type(original.package).model_validate({**original.package.model_dump(), "scheduled_cycle": cycle})
        record = DetectorSelectionEvidence.model_validate({**original.model_dump(), "package": package,
            "run_id": uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{original.dataset_id}:{cycle.isoformat()}"),
            "selected_at": original.selected_at + timedelta(seconds=index), "selection_status": status,
            "selection_reason": {"SELECTED": "WITHIN_RUN_BUDGET", "REPEAT": "PRIOR_ALERT", "NOT_SELECTED": "RUN_CAP"}[status],
            "first_candidate_id": original.candidate_id if status == "REPEAT" else None})
        repo.persist_run((record,))
        if index == 3:
            continue
        run = build_detector_run(**detector_run_inputs((record,)))
        retained.append(run)
        with closing(connection(schema=isolated_schema)) as current:
            with current.cursor() as cursor:
                cursor.execute("""INSERT INTO option_board_publications (
                    publication_id,scheduled_cycle,as_of_session,status,selector_version,selector_sha256,
                    strategy_policy_sha256,configuration_sha256,expected_underlying_count,covered_underlying_count,
                    source_matrix_ids,market_data_time,observed_time,selection_evidence,published_at)
                    VALUES (%s,%s,%s,'COMPLETE','option_detector_run_v1',%s,%s,%s,1,1,%s,%s,%s,%s,%s)""",
                    (run.run_id, run.scheduled_cycle, run.scheduled_cycle.date(), run.scope_sha256,
                     run.strategy_policy_sha256, run.configuration_sha256, [record.matrix_id],
                     run.market_time, run.observed_time, Json(dict(kind="DUAL_ORIGIN_COMPLETE_RUN", dataset_id=run.dataset_id,
                        payload_text=run.canonical_json(), payload_sha256=run.sha256)), run.selected_at))
            current.commit()
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            cutoff = cursor.fetchone()[0]
    counts = repo.repeat_counts(dataset_id=original.dataset_id, records=(original,), as_of=cutoff)
    assert counts[original.evaluation_id]["repeats"] == 1
    assert counts[original.evaluation_id]["last_seen_at"] == original.selected_at + timedelta(seconds=1)
    latest = build_detector_alert_review(dataset_id=original.dataset_id, as_of=cutoff, repository=repo)
    assert latest["rows"] == [] and latest["new_alerts"] == 0
    history = build_detector_alert_review(dataset_id=original.dataset_id, scope="HISTORY", as_of=cutoff, repository=repo)
    assert history["total"] == 1 and history["rows"][0]["hit_count"] == 2
    assert history["rows"][0]["plan_sha256"] == original.package.plan_sha256


def test_detector_evaluation_concurrent_retry_records_one_batch(isolated_schema):
    records = make_evaluation_records(isolated_schema)
    barrier = Barrier(2)

    def persist():
        barrier.wait(timeout=5)
        return evaluation_repository(isolated_schema).persist_run(records)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(persist) for _ in range(2)]
        assert sorted(future.result(timeout=10) for future in futures) == [0, 1]


@pytest.mark.parametrize("mutation", ["missing", "candidate", "clock", "hash", "observation_selected"])
def test_detector_evaluation_database_rejects_unbound_payload(isolated_schema, mutation):
    from psycopg2.extras import execute_values

    record = make_evaluation_records(isolated_schema, "O2" if mutation == "observation_selected" else "O1")[0]
    payload = json.loads(record.canonical_json())
    if mutation == "missing": payload.pop("selection_reason")
    if mutation == "candidate": payload["package"]["candidate_identity_sha256"] = "0" * 64
    if mutation == "clock": payload["package"]["entry_deadline"] = record.selected_at.isoformat()
    if mutation == "observation_selected": payload["selection_status"] = "SELECTED"
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = "0" * 64 if mutation == "hash" else hashlib.sha256(text.encode("ascii")).hexdigest()
    with closing(connection(schema=isolated_schema)) as current:
        with pytest.raises(psycopg2.Error):
            with current.cursor() as cursor:
                execute_values(cursor, """INSERT INTO option_detector_evaluations (
                    evaluation_id,dataset_id,run_id,scheduled_cycle,session_date,selector_sha256,
                    detector_id,candidate_id,matrix_id,recurrence_sha256,selection_status,selection_reason,
                    selected_at,payload_text,payload_sha256) VALUES %s""", [
                    (record.evaluation_id, record.dataset_id, record.run_id, record.scheduled_cycle,
                     record.scheduled_cycle.date(), record.selector_sha256, record.detector_id, record.candidate_id,
                     record.matrix_id, record.recurrence_sha256, payload["selection_status"], record.selection_reason,
                     record.selected_at, text, digest)])
        current.rollback()


def make_plan(schema, *, detail=None, entry_limit=Decimal("200")):
    if detail is None:
        detail = retained_wheel_detail()
        now = datetime.now(timezone.utc) - timedelta(seconds=2)
        detail["candidate"].update({"observed_time": now, "market_data_time": now - timedelta(minutes=15), "valid_until": now + timedelta(minutes=10)})
        for row in (*detail["legs"], *detail["source_snapshots"]):
            row.update({"contract_id": int(uuid4().hex[:7], 16), "expiration_date": (now + timedelta(days=30)).date().isoformat(), "source_market_time": now - timedelta(minutes=15)})
        detail["source_snapshots"][0]["contract_id"] = detail["legs"][0]["contract_id"]
        detail["source_snapshots"][0].update({"market_data_time": now - timedelta(minutes=15), "first_observed_at": now, "expiration_cutoff": now + timedelta(days=30)})
    plan = freeze_indicative_alert_plan(
        detail, decision_at=detail["candidate"]["observed_time"],
        entry_deadline=detail["candidate"]["valid_until"] - timedelta(minutes=1),
        exit_deadline=detail["candidate"]["observed_time"] + timedelta(days=2), entry_limit=entry_limit,
    )
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("INSERT INTO option_strategy_candidates(candidate_id) VALUES (%s) ON CONFLICT DO NOTHING", (detail["candidate"]["candidate_id"],))
        current.commit()
    return plan, detail


def rows_for(schema, plan_id):
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT event_type, recorded_at, sequence FROM option_alert_publication_events WHERE plan_id = %s ORDER BY sequence", (plan_id,))
            return cursor.fetchall()


def test_postgres_repository_commit_retry_and_lifecycle(isolated_schema):
    plan, _ = make_plan(isolated_schema)
    repo = repository(isolated_schema)
    published = repo.publish(plan, request_key="publish")
    retry = repo.publish(plan, request_key="publish")
    assert published["status"] == "RECORDED" and retry["status"] == "ALREADY_RECORDED"
    assert published["event_id"] == retry["event_id"] and published["recorded_at"] == retry["recorded_at"]
    source_at = datetime.now(timezone.utc)
    event = repo.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="source-1", source_ids=("snapshot:one",), source_available_at=source_at)
    retried = repo.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="source-1", source_ids=("snapshot:one",), source_available_at=source_at)
    assert event["event_id"] == retried["event_id"]
    with pytest.raises(ValueError, match="idempotency"):
        repo.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="source-1", source_ids=("snapshot:changed",), source_available_at=source_at)
    repo.append_event(plan.plan_id, AlertPublicationEvent.INVALIDATED, request_key="invalid", source_ids=("context:opposed",), source_available_at=datetime.now(timezone.utc), reason="THESIS_INVALIDATED")
    with pytest.raises(ValueError, match="terminal"):
        repo.append_event(plan.plan_id, AlertPublicationEvent.OBSERVED, request_key="reopen", source_ids=("snapshot:later",), source_available_at=datetime.now(timezone.utc))
    history = rows_for(isolated_schema, plan.plan_id)
    assert [row[0] for row in history] == ["PUBLISHED", "OBSERVED", "INVALIDATED"]
    assert [row[1] for row in history] == sorted(row[1] for row in history)
    assert len({row[2] for row in history}) == 3
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("SELECT payload_text, plan_sha256 FROM option_alert_plans WHERE plan_id=%s", (plan.plan_id,))
            assert cursor.fetchone() == (plan.payload_json, plan.sha256)


def test_postgres_concurrent_identical_retry_and_exposure_exclusion(isolated_schema):
    plan, detail = make_plan(isolated_schema)
    barrier = Barrier(2)
    def publish_once():
        barrier.wait(timeout=10)
        return repository(isolated_schema).publish(plan, request_key="concurrent")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish_once) for _ in range(2)]
        results = [future.result(timeout=20) for future in futures]
    assert sorted(row["status"] for row in results) == ["ALREADY_RECORDED", "RECORDED"]
    assert len(rows_for(isolated_schema, plan.plan_id)) == 1
    first, source = make_plan(isolated_schema)
    second, _ = make_plan(isolated_schema, detail=source, entry_limit=Decimal("201"))
    barrier = Barrier(2)
    def publish_exposure(candidate_plan):
        barrier.wait(timeout=10)
        try:
            return repository(isolated_schema).publish(candidate_plan, request_key="exposure")["status"]
        except ValueError as failure:
            assert "active publication" in str(failure)
            return "EXPOSURE_BLOCKED"
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish_exposure, candidate) for candidate in (first, second)]
        assert sorted(future.result(timeout=20) for future in futures) == ["EXPOSURE_BLOCKED", "RECORDED"]
    assert len(rows_for(isolated_schema, first.plan_id)) + len(rows_for(isolated_schema, second.plan_id)) == 1


def test_postgres_guards_mutation_and_database_clock(isolated_schema):
    plan, _ = make_plan(isolated_schema)
    repo = repository(isolated_schema)
    repo.publish(plan, request_key="publish")
    for statement in (
        "UPDATE option_alert_plans SET exposure_key=exposure_key WHERE plan_id=%s",
        "DELETE FROM option_alert_plans WHERE plan_id=%s",
        "UPDATE option_alert_publication_events SET request_key=request_key WHERE plan_id=%s",
        "DELETE FROM option_alert_publication_events WHERE plan_id=%s",
    ):
        with closing(connection(schema=isolated_schema)) as current:
            with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
                cursor.execute(statement, (plan.plan_id,))
            current.rollback()
    with closing(connection(admin=True, schema=isolated_schema)) as current:
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
            cursor.execute("TRUNCATE option_alert_publication_events")
        current.rollback()
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            request = repo._request(plan.plan_id, AlertPublicationEvent.OBSERVED, "database-clock", ("source:clock",), datetime.now(timezone.utc), None)
            before = datetime.now(timezone.utc)
            cursor.execute("INSERT INTO option_alert_publication_events(event_id,plan_id,event_type,request_key,request_text,recorded_at,sequence) VALUES (%s,%s,'OBSERVED','database-clock',%s,'2000-01-01',-99) RETURNING recorded_at,sequence", (uuid4(), plan.plan_id, request))
            stamp, sequence = cursor.fetchone()
            assert stamp >= before and sequence > 0
        current.commit()
    assert len(rows_for(isolated_schema, plan.plan_id)) == 2


def test_postgres_failed_event_rolls_back_new_plan(isolated_schema):
    plan, _ = make_plan(isolated_schema)
    payload = plan.to_dict()["plan"]
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute("INSERT INTO option_alert_plans(plan_id,candidate_id,exposure_key,plan_sha256,payload_text,decision_at,entry_deadline,exit_deadline) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                           (plan.plan_id, payload["candidate_id"], publication_exposure_key(payload), plan.sha256, plan.payload_json, payload["decision_at"], payload["entry_deadline"], payload["exit_deadline"]))
            request = OptionAlertPublicationRepository._request(plan.plan_id, AlertPublicationEvent.EXPIRED, "not-published", (), None, None)
            with pytest.raises(psycopg2.Error, match="published before"):
                cursor.execute("INSERT INTO option_alert_publication_events(event_id,plan_id,event_type,request_key,request_text) VALUES (%s,%s,'EXPIRED','not-published',%s)", (uuid4(), plan.plan_id, request))
        current.rollback()
        with current.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM option_alert_plans WHERE plan_id=%s", (plan.plan_id,))
            assert cursor.fetchone()[0] == 0


def test_postgres_rejects_expired_publish_and_payload_hash(isolated_schema):
    plan, _ = make_plan(isolated_schema)
    payload = copy.deepcopy(plan.to_dict()["plan"])
    now = datetime.now(timezone.utc)
    payload.update({"decision_at": (now - timedelta(minutes=3)).isoformat(), "entry_deadline": (now - timedelta(minutes=1)).isoformat()})
    serialized = _canonical(payload)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            values = (plan.plan_id, payload["candidate_id"], publication_exposure_key(payload), hashlib.sha256(serialized.encode("ascii")).hexdigest(), serialized, payload["decision_at"], payload["entry_deadline"], payload["exit_deadline"])
            cursor.execute("INSERT INTO option_alert_plans(plan_id,candidate_id,exposure_key,plan_sha256,payload_text,decision_at,entry_deadline,exit_deadline) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", values)
            request = OptionAlertPublicationRepository._request(plan.plan_id, AlertPublicationEvent.PUBLISHED, "expired", (), None, None)
            with pytest.raises(psycopg2.Error, match="unexpired"):
                cursor.execute("INSERT INTO option_alert_publication_events(event_id,plan_id,event_type,request_key,request_text) VALUES (%s,%s,'PUBLISHED','expired',%s)", (uuid4(), plan.plan_id, request))
        current.rollback()
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="check constraint"):
            cursor.execute("INSERT INTO option_alert_plans(plan_id,candidate_id,exposure_key,plan_sha256,payload_text,decision_at,entry_deadline,exit_deadline) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (*values[:3], "0" * 64, *values[4:]))
        current.rollback()