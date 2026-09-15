import os
from contextlib import nullcontext
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pytest
from dotenv import load_dotenv


@pytest.fixture
def revision_cursor():
    if os.getenv("TEST_UNIVERSE_REVISION_DATABASE") != "1":
        pytest.skip("explicit rollback-only PostgreSQL migration validation")
    backend = Path(__file__).resolve().parent.parent
    load_dotenv(backend / ".env")
    connection = psycopg2.connect(
        dbname=os.environ["DB_NAME"], user=os.environ["POSTGRES_ADMIN_USER"],
        password=os.environ["POSTGRES_ADMIN_PASSWORD"], host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
    )
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute((backend / "migrations" / "039_equity_universe_revisions.sql").read_text())
            cursor.execute((backend / "migrations" / "040_equity_action_coverage_responses.sql").read_text())
            yield cursor
    finally:
        connection.rollback()
        connection.close()


def insert_run(cursor, parent=None, **overrides):
    observed = datetime.now(timezone.utc) - timedelta(days=2)
    values = dict(
        universe_run_id=str(uuid4()), source="REVISION_CONTRACT_TEST", mode="REPLAY",
        effective_from=datetime(2024, 3, 4, tzinfo=timezone.utc), observed_at=observed,
        expected_members=0, admitted_members=0, status="COMPLETE", policy_version="revision_contract_test",
        policy_sha256="a" * 64, availability_mode="HISTORICAL_RECONSTRUCTED",
        replay_available_at=datetime(2024, 3, 4, 21, tzinfo=timezone.utc),
    )
    if parent:
        values.update(supersedes_universe_run_id=parent,
                      revision_published_at=datetime.now(timezone.utc) - timedelta(seconds=1),
                      revision_sha256="b" * 64, revision_reason="contract test")
    values.update(overrides)
    cursor.execute(sql.SQL("INSERT INTO public.equity_universe_runs ({}) VALUES ({})").format(
        sql.SQL(",").join(map(sql.Identifier, values)), sql.SQL(",").join(sql.Placeholder() for _ in values),
    ), tuple(values.values()))
    return values["universe_run_id"]


def insert_member(cursor, run_id):
    cursor.execute(
        """INSERT INTO public.equity_universe_members
           (universe_run_id, security_id, ticker, member_rank, effective_from, first_observed_at)
           VALUES (%s,%s,'TEST',1,'2024-03-04T00:00:00Z',NOW())""", (run_id, str(uuid4())),
    )


def test_complete_revision_is_separate_from_original_view(revision_cursor):
    cursor = revision_cursor
    original = insert_run(cursor)
    corrected = insert_run(cursor, original, expected_members=1, admitted_members=1)
    insert_member(cursor, corrected)
    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    cursor.execute("SELECT universe_run_id FROM public.equity_original_universe_runs WHERE universe_run_id = ANY(%s::uuid[])", ([original, corrected],))
    assert [str(row["universe_run_id"]) for row in cursor.fetchall()] == [original]


def test_incomplete_revision_cannot_commit(revision_cursor):
    original = insert_run(revision_cursor)
    insert_run(revision_cursor, original, expected_members=1, admitted_members=1)
    with pytest.raises(psycopg2.Error, match="membership is incomplete"):
        revision_cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")


@pytest.mark.parametrize("changes", [
    {"policy_sha256": "c" * 64}, {"policy_version": "other"},
    {"effective_from": datetime(2024, 3, 5, tzinfo=timezone.utc)},
    {"replay_available_at": datetime(2024, 3, 5, tzinfo=timezone.utc)},
    {"revision_published_at": datetime(2024, 3, 5, tzinfo=timezone.utc)},
])
def test_invalid_parent_contract_is_rejected(revision_cursor, changes):
    original = insert_run(revision_cursor)
    with pytest.raises(psycopg2.Error):
        insert_run(revision_cursor, original, **changes)


def test_revision_chain_cannot_branch(revision_cursor):
    original = insert_run(revision_cursor)
    insert_run(revision_cursor, original)
    with pytest.raises(psycopg2.Error, match="uq_equity_universe_revision_successor"):
        insert_run(revision_cursor, original)


@pytest.mark.parametrize("target, operation", [("original", "UPDATE"), ("corrected", "UPDATE"), ("original", "DELETE"), ("corrected", "DELETE")])
def test_published_lineage_cannot_be_changed(revision_cursor, target, operation):
    original = insert_run(revision_cursor)
    corrected = insert_run(revision_cursor, original)
    run_id = original if target == "original" else corrected
    statement = "UPDATE public.equity_universe_runs SET admitted_members=2 WHERE universe_run_id=%s" if operation == "UPDATE" else "DELETE FROM public.equity_universe_runs WHERE universe_run_id=%s"
    with pytest.raises(psycopg2.Error, match="lineage is immutable"):
        revision_cursor.execute(statement, (run_id,))


def test_members_cannot_be_appended_to_superseded_original(revision_cursor):
    original = insert_run(revision_cursor)
    insert_run(revision_cursor, original)
    with pytest.raises(psycopg2.Error, match="members are immutable"):
        insert_member(revision_cursor, original)


def test_original_cannot_be_converted_to_revision_in_place(revision_cursor):
    parent = insert_run(revision_cursor)
    original = insert_run(revision_cursor)
    with pytest.raises(psycopg2.Error, match="never converted in place"):
        revision_cursor.execute("UPDATE public.equity_universe_runs SET supersedes_universe_run_id=%s WHERE universe_run_id=%s", (parent, original))


def test_member_cannot_be_moved_into_a_published_revision(revision_cursor):
    original = insert_run(revision_cursor)
    corrected = insert_run(revision_cursor, original)
    unrelated = insert_run(revision_cursor)
    insert_member(revision_cursor, unrelated)
    with pytest.raises(psycopg2.Error, match="members are immutable"):
        revision_cursor.execute("UPDATE public.equity_universe_members SET universe_run_id=%s WHERE universe_run_id=%s", (corrected, unrelated))


@pytest.mark.parametrize("target", ["original", "corrected"])
def test_member_updates_cannot_change_published_lineage(revision_cursor, target):
    original = insert_run(revision_cursor, expected_members=1, admitted_members=1)
    insert_member(revision_cursor, original)
    corrected = insert_run(revision_cursor, original, expected_members=1, admitted_members=1)
    insert_member(revision_cursor, corrected)
    with pytest.raises(psycopg2.Error, match="members are immutable"):
        revision_cursor.execute("UPDATE public.equity_universe_members SET ticker='OTHER' WHERE universe_run_id=%s", (original if target == "original" else corrected,))


def response_coverage_fixture(cursor, *, empty=False):
    from equity.domain import BarAvailabilityMode
    from equity.polygon import normalize_corporate_actions
    from equity.repositories import EquityCorporateActionRepository
    from scripts.run_corporate_action_worker import build_coverage

    observed = datetime.now(timezone.utc)
    identity = uuid4()
    responses = {"SPLIT": [], "DIVIDEND": [] if empty else [{
        "id": str(uuid4()), "ticker": "TEST_RESPONSE", "ex_dividend_date": "2024-03-01", "cash_amount": 0.3,
    }]}
    coverage = build_coverage({"TEST_RESPONSE": identity}, start=datetime(2024, 3, 1).date(),
                              end=datetime(2024, 3, 1).date(), observed_at=observed,
                              availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED, responses=responses)
    actions = normalize_corporate_actions(responses["DIVIDEND"], security_ids={"TEST_RESPONSE": identity},
                                          action_type="DIVIDEND", observed_at=observed,
                                          availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED)
    repository = EquityCorporateActionRepository()
    repository._cursor = lambda: nullcontext(cursor)
    repository.persist(actions)
    return repository, coverage, actions


@pytest.mark.parametrize("empty", [True, False])
def test_response_coverage_repository_persists_complete_and_empty_sets(revision_cursor, empty):
    repository, coverage, actions = response_coverage_fixture(revision_cursor, empty=empty)
    assert repository.persist_coverage(coverage, actions) == 2
    revision_cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert repository.persist_coverage(coverage, actions) == 0


def test_incomplete_response_bound_coverage_cannot_commit(revision_cursor):
    _, coverage, _ = response_coverage_fixture(revision_cursor)
    row = coverage[1]
    revision_cursor.execute(
        """INSERT INTO equity_corporate_action_coverage
           (coverage_id,ticker,action_type,window_start,window_end,source,source_key,first_observed_at,
            availability_mode,replay_available_at,payload_sha256,security_id,response_action_count,response_sha256)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        tuple(asdict(row)[key] for key in ("coverage_id", "ticker", "action_type", "window_start", "window_end",
                                         "source", "source_key", "first_observed_at", "availability_mode",
                                         "replay_available_at", "payload_sha256", "security_id", "response_action_count", "response_sha256")),
    )
    with pytest.raises(psycopg2.Error, match="incomplete or mismatched"):
        revision_cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_response_coverage_writer_rejects_normalization_loss(revision_cursor):
    repository, coverage, _ = response_coverage_fixture(revision_cursor)
    with pytest.raises(ValueError, match="response membership"):
        repository.persist_coverage(coverage, ())


def test_response_coverage_writer_detects_stored_action_identity_collision(revision_cursor):
    repository, coverage, actions = response_coverage_fixture(revision_cursor)
    revision_cursor.execute("UPDATE equity_corporate_actions SET security_id=%s WHERE corporate_action_id=%s", (str(uuid4()), actions[0].corporate_action_id))
    with pytest.raises(ValueError, match="security identity conflict"):
        repository.persist_coverage(coverage, actions)


@pytest.mark.parametrize("target", ["coverage", "member", "action"])
def test_response_bound_evidence_cannot_be_modified(revision_cursor, target):
    repository, coverage, actions = response_coverage_fixture(revision_cursor)
    repository.persist_coverage(coverage, actions)
    statement = ("UPDATE equity_corporate_action_coverage SET response_action_count=0 WHERE coverage_id=%s"
                 if target == "coverage" else "DELETE FROM equity_corporate_action_coverage_members WHERE coverage_id=%s")
    if target == "action":
        statement = "UPDATE equity_corporate_actions SET cash_amount=99 WHERE corporate_action_id=%s"
    with pytest.raises(psycopg2.Error, match="immutable"):
        revision_cursor.execute(statement, (actions[0].corporate_action_id if target == "action" else coverage[1].coverage_id,))


@pytest.mark.parametrize("empty", [True, False])
def test_response_bound_action_manifest_rereads_exact_evidence(revision_cursor, empty):
    from equity.historical_actions import read_historical_actions, reread_historical_actions

    repository, coverage, actions = response_coverage_fixture(revision_cursor, empty=empty)
    repository.persist_coverage(coverage, actions)
    scope = {key: asdict(coverage[1])[key] for key in ("ticker", "security_id", "window_start", "window_end")}
    result = read_historical_actions(revision_cursor, scopes=[scope], action_types=["SPLIT", "DIVIDEND"],
                                      source_cutoff=datetime.now(timezone.utc), identity_evidence_sha256="c" * 64)
    assert reread_historical_actions(revision_cursor, result.manifest).manifest == result.manifest
    assert sum(len(rows) for rows in result.actions_by_session_security.values()) == (0 if empty else 1)