"""V2-10 reference-copy plan: PLAN mode only (offline invariants plus a DB variant on the V2 catalog)."""
from __future__ import annotations

import importlib
import json

import pytest

from realtime.reference_copy_plan import (
    V2_SEEDED_REFERENCE, build_reference_copy_plan, foreign_key_order, reference_copy_scope, relation_statements,
)
from v2_database import db_tests_enabled, disposable_v2_database

DESIGN_REFERENCE_TABLES = {
    "ref.securities", "ref.security_revisions", "ref.option_contracts", "ref.option_contract_versions",
    "ref.corporate_action_revisions", "ref.corporate_action_coverage", "ref.oi_baselines",
    "ref.market_event_revisions", "ref.market_event_coverage", "ref.risk_free_rate_observations",
    "ref.fundamental_reports", "ref.fundamental_report_coverage",
}


def test_scope_is_the_designs_twelve_reference_tables_plus_instrument_identities():
    scope = reference_copy_scope()
    assert set(scope) == DESIGN_REFERENCE_TABLES | {"rt.instruments"}
    assert not set(scope) & V2_SEEDED_REFERENCE
    assert all("stocks_realtime_v1" not in name for name in scope)


def test_copy_order_puts_parents_first_ignores_outside_parents_and_rejects_cycles():
    parents = {"child": {"parent", "outside", "child"}, "parent": set(), "grandchild": {"child"}}
    assert foreign_key_order(["grandchild", "child", "parent"], parents) == ("parent", "child", "grandchild")
    with pytest.raises(ValueError, match="foreign-key cycle"):
        foreign_key_order(["a", "b"], {"a": {"b"}, "b": {"a"}})


def test_statements_read_the_source_select_only_and_never_interpolate_values():
    statements = relation_statements("ref.securities", ["security_id", "created_at"], ["security_id"])
    assert statements["source_select"] == 'SELECT "security_id", "created_at" FROM "ref"."securities" ORDER BY "security_id"'
    assert statements["target_insert"].endswith('VALUES (%s, %s)')
    assert "INSERT" not in statements["source_select"] and "DEFAULT" not in statements["target_insert"]
    assert "md5(" in statements["checksum"] and "ORDER BY" in statements["checksum"]
    with pytest.raises(ValueError):
        relation_statements("ref.securities", ["security_id"], [])


def test_the_entry_script_refuses_execution_and_non_disposable_targets():
    script = importlib.import_module("scripts.plan_reference_copy")
    base = ["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x"]
    assert script.parse_args(base).database == "test_realtime_v2_x"
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--execute"])
    with pytest.raises(ValueError):
        script.parse_args(["--database", "stocks_realtime_v1", "--confirm-database-name", "stocks_realtime_v1"])
    with pytest.raises(ValueError):
        script.parse_args(["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2"])


@pytest.fixture(scope="module")
def plan():
    if not db_tests_enabled():
        pytest.skip("DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1, no offline I/O guard)")
    with disposable_v2_database("test_realtime_v2_c1") as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                result = build_reference_copy_plan(cursor)
                statements = {row["relation"]: row["statements"] for row in result["relations"]}
                outcomes = {}
                for relation, entry in statements.items():
                    cursor.execute(entry["checksum"])
                    outcomes[relation] = cursor.fetchone()
                    cursor.execute(entry["target_empty_check"])
                    outcomes[relation] += (cursor.fetchone()[0],)
        finally:
            connection.rollback()
            pool.putconn(connection)
        yield result, outcomes


def test_the_plan_orders_every_relation_after_its_parents_and_carries_all_columns(plan):
    result, _ = plan
    json.dumps(result)
    names = [row["relation"] for row in result["relations"]]
    assert set(names) == DESIGN_REFERENCE_TABLES | {"rt.instruments"}
    assert result["executor"] == "reference_copy_executor_v1"
    assert names.index("ref.securities") < names.index("ref.security_revisions")
    for row in result["relations"]:
        assert row["columns"] and row["primary_key"] and set(row["primary_key"]) <= set(row["columns"])
        assert all(names.index(parent) < names.index(row["relation"]) for parent in row["parents"])
        assert isinstance(row["self_referencing"], bool)
        assert isinstance(row["self_foreign_keys"], list)
    timestamp_columns = [column for row in result["relations"] for column in row["columns"]
                         if column.endswith("_at")]
    assert timestamp_columns, "reference rows keep their original timestamps"


def test_the_instrument_sequence_is_advanced_past_the_copied_maximum(plan):
    result, _ = plan
    (sequence,) = [row for row in result["sequences"] if row["relation"] == "rt.instruments"]
    assert sequence["sequence"] == "rt.instruments_instrument_id_seq" and sequence["column"] == "instrument_id"
    assert sequence["setval"].startswith("SELECT setval('rt.instruments_instrument_id_seq'")
    assert "COALESCE(max(" in sequence["setval"] and sequence["setval"].endswith("true)")


def test_the_parity_statements_run_on_the_empty_baseline_tables(plan):
    _, outcomes = plan
    assert all(count == 0 and checksum == "d41d8cd98f00b204e9800998ecf8427e" and empty is True
               for count, checksum, empty in outcomes.values())
