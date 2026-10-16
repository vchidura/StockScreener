"""V2-10 reference-copy plan (PLAN mode only; no executor exists).

Describes how the reference data of `stocks_realtime_v1` would be copied into `stocks_realtime_v2`
once that step is authorized: which relations, in which foreign-key order, with which columns (every
column listed explicitly, so original timestamps are copied verbatim and no DEFAULT such as
clock_timestamp() ever fires), which sequences to advance (`setval`) and the row-count plus checksum
statements that prove parity on both sides. The plan is derived from the V2 baseline catalog (a
disposable database provisioned from `sql_v2`); building it never connects to v1 or to
`stocks_realtime_v2`. The source side is read-only by construction: the plan carries SELECT statements
only, to be run in a READ ONLY transaction.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from .relation_registry import RELATIONS

PLAN_VERSION = "reference_copy_plan_v1"
# Reference relations the baseline seeds itself (synthetic or reviewed fixtures), never copied from v1.
V2_SEEDED_REFERENCE = frozenset({"ref.trade_condition_codes", "ref.exchange_codes", "ref.option_condition_semantics"})
# Control-plane identity the reference rows are keyed by; owns the only copied sequence.
COPIED_CONTROL_RELATIONS = ("rt.instruments",)
SOURCE_DATABASE = "stocks_realtime_v1"
TARGET_DATABASE = "stocks_realtime_v2"


def reference_copy_scope() -> tuple[str, ...]:
    """Relations copied from v1: the registry's REFERENCE-class `ref` tables the baseline does not seed,
    plus the instrument identities. Sorted for a stable plan; copy order is computed from the catalog."""
    copied = tuple(sorted(
        name for name, spec in RELATIONS.items()
        if name.startswith("ref.") and spec.retention == "REFERENCE" and name not in V2_SEEDED_REFERENCE))
    return (*copied, *COPIED_CONTROL_RELATIONS)


def foreign_key_order(relations: Sequence[str], parents: Mapping[str, set[str]]) -> tuple[str, ...]:
    """Parents before children (stable for ties); a cycle among the copied relations is an error."""
    remaining = {name: {parent for parent in parents.get(name, set()) if parent in relations and parent != name}
                 for name in relations}
    ordered: list[str] = []
    while remaining:
        ready = sorted(name for name, needs in remaining.items() if not needs)
        if not ready:
            raise ValueError("reference copy relations contain a foreign-key cycle: " + ", ".join(sorted(remaining)))
        for name in ready:
            ordered.append(name)
            del remaining[name]
        for needs in remaining.values():
            needs.difference_update(ready)
    return tuple(ordered)


def _quote(name: str) -> str:
    schema, table = name.split(".")
    return f'"{schema}"."{table}"'


def relation_statements(relation: str, columns: Sequence[str], primary_key: Sequence[str]) -> dict[str, str]:
    """The SELECT/INSERT/count/checksum statements of one relation (no value is ever interpolated)."""
    if not columns or not primary_key:
        raise ValueError(f"{relation} needs columns and a primary key for a deterministic copy")
    names = ", ".join(f'"{column}"' for column in columns)
    order = ", ".join(f'"{column}"' for column in primary_key)
    placeholders = ", ".join("%s" for _ in columns)
    return dict(
        source_select=f"SELECT {names} FROM {_quote(relation)} ORDER BY {order}",
        target_insert=f"INSERT INTO {_quote(relation)} ({names}) VALUES ({placeholders})",
        target_empty_check=f"SELECT NOT EXISTS (SELECT 1 FROM {_quote(relation)})",
        count=f"SELECT count(*) FROM {_quote(relation)}",
        checksum=(f"SELECT count(*), md5(COALESCE(string_agg(t::text, E'\\n' ORDER BY {order}), '')) "
                  f"FROM (SELECT {names} FROM {_quote(relation)}) AS t"),
    )


def catalog_facts(cursor, relations: Sequence[str]) -> dict[str, dict[str, object]]:
    """Columns, primary key, parents and sequences of each relation from the live V2 catalog."""
    facts: dict[str, dict[str, object]] = {}
    for relation in relations:
        schema, table = relation.split(".")
        cursor.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attgenerated <> '', "
            "pg_get_expr(d.adbin, d.adrelid) FROM pg_attribute a "
            "LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            (relation,))
        columns = [dict(name=name, type=type_, generated=bool(generated), default=default)
                   for name, type_, generated, default in cursor.fetchall()]
        cursor.execute(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "AND a.attnum = ANY (i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
            "ORDER BY array_position(i.indkey::int2[], a.attnum)", (relation,))
        primary_key = [row[0] for row in cursor.fetchall()]
        cursor.execute(
            "SELECT DISTINCT n.nspname || '.' || c.relname FROM pg_constraint k "
            "JOIN pg_class c ON c.oid = k.confrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE k.conrelid = %s::regclass AND k.contype = 'f'", (relation,))
        parents = {row[0] for row in cursor.fetchall()}
        cursor.execute(
            "SELECT array_agg(child.attname ORDER BY keys.ordinality),"
            "array_agg(parent.attname ORDER BY keys.ordinality) "
            "FROM pg_constraint k "
            "CROSS JOIN LATERAL unnest(k.conkey, k.confkey) WITH ORDINALITY "
            "AS keys(child_num, parent_num, ordinality) "
            "JOIN pg_attribute child ON child.attrelid=k.conrelid AND child.attnum=keys.child_num "
            "JOIN pg_attribute parent ON parent.attrelid=k.confrelid AND parent.attnum=keys.parent_num "
            "WHERE k.conrelid=%s::regclass AND k.confrelid=k.conrelid AND k.contype='f' "
            "GROUP BY k.oid ORDER BY k.oid",
            (relation,),
        )
        self_foreign_keys = [
            dict(columns=list(columns), referenced_columns=list(referenced_columns))
            for columns, referenced_columns in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT a.attname, pg_get_serial_sequence(%s, a.attname) FROM pg_attribute a "
            "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
            "AND pg_get_serial_sequence(%s, a.attname) IS NOT NULL", (relation, relation, relation))
        sequences = [dict(column=column, sequence=sequence) for column, sequence in cursor.fetchall()]
        cursor.execute(
            "SELECT t.tgname, pg_get_triggerdef(t.oid) FROM pg_trigger t "
            "WHERE t.tgrelid = %s::regclass AND NOT t.tgisinternal ORDER BY t.tgname", (relation,))
        triggers = [dict(name=name, definition=definition) for name, definition in cursor.fetchall()]
        facts[relation] = dict(
            columns=columns,
            primary_key=primary_key,
            parents=parents,
            self_foreign_keys=self_foreign_keys,
            sequences=sequences,
            triggers=triggers,
            schema=schema,
            table=table,
        )
    return facts


def build_reference_copy_plan(cursor) -> dict[str, object]:
    scope = reference_copy_scope()
    facts = catalog_facts(cursor, scope)
    order = foreign_key_order(scope, {name: facts[name]["parents"] for name in scope})
    relations, sequences = [], []
    for name in order:
        fact = facts[name]
        columns = [column["name"] for column in fact["columns"] if not column["generated"]]
        relations.append(dict(
            relation=name,
            columns=columns,
            column_types={
                column["name"]: column["type"]
                for column in fact["columns"]
                if not column["generated"]
            },
            primary_key=fact["primary_key"],
            parents=sorted(parent for parent in fact["parents"] if parent in scope and parent != name),
            # A table that references itself cannot be copied in primary-key order alone: the executor
            # must insert parents first or defer the foreign key for the load.
            self_referencing=name in fact["parents"],
            self_foreign_keys=fact["self_foreign_keys"],
            generated_columns_skipped=[column["name"] for column in fact["columns"] if column["generated"]],
            insert_triggers=[trigger["name"] for trigger in fact["triggers"]
                             if " INSERT" in trigger["definition"].upper()],
            statements=relation_statements(name, columns, fact["primary_key"]),
        ))
        for sequence in fact["sequences"]:
            sequences.append(dict(
                relation=name, column=sequence["column"], sequence=sequence["sequence"],
                setval=(f"SELECT setval('{sequence['sequence']}', "
                        f"(SELECT COALESCE(max(\"{sequence['column']}\"), 1) FROM {_quote(name)}), true)")))
    return dict(
        version=PLAN_VERSION, mode="PLAN", executor="reference_copy_executor_v1",
        source=SOURCE_DATABASE, target=TARGET_DATABASE,
        source_access="READ ONLY transaction, SELECT only; v1 is never modified",
        timestamps="copied verbatim: every column is listed, no target DEFAULT is used",
        relations=relations, sequences=sequences,
        preconditions=["target relations are empty (target_empty_check)", "the V2 baseline is applied and AUDITED or DEFERRED",
                       "explicit owner authorization for V2-10 (database provisioning and copy)"],
        verification=["row count and checksum equal on both sides for every relation",
                      "every sequence is at or beyond the copied maximum"],
    )
