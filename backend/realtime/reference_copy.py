"""Execute the V2-10 V1-to-V2 reference copy plan with source read-only and parity checks."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence

from psycopg2.extras import Json, execute_batch

from .reference_copy_plan import SOURCE_DATABASE, TARGET_DATABASE

COPY_PAGE_SIZE = 1_000


def _adapt_rows(rows, *, columns, column_types):
    json_indexes = {
        index
        for index, column in enumerate(columns)
        if column_types[column] in {"json", "jsonb"}
    }
    if not json_indexes:
        return rows
    return tuple(
        tuple(Json(value) if index in json_indexes and value is not None else value
              for index, value in enumerate(row))
        for row in rows
    )


def order_self_referencing_rows(
    rows: Sequence[tuple],
    *,
    columns: Sequence[str],
    primary_key: Sequence[str],
    foreign_keys: Sequence[Mapping[str, Sequence[str]]],
) -> tuple[tuple, ...]:
    if not foreign_keys:
        return tuple(rows)
    indexes = {name: index for index, name in enumerate(columns)}
    try:
        primary_indexes = tuple(indexes[name] for name in primary_key)
        key_indexes = tuple(
            (
                tuple(indexes[name] for name in foreign_key["columns"]),
                tuple(foreign_key["referenced_columns"]),
            )
            for foreign_key in foreign_keys
        )
    except KeyError as error:
        raise ValueError("reference copy self-FK names are absent from the copied columns") from error
    by_key = {
        tuple(row[index] for index in primary_indexes): row
        for row in rows
    }
    if len(by_key) != len(rows):
        raise ValueError("reference copy source rows repeat a primary key")
    dependencies = {}
    children = defaultdict(set)
    for key, row in by_key.items():
        parents = set()
        for child_indexes, referenced_columns in key_indexes:
            child_key = tuple(row[index] for index in child_indexes)
            if all(value is None for value in child_key):
                continue
            if any(value is None for value in child_key):
                raise ValueError("reference copy source has a partial self-FK")
            referenced = dict(zip(referenced_columns, child_key, strict=True))
            try:
                parent_key = tuple(referenced[name] for name in primary_key)
            except KeyError as error:
                raise ValueError("reference copy self-FK must reference the primary key") from error
            if parent_key not in by_key:
                raise ValueError("reference copy source self-FK parent is missing")
            if parent_key != key:
                parents.add(parent_key)
                children[parent_key].add(key)
        dependencies[key] = parents
    ready = sorted(key for key, parents in dependencies.items() if not parents)
    ordered = []
    while ready:
        key = ready.pop(0)
        ordered.append(by_key[key])
        for child in sorted(children[key]):
            dependencies[child].discard(key)
            if not dependencies[child] and child not in ready:
                ready.append(child)
        ready.sort()
    if len(ordered) != len(rows):
        raise ValueError("reference copy source self-FKs contain a cycle")
    return tuple(ordered)


def execute_reference_copy(
    source_connection,
    target_connection,
    plan: Mapping[str, object],
    *,
    values_executor=execute_batch,
) -> dict[str, object]:
    if (
        plan.get("source") != SOURCE_DATABASE
        or plan.get("target") != TARGET_DATABASE
        or plan.get("executor") != "reference_copy_executor_v1"
    ):
        raise ValueError("reference copy plan identity is invalid")
    copied = []
    source_connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with source_connection.cursor() as source, target_connection.cursor() as target:
            source.execute(
                "SELECT current_database()::text,current_setting('transaction_read_only')"
            )
            if tuple(source.fetchone()) != (SOURCE_DATABASE, "on"):
                raise RuntimeError("reference copy source identity/read-only guard failed")
            target.execute("SELECT current_database()::text")
            if target.fetchone()[0] != TARGET_DATABASE:
                raise RuntimeError("reference copy target identity guard failed")
            for relation in plan["relations"]:
                statements = relation["statements"]
                target.execute(statements["target_empty_check"])
                if target.fetchone()[0] is not True:
                    raise RuntimeError(f"reference copy target is not empty: {relation['relation']}")
                source.execute(statements["source_select"])
                rows = tuple(source.fetchall())
                if relation["self_referencing"]:
                    rows = order_self_referencing_rows(
                        rows,
                        columns=relation["columns"],
                        primary_key=relation["primary_key"],
                        foreign_keys=relation["self_foreign_keys"],
                    )
                if rows:
                    values_executor(
                        target,
                        statements["target_insert"],
                        _adapt_rows(
                            rows,
                            columns=relation["columns"],
                            column_types=relation["column_types"],
                        ),
                        page_size=COPY_PAGE_SIZE,
                    )
                source.execute(statements["checksum"])
                source_parity = tuple(source.fetchone())
                target.execute(statements["checksum"])
                target_parity = tuple(target.fetchone())
                if source_parity != target_parity:
                    raise RuntimeError(f"reference copy parity failed: {relation['relation']}")
                copied.append({
                    "relation": relation["relation"],
                    "rows": int(source_parity[0]),
                    "checksum": source_parity[1],
                })
            for sequence in plan["sequences"]:
                target.execute(sequence["setval"])
        target_connection.commit()
        source_connection.rollback()
    except Exception:
        target_connection.rollback()
        source_connection.rollback()
        raise
    return {
        "source": SOURCE_DATABASE,
        "target": TARGET_DATABASE,
        "relations": copied,
        "relation_count": len(copied),
        "row_count": sum(row["rows"] for row in copied),
        "source_read_only": True,
        "timestamps_copied_verbatim": True,
    }
