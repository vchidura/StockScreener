"""Copy the exact retained V1 control-plane closure needed by the V2-10 backfill."""
from __future__ import annotations

from psycopg2 import sql
from psycopg2.extras import execute_batch

from .contracts import EQUITY_VALIDATION_UNIVERSE
from .reference_copy import COPY_PAGE_SIZE, _adapt_rows
from .reference_copy_plan import catalog_facts

CONTROL_RELATIONS = (
    "rt.capability_profiles",
    "rt.capability_checks",
    "rt.seed_manifests",
    "rt.seed_manifest_members",
    "rt.seed_current",
    "rt.subscription_manifests",
    "rt.subscription_members",
    "rt.cohorts",
)


def _select_statement(relation, columns, primary_key, where):
    schema, table = relation.split(".")
    return sql.SQL("SELECT {} FROM {}.{} WHERE {} ORDER BY {}").format(
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(where),
        sql.SQL(", ").join(map(sql.Identifier, primary_key)),
    )


def _insert_statement(relation, columns):
    schema, table = relation.split(".")
    return sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )


def _copy_filtered(
    source,
    target,
    *,
    relation,
    fact,
    where,
    parameters,
    values_executor=execute_batch,
):
    columns = [column["name"] for column in fact["columns"] if not column["generated"]]
    primary_key = fact["primary_key"]
    statement = _select_statement(relation, columns, primary_key, where)
    source.execute(statement, parameters)
    rows = tuple(source.fetchall())
    target.execute(statement, parameters)
    existing = tuple(target.fetchall())
    if existing:
        if existing != rows:
            raise RuntimeError(f"V2 backfill control identity conflict: {relation}")
        return len(rows)
    target.execute(sql.SQL("SELECT NOT EXISTS (SELECT 1 FROM {})").format(
        sql.SQL(".").join(map(sql.Identifier, relation.split(".")))
    ))
    if target.fetchone()[0] is not True:
        raise RuntimeError(f"V2 backfill control target contains another scope: {relation}")
    if rows:
        values_executor(
            target,
            _insert_statement(relation, columns),
            _adapt_rows(
                rows,
                columns=columns,
                column_types={
                    column["name"]: column["type"]
                    for column in fact["columns"]
                    if not column["generated"]
                },
            ),
            page_size=COPY_PAGE_SIZE,
        )
    target.execute(statement, parameters)
    if tuple(target.fetchall()) != rows:
        raise RuntimeError(f"V2 backfill control parity failed: {relation}")
    return len(rows)


def _copy_subscription_members(source, target, *, manifest_id):
    source.execute(
        "SELECT manifest_id,instrument_id,channel,priority,purpose,false AS option_underlying "
        "FROM rt.subscription_members WHERE manifest_id=%s AND feed='STOCKS' "
        "ORDER BY manifest_id,channel,instrument_id",
        (str(manifest_id),),
    )
    rows = tuple(source.fetchall())
    target.execute(
        "SELECT manifest_id,instrument_id,channel,priority,purpose,option_underlying "
        "FROM rt.subscription_members WHERE manifest_id=%s "
        "ORDER BY manifest_id,channel,instrument_id",
        (str(manifest_id),),
    )
    existing = tuple(target.fetchall())
    if existing:
        if existing != rows:
            raise RuntimeError("V2 backfill control identity conflict: rt.subscription_members")
        return len(rows)
    target.execute("SELECT NOT EXISTS (SELECT 1 FROM rt.subscription_members)")
    if target.fetchone()[0] is not True:
        raise RuntimeError("V2 backfill control target contains another scope: rt.subscription_members")
    if rows:
        execute_batch(
            target,
            "INSERT INTO rt.subscription_members "
            "(manifest_id,instrument_id,channel,priority,purpose,option_underlying) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            rows,
            page_size=COPY_PAGE_SIZE,
        )
    target.execute(
        "SELECT manifest_id,instrument_id,channel,priority,purpose,option_underlying "
        "FROM rt.subscription_members WHERE manifest_id=%s "
        "ORDER BY manifest_id,channel,instrument_id",
        (str(manifest_id),),
    )
    if tuple(target.fetchall()) != rows:
        raise RuntimeError("V2 backfill control parity failed: rt.subscription_members")
    return len(rows)


def copy_v2_backfill_control_plane(
    source_connection,
    target_connection,
    *,
    cohort_id,
) -> dict:
    source_connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with source_connection.cursor() as source, target_connection.cursor() as target:
            source.execute(
                "SELECT current_database()::text,current_setting('transaction_read_only')"
            )
            if tuple(source.fetchone()) != ("stocks_realtime_v1", "on"):
                raise RuntimeError("V2 backfill control source identity/read-only guard failed")
            target.execute(
                "SELECT current_database()::text,lineage_name FROM rt.database_identity"
            )
            if tuple(target.fetchone()) != ("stocks_realtime_v2", "advanced_realtime_v2"):
                raise RuntimeError("V2 backfill control target identity guard failed")
            source.execute(
                "SELECT cohort.manifest_id,manifest.profile_id,manifest.universe_sha256 "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
                "WHERE cohort.cohort_id=%s",
                (str(cohort_id),),
            )
            identity = source.fetchone()
            if identity is None or identity[2] != EQUITY_VALIDATION_UNIVERSE.sha256:
                raise ValueError("V2 backfill source cohort identity is absent or changed")
            manifest_id, profile_id, _ = identity
            source.execute(
                "SELECT seed.seed_id FROM rt.seed_current AS current "
                "JOIN rt.seed_manifests AS seed USING (seed_id) "
                "WHERE seed.universe_sha256=%s AND seed.status='READY' "
                "ORDER BY current.generation DESC LIMIT 1",
                (EQUITY_VALIDATION_UNIVERSE.sha256,),
            )
            seed_row = source.fetchone()
            if seed_row is None:
                raise ValueError("V2 backfill source has no current ready seed")
            seed_id = seed_row[0]
            facts = catalog_facts(target, CONTROL_RELATIONS)
            scopes = (
                ("rt.capability_profiles", "profile_id=%s", (str(profile_id),)),
                ("rt.capability_checks", "profile_id=%s", (str(profile_id),)),
                ("rt.seed_manifests", "seed_id=%s", (str(seed_id),)),
                ("rt.seed_manifest_members", "seed_id=%s", (str(seed_id),)),
                ("rt.seed_current", "seed_id=%s", (str(seed_id),)),
                ("rt.subscription_manifests", "manifest_id=%s", (str(manifest_id),)),
                ("rt.cohorts", "cohort_id=%s", (str(cohort_id),)),
            )
            copied = {}
            for relation, where, parameters in scopes:
                copied[relation] = _copy_filtered(
                    source,
                    target,
                    relation=relation,
                    fact=facts[relation],
                    where=where,
                    parameters=parameters,
                )
            copied["rt.subscription_members"] = _copy_subscription_members(
                source,
                target,
                manifest_id=manifest_id,
            )
        target_connection.commit()
        source_connection.rollback()
    except Exception:
        target_connection.rollback()
        source_connection.rollback()
        raise
    if (
        copied["rt.capability_profiles"] != 1
        or copied["rt.seed_manifests"] != 1
        or copied["rt.seed_current"] != 1
        or copied["rt.subscription_manifests"] != 1
        or copied["rt.cohorts"] != 1
        or copied["rt.subscription_members"] != 27
    ):
        raise RuntimeError("V2 backfill control closure counts changed")
    return {
        "cohort_id": str(cohort_id),
        "manifest_id": str(manifest_id),
        "profile_id": str(profile_id),
        "seed_id": str(seed_id),
        "relations": copied,
        "timestamps_copied_verbatim": True,
    }
