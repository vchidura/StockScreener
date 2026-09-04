"""Canonical schema installation without importing runtime database services."""
from __future__ import annotations

from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
BASELINE_VERSION = "000_canonical_schema"
BASELINE_MIGRATION = BACKEND_DIR / "migrations" / f"{BASELINE_VERSION}.sql"
REQUIRED_SCHEMA_RELATIONS = (
    "selected_tickers",
    "equity_bar_revisions",
    "equity_current_bar_projection",
    "equity_evidence",
    "equity_research_outcomes",
    "equity_qualification_revisions",
    "equity_portal_source_state",
    "equity_portal_snapshots",
    "option_contract_catalog",
    "option_chain_snapshots",
    "option_strategy_candidates",
)


def install_or_adopt_schema(connection) -> str:
    """Install the baseline on empty DBs or record an equivalent complete schema."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT relation_name, to_regclass('public.' || relation_name)
                         IS NOT NULL AS present
                FROM unnest(%s::text[]) AS relation_name
                """,
                (list(REQUIRED_SCHEMA_RELATIONS),),
            )
            presence = dict(cursor.fetchall())
            missing = sorted(
                relation for relation in REQUIRED_SCHEMA_RELATIONS
                if not presence.get(relation, False)
            )
            present_count = len(REQUIRED_SCHEMA_RELATIONS) - len(missing)

            cursor.execute(
                "SELECT to_regclass('public.schema_migrations') IS NOT NULL"
            )
            migration_table_present = bool(cursor.fetchone()[0])
            baseline_recorded = False
            if migration_table_present:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = %s)",
                    (BASELINE_VERSION,),
                )
                baseline_recorded = bool(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_proc
                    WHERE oid = to_regprocedure(
                        'public.ensure_option_market_data_partitions(date)'
                    )
                      AND prosecdef = TRUE
                      AND proconfig @> ARRAY['search_path=pg_catalog, public']
                )
                """
            )
            partition_maintenance_ready = bool(cursor.fetchone()[0])

            if baseline_recorded:
                if missing:
                    raise RuntimeError(
                        "canonical schema baseline is recorded but relations are missing: "
                        + ", ".join(missing)
                    )
                if not partition_maintenance_ready:
                    raise RuntimeError(
                        "canonical schema baseline is recorded but option partition "
                        "maintenance is not hardened"
                    )
                result = "ALREADY_APPLIED"
            elif present_count == 0:
                cursor.execute(BASELINE_MIGRATION.read_text(encoding="utf-8"))
                result = "APPLIED_TO_EMPTY_DATABASE"
            elif missing:
                raise RuntimeError(
                    "refusing to apply canonical baseline to a partial schema; missing: "
                    + ", ".join(missing)
                )
            else:
                if not partition_maintenance_ready:
                    raise RuntimeError(
                        "refusing to adopt schema without hardened option partition "
                        "maintenance"
                    )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version text PRIMARY KEY,
                        applied_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s) "
                    "ON CONFLICT (version) DO NOTHING",
                    (BASELINE_VERSION,),
                )
                result = "ADOPTED_EXISTING_SCHEMA"
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
