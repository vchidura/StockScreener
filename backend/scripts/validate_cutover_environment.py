#!/usr/bin/env python3
"""Validate post-cutover environment and database readiness without printing secrets."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from options.startup import build_option_startup_state


load_dotenv(BACKEND_DIR / ".env")

TRUE_FLAGS = (
    "EQUITY_MATERIALIZED_30M_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1H_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1D_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1WK_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1MO_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_PATTERN_WATCH_ENABLED",
    "EQUITY_MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED",
    "EQUITY_MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED",
)
EXPECTED_INTERVALS = ("5m", "15m", "30m", "1h", "1d", "1wk", "1mo")
PLACEHOLDER_VALUES = {
    "your_db_user",
    "your_secure_password",
    "your_polygon_api_key_here",
    "changeme",
}


def _is_configured(name: str) -> bool:
    value = os.getenv(name, "").strip()
    return bool(value) and value.lower() not in PLACEHOLDER_VALUES


def validate() -> dict[str, object]:
    failures = []
    warnings = []
    environment = os.getenv("APP_ENV", "development").lower()
    if environment not in {"development", "production", "test"}:
        failures.append("APP_ENV")
    required_present = {
        name: _is_configured(name)
        for name in ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "POLYGON_API_KEY")
    }
    failures.extend(name for name, present in required_present.items() if not present)

    read_flags = {name: os.getenv(name, "true").lower() == "true" for name in TRUE_FLAGS}
    failures.extend(name for name, enabled in read_flags.items() if not enabled)
    safe_options = {
        "OPTION_EQUITY_CONTEXT_ENABLED": os.getenv("OPTION_EQUITY_CONTEXT_ENABLED", "false").lower() == "false",
        "OPTION_START_READ_ONLY": os.getenv("OPTION_START_READ_ONLY", "true").lower() == "true",
        "OPTION_RAW_ARCHIVE_ENABLED": os.getenv("OPTION_RAW_ARCHIVE_ENABLED", "false").lower() == "false",
    }
    failures.extend(name for name, valid in safe_options.items() if not valid)
    option_runtime = {}
    try:
        option_configuration = load_option_runtime_configuration(os.environ, BACKEND_DIR)
        option_startup = build_option_startup_state(option_configuration)
        option_runtime = {
            "mode": option_startup.mode.value,
            "data_engine": option_configuration.settings.data_engine.value,
            "execution_engine": option_configuration.settings.execution_engine.value,
            "universe_mode": option_configuration.settings.universe_mode.value,
            "underlyer_count": len(option_configuration.settings.underlyers),
        }
    except (OSError, ValueError, RuntimeError) as exc:
        option_runtime = {"valid": False, "error_type": type(exc).__name__}
        failures.append("OPTION_RUNTIME_CONFIGURATION")

    intervals = tuple(
        value.strip() for value in os.getenv(
            "EQUITY_MATERIALIZATION_INTERVALS", "5m,15m,30m,1h,1d,1wk,1mo"
        ).split(",")
        if value.strip()
    )
    if intervals != EXPECTED_INTERVALS:
        failures.append("EQUITY_MATERIALIZATION_INTERVALS")
    positive_numbers = {}
    positive_defaults = {
        "EQUITY_WORKER_POLL_SECONDS": "15",
        "EQUITY_NATIVE_FETCH_WORKERS": "8",
        "EQUITY_STALE_RUN_MINUTES": "60",
        "EQUITY_PORTAL_SNAPSHOT_POLL_SECONDS": "60",
        "EQUITY_STREAM_FLUSH_SECONDS": "1",
        "EQUITY_STREAM_MAX_RECONNECTS": "20",
    }
    for name, default in positive_defaults.items():
        try:
            positive_numbers[name] = float(os.getenv(name, default)) > 0
        except ValueError:
            positive_numbers[name] = False
    failures.extend(name for name, valid in positive_numbers.items() if not valid)
    try:
        provider_delay_valid = float(os.getenv("EQUITY_PROVIDER_DELAY_MINUTES", "15")) >= 0
    except ValueError:
        provider_delay_valid = False
    if not provider_delay_valid:
        failures.append("EQUITY_PROVIDER_DELAY_MINUTES")
    try:
        publication_grace_valid = float(
            os.getenv("EQUITY_PUBLICATION_GRACE_SECONDS", "600")
        ) >= 0
    except (KeyError, ValueError):
        publication_grace_valid = False
    if not publication_grace_valid:
        failures.append("EQUITY_PUBLICATION_GRACE_SECONDS")
    option_worker_settings = {}
    option_defaults = {
        "OPTION_WORKER_POLL_SECONDS": "15",
        "OPTION_SLOT_SECONDS": "900",
        "OPTION_PROVIDER_DELAY_SECONDS": "900",
        "OPTION_PUBLICATION_GRACE_SECONDS": "30",
    }
    for name, default in option_defaults.items():
        try:
            value = float(os.getenv(name, default))
            option_worker_settings[name] = (
                value > 0
                if name in {"OPTION_WORKER_POLL_SECONDS", "OPTION_SLOT_SECONDS"}
                else value >= 0
            )
        except ValueError:
            option_worker_settings[name] = False
    failures.extend(
        name for name, valid in option_worker_settings.items() if not valid
    )

    cors = {value.strip() for value in os.getenv("CORS_ORIGINS", "").split(",") if value.strip()}
    invalid_cors = []
    for origin in cors:
        parsed = urlparse(origin)
        if origin == "*" or parsed.scheme not in {"http", "https"} or not parsed.netloc:
            invalid_cors.append(origin)
    if not cors or invalid_cors:
        failures.append("CORS_ORIGINS")
    if environment == "development":
        for required_origin in ("http://127.0.0.1:5174", "http://localhost:5174"):
            if required_origin not in cors:
                failures.append(f"CORS_ORIGINS:{required_origin}")

    database = {}
    if all(required_present[name] for name in ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT")):
        try:
            connection = psycopg2.connect(
                dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"], host=os.environ["DB_HOST"],
                port=os.environ["DB_PORT"],
            )
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT role.rolsuper,
                               current_user = pg_get_userbyid(database.datdba),
                               EXISTS (
                                   SELECT 1
                                   FROM pg_class relation
                                   JOIN pg_namespace namespace
                                     ON namespace.oid = relation.relnamespace
                                   WHERE namespace.nspname = 'public'
                                     AND relation.relkind IN ('r', 'p', 'v', 'm', 'S')
                                     AND relation.relowner = role.oid
                               ),
                               EXISTS (
                                   SELECT 1 FROM public.schema_migrations
                                   WHERE version = '000_canonical_schema'
                               ),
                               to_regclass('public.equity_bar_revisions') IS NOT NULL,
                               to_regclass('public.equity_current_bar_projection') IS NOT NULL,
                               to_regclass('public.equity_canonical_bars') IS NOT NULL,
                               to_regclass('public.stock_prices_daily') IS NULL,
                               to_regclass('public.stock_prices_hourly') IS NULL,
                               to_regclass('public.stock_prices_intraday') IS NULL
                        FROM pg_roles role
                        CROSS JOIN pg_database database
                        WHERE role.rolname = current_user
                          AND database.datname = current_database()
                        """
                    )
                    row = cursor.fetchone()
                database = {
                    "role_is_superuser": row[0],
                    "role_owns_database": row[1],
                    "role_owns_public_relations": row[2],
                    "schema_baseline_ready": row[3],
                    "bar_revisions_ready": row[4],
                    "bar_projection_ready": row[5],
                    "canonical_view_ready": row[6],
                    "legacy_public_relations_absent": all(row[7:]),
                }
            finally:
                connection.close()
        except psycopg2.Error as exc:
            database = {"reachable": False, "error_type": type(exc).__name__}
            failures.append("DATABASE_CONNECTION")
        else:
            readiness_exclusions = {
                "role_is_superuser", "role_owns_database", "role_owns_public_relations"
            }
            if not all(
                value for key, value in database.items() if key not in readiness_exclusions
            ):
                failures.append("DATABASE_CANONICAL_READINESS")
            role_failures = []
            if database["role_is_superuser"]:
                role_failures.append("DB_USER_SUPERUSER")
            if database["role_owns_database"] or database["role_owns_public_relations"]:
                role_failures.append("DB_USER_OWNER")
            if environment == "production":
                failures.extend(role_failures)
            elif role_failures:
                warnings.append(
                    "DB_USER is privileged; use a restricted non-owner role in production"
                )

    return {
        "status": "PASS" if not failures else "FAIL",
        "environment": environment,
        "required_values_present": required_present,
        "read_flags": read_flags,
        "safe_option_guards": safe_options,
        "option_runtime": option_runtime,
        "worker_intervals": intervals,
        "positive_worker_settings": positive_numbers,
        "provider_delay_valid": provider_delay_valid,
        "publication_grace_valid": publication_grace_valid,
        "option_worker_settings": option_worker_settings,
        "cors_origin_count": len(cors),
        "database": database,
        "warnings": warnings,
        "failures": sorted(set(failures)),
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())