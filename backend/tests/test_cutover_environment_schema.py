from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import validate_cutover_environment


BACKEND_DIR = Path(__file__).resolve().parent.parent


def test_environment_example_contains_only_required_and_intentional_overrides():
    source = (BACKEND_DIR / ".env.example").read_text(encoding="utf-8")

    for name in (
        "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT",
        "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD", "POLYGON_API_KEY",
        "CORS_ORIGINS", "EQUITY_PROVIDER_DELAY_MINUTES",
        "OPTION_PROVIDER_DELAY_SECONDS", "OPTION_START_READ_ONLY",
        "OPTION_EQUITY_CONTEXT_ENABLED", "OPTION_RAW_ARCHIVE_ENABLED",
    ):
        assert f"{name}=" in source
    assert "EQUITY_UNIVERSE_TARGET_SIZE=350" in source
    assert "EQUITY_UNIVERSE_LOOKBACK_DAYS=20" in source
    assert "EQUITY_MATERIALIZED_" not in source
    assert "EQUITY_PORTAL_CUTOVER_API_BASE_URL" not in source
    assert "DB_BOOTSTRAP_BACKUP=" not in source


def test_validation_uses_safe_runtime_defaults_for_omitted_tuning(monkeypatch):
    required = {
        "DB_NAME": "stocks_db", "DB_USER": "app", "DB_PASSWORD": "secret",
        "DB_HOST": "db", "DB_PORT": "5432", "POLYGON_API_KEY": "present",
        "APP_ENV": "production", "CORS_ORIGINS": "https://stocks.example.com",
    }
    optional = (
        *validate_cutover_environment.TRUE_FLAGS,
        "EQUITY_MATERIALIZATION_INTERVALS", "EQUITY_WORKER_POLL_SECONDS",
        "EQUITY_NATIVE_FETCH_WORKERS", "EQUITY_STALE_RUN_MINUTES",
        "EQUITY_PORTAL_SNAPSHOT_POLL_SECONDS", "EQUITY_STREAM_FLUSH_SECONDS",
        "EQUITY_STREAM_MAX_RECONNECTS", "EQUITY_PROVIDER_DELAY_MINUTES",
        "EQUITY_PUBLICATION_GRACE_SECONDS", "OPTION_WORKER_POLL_SECONDS",
        "OPTION_SLOT_SECONDS", "OPTION_PROVIDER_DELAY_SECONDS",
        "OPTION_PUBLICATION_GRACE_SECONDS", "OPTION_EQUITY_CONTEXT_ENABLED",
        "OPTION_START_READ_ONLY", "OPTION_RAW_ARCHIVE_ENABLED",
    )
    for name in optional:
        monkeypatch.delenv(name, raising=False)
    for name, value in required.items():
        monkeypatch.setenv(name, value)
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        False, False, False, True, True, True, True, True, True, True,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch.object(
        validate_cutover_environment.psycopg2, "connect", return_value=connection,
    ):
        result = validate_cutover_environment.validate()

    assert result["status"] == "PASS"
    assert all(result["read_flags"].values())
    assert all(result["positive_worker_settings"].values())


def test_template_placeholders_are_not_treated_as_configured(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "your_polygon_api_key_here")

    assert validate_cutover_environment._is_configured("POLYGON_API_KEY") is False


def test_compose_separates_admin_and_runtime_database_roles():
    source = (BACKEND_DIR.parent / "docker-compose.yml").read_text(encoding="utf-8")

    assert "POSTGRES_USER: ${POSTGRES_ADMIN_USER:-postgres}" in source
    assert "APP_DB_USER: ${DB_USER:?DB_USER is required}" in source
    assert "image: postgres:17-alpine" in source
    assert "000_canonical_schema.sql:/docker-entrypoint-initdb.d/00_canonical_schema.sql" in source
    assert "00_restore_database.sh" not in source
    assert "equity-universe-bootstrap:" in source
    assert "condition: service_completed_successfully" in source
    assert "--if-empty" in source
    assert "EQUITY_UNIVERSE_TARGET_SIZE" in source
    assert "01_configure_app_role.sh" in source
    assert "changeme" not in source


def test_compose_forwards_complete_option_and_worker_contract():
    source = (BACKEND_DIR.parent / "docker-compose.yml").read_text(encoding="utf-8")

    assert "OPTION_FIXED_STOCK_UNDERLYERS:" in source
    assert "OPTION_POLICY_FILE:" in source
    assert source.count(
        "OPTION_POLL_SECONDS: ${OPTION_POLL_SECONDS:-900}"
    ) == 2
    assert "EQUITY_MATERIALIZATION_INTERVALS:" in source
    assert source.count("APP_ENV: ${APP_ENV:-production}") == 6
    assert "equity-migrate:" in source
    assert "option-worker:" in source
    assert 'profiles: ["options"]' in source
    assert "DB_USER: ${POSTGRES_ADMIN_USER:-postgres}" in source


def test_compose_role_script_denies_superuser_and_schema_ownership():
    source = (BACKEND_DIR / "scripts" / "01_configure_app_role.sh").read_text(
        encoding="utf-8"
    )

    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION" in source
    assert "REVOKE CREATE ON SCHEMA public" in source
    assert "GRANT USAGE ON SCHEMA public" in source
    assert "GRANT USAGE ON SCHEMA legacy_archive" not in source
    assert "\\getenv app_password APP_DB_PASSWORD" in source
    assert "--set=app_password" not in source


def test_production_frontend_uses_nginx_same_origin_api_proxy():
    source = (BACKEND_DIR.parent / "frontend" / ".env.production").read_text(
        encoding="utf-8"
    )

    assert "VITE_API_BASE_URL=/api" in source
    assert "localhost:8001" not in source


def test_production_validation_rejects_superuser_database_role(monkeypatch):
    required = {
        "DB_NAME": "stocks_db", "DB_USER": "app", "DB_PASSWORD": "secret",
        "DB_HOST": "db", "DB_PORT": "5432", "POLYGON_API_KEY": "present",
        "APP_ENV": "production",
        "EQUITY_MATERIALIZATION_INTERVALS": "5m,15m,30m,1h,1d,1wk,1mo",
        "EQUITY_WORKER_POLL_SECONDS": "15", "EQUITY_NATIVE_FETCH_WORKERS": "8",
        "EQUITY_STALE_RUN_MINUTES": "60",
        "EQUITY_PORTAL_SNAPSHOT_POLL_SECONDS": "60",
        "EQUITY_STREAM_FLUSH_SECONDS": "1", "EQUITY_STREAM_MAX_RECONNECTS": "20",
        "CORS_ORIGINS": "http://127.0.0.1:5174,http://localhost:5174",
        "OPTION_EQUITY_CONTEXT_ENABLED": "false", "OPTION_START_READ_ONLY": "true",
        "OPTION_RAW_ARCHIVE_ENABLED": "false",
    }
    required.update({name: "true" for name in validate_cutover_environment.TRUE_FLAGS})
    for name, value in required.items():
        monkeypatch.setenv(name, value)
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        True, False, False, True, True, True, True, True, True, True,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch.object(validate_cutover_environment.psycopg2, "connect", return_value=connection):
        result = validate_cutover_environment.validate()

    assert result["status"] == "FAIL"
    assert "DB_USER_SUPERUSER" in result["failures"]


def test_production_validation_rejects_database_or_public_relation_owner(monkeypatch):
    required = {
        "DB_NAME": "stocks_db", "DB_USER": "app", "DB_PASSWORD": "secret",
        "DB_HOST": "db", "DB_PORT": "5432", "POLYGON_API_KEY": "present",
        "APP_ENV": "production",
        "EQUITY_MATERIALIZATION_INTERVALS": "5m,15m,30m,1h,1d,1wk,1mo",
        "EQUITY_WORKER_POLL_SECONDS": "15", "EQUITY_NATIVE_FETCH_WORKERS": "8",
        "EQUITY_STALE_RUN_MINUTES": "60",
        "EQUITY_PORTAL_SNAPSHOT_POLL_SECONDS": "60",
        "EQUITY_STREAM_FLUSH_SECONDS": "1", "EQUITY_STREAM_MAX_RECONNECTS": "20",
        "CORS_ORIGINS": "https://stocks.example.com",
        "OPTION_EQUITY_CONTEXT_ENABLED": "false", "OPTION_START_READ_ONLY": "true",
        "OPTION_RAW_ARCHIVE_ENABLED": "false",
    }
    required.update({name: "true" for name in validate_cutover_environment.TRUE_FLAGS})
    for name, value in required.items():
        monkeypatch.setenv(name, value)
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        False, True, True, True, True, True, True, True, True, True,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch.object(validate_cutover_environment.psycopg2, "connect", return_value=connection):
        result = validate_cutover_environment.validate()

    assert result["status"] == "FAIL"
    assert "DB_USER_OWNER" in result["failures"]


def test_validation_reports_database_connection_failure_without_raising(monkeypatch):
    monkeypatch.setenv("DB_NAME", "stocks_db")
    monkeypatch.setenv("DB_USER", "app")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("DB_HOST", "db")
    monkeypatch.setenv("DB_PORT", "5432")

    with patch.object(
        validate_cutover_environment.psycopg2,
        "connect",
        side_effect=validate_cutover_environment.psycopg2.OperationalError(),
    ):
        result = validate_cutover_environment.validate()

    assert result["database"] == {
        "reachable": False,
        "error_type": "OperationalError",
    }
    assert "DATABASE_CONNECTION" in result["failures"]