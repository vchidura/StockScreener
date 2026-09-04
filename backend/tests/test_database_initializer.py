import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import initialize_database as command


def settings() -> dict[str, str]:
    return {
        "DB_NAME": "stocks_probe",
        "DB_USER": "stocks_app",
        "DB_PASSWORD": "runtime-secret",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "POSTGRES_ADMIN_USER": "postgres",
        "POSTGRES_ADMIN_PASSWORD": "admin-secret",
    }


def test_initializer_requires_distinct_admin_and_runtime_roles(monkeypatch):
    for name, value in settings().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("DB_USER", "postgres")

    with pytest.raises(RuntimeError, match="must differ"):
        command._required_environment()


def test_recreate_requires_exact_database_confirmation(monkeypatch):
    with (
        patch.object(command, "_required_environment", return_value=settings()),
        patch.object(command, "ensure_database") as ensure_database,
        patch.object(sys, "argv", [
            "initialize_database.py", "--recreate",
            "--confirm-database-name", "wrong_database",
        ]),
    ):
        with pytest.raises(RuntimeError, match="equal to DB_NAME"):
            command.main()

    ensure_database.assert_not_called()


def test_initializer_creates_schema_then_configures_runtime_role(monkeypatch, capsys):
    connection = MagicMock()
    with (
        patch.object(command, "_required_environment", return_value=settings()),
        patch.object(command, "ensure_database", return_value="CREATED") as ensure,
        patch.object(command, "_admin_connection", return_value=connection),
        patch.object(
            command, "install_or_adopt_schema",
            return_value="APPLIED_TO_EMPTY_DATABASE",
        ) as install,
        patch.object(command, "configure_runtime_role") as configure,
        patch.object(sys, "argv", ["initialize_database.py"]),
    ):
        assert command.main() == 0

    ensure.assert_called_once_with(settings(), recreate=False)
    install.assert_called_once_with(connection)
    configure.assert_called_once_with(settings())
    connection.close.assert_called_once_with()
    output = capsys.readouterr().out
    assert "DATABASE_INITIALIZED" in output
    assert "runtime-secret" not in output
    assert "admin-secret" not in output
