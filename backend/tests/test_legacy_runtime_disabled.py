import inspect
from pathlib import Path

import main


BACKEND_DIR = Path(__file__).resolve().parent.parent


def test_primary_startup_does_not_create_legacy_price_tables():
    source = inspect.getsource(main.startup_event)

    assert "create_daily_table" not in source
    assert "create_hourly_table" not in source
    assert "create_intraday_table" not in source


def test_health_contract_requires_canonical_storage_and_absent_legacy_relations():
    source = inspect.getsource(main.api_health)

    assert "equity_bar_revisions" in source
    assert "equity_current_bar_projection" in source
    assert "legacy_public_relations_absent" in source
    assert "restricted_role_ready" in source
    assert "role_owns_database" in source
    assert "role_owns_public_relations" in source
    assert "portal_snapshots_ready" in source
    assert 'APP_ENV", "development"' in source


def test_wheel_build_does_not_generate_duplicate_application_sources():
    source = (BACKEND_DIR / "build_wheel.py").read_text(encoding="utf-8")

    assert "FILE_MAP" not in source
    assert "sync_file" not in source
    assert not (BACKEND_DIR / "src" / "stock_screener" / "app.py").exists()


def test_compose_defaults_to_canonical_storage_and_removes_legacy_scheduler():
    source = (BACKEND_DIR.parent / "docker-compose.yml").read_text(encoding="utf-8")

    assert "to_regclass('public.equity_bar_revisions')" in source
    assert "run_scheduler.py" not in source
    assert "EQUITY_MATERIALIZED_1MO_SETUP_ENABLED" in source