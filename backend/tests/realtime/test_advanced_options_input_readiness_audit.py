from datetime import date, datetime, timezone

import pytest

from scripts.audit_advanced_options_input_readiness import (
    REQUIRED_CATEGORIES,
    ReadOnlyCursor,
    _classify,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 17, 30, tzinfo=UTC)


class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))


def test_read_only_cursor_rejects_database_effects():
    raw = _Cursor()
    cursor = ReadOnlyCursor(raw)

    cursor.execute("SELECT 1")
    cursor.execute("WITH bounded AS (SELECT 1) SELECT * FROM bounded")
    cursor.execute("SET LOCAL statement_timeout = '1s'")

    with pytest.raises(RuntimeError, match="non-read-only"):
        cursor.execute("INSERT INTO rt.cycle_receipts DEFAULT VALUES")
    with pytest.raises(RuntimeError, match="non-read-only"):
        cursor.execute(
            "WITH removed AS (DELETE FROM rt.cycle_receipts RETURNING 1) "
            "SELECT * FROM removed"
        )
    assert cursor.statement_count == 3


def test_absent_canonical_scope_blocks_every_required_dependency():
    source = {
        "production_wiring_counts": {
            "reference_state_builder_production_calls": 0,
            "rolling_state_builder_production_calls": 0,
            "postgres_runtime_scope_production_calls": 0,
        }
    }
    snapshot = {
        "cohort": None,
        "cohort_reason_codes": ("MARKET_OPTIONS_COHORT_ABSENT",),
        "slot_members": (),
        "reference_rows": (),
        "reference_state": None,
        "reference_identity_error": None,
        "rolling_rows": (),
        "rolling_state": None,
        "rolling_identity_error": None,
        "source_flows": (),
        "quotes": (),
        "receipts": (),
        "bar_windows": (),
        "checkpoints": (),
        "gaps": (),
    }

    categories = _classify(
        snapshot,
        source,
        session_date=date(2026, 10, 20),
        cutoff=NOW,
        observed_at=NOW,
    )

    assert set(categories) == set(REQUIRED_CATEGORIES)
    assert categories["o1_equity_context"]["status"] == "AVAILABLE"
    assert all(
        row["status"] == "BLOCKED"
        for name, row in categories.items()
        if name != "o1_equity_context"
    )
    assert categories["unresolved_gaps"]["reason_codes"] == [
        "SOURCE_FLOW_IDENTITY_ABSENT"
    ]
    assert "OPTIONS_INPUT_READY_DEPENDENCIES_BLOCKED" in (
        categories["options_input_ready_closure"]["reason_codes"]
    )
