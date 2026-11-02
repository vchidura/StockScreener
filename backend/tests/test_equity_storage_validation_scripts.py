import json
import shutil
import subprocess
from pathlib import Path

import pytest

from realtime.migrations import RETIRED_REALTIME_MIGRATIONS
from scripts.validate_advanced_realtime_v3_restore import (
    NORMALIZED_CHECKPOINT,
    RAW_CHECKPOINT,
    _migration_mismatches,
    checkpoint_exceeds_boundary,
)


BACKEND_DIR = Path(__file__).resolve().parent.parent


def test_restore_validator_ignores_only_the_exact_retired_ledger_row():
    retired, checksum = next(iter(RETIRED_REALTIME_MIGRATIONS.items()))
    expected = {"000_realtime_baseline": "a" * 64}

    assert _migration_mismatches(expected, {**expected, retired: checksum}) == []
    assert _migration_mismatches(expected, {**expected, retired: "b" * 64}) == [
        retired
    ]
    assert _migration_mismatches(expected, {**expected, "x": checksum}) == ["x"]
    assert _migration_mismatches(expected, {retired: checksum}) == [
        "000_realtime_baseline"
    ]


def test_canonical_validator_requires_lineage_only_for_canonical_derivations():
    source = (BACKEND_DIR / "scripts" / "validate_equity_storage.py").read_text(
        encoding="utf-8"
    )

    assert "DERIVED_FROM_CANONICAL_%" in source
    assert "unresolved_bar_lineage" in source
    assert "invalid_publication_counts" in source
    assert "latest.market_time = projection.bar_end" in source


def test_backup_restore_verifier_covers_published_analysis_and_portal_state():
    source = (BACKEND_DIR / "scripts" / "verify_database_backup.ps1").read_text(
        encoding="utf-8"
    )

    assert "[string]$Database" in source
    assert "[string]$ValidationScope" in source
    assert '[ValidateSet("Legacy", "RealtimeV3", "Both")]' in source
    assert "POSTGRES_ADMIN_USER" in source
    assert "POSTGRES_ADMIN_PASSWORD" in source
    for table in (
        "equity_bar_publication_members",
        "equity_analysis_runs",
        "equity_analysis_members",
        "equity_context_evidence",
        "equity_portal_source_state",
        "equity_portal_snapshots",
        "equity_portal_current_projections",
        "equity_outcome_policies",
        "equity_research_outcomes",
        "equity_qualification_revisions",
        "realtime_desired_revisions",
        "advanced_options_preparation_sessions",
        "advanced_options_preparation_evidence_history",
        "advanced_options_preparation_refresh_requests",
        "advanced_options_continuous_sessions",
        "advanced_journal_registry",
        "advanced_options_rolling_checkpoints",
        "advanced_options_selection_generations",
        "advanced_options_physical_subscriptions",
        "advanced_options_consumer_states",
        "advanced_options_detector_routing_states",
        "portal_publication_work",
        "portal_release_current",
        "advanced_options_read_pins",
        "advanced_realtime_service_states",
    ):
        assert table in source
    assert "validate_equity_storage.py" in source
    assert "validate_advanced_realtime_v3_restore.py" in source
    assert "CANONICAL_RESTORE_VALIDATED" in source
    assert "ADVANCED_REALTIME_V3_RESTORE_VALIDATED" in source


@pytest.mark.parametrize(
    ("scope", "expected_validators", "legacy_table", "v3_table"),
    (
        (
            "Legacy",
            ["validate_equity_storage.py"],
            True,
            False,
        ),
        (
            "RealtimeV3",
            ["validate_advanced_realtime_v3_restore.py"],
            False,
            True,
        ),
        (
            "Both",
            [
                "validate_equity_storage.py",
                "validate_advanced_realtime_v3_restore.py",
            ],
            True,
            True,
        ),
    ),
)
def test_backup_restore_validation_scope_builds_provider_free_plan(
    scope,
    expected_validators,
    legacy_table,
    v3_table,
):
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        pytest.skip("Windows PowerShell is unavailable")
    script = BACKEND_DIR / "scripts" / "verify_database_backup.ps1"
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-BackupPath",
            "unused-plan-only.dump",
            "-ValidationScope",
            scope,
            "-PlanOnly",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    plan = json.loads(completed.stdout)

    assert plan["ValidationScope"] == scope
    assert plan["Validators"] == expected_validators
    assert (
        "public.selected_tickers" in plan["CriticalTables"]
    ) is legacy_table
    assert (
        "rt.realtime_desired_revisions" in plan["CriticalTables"]
    ) is v3_table


def test_advanced_realtime_restore_validator_covers_v3_state():
    source = (
        BACKEND_DIR
        / "scripts"
        / "validate_advanced_realtime_v3_restore.py"
    ).read_text(encoding="utf-8")

    for check in (
        "desired_pointer_metadata",
        "completed_preparation_without_evidence",
        "completed_preparation_without_current_evidence_history",
        "invalid_downstream_head",
        "selection_physical_membership",
        "selection_shard_membership",
        "unknown_selection_consumer_coordinate",
        "consumer_checkpoint_beyond_journal",
        "unknown_journal_checkpoint_coordinate",
        "journal_checkpoint_beyond_boundary",
        "rolling_checkpoint_beyond_journal",
        "detector_checkpoint_beyond_journal",
        "current_pointer_without_release",
        "completed_publication_without_release",
        "enabled_read_pin_without_release",
        "STALE_UNTIL_NEW_LEASE",
    ):
        assert check in source
    assert "GREATEST(" not in source


def test_restore_checkpoint_offsets_use_matching_durable_boundaries():
    assert checkpoint_exceeds_boundary(
        coordinate=NORMALIZED_CHECKPOINT,
        checkpoint_offset=750,
        raw_committed_offset=1000,
        normalized_committed_offset=500,
    )
    assert not checkpoint_exceeds_boundary(
        coordinate=NORMALIZED_CHECKPOINT,
        checkpoint_offset=500,
        raw_committed_offset=1000,
        normalized_committed_offset=500,
    )
    assert not checkpoint_exceeds_boundary(
        coordinate=RAW_CHECKPOINT,
        checkpoint_offset=1000,
        raw_committed_offset=1000,
        normalized_committed_offset=500,
    )
    with pytest.raises(ValueError, match="unknown checkpoint coordinate"):
        checkpoint_exceeds_boundary(
            coordinate="UNDECLARED",
            checkpoint_offset=1,
            raw_committed_offset=1000,
            normalized_committed_offset=500,
        )


def test_backup_script_accepts_an_explicit_database_and_hardened_admin():
    source = (BACKEND_DIR / "scripts" / "backup_database.ps1").read_text(
        encoding="utf-8"
    )

    assert "[string]$Database" in source
    assert "POSTGRES_ADMIN_USER" in source
    assert "POSTGRES_ADMIN_PASSWORD" in source
    assert '"--dbname=$databaseName"' in source