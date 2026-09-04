import inspect
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent


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
    ):
        assert table in source
    assert "validate_equity_storage.py" in source
    assert "CANONICAL_RESTORE_VALIDATED" in source