from pathlib import Path


def test_requeue_is_exact_error_scoped_and_confirmation_gated():
    source = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "requeue_option_terminal_work.py"
    ).read_text(encoding="utf-8")

    assert "work.last_error = %s" in source
    assert "ingestion.configuration_sha256 = %s" in source
    assert "ingestion.policy_sha256 = %s" in source
    assert 'args.confirm != "REQUEUE"' in source
    assert "status = 'TERMINAL_FAILED'" in source
    assert "--all-current-config" in source
    assert "ingestion.configuration_sha256 = %s" in source