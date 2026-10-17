import importlib

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_journal_replay"
)


def test_replay_cli_requires_explicit_mode_execution_and_mounted_root(
    tmp_path,
):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    root = tmp_path / "journals"
    root.mkdir()
    base = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--journal-root",
        str(root),
        "--env-file",
        str(env_file),
        "--execute",
    ]

    args = script.parse_args([*base, "--once"])

    assert args.once
    assert args.journal_root == root.resolve()
    with pytest.raises(SystemExit):
        script.parse_args(base)
    with pytest.raises(SystemExit):
        script.parse_args([*base[:-1], "--once"])
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--once", "--continuous"])
