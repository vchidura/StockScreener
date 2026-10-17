import importlib

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_scheduler"
)
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--authorization-id",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--session-date",
    "2026-10-16",
    "--first-cutoff",
    "2026-10-16T13:35:00+00:00",
    "--finality-delay-seconds",
    "5",
    "--authorization-reference",
    "advanced-equity-v2-20261016",
    "--report-directory",
    "advanced-reports",
    "--continuous",
    "--execute",
]


def test_scheduler_builds_exact_remaining_session_windows(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args(BASE)

    assert len(args.windows) == 78
    assert args.windows[0].cutoff.minute == 35
    assert args.report_directory == (
        tmp_path / "advanced-reports"
    ).resolve()


def test_scheduler_rejects_non_boundary_and_unexecuted_modes():
    with pytest.raises(SystemExit):
        script.parse_args(BASE[:-1])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("2026-10-16T13:35:00+00:00")],
            "2026-10-16T13:36:00+00:00",
            *BASE[BASE.index("2026-10-16T13:35:00+00:00") + 1:],
        ])
