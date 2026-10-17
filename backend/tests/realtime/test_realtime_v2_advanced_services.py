import importlib
from pathlib import Path

from realtime.equity_pipeline_config import RealtimeV2PipelineConfig


script = importlib.import_module("scripts.run_realtime_v2_advanced_services")
selector = importlib.import_module("scripts.run_realtime_v2_equity_services")
BASE = [
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--authorization-id",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "--universe-target",
    "1",
    "--session-date",
    "2026-10-16",
    "--first-cutoff",
    "2026-10-16T13:35:00+00:00",
    "--authorization-reference",
    "advanced-equity-v2-20261016",
    "--decoded-byte-ceiling",
    "10485760",
    "--stream-sessions",
    "3",
    "--connection-attempts",
    "3",
    "--rest-repair-requests",
    "0",
    "--rest-repair-decoded-bytes",
    "0",
    "--finality-delay-seconds",
    "5",
    "--run-directory",
    "advanced-services",
    "--execute",
]


def _config(pipeline):
    values = {
        "REALTIME_V2_EQUITY_PIPELINE": pipeline,
        "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
        "REALTIME_V2_APP_DSN": "postgresql://app:secret@db/stocks_realtime_v2",
        "REALTIME_V2_API_DSN": "postgresql://api:secret@db/stocks_realtime_v2",
        "POLYGON_API_KEY": "configured",
    }
    if pipeline == "MASSIVE_ADVANCED":
        values.update({
            "REALTIME_V2_ADVANCED_JOURNAL_DIR": "C:\\runtime\\advanced",
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH": str(
                Path(__file__).resolve().parents[2]
                / "realtime"
                / "policies"
                / "advanced_equity_ingress_v1.json"
            ),
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION": (
                "advanced-equity-ingress-v1"
            ),
        })
    return RealtimeV2PipelineConfig.from_mapping(values)


def test_advanced_supervisor_has_no_developer_rest_process(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args(BASE)
    commands = script.build_commands(args)

    assert set(commands) == {
        "api",
        "frontend",
        "finalizer",
        "bridge",
        "controller",
        "scheduler",
    }
    flattened = " ".join(
        value
        for command in commands.values()
        for value in command
    )
    assert "run_realtime_v2_advanced_controller.py" in flattened
    assert "run_realtime_v2_advanced_journal_replay.py" in flattened
    assert "run_realtime_v2_advanced_equity_scheduler.py" in flattened
    assert "developer" not in flattened.lower()
    assert "--publish" not in flattened
    controller = commands["controller"]
    assert "--authorization-id" in controller
    assert "--universe-target" in controller
    assert "--cohort-id" not in controller
    assert "--authorization-reference" not in controller


def test_environment_selector_chooses_separate_supervisors():
    advanced_main = selector.selected_main(
        _config("MASSIVE_ADVANCED")
    )
    developer_main = selector.selected_main(
        _config("MASSIVE_DEVELOPER")
    )

    assert advanced_main.__module__.endswith(
        "run_realtime_v2_advanced_services"
    )
    assert developer_main.__module__.endswith(
        "run_realtime_v2_services"
    )
