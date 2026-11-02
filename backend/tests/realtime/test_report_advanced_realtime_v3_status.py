import json

from scripts import report_advanced_realtime_v3_status as script


def test_compose_status_parses_service_health_and_never_returns_null(
    tmp_path,
):
    status_path = tmp_path / "compose-services.json"
    status_path.write_text(
        json.dumps([
            {
                "Service": "realtime-v3-options-lifecycle",
                "Name": "stock-screener-realtime-v3-options-lifecycle",
                "State": "running",
                "Health": "healthy",
                "ExitCode": 0,
            },
            {
                "Service": "realtime-v3-options-acquisition",
                "Name": "stock-screener-realtime-v3-options-acquisition",
                "State": "running",
                "Health": "healthy",
                "ExitCode": 0,
            },
        ]),
        encoding="utf-8",
    )

    services = script._compose_services(status_path)

    assert services == [
        {
            "service": "realtime-v3-options-lifecycle",
            "name": (
                "stock-screener-realtime-v3-options-lifecycle"
            ),
            "state": "running",
            "health": "healthy",
            "exit_code": 0,
        },
        {
            "service": "realtime-v3-options-acquisition",
            "name": (
                "stock-screener-realtime-v3-options-acquisition"
            ),
            "state": "running",
            "health": "healthy",
            "exit_code": 0,
        },
    ]
    assert services is not None
