from pathlib import Path

import pytest

from realtime.equity_pipeline_config import (
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)

POLICY = (
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)


def _advanced():
    return {
        "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
        "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
        "REALTIME_V2_APP_DSN": "postgresql://app:secret@db/stocks_realtime_v2",
        "REALTIME_V2_API_DSN": "postgresql://api:secret@db/stocks_realtime_v2",
        "POLYGON_API_KEY": "configured-provider-key",
        "REALTIME_V2_ADVANCED_JOURNAL_DIR": (
            "C:\\runtime\\stockscreener\\advanced-equity"
        ),
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH": str(POLICY),
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION": (
            "advanced-equity-ingress-v1"
        ),
    }


def test_advanced_pipeline_requires_exact_am_and_separate_credentials():
    config = RealtimeV2PipelineConfig.from_mapping(_advanced())

    assert config.equity_pipeline is EquityPipeline.MASSIVE_ADVANCED
    assert config.options_pipeline is OptionsPipeline.DISABLED
    assert config.advanced_ingress_policy_path == POLICY
    assert config.advanced_ingress_policy_version == (
        "advanced-equity-ingress-v1"
    )
    assert config.advanced_journal_dir == Path(
        "C:\\runtime\\stockscreener\\advanced-equity"
    )
    assert "secret" not in repr(config)
    assert "configured-provider-key" not in repr(config)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    (
        ("REALTIME_V2_APP_DSN", "...", "placeholder"),
        ("POLYGON_API_KEY", "your_polygon_api_key_here", "placeholder"),
        (
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            "",
            "missing",
        ),
    ),
)
def test_advanced_pipeline_fails_closed_for_unreviewed_values(
    name,
    value,
    message,
):
    settings = _advanced()
    settings[name] = value

    with pytest.raises(ValueError, match=message):
        RealtimeV2PipelineConfig.from_mapping(settings)


def test_developer_pipeline_does_not_activate_advanced_runtime_values():
    settings = _advanced()
    settings["REALTIME_V2_EQUITY_PIPELINE"] = "MASSIVE_DEVELOPER"

    config = RealtimeV2PipelineConfig.from_mapping(settings)

    assert config.equity_pipeline is EquityPipeline.MASSIVE_DEVELOPER
    assert config.advanced_journal_dir is None
    assert config.advanced_ingress_policy_path is None


@pytest.mark.parametrize(
    ("process", "required", "absent"),
    (
        (
            RealtimeV2Process.CONTROLLER,
            {
                "REALTIME_V2_APP_DSN",
                "POLYGON_API_KEY",
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            },
            {"REALTIME_V2_API_DSN"},
        ),
        (
            RealtimeV2Process.BRIDGE,
            {
                "REALTIME_V2_APP_DSN",
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
            },
            {
                "REALTIME_V2_API_DSN",
                "POLYGON_API_KEY",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            },
        ),
        (
            RealtimeV2Process.SCHEDULER,
            {
                "REALTIME_V2_APP_DSN",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            },
            {
                "REALTIME_V2_API_DSN",
                "POLYGON_API_KEY",
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
            },
        ),
        (
            RealtimeV2Process.APP_WORKER,
            {"REALTIME_V2_APP_DSN"},
            {
                "REALTIME_V2_API_DSN",
                "POLYGON_API_KEY",
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            },
        ),
        (
            RealtimeV2Process.API,
            {"REALTIME_V2_API_DSN"},
            {
                "REALTIME_V2_APP_DSN",
                "POLYGON_API_KEY",
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
            },
        ),
    ),
)
def test_process_configuration_requires_only_owned_secrets(
    process,
    required,
    absent,
):
    settings = _advanced()
    for name in absent:
        settings.pop(name)

    config = RealtimeV2PipelineConfig.from_mapping(
        settings,
        process=process,
    )

    assert config.process is process
    assert (config.app_dsn is not None) == (
        "REALTIME_V2_APP_DSN" in required
    )
    assert (config.api_dsn is not None) == (
        "REALTIME_V2_API_DSN" in required
    )
    assert (config.massive_api_key is not None) == (
        "POLYGON_API_KEY" in required
    )
