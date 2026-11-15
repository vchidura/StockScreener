from scripts import configure_realtime_v2_advanced_environment as script


def test_safe_advanced_configuration_binds_runtime_policy(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    args = script.parse_args([
        "--env-file",
        str(env_file),
        "--journal-directory",
        str(tmp_path),
        "--ingress-policy-path",
        str(script.BACKEND_DIR / "realtime" / "policies"
            / "advanced_equity_ingress_v1.json"),
        "--ingress-policy-version",
        "advanced-equity-ingress-v1",
    ])

    assert args.safe_settings[
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH"
    ].endswith("advanced_equity_ingress_v1.json")
    assert args.safe_settings[
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION"
    ] == "advanced-equity-ingress-v1"


def test_retired_scale_settings_are_not_written_and_are_removed(tmp_path):
    """Universe sizing (Nov 15): the October scale-ladder settings are read by no code; the writer removes them."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "KEEP=1" + chr(10) + "REALTIME_V2_ADVANCED_UNIVERSE_LADDER=1,9,25,50,100,200,400" + chr(10)
        + "REALTIME_V2_ADVANCED_UNIVERSE_TARGET=9" + chr(10),
        encoding="utf-8",
    )
    argv = [
        "--env-file", str(env_file), "--journal-directory", str(tmp_path),
        "--ingress-policy-path", str(script.BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json"),
        "--execute",
    ]
    assert not set(script.RETIRED_SETTINGS) & set(script.parse_args(argv).safe_settings)
    assert script.main(argv) == 0
    text = env_file.read_text(encoding="utf-8")
    assert "KEEP=1" in text
    assert "UNIVERSE_LADDER" not in text and "UNIVERSE_TARGET" not in text
