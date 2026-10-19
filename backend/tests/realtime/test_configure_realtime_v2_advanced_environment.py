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
        "--universe-target",
        "1",
    ])

    assert args.safe_settings[
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH"
    ].endswith("advanced_equity_ingress_v1.json")
    assert args.safe_settings[
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION"
    ] == "advanced-equity-ingress-v1"
