import importlib

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_window"
)
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--session-date",
    "2026-10-16",
    "--cutoff",
    "2026-10-16T16:30:00+00:00",
    "--finality-delay-seconds",
    "5",
    "--authorization-reference",
    "advanced-equity-v2-20261016",
    "--report-path",
    "advanced-window.json",
    "--execute",
]


def test_advanced_window_is_unpublished_by_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args(BASE)

    assert args.enabled_stages == (
        "EQUITY_INPUT_GATE",
        "EQUITY_MATERIALIZE",
        "EQUITY_PROJECTION",
    )
    assert not args.publish


def test_alert_and_strategy_stages_remain_disabled():
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--include-alerts"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--include-strategies"])


def test_p11_publication_requires_exact_authority_and_receipt_retry(
    tmp_path,
):
    authority = tmp_path / "authority.json"
    authority.write_text("{}", encoding="utf-8")
    digest = "a" * 64
    publish = [
        *BASE[:BASE.index("advanced-window.json")],
        str(tmp_path / "advanced-window.json"),
        *BASE[BASE.index("advanced-window.json") + 1:],
        "--publish",
        "--publication-reference",
        "P11-TEST",
        "--publication-authority",
        str(authority),
        "--publication-authority-semantic-sha256",
        digest,
        "--publication-authority-file-sha256",
        digest,
    ]
    with pytest.raises(SystemExit):
        script.parse_args(publish)

    args = script.parse_args([
        *publish,
        "--window-key-suffix=-retry1",
        "--retry-release-now",
        "--reuse-input-receipt",
        "--reuse-portal-receipt",
    ])

    assert args.enabled_stages[-1] == "PORTAL_PUBLISH"
    assert args.publish


def test_window_requires_execution_and_aligned_scope():
    with pytest.raises(SystemExit):
        script.parse_args(BASE[:-1])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("2026-10-16T16:30:00+00:00")],
            "2026-10-17T16:30:00+00:00",
            *BASE[BASE.index("2026-10-16T16:30:00+00:00") + 1:],
        ])


def test_receipt_reuse_requires_a_durable_retry_identity():
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--reuse-input-receipt"])

    args = script.parse_args([
        *BASE,
        "--window-key-suffix=-retry1",
        "--retry-release-now",
        "--reuse-input-receipt",
    ])

    assert args.window_key_suffix == "-retry1"
    assert args.reuse_input_receipt

    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--window-key-suffix=-retry1",
            "--retry-release-now",
            "--reuse-portal-receipt",
        ])
