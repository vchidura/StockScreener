import hashlib
import importlib
import json
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.runner_environment import StockAlertProjectionRequest


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


def test_exact_nine_reference_seed_satisfies_readiness():
    statements = []

    class Cursor:
        rows = iter((
            (
                "seed-1",
                "READY",
                script.datetime(
                    2026,
                    10,
                    27,
                    12,
                    tzinfo=script.timezone.utc,
                ),
                script.datetime(
                    2026,
                    10,
                    27,
                    18,
                    tzinfo=script.timezone.utc,
                ),
            ),
            (9, 9),
        ))

        def execute(self, statement, _parameters=None):
            statements.append(statement)

        def fetchone(self):
            return next(self.rows)

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=Cursor())

    readiness = []
    repository = SimpleNamespace(
        upsert_readiness=lambda *args: readiness.append(args)
    )
    args = SimpleNamespace(
        cohort_id=script.UUID(
            "11111111-1111-1111-1111-111111111111"
        ),
        cutoff=script.datetime(
            2026,
            10,
            27,
            14,
            tzinfo=script.timezone.utc,
        ),
    )

    script._ensure_reference_readiness(
        SimpleNamespace(transaction=transaction),
        repository,
        args,
    )

    assert "EQUITY_VALIDATION_REFERENCES" in statements[0]
    assert readiness[0][1:4] == (
        "REFERENCE",
        "stages_v3",
        "READY",
    )


def test_stock_alert_projection_reads_v2_repeatable_snapshot(monkeypatch):
    from realtime import canonical_stock_alert_repository

    statements = []
    transaction_options = []
    cursor = SimpleNamespace(
        execute=lambda statement: statements.append(statement)
    )

    @contextmanager
    def transaction(*_args, **kwargs):
        transaction_options.append(kwargs)
        yield SimpleNamespace(cursor=cursor)

    expected = {"projection_sha256": "a" * 64}

    def load(own_cursor, binding_request):
        assert own_cursor is cursor
        assert binding_request is request
        return expected

    monkeypatch.setattr(
        canonical_stock_alert_repository,
        "load_canonical_stock_alert_projection_cursor",
        load,
    )
    known_by = script.datetime(
        2026,
        10,
        27,
        17,
        tzinfo=script.timezone.utc,
    )

    request = StockAlertProjectionRequest(
        cohort_id=UUID("11111111-1111-4111-8111-111111111111"),
        window_id=UUID("22222222-2222-4222-8222-222222222222"),
        input_receipt_id=UUID("33333333-3333-4333-8333-333333333333"),
        cutoff=known_by,
        input_sha256="a" * 64,
        known_by=known_by,
    )
    assert script._load_stock_alert_projection(
        SimpleNamespace(transaction=transaction),
        request,
    ) == expected
    assert transaction_options == [{
        "read_only": True,
        "dict_cursor": True,
        "isolation_level": "REPEATABLE READ",
    }]
    assert statements == [
        "SET LOCAL search_path TO rt,ref,pg_catalog"
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


def test_development_diagnostic_equity_stops_at_projection():
    environment = {
        "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
        "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
    }
    policy = script._validation_policy(
        SimpleNamespace(publish=False),
        environment,
    )

    assert policy.equity_stage_order == script.BASE_STAGES
    assert policy.equity_terminal_receipt == "PORTAL_READY"
    assert not policy.release_allowed
    assert not policy.pointer_allowed
    assert not policy.read_pin_allowed

    with pytest.raises(
        RuntimeError,
        match="cannot publish",
    ):
        script._validation_policy(
            SimpleNamespace(publish=True),
            environment,
        )


def _history_binding(tmp_path):
    path = tmp_path / "history-binding.json"
    value = {
        "mode": "EXECUTE",
        "status": "BOUND",
        "target_cohort_id": (
            "11111111-1111-1111-1111-111111111111"
        ),
        "source_cohort_id": (
            "99999999-8888-7777-6666-555555555555"
        ),
        "symbols": [
            "AAPL",
            "AMZN",
            "IWM",
            "MSFT",
            "NFLX",
            "NVDA",
            "QQQ",
            "SPY",
            "TSLA",
        ],
        "binding_sha256": "b" * 64,
        "coverage": {
            "adjusted_daily_sessions": 283,
            "raw_daily_sessions": 90,
            "five_minute_bars": 1,
            "one_hour_bars": 1,
        },
        "provider_requests": 0,
    }
    path.write_text(json.dumps(value), encoding="utf-8")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_existing_alert_and_bound_strategy_stages_can_be_selected(
    tmp_path,
):
    alerts = script.parse_args([*BASE, "--include-alerts"])

    assert alerts.enabled_stages == (
        *script.BASE_STAGES,
        "STOCK_ALERTS",
    )
    path, digest = _history_binding(tmp_path)
    strategies = script.parse_args([
        *BASE,
        "--include-strategies",
        "--history-binding-report",
        str(path),
        "--history-binding-file-sha256",
        digest,
    ])

    assert strategies.enabled_stages == (
        *script.BASE_STAGES,
        "STRATEGY_PUBLISH",
    )
    assert (
        script._validate_strategy_history_binding(strategies)[
            "binding_sha256"
        ]
        == "b" * 64
    )
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
