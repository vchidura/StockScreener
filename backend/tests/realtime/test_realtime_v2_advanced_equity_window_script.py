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


def _run_reference_gate(rows, *, continuous_publication=False):
    statements = []
    remaining = iter(rows)
    readiness = []

    class Cursor:
        def execute(self, statement, _parameters=None):
            statements.append(statement)

        def fetchone(self):
            return next(remaining)

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=Cursor())

    args = SimpleNamespace(
        cohort_id=script.UUID(
            "11111111-1111-1111-1111-111111111111"
        ),
        session_date=script.datetime(2026, 10, 27).date(),
        cutoff=script.datetime(
            2026,
            10,
            27,
            14,
            tzinfo=script.timezone.utc,
        ),
        continuous_publication=continuous_publication,
    )
    try:
        script._ensure_reference_readiness(
            SimpleNamespace(transaction=transaction),
            SimpleNamespace(
                upsert_readiness=lambda *values: readiness.append(values)
            ),
            args,
        )
    except ValueError as error:
        return statements, readiness, error
    return statements, readiness, None


def _seed_row(valid_until_hour):
    return (
        "seed-1",
        "READY",
        script.datetime(2026, 10, 27, 12, tzinfo=script.timezone.utc),
        script.datetime(
            2026,
            10,
            27,
            valid_until_hour,
            tzinfo=script.timezone.utc,
        ),
    )


def test_exact_nine_reference_seed_satisfies_readiness():
    statements, readiness, error = _run_reference_gate(
        (None, _seed_row(18), (9, 9))
    )

    assert error is None
    assert "realtime_desired_session_snapshots" in statements[0]
    assert "EQUITY_VALIDATION_REFERENCES" in statements[1]
    assert readiness[0][1:4] == (
        "REFERENCE",
        "stages_v3",
        "READY",
    )
    assert readiness[0][5] == "advanced-reference:seed-1"


def test_continuous_cohort_is_proven_without_a_global_seed():
    statements, readiness, error = _run_reference_gate(
        (("a" * 64,), (9, 9, 9))
    )

    assert error is None
    assert not any("rt.seed_current" in row for row in statements)
    assert "ref.corporate_action_coverage" in statements[1]
    assert readiness[0][1:4] == (
        "REFERENCE",
        "stages_v3",
        "READY",
    )
    assert readiness[0][5] == "advanced-reference-c4:" + "a" * 64


@pytest.mark.parametrize(
    "members",
    ((9, 8, 9), (9, 9, 10), (10, 9, 9), None),
)
def test_continuous_cohort_requires_every_member_and_never_falls_back(
    members,
):
    statements, readiness, error = _run_reference_gate(
        (("a" * 64,), members)
    )

    assert "continuous member references" in str(error)
    assert readiness == []
    assert not any("rt.seed_current" in row for row in statements)


@pytest.mark.parametrize(
    "rows",
    (
        (None, _seed_row(13), (9, 9)),
        (None, None, (9, 9)),
        (None, _seed_row(18), (9, 8)),
    ),
)
def test_other_cohorts_still_require_a_current_global_seed(rows):
    _statements, readiness, error = _run_reference_gate(rows)

    assert "reference readiness is unavailable" in str(error)
    assert readiness == []


def test_continuous_publication_requires_a_desired_snapshot_cohort():
    _statements, readiness, error = _run_reference_gate(
        (None, _seed_row(18), (9, 9)),
        continuous_publication=True,
    )

    assert "requires a desired-snapshot cohort" in str(error)
    assert readiness == []


def test_continuous_publication_enables_the_publish_stage_without_a_file():
    args = script.parse_args([*BASE, "--continuous-publication"])

    assert "PORTAL_PUBLISH" in args.enabled_stages
    assert args.publication_authority is None
    assert "PORTAL_PUBLISH" not in script.parse_args(BASE).enabled_stages


def test_continuous_publication_replaces_the_one_shot_publish_path():
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--continuous-publication",
            "--publish",
            "--publication-reference=one-shot",
        ])


def test_diagnostic_mode_cannot_publish_continuously():
    with pytest.raises(RuntimeError, match="cannot publish"):
        script._validation_policy(
            SimpleNamespace(publish=False, continuous_publication=True),
            {
                "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
                "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
            },
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
        SimpleNamespace(publish=False, continuous_publication=False),
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


def test_retry_execution_deadline_is_explicit_and_timezone_aware():
    deadline = "2026-10-16T16:34:30+00:00"
    args = script.parse_args([
        *BASE,
        "--window-key-suffix=-retry1",
        "--retry-release-now",
        "--execution-deadline",
        deadline,
    ])

    assert args.execution_deadline.isoformat() == deadline

    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--execution-deadline",
            deadline,
        ])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--window-key-suffix=-retry1",
            "--retry-release-now",
            "--execution-deadline",
            "2026-10-16T16:34:30",
        ])


# G85: the incremental loader and projection state stay warm between windows of one cohort.
COHORT_A = UUID("11111111-1111-1111-1111-111111111111")
COHORT_B = UUID("22222222-2222-2222-2222-222222222222")


def _at(minute):
    from datetime import datetime, timezone
    return datetime(2026, 10, 16, 16, minute, tzinfo=timezone.utc)


def test_warm_pair_serves_only_newer_cutoffs_of_the_same_cohort():
    warm = script._WarmProjection()
    state, loader, used = warm.take(COHORT_A, _at(30))
    assert used is False
    warm.settle(COHORT_A, _at(30), state, loader, warm=False, complete=True)

    again = warm.take(COHORT_A, _at(35))
    assert again == (state, loader, True)
    warm.settle(COHORT_A, _at(35), state, loader, warm=True, complete=True)
    assert warm.cutoff == _at(35)

    # A retry or recovery of an equal or older cutoff gets a fresh pair and leaves the warm one alone.
    for cutoff in (_at(35), _at(30)):
        retry_state, retry_loader, used = warm.take(COHORT_A, cutoff)
        assert used is False and retry_state is not state and retry_loader is not loader
        warm.settle(COHORT_A, cutoff, retry_state, retry_loader, warm=False, complete=True)
        assert (warm.state, warm.loader, warm.cutoff) == (state, loader, _at(35))


def test_a_window_that_does_not_complete_drops_the_warm_pair():
    warm = script._WarmProjection()
    state, loader, _ = warm.take(COHORT_A, _at(30))
    warm.settle(COHORT_A, _at(30), state, loader, warm=False, complete=True)
    state, loader, used = warm.take(COHORT_A, _at(35))
    assert used is True
    warm.settle(COHORT_A, _at(35), state, loader, warm=True, complete=False)
    assert warm.state is None and warm.loader is None
    assert warm.take(COHORT_A, _at(40))[2] is False


def test_a_new_cohort_starts_cold_and_becomes_the_warm_one():
    warm = script._WarmProjection()
    state, loader, _ = warm.take(COHORT_A, _at(30))
    warm.settle(COHORT_A, _at(30), state, loader, warm=False, complete=True)
    other_state, other_loader, used = warm.take(COHORT_B, _at(35))
    assert used is False and other_state is not state
    warm.settle(COHORT_B, _at(35), other_state, other_loader, warm=False, complete=True)
    assert (warm.cohort_id, warm.state) == (COHORT_B, other_state)


@pytest.mark.parametrize("outcome", [0, 2, RuntimeError("stage blew up")])
def test_main_passes_the_warm_pair_into_the_window_and_settles_it(monkeypatch, outcome):
    warm = script._WarmProjection()
    monkeypatch.setattr(script, "_WARM", warm)
    seen = []

    def fake_main(args, *, ticker_state, screening_loader, dashboard_due):
        seen.append((args.cutoff, ticker_state, screening_loader, dashboard_due))
        if len(seen) == 1:
            return 0
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(script, "_main", fake_main)
    assert script.main(list(BASE)) == 0
    warm_state, warm_loader = seen[0][1], seen[0][2]
    later = [*BASE[:9], "2026-10-16T16:35:00+00:00", *BASE[10:]]
    if isinstance(outcome, Exception):
        with pytest.raises(RuntimeError):
            script.main(later)
    else:
        assert script.main(later) == outcome
    assert seen[1][1] is warm_state and seen[1][2] is warm_loader
    # The first window of the process builds the Dashboard; 16:35 is an hourly :35 window.
    assert seen[0][3] is True and seen[1][3] is True
    if outcome == 0:
        assert warm.state is warm_state
    else:
        assert warm.state is None


@pytest.mark.parametrize("minute,warm,due", [
    (35, False, True),   # first window of a process or cohort
    (40, False, True),   # retry or restart with a fresh pair
    (40, True, False),   # warm, between hourly rebuilds: the previous Dashboard carries forward
    (35, True, True),    # hourly rebuild
    (0, True, False),
])
def test_dashboard_is_rebuilt_on_fresh_pairs_and_hourly(minute, warm, due):
    assert script.dashboard_due(_at(minute), warm=warm) is due
