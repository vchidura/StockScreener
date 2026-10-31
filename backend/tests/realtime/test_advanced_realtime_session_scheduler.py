from __future__ import annotations

import json
import re
import threading
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.acquisition_ledger import ProviderRequestLane
from realtime.advanced_options_v3_authority import (
    SessionAcquisitionAuthorization,
    session_acquisition_authorization_payload,
)
from realtime.advanced_realtime_session_scheduler import (
    ADVANCED_SESSION_EQUITY_SYMBOLS,
    AdvancedRealtimeSessionScheduler,
    AdvancedSessionProviderContract,
    SchedulerEquityReceipt,
    SchedulerOptionsCapture,
    SchedulerPublishedCycle,
    advanced_realtime_session_authorization_from_payload,
    advanced_realtime_session_authorization_payload,
    build_advanced_realtime_session_authorization,
    build_advanced_session_windows,
)
from realtime.provider_budget import ProviderBudgetScope
from scripts import qualify_advanced_realtime_session_scheduler as qualification_cli
from scripts import run_advanced_realtime_session_scheduler as cli


UTC = timezone.utc
SESSION_DATE = date(2026, 10, 28)
CONTRACTS = (
    "O:QQQ261028C00600000",
    "O:QQQ261028P00600000",
    "O:SPY261028C00670000",
    "O:SPY261028P00670000",
)
EQUITY_AUTHORIZATION_ID = UUID(
    "11111111-2222-3333-4444-555555555555"
)
COHORT_ID = UUID("22222222-3333-4444-5555-666666666666")
SESSION_AUTHORIZATION_ID = UUID(
    "33333333-4444-5555-6666-777777777777"
)


def _session_authority(decision_stop):
    authority = SessionAcquisitionAuthorization.__new__(
        SessionAcquisitionAuthorization
    )
    values = {
        "authorization_id": SESSION_AUTHORIZATION_ID,
        "authorization_sha256": "a" * 64,
        "authorization_reference": "options-session-test",
        "cohort_id": COHORT_ID,
        "session_date": SESSION_DATE,
        "prepared_at": datetime(
            2026,
            10,
            28,
            13,
            15,
            tzinfo=UTC,
        ),
        "acquisition_start": datetime(
            2026,
            10,
            28,
            13,
            20,
            tzinfo=UTC,
        ),
        "capture_boundary": datetime(
            2026,
            10,
            28,
            13,
            30,
            tzinfo=UTC,
        ),
        "acquisition_stop": decision_stop,
        "admission_id": UUID(
            "44444444-5555-6666-7777-888888888888"
        ),
        "admission_sha256": "b" * 64,
        "readiness_sha256": "c" * 64,
        "slot_set_member_sha256": "d" * 64,
        "provider_budget_sha256": "e" * 64,
        "socket_budget_sha256": "f" * 64,
        "journal_generation": UUID(
            "55555555-6666-7777-8888-999999999999"
        ),
        "journal_identity_sha256": "1" * 64,
    }
    for name, value in values.items():
        object.__setattr__(authority, name, value)
    return authority


def _provider_contract(*, request_ceiling=78):
    response_ceiling = 1_000_000
    decision_stop = datetime(
        2026,
        10,
        28,
        20,
        5,
        tzinfo=UTC,
    )
    return AdvancedSessionProviderContract(
        budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=SESSION_DATE,
            authorization_reference="session-scheduler-test",
            provider_authorization_reference="provider-test",
            expected_cost="exact unified snapshot grant",
            request_ceiling=request_ceiling,
            decoded_byte_ceiling=(
                request_ceiling * response_ceiling
            ),
            lanes=frozenset({
                ProviderRequestLane.OPTIONS_ALIGNMENT,
            }),
            authorized_until=decision_stop,
        ),
        response_byte_ceiling=response_ceiling,
        contract_tickers=CONTRACTS,
    )


def _authority():
    decision_stop = datetime(
        2026,
        10,
        28,
        20,
        5,
        tzinfo=UTC,
    )
    return build_advanced_realtime_session_authorization(
        authorization_reference="session-scheduler-test",
        session_date=SESSION_DATE,
        prepared_at=datetime(
            2026,
            10,
            28,
            13,
            0,
            tzinfo=UTC,
        ),
        acquisition_start=datetime(
            2026,
            10,
            28,
            13,
            20,
            tzinfo=UTC,
        ),
        equity_authorization_id=EQUITY_AUTHORIZATION_ID,
        equity_cohort_id=COHORT_ID,
        options_session_authority=_session_authority(
            decision_stop
        ),
        package_sha256="2" * 64,
        bundle_sha256="3" * 64,
        provider_contract=_provider_contract(),
    )


def test_20261028_schedule_is_exact_target9_and_78_five_minute_cycles():
    authority = _authority()
    payload = advanced_realtime_session_authorization_payload(
        authority
    )

    assert len(authority.windows) == 78
    assert authority.windows[0].equity_cutoff == datetime(
        2026,
        10,
        28,
        13,
        35,
        tzinfo=UTC,
    )
    assert authority.windows[-1].equity_cutoff == datetime(
        2026,
        10,
        28,
        20,
        0,
        tzinfo=UTC,
    )
    assert authority.decision_stop == datetime(
        2026,
        10,
        28,
        20,
        5,
        tzinfo=UTC,
    )
    assert payload["equity"]["symbols"] == list(
        ADVANCED_SESSION_EQUITY_SYMBOLS
    )
    assert payload["options"]["underlyings"] == ["QQQ", "SPY"]
    assert payload["options"][
        "nine_underlying_expansion_authorized"
    ] is False
    assert payload["provider_contract"]["budget"][
        "request_ceiling"
    ] == 78
    assert payload["provider_contract"]["requests_per_cycle"] == 1
    assert payload["provider_contract"]["query"][
        "ticker.any_of"
    ] == ",".join(sorted(CONTRACTS))
    assert payload["authority"]["canonical_publication"] is True
    assert payload["authority"]["stored_read_pin"] is True
    assert payload["authority"]["dispatch"] is False
    assert advanced_realtime_session_authorization_from_payload(
        payload
    ) == authority


def test_short_session_derives_42_windows_deadline_and_provider_budget():
    session_date = date(2026, 11, 27)
    windows = build_advanced_session_windows(session_date)
    decision_stop = windows[-1].latest_options_cutoff
    options = SessionAcquisitionAuthorization.__new__(
        SessionAcquisitionAuthorization
    )
    for name, value in {
        "authorization_id": SESSION_AUTHORIZATION_ID,
        "authorization_sha256": "a" * 64,
        "cohort_id": COHORT_ID,
        "session_date": session_date,
        "acquisition_stop": decision_stop,
    }.items():
        object.__setattr__(options, name, value)
    response_ceiling = 1_000_000
    provider = AdvancedSessionProviderContract(
        budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=session_date,
            authorization_reference="short-session-test",
            provider_authorization_reference="provider-short-test",
            expected_cost="calendar-derived short session",
            request_ceiling=len(windows),
            decoded_byte_ceiling=len(windows) * response_ceiling,
            lanes=frozenset({ProviderRequestLane.OPTIONS_ALIGNMENT}),
            authorized_until=decision_stop,
        ),
        response_byte_ceiling=response_ceiling,
        contract_tickers=CONTRACTS,
    )

    authority = build_advanced_realtime_session_authorization(
        authorization_reference="short-session-test",
        session_date=session_date,
        prepared_at=datetime(2026, 11, 27, 14, 0, tzinfo=UTC),
        acquisition_start=datetime(2026, 11, 27, 14, 10, tzinfo=UTC),
        equity_authorization_id=EQUITY_AUTHORIZATION_ID,
        equity_cohort_id=COHORT_ID,
        options_session_authority=options,
        package_sha256="2" * 64,
        bundle_sha256="3" * 64,
        provider_contract=provider,
    )
    payload = advanced_realtime_session_authorization_payload(authority)

    assert len(authority.windows) == 42
    assert authority.market_capture_stop == datetime(
        2026, 11, 27, 18, 0, tzinfo=UTC
    )
    assert authority.decision_stop == datetime(
        2026, 11, 27, 18, 5, tzinfo=UTC
    )
    assert authority.provider_contract.budget.request_ceiling == 42
    assert payload["run_scope"] == {
        "kind": "FULL_EXCHANGE_SESSION",
        "window_count": 42,
        "first_window_index": 1,
        "last_window_index": 42,
    }
    assert advanced_realtime_session_authorization_from_payload(
        payload
    ) == authority


def test_calendar_windows_cover_holiday_and_dst_transitions():
    with pytest.raises(ValueError):
        build_advanced_session_windows(date(2026, 11, 26))
    with pytest.raises(ValueError):
        build_advanced_session_windows(date(2026, 10, 31))

    before_dst = build_advanced_session_windows(date(2026, 3, 6))
    after_dst = build_advanced_session_windows(date(2026, 3, 9))
    assert len(before_dst) == len(after_dst) == 78
    assert before_dst[0].equity_cutoff.hour == 14
    assert after_dst[0].equity_cutoff.hour == 13
    assert (
        before_dst[-1].latest_options_cutoff
        - before_dst[0].equity_cutoff
    ) == (
        after_dst[-1].latest_options_cutoff
        - after_dst[0].equity_cutoff
    )


def test_scheduler_contract_refuses_naive_156_request_budget():
    with pytest.raises(
        ValueError,
        match="scheduler scope is invalid",
    ):
        build_advanced_realtime_session_authorization(
            authorization_reference="session-scheduler-test",
            session_date=SESSION_DATE,
            prepared_at=datetime(
                2026,
                10,
                28,
                13,
                0,
                tzinfo=UTC,
            ),
            acquisition_start=datetime(
                2026,
                10,
                28,
                13,
                20,
                tzinfo=UTC,
            ),
            equity_authorization_id=EQUITY_AUTHORIZATION_ID,
            equity_cohort_id=COHORT_ID,
            options_session_authority=_session_authority(
                datetime(
                    2026,
                    10,
                    28,
                    20,
                    5,
                    tzinfo=UTC,
                )
            ),
            package_sha256="2" * 64,
            bundle_sha256="3" * 64,
            provider_contract=_provider_contract(
                request_ceiling=156
            ),
        )


def test_scheduler_contract_refuses_nine_underlying_options_scope():
    with pytest.raises(
        ValueError,
        match="provider contract is invalid",
    ):
        replace(
            _provider_contract(),
            contract_tickers=tuple(
                f"O:{symbol}261028C00100000"
                for symbol in ADVANCED_SESSION_EQUITY_SYMBOLS
            ),
        )


def test_provider_free_plan_reports_exact_grant_without_effects(
    tmp_path,
    capsys,
):
    path = tmp_path / "scheduler-authorization.json"
    path.write_text(
        json.dumps(
            advanced_realtime_session_authorization_payload(
                _authority()
            )
        ),
        encoding="utf-8",
    )

    assert cli.main([
        "plan",
        "--authorization",
        str(path),
    ]) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["status"] == (
        "ACTIVATION_AND_BUDGET_GRANT_REQUIRED"
    )
    assert report["decision_cycles"] == 78
    assert report["options_rest_requests"] == 78
    assert report["provider_effects"] == 0
    assert report["production_effects"] == 0
    assert report["host_schedule_installed"] is False
    assert (
        "realtime-v2-canonical-stock-alerts"
        in cli.EQUITY_SERVICES
    )
    assert "realtime-v2-stock-alert-results" not in cli.EQUITY_SERVICES


def test_qualification_and_authorization_accept_an_xnys_session(
    tmp_path,
):
    session_date = date(2026, 10, 30)
    qualification = qualification_cli.parse_args([
        "--session-date",
        session_date.isoformat(),
        "--output-directory",
        str(tmp_path / "qualification"),
        "--execute",
    ])
    assert qualification.session_date == session_date

    package = tmp_path / "package.json"
    bundle = tmp_path / "bundle.json"
    package.write_text("{}", encoding="utf-8")
    bundle.write_text("{}", encoding="utf-8")
    authorization = cli.parse_args([
        "prepare-authorization",
        "--authorization-reference",
        "advanced-realtime-v3-20261030-test",
        "--provider-authorization-reference",
        "provider-20261030-test",
        "--expected-cost",
        "test",
        "--session-date",
        session_date.isoformat(),
        "--prepared-at",
        "2026-10-30T12:40:00+00:00",
        "--acquisition-start",
        "2026-10-30T13:10:00+00:00",
        "--equity-authorization-id",
        str(EQUITY_AUTHORIZATION_ID),
        "--equity-cohort-id",
        str(COHORT_ID),
        "--response-byte-ceiling",
        "1048576",
        "--package",
        str(package),
        "--bundle",
        str(bundle),
        "--output",
        str(tmp_path / "authorization.json"),
        "--execute",
    ])
    assert authorization.session_date == session_date


def test_qualification_and_authorization_reject_a_non_session(
    tmp_path,
):
    with pytest.raises(SystemExit):
        qualification_cli.parse_args([
            "--session-date",
            "2026-10-31",
            "--output-directory",
            str(tmp_path / "qualification"),
            "--execute",
        ])

    package = tmp_path / "package.json"
    bundle = tmp_path / "bundle.json"
    package.write_text("{}", encoding="utf-8")
    bundle.write_text("{}", encoding="utf-8")
    with pytest.raises(SystemExit):
        cli.parse_args([
            "prepare-authorization",
            "--authorization-reference",
            "advanced-realtime-v3-weekend-test",
            "--provider-authorization-reference",
            "provider-weekend-test",
            "--expected-cost",
            "test",
            "--session-date",
            "2026-10-31",
            "--prepared-at",
            "2026-10-30T12:40:00+00:00",
            "--acquisition-start",
            "2026-10-30T13:10:00+00:00",
            "--equity-authorization-id",
            str(EQUITY_AUTHORIZATION_ID),
            "--equity-cohort-id",
            str(COHORT_ID),
            "--response-byte-ceiling",
            "1048576",
            "--package",
            str(package),
            "--bundle",
            str(bundle),
            "--output",
            str(tmp_path / "authorization.json"),
            "--execute",
        ])


def test_provider_free_authorization_preparation_binds_exact_artifacts(
    tmp_path,
    monkeypatch,
):
    package_path = tmp_path / "PACKAGE.json"
    bundle_path = tmp_path / "SESSION-BUNDLE.json"
    output = tmp_path / "scheduler-authorization.json"
    package = {
        "package_sha256": "2" * 64,
        "v3_admitted_contracts": list(CONTRACTS),
    }
    session = _session_authority(
        datetime(2026, 10, 28, 20, 5, tzinfo=UTC)
    )
    bundle = {
        "bundle_sha256": "3" * 64,
        "v3_authorities": {
            "session_acquisition": (
                session_acquisition_authorization_payload(session)
            ),
        },
    }
    package_path.write_text(
        json.dumps(package),
        encoding="utf-8",
    )
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "validate_p8_execution_bundle",
        lambda *_: bundle,
    )
    monkeypatch.setattr(
        cli,
        "session_acquisition_authorization_from_payload",
        lambda *_: session,
    )

    result = cli._prepare_authorization(SimpleNamespace(
        package=package_path,
        bundle=bundle_path,
        authorization_reference="session-scheduler-test",
        provider_authorization_reference="fresh-grant-test",
        expected_cost=(
            "included Massive Advanced aggregate entitlement"
        ),
        session_date=SESSION_DATE,
        prepared_at=datetime(
            2026,
            10,
            28,
            12,
            40,
            tzinfo=UTC,
        ),
        acquisition_start=datetime(
            2026,
            10,
            28,
            13,
            10,
            tzinfo=UTC,
        ),
        equity_authorization_id=EQUITY_AUTHORIZATION_ID,
        equity_cohort_id=COHORT_ID,
        response_byte_ceiling=1_048_576,
        output=output,
    ))
    authority = advanced_realtime_session_authorization_from_payload(
        json.loads(output.read_text(encoding="utf-8"))
    )

    assert result["status"] == "SCHEDULER_AUTHORIZATION_PREPARED"
    assert result["provider_effects"] == 0
    assert result["production_effects"] == 0
    assert authority.provider_contract.budget.request_ceiling == 78
    assert (
        authority.provider_contract.budget.decoded_byte_ceiling
        == 81_788_928
    )
    assert authority.package_sha256 == "2" * 64
    assert authority.bundle_sha256 == "3" * 64


class _Port:
    def __init__(self, window, *, replay=False):
        self.window = window
        self.replay = replay
        self.calls = []
        self.published = SchedulerPublishedCycle(
            target_window_id=UUID(
                "66666666-7777-8888-9999-aaaaaaaaaaaa"
            ),
            release_id=UUID(
                "77777777-8888-9999-aaaa-bbbbbbbbbbbb"
            ),
            release_cutoff=window.equity_available_after
            + timedelta(seconds=1),
            read_pin_release_id=UUID(
                "77777777-8888-9999-aaaa-bbbbbbbbbbbb"
            ),
            stored_reader_count=4,
            stock_alert_batch_id=UUID(
                "99999999-aaaa-bbbb-cccc-dddddddddddd"
            ),
            stock_alert_batch_sha256="5" * 64,
            stock_alert_evaluation_count=9,
            stock_alert_unavailable_count=9,
            stock_alert_reason_codes_exact=True,
        )

    def existing_cycle(self, window):
        self.calls.append(("existing", window.index))
        return self.published if self.replay else None

    def load_equity_receipt(self, window):
        self.calls.append(("equity", window.index))
        return SchedulerEquityReceipt(
            receipt_id=UUID(
                "88888888-9999-aaaa-bbbb-cccccccccccc"
            ),
            cutoff=window.equity_cutoff,
            created_at=window.equity_available_after,
            input_sha256="7" * 64,
        )

    def capture_options(self, window, receipt):
        self.calls.append(("capture", window.index))
        return SchedulerOptionsCapture(
            evidence_sha256="4" * 64,
            reviewed_at=receipt.created_at + timedelta(seconds=1),
            contract_tickers=tuple(reversed(CONTRACTS)),
            request_count=1,
            pagination_requests=0,
        )

    def run_cycle(self, window, receipt, capture):
        self.calls.append(("cycle", window.index))
        return self.published


def test_scheduler_runs_one_capture_publication_and_pin_per_finalized_e():
    authority = _authority()
    window = authority.windows[0]
    port = _Port(window)
    scheduler = AdvancedRealtimeSessionScheduler(
        authority,
        port,
        clock=lambda: window.equity_available_after,
    )

    report = scheduler.run_window(window)

    assert report["status"] == "COMPLETE_PUBLISHED_PINNED"
    assert report["stored_reader_count"] == 4
    assert report["replayed"] is False
    assert port.calls == [
        ("existing", 1),
        ("equity", 1),
        ("capture", 1),
        ("cycle", 1),
    ]


def test_scheduler_replays_complete_cycle_without_new_effects():
    authority = _authority()
    window = authority.windows[-1]
    port = _Port(window, replay=True)
    scheduler = AdvancedRealtimeSessionScheduler(
        authority,
        port,
        clock=lambda: window.latest_options_cutoff,
    )

    report = scheduler.run_window(window)

    assert report["replayed"] is True
    assert port.calls == [("existing", 78)]


def test_scheduler_fails_closed_after_e_o_deadline():
    authority = _authority()
    window = build_advanced_session_windows(SESSION_DATE)[0]
    port = _Port(window)
    scheduler = AdvancedRealtimeSessionScheduler(
        authority,
        port,
        clock=lambda: (
            window.latest_options_cutoff + timedelta(microseconds=1)
        ),
    )

    with pytest.raises(RuntimeError, match="deadline expired"):
        scheduler.run_window(window)
    assert port.calls == [("existing", 1)]


class _FullSessionPort:
    def __init__(self):
        self.captures = 0
        self.cycles = 0

    def existing_cycle(self, _window):
        return None

    def load_equity_receipt(self, window):
        return SchedulerEquityReceipt(
            receipt_id=uuid5(
                NAMESPACE_URL,
                f"equity:{window.key}",
            ),
            cutoff=window.equity_cutoff,
            created_at=window.equity_available_after,
            input_sha256=f"{window.index + 156:064x}",
        )

    def capture_options(self, window, receipt):
        self.captures += 1
        return SchedulerOptionsCapture(
            evidence_sha256=f"{window.index:064x}",
            reviewed_at=receipt.created_at,
            contract_tickers=CONTRACTS,
            request_count=1,
            pagination_requests=0,
        )

    def run_cycle(self, window, _receipt, _capture):
        self.cycles += 1
        release_id = uuid5(
            NAMESPACE_URL,
            f"release:{window.key}",
        )
        return SchedulerPublishedCycle(
            target_window_id=uuid5(
                NAMESPACE_URL,
                f"window:{window.key}",
            ),
            release_id=release_id,
            release_cutoff=window.equity_available_after,
            read_pin_release_id=release_id,
            stored_reader_count=4,
            stock_alert_batch_id=uuid5(
                NAMESPACE_URL,
                f"stock-alert-batch:{window.key}",
            ),
            stock_alert_batch_sha256=f"{window.index + 78:064x}",
            stock_alert_evaluation_count=9,
            stock_alert_unavailable_count=9,
            stock_alert_reason_codes_exact=True,
        )


def test_scheduler_qualifies_all_78_cycles_provider_free():
    authority = _authority()
    port = _FullSessionPort()

    for window in authority.windows:
        report = AdvancedRealtimeSessionScheduler(
            authority,
            port,
            clock=lambda window=window: window.equity_available_after,
        ).run_window(window)
        assert report["status"] == "COMPLETE_PUBLISHED_PINNED"

    assert port.captures == 78
    assert port.cycles == 78


def test_session_start_launches_acquisition_and_all_residents_preopen(
    monkeypatch,
    tmp_path,
):
    compose_calls = []
    resident_calls = []
    monkeypatch.setattr(
        cli,
        "_compose_command",
        lambda _args, *values, **_kwargs: compose_calls.append(
            values
        ),
    )
    monkeypatch.setattr(
        cli.production_cli,
        "_compose",
        lambda _args, kinds: resident_calls.append(tuple(kinds)),
    )

    cli._start_services(SimpleNamespace(
        package=tmp_path / "package.json",
        bundle=tmp_path / "bundle.json",
        docker_executable=tmp_path / "docker.exe",
    ))

    assert cli.OPTIONS_ACQUISITION_SERVICE in compose_calls[0]
    assert resident_calls == [tuple(cli.AdvancedOptionsProcessKind)]


def test_preopen_gates_are_exactly_five_minutes_apart():
    authority = _authority()

    gates = cli._preopen_gate_times(authority)

    assert gates == {
        "health": authority.acquisition_start + timedelta(minutes=5),
        "identity": authority.acquisition_start + timedelta(minutes=10),
        "final_go": authority.acquisition_start + timedelta(minutes=15),
    }


def test_runtime_container_state_enforces_three_restart_limit(
    monkeypatch,
    tmp_path,
):
    image_id = "sha256:" + "a" * 64
    services = (*cli.EQUITY_SERVICES, *cli.OPTIONS_SERVICES)

    def compose(_args, *values, **_kwargs):
        service = values[-1]
        return SimpleNamespace(stdout=f"{service}-id\n".encode("ascii"))

    def inspect(*_args, **_kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([
                {
                    "Id": f"{service}-id",
                    "Image": image_id,
                    "RestartCount": (
                        4
                        if service == cli.EQUITY_SERVICES[0]
                        else 3
                    ),
                    "State": {
                        "Status": "running",
                        "Health": {"Status": "healthy"},
                    },
                }
                for service in services
            ]).encode("utf-8"),
        )

    monkeypatch.setattr(cli, "_compose_command", compose)
    monkeypatch.setattr(cli.subprocess, "run", inspect)
    args = SimpleNamespace(
        docker_executable=tmp_path / "docker.exe",
    )

    with pytest.raises(RuntimeError, match="container health changed"):
        cli._runtime_container_state(
            args,
            backend_image_id=image_id,
        )


def test_runtime_metrics_must_remain_monotonic():
    def snapshot(value):
        return {
            "database": {
                "equity": {
                    "owner_epoch": 1,
                    "received_frames": value,
                    "received_bytes": value,
                    "raw_offset": value,
                    "normalized_offset": value,
                    "bridge_offset": value,
                },
                "options": {
                    "cycle_authority_count": value,
                    "raw_offset": value,
                    "normalized_offset": value,
                    "consumer_checkpoints": {
                        "market-state": value,
                    },
                    "resident_barriers": {
                        "FLOW": value,
                    },
                    "completed_requests": value,
                    "response_bytes": value,
                },
            },
            "options_acquisition": {
                "connection_attempts": 1,
                "decoded_messages": value,
                "decoded_bytes": value,
                "last_raw_offset": value,
                "last_event_offset": value,
            },
            "containers": {
                "service": {"restart_count": 0},
            },
        }

    cli._assert_monotonic_runtime(snapshot(2), snapshot(3))
    with pytest.raises(RuntimeError, match="regressed"):
        cli._assert_monotonic_runtime(snapshot(3), snapshot(2))


def test_runtime_monitor_recovers_from_bounded_sampling_failures(
    monkeypatch,
    tmp_path,
):
    monitor_stop = threading.Event()
    session_stop = threading.Event()
    failures = []
    calls = []

    def snapshot(*_args, **_kwargs):
        calls.append(len(calls) + 1)
        if len(calls) < 3:
            raise RuntimeError("Docker inspect failed with 1")
        monitor_stop.set()
        return {
            "identity_sha256": "a" * 64,
            "database": {"equity": {}, "options": {
                "consumer_checkpoints": {},
                "resident_barriers": {},
            }},
            "options_acquisition": {},
            "containers": {},
        }

    monkeypatch.setattr(cli, "_runtime_snapshot", snapshot)
    monkeypatch.setattr(cli, "_assert_monotonic_runtime", lambda *_: None)
    cli._monitor_runtime(
        SimpleNamespace(
            output_directory=tmp_path,
            monitor_seconds=0.001,
        ),
        object(),
        _authority(),
        {},
        "sha256:" + "1" * 64,
        "a" * 64,
        session_stop,
        monitor_stop,
        failures,
    )

    reports = sorted((tmp_path / "monitoring").glob("failure-*.json"))
    assert calls == [1, 2, 3]
    assert session_stop.is_set() is False
    assert failures == []
    assert len(reports) == 2
    assert all(
        json.loads(path.read_text(encoding="utf-8"))["terminal"] is False
        for path in reports
    )


def test_runtime_monitor_resumes_after_existing_immutable_samples(
    monkeypatch,
    tmp_path,
):
    monitoring = tmp_path / "monitoring"
    monitoring.mkdir()
    prior = {
        "schema": "advanced-realtime-v3-runtime-monitor-sample-v1",
        "interval_seconds": 15,
        "identity_sha256": "a" * 64,
        "database": {"equity": {}, "options": {
            "consumer_checkpoints": {}, "resident_barriers": {},
        }},
        "options_acquisition": {},
        "containers": {},
    }
    cli._write_runtime_report(monitoring / "sample-00001.json", prior)
    monitor_stop = threading.Event()

    def snapshot(*_args, **_kwargs):
        monitor_stop.set()
        return {key: value for key, value in prior.items() if key != "schema"}

    monkeypatch.setattr(cli, "_runtime_snapshot", snapshot)
    monkeypatch.setattr(cli, "_assert_monotonic_runtime", lambda *_: None)
    cli._monitor_runtime(
        SimpleNamespace(output_directory=tmp_path, monitor_seconds=0.001),
        object(), _authority(), {}, "sha256:" + "1" * 64,
        "a" * 64, threading.Event(), monitor_stop, [],
    )

    assert (monitoring / "sample-00001.json").is_file()
    assert (monitoring / "sample-00002.json").is_file()


def test_runtime_monitor_stops_immediately_for_identity_corruption(
    monkeypatch,
    tmp_path,
):
    monitor_stop = threading.Event()
    session_stop = threading.Event()
    failures = []
    monkeypatch.setattr(
        cli,
        "_runtime_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("runtime identity diverged from final GO")
        ),
    )

    cli._monitor_runtime(
        SimpleNamespace(
            output_directory=tmp_path,
            monitor_seconds=0.001,
        ),
        object(),
        _authority(),
        {},
        "sha256:" + "1" * 64,
        "a" * 64,
        session_stop,
        monitor_stop,
        failures,
    )

    report = json.loads(next(
        (tmp_path / "monitoring").glob("failure-*.json")
    ).read_text(encoding="utf-8"))
    assert session_stop.is_set() is True
    assert len(failures) == 1
    assert report["classification"] == "FATAL_IDENTITY_OR_INTEGRITY"
    assert report["terminal"] is True


def test_coordinator_identity_allows_only_exact_owned_resume(
    monkeypatch,
    tmp_path,
):
    authority = _authority()
    acquisition_path = tmp_path / "acquisition.json"
    acquisition_path.write_text("{}", encoding="utf-8")
    output = tmp_path / "run"
    monkeypatch.setattr(
        cli,
        "acquisition_binding_from_payload",
        lambda _value: SimpleNamespace(binding_sha256="f" * 64),
    )
    args = SimpleNamespace(
        output_directory=output,
        acquisition_binding=acquisition_path,
        resume=True,
    )

    assert cli._initialize_coordinator_identity(args, authority) is False
    identity = json.loads(
        (output / "COORDINATOR.json").read_text(encoding="utf-8")
    )
    assert identity["authorization_sha256"] == (
        authority.authorization_sha256
    )
    assert cli._initialize_coordinator_identity(args, authority) is True

    identity["authorization_sha256"] = "0" * 64
    (output / "COORDINATOR.json").write_text(
        json.dumps(identity),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="resume identity changed"):
        cli._initialize_coordinator_identity(args, authority)


def test_coordinator_reuses_complete_topology_but_rejects_partial(
    monkeypatch,
):
    services = (*cli.EQUITY_SERVICES, *cli.OPTIONS_SERVICES)
    starts = []
    args = SimpleNamespace()

    monkeypatch.setattr(
        cli,
        "_compose_command",
        lambda *_args, **_kwargs: SimpleNamespace(
            stdout="\n".join(
                f"container-{index}"
                for index, _service in enumerate(services)
            ).encode("ascii")
        ),
    )
    monkeypatch.setattr(
        cli,
        "_start_services",
        lambda _args: starts.append(True),
    )

    assert cli._reconcile_or_start_services(
        args,
        resuming=True,
    ) == "RECONCILED_EXISTING"
    assert starts == []

    monkeypatch.setattr(
        cli,
        "_compose_command",
        lambda *_args, **_kwargs: SimpleNamespace(
            stdout=b"only-one\n"
        ),
    )
    with pytest.raises(RuntimeError, match="partial container topology"):
        cli._reconcile_or_start_services(args, resuming=True)


def test_coordinator_refuses_restart_over_active_durable_owners(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_cleanup_database_state",
        lambda *_args: {
            "active_stream_sessions": 1,
            "active_worker_leases": 0,
            "open_or_ambiguous_provider_requests": 0,
        },
    )
    with pytest.raises(RuntimeError, match="durable streams"):
        cli._assert_resume_can_start_services(object(), _authority(), {})


def test_coordinator_preserves_terminal_failed_window_on_resume(tmp_path):
    window = _authority().windows[1]
    failure = {
        "contract": cli.ADVANCED_SESSION_SCHEDULER_VERSION,
        "status": "FAILED_CYCLE",
        "window": window.key,
        "error_type": "RuntimeError",
        "message": "explicit missed window",
    }
    (tmp_path / f"{window.key}-failure.json").write_text(
        json.dumps(failure),
        encoding="utf-8",
    )

    assert cli._existing_terminal_cycle_failure(
        tmp_path,
        window,
    ) == failure


def test_secret_cleanup_refuses_unowned_paths(tmp_path):
    secret_directory = tmp_path / "secrets"
    secret_directory.mkdir()
    for name in cli.EXPECTED_SECRET_FILES:
        (secret_directory / name).write_text("value", encoding="utf-8")
    (secret_directory / "unexpected").write_text(
        "value",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="ownership changed"):
        cli._remove_secret_directory(secret_directory)


def test_secret_cleanup_removes_exact_owned_directory(tmp_path):
    secret_directory = tmp_path / "secrets"
    secret_directory.mkdir()
    for name in cli.EXPECTED_SECRET_FILES:
        (secret_directory / name).write_text("value", encoding="utf-8")

    removed = cli._remove_secret_directory(secret_directory)

    assert removed == sorted(cli.EXPECTED_SECRET_FILES)
    assert not secret_directory.exists()


def test_equity_full_session_services_cap_restarts_at_three():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")

    for service in cli.EQUITY_SERVICES:
        match = re.search(
            rf"^  {re.escape(service)}:\n"
            r"(?P<body>.*?)(?=^  [a-z0-9][^\n]*:\n|\Z)",
            compose,
            flags=re.MULTILINE | re.DOTALL,
        )
        assert match is not None
        assert 'restart: "on-failure:3"' in match.group("body")


def test_runtime_status_reads_latest_sample_without_mutation(tmp_path):
    monitoring = tmp_path / "monitoring"
    monitoring.mkdir()
    (monitoring / "preopen-sample-00001.json").write_text(
        json.dumps({"checked_at_utc": "2026-10-28T13:15:00+00:00"}),
        encoding="utf-8",
    )
    (monitoring / "sample-00001.json").write_text(
        json.dumps({"checked_at_utc": "2026-10-28T13:25:15+00:00"}),
        encoding="utf-8",
    )
    (tmp_path / "FINAL-GO.json").write_text(
        json.dumps({"decision": "GO"}),
        encoding="utf-8",
    )

    report = cli._read_runtime_status(tmp_path)

    assert report["latest_sample"]["checked_at_utc"] == (
        "2026-10-28T13:25:15+00:00"
    )
    assert report["sample_count"] == 2
    assert report["failures"] == []
    assert report["gates"]["FINAL-GO.json"]["decision"] == "GO"
    assert report["provider_effects"] == 0
    assert report["production_effects"] == 0


def test_cycle_deadline_reports_stop_retry_at_e_plus_240(tmp_path):
    window = _authority().windows[0]
    cli._write_cycle_deadline_report(
        SimpleNamespace(output_directory=tmp_path),
        window,
        threshold_seconds=240,
        classification="FINALIZATION",
        error=RuntimeError("finalized input unavailable"),
    )

    report = json.loads(
        (tmp_path / f"{window.key}-E+240.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["classification"] == "FINALIZATION"
    assert report["retry"] == "STOPPED"
    assert report["terminal_at_utc"] == (
        window.latest_options_cutoff.isoformat()
    )
