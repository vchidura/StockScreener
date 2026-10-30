from __future__ import annotations

import copy
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from realtime.advanced_options_v3_deployment import (
    ADVANCED_OPTIONS_V3_COMPOSE_PROFILE,
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
    advanced_options_v3_package_plan,
    advanced_options_v3_package_status,
    advanced_options_v3_profile_payload,
    build_advanced_options_v3_authority_references,
    build_disabled_advanced_options_v3_package,
    provider_free_advanced_options_v3_preflight,
    validate_advanced_options_v3_runtime_identity,
    validate_disabled_advanced_options_v3_package,
)
from realtime.advanced_options_v3_authority import (
    session_acquisition_authorization_payload,
)
from realtime.advanced_options_service import (
    AdvancedOptionsProcessKind,
    OPTIONS_V3_STAGE_ORDER,
)
from realtime.contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5
from scripts import (
    audit_advanced_options_v3_operations,
    inspect_advanced_options_v3_package,
    qualify_advanced_options_v3_docker,
    render_advanced_realtime_v3_compose,
    run_advanced_options_v3_docker_rehearsal,
)
from tests.realtime.test_advanced_options_v3_authority import (
    _authority_bundle,
)


ROOT = Path(__file__).resolve().parents[3]
UTC = timezone.utc


def test_docker_qualification_requires_exact_image_labels(monkeypatch):
    args = SimpleNamespace(
        image_id=f"sha256:{'a' * 64}",
        qualification_semantic_sha256="b" * 64,
        session_date=date(2026, 10, 30),
    )
    labels = {
        "com.stockscreener.source-semantic-sha256": "b" * 64,
        "com.stockscreener.session-date": "2026-10-30",
    }
    monkeypatch.setattr(
        qualify_advanced_options_v3_docker,
        "_docker",
        lambda *_args: json.dumps(labels),
    )

    assert (
        qualify_advanced_options_v3_docker._assert_image_binding(args)
        == labels
    )

    labels["com.stockscreener.session-date"] = "2026-10-29"
    with pytest.raises(RuntimeError, match="not bound"):
        qualify_advanced_options_v3_docker._assert_image_binding(args)


def test_operations_audit_exposes_named_freeze_facts():
    profile_sha = "a" * 64
    cohort_sha = "b" * 64
    manifest_sha = "c" * 64
    normalizer_sha = "d" * 64
    scheduler_sha = "e" * 64
    gaps = {
        "buckets": {
            "AVAILABLE": [
                {
                    "fact": "active_target9_stocks_am_profile",
                    "evidence": [{
                        "runtime_profile_sha256": profile_sha,
                    }],
                },
                {
                    "fact": "target9_equity_cohort",
                    "evidence": [{
                        "cohort_sha256": cohort_sha,
                        "manifest_sha256": manifest_sha,
                    }],
                },
            ],
        },
        "counts": {
            "AVAILABLE": 2,
            "MISSING_STORED": 0,
            "STALE": 0,
            "AMBIGUOUS": 0,
            "REQUIRES_PROVIDER": 0,
        },
    }
    source = {
        "files": [
            {
                "path": "backend/realtime/normalizer.py",
                "sha256": normalizer_sha,
            },
            {
                "path": (
                    "backend/realtime/"
                    "advanced_realtime_session_scheduler.py"
                ),
                "sha256": scheduler_sha,
            },
        ],
    }

    facts = audit_advanced_options_v3_operations._freeze_facts(
        gaps,
        source,
    )

    assert facts["stored_active_profile_sha256"] == profile_sha
    assert facts["rendered_source_profile_sha256"] == profile_sha
    assert facts["normalizer_source_sha256"] == normalizer_sha
    assert facts["policy_command_sha256"] == scheduler_sha
    assert facts["all_gap_counts_zero"] is True


def test_compose_render_requires_exact_images_and_redaction():
    image_id = f"sha256:{'a' * 64}"
    rendered = {
        "services": {
            name: {"image": image_id}
            for name in render_advanced_realtime_v3_compose.TARGET_SERVICES
        },
    }

    render_advanced_realtime_v3_compose._validate_rendered(
        rendered,
        image_id,
        ("not-present-secret",),
    )

    rendered["services"][
        render_advanced_realtime_v3_compose.TARGET_SERVICES[0]
    ]["environment"] = {"KEY": "secret-value"}
    with pytest.raises(ValueError, match="exposed"):
        render_advanced_realtime_v3_compose._validate_rendered(
            rendered,
            image_id,
            ("secret-value",),
        )


def _docker_rehearsal_files(tmp_path):
    window_ids = [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "33333333-3333-3333-3333-333333333333",
    ]
    package = {
        "schema": "advanced-options-v3-docker-rehearsal-package-v1",
        "contracts": ["O:QQQ261023C00350000"],
        "window_ids": window_ids,
    }
    package["package_sha256"] = canonical_sha256(package)
    bundle = {
        "schema": "advanced-options-v3-docker-rehearsal-bundle-v1",
        "package_sha256": package["package_sha256"],
        "contracts": list(package["contracts"]),
        "window_ids": window_ids,
        "catalog_version": STAGE_CATALOG_VERSION_V5,
        "stage_order": list(OPTIONS_V3_STAGE_ORDER),
        "roles": list(run_advanced_options_v3_docker_rehearsal.ROLES),
        "acceptance": {
            "window_count": 3,
            "window_2": "DEFERRED_THEN_COMPLETE",
            "window_3": {
                "O1": "COMPLETE_SELECTED",
                "O3": "BLOCKED",
            },
        },
        "effects": dict(
            run_advanced_options_v3_docker_rehearsal.ZERO_EFFECTS
        ),
    }
    bundle["bundle_sha256"] = canonical_sha256(bundle)
    package_path = tmp_path / "package.json"
    bundle_path = tmp_path / "bundle.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    return package_path, bundle_path


def _disabled_package():
    return build_disabled_advanced_options_v3_package(
        repository_root=ROOT,
        source_commit="a" * 40,
        backend_image="backend@sha256:" + "b" * 64,
        qualification_semantic_sha256="c" * 64,
        qualification_file_sha256="d" * 64,
    )


def _runtime_identity():
    package = {
        "package_sha256": "1" * 64,
        "v3_deployment": {
            "profile": advanced_options_v3_profile_payload(),
            "profile_sha256": ADVANCED_OPTIONS_V3_PROFILE_SHA256,
            "state": ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
            "window_role_authorized": True,
            "qualification_verdict": "PASS",
        },
        "equity_input_binding": {
            "symbols": list(
                EQUITY_VALIDATION_UNIVERSE.equity_tickers
            ),
        },
        "cohort": {"underlyings": ["QQQ", "SPY"]},
        "owners": {
            "processes": [
                {"kind": kind.value}
                for kind in AdvancedOptionsProcessKind
            ],
        },
    }
    bundle = {
        "package_sha256": package["package_sha256"],
        "orchestration": {
            "catalog_version": STAGE_CATALOG_VERSION_V5,
            "stage_order": list(OPTIONS_V3_STAGE_ORDER),
        },
        "production_binding": {
            "control": {"option_underlyings": ["QQQ", "SPY"]},
        },
    }
    return package, bundle


def test_disabled_package_is_exact_and_provider_free():
    package = _disabled_package()

    assert validate_disabled_advanced_options_v3_package(
        package,
        repository_root=ROOT,
    ) == package
    assert package["profile"]["equity"]["target"] == 9
    assert package["profile"]["equity"]["feed"] == "STOCKS"
    assert package["profile"]["equity"]["channels"] == ["AM"]
    assert package["profile"]["options"]["underlyings"] == [
        "QQQ",
        "SPY",
    ]
    assert package["profile"]["orchestration"]["stage_order"] == list(
        OPTIONS_V3_STAGE_ORDER
    )
    assert package["docker"]["compose_profile"] == (
        ADVANCED_OPTIONS_V3_COMPOSE_PROFILE
    )
    assert not any(package["effects"].values())


def test_plan_preflight_and_status_separate_future_authorities():
    package = _disabled_package()

    plan = advanced_options_v3_package_plan(package)
    preflight = provider_free_advanced_options_v3_preflight(
        package,
        repository_root=ROOT,
    )
    status = advanced_options_v3_package_status(package)

    assert plan["authority_phases"] == {
        "session_acquisition": "SEPARATE",
        "window_cycle": "SEPARATE",
        "canonical_publication": "SEPARATE",
        "stored_read": "SEPARATE_NOT_AUTHORIZED",
    }
    assert len(preflight["live_blockers"]) == 6
    assert status["status"] == "DISABLED"
    assert not any(plan["effects"].values())
    assert not any(preflight["effects"].values())
    assert not any(status["effects"].values())


def test_session_bundle_carries_only_durable_post_e_references():
    authority = _authority_bundle(target_window_id=uuid4())
    references = build_advanced_options_v3_authority_references(
        session_acquisition_authorization_payload(authority.session),
        target_window_id=authority.cycle.target_window_id,
    )

    assert references["session_acquisition"] == {
        "authorization_id": str(authority.session.authorization_id),
        "authorization_sha256": (
            authority.session.authorization_sha256
        ),
    }
    assert references["window_cycle"] == {
        "load_mode": "DURABLE_SESSION_WINDOWS",
        "session_authorization_id": str(
            authority.session.authorization_id
        ),
        "window_id_version": "session-cohort-key-cutoff-v1",
    }
    assert (
        references["canonical_publication"]["load_mode"]
        == "DERIVE_FROM_DURABLE_WINDOW_CYCLE"
    )
    assert references["stored_read"]["authorized"] is False
    assert "authorization_sha256" not in references["window_cycle"]


def test_runtime_identity_accepts_only_exact_qualified_v3():
    package, bundle = _runtime_identity()
    validate_advanced_options_v3_runtime_identity(package, bundle)

    mutations = (
        ("target-2 Equity", ("equity_input_binding", "symbols"), [
            "QQQ",
            "SPY",
        ]),
        ("nine Options", ("cohort", "underlyings"), list(
            EQUITY_VALIDATION_UNIVERSE.equity_tickers
        )),
        ("stages_v4", ("bundle", "catalog_version"), "stages_v4"),
        ("disabled role", (
            "v3_deployment",
            "window_role_authorized",
        ), False),
    )
    for _name, path, value in mutations:
        changed_package = copy.deepcopy(package)
        changed_bundle = copy.deepcopy(bundle)
        if path[0] == "bundle":
            changed_bundle["orchestration"][path[1]] = value
        else:
            changed_package[path[0]][path[1]] = value
        with pytest.raises(
            ValueError,
            match="exact qualified V3 identity",
        ):
            validate_advanced_options_v3_runtime_identity(
                changed_package,
                changed_bundle,
            )


def test_docker_rehearsal_rejects_contract_identity_drift(tmp_path):
    package_path, bundle_path = _docker_rehearsal_files(tmp_path)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["contracts"] = ["O:SPY261023C00600000"]
    bundle.pop("bundle_sha256")
    bundle["bundle_sha256"] = canonical_sha256(bundle)
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="V3 Docker rehearsal identity changed",
    ):
        run_advanced_options_v3_docker_rehearsal._validate(
            package_path,
            bundle_path,
        )


def test_docker_rehearsal_main_owns_connection_without_reentry(
    monkeypatch,
    tmp_path,
):
    package_path, bundle_path = _docker_rehearsal_files(tmp_path)

    class Connection:
        autocommit = None
        closed = False

        def __enter__(self):
            raise AssertionError("main must not enter the connection")

        def close(self):
            self.closed = True

    connection = Connection()
    calls = []
    monkeypatch.setattr(
        run_advanced_options_v3_docker_rehearsal,
        "_connect",
        lambda _path: connection,
    )
    monkeypatch.setattr(
        run_advanced_options_v3_docker_rehearsal,
        "_run_role",
        lambda value, bundle, role: calls.append((value, bundle, role)),
    )

    result = run_advanced_options_v3_docker_rehearsal.main([
        "--role",
        "INGRESS",
        "--package",
        str(package_path),
        "--bundle",
        str(bundle_path),
    ])

    assert result == 0
    assert calls[0][0] is connection
    assert calls[0][2] == "INGRESS"
    assert connection.autocommit is False
    assert connection.closed


def test_docker_qualification_uses_actual_production_entrypoints(
    tmp_path,
):
    environment = {
        "REALTIME_V2_ADVANCED_COHORT_ID": (
            "11111111-2222-3333-4444-555555555555"
        ),
        "REALTIME_V2_ADVANCED_SESSION_DATE": "2026-10-28",
        "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID": (
            "22222222-3333-4444-5555-666666666666"
        ),
    }
    override = qualify_advanced_options_v3_docker._override(
        environment=environment,
        diagnostic_path=tmp_path / "diagnostic.json",
    )

    for role, service in (
        qualify_advanced_options_v3_docker.SERVICES.items()
    ):
        expected_environment = {
            "REALTIME_V3_VALIDATION_MODE": (
                "DEVELOPMENT_DIAGNOSTIC"
            ),
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
        }
        if role == "CANONICAL_STOCK_ALERTS":
            expected_environment[
                "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID"
            ] = environment[
                "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID"
            ]
            assert "command" in override["services"][service]
        else:
            assert "command" not in override["services"][service]
        assert (
            override["services"][service]["environment"]
            == expected_environment
        )
        command = qualify_advanced_options_v3_docker._expected_command(
            role,
            environment,
        )
        expected_script = (
            "scripts/run_realtime_v2_canonical_stock_alerts.py"
            if role == "CANONICAL_STOCK_ALERTS"
            else "scripts/run_advanced_options_v3_acquisition.py"
            if role == "OPTIONS_ACQUISITION"
            else "scripts/run_advanced_options_v3_production.py"
        )
        assert expected_script in command
        assert "scripts/run_advanced_options_v3_docker_rehearsal.py" not in (
            command
        )
        if role == "CANONICAL_STOCK_ALERTS":
            assert "--continuous" in command
            assert "DIAGNOSTIC_NON_SERVING" in command
            assert "--diagnostic-authorization" in command
        elif role == "OPTIONS_ACQUISITION":
            assert "--continuous" in command
            assert "--binding" in command
            assert "--docker-service-kind" not in command
        else:
            assert "--docker-service-kind" in command


def test_docker_qualification_waits_for_completed_role_evidence(
    monkeypatch,
):
    running = {
        "state": "ROLE_RUNNING",
        "kind": "WINDOW",
        "role_evidence": None,
    }
    complete = {
        "state": "ROLE_COMPLETE",
        "kind": "WINDOW",
        "role_evidence": {
            "status": "COMPLETE",
            "kind": "WINDOW",
            "effects": dict(
                qualify_advanced_options_v3_docker.P8_EFFECT_DENIALS
            ),
        },
    }
    values = iter((running, complete))
    monkeypatch.setattr(
        qualify_advanced_options_v3_docker,
        "_docker",
        lambda *_args: json.dumps(next(values)),
    )
    monkeypatch.setattr(
        qualify_advanced_options_v3_docker.time,
        "sleep",
        lambda _seconds: None,
    )

    assert qualify_advanced_options_v3_docker._role_status(
        "a" * 64,
        "WINDOW",
    ) == complete


def test_operations_audit_separates_missing_and_stale_runtime_state():
    observed = datetime(2026, 10, 23, 9, 40, tzinfo=UTC)
    stale_window = {
        "deadline": observed - timedelta(hours=12),
        "lease_until": observed - timedelta(hours=11),
        "state": "RUNNING",
    }
    active_window = {
        "deadline": observed + timedelta(hours=1),
        "lease_until": None,
        "state": "PENDING",
    }
    terminal_window = {
        "deadline": observed + timedelta(hours=1),
        "lease_until": None,
        "state": "COMPLETE",
    }
    ended_stream = {"ended_at": observed - timedelta(hours=10)}
    ended_stream["session_date"] = date(2026, 10, 22)
    current_ended_stream = {
        "ended_at": observed - timedelta(hours=1),
        "session_date": date(2026, 10, 23),
    }
    live_stream = {
        "ended_at": None,
        "session_date": date(2026, 10, 23),
    }

    conflicts, stale, retained = (
        audit_advanced_options_v3_operations
        ._runtime_record_classification(
            {
                "live_worker_leases": (),
                "conflicting_windows": (
                    stale_window,
                    active_window,
                    terminal_window,
                ),
                "nonterminal_stream_sessions": (
                    ended_stream,
                    current_ended_stream,
                    live_stream,
                ),
            },
            observed,
            date(2026, 10, 23),
        )
    )

    assert audit_advanced_options_v3_operations._exactly_one_status(()) == (
        "MISSING_STORED"
    )
    assert conflicts["windows"] == (active_window,)
    assert conflicts["streams"] == (live_stream,)
    assert stale["windows"] == (stale_window,)
    assert stale["streams"] == (current_ended_stream,)
    assert retained["streams"] == (ended_stream,)


def test_operations_audit_coalesces_identical_catalog_versions():
    base = {
        "contract_id": 7,
        "contract_ticker": "O:SPY261027C00768000",
        "underlying_security_id": str(UUID(int=8)),
        "version_id": 1,
        "call_put": "C",
        "expiration": date(2026, 10, 27),
        "strike": Decimal("768"),
        "exercise_style": "AMERICAN",
        "multiplier": Decimal("100"),
        "eligibility": "VALIDATED_ACTIVE",
        "effective_from": datetime(2026, 10, 27, 12, tzinfo=UTC),
        "observed_at": datetime(2026, 10, 27, 12, tzinfo=UTC),
        "payload_sha256": "a" * 64,
    }
    latest = {
        **base,
        "version_id": 2,
        "effective_from": datetime(
            2026,
            10,
            27,
            12,
            1,
            tzinfo=UTC,
        ),
        "observed_at": datetime(
            2026,
            10,
            27,
            12,
            1,
            tzinfo=UTC,
        ),
    }

    assert (
        audit_advanced_options_v3_operations
        ._coalesce_option_catalog_rows((base, latest))
    ) == (latest,)
    divergent = {**latest, "strike": Decimal("769")}
    assert len(
        audit_advanced_options_v3_operations
        ._coalesce_option_catalog_rows((base, divergent))
    ) == 2


def test_cli_has_no_execute_mode(tmp_path, capsys):
    package_path = tmp_path / "v3-disabled.json"
    package_path.write_text(
        json.dumps(_disabled_package(), sort_keys=True),
        encoding="utf-8",
    )

    assert inspect_advanced_options_v3_package.main([
        "--package",
        str(package_path),
        "--status",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "DISABLED"
    with pytest.raises(SystemExit):
        inspect_advanced_options_v3_package.parse_args([
            "--package",
            str(package_path),
            "--execute",
        ])
