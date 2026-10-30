from types import SimpleNamespace
from datetime import date
from uuid import uuid4

import pytest

import realtime.advanced_options_p8_roles as roles
import realtime.advanced_options_v3_qualification as qualification
from realtime.advanced_options_p8_production import (
    run_p8_production_role,
)
from realtime.advanced_options_validation_mode import (
    AdvancedOptionsValidationMode,
    advanced_options_projection_policy,
    advanced_options_validation_mode,
)
from scripts.qualify_advanced_options_v3_docker import (
    _qualification_artifacts,
)


def test_missing_validation_mode_is_strict():
    assert advanced_options_validation_mode({}) is (
        AdvancedOptionsValidationMode.PRODUCTION_STRICT
    )


def test_diagnostic_projection_policy_stops_before_publish():
    policy = advanced_options_projection_policy({
        "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
        "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
    })

    assert policy.equity_stage_order[-1] == "EQUITY_PROJECTION"
    assert policy.equity_terminal_receipt == "PORTAL_READY"
    assert policy.options_stage_order[-1] == "OPTIONS_PROJECTION"
    assert policy.options_terminal_receipt == "OPTIONS_PORTAL_READY"
    assert not policy.release_allowed
    assert not policy.pointer_allowed
    assert not policy.read_pin_allowed
    assert not policy.normal_portal_serving_allowed


def test_development_mode_requires_provider_free_sentinel():
    with pytest.raises(
        ValueError,
        match="requires the provider-free qualification sentinel",
    ):
        advanced_options_validation_mode({
            "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
        })

    with pytest.raises(
        ValueError,
        match="requires the provider-free qualification sentinel",
    ):
        advanced_options_validation_mode({
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
        })


def test_unknown_validation_mode_fails_closed():
    with pytest.raises(
        ValueError,
        match="REALTIME_V3_VALIDATION_MODE is invalid",
    ):
        advanced_options_validation_mode({
            "REALTIME_V3_VALIDATION_MODE": "DISABLED",
        })


def test_production_role_routes_only_explicit_diagnostic_mode(
    monkeypatch,
):
    calls = []
    monkeypatch.setattr(
        roles,
        "run_p8_role",
        lambda **kwargs: calls.append(("strict", kwargs)) or "strict",
    )
    monkeypatch.setattr(
        qualification,
        "run_provider_free_v3_role",
        lambda **kwargs: calls.append(("diagnostic", kwargs))
        or "diagnostic",
    )

    strict = run_p8_production_role(
        environment={},
        kind=SimpleNamespace(),
    )
    diagnostic = run_p8_production_role(
        environment={
            "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
        },
        kind=SimpleNamespace(),
    )

    assert strict == "strict"
    assert diagnostic == "diagnostic"
    assert [row[0] for row in calls] == ["strict", "diagnostic"]


def test_diagnostic_artifacts_require_durable_mode_marker():
    window_ids = tuple(str(uuid4()) for _ in range(3))
    seed = {
        "cohort_id": str(uuid4()),
        "window_ids": window_ids,
        "journal_generation": str(uuid4()),
        "plan_id": str(uuid4()),
        "window_id": window_ids[0],
    }
    args = SimpleNamespace(
        session_date=date(2026, 10, 26),
        qualification_semantic_sha256="a" * 64,
        qualification_file_sha256="b" * 64,
    )
    package, bundle = _qualification_artifacts(seed, args)

    assert package["orchestration"]["stage_order"][-1] == (
        "OPTIONS_PROJECTION"
    )
    assert "OPTIONS_PUBLISH" not in package["orchestration"][
        "stage_order"
    ]
    assert package["provider_free_qualification"]["acceptance"][
        "publication"
    ] == {
        "release_created": False,
        "pointer_advanced": False,
        "read_pin_enabled": False,
        "normal_portal_serving": False,
    }
    assert qualification._qualification_payload(
        package,
        bundle,
    )["schema"] == (
        "advanced-options-v3-production-role-qualification-v1"
    )

    without_package_mode = dict(package)
    without_package_mode.pop("validation_mode")
    with pytest.raises(
        RuntimeError,
        match="qualification identity changed",
    ):
        qualification._qualification_payload(
            without_package_mode,
            bundle,
        )

    without_bundle_mode = dict(bundle)
    without_bundle_mode.pop("validation_mode")
    with pytest.raises(
        RuntimeError,
        match="qualification identity changed",
    ):
        qualification._qualification_payload(
            package,
            without_bundle_mode,
        )
