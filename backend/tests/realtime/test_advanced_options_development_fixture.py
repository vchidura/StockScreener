from copy import deepcopy
from datetime import date, datetime, timezone

import pytest

from realtime.advanced_options_development_fixture import (
    build_development_fixture_e,
    validate_development_fixture_e,
)


ENVIRONMENT = {
    "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
    "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
}


def _fixture():
    return build_development_fixture_e(
        session_date=date(2026, 10, 26),
        equity_cutoff=datetime(
            2026,
            10,
            26,
            13,
            45,
            tzinfo=timezone.utc,
        ),
        authorization_reference=(
            "MONDAY-DEVELOPMENT-FIXTURE-E-20261026-B0DA3749"
        ),
        qualification_semantic_sha256="a" * 64,
        qualification_file_sha256="b" * 64,
    )


def test_development_fixture_e_is_exact_and_diagnostic_only():
    fixture = _fixture()

    assert validate_development_fixture_e(
        fixture,
        environment=ENVIRONMENT,
    ) is fixture
    assert fixture["production_eligible"] is False
    assert fixture["serving_authorized"] is False
    assert fixture["publication_authorized"] is False
    assert len(fixture["bars"]) == 6
    assert fixture["equity_receipt"]["receipt_kind"] == "INPUT_READY"


def test_production_strict_rejects_development_fixture_e():
    with pytest.raises(
        ValueError,
        match="fixture E identity changed",
    ):
        validate_development_fixture_e(
            _fixture(),
            environment={},
        )


def test_development_fixture_e_detects_bar_drift():
    fixture = deepcopy(_fixture())
    fixture["bars"][0]["close"] = "0"

    with pytest.raises(
        ValueError,
        match="contents changed",
    ):
        validate_development_fixture_e(
            fixture,
            environment=ENVIRONMENT,
        )
