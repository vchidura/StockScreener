from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, uuid5

from realtime.contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    CapabilityVerdict,
    ReadinessStatus,
)
from realtime.equity_validation_control_plane import (
    build_equity_validation_control_plane_plan,
    persist_equity_validation_control_plane,
    persist_equity_validation_instruments,
)
from realtime.preflight import ProviderTier
from realtime.reference_seed import SeedStatus
from tests.realtime.test_preflight import NOW


def _seed():
    return SimpleNamespace(
        seed_id=uuid5(NAMESPACE_URL, "equity-validation-seed"),
        seed_sha256="a" * 64,
        universe_sha256=EQUITY_VALIDATION_UNIVERSE.sha256,
        session_date=NOW.date(),
        status=SeedStatus.READY,
        effective_at=NOW,
    )


def _profile():
    return SimpleNamespace(
        profile_id=uuid5(NAMESPACE_URL, "equity-validation-profile"),
        profile_sha256="b" * 64,
        universe_sha256=EQUITY_VALIDATION_UNIVERSE.sha256,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        verdict=CapabilityVerdict.FAIL,
        readiness=SimpleNamespace(status=ReadinessStatus.BLOCKED),
    )


def _plan():
    security_ids = {
        ticker: uuid5(NAMESPACE_URL, f"equity-validation-security:{ticker}")
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    }
    return build_equity_validation_control_plane_plan(
        _seed(),
        _profile(),
        security_ids=security_ids,
        configuration_sha256="c" * 64,
        model_bundle_sha256="d" * 64,
        valuation_sha256="e" * 64,
        effective_at=NOW,
    )


def test_equity_validation_plan_is_exact_deterministic_and_option_free():
    first = _plan()
    repeated = _plan()

    assert first == repeated
    assert first.manifest.universe == EQUITY_VALIDATION_UNIVERSE
    assert first.manifest.maximum_option_quote_contracts == 0
    assert len(first.manifest.members) == 27
    assert len(first.security_ids) == 9


def test_equity_validation_repositories_persist_only_shadow_control_state():
    plan = _plan()
    instrument_cursor = MagicMock()
    instrument_cursor.fetchone.side_effect = [(index,) for index in range(1, 10)]
    instrument_connection = MagicMock()
    instrument_connection.cursor.return_value.__enter__.return_value = instrument_cursor

    instrument_ids = persist_equity_validation_instruments(instrument_connection, plan)

    assert len(instrument_ids) == 9
    assert instrument_connection.commit.call_count == 1

    control_cursor = MagicMock()
    control_cursor.fetchone.side_effect = [
        ("FAIL", plan.profile_sha256),
        (plan.manifest.sha256, "EQUITY_VALIDATION"),
        (plan.cohort_sha256, "SHADOW"),
    ]
    control_connection = MagicMock()
    control_connection.cursor.return_value.__enter__.return_value = control_cursor

    persist_equity_validation_control_plane(control_connection, plan, instrument_ids)

    statements = [call.args[0] for call in control_cursor.execute.call_args_list]
    assert sum("INSERT INTO rt.subscription_members" in row for row in statements) == 27
    assert any("'EQUITY_VALIDATION'" in row for row in statements)
    assert any("INSERT INTO rt.cohorts" in row for row in statements)
    assert not any("readiness" in row for row in statements)
    assert not any("portal_cohort" in row for row in statements)
    assert control_connection.commit.call_count == 1