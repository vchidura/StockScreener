from unittest.mock import MagicMock

from realtime.preflight_repository import (
    persist_preflight_profile,
    persist_tier_transition,
)
from realtime.preflight import TierState, transition_tier
from test_preflight import NOW, _configuration, _evidence
from realtime.preflight import run_passive_preflight


def _connection(*rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = rows
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_preflight_profile_persists_header_and_all_checks_atomically():
    profile = run_passive_preflight(
        _configuration(),
        _evidence(),
        checked_at=NOW,
    )
    connection, cursor = _connection((profile.profile_sha256,), None)

    persist_preflight_profile(connection, profile)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "INSERT INTO rt.capability_profiles" in statements[0]
    assert sum("INSERT INTO rt.capability_checks" in statement for statement in statements) == len(profile.checks)
    assert any("INSERT INTO rt.readiness_transitions" in statement for statement in statements)
    assert any("INSERT INTO rt.readiness_current" in statement for statement in statements)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_tier_transition_persists_approval_reference_and_identity():
    transition = transition_tier(
        TierState.FALLBACK_PENDING_APPROVAL,
        TierState.DEVELOPER_WARMING,
        transitioned_at=NOW,
        reason_code="OPERATOR_SWITCH",
        operator_approved=True,
        operator_approval_reference="approval:fallback-20261003",
    )
    connection, cursor = _connection((transition.transition_sha256,))

    persist_tier_transition(connection, transition)

    statement, parameters = cursor.execute.call_args_list[0].args
    assert "INSERT INTO rt.provider_tier_transitions" in statement
    assert "approval:fallback-20261003" in parameters
    assert connection.commit.call_count == 1


def test_preflight_profile_rejects_stale_readiness_epoch():
    profile = run_passive_preflight(
        _configuration(),
        _evidence(),
        checked_at=NOW,
    )
    connection, _ = _connection(
        (profile.profile_sha256,),
        (
            profile.readiness.status.value,
            list(profile.readiness.reason_codes),
            2,
            str(profile.profile_id),
        ),
    )

    try:
        persist_preflight_profile(
            connection,
            profile,
            readiness_epoch=1,
            owner_epoch=1,
        )
    except ValueError as error:
        assert "stale readiness epoch" in str(error)
    else:
        raise AssertionError("stale readiness epoch was accepted")
    assert connection.rollback.call_count == 1