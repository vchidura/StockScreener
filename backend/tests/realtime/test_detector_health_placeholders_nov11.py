"""R42 (Nov 11): the detectors health check failed on live with psycopg2's 'IndexError: tuple index out of range'
because a LIKE pattern with a literal '%' sat in a parameterized statement. Every statement the detector health
check runs must hold only %s placeholders (or %%), one per parameter."""
import re
from unittest.mock import MagicMock
from uuid import UUID

from scripts import report_realtime_v2_advanced_health as health

COHORT = UUID(int=7)


def _placeholders_match(statement, parameters):
    without_escapes = statement.replace("%%", "")
    return re.sub(r"%s", "", without_escapes).count("%") == 0 and without_escapes.count("%s") == len(parameters)


def test_every_detector_health_statement_has_one_placeholder_per_parameter():
    cursor = MagicMock()
    cursor.fetchone.side_effect = ((4,), (1,), (4,), (2,))
    state, _ = health._detector_health(cursor, COHORT)
    assert state == "READY"
    calls = cursor.execute.call_args_list
    assert len(calls) == 4
    for call in calls:
        statement, parameters = call.args
        assert _placeholders_match(statement, parameters), statement


def test_the_checkpoint_pattern_is_passed_as_a_parameter():
    cursor = MagicMock()
    cursor.fetchone.side_effect = ((4,), (1,), (4,), (2,))
    health._detector_health(cursor, COHORT)
    statement, parameters = cursor.execute.call_args_list[3].args
    assert "consumer_checkpoints" in statement
    assert parameters == (str(COHORT), "advanced-equity-detector-shadow:ADVANCED_EQUITY_S_:%:0")
