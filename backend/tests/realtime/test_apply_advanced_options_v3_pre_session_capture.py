from types import SimpleNamespace

import pytest

from scripts.apply_advanced_options_v3_pre_session_capture import (
    _authorized_effects,
)


def _args(**changes):
    values = {
        "expected_contract_inserts": 0,
        "expected_version_inserts": 4,
        "expected_instrument_inserts": 4,
        "expected_oi_inserts": 4,
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_capture_application_effects_are_exact():
    effects = _authorized_effects(
        _args(),
        {
            "conflicts": [],
            "contract_inserts": 0,
            "version_inserts": 4,
            "instrument_inserts": 4,
        },
        4,
    )

    assert effects == {
        "contract_inserts": 0,
        "version_inserts": 4,
        "instrument_inserts": 4,
        "oi_inserts": 4,
    }


def test_capture_application_plan_reports_unbound_effects():
    effects = _authorized_effects(
        _args(
            expected_contract_inserts=None,
            expected_version_inserts=None,
            expected_instrument_inserts=None,
            expected_oi_inserts=None,
        ),
        {
            "conflicts": [],
            "contract_inserts": 1,
            "version_inserts": 4,
            "instrument_inserts": 4,
        },
        4,
        enforce=False,
    )

    assert effects == {
        "contract_inserts": 1,
        "version_inserts": 4,
        "instrument_inserts": 4,
        "oi_inserts": 4,
    }


@pytest.mark.parametrize(
    "changes",
    (
        {"expected_contract_inserts": 1},
        {"expected_version_inserts": 3},
        {"expected_instrument_inserts": 3},
        {"expected_oi_inserts": 3},
    ),
)
def test_capture_application_rejects_effect_budget_drift(changes):
    with pytest.raises(
        ValueError,
        match="application effects changed",
    ):
        _authorized_effects(
            _args(**changes),
            {
                "conflicts": [],
                "contract_inserts": 0,
                "version_inserts": 4,
                "instrument_inserts": 4,
            },
            4,
        )
