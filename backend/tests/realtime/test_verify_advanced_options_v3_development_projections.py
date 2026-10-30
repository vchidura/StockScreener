from copy import deepcopy

import pytest

from realtime.advanced_options_projection import (
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
)
from scripts.verify_advanced_options_v3_development_projections import (
    _validate_summary,
)


def _summary():
    equity_cohort = "11111111-1111-4111-8111-111111111111"
    options_cohort = "22222222-2222-4222-8222-222222222222"
    return {
        "validation_mode": "DEVELOPMENT_DIAGNOSTIC",
        "equity_cohort_id": equity_cohort,
        "equity_receipt": {"cohort_id": equity_cohort},
        "options_cohort_id": options_cohort,
        "options_receipt": {"cohort_id": options_cohort},
        "options_snapshot_types": sorted(
            set(OPTIONS_PROJECTION_TYPES)
            | set(OPTIONS_PAGE_SNAPSHOT_TYPES.values())
        ),
        "release_count": 0,
        "pointer_count": 0,
        "read_pin_count": 0,
        "outbox_count": 0,
    }


def test_development_projection_summary_requires_non_serving_completion():
    summary = _summary()

    assert _validate_summary(summary) is summary


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("validation_mode", "PRODUCTION_STRICT"),
        ("release_count", 1),
        ("pointer_count", 1),
        ("read_pin_count", 1),
        ("outbox_count", 1),
        ("options_snapshot_types", []),
    ),
)
def test_development_projection_summary_rejects_serving_or_gaps(
    field,
    value,
):
    summary = deepcopy(_summary())
    summary[field] = value

    with pytest.raises(
        RuntimeError,
        match="projection postcondition failed",
    ):
        _validate_summary(summary)
