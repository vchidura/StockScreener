import pytest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from realtime.advanced_options_api import (
    AdvancedOptionsStoredProjectionRepository,
    advanced_options_v2_disabled_payload,
    advanced_options_v2_status_payload,
)


def test_advanced_options_v2_read_seam_is_explicitly_disabled():
    status = advanced_options_v2_disabled_payload()
    projection = advanced_options_v2_disabled_payload("OPTIONS_ALERTS")

    assert status["enabled"] is False
    assert status["snapshot_type"] is None
    assert projection["snapshot_type"] == "OPTIONS_ALERTS"
    assert not any(
        projection[name]
        for name in (
            "provider_fetch_performed",
            "detector_work_performed",
            "publication_performed",
            "pointer_advanced",
        )
    )

    with pytest.raises(ValueError, match="projection type"):
        advanced_options_v2_disabled_payload("NOT_A_PROJECTION")


def test_stored_projection_repository_uses_exact_cohort_read_only_cursor():
    cohort_id = uuid4()
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    @contextmanager
    def cursor_factory():
        yield cursor

    repository = AdvancedOptionsStoredProjectionRepository(cursor_factory)
    result = repository.read(
        cohort_id=cohort_id,
        snapshot_type="OPTIONS_ALERTS",
        available_by=datetime(
            2026,
            10,
            20,
            15,
            tzinfo=timezone.utc,
        ),
    )

    assert result["cohort_id"] == str(cohort_id)
    assert result["snapshot_type"] == "OPTIONS_ALERTS"
    assert result["advanced_options_v2_enabled"] is True
    assert result["available"] is False
    assert result["provider_fetch_performed"] is False
    assert result["detector_work_performed"] is False
    assert result["publication_performed"] is False
    assert str(cohort_id) in cursor.execute.call_args.args[1]


def test_status_payload_is_disabled_by_default_and_explicit_when_enabled():
    assert advanced_options_v2_status_payload(enabled=False)["reason"] == (
        "ADVANCED_OPTIONS_V2_DISABLED"
    )
    enabled = advanced_options_v2_status_payload(enabled=True)
    assert enabled["enabled"] is True
    assert enabled["reason"] is None
