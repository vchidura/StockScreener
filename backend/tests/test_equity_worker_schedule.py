import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.run_equity_worker import (
    HEARTBEAT_SECONDS,
    ingest_due_interval,
    latest_completed_slot,
    latest_due_slot,
    mature_prospective_scanner_outcomes,
    parser,
)
from equity.orchestration import IngestionCoverageError


UTC = timezone.utc


def test_30m_slot_uses_xnys_open_and_only_completed_window():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 59, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 28, 14, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 14, 0, tzinfo=UTC)


def test_15m_slot_and_weekend_handling():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 45, tzinfo=UTC), "15m"
    ) == datetime(2026, 8, 28, 13, 45, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 29, 15, 0, tzinfo=UTC), "15m"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


def test_5m_slot_uses_only_completed_xnys_window():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 34, tzinfo=UTC), "5m"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 35, tzinfo=UTC), "5m"
    ) == datetime(2026, 8, 28, 13, 35, tzinfo=UTC)


def test_latest_due_slot_schedules_derived_intervals_at_completed_boundaries():
    assert latest_due_slot(
        datetime(2026, 8, 28, 14, 30, tzinfo=UTC), "1h"
    ) == datetime(2026, 8, 28, 14, 30, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC), "1d"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 30, 20, 0, tzinfo=UTC), "1mo"
    ) == datetime(2026, 7, 31, 20, 0, tzinfo=UTC)


def test_latest_due_slot_respects_delayed_provider_watermark():
    delay = timedelta(minutes=15)

    assert latest_due_slot(
        datetime(2026, 8, 31, 13, 57, tzinfo=UTC),
        "5m",
        provider_delay=delay,
    ) == datetime(2026, 8, 31, 13, 40, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 31, 13, 57, tzinfo=UTC),
        "15m",
        provider_delay=delay,
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
        "15m",
        provider_delay=delay,
    ) == datetime(2026, 8, 31, 13, 45, tzinfo=UTC)


def test_latest_due_slot_catches_up_prior_session_after_utc_midnight():
    assert latest_due_slot(
        datetime(2026, 9, 2, 2, 30, tzinfo=UTC),
        "30m",
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 9, 1, 20, 0, tzinfo=UTC)


def test_latest_due_slot_catches_up_prior_session_on_weekend():
    assert latest_due_slot(
        datetime(2026, 9, 5, 15, 0, tzinfo=UTC),
        "5m",
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 9, 4, 20, 0, tzinfo=UTC)


def test_latest_due_slot_rejects_negative_provider_delay():
    try:
        latest_due_slot(
            datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
            "5m",
            provider_delay=timedelta(minutes=-1),
        )
    except ValueError as exc:
        assert str(exc) == "provider_delay must not be negative"
    else:
        raise AssertionError("negative provider delay must fail")


def test_worker_once_flag_is_opt_in():
    assert parser().parse_args([]).once is False
    assert parser().parse_args(["--once"]).once is True
    assert HEARTBEAT_SECONDS > 0


def test_continuous_worker_retries_native_coverage_failure(caplog):
    class Service:
        def ingest_native_interval(self, *args, **kwargs):
            raise IngestionCoverageError(
                "native 5m ingestion coverage failed: 48/386 tickers unavailable"
            )

    slot = datetime(2026, 9, 2, 20, 45, tzinfo=UTC)

    assert ingest_due_interval(
        Service(), (), interval="5m", slot=slot, observed_at=slot, once=False
    ) is None
    assert "publication was skipped and the cycle will retry" in caplog.text


def test_worker_once_surfaces_native_coverage_failure():
    class Service:
        def ingest_native_interval(self, *args, **kwargs):
            raise IngestionCoverageError("provider coverage failed")

    slot = datetime(2026, 9, 2, 20, 45, tzinfo=UTC)

    with pytest.raises(IngestionCoverageError, match="provider coverage failed"):
        ingest_due_interval(
            Service(), (), interval="5m", slot=slot, observed_at=slot, once=True
        )


def test_early_close_never_creates_slot_after_session_close():
    assert latest_completed_slot(
        datetime(2026, 11, 27, 20, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_worker_matures_both_composite_return_modes_prospectively():
    class Service:
        def __init__(self):
            self.calls = []

        def evaluate_directional_outcomes(self, policy, horizon_key, **kwargs):
            self.calls.append((policy, horizon_key, kwargs))
            return object()

    service = Service()
    available_by = datetime(2026, 9, 1, 20, 15, tzinfo=UTC)

    results = mature_prospective_scanner_outcomes(
        service, "1d", available_by=available_by
    )

    assert len(results) == 42
    assert len(service.calls) == 42
    assert {
        policy.policy_key.removesuffix(":SECTOR_PRIMARY").rsplit(":", 1)[-1]
        for policy, _, _ in service.calls
    } == {"SIGNED", "RECOMMENDATION_PLAN"}
    assert all(
        policy.policy_key.endswith(":SECTOR_PRIMARY")
        for policy, _, _ in service.calls
    )
    assert all(
        kwargs == {"available_by": available_by, "prospective_only": True}
        for _, _, kwargs in service.calls
    )


def test_worker_skips_scanner_outcomes_for_non_scanner_interval():
    class Service:
        def evaluate_directional_outcomes(self, *args, **kwargs):
            raise AssertionError("non-scanner interval must not evaluate outcomes")

    assert mature_prospective_scanner_outcomes(
        Service(), "15m", available_by=datetime(2026, 9, 1, tzinfo=UTC)
    ) == ()