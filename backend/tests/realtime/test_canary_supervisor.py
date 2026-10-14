import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from realtime.acquisition_producer import DeveloperAcquisitionProducerResult
from realtime.acquisition_worker import DeveloperAcquisitionWorkerStatus
from realtime.canary_supervisor import (
    MAX_STATUS_BYTES,
    CanaryStageState,
    CanaryStopRequest,
    CanarySupervisorState,
    build_canary_supervisor_status,
    compute_stage_status,
    portal_stage_status,
    producer_stage_status,
    publication_stage_status,
    read_canary_status,
    read_canary_stop_request,
    write_canary_status,
    write_canary_stop_request,
)
from realtime.portal_publisher import PortalPublicationStatus


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
INSTANCE = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
COHORT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _producer():
    return DeveloperAcquisitionProducerResult(
        (
            UUID("11111111-1111-1111-1111-111111111111"),
            UUID("22222222-2222-2222-2222-222222222222"),
        ),
        8,
        8,
        24,
        24_000,
    )


def _compute(*, retried=0, reasons=()):
    claimed = 2 if not retried else 1
    return DeveloperAcquisitionWorkerStatus(
        checked_at=NOW,
        discovered_equity=1,
        discovered_option=1,
        claimed=claimed,
        completed=claimed - retried,
        retried=retried,
        failed=0,
        blocked=0,
        output_count=16 if not retried else 0,
        reason_codes=reasons,
    )


def _publication():
    return PortalPublicationStatus(
        checked_at=NOW,
        promoted_snapshots=8,
        claimed=1,
        delivered=1,
        retried=0,
        abandoned=0,
        reason_codes=(),
    )


def _portal():
    return {
        "status": "READY",
        "ready_tickers": ["AAPL", "AMZN", "IWM", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"],
        "reason_codes": [],
        "canonical_fallback": False,
        "provider_fetch_performed": False,
    }


def _stages():
    return (
        producer_stage_status(_producer(), checked_at=NOW),
        compute_stage_status(_compute()),
        publication_stage_status(_publication()),
        portal_stage_status(_portal(), checked_at=NOW),
    )


def _status(stages=None):
    return build_canary_supervisor_status(
        instance_id=INSTANCE,
        cohort_id=COHORT,
        checked_at=NOW,
        stages=stages or _stages(),
    )


def test_complete_fresh_stage_set_is_deterministic_ready_and_isolated():
    status = _status()

    assert status.state is CanarySupervisorState.READY
    assert status.database_target == "stocks_realtime_v1"
    assert status.canonical_fallback is False
    assert status.reason_codes == ()
    assert len(status.status_sha256) == 64
    assert status.status_sha256 == _status().status_sha256


def test_degraded_and_stale_stage_statuses_fail_closed():
    degraded_stages = (
        _stages()[0],
        compute_stage_status(
            _compute(
                retried=1,
                reasons=("DEVELOPER_ACQUISITION_OUTPUT_FAILED",),
            )
        ),
        *_stages()[2:],
    )
    degraded = _status(degraded_stages)

    assert degraded.state is CanarySupervisorState.DEGRADED
    assert "COMPUTE:DEVELOPER_ACQUISITION_OUTPUT_FAILED" in degraded.reason_codes

    stale_ingress = producer_stage_status(_producer(), checked_at=NOW - timedelta(seconds=31))
    blocked = _status((stale_ingress, *_stages()[1:]))

    assert blocked.state is CanarySupervisorState.BLOCKED
    assert blocked.reason_codes[0] == "INGRESS_STATUS_STALE"


def test_status_write_is_atomic_bounded_redacted_and_hash_checked(tmp_path):
    path = tmp_path / "canary" / "status.json"
    status = _status()

    write_canary_status(path, status)
    restored = read_canary_status(path)

    assert restored == status
    assert not tuple(path.parent.glob("*.tmp"))
    text = path.read_text(encoding="utf-8").lower()
    assert all(secret not in text for secret in ("password", "api_key", "dsn", "token"))

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["state"] = "BLOCKED"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="status hash"):
        read_canary_status(path)

    path.write_bytes(b"x" * (MAX_STATUS_BYTES + 1))
    with pytest.raises(ValueError, match="byte bound"):
        read_canary_status(path)


def test_stop_request_is_atomic_hash_checked_and_instance_fenced(tmp_path):
    path = tmp_path / "stop.json"
    request = CanaryStopRequest(INSTANCE, NOW)

    assert read_canary_stop_request(path, expected_instance_id=INSTANCE) is None
    write_canary_stop_request(path, request)

    assert read_canary_stop_request(path, expected_instance_id=INSTANCE) == request
    with pytest.raises(ValueError, match="another instance"):
        read_canary_stop_request(path, expected_instance_id=UUID(int=0))


def test_portal_stage_rejects_provider_fetch_or_canonical_fallback():
    for change in (
        {"provider_fetch_performed": True},
        {"canonical_fallback": True},
    ):
        payload = {**_portal(), **change}
        with pytest.raises(ValueError, match="violates isolation"):
            portal_stage_status(payload, checked_at=NOW)

    blocked = portal_stage_status(
        {
            **_portal(),
            "status": "BLOCKED",
            "ready_tickers": [],
            "reason_codes": ["TICKER_PAGE_COVERAGE_INCOMPLETE"],
        },
        checked_at=NOW,
    )
    assert blocked.state is CanaryStageState.BLOCKED
