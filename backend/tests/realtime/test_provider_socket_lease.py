import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from realtime.provider_socket_lease import (
    MASSIVE_OPTIONS_SOCKET_RESOURCE,
    HostProviderSocketLease,
    ProviderSocketLeaseUnavailable,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 14, 0, tzinfo=UTC)
OWNER_ONE = UUID("11111111-1111-1111-1111-111111111111")
OWNER_TWO = UUID("22222222-2222-2222-2222-222222222222")


def test_host_provider_socket_lease_is_exclusive_and_reusable(tmp_path):
    path = tmp_path / "massive-options.lock"
    first = HostProviderSocketLease(
        path,
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        OWNER_ONE,
    )
    second = HostProviderSocketLease(
        path,
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        OWNER_TWO,
    )

    first.acquire(now=NOW)

    with pytest.raises(
        ProviderSocketLeaseUnavailable,
        match="massive:options:websocket",
    ):
        second.acquire(now=NOW)

    assert first.evidence()["scope"] == "HOST_LOCAL"
    assert first.evidence()["owner_id"] == str(OWNER_ONE)

    first.release(now=NOW + timedelta(seconds=1))
    second.acquire(now=NOW + timedelta(seconds=2))
    second.release(now=NOW + timedelta(seconds=3))

    metadata = json.loads(second.metadata_path.read_text(encoding="utf-8"))
    assert metadata["owner_id"] == str(OWNER_TWO)
    assert metadata["resource_key"] == MASSIVE_OPTIONS_SOCKET_RESOURCE
    assert metadata["status"] == "RELEASED"
