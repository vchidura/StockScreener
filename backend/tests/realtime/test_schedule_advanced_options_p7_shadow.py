import hashlib
import json
from datetime import datetime, timezone

import pytest

from realtime.advanced_options_p7_shadow import build_live_authorization
from realtime.provider_socket_lease import ProviderSocketLeaseUnavailable
import scripts.schedule_advanced_options_p7_shadow as scheduler
from scripts.schedule_advanced_options_p7_shadow import (
    LIVE_AUTH_VERSION,
    _validate_live_authorization,
)


UTC = timezone.utc


def test_scheduler_blocks_before_provider_when_socket_lease_is_owned(
    tmp_path,
    monkeypatch,
):
    start = datetime(2099, 10, 20, 13, 35, tzinfo=UTC)
    plan = {
        "run_id": "11111111-1111-1111-1111-111111111111",
        "manifest_id": "22222222-2222-2222-2222-222222222222",
        "provider": {
            "connection_lease_resource_key": (
                "massive:options:websocket"
            ),
        },
        "window": {
            "start_utc": start.isoformat(),
            "stop_utc": start.replace(minute=45).isoformat(),
        },
    }
    manifest = tmp_path / "manifest.json"
    approval = tmp_path / "approval.json"
    live_authorization = tmp_path / "live.json"
    status = tmp_path / "status.json"
    lock = tmp_path / "scheduler.lock"
    provider_lock = tmp_path / "provider.lock"
    for path in (manifest, approval, live_authorization):
        path.write_text("{}\n", encoding="utf-8")

    class BlockedLease:
        def __init__(self, *_args):
            pass

        def acquire(self, *, now):
            raise ProviderSocketLeaseUnavailable("owned")

    monkeypatch.setattr(
        scheduler,
        "approved_authorization",
        lambda *_args, **_kwargs: plan,
    )
    monkeypatch.setattr(
        scheduler,
        "_validate_live_authorization",
        lambda _value, **_kwargs: {
            "live_authorization_reference": "TEST:LIVE",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "provider_free_preflight",
        lambda *_args, **_kwargs: {"status": "PASS"},
    )
    monkeypatch.setattr(scheduler, "HostProviderSocketLease", BlockedLease)
    monkeypatch.setattr(
        scheduler.live,
        "main",
        lambda _args: (_ for _ in ()).throw(
            AssertionError("provider execution must not start")
        ),
    )

    result = scheduler.main([
        "--manifest",
        str(manifest),
        "--approval",
        str(approval),
        "--live-authorization",
        str(live_authorization),
        "--status-path",
        str(status),
        "--lock-path",
        str(lock),
        "--provider-lock-path",
        str(provider_lock),
    ])

    assert result == 2
    blocked = json.loads(status.read_text(encoding="utf-8"))
    assert blocked["status"] == "BLOCKED"
    assert blocked["reason"] == "OPTIONS_SOCKET_LEASE_UNAVAILABLE"
    assert blocked["provider_requests"] == 0
    assert blocked["external_socket_connections"] == 0


def test_live_authorization_binds_exact_manifest_approval_and_scope(tmp_path):
    manifest = tmp_path / "manifest.json"
    approval = tmp_path / "approval.json"
    manifest.write_text("manifest\n", encoding="utf-8")
    approval.write_text("approval\n", encoding="utf-8")
    plan = {
        "authorization_reference": "ADVANCED-OPTIONS-P7-TEST",
        "manifest_id": "11111111-1111-1111-1111-111111111111",
        "window": {
            "start_utc": "2026-10-20T13:35:00Z",
            "controlled_reconnect_utc": "2026-10-20T13:40:00Z",
            "stop_utc": "2026-10-20T13:45:00Z",
            "missed_window_policy": "EXPIRE_WITHOUT_ROLLOVER",
        },
        "scope": {
            "contracts": [
                "O:SPY261027C00768000",
                "O:QQQ261027C00710000",
            ]
        },
        "provider": {
            "connection_lease_resource_key": (
                "massive:options:websocket"
            ),
        },
        "socket_ceiling": {
            "connection_attempts": 6,
            "reconnects": 5,
            "reconnect_backoff": {
                "initial_seconds": 5,
                "minimum_seconds": 5,
                "multiplier": 2,
                "maximum_seconds": 60,
                "jitter_ceiling_seconds": 60,
            },
        },
        "database": {
            "name": "test_realtime_v2_options_p7_20261020",
            "role_prefix": "test_rt_options_p7_20261020",
        },
    }
    authorization = build_live_authorization(
        plan,
        manifest_sha256=hashlib.sha256(
            manifest.read_bytes()
        ).hexdigest(),
        approval_sha256=hashlib.sha256(
            approval.read_bytes()
        ).hexdigest(),
        authorized_at=datetime.now(UTC),
    )
    assert authorization["version"] == LIVE_AUTH_VERSION

    assert _validate_live_authorization(
        authorization,
        manifest_path=manifest,
        approval_path=approval,
        plan=plan,
    ) == authorization

    changed = json.loads(json.dumps(authorization))
    changed["provider_scope"]["connection_attempts"] = 7
    with pytest.raises(ValueError, match="scope changed"):
        _validate_live_authorization(
            changed,
            manifest_path=manifest,
            approval_path=approval,
            plan=plan,
        )
