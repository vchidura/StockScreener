import hashlib
import json
from datetime import datetime, timezone

import pytest

from scripts.schedule_advanced_options_p7_shadow import (
    LIVE_AUTH_REFERENCE,
    LIVE_AUTH_VERSION,
    _validate_live_authorization,
)


UTC = timezone.utc


def test_live_authorization_binds_exact_manifest_approval_and_scope(tmp_path):
    manifest = tmp_path / "manifest.json"
    approval = tmp_path / "approval.json"
    manifest.write_text("manifest\n", encoding="utf-8")
    approval.write_text("approval\n", encoding="utf-8")
    plan = {
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
        "database": {
            "name": "test_realtime_v2_options_p7_20261020",
            "role_prefix": "test_rt_options_p7_20261020",
        },
    }
    authorization = {
        "version": LIVE_AUTH_VERSION,
        "state": "AUTHORIZED_FOR_ONE_EXECUTION",
        "live_authorization_reference": LIVE_AUTH_REFERENCE,
        "authorized_at_utc": datetime.now(UTC).isoformat(),
        "manifest_id": plan["manifest_id"],
        "manifest_sha256": hashlib.sha256(
            manifest.read_bytes()
        ).hexdigest(),
        "approval_sha256": hashlib.sha256(
            approval.read_bytes()
        ).hexdigest(),
        "window": plan["window"],
        "contracts": plan["scope"]["contracts"],
        "provider_scope": {
            "feed": "OPTIONS",
            "channels": ["T", "Q"],
            "connection_attempts": 2,
            "reconnects": 1,
            "rest_requests": 0,
        },
        "disposable_scope": {
            "database": plan["database"]["name"],
            "role_prefix": plan["database"]["role_prefix"],
            "owned_local_evidence": True,
        },
        "confirmations": {
            "no_equity_qualification_overlap": True,
            "no_stages_v4_selection": True,
            "no_production_database_writes": True,
            "no_publication_release_pointer": True,
            "no_outbox_delivery_execution": True,
            "no_repair_replay_backfill": True,
            "no_archive_retention_execution": True,
        },
    }

    assert _validate_live_authorization(
        authorization,
        manifest_path=manifest,
        approval_path=approval,
        plan=plan,
    ) == authorization

    changed = json.loads(json.dumps(authorization))
    changed["provider_scope"]["connection_attempts"] = 3
    with pytest.raises(ValueError, match="scope changed"):
        _validate_live_authorization(
            changed,
            manifest_path=manifest,
            approval_path=approval,
            plan=plan,
        )
