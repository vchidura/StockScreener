import importlib
import json
from datetime import datetime, timezone

import pytest


capture = importlib.import_module(
    "scripts.capture_realtime_v2_advanced_equity_canary"
)
script = importlib.import_module(
    "scripts.provision_realtime_v2_advanced_equity_scope"
)


def test_entitlement_loader_accepts_only_redacted_spy_am_pass(tmp_path):
    path = tmp_path / "entitlement.json"
    path.write_text(
        json.dumps({
            "status": "PASS",
            "checked_at": "2026-10-16T16:35:01+00:00",
            "credentials_printed": False,
            "scope": {
                "authorized_universe": ["SPY"],
                "authorized_channels": ["AM"],
            },
        }),
        encoding="utf-8",
    )

    digest, checked_at = script._load_entitlement(path, ("SPY",))

    assert len(digest) == 64
    assert checked_at == "2026-10-16T16:35:01+00:00"


def test_local_am_canary_emits_provisionable_entitlement_evidence(tmp_path):
    checked_at = datetime(2026, 10, 19, 16, 30, tzinfo=timezone.utc)
    path = tmp_path / "entitlement.json"
    path.write_text(json.dumps({
        "status": "PASS",
        **capture._entitlement_evidence(checked_at),
    }), encoding="utf-8")

    _digest, observed = script._load_entitlement(path, ("SPY",))

    assert observed == checked_at.isoformat()


def test_local_am_canary_requires_only_bounded_provider_settings():
    assert capture._canary_api_key({
        "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
        "POLYGON_API_KEY": "test-provider-key",
    }) == "test-provider-key"

    with pytest.raises(RuntimeError, match="configured Massive API key"):
        capture._canary_api_key({
            "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
            "POLYGON_API_KEY": "...",
        })


def test_entitlement_loader_rejects_broader_or_unredacted_scope(tmp_path):
    path = tmp_path / "entitlement.json"
    path.write_text(
        json.dumps({
            "status": "PASS",
            "checked_at": "2026-10-16T16:35:01+00:00",
            "credentials_printed": True,
            "scope": {
                "authorized_universe": ["SPY", "QQQ"],
                "authorized_channels": ["AM"],
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exact selected-universe/AM"):
        script._load_entitlement(path, ("SPY",))
