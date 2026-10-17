import importlib
import json

import pytest


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
