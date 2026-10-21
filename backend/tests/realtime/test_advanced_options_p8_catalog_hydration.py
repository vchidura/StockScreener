from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.apply_advanced_options_p8_catalog_hydration import (
    _canonical_sha256,
    _entries,
    load_manifest,
)


UTC = timezone.utc


def _manifest():
    captured = datetime(2026, 10, 21, 15, 6, tzinfo=UTC)
    responses = []
    for underlyer, security_id, ticker, kind, strike in (
        (
            "QQQ",
            "11111111-1111-1111-1111-111111111111",
            "O:QQQ261021C00700000",
            "call",
            "700",
        ),
        (
            "SPY",
            "22222222-2222-2222-2222-222222222222",
            "O:SPY261021P00750000",
            "put",
            "750",
        ),
    ):
        payload = {
            "ticker": ticker,
            "underlying_ticker": underlyer,
            "contract_type": kind,
            "expiration_date": "2026-10-21",
            "strike_price": strike,
            "exercise_style": "american",
            "shares_per_contract": 100,
            "primary_exchange": "B",
        }
        responses.append({
            "underlyer": underlyer,
            "candidates": [{
                "ticker": ticker,
                "underlying_ticker": underlyer,
                "underlying_security_id": security_id,
                "contract_type": kind,
                "expiration_date": "2026-10-21",
                "strike_price": strike,
                "exercise_style": "american",
                "shares_per_contract": 100,
                "primary_exchange": "B",
                "provider_payload_sha256": _canonical_sha256(payload),
                "provider_payload": payload,
            }],
        })
    manifest = {
        "schema": "advanced-options-p8-catalog-hydration-candidates-v1",
        "captured_at_utc": captured.isoformat(),
        "session_date": "2026-10-21",
        "provider_requests": 2,
        "pagination_requests": 0,
        "database_writes": 0,
        "responses": responses,
    }
    manifest["manifest_sha256"] = _canonical_sha256(manifest)
    return manifest


def test_manifest_loader_and_catalog_identities_are_deterministic(
    tmp_path: Path,
):
    manifest = _manifest()
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    file_sha256 = __import__("hashlib").sha256(
        path.read_bytes()
    ).hexdigest()

    loaded = load_manifest(
        path,
        expected_manifest_sha256=manifest["manifest_sha256"],
        expected_file_sha256=file_sha256,
    )
    first = _entries(loaded, expected_candidate_count=2)
    second = _entries(loaded, expected_candidate_count=2)

    assert first == second
    assert {
        row.catalog.contract_ticker for row, _ in first
    } == {
        "O:QQQ261021C00700000",
        "O:SPY261021P00750000",
    }
