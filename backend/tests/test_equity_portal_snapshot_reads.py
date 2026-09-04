from fastapi import HTTPException

import main


def test_portal_snapshot_payload_fails_closed_when_missing(monkeypatch):
    monkeypatch.setattr(main, "current_equity_portal_snapshot", lambda _: None)

    try:
        main._portal_snapshot_payload("MARKET_REGIME")
    except HTTPException as exc:
        assert exc.status_code == 503
        assert "unavailable" in exc.detail
    else:
        raise AssertionError("missing snapshot must fail closed")


def test_portal_snapshot_payload_fails_closed_when_stale(monkeypatch):
    monkeypatch.setattr(
        main,
        "current_equity_portal_snapshot",
        lambda _: {"is_fresh": False, "payload": {"bad": True}},
    )

    try:
        main._portal_snapshot_payload("MARKET_REGIME")
    except HTTPException as exc:
        assert exc.status_code == 503
        assert "stale" in exc.detail
    else:
        raise AssertionError("stale snapshot must fail closed")


def test_portal_snapshot_payload_returns_fresh_payload(monkeypatch):
    expected = {"regime": "Strong Bull"}
    monkeypatch.setattr(
        main,
        "current_equity_portal_snapshot",
        lambda _: {"is_fresh": True, "payload": expected},
    )

    assert main._portal_snapshot_payload("MARKET_REGIME") == expected