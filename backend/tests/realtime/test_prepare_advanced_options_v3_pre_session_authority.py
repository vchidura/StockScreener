from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from scripts import prepare_advanced_options_v3_pre_session_authority as script


UTC = timezone.utc
SESSION = date(2026, 10, 23)


def _args(tmp_path, captured_at):
    capture = tmp_path / "capture.json"
    value = {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "captured_at_utc": captured_at.isoformat(),
        "session_date": SESSION.isoformat(),
        "semantic_sha256": "a" * 64,
    }
    capture.write_text(
        json.dumps(value, sort_keys=True),
        encoding="utf-8",
    )
    return SimpleNamespace(
        capture=capture,
        capture_semantic_sha256="a" * 64,
        capture_file_sha256=hashlib.sha256(
            capture.read_bytes()
        ).hexdigest(),
        session_date=SESSION,
        authorization_reference="V3-SPLIT-AUTHORITY-TEST",
        package_nonce=UUID(
            "11111111-2222-3333-4444-555555555555"
        ),
        acquisition_start=datetime(
            2026,
            10,
            23,
            13,
            10,
            tzinfo=UTC,
        ),
        hard_stop=datetime(2026, 10, 23, 20, 0, tzinfo=UTC),
        output=tmp_path / "authority.json",
    )


def test_pre_session_authority_uses_capture_clock_before_open(
    monkeypatch,
    tmp_path,
):
    captured_at = datetime(2026, 10, 23, 13, 0, tzinfo=UTC)
    args = _args(tmp_path, captured_at)
    calls = []
    pool = SimpleNamespace(closeall=lambda: None)
    monkeypatch.setattr(
        script,
        "_provider",
        lambda: (object(), pool, {}),
    )

    def prepare(provider, capture, **kwargs):
        calls.append((provider, capture, kwargs))
        return (
            SimpleNamespace(
                contracts=(
                    SimpleNamespace(contract_ticker="O:QQQ"),
                ),
            ),
            SimpleNamespace(readiness_sha256="b" * 64),
            SimpleNamespace(),
        )

    monkeypatch.setattr(
        script,
        "_prepare_session_authority",
        prepare,
    )
    monkeypatch.setattr(
        script,
        "prospective_opening_admission_payload",
        lambda _value: {"admission_sha256": "c" * 64},
    )
    monkeypatch.setattr(
        script,
        "session_acquisition_authorization_payload",
        lambda _value: {"authorization_sha256": "d" * 64},
    )

    result = script.execute(args)
    document = json.loads(args.output.read_text(encoding="utf-8"))

    assert result["status"] == "COMPLETE"
    assert result["provider_requests"] == 0
    assert document["provider_requests"] == 0
    assert document["source_capture_semantic_sha256"] == "a" * 64
    assert calls[0][2]["prepared_at"] == captured_at
    assert calls[0][2]["acquisition_start"] == datetime(
        2026,
        10,
        23,
        13,
        10,
        tzinfo=UTC,
    )
    assert calls[0][2]["capture_boundary"] == datetime(
        2026,
        10,
        23,
        13,
        30,
        tzinfo=UTC,
    )


def test_pre_session_authority_rejects_post_open_capture(tmp_path):
    args = _args(
        tmp_path,
        datetime(2026, 10, 23, 13, 30, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="actual pre-opening capture"):
        script.execute(args)


def test_pre_session_authority_accepts_only_five_minute_close_drain(
    tmp_path,
):
    capture = tmp_path / "capture.json"
    capture.write_text("{}", encoding="utf-8")
    common = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--session-date",
        "2026-10-28",
        "--capture",
        str(capture),
        "--capture-semantic-sha256",
        "a" * 64,
        "--capture-file-sha256",
        "b" * 64,
        "--authorization-reference",
        "close-drain-test",
        "--package-nonce",
        "11111111-2222-3333-4444-555555555555",
        "--acquisition-start",
        "2026-10-28T13:10:00+00:00",
        "--output",
        str(tmp_path / "authority.json"),
        "--execute",
    ]

    accepted = script.parse_args([
        *common,
        "--hard-stop",
        "2026-10-28T20:05:00+00:00",
    ])
    assert accepted.hard_stop == datetime(
        2026,
        10,
        28,
        20,
        5,
        tzinfo=UTC,
    )
    assert accepted.acquisition_start == datetime(
        2026,
        10,
        28,
        13,
        10,
        tzinfo=UTC,
    )
    with pytest.raises(SystemExit):
        script.parse_args([
            *common,
            "--hard-stop",
            "2026-10-28T20:05:00.000001+00:00",
        ])
