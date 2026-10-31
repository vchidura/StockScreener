import json
from datetime import date

import pytest

from realtime.contracts import canonical_sha256
from scripts.prepare_advanced_realtime_v3_post_close_replay import (
    _cycles,
    _terminal,
)
from scripts.export_advanced_realtime_v3_post_close_history import (
    _bar_queries,
)
from scripts.qualify_advanced_options_v3_docker import (
    _qualification_acceptance,
)
from scripts.validate_advanced_realtime_v3_post_close_replay import (
    ThrottledArtifactConnection,
)


SESSION_DATE = date(2026, 10, 30)


def _cycle(index, boundary):
    value = {
        "schema": "advanced-realtime-v3-host-segment-cycle-v1",
        "index": index,
        "boundary_utc": boundary,
        "completed_at_utc": boundary,
        "request_count": 1,
        "decoded_bytes": 10,
        "contracts": [
            "O:QQQ261030C00696000",
            "O:QQQ261030P00696000",
            "O:SPY261030C00761000",
            "O:SPY261030P00761000",
        ],
    }
    value["semantic_sha256"] = canonical_sha256(value)
    return value


def _terminal_value():
    return {
        "schema": (
            "advanced-realtime-v3-host-market-segment-terminal-failure-v1"
        ),
        "status": "FAILED_CLOSED",
        "observed_at_utc": "2026-10-30T15:30:30+00:00",
        "last_completed_boundary_utc": "2026-10-30T15:25:00+00:00",
        "failed_boundary_utc": "2026-10-30T15:30:00+00:00",
        "blockers": [],
        "durable_evidence": {
            "cycle_file_count": 2,
            "summary_exists": False,
        },
        "closure": {
            "owned_process_count": 0,
            "retry_authorized": False,
            "restart_authorized": False,
            "post_close_replay_eligible_cycles": 2,
        },
        "production_effects": {
            name: 0
            for name in (
                "database_writes",
                "docker_actions",
                "service_actions",
                "publication",
                "pointer_changes",
                "read_pin_changes",
                "dispatch",
                "delivery",
                "broker",
                "trading",
            )
        },
    }


def test_post_close_cycles_freeze_exact_sequence_and_hashes(tmp_path):
    directory = tmp_path / "cycles"
    directory.mkdir()
    for index, boundary in (
        (1, "2026-10-30T15:20:00+00:00"),
        (2, "2026-10-30T15:25:00+00:00"),
    ):
        (directory / f"cycle-{index:03d}.json").write_text(
            json.dumps(_cycle(index, boundary)),
            encoding="utf-8",
        )

    result = _cycles(
        directory,
        session_date=SESSION_DATE,
        expected_cycles=2,
    )

    assert result["count"] == 2
    assert result["total_decoded_bytes"] == 20
    assert result["last_boundary_utc"] == "2026-10-30T15:25:00+00:00"
    assert len(result["cycles_sha256"]) == 64


def test_post_close_cycles_reject_gaps_and_tampering(tmp_path):
    directory = tmp_path / "cycles"
    directory.mkdir()
    first = _cycle(1, "2026-10-30T15:20:00+00:00")
    second = _cycle(2, "2026-10-30T15:30:00+00:00")
    for index, value in ((1, first), (2, second)):
        (directory / f"cycle-{index:03d}.json").write_text(
            json.dumps(value),
            encoding="utf-8",
        )

    with pytest.raises(ValueError, match="sealed cycle is invalid"):
        _cycles(
            directory,
            session_date=SESSION_DATE,
            expected_cycles=2,
        )


def test_terminal_receipt_requires_zero_effect_failed_close():
    result = _terminal(
        _terminal_value(),
        session_date=SESSION_DATE,
        expected_cycles=2,
    )
    assert result["completed_cycles"] == 2

    invalid = _terminal_value()
    invalid["production_effects"]["database_writes"] = 1
    with pytest.raises(ValueError, match="not replay eligible"):
        _terminal(
            invalid,
            session_date=SESSION_DATE,
            expected_cycles=2,
        )


def test_post_close_export_covers_catalog_revisions_and_current_bars():
    queries = _bar_queries()
    assert set(queries) == {
        "rt.bar_catalog",
        "rt.bar_revisions",
        "rt.bar_current",
    }
    assert all(isinstance(statement, str) for statement in queries.values())


def test_provider_free_qualification_accepts_45_complete_windows():
    acceptance = _qualification_acceptance(45)

    assert acceptance["window_count"] == 45
    assert acceptance["window_mode"] == "ALL_COMPLETE"
    assert "window_2" not in acceptance
    assert acceptance["publication"]["pointer_advanced"] is False


def test_post_close_replay_connection_is_explicitly_throttled():
    connection = ThrottledArtifactConnection(
        [b'[{"ev":"Q"}]'],
        delay_seconds=0.001,
        yield_every_frames=8,
    )

    assert connection.delay_seconds == 0.001
    assert connection.yield_every_frames == 8
