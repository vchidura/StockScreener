import importlib
import json
from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_equity_journal_bridge import (
    AdvancedJournalBlockedError,
)
from realtime.advanced_journal_replay import JournalReplayQuarantine


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_journal_replay"
)


def test_replay_cli_requires_explicit_mode_execution_and_mounted_root(
    tmp_path,
):
    root = tmp_path / "journals"
    root.mkdir()
    base = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--journal-root",
        str(root),
        "--execute",
    ]

    args = script.parse_args([*base, "--once"])

    assert args.once
    assert args.journal_root == root.resolve()
    with pytest.raises(SystemExit):
        script.parse_args(base)
    with pytest.raises(SystemExit):
        script.parse_args([*base[:-1], "--once"])
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--once", "--continuous"])


def test_replay_isolates_quarantine_gap_and_infrastructure_failures(
    tmp_path,
    monkeypatch,
):
    registrations = tuple(
        SimpleNamespace(
            journal_generation=UUID(int=index),
            normalized_committed_offset=5,
        )
        for index in range(1, 5)
    )
    monkeypatch.setattr(
        script,
        "discover_incomplete_advanced_journals_cursor",
        lambda _cursor: registrations,
    )
    outcomes = iter((
        JournalReplayQuarantine("CORRUPT_JOURNAL", "invalid"),
        AdvancedJournalBlockedError("unresolved gap"),
        OSError("database transport unavailable"),
        SimpleNamespace(committed_offset=5, persisted_bars=5),
    ))

    def replay(*_args, **_kwargs):
        result = next(outcomes)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(script, "replay_registered_journal_once", replay)
    quarantined = []
    monkeypatch.setattr(
        script,
        "quarantine_advanced_journal_cursor",
        lambda _cursor, generation, **_kwargs: quarantined.append(generation),
    )
    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(cursor=object())
        )
    )
    emitted = []

    result = script.replay_registered_once(
        provider,
        object(),
        tmp_path,
        checked_at=datetime.now(timezone.utc),
        emit=emitted.append,
    )

    assert result == (1, 1)
    assert quarantined == [registrations[0].journal_generation]
    assert [json.loads(row)["status"] for row in emitted] == [
        "QUARANTINED",
        "BLOCKED_GAP",
        "RETRYABLE_INFRASTRUCTURE",
        "REPLAYED",
    ]
