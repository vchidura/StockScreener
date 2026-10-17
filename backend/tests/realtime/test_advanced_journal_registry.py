from datetime import date
from uuid import UUID

import pytest

from realtime.advanced_journal_registry import (
    build_advanced_journal_registration,
    journal_relative_path,
    measure_journal_capacity,
    resolve_registered_journal_path,
)
from realtime.advanced_journal_replay import (
    JournalReplayQuarantine,
    inspect_registered_journal,
)
from realtime.journal import RawJournalWriter, journal_files_sha256


GENERATION = UUID("11111111-1111-1111-1111-111111111111")
PLAN = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")
COHORT = UUID("44444444-4444-4444-4444-444444444444")
MANIFEST = UUID("55555555-5555-5555-5555-555555555555")


def _registration(root, path):
    return build_advanced_journal_registration(
        root=root,
        path=path,
        journal_generation=GENERATION,
        plan_id=PLAN,
        stream_session_id=SESSION,
        cohort_id=COHORT,
        manifest_id=MANIFEST,
        session_date=date(2026, 10, 17),
        owner_epoch=7,
        normalizer_version="massive_market_event_v1",
        shard_ids=(0,),
        shard_map_sha256="a" * 64,
    )


def test_registered_paths_are_relative_to_one_mounted_root(tmp_path):
    root = tmp_path / "mounted"
    root.mkdir()
    path = root / "sessions" / "journal.sqlite"
    path.parent.mkdir()

    relative = journal_relative_path(root, path)

    assert relative == "sessions/journal.sqlite"
    assert resolve_registered_journal_path(root, relative) == path.resolve()
    with pytest.raises(ValueError, match="beneath"):
        journal_relative_path(root, tmp_path / "outside.sqlite")
    with pytest.raises(ValueError, match="container-stable"):
        resolve_registered_journal_path(root, "../outside.sqlite")


def test_disk_headroom_blocks_when_either_reviewed_threshold_fails(
    tmp_path,
):
    root = tmp_path / "mounted"
    root.mkdir()

    admitted = measure_journal_capacity(
        root,
        minimum_free_bytes=100,
        minimum_free_percent=20,
        disk_usage=lambda _path: (1_000, 700, 300),
    )
    blocked = measure_journal_capacity(
        root,
        minimum_free_bytes=400,
        minimum_free_percent=20,
        disk_usage=lambda _path: (1_000, 700, 300),
    )

    assert admitted.admitted
    assert not blocked.admitted
    assert admitted.root_sha256 == blocked.root_sha256


def test_registry_identity_and_empty_journal_recovery_are_deterministic(
    tmp_path,
):
    root = tmp_path / "mounted"
    root.mkdir()
    path = root / "journal.sqlite"
    registration = _registration(root, path)
    repeated = _registration(root, path)
    with RawJournalWriter(path, generation=GENERATION):
        pass

    recovered_path, _reader, state = inspect_registered_journal(
        root,
        registration,
    )

    assert registration.metadata_sha256 == repeated.metadata_sha256
    assert recovered_path == path
    assert state.generation == GENERATION
    assert state.integrity_check == "ok"
    assert state.raw_committed_offset == 0
    assert len(journal_files_sha256(path)) == 64


def test_missing_registered_journal_is_explicit_quarantine_evidence(
    tmp_path,
):
    root = tmp_path / "mounted"
    root.mkdir()
    registration = _registration(root, root / "missing.sqlite")

    with pytest.raises(
        JournalReplayQuarantine,
        match="MISSING_JOURNAL",
    ) as captured:
        inspect_registered_journal(root, registration)

    assert captured.value.reason == "MISSING_JOURNAL"
