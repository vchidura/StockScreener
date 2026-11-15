"""Options journal retention (R54 part 3, G106 H3, R81; Nov 15): hot window, current/previous session protection,
finalize gating, archive + verify + delete on temp files, refusals, and a dry run that writes nothing."""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID, uuid5, NAMESPACE_URL

import pytest

from realtime.advanced_options_journal_finalize import (
    FLOW_CONSUMER,
    MARKET_STATE_CONSUMER,
    JournalEnd,
    OptionsJournalRegistration,
    plan_finalize,
)
from realtime.advanced_options_journal_retention import (
    ArchiveManifest,
    RetentionError,
    Step,
    Tier,
    archive_options_journal,
    delete_options_journal_originals,
    load_archive_manifests,
    manifest_path,
    plan_options_journal_retention,
    protected_sessions,
    restore_options_journal,
    scan_options_journals,
    select_codec,
    verify_options_journal_archive,
)

NOW = datetime(2026, 11, 15, 8, 0, tzinfo=timezone.utc)
SUNDAY = date(2026, 11, 15)
# The live Options sessions on Nov 15 (one journal each).
SESSIONS = (
    date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6), date(2026, 11, 9),
    date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13),
)


def _generation(session: date) -> UUID:
    return uuid5(NAMESPACE_URL, f"test-options-journal:{session}")


def _registration(session: date, status: str = "DRAINED", path: str | None = None) -> OptionsJournalRegistration:
    generation = _generation(session)
    return OptionsJournalRegistration(
        generation, uuid5(NAMESPACE_URL, "cohort"), session,
        path or f"options-acquisition/{generation}/options.sqlite", status, 10, 20, "c" * 64,
    )


def _write_journal(root: Path, generation: UUID, *, payload: int = 4096) -> Path:
    """A real WAL journal: main file + -wal + -shm copied while the writer is open (rows still in the WAL)."""
    build = root.parent / f".build-{generation}"
    build.mkdir(parents=True, exist_ok=True)
    source = build / "options.sqlite"
    connection = sqlite3.connect(source)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE raw_messages (raw_offset INTEGER PRIMARY KEY, payload BLOB)")
    connection.execute("CREATE TABLE normalized_events (event_offset INTEGER PRIMARY KEY, payload BLOB)")
    connection.executemany("INSERT INTO raw_messages VALUES (?, ?)", [(n, os.urandom(payload)) for n in range(1, 11)])
    connection.executemany("INSERT INTO normalized_events VALUES (?, ?)", [(n, b"e") for n in range(1, 21)])
    connection.commit()
    directory = root / "options-acquisition" / str(generation)
    directory.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        shutil.copyfile(str(source) + suffix, directory / ("options.sqlite" + suffix))
    connection.close()
    shutil.rmtree(build)
    return directory / "options.sqlite"


def _tree(root: Path) -> dict[str, tuple[int, int]]:
    """Every file's size and mtime, plus content hashes. A WAL journal's -shm is SQLite's shared-memory index: the
    R54 read of a journal's end (read-only connection) may update its read marks, so only its size is compared."""
    tree = {}
    for path in sorted(root.rglob("*")):
        stat = path.stat()
        if path.is_file() and path.name.endswith("-shm"):
            tree[str(path.relative_to(root))] = (stat.st_size, 0, "")
        elif path.is_file():
            tree[str(path.relative_to(root))] = (stat.st_size, stat.st_mtime_ns, path.read_bytes())
        else:
            tree[str(path.relative_to(root))] = (0, 0, "dir")
    return tree


def _clock():
    return NOW


def _hot_filler() -> OptionsJournalRegistration:
    """A newer registered session (no files) that takes the one hot slot in hot_sessions=1 plans."""
    return _registration(date(2026, 11, 12))


# ------------------------------------------------------------------------------------------------------- planning


def test_protected_sessions_on_a_weekend_and_on_a_session_day():
    assert protected_sessions(SUNDAY) == (date(2026, 11, 16), date(2026, 11, 13))
    assert protected_sessions(date(2026, 11, 16)) == (date(2026, 11, 16), date(2026, 11, 13))
    assert protected_sessions(date(2026, 11, 17)) == (date(2026, 11, 17), date(2026, 11, 16))


def test_hot_ten_archives_nothing_with_nine_sessions():
    plan = plan_options_journal_retention(
        [_registration(day) for day in SESSIONS], {}, {}, today=SUNDAY, hot_sessions=10,
    )
    summary = plan.summary()
    assert summary["archive"] == summary["delete"] == summary["finalize"] == []
    assert summary["cold_sessions"] == []
    assert {entry.tier for entry in plan.entries} == {Tier.HOT, Tier.PROTECTED}
    assert plan.entry(_generation(date(2026, 11, 13))).tier is Tier.PROTECTED


def test_hot_three_plans_exactly_the_older_sessions(tmp_path):
    for day in SESSIONS:
        _write_journal(tmp_path, _generation(day), payload=16)
    plan = plan_options_journal_retention(
        [_registration(day) for day in SESSIONS], scan_options_journals(tmp_path), {}, today=SUNDAY, hot_sessions=3,
    )
    summary = plan.summary()
    assert summary["hot_dates"] == ["2026-11-13", "2026-11-12", "2026-11-11"]
    assert summary["cold_sessions"] == [day.isoformat() for day in SESSIONS[:6]]
    assert summary["archive"] == summary["delete"] == [str(_generation(day)) for day in SESSIONS[:6]]
    assert plan.entry(_generation(date(2026, 11, 13))).steps == ()


def test_current_and_previous_session_are_protected_even_with_hot_one():
    rows = [_registration(day, status="WRITING") for day in (date(2026, 11, 12), date(2026, 11, 13))]
    rows.append(_registration(date(2026, 11, 16), status="WRITING"))
    plan = plan_options_journal_retention(rows, {}, {}, today=date(2026, 11, 16), hot_sessions=1)
    assert plan.entry(_generation(date(2026, 11, 16))).tier is Tier.PROTECTED
    assert plan.entry(_generation(date(2026, 11, 13))).tier is Tier.PROTECTED
    assert plan.entry(_generation(date(2026, 11, 13))).reasons == ("PROTECTED_CURRENT_OR_PREVIOUS_SESSION",)
    assert plan.entry(_generation(date(2026, 11, 12))).tier is Tier.COLD


def test_finalize_gating_and_delete_waits_for_drained(tmp_path):
    days = (date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6))
    for day in days:
        _write_journal(tmp_path, _generation(day), payload=16)
    rows = [
        _registration(days[0], status="DRAINING"),  # finalizable -> FINALIZE, ARCHIVE, DELETE
        _registration(days[1], status="WRITING"),  # consumers behind -> archive only
        _registration(days[2], status="QUARANTINED"),  # never touched
        _registration(days[3], status="DRAINED"),  # ARCHIVE, DELETE
    ]
    end = JournalEnd(10, 20, "d" * 64)
    finalize = {
        rows[0].journal_generation: plan_finalize(rows[0], end, {MARKET_STATE_CONSUMER: 20, FLOW_CONSUMER: 10}),
        rows[1].journal_generation: plan_finalize(rows[1], end, {MARKET_STATE_CONSUMER: 5, FLOW_CONSUMER: 10}),
    }
    plan = plan_options_journal_retention(
        [*rows, _hot_filler()], scan_options_journals(tmp_path), {}, today=SUNDAY, hot_sessions=1,
        finalize_plans=finalize,
    )
    assert plan.entry(rows[0].journal_generation).steps == (Step.FINALIZE, Step.ARCHIVE, Step.DELETE)
    behind = plan.entry(rows[1].journal_generation)
    assert behind.steps == (Step.ARCHIVE,)
    assert "DELETE_WAITS_FOR_DRAINED" in behind.reasons
    assert any(reason.startswith("FINALIZE_SEAL_ONLY") for reason in behind.reasons)
    assert plan.entry(rows[2].journal_generation).steps == ()
    assert plan.entry(rows[2].journal_generation).reasons == ("STATUS_QUARANTINED_KEPT",)
    assert plan.entry(rows[3].journal_generation).steps == (Step.ARCHIVE, Step.DELETE)


def test_orphans_missing_files_and_unexpected_paths_are_kept(tmp_path):
    orphan = uuid5(NAMESPACE_URL, "orphan")
    _write_journal(tmp_path, orphan, payload=16)
    rows = [
        _registration(date(2026, 11, 3)),  # registered, no files on disk
        _registration(date(2026, 11, 4), path="options-acquisition/elsewhere/options.sqlite"),
    ]
    plan = plan_options_journal_retention(
        [*rows, _hot_filler()], scan_options_journals(tmp_path), {}, today=SUNDAY, hot_sessions=1,
    )
    assert plan.entry(orphan).tier is Tier.ORPHAN and plan.entry(orphan).steps == ()
    assert plan.entry(rows[0].journal_generation).reasons == ("JOURNAL_FILES_MISSING",)
    assert plan.entry(rows[1].journal_generation).reasons == ("UNEXPECTED_RELATIVE_PATH",)


# ---------------------------------------------------------------------------------------- archive / verify / delete


def _cold_entry(root: Path, day: date = date(2026, 11, 3), status: str = "DRAINED"):
    plan = plan_options_journal_retention(
        [_registration(day, status=status), _hot_filler()], scan_options_journals(root),
        load_archive_manifests(root / "archive"),
        today=SUNDAY, hot_sessions=1,
        finalize_plans=None,
    )
    return plan.entry(_generation(day))


def test_archive_verify_delete_and_restore_round_trip(tmp_path):
    root, archive_root = tmp_path / "journals", tmp_path / "journals" / "archive"
    path = _write_journal(root, _generation(date(2026, 11, 3)))
    originals = {p.name: p.read_bytes() for p in path.parent.iterdir()}
    assert "options.sqlite-wal" in originals
    entry = _cold_entry(root)
    assert entry.steps == (Step.ARCHIVE, Step.DELETE)
    manifest = archive_options_journal(root, archive_root, entry, codec="xz", now=_clock)
    assert not manifest.verified
    assert {m.name for m in manifest.members} == set(originals)
    verified = verify_options_journal_archive(archive_root, manifest, now=_clock)
    assert verified.verified
    stored = json.loads(manifest_path(archive_root, entry.journal_generation).read_text(encoding="utf-8"))
    assert stored["verified"] is True and stored["codec"] == "xz"
    assert stored["archive_name"].endswith(".tar.xz")
    # a verified archive covers the files: only the delete step is left
    again = _cold_entry(root)
    assert again.steps == (Step.DELETE,)
    deleted = delete_options_journal_originals(root, archive_root, again, verified, registry_status="DRAINED", now=_clock)
    assert deleted.deleted_at == NOW
    assert not path.parent.exists()
    after = _cold_entry(root)
    assert after.steps == () and after.reasons == ("ARCHIVED_ORIGINALS_DELETED",)
    restored = restore_options_journal(root, archive_root, deleted)
    assert {p.name: p.read_bytes() for p in restored.iterdir()} == originals


def test_delete_refuses_unverified_archive_not_drained_or_changed_originals(tmp_path):
    root, archive_root = tmp_path / "journals", tmp_path / "journals" / "archive"
    path = _write_journal(root, _generation(date(2026, 11, 3)))
    entry = _cold_entry(root)
    manifest = archive_options_journal(root, archive_root, entry, codec="xz", now=_clock)
    with pytest.raises(RetentionError, match="not verified"):
        delete_options_journal_originals(root, archive_root, entry, manifest, registry_status="DRAINED")
    verified = verify_options_journal_archive(archive_root, manifest, now=_clock)
    with pytest.raises(RetentionError, match="not DRAINED"):
        delete_options_journal_originals(root, archive_root, entry, verified, registry_status="DRAINING")
    with path.open("r+b") as handle:  # same size, different bytes
        handle.seek(200)
        byte = handle.read(1)
        handle.seek(200)
        handle.write(bytes([byte[0] ^ 0xFF]))
    with pytest.raises(RetentionError, match="changed since it was archived"):
        delete_options_journal_originals(root, archive_root, entry, verified, registry_status="DRAINED")
    assert path.is_file()


def test_verify_refuses_a_corrupted_archive(tmp_path):
    root, archive_root = tmp_path / "journals", tmp_path / "journals" / "archive"
    _write_journal(root, _generation(date(2026, 11, 3)))
    entry = _cold_entry(root)
    manifest = archive_options_journal(root, archive_root, entry, codec="xz", now=_clock)
    archive = archive_root / manifest.archive_name
    data = bytearray(archive.read_bytes())
    data[len(data) // 2] ^= 0xFF
    archive.write_bytes(bytes(data))
    with pytest.raises(RetentionError):
        verify_options_journal_archive(archive_root, manifest, now=_clock)
    stored = json.loads(manifest_path(archive_root, entry.journal_generation).read_text(encoding="utf-8"))
    assert stored["verified"] is False


def test_archive_refuses_unplanned_journals(tmp_path):
    root = tmp_path / "journals"
    _write_journal(root, _generation(date(2026, 11, 13)))
    plan = plan_options_journal_retention(
        [_registration(date(2026, 11, 13), status="WRITING")], scan_options_journals(root), {},
        today=SUNDAY, hot_sessions=1,
    )
    entry = plan.entry(_generation(date(2026, 11, 13)))
    assert entry.tier is Tier.PROTECTED
    with pytest.raises(RetentionError, match="not planned"):
        archive_options_journal(root, root / "archive", entry, codec="xz")
    assert not (root / "archive").exists()


def test_codec_selection():
    assert select_codec("auto", which=lambda _: None) == "xz"
    assert select_codec("auto", which=lambda _: "/usr/bin/zstd") == "zstd"
    with pytest.raises(RetentionError):
        select_codec("zstd", which=lambda _: None)
    with pytest.raises(ValueError):
        select_codec("gzip")


# ------------------------------------------------------------------------------------------------- CLI dry run


def _snapshot(tmp_path: Path, rows) -> Path:
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({
        "registrations": [{
            "journal_generation": str(row.journal_generation), "cohort_id": str(row.cohort_id),
            "session_date": row.session_date.isoformat(), "relative_path": row.relative_path, "status": row.status,
            "raw_committed_offset": row.raw_committed_offset,
            "normalized_committed_offset": row.normalized_committed_offset,
            "sealed_files_sha256": row.sealed_files_sha256,
        } for row in rows],
        "consumers": {str(row.journal_generation): {MARKET_STATE_CONSUMER: 20, FLOW_CONSUMER: 10} for row in rows},
    }), encoding="utf-8")
    return path


def test_cli_dry_run_writes_nothing(tmp_path, capsys):
    from scripts.retain_advanced_options_journals import main

    root = tmp_path / "journals"
    rows = [_registration(day, status="DRAINING") for day in SESSIONS]
    for day in SESSIONS:
        _write_journal(root, _generation(day), payload=16)
    snapshot = _snapshot(tmp_path, rows)
    before = _tree(tmp_path)
    assert main(["--registry-snapshot", str(snapshot), "--journal-dir", str(root), "--hot-sessions", "3",
                 "--today", "2026-11-15"]) == 0
    assert _tree(tmp_path) == before
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    plan = [line for line in lines if line["line"] == "plan"][0]
    assert plan["dry_run"] is True
    assert plan["cold_sessions"] == [day.isoformat() for day in SESSIONS[:6]]
    # stub registry offsets (10, 20) equal the journal ends: DRAINING with a main-only hash -> SEAL_AND_DRAIN
    assert plan["finalize"] == [str(_generation(day)) for day in SESSIONS[:6]]


def test_cli_rehearsal_apply_archive_and_delete_from_a_snapshot(tmp_path, capsys):
    from scripts.retain_advanced_options_journals import main

    root = tmp_path / "journals"
    rows = [_registration(day, status="DRAINED") for day in SESSIONS]
    for day in SESSIONS:
        _write_journal(root, _generation(day), payload=16)
    snapshot = _snapshot(tmp_path, rows)
    with pytest.raises(SystemExit):  # applying from a snapshot needs --rehearsal
        main(["--registry-snapshot", str(snapshot), "--journal-dir", str(root), "--apply", "archive"])
    target = str(_generation(SESSIONS[0]))
    assert main(["--registry-snapshot", str(snapshot), "--journal-dir", str(root), "--hot-sessions", "3",
                 "--today", "2026-11-15", "--rehearsal", "--codec", "xz", "--generation", target,
                 "--apply", "archive", "--apply", "delete"]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["line"] for line in lines if line["line"] in {"archive", "delete"}] == ["archive", "delete"]
    assert not (root / "options-acquisition" / target).exists()
    assert (root / "options-archive" / f"{target}.tar.xz").is_file()
    # the other cold journals were not touched (the --generation filter)
    assert (root / "options-acquisition" / str(_generation(SESSIONS[1])) / "options.sqlite").is_file()
    assert manifest_path(root / "options-archive", UUID(target)).is_file()
