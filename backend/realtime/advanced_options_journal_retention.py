"""Options journal retention (R54 part 3, G106 H3, R81; Nov 15).

Options journals (`<journal root>/options-acquisition/<generation>/options.sqlite{,-wal,-shm}`, ~4-12 GB a session at
V4) were never cleaned up. This module plans and applies the retention policy:

- the newest N sessions with an Options journal stay HOT (default 10);
- the current XNYS session and the previous one are PROTECTED whatever N is (on a weekend "current" is the next
  session), so a running or just-closed session is never touched;
- for every older (COLD) journal: (1) FINALIZE the registration with the R54 rule (`advanced_options_journal_finalize`,
  only when its own plan says SEAL_AND_DRAIN or DRAIN), (2) ARCHIVE the member files into one tar archive (zstd when
  the `zstd` CLI exists, else xz) with a manifest (generation, session, registry status, members, sizes, sha256,
  codec), verified right after writing by decompressing and re-hashing every member, (3) DELETE the originals only
  when the archive is verified, the registration is DRAINED and the originals still hash to the manifest;
- QUARANTINED registrations, missing files and directories without a registration (ORPHAN) are reported, never
  touched.

Planning is pure and read-only. Applying needs one explicit flag per step (the CLI's `--apply`); the first live apply is
an operator action that needs the user's yes. No schema change: the archive manifests live next to the archives.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tarfile
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, Mapping
from uuid import UUID

from .advanced_options_journal_finalize import FinalizePlan, OptionsJournalRegistration

UTC = timezone.utc
OPTIONS_DIRECTORY = "options-acquisition"
DEFAULT_ARCHIVE_DIRECTORY = "options-archive"
JOURNAL_FILE = "options.sqlite"
MEMBER_SUFFIXES = ("", "-wal", "-shm")
DEFAULT_HOT_SESSIONS = 10
PLAN_SCHEMA = "options-journal-retention-plan-v1"
MANIFEST_SCHEMA = "options-journal-archive-v1"
FINALIZE_ACTIONS = frozenset({"SEAL_AND_DRAIN", "DRAIN"})
_CHUNK = 4 * 1024 * 1024


class Tier(str, Enum):
    PROTECTED = "PROTECTED"
    HOT = "HOT"
    COLD = "COLD"
    ORPHAN = "ORPHAN"


class Step(str, Enum):
    FINALIZE = "FINALIZE"
    ARCHIVE = "ARCHIVE"
    DELETE = "DELETE"


class RetentionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MemberFile:
    name: str
    size: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class ArchiveMember:
    name: str
    size: int
    sha256: str
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    journal_generation: UUID
    session_date: date | None
    registration_status: str | None
    relative_dir: str
    members: tuple[ArchiveMember, ...]
    codec: str
    archive_name: str
    archive_bytes: int
    archive_sha256: str
    created_at: datetime
    verified_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def verified(self) -> bool:
        return self.verified_at is not None

    def document(self) -> dict[str, object]:
        return {
            "schema": MANIFEST_SCHEMA,
            "journal_generation": str(self.journal_generation),
            "session_date": None if self.session_date is None else self.session_date.isoformat(),
            "registration_status": self.registration_status,
            "relative_dir": self.relative_dir,
            "members": [
                {"name": m.name, "size": m.size, "sha256": m.sha256, "mtime_ns": m.mtime_ns}
                for m in self.members
            ],
            "codec": self.codec,
            "archive_name": self.archive_name,
            "archive_bytes": self.archive_bytes,
            "archive_sha256": self.archive_sha256,
            "created_at": self.created_at.isoformat(),
            "verified": self.verified,
            "verified_at": None if self.verified_at is None else self.verified_at.isoformat(),
            "deleted_at": None if self.deleted_at is None else self.deleted_at.isoformat(),
        }


def manifest_from_document(value: Mapping[str, object]) -> ArchiveManifest:
    if value.get("schema") != MANIFEST_SCHEMA:
        raise RetentionError("Options journal archive manifest schema is unknown")

    def stamp(name):
        raw = value.get(name)
        return None if raw in (None, "") else datetime.fromisoformat(str(raw))

    return ArchiveManifest(
        journal_generation=UUID(str(value["journal_generation"])),
        session_date=None if value.get("session_date") is None else date.fromisoformat(str(value["session_date"])),
        registration_status=None if value.get("registration_status") is None else str(value["registration_status"]),
        relative_dir=str(value["relative_dir"]),
        members=tuple(
            ArchiveMember(str(m["name"]), int(m["size"]), str(m["sha256"]), int(m.get("mtime_ns") or 0))
            for m in value["members"]
        ),
        codec=str(value["codec"]),
        archive_name=str(value["archive_name"]),
        archive_bytes=int(value["archive_bytes"]),
        archive_sha256=str(value["archive_sha256"]),
        created_at=datetime.fromisoformat(str(value["created_at"])),
        verified_at=stamp("verified_at"),
        deleted_at=stamp("deleted_at"),
    )


@dataclass(frozen=True, slots=True)
class RetentionEntry:
    journal_generation: UUID
    session_date: date | None
    status: str | None
    tier: Tier
    members: tuple[MemberFile, ...]
    archive: ArchiveManifest | None
    finalize: FinalizePlan | None
    steps: tuple[Step, ...]
    reasons: tuple[str, ...]

    @property
    def original_bytes(self) -> int:
        return sum(member.size for member in self.members)

    def summary(self) -> dict[str, object]:
        return {
            "journal_generation": str(self.journal_generation),
            "session_date": None if self.session_date is None else self.session_date.isoformat(),
            "status": self.status,
            "tier": self.tier.value,
            "members": {member.name: member.size for member in self.members},
            "original_bytes": self.original_bytes,
            "archive": None if self.archive is None else {
                "name": self.archive.archive_name, "codec": self.archive.codec,
                "bytes": self.archive.archive_bytes, "verified": self.archive.verified,
                "originals_deleted": self.archive.deleted_at is not None,
            },
            "finalize": None if self.finalize is None else {
                "action": self.finalize.action, "reasons": list(self.finalize.reasons),
            },
            "steps": [step.value for step in self.steps],
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    today: date
    current_session: date
    previous_session: date
    hot_sessions: int
    hot_dates: tuple[date, ...]
    entries: tuple[RetentionEntry, ...]

    def entry(self, generation: UUID) -> RetentionEntry:
        for item in self.entries:
            if item.journal_generation == generation:
                return item
        raise KeyError(str(generation))

    def summary(self) -> dict[str, object]:
        def by(step):
            return [str(e.journal_generation) for e in self.entries if step in e.steps]

        return {
            "schema": PLAN_SCHEMA,
            "today": self.today.isoformat(),
            "current_session": self.current_session.isoformat(),
            "previous_session": self.previous_session.isoformat(),
            "protected_from": self.previous_session.isoformat(),
            "hot_sessions": self.hot_sessions,
            "hot_dates": [value.isoformat() for value in self.hot_dates],
            "tiers": {tier.value: sum(e.tier is tier for e in self.entries) for tier in Tier},
            "finalize": by(Step.FINALIZE),
            "archive": by(Step.ARCHIVE),
            "delete": by(Step.DELETE),
            "cold_sessions": sorted({e.session_date.isoformat() for e in self.entries
                                     if e.tier is Tier.COLD and e.session_date is not None}),
            "bytes_to_free": sum(e.original_bytes for e in self.entries if Step.DELETE in e.steps),
        }


# ---------------------------------------------------------------------------------------------------------- planning


def protected_sessions(today: date, calendar=None) -> tuple[date, date]:
    """(current, previous) XNYS sessions: current = today's session or the next one; previous = the one before."""
    if calendar is None:
        from options.calendar import OptionExchangeCalendar

        calendar = OptionExchangeCalendar()
    current = calendar.session_on_or_after(today)
    return current, calendar.previous_session(current)


def options_journal_directory(root: Path, generation: UUID) -> Path:
    return Path(root) / OPTIONS_DIRECTORY / str(generation)


def scan_options_journals(root: Path) -> dict[UUID, tuple[MemberFile, ...]]:
    """Generation directories under the journal root and their member files (read-only)."""
    base = Path(root) / OPTIONS_DIRECTORY
    found: dict[UUID, tuple[MemberFile, ...]] = {}
    if not base.is_dir():
        return found
    for directory in sorted(base.iterdir()):
        if not directory.is_dir():
            continue
        try:
            generation = UUID(directory.name)
        except ValueError:
            continue
        members = []
        for suffix in MEMBER_SUFFIXES:
            candidate = directory / (JOURNAL_FILE + suffix)
            if candidate.is_file():
                stat = candidate.stat()
                members.append(MemberFile(candidate.name, int(stat.st_size), int(stat.st_mtime_ns)))
        found[generation] = tuple(members)
    return found


def manifest_path(archive_root: Path, generation: UUID) -> Path:
    return Path(archive_root) / f"{generation}.manifest.json"


def load_archive_manifests(archive_root: Path) -> dict[UUID, ArchiveManifest]:
    root = Path(archive_root)
    manifests: dict[UUID, ArchiveManifest] = {}
    if not root.is_dir():
        return manifests
    for path in sorted(root.glob("*.manifest.json")):
        manifest = manifest_from_document(json.loads(path.read_text(encoding="utf-8")))
        manifests[manifest.journal_generation] = manifest
    return manifests


def _archive_covers(manifest: ArchiveManifest | None, members: tuple[MemberFile, ...]) -> bool:
    """A verified archive whose members match the originals on disk by name and size."""
    if manifest is None or not manifest.verified:
        return False
    stored = {member.name: member.size for member in manifest.members}
    return stored == {member.name: member.size for member in members}


def preliminary_tiers(
    registrations: Iterable[OptionsJournalRegistration],
    *,
    today: date,
    hot_sessions: int,
    calendar=None,
) -> tuple[date, date, tuple[date, ...], dict[UUID, Tier]]:
    """Tier per registered generation before finalize planning (so only COLD journals get hashed)."""
    if type(hot_sessions) is not int or hot_sessions < 1:
        raise ValueError("hot session count must be a positive integer")
    rows = tuple(registrations)
    current, previous = protected_sessions(today, calendar)
    dates = sorted({row.session_date for row in rows}, reverse=True)
    hot = tuple(dates[:hot_sessions])
    tiers = {}
    for row in rows:
        if row.session_date >= previous:
            tiers[row.journal_generation] = Tier.PROTECTED
        elif row.session_date in hot:
            tiers[row.journal_generation] = Tier.HOT
        else:
            tiers[row.journal_generation] = Tier.COLD
    return current, previous, hot, tiers


def plan_options_journal_retention(
    registrations: Iterable[OptionsJournalRegistration],
    disk: Mapping[UUID, tuple[MemberFile, ...]],
    archives: Mapping[UUID, ArchiveManifest],
    *,
    today: date,
    hot_sessions: int = DEFAULT_HOT_SESSIONS,
    finalize_plans: Mapping[UUID, FinalizePlan] | None = None,
    calendar=None,
) -> RetentionPlan:
    """Pure: what retention would do, from the registry, the files on disk and the archive manifests."""
    rows = tuple(registrations)
    if len({row.journal_generation for row in rows}) != len(rows):
        raise RetentionError("Options journal registrations repeat a generation")
    current, previous, hot, tiers = preliminary_tiers(
        rows, today=today, hot_sessions=hot_sessions, calendar=calendar,
    )
    finalize_plans = finalize_plans or {}
    entries = []
    for row in sorted(rows, key=lambda item: (item.session_date, str(item.journal_generation))):
        generation = row.journal_generation
        members = tuple(disk.get(generation, ()))
        archive = archives.get(generation)
        tier = tiers[generation]
        steps: list[Step] = []
        reasons: list[str] = []
        finalize = finalize_plans.get(generation)
        expected_dir = f"{OPTIONS_DIRECTORY}/{generation}/{JOURNAL_FILE}"
        if tier is Tier.PROTECTED:
            reasons.append("PROTECTED_CURRENT_OR_PREVIOUS_SESSION")
        elif tier is Tier.HOT:
            reasons.append("HOT_WINDOW")
        elif row.status == "QUARANTINED":
            reasons.append("STATUS_QUARANTINED_KEPT")
        elif row.status == "REGISTERED":
            reasons.append("STATUS_REGISTERED_KEPT")
        elif row.relative_path.replace("\\", "/") != expected_dir:
            reasons.append("UNEXPECTED_RELATIVE_PATH")
        else:
            drained = row.status == "DRAINED"
            will_drain = False
            if not drained:
                if finalize is None:
                    reasons.append("FINALIZE_NOT_PLANNED")
                elif finalize.action in FINALIZE_ACTIONS:
                    steps.append(Step.FINALIZE)
                    will_drain = True
                else:
                    reasons.append("FINALIZE_" + finalize.action + ":" + ",".join(finalize.reasons))
            if not members:
                if archive is not None and archive.deleted_at is not None:
                    reasons.append("ARCHIVED_ORIGINALS_DELETED")
                else:
                    reasons.append("JOURNAL_FILES_MISSING")
            elif not any(member.name == JOURNAL_FILE for member in members):
                reasons.append("JOURNAL_MAIN_FILE_MISSING")
            else:
                if not _archive_covers(archive, members):
                    steps.append(Step.ARCHIVE)
                if drained or will_drain:
                    steps.append(Step.DELETE)
                else:
                    reasons.append("DELETE_WAITS_FOR_DRAINED")
        entries.append(RetentionEntry(
            journal_generation=generation, session_date=row.session_date, status=row.status, tier=tier,
            members=members, archive=archive, finalize=finalize, steps=tuple(steps), reasons=tuple(reasons),
        ))
    registered = {row.journal_generation for row in rows}
    for generation in sorted(set(disk) - registered, key=str):
        entries.append(RetentionEntry(
            journal_generation=generation, session_date=None, status=None, tier=Tier.ORPHAN,
            members=tuple(disk[generation]), archive=archives.get(generation), finalize=None, steps=(),
            reasons=("NO_REGISTRATION_KEPT",),
        ))
    return RetentionPlan(
        today=today, current_session=current, previous_session=previous, hot_sessions=hot_sessions,
        hot_dates=hot, entries=tuple(entries),
    )


# ------------------------------------------------------------------------------------------------------- archiving


def select_codec(preferred: str = "auto", *, which: Callable[[str], str | None] = shutil.which) -> str:
    choice = str(preferred).strip().lower()
    if choice not in {"auto", "zstd", "xz"}:
        raise ValueError("archive codec must be auto, zstd or xz")
    if choice == "auto":
        return "zstd" if which("zstd") else "xz"
    if choice == "zstd" and not which("zstd"):
        raise RetentionError("zstd archive codec requested but the zstd CLI is unavailable")
    return choice


def archive_name(generation: UUID, codec: str) -> str:
    return f"{generation}.tar.{'zst' if codec == 'zstd' else 'xz'}"


class _HashingReader:
    """What `TarFile.addfile` reads from (`read(n)` only): hashes and counts the bytes on the way."""

    def __init__(self, handle) -> None:
        self._handle = handle
        self.digest = hashlib.sha256()
        self.count = 0

    def read(self, size=-1):
        chunk = self._handle.read(size)
        self.digest.update(chunk)
        self.count += len(chunk)
        return chunk


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _stat_member(path: Path) -> MemberFile:
    stat = path.stat()
    return MemberFile(path.name, int(stat.st_size), int(stat.st_mtime_ns))


def _write_manifest(path: Path, manifest: ArchiveManifest) -> None:
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(manifest.document(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _fsync(path: Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def archive_options_journal(
    journal_root: Path,
    archive_root: Path,
    entry: RetentionEntry,
    *,
    codec: str,
    xz_preset: int = 1,
    zstd_level: int = 3,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ArchiveManifest:
    """Write `<generation>.tar.<zst|xz>` + its manifest (unverified), hashing every member on the way."""
    if Step.ARCHIVE not in entry.steps:
        raise RetentionError("archive step is not planned for this journal")
    if codec not in {"zstd", "xz"}:
        raise ValueError("archive codec must be zstd or xz")
    directory = options_journal_directory(journal_root, entry.journal_generation)
    archive_root = Path(archive_root)
    archive_root.mkdir(parents=True, exist_ok=True)
    final = archive_root / archive_name(entry.journal_generation, codec)
    partial = final.with_name(final.name + ".partial")
    before = tuple(_stat_member(directory / member.name) for member in entry.members)
    hashed: list[ArchiveMember] = []

    def add_members(tar: tarfile.TarFile) -> None:
        for member in before:
            path = directory / member.name
            info = tarfile.TarInfo(f"{OPTIONS_DIRECTORY}/{entry.journal_generation}/{member.name}")
            info.size = member.size
            info.mtime = member.mtime_ns // 1_000_000_000
            info.mode = 0o640
            with path.open("rb") as handle:
                reader = _HashingReader(handle)
                tar.addfile(info, reader)
            if reader.count != member.size:
                raise RetentionError(f"journal member changed size while archiving: {member.name}")
            hashed.append(ArchiveMember(member.name, member.size, reader.digest.hexdigest(), member.mtime_ns))

    try:
        if codec == "zstd":
            with partial.open("wb") as sink:
                process = subprocess.Popen(
                    ["zstd", "-q", "-T0", f"-{int(zstd_level)}", "-c"], stdin=subprocess.PIPE, stdout=sink,
                )
                try:
                    with tarfile.open(fileobj=process.stdin, mode="w|", format=tarfile.PAX_FORMAT) as tar:
                        add_members(tar)
                finally:
                    process.stdin.close()
                    code = process.wait()
                if code != 0:
                    raise RetentionError(f"zstd exited {code}")
        else:
            with tarfile.open(partial, mode="w:xz", preset=int(xz_preset), format=tarfile.PAX_FORMAT) as tar:
                add_members(tar)
        _fsync(partial)
        after = tuple(_stat_member(directory / member.name) for member in entry.members)
        if after != before:
            raise RetentionError("journal files changed while archiving")
        os.replace(partial, final)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    manifest = ArchiveManifest(
        journal_generation=entry.journal_generation,
        session_date=entry.session_date,
        registration_status=entry.status,
        relative_dir=f"{OPTIONS_DIRECTORY}/{entry.journal_generation}",
        members=tuple(hashed),
        codec=codec,
        archive_name=final.name,
        archive_bytes=int(final.stat().st_size),
        archive_sha256=_file_sha256(final),
        created_at=now(),
    )
    _write_manifest(manifest_path(archive_root, entry.journal_generation), manifest)
    return manifest


def _open_archive_stream(path: Path, codec: str):
    if codec == "zstd":
        process = subprocess.Popen(["zstd", "-q", "-d", "-c", str(path)], stdout=subprocess.PIPE)
        return tarfile.open(fileobj=process.stdout, mode="r|"), process
    return tarfile.open(path, mode="r|xz"), None


def verify_options_journal_archive(
    archive_root: Path,
    manifest: ArchiveManifest,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ArchiveManifest:
    """Decompress the archive and re-hash every member against the manifest; mark the manifest verified."""
    archive = Path(archive_root) / manifest.archive_name
    if not archive.is_file():
        raise RetentionError("Options journal archive is missing")
    if _file_sha256(archive) != manifest.archive_sha256 or archive.stat().st_size != manifest.archive_bytes:
        raise RetentionError("Options journal archive bytes differ from the manifest")
    expected = {member.name: member for member in manifest.members}
    seen: dict[str, tuple[int, str]] = {}
    tar, process = _open_archive_stream(archive, manifest.codec)
    try:
        for info in tar:
            prefix = manifest.relative_dir + "/"
            if not info.isfile() or not info.name.startswith(prefix):
                raise RetentionError(f"unexpected archive member {info.name}")
            name = info.name[len(prefix):]
            handle = tar.extractfile(info)
            digest = hashlib.sha256()
            size = 0
            while chunk := handle.read(_CHUNK):
                digest.update(chunk)
                size += len(chunk)
            seen[name] = (size, digest.hexdigest())
    finally:
        tar.close()
        if process is not None:
            process.stdout.close()
            if process.wait() != 0:
                raise RetentionError("zstd could not decompress the archive")
    if set(seen) != set(expected):
        raise RetentionError("archive members differ from the manifest")
    for name, (size, sha256) in seen.items():
        if size != expected[name].size or sha256 != expected[name].sha256:
            raise RetentionError(f"archive member {name} differs from the manifest")
    verified = replace(manifest, verified_at=now())
    _write_manifest(manifest_path(archive_root, manifest.journal_generation), verified)
    return verified


def delete_options_journal_originals(
    journal_root: Path,
    archive_root: Path,
    entry: RetentionEntry,
    manifest: ArchiveManifest,
    *,
    registry_status: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ArchiveManifest:
    """Delete the original member files: only verified, only DRAINED, only if they still hash to the manifest."""
    if Step.DELETE not in entry.steps:
        raise RetentionError("delete step is not planned for this journal")
    if manifest.journal_generation != entry.journal_generation:
        raise RetentionError("manifest belongs to another journal")
    if not manifest.verified:
        raise RetentionError("refusing to delete: the archive is not verified")
    if registry_status != "DRAINED":
        raise RetentionError(f"refusing to delete: registration is {registry_status}, not DRAINED")
    directory = options_journal_directory(journal_root, entry.journal_generation)
    present = {path.name for path in directory.iterdir()} if directory.is_dir() else set()
    expected = {member.name: member for member in manifest.members}
    if present != set(expected):
        raise RetentionError("journal directory differs from the archived members")
    for name, member in expected.items():
        path = directory / name
        if path.stat().st_size != member.size or _file_sha256(path) != member.sha256:
            raise RetentionError(f"refusing to delete: {name} changed since it was archived")
    for name in expected:
        (directory / name).unlink()
    directory.rmdir()
    deleted = replace(manifest, deleted_at=now())
    _write_manifest(manifest_path(archive_root, manifest.journal_generation), deleted)
    return deleted


def restore_options_journal(journal_root: Path, archive_root: Path, manifest: ArchiveManifest) -> Path:
    """Put an archived journal back at its registered place (refuses to overwrite), then re-hash it."""
    if not manifest.verified:
        raise RetentionError("refusing to restore an unverified archive")
    directory = Path(journal_root) / manifest.relative_dir
    if directory.exists() and any(directory.iterdir()):
        raise RetentionError("refusing to restore over existing journal files")
    directory.mkdir(parents=True, exist_ok=True)
    archive = Path(archive_root) / manifest.archive_name
    tar, process = _open_archive_stream(archive, manifest.codec)
    try:
        for info in tar:
            prefix = manifest.relative_dir + "/"
            if not info.isfile() or not info.name.startswith(prefix) or "/" in info.name[len(prefix):]:
                raise RetentionError(f"unexpected archive member {info.name}")
            target = directory / info.name[len(prefix):]
            with target.open("xb") as sink:
                shutil.copyfileobj(tar.extractfile(info), sink, _CHUNK)
    finally:
        tar.close()
        if process is not None:
            process.stdout.close()
            process.wait()
    for member in manifest.members:
        if _file_sha256(directory / member.name) != member.sha256:
            raise RetentionError(f"restored {member.name} differs from the manifest")
    return directory
