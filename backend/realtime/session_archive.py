"""Session archive files and the partition drop decision (docs/REALTIME_SCHEMA_V2_DESIGN.md).

Pure logic: row serialization, deterministic gzip JSONL bytes, the checksum manifest,
append-only file writes, verification, and the DROP or KEEP decision for an ARCHIVE_FIRST
partition. The only I/O is the local archive directory; there is no database or network
access here. The database-backed worker lives in session_archive_worker.py.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import zlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from uuid import UUID, uuid4


ARCHIVE_SCHEMA_VERSION = "realtime-session-archive-jsonl-gzip-v1"

STOCK_ALERTS = "STOCK_ALERTS"
OPTION_ALERTS = "OPTION_ALERTS"
DETECTOR_EVALUATIONS = "DETECTOR_EVALUATIONS"
OPTION_FLOW_EVENTS = "OPTION_FLOW_EVENTS"
OPTION_FLOW_CLUSTERS = "OPTION_FLOW_CLUSTERS"
OUTCOMES = "OUTCOMES"
ALERT_KINDS = (STOCK_ALERTS, OPTION_ALERTS, DETECTOR_EVALUATIONS)
FLOW_KINDS = (OPTION_FLOW_EVENTS, OPTION_FLOW_CLUSTERS)
SESSION_KINDS = (*ALERT_KINDS, *FLOW_KINDS)
ALL_KINDS = (*SESSION_KINDS, OUTCOMES)

# Relations stored in each kind of file; the order is the order of lines in the file.
_ALERT_RELATIONS = ("rt.alert_current", "rt.detector_events", "rt.detector_event_inputs")
KIND_RELATIONS: Mapping[str, tuple[str, ...]] = {
    STOCK_ALERTS: _ALERT_RELATIONS,
    OPTION_ALERTS: _ALERT_RELATIONS,
    DETECTOR_EVALUATIONS: (
        "rt.detector_datasets", "rt.detector_evaluations", "rt.detector_events",
        "rt.detector_event_inputs",
    ),
    OPTION_FLOW_EVENTS: ("rt.option_flow_events",),
    OPTION_FLOW_CLUSTERS: ("rt.option_flow_clusters",),
    OUTCOMES: ("rt.alert_management_assessments",),
}

CLOCKS = ("market", "observed", "durable", "replay")
# Stored columns that carry each clock. No v2 column stores the replay clock, so it stays
# empty in every manifest until the schema adds one.
CLOCK_COLUMNS: Mapping[str, tuple[tuple[str, str], ...]] = {
    "market": (
        ("rt.detector_events", "trigger_at"),
        ("rt.detector_event_inputs", "available_at"),
        ("rt.option_flow_events", "event_time"),
        ("rt.option_flow_events", "quote_time"),
        ("rt.option_flow_clusters", "first_event_time"),
        ("rt.option_flow_clusters", "last_event_time"),
    ),
    "observed": (
        ("rt.alert_current", "as_of"),
        ("rt.detector_events", "decision_at"),
        ("rt.detector_event_inputs", "received_at"),
        ("rt.detector_evaluations", "observed_at"),
        ("rt.alert_management_assessments", "assessed_at"),
    ),
    "durable": (
        ("rt.alert_current", "updated_at"),
        ("rt.detector_events", "created_at"),
        ("rt.option_flow_events", "created_at"),
        ("rt.option_flow_clusters", "created_at"),
    ),
    "replay": (),
}

PIN_OPEN_HORIZON = "OPEN_HORIZON"
PIN_OUTCOME_UNVERIFIED = "OUTCOME_UNVERIFIED"
PIN_ARCHIVE_REFRESH_FAILED = "ARCHIVE_REFRESH_FAILED"
PIN_LOOKUP_FAILED = "PIN_LOOKUP_FAILED"
PIN_ROWS_OUTSIDE_SESSIONS = "ROWS_OUTSIDE_SESSIONS"
PIN_COVERAGE_UNKNOWN = "COVERAGE_UNKNOWN"

DROP = "DROP"
KEEP = "KEEP"
REASON_ARCHIVES_VERIFIED = "ARCHIVES_VERIFIED_NO_PINS"
REASON_WITHIN_ONLINE_WINDOW = "WITHIN_ONLINE_WINDOW"
REASON_NO_SESSIONS = "NO_SESSIONS_IN_PARTITION"
REASON_ARCHIVE_MISSING = "ARCHIVE_MISSING"
REASON_ARCHIVE_UNVERIFIED = "ARCHIVE_UNVERIFIED"
REASON_OPEN_PIN = "OPEN_PIN"

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_SHA256_CHARACTERS = frozenset("0123456789abcdef")


class VerificationCode(str, Enum):
    MANIFEST_MISSING = "MANIFEST_MISSING"
    MANIFEST_INVALID = "MANIFEST_INVALID"
    SCHEMA_UNSUPPORTED = "SCHEMA_UNSUPPORTED"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    FILE_MISSING = "FILE_MISSING"
    HASH_MISMATCH = "HASH_MISMATCH"
    UNREADABLE_CONTENT = "UNREADABLE_CONTENT"
    CONTENT_HASH_MISMATCH = "CONTENT_HASH_MISMATCH"
    LINE_INVALID = "LINE_INVALID"
    ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"
    RELATION_COUNT_MISMATCH = "RELATION_COUNT_MISMATCH"
    CLOCKS_MISMATCH = "CLOCKS_MISMATCH"
    SOURCE_COUNT_MISMATCH = "SOURCE_COUNT_MISMATCH"


class ArchiveVerificationError(ValueError):
    def __init__(self, code: VerificationCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


def _sha256_hex(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in _SHA256_CHARACTERS for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class ArchiveIdentity:
    """Build and policy identity written into every manifest and ledger row."""

    build_id: str
    policy_id: str

    def __post_init__(self) -> None:
        build_id, policy_id = self.build_id.strip(), self.policy_id.strip()
        if not build_id or len(build_id) > 96:
            raise ValueError("archive build_id must be 1 to 96 characters")
        if not policy_id:
            raise ValueError("archive policy_id cannot be blank")
        object.__setattr__(self, "build_id", build_id)
        object.__setattr__(self, "policy_id", policy_id)


@dataclass(frozen=True, slots=True)
class ArchiveRow:
    """One stored row. `key` is its primary key (strings) and fixes the file order."""

    relation: str
    key: tuple[str, ...]
    document: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.relation or not self.key:
            raise ValueError("archive row requires a relation and a key")
        object.__setattr__(self, "key", tuple(str(part) for part in self.key))


@dataclass(frozen=True, slots=True)
class BuiltArchive:
    kind: str
    session_date: date
    horizon_close_date: date | None
    payload: bytes
    sha256: str
    content_sha256: str
    uncompressed_bytes: int
    row_count: int
    relation_counts: Mapping[str, int]
    clocks: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class WrittenArchive:
    data_path: Path
    manifest_path: Path
    created: bool
    revision: bool


@dataclass(frozen=True, slots=True)
class VerifiedArchive:
    kind: str
    session_date: date
    horizon_close_date: date | None
    uri: str
    sha256: str
    content_sha256: str
    row_count: int
    relation_counts: Mapping[str, int]
    schema_version: str
    build_id: str
    policy_id: str


@dataclass(frozen=True, slots=True)
class ArchiveRecord:
    """A row of rt.session_archives. `verified_at` is None for a file not yet verified."""

    session_date: date
    kind: str
    uri: str
    sha256: str
    row_count: int
    schema_version: str
    build_id: str
    verified_at: datetime | None
    horizon_close_date: date | None = None


@dataclass(frozen=True, slots=True)
class Pin:
    """Why data of a session must stay online even when its archives are verified."""

    session_date: date
    reason: str
    ref: str = ""


@dataclass(frozen=True, slots=True)
class PartitionRange:
    """Half-open range [lower, upper) of session dates held by one partition."""

    relation: str
    lower: date
    upper: date

    def __post_init__(self) -> None:
        if self.upper <= self.lower:
            raise ValueError("partition range must be nonempty")

    def contains(self, value: date) -> bool:
        return self.lower <= value < self.upper


@dataclass(frozen=True, slots=True)
class DropReason:
    code: str
    session_date: date | None = None
    kind: str | None = None
    detail: str | None = None

    def sort_key(self) -> tuple[str, str, str, str]:
        return (
            self.code,
            self.session_date.isoformat() if self.session_date else "",
            self.kind or "",
            self.detail or "",
        )

    def as_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "session_date": self.session_date.isoformat() if self.session_date else None,
            "kind": self.kind,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class DropDecision:
    action: str
    reasons: tuple[DropReason, ...]

    @property
    def drop(self) -> bool:
        return self.action == DROP


def to_jsonable(value: object) -> object:
    """Convert a stored value to JSON with one fixed text form per type (stable bytes)."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("archive values cannot be NaN or infinite")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("archive Decimal must be finite")
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("archive timestamps must be timezone-aware")
        return value.astimezone(timezone.utc).strftime(_TIMESTAMP_FORMAT)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("archive mapping keys must be strings")
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    raise TypeError(f"archive value of type {type(value).__name__} is not serializable")


def _dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _check_kind(kind: str, horizon_close_date: date | None) -> None:
    if kind not in KIND_RELATIONS:
        raise ValueError(f"unsupported archive kind {kind}")
    if (kind == OUTCOMES) != (horizon_close_date is not None):
        raise ValueError("only OUTCOMES archives carry a horizon close date, and they require one")


def _clock_columns() -> dict[str, list[tuple[str, str]]]:
    by_relation: dict[str, list[tuple[str, str]]] = {}
    for clock, columns in CLOCK_COLUMNS.items():
        for relation, column in columns:
            by_relation.setdefault(relation, []).append((clock, column))
    return by_relation


_CLOCKS_BY_RELATION = _clock_columns()


def summarize_clocks(entries: Iterable[tuple[str, Mapping[str, object]]]) -> dict[str, object]:
    """Min and max of each clock as stored; every timestamp is a fixed-width UTC string."""
    spans: dict[str, dict[str, object]] = {}
    for relation, document in entries:
        for clock, column in _CLOCKS_BY_RELATION.get(relation, ()):
            value = document.get(column)
            if not isinstance(value, str):
                continue
            span = spans.setdefault(clock, {"min": value, "max": value, "columns": set()})
            span["min"] = min(str(span["min"]), value)
            span["max"] = max(str(span["max"]), value)
            span["columns"].add(f"{relation}.{column}")
    return {
        clock: (
            {"columns": sorted(spans[clock]["columns"]), "max": spans[clock]["max"],
             "min": spans[clock]["min"]}
            if clock in spans else None
        )
        for clock in CLOCKS
    }


def build_archive(
    kind: str,
    session_date: date,
    rows: Iterable[ArchiveRow],
    *,
    horizon_close_date: date | None = None,
) -> BuiltArchive:
    """Deterministic gzip JSONL: the same rows in any order give the same bytes."""
    _check_kind(kind, horizon_close_date)
    rank = {relation: index for index, relation in enumerate(KIND_RELATIONS[kind])}
    prepared: list[tuple[int, tuple[str, ...], str, dict[str, object]]] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    for row in rows:
        if row.relation not in rank:
            raise ValueError(f"{row.relation} is not part of {kind} archives")
        identity = (row.relation, row.key)
        if identity in seen:
            raise ValueError(f"duplicate archive row {row.relation} {row.key}")
        seen.add(identity)
        document = to_jsonable(row.document)
        if not isinstance(document, dict):
            raise TypeError("archive row document must be a mapping")
        prepared.append((rank[row.relation], row.key, row.relation, document))
    prepared.sort(key=lambda item: (item[0], item[1]))
    counts = {relation: 0 for relation in KIND_RELATIONS[kind]}
    lines: list[bytes] = []
    for _, _, relation, document in prepared:
        counts[relation] += 1
        lines.append(_dumps({"relation": relation, "row": document}).encode("ascii"))
    content = b"".join(line + b"\n" for line in lines)
    payload = gzip.compress(content, compresslevel=9, mtime=0)
    return BuiltArchive(
        kind=kind,
        session_date=session_date,
        horizon_close_date=horizon_close_date,
        payload=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        content_sha256=hashlib.sha256(content).hexdigest(),
        uncompressed_bytes=len(content),
        row_count=len(lines),
        relation_counts=counts,
        clocks=summarize_clocks((relation, document) for _, _, relation, document in prepared),
    )


def archive_file_names(
    kind: str,
    horizon_close_date: date | None = None,
    content_sha256: str | None = None,
) -> tuple[str, str]:
    """(data file, manifest file). A content-addressed suffix names a later revision."""
    _check_kind(kind, horizon_close_date)
    stem = f"outcomes-{horizon_close_date.isoformat()}" if kind == OUTCOMES else kind.lower()
    if content_sha256 is not None:
        stem += "." + _sha256_hex(content_sha256, "content_sha256")[:16]
    return stem + ".jsonl.gz", stem + ".manifest.json"


def archive_directory(root: Path, session_date: date) -> Path:
    return Path(root) / session_date.isoformat()


def render_manifest(built: BuiltArchive, *, file_name: str, identity: ArchiveIdentity) -> bytes:
    """Manifest bytes. No wall-clock value is included, so they are reproducible."""
    document = {
        "archive_kind": built.kind,
        "build_id": identity.build_id,
        "clocks": built.clocks,
        "content_sha256": built.content_sha256,
        "file": file_name,
        "horizon_close_date": built.horizon_close_date.isoformat() if built.horizon_close_date else None,
        "policy_id": identity.policy_id,
        "relation_counts": dict(built.relation_counts),
        "row_count": built.row_count,
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "session_date": built.session_date.isoformat(),
        "sha256": built.sha256,
        "uncompressed_bytes": built.uncompressed_bytes,
    }
    return (json.dumps(document, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")


def _publish(temporary: Path, final: Path) -> None:
    """Move a finished temp file into place without ever replacing an existing file."""
    try:
        os.link(temporary, final)
    except FileExistsError:
        raise
    except OSError:
        if final.exists():
            raise FileExistsError(str(final)) from None
        os.replace(temporary, final)


def _write_new_file(path: Path, data: bytes) -> None:
    temporary = path.with_name(f"{path.name}.{uuid4().hex}.partial")
    try:
        with open(temporary, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        _publish(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _holds_content(path: Path, built: BuiltArchive) -> bool:
    data = path.read_bytes()
    if data == built.payload:
        return True
    try:
        return hashlib.sha256(gzip.decompress(data)).hexdigest() == built.content_sha256
    except (OSError, EOFError, zlib.error):
        return False


def write_archive(directory: Path, built: BuiltArchive, identity: ArchiveIdentity) -> WrittenArchive:
    """Write the data file then its manifest atomically; an existing file is never rewritten.

    The first file name is reused when it already holds the same content (idempotent
    re-run). When it holds different content, the new content goes to a content-addressed
    revision name and the original stays untouched.
    """
    directory.mkdir(parents=True, exist_ok=True)
    candidates = (
        archive_file_names(built.kind, built.horizon_close_date),
        archive_file_names(built.kind, built.horizon_close_date, built.content_sha256),
    )
    for index, (data_name, manifest_name) in enumerate(candidates):
        data_path = directory / data_name
        created = False
        if not data_path.exists():
            _write_new_file(data_path, built.payload)
            created = True
        elif not _holds_content(data_path, built):
            if index == 0:
                continue
            raise FileExistsError("content-addressed archive revision holds different content")
        manifest_path = directory / manifest_name
        if not manifest_path.exists():
            _write_new_file(manifest_path, render_manifest(built, file_name=data_name, identity=identity))
        return WrittenArchive(data_path, manifest_path, created, revision=index == 1)
    raise AssertionError("unreachable: the revision candidate always returns or raises")


def _fail(code: VerificationCode, detail: str) -> ArchiveVerificationError:
    return ArchiveVerificationError(code, detail)


def _load_manifest(manifest_path: Path) -> dict[str, object]:
    try:
        raw = manifest_path.read_bytes()
    except FileNotFoundError:
        raise _fail(VerificationCode.MANIFEST_MISSING, manifest_path.name) from None
    try:
        manifest = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _fail(VerificationCode.MANIFEST_INVALID, "not valid JSON") from None
    required = {
        "archive_kind": str, "build_id": str, "clocks": dict, "content_sha256": str, "file": str,
        "policy_id": str, "relation_counts": dict, "row_count": int, "schema_version": str,
        "session_date": str, "sha256": str, "uncompressed_bytes": int,
    }
    if not isinstance(manifest, dict) or any(
        not isinstance(manifest.get(name), kind) or isinstance(manifest.get(name), bool)
        for name, kind in required.items()
    ):
        raise _fail(VerificationCode.MANIFEST_INVALID, "missing or mistyped fields")
    return manifest


def verify_archive(
    data_path: Path,
    manifest_path: Path,
    *,
    kind: str,
    session_date: date,
    horizon_close_date: date | None = None,
    source_counts: Mapping[str, int] | None = None,
    identity: ArchiveIdentity | None = None,
) -> VerifiedArchive:
    """Re-read the files from disk and check them against the manifest and the source.

    Recomputes sha256, decompresses, counts rows per relation and compares with the manifest;
    with `source_counts` (a fresh COUNT of the source rows) it also compares those. When an
    identity is supplied, the manifest must match its build and policy.
    """
    _check_kind(kind, horizon_close_date)
    manifest = _load_manifest(manifest_path)
    if manifest["schema_version"] != ARCHIVE_SCHEMA_VERSION:
        raise _fail(VerificationCode.SCHEMA_UNSUPPORTED, str(manifest["schema_version"])[:64])
    expected_horizon = horizon_close_date.isoformat() if horizon_close_date else None
    if (
        manifest["archive_kind"] != kind
        or manifest["session_date"] != session_date.isoformat()
        or manifest.get("horizon_close_date") != expected_horizon
        or manifest["file"] != data_path.name
    ):
        raise _fail(VerificationCode.IDENTITY_MISMATCH, "manifest does not describe this archive file")
    if identity is not None and (
        manifest["build_id"] != identity.build_id
        or manifest["policy_id"] != identity.policy_id
    ):
        raise _fail(VerificationCode.IDENTITY_MISMATCH, "manifest build or policy identity differs from requested identity")
    try:
        payload = data_path.read_bytes()
    except FileNotFoundError:
        raise _fail(VerificationCode.FILE_MISSING, data_path.name) from None
    sha256 = _sha256_hex(str(manifest["sha256"]), "manifest sha256")
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise _fail(VerificationCode.HASH_MISMATCH, "file sha256 differs from its manifest")
    try:
        content = gzip.decompress(payload)
    except (OSError, EOFError, zlib.error):
        raise _fail(VerificationCode.UNREADABLE_CONTENT, "file is not a complete gzip stream") from None
    if hashlib.sha256(content).hexdigest() != manifest["content_sha256"] or len(content) != manifest["uncompressed_bytes"]:
        raise _fail(VerificationCode.CONTENT_HASH_MISMATCH, "decompressed content differs from its manifest")

    lines = content.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    allowed = KIND_RELATIONS[kind]
    counts = {relation: 0 for relation in allowed}
    entries: list[tuple[str, dict[str, object]]] = []
    for line in lines:
        try:
            parsed = json.loads(line.decode("ascii"))
            relation, document = parsed["relation"], parsed["row"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
            raise _fail(VerificationCode.LINE_INVALID, "line is not a relation row") from None
        if relation not in counts or not isinstance(document, dict):
            raise _fail(VerificationCode.LINE_INVALID, "line holds an unexpected relation")
        counts[relation] += 1
        entries.append((relation, document))
    if len(lines) != manifest["row_count"]:
        raise _fail(VerificationCode.ROW_COUNT_MISMATCH,
                    f"file holds {len(lines)} rows, manifest says {manifest['row_count']}")
    if counts != manifest["relation_counts"]:
        raise _fail(VerificationCode.RELATION_COUNT_MISMATCH, "per-relation counts differ from the manifest")
    if summarize_clocks(entries) != manifest["clocks"]:
        raise _fail(VerificationCode.CLOCKS_MISMATCH, "clock ranges differ from the manifest")
    if source_counts is not None and {relation: int(source_counts.get(relation, 0)) for relation in allowed} != counts:
        raise _fail(VerificationCode.SOURCE_COUNT_MISMATCH, "source row counts differ from the file")
    return VerifiedArchive(
        kind=kind,
        session_date=session_date,
        horizon_close_date=horizon_close_date,
        uri=data_path.resolve().as_uri(),
        sha256=sha256,
        content_sha256=str(manifest["content_sha256"]),
        row_count=len(lines),
        relation_counts=counts,
        schema_version=str(manifest["schema_version"]),
        build_id=str(manifest["build_id"]),
        policy_id=str(manifest["policy_id"]),
    )


def decide_partition_drop(
    partition: PartitionRange,
    sessions: Sequence[date],
    records: Sequence[ArchiveRecord],
    pins: Sequence[Pin],
    *,
    required_kinds: Sequence[str],
    window_start: date | None = None,
) -> DropDecision:
    """DROP only when every session of the partition has a verified archive of every
    kind its relation requires (rt.partition_policies.archive_kinds), no pin is open, and
    the partition is older than the online window.

    `sessions` are the exchange sessions inside the partition range. `window_start` is the
    oldest session still inside the online window; a partition that reaches it is kept.
    `required_kinds` has no default and cannot be empty: a relation without a required kind
    is not archive-gated and is not decided here. Any other input is KEEP: an empty session
    list fails closed. All reasons are returned in a stable order so a decision can be
    logged and compared.
    """
    if not required_kinds:
        raise ValueError("a partition drop decision needs at least one required archive kind")
    outside = [value for value in sessions if not partition.contains(value)]
    if outside:
        raise ValueError(f"sessions outside the partition range: {sorted(outside)}")
    reasons: list[DropReason] = []
    if window_start is not None and partition.upper > window_start:
        reasons.append(DropReason(REASON_WITHIN_ONLINE_WINDOW, detail=window_start.isoformat()))
    session_set = sorted(set(sessions))
    if not session_set:
        reasons.append(DropReason(REASON_NO_SESSIONS))
    for session in session_set:
        for kind in required_kinds:
            matching = [
                record for record in records
                if record.session_date == session and record.kind == kind
            ]
            if any(record.verified_at is not None for record in matching):
                continue
            code = REASON_ARCHIVE_UNVERIFIED if matching else REASON_ARCHIVE_MISSING
            reasons.append(DropReason(code, session, kind))
    for pin in pins:
        if partition.contains(pin.session_date):
            reasons.append(DropReason(
                REASON_OPEN_PIN, pin.session_date, detail=f"{pin.reason}:{pin.ref}" if pin.ref else pin.reason,
            ))
    if reasons:
        return DropDecision(KEEP, tuple(sorted(set(reasons), key=DropReason.sort_key)))
    return DropDecision(DROP, (DropReason(REASON_ARCHIVES_VERIFIED),))
