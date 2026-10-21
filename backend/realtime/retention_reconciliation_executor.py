"""Guarded one-use executor for the frozen retention reconciliation package."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .retention_reconciliation_authorization import (
    POSTFLIGHT_OBSERVATION_SCHEMA,
    _canonical_sha256,
    _timestamp,
    validate_postflight_observation,
    validate_reconciliation_authorization,
)
from .session_archive import (
    ArchiveIdentity,
    archive_file_names,
    build_archive,
    render_manifest,
)


EXPECTED_PACKAGE_SHA256 = (
    "afb1e67ac403f2d64c8c6fcdc4575f5bf77df801baa6ac7219315371ea9fd991"
)
EXPECTED_SCOPE_SHA256 = (
    "5860ba57f7428daa62e75b67b2c6aa1e93ae17f3d13403f85efbf646f866ca4d"
)
EXPECTED_NONCE = "c9d6025a-57e4-4c65-a792-9e21d2f06731"
EXPECTED_WINDOW_START = datetime(2026, 10, 22, 16, 30, tzinfo=timezone.utc)
EXPECTED_WINDOW_END = datetime(2026, 10, 22, 16, 40, tzinfo=timezone.utc)
EXPECTED_PACKAGE_FILE_SHA256 = (
    "6b4db0e109f8dd35384ecc2725bb48e1cd678ee768fe0bdcfb5552ce005fdd52"
)
EXPECTED_POSTFLIGHT_FILE_SHA256 = (
    "0c0c0552f3d594e358827939359a81558db8a6ebe60a8ccb4dbe56f7a4b92cdb"
)
EXECUTION_LOCK_KEY = "options-owp5-retention-reconciliation-v1"
STATUS_READY = "READY"
STATUS_LOCKED = "LOCKED_OR_INTERRUPTED"
STATUS_AMBIGUOUS = "AMBIGUOUS_PARTIAL_EXECUTION"
STATUS_CONSUMED = "CONSUMED_COMMITTED_POSTFLIGHT_REQUIRED"
STATUS_POSTFLIGHT_PASS = "PASS_RECONCILIATION_POSTFLIGHT_RETENTION_STOPPED"
_UTC = timezone.utc
_SOURCE_COUNT_EXPRESSIONS = {
    "rt.option_flow_events": (
        "(event_time AT TIME ZONE 'America/New_York')::date = %s"
    ),
    "rt.option_flow_event_profiles": "session_date = %s",
    "rt.option_flow_clusters": "session_date = %s",
    "rt.option_flow_cluster_members": "session_date = %s",
}


class ReconciliationExecutionError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class LedgerRow:
    archive_id: UUID
    session_date: date
    kind: str
    uri: str
    uri_relative: str
    sha256: str
    row_count: int
    schema_version: str
    build_id: str
    verified_at: datetime

    def postflight_dict(self) -> dict[str, object]:
        return {
            "session_date": self.session_date.isoformat(),
            "kind": self.kind,
            "uri_relative": self.uri_relative,
            "sha256": self.sha256,
            "row_count": self.row_count,
            "schema_version": self.schema_version,
            "build_id": self.build_id,
        }


@dataclass(frozen=True, slots=True)
class DatabaseObservation:
    source_counts: Mapping[str, int]
    attached_partitions: tuple[str, ...]
    ledger_rows: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class ExecutionStatus:
    state: str
    detail: str
    database: DatabaseObservation
    target_files_present: tuple[str, ...]
    target_files_invalid: tuple[str, ...]
    partial_files: tuple[str, ...]
    lock_present: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "detail": self.detail,
            "source_counts": dict(self.database.source_counts),
            "attached_partitions": list(
                self.database.attached_partitions
            ),
            "ledger_rows": [dict(row) for row in self.database.ledger_rows],
            "target_files_present": list(self.target_files_present),
            "target_files_invalid": list(self.target_files_invalid),
            "partial_files": list(self.partial_files),
            "lock_present": self.lock_present,
        }


class ReconciliationRepository(Protocol):
    def observe(
        self,
        package: Mapping[str, object],
        ledger_rows: Sequence[LedgerRow],
    ) -> DatabaseObservation: ...

    def append(
        self,
        package: Mapping[str, object],
        ledger_rows: Sequence[LedgerRow],
        *,
        promote: Callable[[], None],
        fault: Callable[[str], None],
    ) -> None: ...


class ServiceStateProbe(Protocol):
    def __call__(self) -> Mapping[str, object]: ...


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(_UTC)


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def load_exact_package(
    package_path: Path,
    postflight_path: Path,
) -> dict[str, object]:
    """Load only the frozen package and its exact standalone postflight contract."""
    package_file_sha256 = hashlib.sha256(package_path.read_bytes()).hexdigest()
    postflight_file_sha256 = hashlib.sha256(
        postflight_path.read_bytes()
    ).hexdigest()
    if package_file_sha256 != EXPECTED_PACKAGE_FILE_SHA256:
        raise ValueError("reconciliation package file identity changed")
    if postflight_file_sha256 != EXPECTED_POSTFLIGHT_FILE_SHA256:
        raise ValueError("reconciliation postflight file identity changed")
    package = _json(package_path)
    postflight = _json(postflight_path)
    if (
        package.get("package_sha256") != EXPECTED_PACKAGE_SHA256
        or package.get("authorization_scope_sha256")
        != EXPECTED_SCOPE_SHA256
        or package.get("execution_nonce") != EXPECTED_NONCE
        or _timestamp(
            package.get("window", {}).get("start_utc"),
            "window.start_utc",
        )
        != EXPECTED_WINDOW_START
        or _timestamp(
            package.get("window", {}).get("stop_utc"),
            "window.stop_utc",
        )
        != EXPECTED_WINDOW_END
        or package.get("postflight_contract") != postflight
        or _canonical_sha256(postflight)
        != package.get("postflight_contract_sha256")
    ):
        raise ValueError("reconciliation package exact scope changed")
    return package


def _revision_rows(
    package: Mapping[str, object],
    archive_root: Path,
    verified_at: datetime,
) -> tuple[LedgerRow, ...]:
    session_date = date.fromisoformat(
        str(package["authorization_scope"]["scope"]["session_date"])
    )
    nonce = package["execution_nonce"]
    package_sha256 = package["package_sha256"]
    rows = []
    for revision in package["authorization_scope"]["revisions"]:
        append_only = revision["append_only_revision"]
        relative = str(append_only["data_file"])
        path = archive_root / Path(relative)
        rows.append(LedgerRow(
            archive_id=uuid5(
                NAMESPACE_URL,
                (
                    "stockscreener:retention-reconciliation:"
                    f"{package_sha256}:{nonce}:{revision['kind']}"
                ),
            ),
            session_date=session_date,
            kind=str(revision["kind"]),
            uri=path.resolve().as_uri(),
            uri_relative=relative.replace("\\", "/"),
            sha256=str(append_only["data_sha256"]),
            row_count=0,
            schema_version=str(
                append_only["archive_schema_version"]
            ),
            build_id=str(append_only["build_id"]),
            verified_at=_utc(verified_at, "verified_at"),
        ))
    return tuple(rows)


def _expected_new_bytes(
    package: Mapping[str, object],
) -> dict[str, bytes]:
    session_date = date.fromisoformat(
        str(package["authorization_scope"]["scope"]["session_date"])
    )
    files: dict[str, bytes] = {}
    for revision in package["authorization_scope"]["revisions"]:
        append_only = revision["append_only_revision"]
        identity = ArchiveIdentity(
            str(append_only["build_id"]),
            str(append_only["policy_id"]),
        )
        built = build_archive(str(revision["kind"]), session_date, ())
        data_name, manifest_name = archive_file_names(
            str(revision["kind"]),
            content_sha256=built.content_sha256,
            identity_sha256=identity.sha256,
        )
        data_relative = str(append_only["data_file"])
        manifest_relative = str(append_only["manifest_file"])
        if (
            Path(data_relative).name != data_name
            or Path(manifest_relative).name != manifest_name
            or hashlib.sha256(built.payload).hexdigest()
            != append_only["data_sha256"]
        ):
            raise ValueError("authorized revision bytes changed")
        manifest = render_manifest(
            built,
            file_name=data_name,
            identity=identity,
        )
        if (
            hashlib.sha256(manifest).hexdigest()
            != append_only["manifest_sha256"]
        ):
            raise ValueError("authorized revision manifest changed")
        files[data_relative] = built.payload
        files[manifest_relative] = manifest
    return files


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _original_capture(
    package: Mapping[str, object],
    archive_root: Path,
) -> tuple[dict[str, object], ...]:
    captures = []
    for revision in package["authorization_scope"]["revisions"]:
        preserved = revision["preserved"]
        data_path = archive_root / Path(str(preserved["data_file"]))
        manifest_path = archive_root / Path(
            str(preserved["manifest_file"])
        )
        if not data_path.is_file() or not manifest_path.is_file():
            raise ReconciliationExecutionError(
                "ORIGINAL_EVIDENCE_MISSING",
                str(revision["kind"]),
            )
        data_sha256 = _hash(data_path)
        if data_sha256 != preserved["data_sha256"]:
            raise ReconciliationExecutionError(
                "ORIGINAL_EVIDENCE_DRIFT",
                f"{revision['kind']} data SHA-256 changed",
            )
        try:
            content = gzip.decompress(data_path.read_bytes())
            manifest = json.loads(
                manifest_path.read_text(encoding="ascii")
            )
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:
            raise ReconciliationExecutionError(
                "ORIGINAL_EVIDENCE_INVALID",
                f"{revision['kind']}: {type(error).__name__}",
            ) from error
        primary = revision["relations"][0]
        if (
            content != b""
            or not isinstance(manifest, dict)
            or manifest.get("archive_kind") != revision["kind"]
            or manifest.get("file") != data_path.name
            or manifest.get("build_id")
            != revision["append_only_revision"]["build_id"]
            or manifest.get("policy_id")
            != revision["append_only_revision"]["policy_id"]
            or manifest.get("relation_counts") != {primary: 0}
            or manifest.get("row_count") != 0
        ):
            raise ReconciliationExecutionError(
                "ORIGINAL_EVIDENCE_SCOPE_DRIFT",
                str(revision["kind"]),
            )
        data_stat = data_path.stat()
        manifest_stat = manifest_path.stat()
        captures.append({
            "kind": revision["kind"],
            "data_file": str(preserved["data_file"]),
            "data_sha256_before": data_sha256,
            "data_mtime_ns_before": data_stat.st_mtime_ns,
            "manifest_file": str(preserved["manifest_file"]),
            "manifest_sha256_before": _hash(manifest_path),
            "manifest_mtime_ns_before": manifest_stat.st_mtime_ns,
        })
    return tuple(captures)


def _directory_inventory(
    package: Mapping[str, object],
    archive_root: Path,
) -> tuple[str, ...]:
    session = str(
        package["authorization_scope"]["scope"]["session_date"]
    )
    directory = archive_root / session
    if not directory.is_dir():
        raise ReconciliationExecutionError(
            "ARCHIVE_DIRECTORY_MISSING",
            session,
        )
    return tuple(sorted(
        str(path.relative_to(archive_root)).replace("\\", "/")
        for path in directory.iterdir()
        if path.is_file()
    ))


class ExecutionLock:
    def __init__(self, path: Path, package: Mapping[str, object]) -> None:
        self.path = path.resolve()
        self.package = package

    def acquire(self, *, acquired_at: datetime) -> None:
        if not self.path.is_absolute() or not self.path.parent.is_dir():
            raise ValueError("execution lock path must have an existing absolute parent")
        value = {
            "schema": "options-retention-reconciliation-lock-v1",
            "state": "LOCKED_PREFLIGHT",
            "package_sha256": self.package["package_sha256"],
            "execution_nonce": self.package["execution_nonce"],
            "acquired_at_utc": _utc(
                acquired_at,
                "acquired_at",
            ).isoformat(),
        }
        try:
            with self.path.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as error:
            raise ReconciliationExecutionError(
                "EXECUTION_LOCK_CONFLICT",
                str(self.path),
            ) from error

    def update(self, value: Mapping[str, object]) -> None:
        current = self.read()
        if (
            current.get("package_sha256")
            != self.package["package_sha256"]
            or current.get("execution_nonce")
            != self.package["execution_nonce"]
        ):
            raise ReconciliationExecutionError(
                "EXECUTION_LOCK_IDENTITY_DRIFT",
                str(self.path),
            )
        document = {
            **current,
            **value,
        }
        temporary = self.path.with_name(
            f"{self.path.name}.{self.package['execution_nonce']}.partial"
        )
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(
                    json.dumps(document, indent=2, sort_keys=True) + "\n"
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def read(self) -> dict[str, object]:
        return _json(self.path)

    def release(self) -> None:
        current = self.read()
        if (
            current.get("package_sha256")
            != self.package["package_sha256"]
            or current.get("execution_nonce")
            != self.package["execution_nonce"]
        ):
            raise ReconciliationExecutionError(
                "EXECUTION_LOCK_IDENTITY_DRIFT",
                str(self.path),
            )
        self.path.unlink()


class PostgresReconciliationRepository:
    """Read and append the exact two immutable ledger rows without DDL."""

    def __init__(
        self,
        connect: Callable[[], object],
        *,
        statement_timeout: str = "30s",
    ) -> None:
        self._connect = connect
        self._statement_timeout = statement_timeout

    @staticmethod
    def _parameters(
        package: Mapping[str, object],
    ) -> tuple[date, tuple[str, ...], tuple[str, ...]]:
        scope = package["authorization_scope"]["scope"]
        return (
            date.fromisoformat(str(scope["session_date"])),
            tuple(scope["read_relations"]),
            tuple(scope["read_partitions"]),
        )

    @staticmethod
    def _observe_cursor(
        cursor,
        package: Mapping[str, object],
        ledger_rows: Sequence[LedgerRow],
    ) -> DatabaseObservation:
        session_date, relations, partitions = (
            PostgresReconciliationRepository._parameters(package)
        )
        counts: dict[str, int] = {}
        for relation in relations:
            expression = _SOURCE_COUNT_EXPRESSIONS.get(relation)
            if expression is None:
                raise ValueError(f"unsupported reconciliation relation {relation}")
            schema, _, table = relation.partition(".")
            statement = sql.SQL(
                "SELECT count(*) AS row_count FROM {}.{} WHERE " + expression
            ).format(sql.Identifier(schema), sql.Identifier(table))
            cursor.execute(statement, (session_date,))
            counts[relation] = int(cursor.fetchone()["row_count"])
        attached = []
        for partition in partitions:
            schema, _, child = partition.partition(".")
            parent = partition.rsplit("_p", 1)[0]
            parent_schema, _, parent_table = parent.partition(".")
            cursor.execute(
                "SELECT EXISTS ("
                "SELECT 1 FROM pg_inherits AS link "
                "JOIN pg_class AS child ON child.oid=link.inhrelid "
                "JOIN pg_namespace AS child_ns ON child_ns.oid=child.relnamespace "
                "JOIN pg_class AS parent ON parent.oid=link.inhparent "
                "JOIN pg_namespace AS parent_ns ON parent_ns.oid=parent.relnamespace "
                "WHERE child_ns.nspname=%s AND child.relname=%s "
                "AND parent_ns.nspname=%s AND parent.relname=%s) AS attached",
                (schema, child, parent_schema, parent_table),
            )
            if bool(cursor.fetchone()["attached"]):
                attached.append(partition)
        ids = [str(row.archive_id) for row in ledger_rows]
        uris = [row.uri for row in ledger_rows]
        kinds = [row.kind for row in ledger_rows]
        cursor.execute(
            "SELECT archive_id::text,session_date,kind,uri,sha256,row_count,"
            "schema_version,build_id,verified_at "
            "FROM rt.session_archives WHERE archive_id=ANY(%s::uuid[]) "
            "OR uri=ANY(%s::text[]) "
            "OR (session_date=%s AND kind=ANY(%s::text[]) "
            "AND sha256=ANY(%s::text[]) AND build_id=ANY(%s::text[])) "
            "ORDER BY kind,archive_id",
            (
                ids,
                uris,
                session_date,
                kinds,
                [row.sha256 for row in ledger_rows],
                [row.build_id for row in ledger_rows],
            ),
        )
        found = tuple(dict(row) for row in cursor.fetchall())
        return DatabaseObservation(
            source_counts=counts,
            attached_partitions=tuple(sorted(attached)),
            ledger_rows=found,
        )

    def observe(
        self,
        package: Mapping[str, object],
        ledger_rows: Sequence[LedgerRow],
    ) -> DatabaseObservation:
        with closing(self._connect()) as connection:
            connection.set_session(
                isolation_level="REPEATABLE READ",
                readonly=True,
                autocommit=False,
            )
            try:
                with connection.cursor(
                    cursor_factory=RealDictCursor,
                ) as cursor:
                    cursor.execute(
                        f"SET LOCAL statement_timeout='{self._statement_timeout}'"
                    )
                    return self._observe_cursor(
                        cursor,
                        package,
                        ledger_rows,
                    )
            finally:
                connection.rollback()

    def append(
        self,
        package: Mapping[str, object],
        ledger_rows: Sequence[LedgerRow],
        *,
        promote: Callable[[], None],
        fault: Callable[[str], None],
    ) -> None:
        with closing(self._connect()) as connection:
            connection.set_session(
                isolation_level="SERIALIZABLE",
                readonly=False,
                autocommit=False,
            )
            try:
                with connection.cursor(
                    cursor_factory=RealDictCursor,
                ) as cursor:
                    cursor.execute(
                        f"SET LOCAL statement_timeout='{self._statement_timeout}'"
                    )
                    cursor.execute("SET LOCAL lock_timeout='5s'")
                    cursor.execute(
                        "SELECT pg_try_advisory_xact_lock("
                        "hashtextextended(%s,0)) AS acquired",
                        (
                            f"{EXECUTION_LOCK_KEY}:"
                            f"{package['package_sha256']}",
                        ),
                    )
                    if not bool(cursor.fetchone()["acquired"]):
                        raise ReconciliationExecutionError(
                            "DATABASE_LOCK_CONFLICT",
                            EXECUTION_LOCK_KEY,
                        )
                    observation = self._observe_cursor(
                        cursor,
                        package,
                        ledger_rows,
                    )
                    _assert_database_ready(package, observation)
                    for row in ledger_rows:
                        cursor.execute(
                            "INSERT INTO rt.session_archives ("
                            "archive_id,session_date,kind,horizon_close_date,"
                            "uri,sha256,row_count,schema_version,build_id,"
                            "verified_at) VALUES (%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s)",
                            (
                                str(row.archive_id),
                                row.session_date,
                                row.kind,
                                row.uri,
                                row.sha256,
                                row.row_count,
                                row.schema_version,
                                row.build_id,
                                row.verified_at,
                            ),
                        )
                    fault("AFTER_DB_INSERTS")
                    promote()
                    fault("BEFORE_COMMIT")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise


def _expected_ledger_signature(
    rows: Sequence[LedgerRow],
) -> set[tuple[str, date, str, str, str, int, str, str]]:
    return {
        (
            str(row.archive_id),
            row.session_date,
            row.kind,
            row.uri,
            row.sha256,
            row.row_count,
            row.schema_version,
            row.build_id,
        )
        for row in rows
    }


def _observed_ledger_signature(
    rows: Sequence[Mapping[str, object]],
) -> set[tuple[str, date, str, str, str, int, str, str]]:
    return {
        (
            str(row["archive_id"]),
            row["session_date"],
            str(row["kind"]),
            str(row["uri"]),
            str(row["sha256"]),
            int(row["row_count"]),
            str(row["schema_version"]),
            str(row["build_id"]),
        )
        for row in rows
    }


def _assert_database_ready(
    package: Mapping[str, object],
    observation: DatabaseObservation,
) -> None:
    expected_relations = tuple(
        package["authorization_scope"]["scope"]["read_relations"]
    )
    expected_partitions = tuple(
        package["authorization_scope"]["scope"]["read_partitions"]
    )
    if (
        tuple(sorted(observation.source_counts)) != expected_relations
        or any(observation.source_counts.values())
    ):
        raise ReconciliationExecutionError(
            "SOURCE_COUNT_DRIFT",
            json.dumps(dict(observation.source_counts), sort_keys=True),
        )
    if observation.attached_partitions != expected_partitions:
        raise ReconciliationExecutionError(
            "PARTITION_SCOPE_DRIFT",
            json.dumps(list(observation.attached_partitions)),
        )
    if observation.ledger_rows:
        raise ReconciliationExecutionError(
            "PRIOR_OR_AMBIGUOUS_EXECUTION",
            f"{len(observation.ledger_rows)} target ledger rows",
        )


def _file_state(
    package: Mapping[str, object],
    archive_root: Path,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    expected = _expected_new_bytes(package)
    present = []
    invalid = []
    partial = []
    nonce = str(package["execution_nonce"])
    for relative, value in expected.items():
        path = archive_root / Path(relative)
        if path.exists():
            present.append(relative)
            if not path.is_file() or _hash(path) != hashlib.sha256(value).hexdigest():
                invalid.append(relative)
        temporary = path.with_name(f"{path.name}.{nonce}.partial")
        if temporary.exists():
            partial.append(
                str(temporary.relative_to(archive_root)).replace("\\", "/")
            )
    return (
        tuple(sorted(present)),
        tuple(sorted(invalid)),
        tuple(sorted(partial)),
    )


def classify_execution_status(
    package: Mapping[str, object],
    archive_root: Path,
    lock_path: Path,
    database: DatabaseObservation,
    ledger_rows: Sequence[LedgerRow],
) -> ExecutionStatus:
    present, invalid, partial = _file_state(package, archive_root)
    expected_files = len(_expected_new_bytes(package))
    expected_ledger = _expected_ledger_signature(ledger_rows)
    observed_ledger = _observed_ledger_signature(database.ledger_rows)
    lock_present = lock_path.exists()
    if (
        not invalid
        and len(present) == expected_files
        and observed_ledger == expected_ledger
    ):
        return ExecutionStatus(
            STATUS_CONSUMED,
            "exact append-only files and durable two-row receipt exist",
            database,
            present,
            invalid,
            partial,
            lock_present,
        )
    if database.ledger_rows or present or invalid or partial:
        return ExecutionStatus(
            STATUS_AMBIGUOUS,
            "partial files or ledger receipt require read-only classification",
            database,
            present,
            invalid,
            partial,
            lock_present,
        )
    if lock_present:
        return ExecutionStatus(
            STATUS_LOCKED,
            "exclusive execution lock exists without committed receipt",
            database,
            present,
            invalid,
            partial,
            True,
        )
    return ExecutionStatus(
        STATUS_READY,
        "no prior execution evidence",
        database,
        present,
        invalid,
        partial,
        False,
    )


def _write_staged(path: Path, value: bytes) -> None:
    if not path.parent.is_dir():
        raise ReconciliationExecutionError(
            "TARGET_DIRECTORY_MISSING",
            str(path.parent),
        )
    with path.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _promote_new(temporary: Path, final: Path) -> None:
    if final.exists():
        raise FileExistsError(str(final))
    try:
        os.link(temporary, final)
        temporary.unlink()
    except OSError:
        if final.exists():
            raise FileExistsError(str(final)) from None
        os.rename(temporary, final)


def _service_state(value: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "retention": "STOPPED",
        "archive": "STOPPED",
        "starts": 0,
        "restarts": 0,
    }
    if dict(value) != expected:
        raise ReconciliationExecutionError(
            "SERVICE_STATE_CHANGED",
            json.dumps(dict(value), sort_keys=True),
        )
    return expected


class ReconciliationPostflightRunner:
    def __init__(
        self,
        repository: ReconciliationRepository,
        archive_root: Path,
        lock: ExecutionLock,
        service_probe: ServiceStateProbe,
    ) -> None:
        self._repository = repository
        self._archive_root = archive_root.resolve()
        self._lock = lock
        self._service_probe = service_probe

    def run(
        self,
        package: Mapping[str, object],
        *,
        observed_at: datetime,
        release_lock_on_pass: bool = True,
    ) -> dict[str, object]:
        receipt = self._lock.read()
        originals = receipt.get("originals")
        inventory_before = receipt.get("inventory_before")
        services_before = receipt.get("services_before")
        if (
            not isinstance(originals, list)
            or not isinstance(inventory_before, list)
            or not isinstance(services_before, dict)
        ):
            raise ReconciliationExecutionError(
                "POSTFLIGHT_RECEIPT_INCOMPLETE",
                str(self._lock.path),
            )
        services_before = _service_state(services_before)
        services_after = _service_state(self._service_probe())
        ledger_rows = _revision_rows(
            package,
            self._archive_root,
            _utc(observed_at, "observed_at"),
        )
        database = self._repository.observe(package, ledger_rows)
        expected_signature = _expected_ledger_signature(ledger_rows)
        if _observed_ledger_signature(database.ledger_rows) != expected_signature:
            raise ReconciliationExecutionError(
                "POSTFLIGHT_LEDGER_MISMATCH",
                "durable receipt is not the exact two-row pair",
            )
        expected_files = _expected_new_bytes(package)
        revisions = []
        for expected in package["postflight_contract"]["expected_revisions"]:
            data_path = self._archive_root / Path(expected["data_file"])
            manifest_path = self._archive_root / Path(
                expected["manifest_file"]
            )
            if (
                not data_path.is_file()
                or not manifest_path.is_file()
                or _hash(data_path) != expected["data_sha256"]
                or _hash(manifest_path) != expected["manifest_sha256"]
            ):
                raise ReconciliationExecutionError(
                    "POSTFLIGHT_REVISION_MISMATCH",
                    str(expected["kind"]),
                )
            revisions.append(dict(expected))
        completed_originals = []
        rewritten = []
        for original in originals:
            data_path = self._archive_root / Path(original["data_file"])
            manifest_path = self._archive_root / Path(
                original["manifest_file"]
            )
            completed = {
                **original,
                "data_sha256_after": _hash(data_path),
                "data_mtime_ns_after": data_path.stat().st_mtime_ns,
                "manifest_sha256_after": _hash(manifest_path),
                "manifest_mtime_ns_after": manifest_path.stat().st_mtime_ns,
            }
            completed_originals.append(completed)
            if (
                completed["data_sha256_before"]
                != completed["data_sha256_after"]
                or completed["data_mtime_ns_before"]
                != completed["data_mtime_ns_after"]
                or completed["manifest_sha256_before"]
                != completed["manifest_sha256_after"]
                or completed["manifest_mtime_ns_before"]
                != completed["manifest_mtime_ns_after"]
            ):
                rewritten.extend([
                    original["data_file"],
                    original["manifest_file"],
                ])
        inventory_after = set(_directory_inventory(package, self._archive_root))
        before = set(str(value) for value in inventory_before)
        authorized_new = set(expected_files)
        unexpected = sorted(inventory_after - before - authorized_new)
        deleted = sorted(before - inventory_after)
        partial = sorted(
            str(path.relative_to(self._archive_root)).replace("\\", "/")
            for path in self._archive_root.rglob(
                f"*.{package['execution_nonce']}.partial"
            )
        )
        observation = {
            "schema": POSTFLIGHT_OBSERVATION_SCHEMA,
            "authorization_package_sha256": package["package_sha256"],
            "mode": "READ_ONLY",
            "observed_at_utc": _utc(
                observed_at,
                "observed_at",
            ).isoformat(),
            "originals": completed_originals,
            "revisions": revisions,
            "database": {
                **package["postflight_contract"]["expected_database"],
                "inserted_session_archive_rows": [
                    row.postflight_dict()
                    for row in ledger_rows
                ],
            },
            "filesystem": {
                "unexpected_files": unexpected,
                "deleted_files": deleted,
                "rewritten_files": sorted(set(rewritten)),
                "cleanup_paths_remaining": partial,
            },
            "services": {
                "retention_before": services_before["retention"],
                "retention_after": services_after["retention"],
                "archive_before": services_before["archive"],
                "archive_after": services_after["archive"],
                "starts": services_after["starts"] - services_before["starts"],
                "restarts": (
                    services_after["restarts"]
                    - services_before["restarts"]
                ),
            },
            "external_effects": package[
                "postflight_contract"
            ]["expected_external_effects"],
            "cleanup": {
                "temporary_files": partial,
                "temporary_databases": [],
                "temporary_roles": [],
            },
        }
        result = validate_postflight_observation(package, observation)
        if release_lock_on_pass:
            self._lock.release()
        return {
            **result,
            "observation": observation,
        }


class ReconciliationExecutor:
    def __init__(
        self,
        repository: ReconciliationRepository,
        *,
        repository_root: Path,
        archive_root: Path,
        report_path: Path,
        lock_path: Path,
        service_probe: ServiceStateProbe,
    ) -> None:
        self._repository = repository
        self._repository_root = repository_root.resolve()
        self._archive_root = archive_root.resolve()
        self._report_path = report_path.resolve()
        self._lock_path = lock_path.resolve()
        self._service_probe = service_probe
        if (
            not self._archive_root.is_absolute()
            or not self._archive_root.is_dir()
        ):
            raise ValueError("archive_root must be an existing absolute directory")

    def plan(self, package: Mapping[str, object]) -> dict[str, object]:
        return {
            "mode": "PLAN",
            "package_sha256": package["package_sha256"],
            "authorization_scope_sha256": package[
                "authorization_scope_sha256"
            ],
            "execution_nonce": package["execution_nonce"],
            "window": package["window"],
            "operations": [
                "validate_exact_package_and_source",
                "acquire_exclusive_file_and_database_locks",
                "stage_four_create-new files",
                "insert_two_immutable_session_archive_rows",
                "promote_files_and_commit_once",
                "run_frozen_read_only_postflight",
            ],
            "database_writes": 0,
            "archive_writes": 0,
            "archive_deletes": 0,
            "retention_started": False,
        }

    def status(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> ExecutionStatus:
        rows = _revision_rows(
            package,
            self._archive_root,
            _utc(checked_at, "checked_at"),
        )
        database = self._repository.observe(package, rows)
        return classify_execution_status(
            package,
            self._archive_root,
            self._lock_path,
            database,
            rows,
        )

    def preflight(
        self,
        package: Mapping[str, object],
        current_plan: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> dict[str, object]:
        status = self.status(package, checked_at=checked_at)
        if status.state != STATUS_READY:
            raise ReconciliationExecutionError(
                "EXECUTION_NOT_READY",
                status.state,
            )
        validate_reconciliation_authorization(
            package,
            current_plan=current_plan,
            report_path=self._report_path,
            repository_root=self._repository_root,
            expected_package_sha256=EXPECTED_PACKAGE_SHA256,
            now=checked_at,
            prior_execution_records=(),
        )
        _assert_database_ready(package, status.database)
        _original_capture(package, self._archive_root)
        _service_state(self._service_probe())
        return {
            "mode": "PREFLIGHT",
            "status": "PASS_RECONCILIATION_EXECUTION_PREFLIGHT",
            "execution_status": status.as_dict(),
            "package_sha256": package["package_sha256"],
            "provider_requests": 0,
            "database_writes": 0,
            "archive_writes": 0,
            "archive_deletes": 0,
            "retention_started": False,
        }

    def execute(
        self,
        package: Mapping[str, object],
        current_plan: Mapping[str, object],
        *,
        checked_at: datetime,
        fault: Callable[[str], None] = lambda _stage: None,
    ) -> dict[str, object]:
        checked = _utc(checked_at, "checked_at")
        if not EXPECTED_WINDOW_START <= checked < EXPECTED_WINDOW_END:
            reason = (
                "EARLY_EXECUTION_WINDOW"
                if checked < EXPECTED_WINDOW_START
                else "MISSED_EXECUTION_WINDOW"
            )
            raise ReconciliationExecutionError(reason, checked.isoformat())
        self.preflight(
            package,
            current_plan,
            checked_at=checked,
        )
        lock = ExecutionLock(self._lock_path, package)
        lock.acquire(acquired_at=checked)
        try:
            fault("AFTER_LOCK")
            status = self.status(package, checked_at=checked)
            if status.state != STATUS_LOCKED:
                raise ReconciliationExecutionError(
                    "EXECUTION_STATE_CHANGED_AFTER_LOCK",
                    status.state,
                )
            originals = list(
                _original_capture(package, self._archive_root)
            )
            inventory_before = list(
                _directory_inventory(package, self._archive_root)
            )
            services_before = _service_state(self._service_probe())
            lock.update({
                "state": "LOCKED_STAGING",
                "originals": originals,
                "inventory_before": inventory_before,
                "services_before": services_before,
            })
            expected = _expected_new_bytes(package)
            staged: list[tuple[Path, Path]] = []
            for relative, value in expected.items():
                final = self._archive_root / Path(relative)
                temporary = final.with_name(
                    f"{final.name}.{package['execution_nonce']}.partial"
                )
                _write_staged(temporary, value)
                staged.append((temporary, final))
            fault("AFTER_STAGE")
            rows = _revision_rows(
                package,
                self._archive_root,
                checked,
            )
            promoted = 0

            def promote() -> None:
                nonlocal promoted
                for temporary, final in staged:
                    _promote_new(temporary, final)
                    promoted += 1
                    if promoted == 1:
                        fault("AFTER_FIRST_PROMOTE")

            self._repository.append(
                package,
                rows,
                promote=promote,
                fault=fault,
            )
            fault("AFTER_COMMIT")
            lock.update({
                "state": "COMMITTED_POSTFLIGHT_PENDING",
                "committed_at_utc": checked.isoformat(),
            })
            fault("BEFORE_POSTFLIGHT")
            postflight = ReconciliationPostflightRunner(
                self._repository,
                self._archive_root,
                lock,
                self._service_probe,
            ).run(package, observed_at=checked)
            fault("AFTER_POSTFLIGHT")
            return {
                "status": STATUS_POSTFLIGHT_PASS,
                "package_sha256": package["package_sha256"],
                "execution_nonce": package["execution_nonce"],
                "created_files": sorted(expected),
                "inserted_ledger_rows": [
                    row.postflight_dict() for row in rows
                ],
                "postflight": postflight,
                "archive_deletes": 0,
                "partition_prunes": 0,
                "retention_started": False,
                "service_starts": 0,
                "service_restarts": 0,
                "provider_requests": 0,
                "publication_actions": 0,
                "delivery_actions": 0,
            }
        except BaseException as error:
            for temporary in self._archive_root.rglob(
                f"*.{package['execution_nonce']}.partial"
            ):
                temporary.unlink(missing_ok=True)
            try:
                status = self.status(package, checked_at=checked)
            except BaseException:
                status = None
            if status is not None and status.state == STATUS_LOCKED:
                lock.release()
            if isinstance(error, ReconciliationExecutionError):
                raise
            raise ReconciliationExecutionError(
                "EXECUTION_INTERRUPTED",
                (
                    f"{type(error).__name__}: {str(error)[:240]}; "
                    f"classification={status.state if status else 'UNKNOWN'}"
                ),
            ) from error
