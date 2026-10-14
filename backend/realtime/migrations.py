from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


EXPECTED_DATABASE_NAME = "stocks_realtime_v1"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "sql"
EXPECTED_V2_DATABASE_NAME = "stocks_realtime_v2"
V2_MIGRATIONS_DIR = Path(__file__).resolve().parent / "sql_v2"
# Deferred: structure only, no roles or grants, runtime connects as the database owner.
# Hardened: the structure plus the final security phase (roles, grants, exact-privilege audit).
SECURITY_MODE_DEFERRED = "deferred"
SECURITY_MODE_HARDENED = "hardened"
SECURITY_MODES_V2 = (SECURITY_MODE_DEFERRED, SECURITY_MODE_HARDENED)
PHASE_STRUCTURE = "STRUCTURE"
PHASE_SECURITY = "SECURITY"
_MIGRATION_PATTERN = re.compile(r"^(\d{3})_([a-z0-9_]+)\.sql$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class RealtimeMigration:
    version: str
    sequence: int
    path: Path
    sql: str
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class RealtimeMigrationPlan:
    database_name: str
    schema_sha256: str
    applied_versions: tuple[str, ...]
    pending: tuple[RealtimeMigration, ...]


def validate_target_database(
    database_name: str,
    confirmation: str,
    *,
    allow_disposable: bool = False,
) -> str:
    target = database_name.strip()
    if not target or target != confirmation.strip():
        raise ValueError("database confirmation must exactly match the target")
    if target == EXPECTED_DATABASE_NAME:
        return target
    if allow_disposable and target.startswith("test_realtime_"):
        return target
    raise ValueError(
        f"realtime migrations require {EXPECTED_DATABASE_NAME} or an explicit "
        "test_realtime_ disposable target"
    )


def validate_target_database_v2(
    database_name: str,
    confirmation: str,
    *,
    allow_disposable: bool = False,
    security_mode: str = SECURITY_MODE_DEFERRED,
) -> str:
    target = database_name.strip()
    if not target or target != confirmation.strip():
        raise ValueError("database confirmation must exactly match the target")
    if security_mode not in SECURITY_MODES_V2:
        raise ValueError(f"security mode must be one of {', '.join(SECURITY_MODES_V2)}")
    if target == EXPECTED_V2_DATABASE_NAME:
        return target
    if allow_disposable and target.startswith("test_realtime_v2_"):
        return target
    raise ValueError(
        f"realtime v2 migrations require {EXPECTED_V2_DATABASE_NAME} or an explicit "
        "test_realtime_v2_ disposable target"
    )


def v2_provisioning_phases(security_mode: str = SECURITY_MODE_DEFERRED) -> tuple[str, ...]:
    """Ordered provisioning phases. Security (roles, grants, audit) is always last and optional."""
    if security_mode not in SECURITY_MODES_V2:
        raise ValueError(f"security mode must be one of {', '.join(SECURITY_MODES_V2)}")
    if security_mode == SECURITY_MODE_DEFERRED:
        return (PHASE_STRUCTURE,)
    return (PHASE_STRUCTURE, PHASE_SECURITY)


def load_realtime_migrations(
    migrations_dir: Path = MIGRATIONS_DIR,
) -> tuple[RealtimeMigration, ...]:
    migrations: list[RealtimeMigration] = []
    seen_sequences: set[int] = set()
    for path in sorted(migrations_dir.glob("*.sql")):
        match = _MIGRATION_PATTERN.fullmatch(path.name)
        if match is None:
            raise ValueError(f"invalid realtime migration filename: {path.name}")
        sequence = int(match.group(1))
        if sequence in seen_sequences:
            raise ValueError(f"duplicate realtime migration sequence: {sequence:03d}")
        seen_sequences.add(sequence)
        sql = path.read_text(encoding="utf-8")
        if not sql.strip():
            raise ValueError(f"realtime migration is empty: {path.name}")
        migrations.append(
            RealtimeMigration(
                version=path.stem,
                sequence=sequence,
                path=path,
                sql=sql,
                checksum_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    if not migrations or migrations[0].version != "000_realtime_baseline":
        raise ValueError("realtime migration lineage must start at 000_realtime_baseline")
    expected_sequences = tuple(range(len(migrations)))
    actual_sequences = tuple(migration.sequence for migration in migrations)
    if actual_sequences != expected_sequences:
        raise ValueError("realtime migration sequences must be contiguous from 000")
    return tuple(migrations)


def schema_fingerprint(migrations: tuple[RealtimeMigration, ...]) -> str:
    manifest = "\n".join(
        f"{migration.version}:{migration.checksum_sha256}"
        for migration in migrations
    )
    return hashlib.sha256(manifest.encode("ascii")).hexdigest()


def plan_realtime_migrations(
    database_name: str,
    applied: Mapping[str, str],
    migrations: tuple[RealtimeMigration, ...] | None = None,
) -> RealtimeMigrationPlan:
    catalog = migrations or load_realtime_migrations()
    by_version = {migration.version: migration for migration in catalog}
    for version, checksum in applied.items():
        migration = by_version.get(version)
        if migration is None:
            raise ValueError(f"database records unknown realtime migration: {version}")
        if not _SHA256_PATTERN.fullmatch(checksum):
            raise ValueError(f"database records invalid checksum for {version}")
        if checksum != migration.checksum_sha256:
            raise ValueError(f"realtime migration checksum drift: {version}")
    pending = tuple(
        migration for migration in catalog if migration.version not in applied
    )
    return RealtimeMigrationPlan(
        database_name=database_name,
        schema_sha256=schema_fingerprint(catalog),
        applied_versions=tuple(
            migration.version for migration in catalog if migration.version in applied
        ),
        pending=pending,
    )