"""Durable exact-cohort/release stored-read control for Advanced Options."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from uuid import UUID

from .database_access_v2 import APP_ROLE, V2ConnectionProvider


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def load_advanced_options_read_pin_cursor(
    cursor,
    *,
    cohort_id: UUID,
    for_update: bool = False,
) -> Mapping[str, object] | None:
    cursor.execute(
        "SELECT cohort_id::text,release_id::text,package_sha256,"
        "release_cutoff,enabled,enabled_at,disabled_at,updated_at "
        "FROM rt.advanced_options_read_pins WHERE cohort_id=%s"
        + (" FOR UPDATE" if for_update else ""),
        (str(cohort_id),),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return dict(zip(
        (
            "cohort_id",
            "release_id",
            "package_sha256",
            "release_cutoff",
            "enabled",
            "enabled_at",
            "disabled_at",
            "updated_at",
        ),
        row,
        strict=True,
    ))


def load_enabled_advanced_options_release_cursor(
    cursor,
    *,
    cohort_id: UUID,
) -> tuple[UUID, datetime] | None:
    pin = load_advanced_options_read_pin_cursor(
        cursor,
        cohort_id=cohort_id,
    )
    if pin is None or pin["enabled"] is not True:
        return None
    return (
        UUID(str(pin["release_id"])),
        _utc(pin["release_cutoff"], "release_cutoff"),
    )


class PostgresAdvancedOptionsStoredReadPort:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        *,
        package_sha256: str,
        clock=lambda: datetime.now(timezone.utc),
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options read-pin provider is invalid")
        if (
            len(package_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in package_sha256
            )
        ):
            raise ValueError("Advanced Options read-pin package is invalid")
        if not callable(clock):
            raise TypeError("Advanced Options read-pin clock is invalid")
        self._provider = provider
        self._package_sha256 = package_sha256
        self._clock = clock

    def enable(
        self,
        *,
        cohort_id: UUID,
        release_id: UUID,
    ) -> Mapping[str, object]:
        checked = _utc(self._clock(), "clock")
        with self._provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
        ) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT release.release_id::text,release.cutoff "
                "FROM rt.portal_releases AS release "
                "JOIN rt.portal_release_current AS current "
                "ON current.cohort_id=release.cohort_id "
                "AND current.provider_tier=release.provider_tier "
                "AND current.release_id=release.release_id "
                "AND current.cutoff=release.cutoff "
                "WHERE release.release_id=%s AND release.cohort_id=%s "
                "AND release.provider_tier='MASSIVE_ADVANCED' FOR UPDATE",
                (str(release_id), str(cohort_id)),
            )
            release = cursor.fetchone()
            if release is None:
                raise RuntimeError(
                    "Advanced Options read pin is not the exact current release"
                )
            release_cutoff = (
                release["cutoff"]
                if isinstance(release, Mapping)
                else release[1]
            )
            current = load_advanced_options_read_pin_cursor(
                cursor,
                cohort_id=cohort_id,
                for_update=True,
            )
            if current is not None and current["enabled"] is True and (
                current["release_id"] != str(release_id)
                or current["release_cutoff"] != release_cutoff
                or current["package_sha256"] != self._package_sha256
            ):
                raise RuntimeError(
                    "another Advanced Options release is already pinned"
                )
            cursor.execute(
                "INSERT INTO rt.advanced_options_read_pins ("
                "cohort_id,release_id,release_cutoff,package_sha256,"
                "enabled,enabled_at,disabled_at,updated_at) "
                "VALUES (%s,%s,%s,%s,true,%s,NULL,%s) "
                "ON CONFLICT (cohort_id) DO UPDATE SET "
                "release_id=EXCLUDED.release_id,"
                "release_cutoff=EXCLUDED.release_cutoff,"
                "package_sha256=EXCLUDED.package_sha256,enabled=true,"
                "enabled_at=EXCLUDED.enabled_at,disabled_at=NULL,"
                "updated_at=EXCLUDED.updated_at",
                (
                    str(cohort_id),
                    str(release_id),
                    release_cutoff,
                    self._package_sha256,
                    checked,
                    checked,
                ),
            )
            stored = load_advanced_options_read_pin_cursor(
                cursor,
                cohort_id=cohort_id,
                for_update=True,
            )
        if (
            stored is None
            or stored["enabled"] is not True
            or stored["release_id"] != str(release_id)
            or stored["release_cutoff"] != release_cutoff
            or stored["package_sha256"] != self._package_sha256
        ):
            raise RuntimeError(
                "Advanced Options read pin verification failed"
            )
        return {
            "cohort_id": str(cohort_id),
            "release_id": str(release_id),
            "release_cutoff_utc": release_cutoff.isoformat(),
            "verified": True,
            "enabled_at_utc": stored["enabled_at"].isoformat(),
        }

    def disable(
        self,
        *,
        cohort_id: UUID,
        release_id: UUID,
    ) -> None:
        checked = _utc(self._clock(), "clock")
        with self._provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
        ) as unit:
            unit.cursor.execute(
                "UPDATE rt.advanced_options_read_pins SET enabled=false,"
                "disabled_at=%s,updated_at=%s WHERE cohort_id=%s "
                "AND release_id=%s AND package_sha256=%s AND enabled "
                "RETURNING cohort_id",
                (
                    checked,
                    checked,
                    str(cohort_id),
                    str(release_id),
                    self._package_sha256,
                ),
            )
            if unit.cursor.fetchone() is None:
                raise RuntimeError(
                    "Advanced Options exact read pin could not be disabled"
                )
