from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_api as api_module
from realtime.advanced_options_api import (
    AdvancedOptionsStoredProjectionRepository,
)
from realtime.advanced_options_read_pin import (
    PostgresAdvancedOptionsStoredReadPort,
)
from realtime.database_access_v2 import V2ConnectionProvider


UTC = timezone.utc
NOW = datetime(2026, 10, 21, 13, 35, tzinfo=UTC)
CUTOFF = NOW - timedelta(minutes=1)
COHORT = UUID("11111111-2222-3333-4444-555555555555")
RELEASE = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
RELEASE_2 = UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
PACKAGE = "1" * 64


class _Cursor:
    def __init__(self):
        self.pin = None
        self._row = None
        self.current_release_id = RELEASE
        self.current_cutoff = CUTOFF

    def execute(self, statement, parameters=()):
        if statement.startswith("SELECT release.release_id"):
            self._row = (
                str(self.current_release_id),
                self.current_cutoff,
            )
        elif statement.startswith(
            "SELECT cohort_id::text,release_id::text"
        ):
            self._row = self.pin
        elif statement.startswith(
            "INSERT INTO rt.advanced_options_read_pins"
        ):
            self.pin = {
                "cohort_id": parameters[0],
                "release_id": parameters[1],
                "package_sha256": parameters[3],
                "release_cutoff": parameters[2],
                "enabled": True,
                "enabled_at": parameters[4],
                "disabled_at": None,
                "updated_at": parameters[5],
            }
            self._row = None
        elif statement.startswith(
            "UPDATE rt.advanced_options_read_pins SET enabled=false"
        ):
            if self.pin and self.pin["enabled"]:
                self.pin.update(
                    enabled=False,
                    disabled_at=NOW,
                    updated_at=NOW,
                )
                self._row = (str(COHORT),)
            else:
                self._row = None
        elif statement.startswith(
            "DELETE FROM rt.advanced_options_read_pins"
        ):
            self._row = (
                (str(COHORT),)
                if self.pin
                and self.pin["release_id"] == parameters[1]
                and self.pin["package_sha256"] == parameters[2]
                else None
            )
            if self._row is not None:
                self.pin = None
        elif statement.startswith(
            "UPDATE rt.advanced_options_read_pins SET release_id="
        ):
            if (
                self.pin
                and self.pin["release_id"] == parameters[8]
                and self.pin["package_sha256"] == parameters[9]
            ):
                self.pin.update({
                    "release_id": parameters[0],
                    "release_cutoff": parameters[1],
                    "package_sha256": parameters[2],
                    "enabled": parameters[3],
                    "enabled_at": parameters[4],
                    "disabled_at": parameters[5],
                    "updated_at": parameters[6],
                })
                self._row = (str(COHORT),)
            else:
                self._row = None
        else:
            raise AssertionError(statement)

    def fetchone(self):
        return self._row


class _Provider(V2ConnectionProvider):
    def __init__(self):
        self.cursor = _Cursor()

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=self.cursor)


def test_exact_release_pin_enables_verifies_and_rolls_back():
    provider = _Provider()
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    result = port.enable(cohort_id=COHORT, release_id=RELEASE)

    assert result == {
        "cohort_id": str(COHORT),
        "release_id": str(RELEASE),
        "release_cutoff_utc": CUTOFF.isoformat(),
        "verified": True,
        "enabled_at_utc": NOW.isoformat(),
    }
    assert provider.cursor.pin["enabled"] is True
    port.disable(cohort_id=COHORT, release_id=RELEASE)
    assert provider.cursor.pin["enabled"] is False


def test_read_pin_refuses_another_enabled_package():
    provider = _Provider()
    provider.cursor.pin = {
        "cohort_id": str(COHORT),
        "release_id": str(UUID(int=3)),
        "package_sha256": "2" * 64,
        "release_cutoff": CUTOFF - timedelta(minutes=1),
        "enabled": True,
        "enabled_at": NOW - timedelta(minutes=1),
        "disabled_at": None,
        "updated_at": NOW - timedelta(minutes=1),
    }
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    with pytest.raises(RuntimeError, match="already pinned"):
        port.enable(cohort_id=COHORT, release_id=RELEASE)


def test_read_pin_rollback_removes_new_pin_when_prior_pin_is_absent():
    provider = _Provider()
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    port.enable(cohort_id=COHORT, release_id=RELEASE)
    restored = port.restore(
        cohort_id=COHORT,
        expected_release_id=RELEASE,
        previous=None,
    )

    assert restored is None
    assert provider.cursor.pin is None


def test_read_pin_rollback_restores_exact_disabled_prior_pin():
    provider = _Provider()
    previous = {
        "cohort_id": str(COHORT),
        "release_id": str(UUID(int=3)),
        "package_sha256": "2" * 64,
        "release_cutoff": CUTOFF - timedelta(minutes=1),
        "enabled": False,
        "enabled_at": NOW - timedelta(minutes=2),
        "disabled_at": NOW - timedelta(minutes=1),
        "updated_at": NOW - timedelta(minutes=1),
    }
    provider.cursor.pin = dict(previous)
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    port.enable(cohort_id=COHORT, release_id=RELEASE)
    restored = port.restore(
        cohort_id=COHORT,
        expected_release_id=RELEASE,
        previous=previous,
    )

    assert restored == previous
    assert provider.cursor.pin == previous


def test_read_pin_snapshot_captures_exact_prior_state():
    provider = _Provider()
    previous = {
        "cohort_id": str(COHORT),
        "release_id": str(UUID(int=3)),
        "package_sha256": "2" * 64,
        "release_cutoff": CUTOFF - timedelta(minutes=1),
        "enabled": False,
        "enabled_at": NOW - timedelta(minutes=2),
        "disabled_at": NOW - timedelta(minutes=1),
        "updated_at": NOW - timedelta(minutes=1),
    }
    provider.cursor.pin = dict(previous)
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    assert port.snapshot(cohort_id=COHORT) == previous


def test_read_pin_rollback_refuses_to_overwrite_a_newer_pin():
    provider = _Provider()
    provider.cursor.pin = {
        "cohort_id": str(COHORT),
        "release_id": str(RELEASE_2),
        "package_sha256": PACKAGE,
        "release_cutoff": CUTOFF + timedelta(minutes=5),
        "enabled": True,
        "enabled_at": NOW,
        "disabled_at": None,
        "updated_at": NOW,
    }
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    with pytest.raises(RuntimeError, match="changed before rollback"):
        port.restore(
            cohort_id=COHORT,
            expected_release_id=RELEASE,
            previous=None,
        )

    assert provider.cursor.pin["release_id"] == str(RELEASE_2)


def test_read_pin_advances_from_exact_prior_release():
    provider = _Provider()
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )
    port.enable(cohort_id=COHORT, release_id=RELEASE)
    previous = dict(provider.cursor.pin)
    provider.cursor.current_release_id = RELEASE_2
    provider.cursor.current_cutoff = CUTOFF + timedelta(minutes=5)

    advanced = port.advance(
        cohort_id=COHORT,
        release_id=RELEASE_2,
        previous=previous,
    )

    assert advanced["release_id"] == str(RELEASE_2)
    assert provider.cursor.pin["release_id"] == str(RELEASE_2)
    assert provider.cursor.pin["release_cutoff"] == (
        CUTOFF + timedelta(minutes=5)
    )
    assert provider.cursor.pin["enabled"] is True


def test_read_pin_advance_rejects_unexpected_prior_pin():
    provider = _Provider()
    provider.cursor.pin = {
        "cohort_id": str(COHORT),
        "release_id": str(RELEASE),
        "package_sha256": PACKAGE,
        "release_cutoff": CUTOFF,
        "enabled": True,
        "enabled_at": NOW,
        "disabled_at": None,
        "updated_at": NOW,
    }
    provider.cursor.current_release_id = RELEASE_2
    provider.cursor.current_cutoff = CUTOFF + timedelta(minutes=5)
    port = PostgresAdvancedOptionsStoredReadPort(
        provider,
        package_sha256=PACKAGE,
        clock=lambda: NOW,
    )

    with pytest.raises(RuntimeError, match="changed before advance"):
        port.advance(
            cohort_id=COHORT,
            release_id=RELEASE_2,
            previous=None,
        )


class _ReadCursor:
    def __init__(self, release_id):
        self.release_id = release_id
        self._row = None

    def execute(self, statement, _parameters=()):
        if statement.startswith(
            "SELECT cohort_id::text,release_id::text"
        ):
            self._row = (
                None
                if self.release_id is None
                else {
                    "cohort_id": str(COHORT),
                    "release_id": str(self.release_id),
                    "package_sha256": PACKAGE,
                    "release_cutoff": CUTOFF,
                    "enabled": True,
                    "enabled_at": NOW,
                    "disabled_at": None,
                    "updated_at": NOW,
                }
            )
        else:
            raise AssertionError(statement)

    def fetchone(self):
        return self._row


def _cursor_factory(release_id):
    @contextmanager
    def factory():
        yield _ReadCursor(release_id)

    return factory


def test_projection_reader_requires_and_passes_exact_pin(monkeypatch):
    absent = AdvancedOptionsStoredProjectionRepository(
        _cursor_factory(None),
        require_read_pin=True,
    )
    assert absent.read(
        cohort_id=COHORT,
        snapshot_type="OPTIONS_FLOW",
        available_by=NOW,
    )["reason"] == "ADVANCED_OPTIONS_READ_PIN_UNAVAILABLE"
    observed = {}

    def read_projection(_cursor, **kwargs):
        observed.update(kwargs)
        return {"available": True, "data": {}}

    monkeypatch.setattr(
        api_module,
        "read_advanced_options_projection_cursor",
        read_projection,
    )
    pinned = AdvancedOptionsStoredProjectionRepository(
        _cursor_factory(RELEASE),
        require_read_pin=True,
    )

    result = pinned.read(
        cohort_id=COHORT,
        snapshot_type="OPTIONS_FLOW",
        available_by=NOW,
    )

    assert result["available"] is True
    assert observed["release_id"] == RELEASE
    assert observed["release_cutoff"] == CUTOFF
