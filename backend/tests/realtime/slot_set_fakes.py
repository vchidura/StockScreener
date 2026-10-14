"""In-memory stand-in for rt.slot_set_* and the rows the slot-set identity check reads (offline tests only).

It mirrors the statements of option_slot_set_repository, including the guarded pointer upsert, so the Python
rules (divergent replay, contiguity, make-before-break, never backwards) are exercised without a database.
The DB-backed variants live in test_option_slot_sets_v2_db.py.
"""
from __future__ import annotations


class FakeSlotSetStore:
    def __init__(self):
        self.versions: dict[tuple[str, int], tuple] = {}
        self.members: dict[tuple[str, int], list[tuple]] = {}
        self.current: dict[str, tuple] = {}
        self.cohorts: dict[str, tuple[str, set[str]]] = {}
        self.instruments: dict[int, tuple[str, int | None]] = {}
        self.catalog: dict[int, tuple[int, str]] = {}
        self.statements: list[str] = []

    def register_cohort(self, cohort_id, manifest_kind: str, underlying_security_ids) -> None:
        self.cohorts[str(cohort_id)] = (manifest_kind, {str(value) for value in underlying_security_ids})

    def register_option(self, instrument_id: int, contract_id: int, version_id: int, underlying_security_id) -> None:
        self.instruments[instrument_id] = ("OPTION", contract_id)
        self.catalog[version_id] = (contract_id, str(underlying_security_id))

    def handles(self, statement: str) -> bool:
        return "rt.slot_set_" in statement or statement.startswith(self._IDENTITY_PREFIXES)

    _IDENTITY_PREFIXES = (
        "SELECT manifest.manifest_kind",
        "SELECT instrument_id, asset_class, contract_id FROM rt.instruments",
        "SELECT version.version_id",
    )

    def execute(self, statement: str, parameters):
        """Return (fetchone, fetchall) for the statement."""
        self.statements.append(statement)
        if statement.startswith("SELECT kind, effective_at"):
            return self.versions.get((parameters[0], parameters[1])), []
        if statement.startswith("SELECT instrument_id, channel, purpose"):
            return None, list(self.members.get((parameters[0], parameters[1]), []))
        if statement.startswith("SELECT manifest.manifest_kind"):
            entry = self.cohorts.get(parameters[0])
            if entry is None:
                return None, []
            kind, securities = entry
            return None, [(kind, value) for value in sorted(securities)] or [(kind, None)]
        if statement.startswith("SELECT instrument_id, asset_class, contract_id FROM rt.instruments"):
            return None, [
                (instrument_id, *self.instruments[instrument_id])
                for instrument_id in parameters[0] if instrument_id in self.instruments
            ]
        if statement.startswith("SELECT version.version_id"):
            return None, [
                (version_id, *self.catalog[version_id])
                for version_id in parameters[0] if version_id in self.catalog
            ]
        if statement.startswith("SELECT COALESCE(max(version), 0)"):
            return (max((v for c, v in self.versions if c == parameters[0]), default=0),), []
        if statement.startswith("INSERT INTO rt.slot_set_versions"):
            cohort, version, kind, effective_at, policy, member_sha, primary, reserve = parameters
            self.versions.setdefault((cohort, version), (kind, effective_at, policy, member_sha, primary, reserve))
            return None, []
        if statement.startswith("INSERT INTO rt.slot_set_members"):
            rows = self.members
            for offset in range(0, len(parameters), 7):
                cohort, version, instrument_id, channel, purpose, priority, catalog_version_id = parameters[offset:offset + 7]
                if (cohort, version) not in self.versions:
                    raise AssertionError("slot set member inserted before its version")
                bucket = rows.setdefault((cohort, version), [])
                if not any(row[0] == instrument_id and row[1] == channel for row in bucket):
                    bucket.append((instrument_id, channel, purpose, priority, catalog_version_id))
            return None, []
        if statement.startswith("INSERT INTO rt.slot_set_current"):
            cohort, version, activated_at = parameters
            row = self.current.get(cohort)
            if row is None or (row[0] < version and row[1] <= activated_at):
                self.current[cohort] = (version, activated_at)
                return (version,), []
            return None, []
        if statement.startswith("SELECT version, activated_at FROM rt.slot_set_current"):
            return self.current.get(parameters[0]), []
        raise AssertionError(f"unexpected slot set statement: {statement[:60]}")


class SlotSetCursor:
    def __init__(self, store: FakeSlotSetStore):
        self.store = store
        self._one = None
        self._many: list = []

    def execute(self, statement, parameters=None):
        self._one, self._many = self.store.execute(statement, parameters)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many
