"""In-memory answers for the V2 portal catalog, hot and kept bodies, releases and pointer."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from realtime.contracts import canonical_sha256
from realtime.portal_compatibility_repository import RELEASE_MANIFEST_VERSION


def snapshot_entry(snapshot, snapshot_class="EPHEMERAL_PORTAL") -> dict:
    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "snapshot_class": snapshot_class,
        "snapshot_type": snapshot.snapshot_type,
        "snapshot_key": snapshot.snapshot_key,
        "source_generation": snapshot.source_generation,
        "generated_at": snapshot.generated_at.astimezone(timezone.utc).isoformat(),
    }


def release_rows(release_id, cohort_id, entries, *, cutoff, session_date, release_class="INTRADAY",
                 provider_tier="MASSIVE_DEVELOPER"):
    """The pointer row and the release row a reader sees for one manifest."""
    manifest = {
        "version": RELEASE_MANIFEST_VERSION,
        "release_class": release_class,
        "session_date": str(session_date),
        "snapshots": sorted(entries, key=lambda item: (item["snapshot_type"], item["snapshot_key"])),
    }
    pointer = {"release_id": release_id, "cutoff": cutoff, "session_date": session_date,
               "provider_tier": provider_tier}
    release = {
        "release_id": release_id, "cohort_id": cohort_id, "provider_tier": provider_tier,
        "release_class": release_class, "session_date": session_date, "cutoff": cutoff,
        "manifest": manifest, "manifest_sha256": canonical_sha256(manifest),
    }
    return pointer, release


class PortalV2Statements:
    """Mixin for fake cursors: answers the portal statements from in-memory state.

    `snapshots` is the catalog joined with its body, `current` the per-identity pointer the
    fake turns into a synthesized release when the reader asks for it."""

    def init_portal(self):
        self.snapshots, self.current, self._releases = {}, {}, {}

    def expire(self, snapshot_id):
        """The hot partition left its window: catalog row and body are gone."""
        self.snapshots.pop(str(snapshot_id), None)

    def advance(self, snapshot):
        key = (str(snapshot.cohort_id), snapshot.snapshot_type, snapshot.snapshot_key)
        incoming = self.snapshots[str(snapshot.snapshot_id)]
        previous = self.snapshots.get(self.current.get(key))
        if previous is None or (incoming["source_generation"], incoming["generated_at"]) > (
                previous["source_generation"], previous["generated_at"]):
            self.current[key] = str(snapshot.snapshot_id)

    def _synthesized_release(self, cohort_id):
        entries = [
            {"snapshot_id": identity, "snapshot_class": row["snapshot_class"],
             "snapshot_type": row["snapshot_type"], "snapshot_key": row["snapshot_key"],
             "source_generation": row["source_generation"],
             "generated_at": row["generated_at"].astimezone(timezone.utc).isoformat()}
            for (cohort, _, _), identity in sorted(self.current.items())
            if cohort == cohort_id and (row := self.snapshots.get(identity)) is not None
        ]
        if not entries:
            return None
        cutoff = max(self.snapshots[entry["snapshot_id"]]["generated_at"] for entry in entries)
        draft = release_rows(None, cohort_id, entries, cutoff=cutoff, session_date=cutoff.date())[1]
        release_id = str(uuid5(NAMESPACE_URL, "fake-release:" + draft["manifest_sha256"]))
        pointer, release = release_rows(release_id, cohort_id, entries, cutoff=cutoff, session_date=cutoff.date())
        self._releases[release_id] = release
        return pointer

    def portal_execute(self, sql, params) -> bool:
        if sql.startswith("INSERT INTO rt.snapshot_catalog"):
            keys = ("snapshot_id", "cohort_id", "provider_tier", "snapshot_class", "snapshot_type",
                    "snapshot_key", "session_date", "source_generation", "source_manifest_sha256",
                    "payload_sha256", "generated_at", "valid_until")
            self.snapshots.setdefault(params[0], dict(zip(keys, params)))
        elif sql.startswith("SELECT snapshot_id, cohort_id, provider_tier, snapshot_class"):
            row = self.snapshots[params[0]]
            self.result = [dict(row, manifest_sha256=row["source_manifest_sha256"])]
        elif sql.startswith("INSERT INTO rt.portal_snapshot_hot"):
            row = self.snapshots[params[0]]
            row["source_manifest"], row["payload"] = json.loads(params[5]), json.loads(params[6])
        elif sql.startswith("INSERT INTO rt.portal_snapshot_kept"):
            row = self.snapshots[params[0]]
            row["source_manifest"], row["payload"] = json.loads(params[5]), json.loads(params[6])
        elif sql.startswith("SELECT snapshot_id, cohort_id, snapshot_type, snapshot_key FROM rt.snapshot_catalog"):
            row = self.snapshots.get(params[0])
            self.result = [row] if row is not None else []
        elif sql.startswith("SELECT release_id, cutoff, session_date, provider_tier FROM rt.portal_release_current"):
            pointer = self._synthesized_release(params[0])
            self.result = [pointer] if pointer is not None else []
        elif sql.startswith("SELECT release_id, cohort_id, provider_tier, release_class, session_date, cutoff"):
            self.result = [self._releases[params[0]]]
        else:
            return False
        if not sql.startswith("SELECT"):
            self.result = []
        return True


def aware(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def release_id_for(value: str) -> UUID:
    return uuid5(NAMESPACE_URL, value)


@contextmanager
def uncapped_hot_manifest_for_kept_only_snapshots():
    """Strategy snapshots are persisted KEPT-only in production (no 16 KiB hot cap applies to kept
    bodies) but the strategy test store serves them through the hot path. Only that store may use
    this; every other snapshot type is published through the real cap."""
    import realtime.portal_projection_repository as repository

    original = repository.HOT_MANIFEST_LIMIT_BYTES
    repository.HOT_MANIFEST_LIMIT_BYTES = 1 << 40
    try:
        yield
    finally:
        repository.HOT_MANIFEST_LIMIT_BYTES = original
