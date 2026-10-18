from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from realtime.contracts import canonical_sha256
from realtime.reference_continuity import (
    reference_continuity_manifest_fields,
    source_manifest_identity_cursor,
)


CUTOFF = datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc)
KNOWN_BY = datetime(2026, 10, 18, 1, 11, tzinfo=timezone.utc)


class _Cursor:
    def __init__(self, members, prior, latest):
        self.members = members
        self.rows = [prior, latest, {"count": 0}]
        self.statements = []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchall(self):
        return self.members

    def fetchone(self):
        return self.rows.pop(0)


def _fixture():
    security_id = uuid4()
    prior_id = uuid4()
    latest_id = uuid4()
    prior = {
        "revision_id": prior_id,
        "security_revision_id": prior_id,
        "security_id": security_id,
        "ticker": "SPY",
        "payload_sha256": "a" * 64,
        "observed_at": CUTOFF,
        "effective_from": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "effective_to": datetime(2026, 10, 5, tzinfo=timezone.utc),
        "active": False,
    }
    latest = {
        "revision_id": latest_id,
        "security_revision_id": latest_id,
        "security_id": security_id,
        "ticker": "SPY",
        "payload_sha256": "b" * 64,
        "observed_at": KNOWN_BY,
        "effective_from": KNOWN_BY,
        "effective_to": None,
        "active": True,
    }
    evidence = ((
        1,
        "SPY",
        "LATE_OBSERVED_CONTINUITY",
        str(prior_id),
        prior["payload_sha256"],
        prior["observed_at"],
        prior["effective_from"],
        prior["effective_to"],
        str(latest_id),
        latest["payload_sha256"],
        latest["observed_at"],
        latest["effective_from"],
        latest["effective_to"],
    ),)
    return security_id, prior, latest, evidence


def test_reference_continuity_manifest_carries_digest_and_visibility_clock():
    inputs = SimpleNamespace(
        identity_continuity_sha256="c" * 64,
        recorded_by=KNOWN_BY,
    )

    assert reference_continuity_manifest_fields(inputs) == {
        "identity_continuity_sha256": "c" * 64,
        "known_by": KNOWN_BY.isoformat(),
    }


def test_source_manifest_identity_recomputes_late_continuity_evidence():
    security_id, prior, latest, evidence = _fixture()
    manifest_sha256 = "d" * 64
    manifest = {
        "identity_continuity_sha256": canonical_sha256(evidence),
        "known_by": KNOWN_BY.isoformat(),
    }
    cursor = _Cursor(
        [{"instrument_id": 1, "security_id": security_id}],
        prior,
        latest,
    )

    identity, rows = source_manifest_identity_cursor(
        cursor,
        manifest,
        manifest_id=uuid4(),
        cohort_manifest_sha256=manifest_sha256,
        history_binding_sha256=None,
        cutoff=CUTOFF,
    )

    assert identity == canonical_sha256({
        "manifest_sha256": manifest_sha256,
        "history_binding_sha256": None,
        "identity_continuity": evidence,
    })
    assert rows[0]["revision_id"] == latest["revision_id"]


def test_source_manifest_identity_rejects_a_changed_continuity_digest():
    security_id, prior, latest, _ = _fixture()
    cursor = _Cursor(
        [{"instrument_id": 1, "security_id": security_id}],
        prior,
        latest,
    )

    with pytest.raises(ValueError, match="digest changed"):
        source_manifest_identity_cursor(
            cursor,
            {
                "identity_continuity_sha256": "0" * 64,
                "known_by": KNOWN_BY.isoformat(),
            },
            manifest_id=uuid4(),
            cohort_manifest_sha256="d" * 64,
            history_binding_sha256=None,
            cutoff=CUTOFF,
        )
