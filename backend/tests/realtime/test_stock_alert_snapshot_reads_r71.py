"""R71 (Nov 15): Stock Alert batches build in a READ ONLY snapshot; SERIALIZABLE only claims and writes."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from uuid import UUID

import pytest

from scripts import run_realtime_v2_canonical_stock_alerts as cli


COHORT = UUID("11111111-2222-3333-4444-555555555555")
SESSION = date(2026, 11, 13)
RECEIPTS = (
    {"receipt_id": "00000000-0000-4000-8000-000000000001", "cutoff": 1},
    {"receipt_id": "00000000-0000-4000-8000-000000000002", "cutoff": 2},
)


class Provider:
    def __init__(self):
        self.transactions = []
        self.current = None

    @contextmanager
    def transaction(self, role, *, read_only=False, isolation_level=None, dict_cursor=False, fence=None):
        record = {
            "read_only": read_only,
            "isolation": isolation_level,
            "calls": [],
            "committed": False,
        }
        self.transactions.append(record)
        self.current = record
        yield SimpleNamespace(cursor=record)
        record["committed"] = True
        self.current = None


@pytest.fixture
def world(monkeypatch):
    state = {
        "fingerprints": ["state-0", "state-0", "state-1", "state-1"],
        "claimable": {item["receipt_id"]: True for item in RECEIPTS},
        "persisted": [],
    }

    def call(cursor, name, *args):
        cursor["calls"].append((name, *args))

    def list_due(cursor, *, cohort_id, session_date, limit):
        call(cursor, "list")
        return RECEIPTS

    def load_due(cursor, *, cohort_id, session_date, limit):
        call(cursor, "load_locked")
        return RECEIPTS

    def claim(cursor, *, receipt_id, cohort_id, session_date):
        call(cursor, "claim", receipt_id)
        return {"receipt_id": receipt_id} if state["claimable"][receipt_id] else None

    def build(cursor, receipt, *, session_date):
        call(cursor, "build", receipt["receipt_id"])
        return SimpleNamespace(
            receipt_id=receipt["receipt_id"],
            cohort_id=COHORT,
            version=SimpleNamespace(detector_sha256="d" * 64),
        )

    def fingerprint(cursor, *, detector_sha256, cohort_id):
        call(cursor, "fingerprint")
        return state["fingerprints"].pop(0)

    def persist(cursor, batch):
        call(cursor, "persist", batch.receipt_id)
        state["persisted"].append(batch.receipt_id)
        return {"input_receipt_id": batch.receipt_id}

    monkeypatch.setattr(cli, "list_due_canonical_stock_alert_receipts_cursor", list_due)
    monkeypatch.setattr(cli, "load_due_canonical_stock_alert_receipts_cursor", load_due)
    monkeypatch.setattr(cli, "claim_canonical_stock_alert_receipt_cursor", claim)
    monkeypatch.setattr(cli, "build_canonical_stock_alert_batch_cursor", build)
    monkeypatch.setattr(cli, "canonical_stock_alert_state_fingerprint_cursor", fingerprint)
    monkeypatch.setattr(cli, "persist_canonical_stock_alert_batch_cursor", persist)
    return state


def _run(provider, reads=cli.BATCH_READS_SNAPSHOT):
    return cli.run_due_batches(
        provider,
        cohort_id=COHORT,
        session_date=SESSION,
        claim_limit=8,
        reads=reads,
    )


def test_snapshot_builds_read_only_and_writes_serializable_in_cutoff_order(world):
    provider = Provider()

    reports = _run(provider)

    assert [item["input_receipt_id"] for item in reports] == [r["receipt_id"] for r in RECEIPTS]
    shapes = [(t["isolation"], t["read_only"], tuple(c[0] for c in t["calls"])) for t in provider.transactions]
    assert shapes == [
        ("REPEATABLE READ", True, ("list",)),
        (None, False, ("claim",)),
        ("REPEATABLE READ", True, ("build", "fingerprint")),
        ("SERIALIZABLE", False, ("claim", "fingerprint", "persist")),
        (None, False, ("claim",)),
        ("REPEATABLE READ", True, ("build", "fingerprint")),
        ("SERIALIZABLE", False, ("claim", "fingerprint", "persist")),
    ]
    # Nothing is built or read for the detector inside SERIALIZABLE: only the claim, the state check and the write.
    assert all(
        "build" not in (c[0] for c in t["calls"])
        for t in provider.transactions
        if t["isolation"] == "SERIALIZABLE"
    )


def test_state_changed_after_the_snapshot_is_never_written(world):
    world["fingerprints"] = ["state-0", "state-CHANGED"]
    provider = Provider()

    with pytest.raises(cli.StockAlertBatchInputsChanged) as raised:
        _run(provider)

    assert world["persisted"] == []
    assert cli.should_retry_conflict(raised.value, 1)
    assert provider.transactions[-1]["committed"] is False


def test_a_receipt_written_meanwhile_stops_the_pass(world):
    world["claimable"][RECEIPTS[0]["receipt_id"]] = False
    provider = Provider()

    assert _run(provider) == []
    assert world["persisted"] == []
    # The probe saw it held: no build was spent on it, and the later receipt waits for the next poll.
    assert [tuple(c[0] for c in t["calls"]) for t in provider.transactions] == [("list",), ("claim",)]


def test_serializable_switch_keeps_the_r102_single_transaction(world):
    provider = Provider()

    reports = _run(provider, reads=cli.BATCH_READS_SERIALIZABLE)

    assert len(reports) == 2
    assert len(provider.transactions) == 1
    only = provider.transactions[0]
    assert only["isolation"] == "SERIALIZABLE" and only["read_only"] is False
    assert [c[0] for c in only["calls"]] == ["load_locked", "build", "persist", "build", "persist"]


def test_batch_reads_switch_values():
    assert cli.batch_reads({}) == cli.BATCH_READS_SNAPSHOT
    assert cli.batch_reads({cli.STOCK_ALERT_BATCH_READS_ENV: " serializable "}) == cli.BATCH_READS_SERIALIZABLE
    assert cli.batch_reads({cli.STOCK_ALERT_BATCH_READS_ENV: ""}) == cli.BATCH_READS_SNAPSHOT
    with pytest.raises(ValueError, match=cli.STOCK_ALERT_BATCH_READS_ENV):
        cli.batch_reads({cli.STOCK_ALERT_BATCH_READS_ENV: "READ_COMMITTED"})


def test_rule_module_is_not_part_of_this_change():
    """The detector version hashes the research rule sources (memory stock-alerts-version-reset); R71 lives in the
    runner and the repository, which are not hashed."""
    from realtime import canonical_stock_alert_research as research
    from realtime import canonical_stock_alert_product as product

    hashed = {path.name for path in (*research._CODE_PATHS, *product._CODE_PATHS)}
    assert "canonical_stock_alert_repository.py" not in hashed
    assert "run_realtime_v2_canonical_stock_alerts.py" not in hashed
