"""Regenerated alerts sit beside the originals, are labelled, and nothing can read them as live."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import canonical_stock_alert_regeneration as regeneration
from realtime.stock_alert_repository import RealtimeStockAlertViewRepository

from tests.realtime.test_canonical_stock_alert_product import (
    RECEIPT,
    WINDOW,
    _ready_context_inputs,
    produce_canonical_stock_alert_batch,
)

UTC = timezone.utc
NOW = datetime(2026, 11, 5, 9, 0, tzinfo=UTC)
COHORT = UUID("11111111-1111-5111-8111-111111111111")


def _matching_batch():
    inputs, _members, _events, _coverage = _ready_context_inputs()
    return produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
    )


def test_a_match_is_labelled_and_cannot_be_read_as_live():
    labelled = regeneration.label_regenerated_batch(_matching_batch(), NOW)

    alert = next(row.alert for row in labelled.evaluations if row.ticker == "AAPL")
    assert alert.lifecycle == "REGENERATED"
    projection = alert.projection
    assert projection["regenerated"] is True
    assert projection["regenerated_at"] == NOW.isoformat()
    assert projection["published_at"] == NOW.isoformat()
    assert projection["status"] == "REGENERATED"
    assert projection["deliverable"] is False
    assert projection["delivery_permission"] is False
    assert projection["execution_permission"] is False
    assert projection["publication"]["run_id"] == projection["run_id"]


def test_rows_that_did_not_match_store_no_alert():
    batch = _matching_batch()
    labelled = regeneration.label_regenerated_batch(batch, NOW)

    for original, row in zip(batch.evaluations, labelled.evaluations, strict=True):
        if original.decision.disposition.value != "MATCH":
            assert row.alert is None


def test_labelling_keeps_the_events_that_the_inputs_decided():
    batch = _matching_batch()
    labelled = regeneration.label_regenerated_batch(batch, NOW)

    assert [row.event.event_id for row in labelled.evaluations] == [
        row.event.event_id for row in batch.evaluations
    ]


class _Cursor:
    def __init__(self, count=0):
        self.count = count
        self.statements = []

    def execute(self, statement, values=None):
        self.statements.append(statement)

    def fetchone(self):
        return {"total": self.count}


class _Provider:
    def __init__(self, cursor):
        self.cursor = cursor

    @contextmanager
    def transaction(self, _role, **_kwargs):
        yield SimpleNamespace(cursor=self.cursor)


def _identity(monkeypatch):
    monkeypatch.setattr(
        regeneration,
        "load_portal_identity_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(session_cohorts={"2026-11-04": COHORT}),
    )


def test_a_session_regenerated_before_is_left_alone(monkeypatch):
    _identity(monkeypatch)

    report = regeneration.regenerate_session(
        _Provider(_Cursor(count=83)),
        session_date=date(2026, 11, 4),
        execute=True,
        clock=lambda: NOW,
    )

    assert report["status"] == "ALREADY_REGENERATED"
    assert report["alerts"] == 83


def test_a_session_still_open_is_refused():
    with pytest.raises(ValueError, match="closed session"):
        regeneration.regenerate_session(
            _Provider(_Cursor()),
            session_date=date(2026, 11, 5),
            execute=True,
            clock=lambda: NOW,
        )


def test_a_plan_rolls_back_and_reports():
    rolled_back = []

    class Provider(_Provider):
        @contextmanager
        def transaction(self, _role, **_kwargs):
            try:
                yield SimpleNamespace(cursor=self.cursor)
            except Exception:
                rolled_back.append(True)
                raise

    original_load = regeneration.load_session_receipts_cursor
    regeneration.load_session_receipts_cursor = lambda *_args, **_kwargs: ()
    original_identity = regeneration.load_portal_identity_cursor
    regeneration.load_portal_identity_cursor = lambda *_args, **_kwargs: SimpleNamespace(
        session_cohorts={"2026-11-04": COHORT},
    )
    try:
        report = regeneration.regenerate_session(
            Provider(_Cursor()),
            session_date=date(2026, 11, 4),
            execute=False,
            clock=lambda: NOW,
        )
    finally:
        regeneration.load_session_receipts_cursor = original_load
        regeneration.load_portal_identity_cursor = original_identity

    assert report["status"] == "PLANNED"
    assert rolled_back == [True]


class _ViewCursor:
    def __init__(self, rows):
        self.rows = rows
        self.values = None

    def execute(self, _statement, values=None):
        self.values = values

    def fetchall(self):
        return self.rows


def _view(rows):
    view = RealtimeStockAlertViewRepository.__new__(RealtimeStockAlertViewRepository)

    @contextmanager
    def cursor_context():
        yield _ViewCursor(rows)

    view._cursor = cursor_context
    view._begin = lambda _cursor: None
    return view


def test_the_page_shows_regenerated_alerts_with_a_note():
    projection = {
        "publication": {"run_id": "run-1", "session": "2026-11-04"},
        "regenerated_at": NOW.isoformat(),
        "run_id": "run-1",
        "ticker": "AAPL",
    }
    snapshot = {
        "alerts": [{"run_id": "original"}],
        "publications": [{"run_id": "original"}],
        "warnings": ["Canonical context unavailable: MISSING_SECTOR_CONTEXT"],
    }

    _view([{"alert_id": "alert-1", "projection": projection}])._add_regenerated_alerts(
        snapshot, NOW, COHORT, "2026-11-04",
    )

    assert [row["run_id"] for row in snapshot["publications"]] == ["original", "run-1"]
    assert snapshot["alerts"][-1]["alert_id"] == "alert-1"
    assert snapshot["alerts"][0] == {"run_id": "original"}
    assert len(snapshot["warnings"]) == 1
    assert "not published live" in snapshot["warnings"][0]
    assert "unavailable" in snapshot["warnings"][0]


def test_a_session_with_nothing_regenerated_is_unchanged():
    snapshot = {"alerts": [], "publications": [], "warnings": ["kept"]}

    _view([])._add_regenerated_alerts(snapshot, NOW, COHORT, "2026-11-04")

    assert snapshot == {"alerts": [], "publications": [], "warnings": ["kept"]}
