from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime import canonical_stock_alert_publication as publication_module
from realtime.canonical_stock_alert_publication import (
    load_due_canonical_stock_alert_publications_cursor,
    publish_due_canonical_stock_alerts,
)
from realtime.portal_publisher import PortalPublicationRetryable


UTC = timezone.utc
SESSION = date(2026, 11, 3)
CUTOFF = datetime(2026, 11, 3, 16, 0, tzinfo=UTC)
COHORT = uuid5(NAMESPACE_URL, "publication-cohort")
WINDOW = uuid5(NAMESPACE_URL, "publication-window")
RECEIPT = uuid5(NAMESPACE_URL, "publication-receipt")
RECEIPT_CREATED = CUTOFF + timedelta(seconds=30)
COMPLETED = CUTOFF + timedelta(seconds=40)
CREATED = COMPLETED + timedelta(milliseconds=5)
NOW = CUTOFF + timedelta(minutes=1)


def _due(cutoff=CUTOFF):
    return {
        "window_id": str(WINDOW),
        "input_receipt_id": str(RECEIPT),
        "cutoff": cutoff,
        "receipt_input_sha256": "b" * 64,
        "completed_at": COMPLETED,
        "created_at": CREATED,
        "reference_generation": 3,
        "universe_sha256": "a" * 64,
        "provider_tier": "MASSIVE_ADVANCED",
        "receipt_created_at": RECEIPT_CREATED,
    }


def _provider(rows):
    class Cursor:
        def __init__(self):
            self.statement = None

        def execute(self, statement, _parameters=None):
            self.statement = statement

        def fetchall(self):
            return rows

    cursor = Cursor()

    @contextmanager
    def transaction(*_args, **kwargs):
        assert kwargs["read_only"] is True
        yield SimpleNamespace(cursor=cursor)

    return SimpleNamespace(transaction=transaction)


def _publish(monkeypatch, rows, *, publish_snapshot):
    projections = []

    def load_projection(_cursor, request):
        projections.append(request)
        return {"batch": str(request.input_receipt_id)}

    monkeypatch.setattr(
        publication_module,
        "load_canonical_stock_alert_projection_cursor",
        load_projection,
    )
    computed = []

    def compute_snapshot(database, **scope):
        computed.append((database, scope))
        return "snapshot"

    reports = publish_due_canonical_stock_alerts(
        _provider(rows),
        compute="compute",
        publication="publication",
        cohort_id=COHORT,
        session_date=SESSION,
        clock=lambda: NOW,
        compute_snapshot=compute_snapshot,
        publish_snapshot=publish_snapshot,
    )
    return reports, projections, computed


def test_nothing_due_publishes_nothing(monkeypatch):
    reports, projections, computed = _publish(
        monkeypatch,
        [],
        publish_snapshot=lambda *_args, **_kwargs: pytest.fail("published"),
    )

    assert reports == ()
    assert projections == []
    assert computed == []


def test_snapshot_reads_the_inputs_the_batch_was_built_from(monkeypatch):
    published = []
    reports, projections, computed = _publish(
        monkeypatch,
        [_due()],
        publish_snapshot=lambda database, snapshot, **values: published.append(
            (database, snapshot, values),
        ),
    )

    assert projections[0].known_by == CREATED
    assert projections[0].window_id == WINDOW
    assert computed[0][0] == "compute"
    assert computed[0][1]["known_by"] == RECEIPT_CREATED
    assert computed[0][1]["source_generation"] == 3
    assert computed[0][1]["retained_projection"] == {"batch": str(RECEIPT)}
    database, snapshot, values = published[0]
    assert (database, snapshot) == ("publication", "snapshot")
    assert values["receipt"].receipt_id == RECEIPT
    assert values["receipt"].source_generation == 3
    assert values["session_close"] is False
    assert values["checked_at"] == NOW
    assert reports == ({
        "input_receipt_id": str(RECEIPT),
        "cutoff": CUTOFF.isoformat(),
        "status": "PUBLISHED",
    },)


def test_final_cutoff_publishes_the_session_close_release(monkeypatch):
    published = []
    close = datetime(2026, 11, 3, 21, 0, tzinfo=UTC)
    monkeypatch.setattr(
        publication_module.OptionExchangeCalendar,
        "session_close",
        lambda _calendar, _session: close,
    )

    _publish(
        monkeypatch,
        [{
            **_due(close),
            "receipt_created_at": close + timedelta(seconds=30),
            "completed_at": close + timedelta(seconds=40),
            "created_at": close + timedelta(seconds=41),
        }],
        publish_snapshot=lambda _database, _snapshot, **values: published.append(values),
    )

    assert published[0]["session_close"] is True
    assert published[0]["checked_at"] == close


def test_retryable_publication_is_left_to_durable_c7_recovery(monkeypatch):
    def publish_snapshot(*_args, **_kwargs):
        raise PortalPublicationRetryable(
            RECEIPT,
            NOW + timedelta(seconds=30),
            "PORTAL_PUBLICATION_RETRY",
        )

    reports, _projections, _computed = _publish(
        monkeypatch,
        [_due()],
        publish_snapshot=publish_snapshot,
    )

    assert reports[0]["status"] == "DEFERRED:PORTAL_PUBLICATION_RETRY"


def test_due_query_is_newest_first_and_skips_handed_off_batches():
    cursor = _provider([_due()]).transaction
    statements = []

    class Cursor:
        def execute(self, statement, parameters):
            statements.append((statement, parameters))

        def fetchall(self):
            return [tuple(_due().values())]

    rows = load_due_canonical_stock_alert_publications_cursor(
        Cursor(),
        cohort_id=COHORT,
        session_date=SESSION,
    )

    assert cursor is not None
    assert rows[0]["input_receipt_id"] == str(RECEIPT)
    assert "ORDER BY batch.cutoff DESC" in statements[0][0]
    assert "product_key='STOCK_ALERTS'" in statements[0][0]
    assert statements[0][1] == (str(COHORT), SESSION, 1)
