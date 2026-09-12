from contextlib import contextmanager
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from options.domain import AssetType, DecisionContext
from options.event_calendar import parse_event_calendar_document
from options.errors import DuplicateFactConflict
from options.repositories.market_events import OptionMarketEventRepository
from options.repositories.strategies import OptionStrategyRepository
from options.strategies.context import OptionStrategyContextRepository
from database import get_db_connection
from scripts import import_option_event_calendar


UTC = timezone.utc
MARKET_TIME = datetime(2026, 9, 16, 18, 0, tzinfo=UTC)
OBSERVED_TIME = MARKET_TIME + timedelta(minutes=15)
RECEIVED_AT = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def _document():
    return {
        "schema_version": 1,
        "source": "Verified_Calendar",
        "observed_at": "2026-09-10T12:00:00Z",
        "coverage": [
            {
                "source_key": "aapl-q3-window",
                "event_type": "EARNINGS",
                "affected_underlying": "aapl",
                "window_start": "2026-09-01T00:00:00Z",
                "window_end": "2026-10-01T00:00:00Z",
            },
            {
                "source_key": "fed-september-window",
                "event_type": "FED_RATE_DECISION",
                "affected_underlying": None,
                "window_start": "2026-09-01T00:00:00Z",
                "window_end": "2026-10-01T00:00:00Z",
            },
        ],
        "events": [
            {
                "source_key": "aapl-q3",
                "event_type": "EARNINGS",
                "affected_underlying": "AAPL",
                "scheduled_time": "2026-09-17T20:05:00Z",
                "announcement_time": "2026-09-01T14:00:00Z",
                "confidence": "CONFIRMED",
                "status": "SCHEDULED",
            }
        ],
    }


def _batch(payload=None):
    return parse_event_calendar_document(
        payload or _document(), received_at=RECEIVED_AT
    )


def _bars(count, interval):
    return [
        {
            "datetime": MARKET_TIME - interval * (count - index),
            "close_price": Decimal("100") + index,
        }
        for index in range(count)
    ]


def _repository(fetches):
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchall.side_effect = fetches
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return OptionStrategyContextRepository(factory), cursor


def test_event_calendar_document_is_typed_normalized_and_deterministic():
    first = _batch()
    repeated = _batch()

    assert first == repeated
    assert first.source == "verified_calendar"
    assert first.coverage[0].affected_underlying == "AAPL"
    assert first.events[0].scheduled_time.tzinfo is UTC
    assert first.events[0].source_observed_at == datetime(
        2026, 9, 10, 12, 0, tzinfo=UTC
    )
    assert first.events[0].first_observed_at == RECEIVED_AT
    assert len(first.events[0].payload_sha256) == 64


def test_event_calendar_rejects_invalid_event_scope():
    payload = _document()
    payload["events"][0]["affected_underlying"] = None

    with pytest.raises(ValueError, match="earnings facts require"):
        _batch(payload)


def test_event_calendar_rejects_duplicate_keys_within_one_document():
    payload = _document()
    payload["coverage"].append(dict(payload["coverage"][0]))

    with pytest.raises(ValueError, match="coverage source_key values must be unique"):
        _batch(payload)


def test_configured_calendar_without_coverage_stays_unavailable():
    repository, _ = _repository([
        _bars(100, timedelta(days=1)),
        _bars(20, timedelta(hours=1)),
        [],
        [],
    ])

    context = repository.build(
        uuid4(), "AAPL", AssetType.STOCK,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        event_calendar_provider="verified_calendar",
        policy_version="phase2_v4", policy_sha256="a" * 64,
    )

    assert context.earnings_blackout_state == "UNAVAILABLE"
    assert context.fed_blackout_state == "UNAVAILABLE"
    assert "EVENT_CALENDAR_UNAVAILABLE" in context.reason_codes


def test_covered_calendar_can_report_blocked_and_clear_states():
    earnings_coverage_id = uuid4()
    fed_coverage_id = uuid4()
    earnings_event_id = uuid4()
    repository, cursor = _repository([
        _bars(100, timedelta(days=1)),
        _bars(20, timedelta(hours=1)),
        [
            {
                "coverage_id": earnings_coverage_id,
                "event_type": "EARNINGS",
                "affected_underlying": "AAPL",
            },
            {
                "coverage_id": fed_coverage_id,
                "event_type": "FED_RATE_DECISION",
                "affected_underlying": None,
            },
        ],
        [
            {
                "event_type": "EARNINGS",
                "market_event_id": earnings_event_id,
                "affected_underlying": "AAPL",
                "scheduled_time": MARKET_TIME + timedelta(hours=24),
                "status": "SCHEDULED",
            }
        ],
    ])

    context = repository.build(
        uuid4(), "AAPL", AssetType.STOCK,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        event_calendar_provider="verified_calendar",
        policy_version="phase2_v4", policy_sha256="a" * 64,
    )

    assert context.earnings_blackout_state == "BLOCKED"
    assert context.fed_blackout_state == "CLEAR"
    assert "EARNINGS_BLACKOUT" in context.reason_codes
    assert "EVENT_CALENDAR_UNAVAILABLE" not in context.reason_codes
    assert context.market_event_ids == (earnings_event_id,)
    assert context.event_coverage_ids == tuple(sorted(
        (earnings_coverage_id, fed_coverage_id), key=str
    ))
    event_sql, event_parameters = cursor.execute.call_args_list[3].args
    assert "WHERE source = %s" in event_sql
    assert event_parameters[0] == "verified_calendar"


def test_canceled_event_revision_is_lineage_but_not_blackout():
    earnings_coverage_id = uuid4()
    fed_coverage_id = uuid4()
    canceled_event_id = uuid4()
    repository, _ = _repository([
        _bars(100, timedelta(days=1)),
        _bars(20, timedelta(hours=1)),
        [
            {
                "coverage_id": earnings_coverage_id,
                "event_type": "EARNINGS",
                "affected_underlying": "AAPL",
            },
            {
                "coverage_id": fed_coverage_id,
                "event_type": "FED_RATE_DECISION",
                "affected_underlying": None,
            },
        ],
        [{
            "market_event_id": canceled_event_id,
            "event_type": "EARNINGS",
            "affected_underlying": "AAPL",
            "scheduled_time": MARKET_TIME + timedelta(hours=24),
            "status": "CANCELED",
        }],
    ])

    context = repository.build(
        uuid4(), "AAPL", AssetType.STOCK,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        event_calendar_provider="verified_calendar",
        policy_version="phase2_v4", policy_sha256="a" * 64,
    )

    assert context.earnings_blackout_state == "CLEAR"
    assert context.fed_blackout_state == "CLEAR"
    assert context.market_event_ids == (canceled_event_id,)


def test_event_repository_is_idempotent_for_identical_facts():
    batch = _batch()
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.side_effect = [
        {"coverage_id": batch.coverage[0].coverage_id},
        {"coverage_id": batch.coverage[1].coverage_id},
        {"market_event_id": batch.events[0].market_event_id},
    ]
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    result = OptionMarketEventRepository(factory).persist_batch(
        batch.coverage, batch.events
    )

    assert result.coverage_inserted == 2
    assert result.events_inserted == 1
    assert connection.commit.call_count == 1


def test_event_repository_is_idempotent_against_postgres_and_rejects_conflict():
    batch = _batch()

    class NoCommitConnection:
        def __init__(self, connection):
            self.connection = connection

        @property
        def closed(self):
            return self.connection.closed

        def cursor(self, *args, **kwargs):
            return self.connection.cursor(*args, **kwargs)

        def commit(self):
            pass

        def rollback(self):
            self.connection.rollback()

    with get_db_connection() as connection:
        connection.rollback()

        @contextmanager
        def factory():
            yield NoCommitConnection(connection)

        repository = OptionMarketEventRepository(factory)
        first = repository.persist_batch(batch.coverage, batch.events)
        repeated = repository.persist_batch(batch.coverage, batch.events)

        assert first.coverage_inserted == 2
        assert first.events_inserted == 1
        assert repeated.coverage_inserted == 0
        assert repeated.events_inserted == 0

        conflict = batch.coverage[0].__class__(
            coverage_id=uuid4(),
            event_type=batch.coverage[0].event_type,
            affected_underlying=batch.coverage[0].affected_underlying,
            window_start=batch.coverage[0].window_start,
            window_end=batch.coverage[0].window_end + timedelta(days=1),
            source=batch.coverage[0].source,
            source_key=batch.coverage[0].source_key,
            first_observed_at=batch.coverage[0].first_observed_at,
            source_observed_at=batch.coverage[0].source_observed_at,
            payload_sha256="b" * 64,
        )
        with pytest.raises(DuplicateFactConflict):
            repository.persist_batch((conflict,), ())
        connection.rollback()


def test_event_calendar_cli_defaults_to_dry_run(tmp_path, monkeypatch, capsys):
    document = tmp_path / "events.json"
    document.write_text(json.dumps(_document()), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["import_option_event_calendar.py", str(document)])

    assert import_option_event_calendar.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "DRY_RUN"
    assert payload["coverage_count"] == 2
    assert payload["event_count"] == 1


def test_event_lineage_links_round_trip_against_postgres():
    batch = _batch()
    with get_db_connection() as connection:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT context_snapshot_id
                FROM option_context_snapshots
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
        if row is None:
            pytest.skip("no persisted option context available for lineage FK test")
        context_snapshot_id = row[0]

        class NoCommitConnection:
            @property
            def closed(self):
                return connection.closed

            def cursor(self, *args, **kwargs):
                return connection.cursor(*args, **kwargs)

            def commit(self):
                pass

            def rollback(self):
                connection.rollback()

        @contextmanager
        def factory():
            yield NoCommitConnection()

        OptionMarketEventRepository(factory).persist_batch(
            batch.coverage, batch.events
        )
        lineage_context = type("LineageContext", (), {
            "context_snapshot_id": context_snapshot_id,
            "market_event_ids": tuple(item.market_event_id for item in batch.events),
            "event_coverage_ids": tuple(item.coverage_id for item in batch.coverage),
        })()
        with connection.cursor() as cursor:
            OptionStrategyRepository._persist_event_lineage(
                cursor, lineage_context
            )
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                                         FROM option_context_market_event_evidence AS link
                                         JOIN option_market_events AS event
                                             USING (market_event_id)
                                         WHERE link.context_snapshot_id = %s
                                             AND event.source = 'verified_calendar'),
                    (SELECT COUNT(*)
                                         FROM option_context_event_coverage_evidence AS link
                                         JOIN option_event_calendar_coverage AS coverage
                                             USING (coverage_id)
                                         WHERE link.context_snapshot_id = %s
                                             AND coverage.source = 'verified_calendar')
                """,
                (context_snapshot_id, context_snapshot_id),
            )
            assert cursor.fetchone() == (len(batch.events), len(batch.coverage))
        connection.rollback()