from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from equity.domain import BarAvailabilityMode, BarSourceKind
from realtime.equity_interval_derivation_worker import (
    ADVANCED_DERIVATION_BUILD_ID,
    DERIVATION_BUILD_ID,
    _output_fact,
)
from realtime import equity_interval_derivation_worker as worker
from realtime.equity_bar_repository import KNOWN_BY_ALL_CLOCKS
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 18, 25, tzinfo=UTC)
COHORT_ID = UUID("713278f5-e83d-55f5-98c3-c930be03e9c8")


def test_advanced_five_minute_sources_keep_their_provider_identity(monkeypatch):
    """R8 (Nov 11): the loader was renamed `load_current_five_minute_facts_cursor` and reads through the central
    known-by read (review R2), so this test stopped collecting; it now fakes that read."""
    assert DERIVATION_BUILD_ID == "v2-incremental-derivation-v1"
    assert ADVANCED_DERIVATION_BUILD_ID == (
        "v2-advanced-interval-derivation-v2"
    )
    row = {
        "bar_revision_id": UUID(int=1),
        "cohort_id": COHORT_ID,
        "provider_tier": ProviderTier.MASSIVE_ADVANCED.value,
        "instrument_id": 7,
        "security_id": UUID(int=2),
        "ticker": "SPY",
        "interval": "5m",
        "session_date": date(2026, 10, 16),
        "session_scope": "RTH",
        "adjustment_basis": "RAW_UNADJUSTED",
        "bar_start": NOW - timedelta(minutes=5),
        "bar_end": NOW,
        "open_price": Decimal("100"),
        "high_price": Decimal("101"),
        "low_price": Decimal("99"),
        "close_price": Decimal("100.5"),
        "volume": Decimal("1000"),
        "vwap": Decimal("100.25"),
        "transaction_count": 10,
        "source_kind": BarSourceKind.DERIVED.value,
        "availability_mode": BarAvailabilityMode.LIVE_OBSERVED.value,
        "is_final": True,
        "observed_at": NOW,
        "replay_available_at": None,
        "provider_published_at": None,
        "ingestion_segment_id": UUID(int=3),
        "source_revision_ids": tuple(
            UUID(int=value) for value in range(4, 9)
        ),
        "supersedes_revision_id": None,
        "payload_sha256": "a" * 64,
        "quality_codes": (),
        "journal_generation": None,
        "stream_session_id": None,
        "source_offset": None,
    }
    calls = []

    def known(cursor, **kwargs):
        calls.append((cursor, kwargs))
        return {7: [
            row,
            {**row, "bar_revision_id": UUID(int=20), "is_final": False},
            {**row, "bar_revision_id": UUID(int=21), "session_date": date(2026, 10, 15)},
        ]}

    monkeypatch.setattr(worker, "load_known_bar_revisions_cursor", known)
    facts = worker.load_current_five_minute_facts_cursor(
        "cursor",
        cohort_id=COHORT_ID,
        instrument_id=7,
        session_date=date(2026, 10, 16),
        checked_at=NOW,
    )

    # Final bars of the session only, read as known at the check time.
    assert len(facts) == 1
    (cursor, kwargs), = calls
    assert cursor == "cursor" and kwargs["instrument_ids"] == (7,) and kwargs["interval"] == "5m"
    assert kwargs["basis"] == "RAW_UNADJUSTED" and kwargs["known_by"] == NOW and kwargs["clocks"] == KNOWN_BY_ALL_CLOCKS
    assert kwargs["start"] == datetime(2026, 10, 16, tzinfo=UTC) and kwargs["end"] == datetime(2026, 10, 17, tzinfo=UTC)
    assert facts[0].cohort_id == COHORT_ID
    assert facts[0].provider_tier is ProviderTier.MASSIVE_ADVANCED
    assert facts[0].revision.source_kind is BarSourceKind.DERIVED
    output = _output_fact(
        facts[0],
        build_id=ADVANCED_DERIVATION_BUILD_ID,
    )
    assert output.revision.bar_revision_id != (
        facts[0].revision.bar_revision_id
    )
