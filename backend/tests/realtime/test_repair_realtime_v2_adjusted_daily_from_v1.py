from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from scripts.repair_realtime_v2_adjusted_daily_from_v1 import (
    _facts,
    _validate_source_rows,
)


def _row(ticker, instrument_id):
    return {
        "bar_revision_id": UUID(int=instrument_id),
        "cohort_id": UUID(int=100),
        "provider_tier": "MASSIVE_DEVELOPER",
        "security_id": UUID(int=instrument_id + 20),
        "instrument_id": instrument_id,
        "ticker": ticker,
        "interval": "1d",
        "session_date": date(2025, 9, 2),
        "session_scope": "RTH",
        "adjustment_basis": "ADJUSTED",
        "bar_start": datetime(2025, 9, 2, 13, 30, tzinfo=timezone.utc),
        "bar_end": datetime(2025, 9, 2, 20, 0, tzinfo=timezone.utc),
        "open_price": Decimal("100"),
        "high_price": Decimal("101"),
        "low_price": Decimal("99"),
        "close_price": Decimal("100"),
        "volume": Decimal("1000"),
        "vwap": None,
        "transaction_count": None,
        "source_kind": "NATIVE_REST",
        "availability_mode": "HISTORICAL_RECONSTRUCTED",
        "is_final": True,
        "observed_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "replay_available_at": None,
        "provider_published_at": None,
        "ingestion_segment_id": UUID(int=instrument_id + 40),
        "source_revision_ids": (),
        "supersedes_revision_id": None,
        "payload_sha256": f"{instrument_id:064x}",
        "quality_codes": (),
    }


def test_retained_copy_requires_exact_universe_and_membership():
    tickers = ("AAPL", "AMZN", "IWM", "MSFT", "NFLX", "NVDA", "QQQ", "SPY", "TSLA")
    rows = tuple(_row(ticker, index) for index, ticker in enumerate(tickers, 1))
    _validate_source_rows(rows)
    facts = _facts(
        rows,
        UUID(int=100),
        tuple(
            (row["instrument_id"], row["security_id"])
            for row in rows
        ),
    )

    assert len(facts) == 9
    assert facts[0].revision.system_observed_at == rows[0]["observed_at"]
    with pytest.raises(ValueError, match="not exact"):
        _validate_source_rows(rows[:-1])
