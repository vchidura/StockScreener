"""Derive finalized XNYS higher-interval bars from canonical source revisions."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from .domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from .polygon import sha256_json


DERIVATION_SOURCES = {
    "1h": "30m",
    "1d": "30m",
    "1wk": "1d",
    "1mo": "1d",
}
DERIVATION_POLICIES = {
    "1h": "xnys_canonical_30m_to_1h_v1",
    "1d": "xnys_canonical_30m_to_1d_v1",
    "1wk": "xnys_canonical_1d_to_1wk_v1",
    "1mo": "xnys_canonical_1d_to_1mo_v1",
}


def derive_canonical_bars(
    security: SecurityReferenceRevision,
    source_bars: Sequence[EquityBarRevision],
    *,
    target_interval: str,
    observed_at: datetime,
    ingestion_segment_id: UUID,
    calendar_name: str = "XNYS",
) -> tuple[EquityBarRevision, ...]:
    if target_interval not in DERIVATION_SOURCES:
        raise ValueError(f"unsupported derived interval: {target_interval}")
    observed_utc = _utc(observed_at)
    expected_source = DERIVATION_SOURCES[target_interval]
    ordered = tuple(sorted(source_bars, key=lambda row: row.bar_start))
    _validate_sources(security, ordered, expected_source)
    calendar = exchange_calendars.get_calendar(calendar_name)

    if target_interval in ("1h", "1d"):
        return _derive_from_intraday(
            security,
            ordered,
            target_interval=target_interval,
            observed_at=observed_utc,
            ingestion_segment_id=ingestion_segment_id,
            calendar=calendar,
        )
    return _derive_from_daily(
        security,
        ordered,
        target_interval=target_interval,
        observed_at=observed_utc,
        ingestion_segment_id=ingestion_segment_id,
        calendar=calendar,
    )


def _derive_from_intraday(
    security: SecurityReferenceRevision,
    bars: Sequence[EquityBarRevision],
    *,
    target_interval: str,
    observed_at: datetime,
    ingestion_segment_id: UUID,
    calendar,
) -> tuple[EquityBarRevision, ...]:
    grouped: dict[date, list[EquityBarRevision]] = defaultdict(list)
    for bar in bars:
        grouped[bar.session_date].append(bar)
    results = []
    for session_date, session_bars in sorted(grouped.items()):
        session = pd.Timestamp(session_date)
        if not calendar.is_session(session):
            continue
        session_open = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
        session_close = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        by_start = {bar.bar_start: bar for bar in session_bars}
        if len(by_start) != len(session_bars):
            raise ValueError(f"duplicate canonical source window: {security.ticker}")

        if target_interval == "1d":
            source_window = _complete_window(
                by_start, session_open, session_close, timedelta(minutes=30)
            )
            if source_window and observed_at >= session_close:
                results.append(_aggregate(
                    security, source_window, target_interval, observed_at,
                    ingestion_segment_id,
                ))
            continue

        window_start = session_open
        while window_start < session_close:
            window_end = min(window_start + timedelta(hours=1), session_close)
            source_window = _complete_window(
                by_start, window_start, window_end, timedelta(minutes=30)
            )
            if source_window and observed_at >= window_end:
                results.append(_aggregate(
                    security, source_window, target_interval, observed_at,
                    ingestion_segment_id,
                ))
            window_start = window_end
    return tuple(results)


def _derive_from_daily(
    security: SecurityReferenceRevision,
    bars: Sequence[EquityBarRevision],
    *,
    target_interval: str,
    observed_at: datetime,
    ingestion_segment_id: UUID,
    calendar,
) -> tuple[EquityBarRevision, ...]:
    grouped: dict[tuple[int, ...], list[EquityBarRevision]] = defaultdict(list)
    for bar in bars:
        if target_interval == "1wk":
            iso = bar.session_date.isocalendar()
            key = (iso.year, iso.week)
        else:
            key = (bar.session_date.year, bar.session_date.month)
        grouped[key].append(bar)

    results = []
    for key, period_bars in sorted(grouped.items()):
        if target_interval == "1wk":
            period_start = date.fromisocalendar(key[0], key[1], 1)
            period_end = date.fromisocalendar(key[0], key[1], 5)
        else:
            period_start = date(key[0], key[1], 1)
            next_month = (
                date(key[0] + 1, 1, 1)
                if key[1] == 12
                else date(key[0], key[1] + 1, 1)
            )
            period_end = next_month - timedelta(days=1)
        expected_sessions = calendar.sessions_in_range(
            pd.Timestamp(period_start), pd.Timestamp(period_end)
        )
        expected_dates = tuple(session.date() for session in expected_sessions)
        ordered = tuple(sorted(period_bars, key=lambda row: row.session_date))
        if not expected_dates or tuple(row.session_date for row in ordered) != expected_dates:
            continue
        final_close = calendar.session_close(expected_sessions[-1]).to_pydatetime().astimezone(
            timezone.utc
        )
        if observed_at < final_close:
            continue
        results.append(_aggregate(
            security, ordered, target_interval, observed_at, ingestion_segment_id,
        ))
    return tuple(results)


def _complete_window(
    bars_by_start: dict[datetime, EquityBarRevision],
    start: datetime,
    end: datetime,
    source_duration: timedelta,
) -> tuple[EquityBarRevision, ...]:
    expected_starts = []
    cursor = start
    while cursor < end:
        expected_starts.append(cursor)
        cursor = min(cursor + source_duration, end)
    selected = tuple(
        bars_by_start[value] for value in expected_starts if value in bars_by_start
    )
    if len(selected) != len(expected_starts):
        return ()
    if selected[0].bar_start != start or selected[-1].bar_end != end:
        return ()
    if any(left.bar_end != right.bar_start for left, right in zip(selected, selected[1:])):
        return ()
    return selected


def _aggregate(
    security: SecurityReferenceRevision,
    sources: Sequence[EquityBarRevision],
    target_interval: str,
    observed_at: datetime,
    ingestion_segment_id: UUID,
) -> EquityBarRevision:
    source_ids = tuple(row.bar_revision_id for row in sources)
    volume = sum((row.volume for row in sources), Decimal("0"))
    vwap_volume = sum(
        (row.volume for row in sources if row.vwap is not None), Decimal("0")
    )
    weighted_vwap = sum(
        (row.vwap * row.volume for row in sources if row.vwap is not None), Decimal("0")
    )
    vwap = weighted_vwap / vwap_volume if vwap_volume > 0 else None
    transaction_counts = [
        row.transaction_count for row in sources if row.transaction_count is not None
    ]
    availability_mode = (
        BarAvailabilityMode.LIVE_OBSERVED
        if all(row.availability_mode is BarAvailabilityMode.LIVE_OBSERVED for row in sources)
        else BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
    )
    replay_available_at = (
        None
        if availability_mode is BarAvailabilityMode.LIVE_OBSERVED
        else max(row.replay_available_at or row.system_observed_at for row in sources)
    )
    payload = {
        "adjusted": sources[0].adjusted,
        "interval": target_interval,
        "policy": DERIVATION_POLICIES[target_interval],
        "session_scope": sources[0].session_scope.value,
        "source_bar_revision_ids": [str(value) for value in source_ids],
        "ticker": security.ticker,
    }
    digest = sha256_json(payload)
    return EquityBarRevision(
        bar_revision_id=uuid5(
            NAMESPACE_URL,
            f"equity-bar:{security.ticker}:{target_interval}:"
            f"{sources[0].bar_start.isoformat()}:DERIVED:{availability_mode.value}:{digest}",
        ),
        security_id=security.security_id,
        ticker=security.ticker,
        interval=target_interval,
        session_date=sources[-1].session_date,
        bar_start=sources[0].bar_start,
        bar_end=sources[-1].bar_end,
        open_price=sources[0].open_price,
        high_price=max(row.high_price for row in sources),
        low_price=min(row.low_price for row in sources),
        close_price=sources[-1].close_price,
        volume=volume,
        vwap=vwap,
        transaction_count=sum(transaction_counts) if transaction_counts else None,
        source_kind=BarSourceKind.DERIVED,
        availability_mode=availability_mode,
        is_final=True,
        system_observed_at=max(observed_at, sources[-1].bar_end),
        replay_available_at=replay_available_at,
        adjusted=sources[0].adjusted,
        payload_sha256=digest,
        quality_codes=(f"DERIVED_FROM_CANONICAL_{sources[0].interval.upper()}",),
        ingestion_segment_id=ingestion_segment_id,
        source_bar_revision_ids=source_ids,
        session_scope=sources[0].session_scope,
    )


def _validate_sources(
    security: SecurityReferenceRevision,
    bars: Iterable[EquityBarRevision],
    expected_interval: str,
) -> None:
    scope = None
    adjusted = None
    for bar in bars:
        if bar.ticker != security.ticker or bar.security_id != security.security_id:
            raise ValueError("source bar does not belong to security")
        if bar.interval != expected_interval or not bar.is_final:
            raise ValueError(f"derivation requires finalized {expected_interval} bars")
        if scope is None:
            scope = bar.session_scope
            adjusted = bar.adjusted
        if bar.session_scope is not scope or bar.adjusted != adjusted:
            raise ValueError("derivation source policy is mixed")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(timezone.utc)