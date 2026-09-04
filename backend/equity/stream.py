"""Advanced equity minute-stream aggregation and native REST reconciliation."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import RLock
from typing import Any, Iterable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from .domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from .polygon import sha256_json
from .repositories import EquityBarRepository


STREAM_INTERVALS = frozenset(("5m", "15m", "30m"))
_INTERVAL_MINUTES = {"5m": 5, "15m": 15, "30m": 30}


@dataclass(frozen=True, slots=True)
class StreamIngestionResult:
    minute_bar: EquityBarRevision | None
    finalized_bars: tuple[EquityBarRevision, ...]


@dataclass(frozen=True, slots=True)
class StreamBatchResult:
    received_messages: int
    accepted_minutes: int
    duplicate_minutes: int
    ignored_messages: int
    finalized_bars: int
    inserted_bars: int
    market_watermark: datetime | None


class EquityStreamProcessor:
    def __init__(
        self,
        securities: Iterable[SecurityReferenceRevision],
        *,
        ingestion_segment_id: UUID,
        accumulator: SessionBarAccumulator | None = None,
        bar_repository: EquityBarRepository | None = None,
    ) -> None:
        self.securities = {row.ticker: row for row in securities}
        if not self.securities:
            raise ValueError("at least one security reference is required")
        self.ingestion_segment_id = ingestion_segment_id
        self.accumulator = accumulator or SessionBarAccumulator()
        self.bar_repository = bar_repository or EquityBarRepository()
        self.market_watermark: datetime | None = None
        self._lock = RLock()

    def process(
        self,
        messages: Iterable[Any],
        *,
        observed_at: datetime,
    ) -> StreamBatchResult:
        with self._lock:
            return self._process(messages, observed_at=observed_at)

    def _process(
        self,
        messages: Iterable[Any],
        *,
        observed_at: datetime,
    ) -> StreamBatchResult:
        observed_utc = _utc(observed_at, "observed_at")
        message_rows = tuple(messages)
        accepted_minutes = duplicate_minutes = ignored_messages = 0
        finalized: list[EquityBarRevision] = []
        persistable: list[EquityBarRevision] = []
        for message in message_rows:
            event_type = _field(message, "event_type", "ev")
            event_value = getattr(event_type, "value", event_type)
            if event_value != "AM":
                ignored_messages += 1
                continue
            ticker = str(_field(message, "symbol", "sym") or "").upper()
            security = self.securities.get(ticker)
            if security is None:
                ignored_messages += 1
                continue
            minute = normalize_stream_minute(
                security.security_id,
                message,
                observed_at=observed_utc,
                ingestion_segment_id=self.ingestion_segment_id,
            )
            if minute is None:
                ignored_messages += 1
                continue
            result = self.accumulator.ingest(minute, finalize=False)
            if result.minute_bar is not None:
                accepted_minutes += 1
                persistable.append(result.minute_bar)
                self.market_watermark = max(
                    value for value in (self.market_watermark, result.minute_bar.bar_end)
                    if value is not None
                )
            else:
                duplicate_minutes += 1
        finalized.extend(self.accumulator.finalize_through(observed_utc))
        persistable.extend(finalized)
        inserted = self.bar_repository.persist(persistable)
        return StreamBatchResult(
            received_messages=len(message_rows),
            accepted_minutes=accepted_minutes,
            duplicate_minutes=duplicate_minutes,
            ignored_messages=ignored_messages,
            finalized_bars=len(finalized),
            inserted_bars=inserted,
            market_watermark=self.market_watermark,
        )

    def flush(self, *, observed_at: datetime) -> StreamBatchResult:
        with self._lock:
            observed_utc = _utc(observed_at, "observed_at")
            finalized = self.accumulator.finalize_through(observed_utc)
            inserted = self.bar_repository.persist(finalized)
            return StreamBatchResult(
                received_messages=0,
                accepted_minutes=0,
                duplicate_minutes=0,
                ignored_messages=0,
                finalized_bars=len(finalized),
                inserted_bars=inserted,
                market_watermark=self.market_watermark,
            )


def normalize_stream_minute(
    security_id: UUID,
    message: Any,
    *,
    observed_at: datetime,
    ingestion_segment_id: UUID | None = None,
    calendar_name: str = "XNYS",
) -> EquityBarRevision | None:
    observed_utc = _utc(observed_at, "observed_at")
    ticker = str(_field(message, "symbol", "sym") or "").upper()
    if not ticker:
        raise ValueError("stream aggregate symbol is required")
    start_value = _field(message, "start_timestamp", "s")
    if start_value is None:
        raise ValueError("stream aggregate start timestamp is required")
    start = pd.Timestamp(start_value, unit="ms", tz="UTC").to_pydatetime()
    canonical_start = start.replace(second=0, microsecond=0)
    if start != canonical_start:
        raise ValueError("stream minute must start on a minute boundary")
    end = start + timedelta(minutes=1)
    if observed_utc < end:
        return None

    calendar = exchange_calendars.get_calendar(calendar_name)
    session = pd.Timestamp(start.date())
    if not calendar.is_session(session):
        return None
    session_open = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    session_close = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
    if start < session_open or end > session_close:
        return None

    open_price = _decimal_field(message, "open", "o")
    high_price = _decimal_field(message, "high", "h")
    low_price = _decimal_field(message, "low", "l")
    close_price = _decimal_field(message, "close", "c")
    volume = _decimal_field(message, "volume", "v", default=Decimal("0"))
    vwap_value = _field(message, "vwap", "vw")
    vwap = Decimal(str(vwap_value)) if vwap_value is not None else None
    payload = {
        "close": str(close_price),
        "end": end.isoformat(),
        "high": str(high_price),
        "low": str(low_price),
        "open": str(open_price),
        "start": start.isoformat(),
        "ticker": ticker,
        "volume": str(volume),
        "vwap": str(vwap) if vwap is not None else None,
    }
    digest = sha256_json(payload)
    return EquityBarRevision(
        bar_revision_id=uuid5(
            NAMESPACE_URL,
            f"equity-bar:{ticker}:1m:{start.isoformat()}:"
            f"REALTIME_STREAM:{BarAvailabilityMode.LIVE_OBSERVED.value}:{digest}",
        ),
        security_id=security_id,
        ticker=ticker,
        interval="1m",
        session_date=session.date(),
        bar_start=start,
        bar_end=end,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        vwap=vwap,
        transaction_count=None,
        source_kind=BarSourceKind.REALTIME_STREAM,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True,
        system_observed_at=observed_utc,
        replay_available_at=None,
        adjusted=False,
        payload_sha256=digest,
        ingestion_segment_id=ingestion_segment_id,
    )


class SessionBarAccumulator:
    def __init__(
        self,
        *,
        intervals: Iterable[str] = STREAM_INTERVALS,
        calendar_name: str = "XNYS",
    ) -> None:
        selected = tuple(dict.fromkeys(intervals))
        unsupported = set(selected) - STREAM_INTERVALS
        if unsupported:
            raise ValueError(f"unsupported stream intervals: {sorted(unsupported)}")
        self.intervals = selected
        self.calendar = exchange_calendars.get_calendar(calendar_name)
        self._minutes: dict[tuple[str, datetime], EquityBarRevision] = {}
        self._emitted: dict[
            tuple[str, str, datetime], EquityBarRevision
        ] = {}
        self._pending: set[tuple[str, str, datetime, datetime]] = set()

    def ingest(
        self,
        minute: EquityBarRevision,
        *,
        finalize: bool = True,
    ) -> StreamIngestionResult:
        self._validate_minute(minute)
        key = (minute.ticker, minute.bar_start)
        previous_minute = self._minutes.get(key)
        if previous_minute is not None and previous_minute.payload_sha256 == minute.payload_sha256:
            return StreamIngestionResult(None, ())
        if previous_minute is not None:
            minute = replace(
                minute,
                supersedes_bar_revision_id=previous_minute.bar_revision_id,
                quality_codes=tuple(dict.fromkeys(
                    (*minute.quality_codes, "STREAM_MINUTE_CORRECTION")
                )),
            )
        self._minutes[key] = minute
        for interval in self.intervals:
            bucket_start, bucket_end = self._bucket(minute.bar_start, interval)
            self._pending.add((minute.ticker, interval, bucket_start, bucket_end))
        return StreamIngestionResult(
            minute,
            self.finalize_through(minute.system_observed_at) if finalize else (),
        )

    def finalize_through(self, watermark: datetime) -> tuple[EquityBarRevision, ...]:
        watermark_utc = _utc(watermark, "watermark")
        finalized = []
        due = sorted(
            window for window in self._pending if window[3] <= watermark_utc
        )
        for ticker, interval, bucket_start, bucket_end in due:
            source_minutes = sorted(
                (
                    minute for (source_ticker, _), minute in self._minutes.items()
                    if source_ticker == ticker
                    and bucket_start <= minute.bar_start < bucket_end
                ),
                key=lambda row: row.bar_start,
            )
            if not source_minutes:
                self._pending.discard((ticker, interval, bucket_start, bucket_end))
                continue
            bar = self._aggregate(interval, bucket_start, bucket_end, source_minutes)
            key = (ticker, interval, bucket_start)
            previous = self._emitted.get(key)
            if previous is not None and previous.payload_sha256 == bar.payload_sha256:
                self._pending.discard((ticker, interval, bucket_start, bucket_end))
                continue
            if previous is not None:
                bar = replace(
                    bar,
                    supersedes_bar_revision_id=previous.bar_revision_id,
                    quality_codes=tuple(dict.fromkeys(
                        (*bar.quality_codes, "STREAM_SOURCE_CORRECTION")
                    )),
                )
            self._emitted[key] = bar
            finalized.append(bar)
            self._pending.discard((ticker, interval, bucket_start, bucket_end))
        return tuple(finalized)

    def discard_before(self, cutoff: datetime) -> None:
        cutoff_utc = _utc(cutoff, "cutoff")
        self._minutes = {
            key: value for key, value in self._minutes.items()
            if value.bar_end > cutoff_utc
        }
        self._emitted = {
            key: value for key, value in self._emitted.items()
            if value.bar_end > cutoff_utc
        }
        self._pending = {
            value for value in self._pending if value[3] > cutoff_utc
        }

    def _validate_minute(self, minute: EquityBarRevision) -> None:
        if minute.interval != "1m":
            raise ValueError("stream accumulator requires one-minute bars")
        if minute.source_kind is not BarSourceKind.REALTIME_STREAM:
            raise ValueError("stream accumulator requires REALTIME_STREAM source")
        if minute.availability_mode is not BarAvailabilityMode.LIVE_OBSERVED:
            raise ValueError("stream accumulator requires LIVE_OBSERVED availability")
        if not minute.is_final:
            raise ValueError("stream accumulator requires finalized minute bars")
        self._bucket(minute.bar_start, self.intervals[0])

    def _bucket(self, start: datetime, interval: str) -> tuple[datetime, datetime]:
        session = pd.Timestamp(start.date())
        if not self.calendar.is_session(session):
            raise ValueError("stream minute is outside an exchange session")
        session_open = self.calendar.session_open(session).to_pydatetime().astimezone(
            timezone.utc
        )
        session_close = self.calendar.session_close(session).to_pydatetime().astimezone(
            timezone.utc
        )
        if not session_open <= start < session_close:
            raise ValueError("stream minute is outside regular session hours")
        minutes = _INTERVAL_MINUTES[interval]
        elapsed = int((start - session_open).total_seconds() // 60)
        bucket_start = session_open + timedelta(minutes=(elapsed // minutes) * minutes)
        return bucket_start, min(bucket_start + timedelta(minutes=minutes), session_close)

    @staticmethod
    def _aggregate(
        interval: str,
        start: datetime,
        end: datetime,
        minutes: list[EquityBarRevision],
    ) -> EquityBarRevision:
        expected_minutes = int((end - start).total_seconds() // 60)
        volume = sum((row.volume for row in minutes), Decimal("0"))
        weighted_vwap = sum(
            (row.vwap * row.volume for row in minutes if row.vwap is not None),
            Decimal("0"),
        )
        vwap_volume = sum(
            (row.volume for row in minutes if row.vwap is not None),
            Decimal("0"),
        )
        vwap = weighted_vwap / vwap_volume if vwap_volume > 0 else None
        source_ids = tuple(row.bar_revision_id for row in minutes)
        quality_codes = (
            ("SPARSE_STREAM_WINDOW",) if len(minutes) < expected_minutes else ()
        )
        payload = {
            "close": str(minutes[-1].close_price),
            "end": end.isoformat(),
            "high": str(max(row.high_price for row in minutes)),
            "interval": interval,
            "low": str(min(row.low_price for row in minutes)),
            "open": str(minutes[0].open_price),
            "source_bar_revision_ids": [str(value) for value in source_ids],
            "start": start.isoformat(),
            "ticker": minutes[0].ticker,
            "volume": str(volume),
            "vwap": str(vwap) if vwap is not None else None,
        }
        digest = sha256_json(payload)
        transaction_counts = [
            row.transaction_count for row in minutes if row.transaction_count is not None
        ]
        return EquityBarRevision(
            bar_revision_id=uuid5(
                NAMESPACE_URL,
                f"equity-bar:{minutes[0].ticker}:{interval}:{start.isoformat()}:"
                f"REALTIME_STREAM:{BarAvailabilityMode.LIVE_OBSERVED.value}:{digest}",
            ),
            security_id=minutes[0].security_id,
            ticker=minutes[0].ticker,
            interval=interval,
            session_date=start.date(),
            bar_start=start,
            bar_end=end,
            open_price=minutes[0].open_price,
            high_price=max(row.high_price for row in minutes),
            low_price=min(row.low_price for row in minutes),
            close_price=minutes[-1].close_price,
            volume=volume,
            vwap=vwap,
            transaction_count=sum(transaction_counts) if transaction_counts else None,
            source_kind=BarSourceKind.REALTIME_STREAM,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            is_final=True,
            system_observed_at=max(end, *(row.system_observed_at for row in minutes)),
            replay_available_at=None,
            adjusted=False,
            payload_sha256=digest,
            ingestion_segment_id=minutes[-1].ingestion_segment_id,
            source_bar_revision_ids=source_ids,
            reconciliation_status="PENDING",
            quality_codes=quality_codes,
        )


def reconcile_interval_bar(
    *,
    stream_bar: EquityBarRevision | None,
    native_bar: EquityBarRevision | None,
    observed_at: datetime,
    prefer_stream: bool = False,
) -> EquityBarRevision:
    if stream_bar is None and native_bar is None:
        raise ValueError("stream_bar or native_bar is required")
    observed_utc = _utc(observed_at, "observed_at")
    base = stream_bar if prefer_stream and stream_bar is not None else native_bar or stream_bar
    assert base is not None
    if observed_utc < base.bar_end:
        raise ValueError("reconciliation cannot precede bar finality")
    if stream_bar is not None and native_bar is not None:
        _same_bar_key(stream_bar, native_bar)
        compared_fields = (
            "open_price", "high_price", "low_price", "close_price", "volume", "vwap"
        )
        changed = tuple(
            field for field in compared_fields
            if getattr(stream_bar, field) != getattr(native_bar, field)
        )
        status = "CORRECTED" if changed else "MATCHED"
        quality_codes = tuple(f"RECONCILED_{field.upper()}" for field in changed)
    elif native_bar is None:
        status = "NATIVE_MISSING"
        quality_codes = ("NATIVE_RECONCILIATION_MISSING",)
    else:
        status = "DERIVED_MISSING"
        quality_codes = ("DERIVED_STREAM_BAR_MISSING",)
    source_ids = tuple(
        row.bar_revision_id for row in (stream_bar, native_bar) if row is not None
    )
    payload = {
        "base_bar_revision_id": str(base.bar_revision_id),
        "preferred_source": base.source_kind.value,
        "reconciliation_status": status,
        "source_bar_revision_ids": [str(value) for value in source_ids],
    }
    digest = sha256_json(payload)
    return replace(
        base,
        bar_revision_id=uuid5(
            NAMESPACE_URL,
            f"equity-bar:{base.ticker}:{base.interval}:{base.bar_start.isoformat()}:"
            f"RECONCILED:{base.availability_mode.value}:{digest}",
        ),
        source_kind=BarSourceKind.RECONCILED,
        system_observed_at=observed_utc,
        ingestion_segment_id=(native_bar or stream_bar).ingestion_segment_id,
        payload_sha256=digest,
        source_bar_revision_ids=source_ids,
        supersedes_bar_revision_id=(
            stream_bar.bar_revision_id if stream_bar is not None else None
        ),
        reconciliation_status=status,
        quality_codes=quality_codes,
    )


def _same_bar_key(left: EquityBarRevision, right: EquityBarRevision) -> None:
    if (
        left.ticker,
        left.interval,
        left.bar_start,
        left.bar_end,
    ) != (
        right.ticker,
        right.interval,
        right.bar_start,
        right.bar_end,
    ):
        raise ValueError("stream and native bars must identify the same interval")


def _field(value: Any, attribute: str, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, value.get(attribute))
    return getattr(value, attribute, None)


def _decimal_field(
    value: Any,
    attribute: str,
    key: str,
    *,
    default: Decimal | None = None,
) -> Decimal:
    raw = _field(value, attribute, key)
    if raw is None:
        if default is not None:
            return default
        raise ValueError(f"stream aggregate {attribute} is required")
    return Decimal(str(raw))


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)