from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Mapping, Sequence
from uuid import UUID

import exchange_calendars
import pandas as pd

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)

from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_bar_repository import (
    MAX_BAR_PERSIST_BATCH,
    persist_realtime_equity_bars_cursor,
)
from .preflight import ProviderTier


@dataclass(frozen=True, slots=True)
class EquityValidationHistoryPolicy:
    version: str = "equity-validation-history-v2"
    adjusted_daily_sessions: int = 283
    raw_intraday_sessions: int = 90
    required_split_tickers: tuple[str, ...] = ("NFLX",)

    def __post_init__(self) -> None:
        if (
            self.adjusted_daily_sessions < 273
            or self.raw_intraday_sessions < 90
        ):
            raise ValueError("equity validation history cannot weaken stable depth")
        required = tuple(sorted(ticker.strip().upper() for ticker in self.required_split_tickers))
        if not set(required).issubset(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
            raise ValueError("required split ticker is outside equity validation")
        object.__setattr__(self, "required_split_tickers", required)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


EQUITY_VALIDATION_HISTORY_POLICY = EquityValidationHistoryPolicy()


@dataclass(frozen=True, slots=True)
class SplitCoverage:
    ticker: str
    window_start: date
    window_end: date
    received_at: datetime


@dataclass(frozen=True, slots=True)
class SplitAction:
    ticker: str
    effective_date: date
    split_from: str
    split_to: str


@dataclass(frozen=True, slots=True)
class AdjustedDiscontinuity:
    ticker: str
    session_date: date
    return_fraction: float
    split_coincident: bool


@dataclass(frozen=True, slots=True)
class EquityValidationHistoryPackage:
    policy_sha256: str
    facts: tuple[RealtimeEquityBarFact, ...]
    counts_by_ticker: tuple[tuple[str, int, int, int, int], ...]
    split_actions: tuple[SplitAction, ...]
    adjusted_discontinuities: tuple[AdjustedDiscontinuity, ...]
    evidence_sha256: str


def _consecutive_sessions(rows: Sequence[RealtimeEquityBarFact]) -> bool:
    sessions = tuple(row.revision.session_date for row in rows)
    if not sessions:
        return False
    calendar = exchange_calendars.get_calendar("XNYS")
    expected = tuple(
        value.date()
        for value in calendar.sessions_in_range(
            pd.Timestamp(sessions[0]),
            pd.Timestamp(sessions[-1]),
        )
    )
    return sessions == expected


def _expected_intraday_counts(sessions: tuple[date, ...]) -> tuple[int, int]:
    calendar = exchange_calendars.get_calendar("XNYS")
    thirty_count = 0
    hourly_count = 0
    for session_date in sessions:
        session = pd.Timestamp(session_date)
        opened = calendar.session_open(session)
        closed = calendar.session_close(session)
        minutes = int((closed - opened).total_seconds() // 60)
        if minutes <= 0 or minutes % 30:
            raise ValueError("XNYS session duration is not compatible with 30m bars")
        thirty_count += minutes // 30
        hourly_count += (minutes + 59) // 60
    return thirty_count, hourly_count


def build_equity_validation_history_package(
    facts: Sequence[RealtimeEquityBarFact],
    *,
    split_coverage: Mapping[str, SplitCoverage],
    split_actions: Sequence[SplitAction],
    policy: EquityValidationHistoryPolicy = EQUITY_VALIDATION_HISTORY_POLICY,
) -> EquityValidationHistoryPackage:
    rows = tuple(sorted(
        facts,
        key=lambda row: (
            row.revision.ticker,
            row.revision.interval,
            row.revision.bar_start,
            row.revision.bar_revision_id,
        ),
    ))
    expected_tickers = set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    if {row.revision.ticker for row in rows} != expected_tickers:
        raise ValueError("equity validation history must cover the exact scope")
    if len({row.cohort_id for row in rows}) != 1:
        raise ValueError("equity validation history cannot cross cohorts")
    if set(split_coverage) != expected_tickers:
        raise ValueError("equity validation split coverage is incomplete")
    action_rows = tuple(sorted(
        split_actions,
        key=lambda row: (row.ticker, row.effective_date, row.split_from, row.split_to),
    ))
    if any(row.ticker not in expected_tickers for row in action_rows):
        raise ValueError("equity validation split action is outside the scope")

    selected = []
    counts = []
    discontinuities = []
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        ticker_rows = tuple(row for row in rows if row.revision.ticker == ticker)
        daily = tuple(
            row for row in ticker_rows
            if row.revision.interval == "1d" and row.revision.adjusted
        )[-policy.adjusted_daily_sessions:]
        raw_daily_rows = tuple(
            row for row in ticker_rows
            if row.revision.interval == "1d" and not row.revision.adjusted
        )
        hourly = tuple(
            row for row in ticker_rows
            if row.revision.interval == "1h" and not row.revision.adjusted
        )
        thirty = tuple(
            row for row in ticker_rows
            if row.revision.interval == "30m" and not row.revision.adjusted
        )
        raw_sessions = tuple(sorted({row.revision.session_date for row in thirty}))
        if len(raw_sessions) > policy.raw_intraday_sessions:
            raw_sessions = raw_sessions[-policy.raw_intraday_sessions:]
        raw_session_set = set(raw_sessions)
        raw_daily = tuple(
            row for row in raw_daily_rows if row.revision.session_date in raw_session_set
        )
        hourly = tuple(
            row for row in hourly if row.revision.session_date in raw_session_set
        )
        thirty = tuple(
            row for row in thirty if row.revision.session_date in raw_session_set
        )
        expected_thirty, expected_hourly = _expected_intraday_counts(raw_sessions)
        if (
            len(daily) != policy.adjusted_daily_sessions
            or len(raw_sessions) != policy.raw_intraday_sessions
            or len(raw_daily) != policy.raw_intraday_sessions
            or len(hourly) != expected_hourly
            or len(thirty) != expected_thirty
        ):
            raise ValueError(
                f"equity validation history depth is incomplete for {ticker}: "
                f"adjusted_daily={len(daily)} raw_daily={len(raw_daily)} "
                f"hourly={len(hourly)}/{expected_hourly} "
                f"thirty={len(thirty)}/{expected_thirty} sessions={len(raw_sessions)}"
            )
        if not _consecutive_sessions(daily):
            raise ValueError(f"adjusted daily sessions are not consecutive for {ticker}")
        split_dates = {
            action.effective_date for action in action_rows if action.ticker == ticker
        }
        for previous, current in zip(daily, daily[1:]):
            return_fraction = float(
                current.revision.close_price / previous.revision.close_price - 1
            )
            if return_fraction <= -0.38 or return_fraction >= 0.60:
                discontinuities.append(AdjustedDiscontinuity(
                    ticker=ticker,
                    session_date=current.revision.session_date,
                    return_fraction=return_fraction,
                    split_coincident=current.revision.session_date in split_dates,
                ))
        if not _consecutive_sessions(raw_daily):
            raise ValueError(f"raw daily sessions are not consecutive for {ticker}")
        if any(
            not row.revision.is_final
            or row.revision.session_scope.value != "RTH"
            or row.revision.availability_mode.value != "HISTORICAL_RECONSTRUCTED"
            for row in (*daily, *raw_daily, *hourly, *thirty)
        ):
            raise ValueError(f"equity validation history source contract drifted for {ticker}")
        intraday_start = min((*hourly, *thirty), key=lambda row: row.revision.bar_start).revision.session_date
        intraday_end = max((*hourly, *thirty), key=lambda row: row.revision.bar_end).revision.session_date
        coverage = split_coverage[ticker]
        if coverage.ticker != ticker or coverage.window_start > intraday_start or coverage.window_end < intraday_end:
            raise ValueError(f"raw intraday split coverage is incomplete for {ticker}")
        crossing_splits = tuple(
            action for action in action_rows
            if action.ticker == ticker
            and intraday_start <= action.effective_date <= intraday_end
        )
        if crossing_splits:
            raise ValueError(f"raw intraday split window requires review for {ticker}")
        selected.extend((*daily, *raw_daily, *hourly, *thirty))
        counts.append((
            ticker,
            len(daily),
            len(raw_daily),
            len(hourly),
            len(thirty),
        ))

    daily_start = min(row.revision.session_date for row in selected if row.revision.interval == "1d")
    daily_end = max(row.revision.session_date for row in selected if row.revision.interval == "1d")
    for ticker in policy.required_split_tickers:
        if not any(
            row.ticker == ticker and daily_start <= row.effective_date <= daily_end
            for row in action_rows
        ):
            raise ValueError(f"required split scenario is absent for {ticker}")
    split_discontinuities = tuple(
        row for row in discontinuities if row.split_coincident
    )
    if split_discontinuities:
        raise ValueError("adjusted history retains split-coincident discontinuities")
    identity = {
        "policy_sha256": policy.sha256,
        "bars": tuple(
            (
                row.revision.ticker,
                row.revision.interval,
                row.revision.adjusted,
                row.revision.bar_revision_id,
                row.revision.payload_sha256,
            )
            for row in selected
        ),
        "split_actions": action_rows,
        "adjusted_discontinuities": tuple(discontinuities),
    }
    return EquityValidationHistoryPackage(
        policy_sha256=policy.sha256,
        facts=tuple(selected),
        counts_by_ticker=tuple(counts),
        split_actions=action_rows,
        adjusted_discontinuities=tuple(discontinuities),
        evidence_sha256=canonical_sha256(identity),
    )


def persist_equity_validation_history_cursor(
    cursor,
    package: EquityValidationHistoryPackage,
) -> None:
    persist_equity_validation_facts_cursor(cursor, package.facts)


def bind_equity_validation_instruments(
    package: EquityValidationHistoryPackage,
    instrument_ids: Mapping[str, int],
) -> EquityValidationHistoryPackage:
    if set(instrument_ids) != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
        raise ValueError("equity validation instrument binding is incomplete")
    if any(type(value) is not int or value <= 0 for value in instrument_ids.values()):
        raise ValueError("equity validation instrument binding is invalid")
    facts = tuple(
        replace(fact, instrument_id=instrument_ids[fact.revision.ticker])
        for fact in package.facts
    )
    return replace(package, facts=facts)


def persist_equity_validation_facts_cursor(
    cursor,
    facts: Sequence[RealtimeEquityBarFact],
) -> None:
    rows = tuple(facts)
    if not rows:
        raise ValueError("equity validation persistence requires facts")
    for offset in range(0, len(rows), MAX_BAR_PERSIST_BATCH):
        persist_realtime_equity_bars_cursor(
            cursor,
            rows[offset:offset + MAX_BAR_PERSIST_BATCH],
        )


def load_equity_validation_history(
    database,
    *,
    cohort_id: UUID,
    split_coverage: Mapping[str, SplitCoverage],
    split_actions: Sequence[SplitAction],
    policy: EquityValidationHistoryPolicy = EQUITY_VALIDATION_HISTORY_POLICY,
) -> EquityValidationHistoryPackage:
    with database.transaction(
        read_only=True,
        isolation_level="REPEATABLE READ",
    ) as cursor:
        cursor.execute(
            "SELECT bar_revision_id::text, security_id::text, instrument_id, ticker, "
            "interval, session_date, session_scope, adjustment_basis, bar_start, bar_end, "
            "open_price, high_price, low_price, close_price, volume, vwap, "
            "transaction_count, source_kind, availability_mode, is_final, observed_at, "
            "replay_available_at, provider_published_at, ingestion_segment_id::text, "
            "source_revision_ids, supersedes_revision_id::text, payload_sha256, "
            "quality_codes FROM rt.bar_revisions WHERE cohort_id=%s "
            "AND provider_tier='MASSIVE_DEVELOPER' AND ((interval='1d' "
            "AND adjustment_basis IN ('ADJUSTED', 'RAW_UNADJUSTED')) "
            "OR (interval IN ('1h', '30m') "
            "AND adjustment_basis='RAW_UNADJUSTED')) ORDER BY ticker, interval, bar_start",
            (str(cohort_id),),
        )
        rows = tuple(cursor.fetchall())
    facts = []
    for row in rows:
        (
            revision_id,
            security_id,
            instrument_id,
            ticker,
            interval,
            session_date,
            session_scope,
            adjustment_basis,
            bar_start,
            bar_end,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            vwap,
            transaction_count,
            source_kind,
            availability_mode,
            is_final,
            observed_at,
            replay_available_at,
            provider_published_at,
            ingestion_segment_id,
            source_revision_ids,
            supersedes_revision_id,
            payload_sha256,
            quality_codes,
        ) = row
        revision = EquityBarRevision(
            bar_revision_id=UUID(revision_id),
            security_id=UUID(security_id),
            ticker=str(ticker),
            interval=str(interval),
            session_date=session_date,
            session_scope=BarSessionScope(str(session_scope)),
            bar_start=bar_start,
            bar_end=bar_end,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            vwap=vwap,
            transaction_count=transaction_count,
            source_kind=BarSourceKind(str(source_kind)),
            availability_mode=BarAvailabilityMode(str(availability_mode)),
            is_final=bool(is_final),
            system_observed_at=observed_at,
            replay_available_at=replay_available_at,
            provider_published_at=provider_published_at,
            ingestion_segment_id=(
                UUID(ingestion_segment_id) if ingestion_segment_id else None
            ),
            adjusted=(adjustment_basis == "ADJUSTED"),
            source_bar_revision_ids=tuple(
                UUID(str(value)) for value in source_revision_ids
            ),
            supersedes_bar_revision_id=(
                UUID(supersedes_revision_id) if supersedes_revision_id else None
            ),
            payload_sha256=str(payload_sha256),
            quality_codes=tuple(quality_codes),
        )
        facts.append(RealtimeEquityBarFact(
            cohort_id=cohort_id,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            instrument_id=int(instrument_id),
            revision=revision,
        ))
    return build_equity_validation_history_package(
        facts,
        split_coverage=split_coverage,
        split_actions=split_actions,
        policy=policy,
    )