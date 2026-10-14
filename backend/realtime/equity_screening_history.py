from __future__ import annotations

from dataclasses import dataclass

from .contracts import canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_validation_history import EquityValidationHistoryPackage


@dataclass(frozen=True, slots=True)
class RealtimeScreeningHistoryPolicy:
    version: str = "realtime-screening-split-adjusted-v1"
    adjusted_daily_sessions: int = 253
    historical_price_basis: str = "PROVIDER_SPLIT_ADJUSTED"
    current_quote_basis: str = "RAW_ACTION_GATED"

    def __post_init__(self) -> None:
        if self.adjusted_daily_sessions < 253:
            raise ValueError("realtime screening cannot weaken the 12-1 history depth")
        if self.historical_price_basis != "PROVIDER_SPLIT_ADJUSTED":
            raise ValueError("realtime screening history must remain split adjusted")
        if self.current_quote_basis != "RAW_ACTION_GATED":
            raise ValueError("realtime screening current quote must remain raw")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


REALTIME_SCREENING_HISTORY_POLICY = RealtimeScreeningHistoryPolicy()


@dataclass(frozen=True, slots=True)
class RealtimeScreeningHistory:
    ticker: str
    adjusted_daily: tuple[RealtimeEquityBarFact, ...]
    current_raw_daily: RealtimeEquityBarFact
    policy_sha256: str
    source_history_sha256: str


def select_realtime_screening_history(
    package: EquityValidationHistoryPackage,
    ticker: str,
    *,
    policy: RealtimeScreeningHistoryPolicy = REALTIME_SCREENING_HISTORY_POLICY,
) -> RealtimeScreeningHistory:
    symbol = ticker.strip().upper()
    adjusted = tuple(
        fact for fact in package.facts
        if fact.revision.ticker == symbol
        and fact.revision.interval == "1d"
        and fact.revision.adjusted
    )[-policy.adjusted_daily_sessions:]
    raw = tuple(
        fact for fact in package.facts
        if fact.revision.ticker == symbol
        and fact.revision.interval == "1d"
        and not fact.revision.adjusted
    )
    if len(adjusted) != policy.adjusted_daily_sessions or not raw:
        raise ValueError(f"realtime screening history is incomplete for {symbol}")
    current_raw = raw[-1]
    if adjusted[-1].revision.session_date != current_raw.revision.session_date:
        raise ValueError(f"realtime screening bases end on different sessions for {symbol}")
    adjusted_identity = {
        (fact.cohort_id, fact.provider_tier, fact.instrument_id, fact.revision.security_id)
        for fact in adjusted
    }
    current_identity = (
        current_raw.cohort_id,
        current_raw.provider_tier,
        current_raw.instrument_id,
        current_raw.revision.security_id,
    )
    if len(adjusted_identity) != 1 or next(iter(adjusted_identity)) != current_identity:
        raise ValueError(f"realtime screening identity changed for {symbol}")
    return RealtimeScreeningHistory(
        ticker=symbol,
        adjusted_daily=adjusted,
        current_raw_daily=current_raw,
        policy_sha256=policy.sha256,
        source_history_sha256=package.evidence_sha256,
    )
