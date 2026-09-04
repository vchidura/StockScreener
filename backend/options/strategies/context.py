from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from options.domain import AssetType, DecisionContext
from options.repositories.base import ConnectionFactory, PostgresRepository
from equity.domain import DecisionWatermark, EquityContextSnapshot
from equity.repositories import EquityEvidenceRepository

from .domain import StrategyContextSnapshot, StrategyContextStatus


_CANONICAL_CLOSE_QUERY = """
    WITH visible AS (
        SELECT DISTINCT ON (bar_start)
               bar_start AS datetime, close_price
        FROM equity_bar_revisions
        WHERE ticker = %s
          AND interval = %s
          AND session_scope = 'RTH'
          AND adjusted = FALSE
          AND is_final = TRUE
          AND bar_end <= %s
          AND COALESCE(replay_available_at, system_observed_at) <= %s
        ORDER BY bar_start,
                 CASE
                     WHEN source_kind = 'RECONCILED' THEN 0
                     WHEN interval IN ('1h', '1d') AND source_kind = 'DERIVED' THEN 1
                     WHEN source_kind = 'NATIVE_REST' THEN 2
                     ELSE 3
                 END,
                 COALESCE(replay_available_at, system_observed_at) DESC,
                 created_at DESC
    )
    SELECT datetime, close_price
    FROM visible
    ORDER BY datetime DESC
    LIMIT %s
"""


def exponential_average(values: tuple[Decimal, ...], span: int) -> Decimal | None:
    if len(values) < span or span <= 0:
        return None
    alpha = Decimal(2) / Decimal(span + 1)
    average = values[0]
    for value in values[1:]:
        average = alpha * value + (Decimal(1) - alpha) * average
    return average


def build_strategy_context(
    matrix_id: UUID,
    underlyer: str,
    asset_type: AssetType,
    decision_context: DecisionContext,
    daily_bars: tuple[dict[str, object], ...],
    hourly_bars: tuple[dict[str, object], ...],
    *,
    event_calendar_available: bool,
    bar_observation_times_available: bool,
    earnings_blackout: bool | None,
    fed_blackout: bool | None,
    policy_version: str,
    policy_sha256: str,
    equity_context: EquityContextSnapshot | None = None,
) -> StrategyContextSnapshot:
    daily_values = tuple(Decimal(str(row["close_price"])) for row in daily_bars)
    hourly_values = tuple(Decimal(str(row["close_price"])) for row in hourly_bars)
    daily_ema = exponential_average(daily_values, 50) if len(daily_values) >= 100 else None
    hourly_ema = exponential_average(hourly_values, 20) if len(hourly_values) >= 20 else None
    daily_close = daily_values[-1] if daily_values else None
    hourly_close = hourly_values[-1] if hourly_values else None
    reasons: list[str] = []
    if daily_ema is None:
        reasons.append("DAILY_TREND_HISTORY_INSUFFICIENT")
    if hourly_ema is None:
        reasons.append("HOURLY_TREND_HISTORY_INSUFFICIENT")
    if not bar_observation_times_available:
        reasons.append("BAR_OBSERVATION_TIME_UNAVAILABLE")
    trend_state = None
    if daily_close is not None and daily_ema is not None and hourly_close is not None and hourly_ema is not None:
        if daily_close > daily_ema and hourly_close > hourly_ema:
            trend_state = "BULLISH"
        elif daily_close < daily_ema and hourly_close < hourly_ema:
            trend_state = "BEARISH"
        else:
            trend_state = "NEUTRAL"
    if not event_calendar_available:
        reasons.append("EVENT_CALENDAR_UNAVAILABLE")
    earnings_state = (
        "NOT_APPLICABLE"
        if asset_type is AssetType.ETF
        else "UNAVAILABLE"
        if earnings_blackout is None
        else "BLOCKED"
        if earnings_blackout
        else "CLEAR"
    )
    fed_state = "UNAVAILABLE" if fed_blackout is None else "BLOCKED" if fed_blackout else "CLEAR"
    if earnings_state == "BLOCKED":
        reasons.append("EARNINGS_BLACKOUT")
    elif earnings_state == "UNAVAILABLE":
        reasons.append("EARNINGS_CALENDAR_UNAVAILABLE")
    if fed_state == "BLOCKED":
        reasons.append("FED_BLACKOUT")
    elif fed_state == "UNAVAILABLE":
        reasons.append("FED_CALENDAR_UNAVAILABLE")
    source_bar_keys = tuple(
        f"daily:{row['datetime'].isoformat()}" for row in daily_bars
    ) + tuple(f"hourly:{row['datetime'].isoformat()}" for row in hourly_bars)
    status = (
        StrategyContextStatus.COMPLETE
        if not reasons
        else StrategyContextStatus.FAILED
        if daily_ema is None or hourly_ema is None
        else StrategyContextStatus.DEGRADED
    )
    return StrategyContextSnapshot(
        context_snapshot_id=uuid5(
            NAMESPACE_URL,
            f"option-context:{matrix_id}:{policy_sha256}",
        ),
        matrix_id=matrix_id,
        underlyer=underlyer,
        market_data_time=decision_context.market_time,
        observed_time=decision_context.observed_time,
        status=status,
        daily_close=daily_close,
        daily_ema_50=daily_ema,
        daily_input_bars=len(daily_values),
        hourly_close=hourly_close,
        hourly_ema_20=hourly_ema,
        hourly_input_bars=len(hourly_values),
        trend_state=trend_state,
        earnings_blackout_state=earnings_state,
        fed_blackout_state=fed_state,
        quote_spread_state="NOT_AVAILABLE",
        reason_codes=tuple(dict.fromkeys(reasons)),
        source_bar_keys=source_bar_keys,
        policy_version=policy_version,
        policy_sha256=policy_sha256,
        equity_context_snapshot_id=(
            equity_context.equity_context_snapshot_id if equity_context else None
        ),
        equity_context_status=(equity_context.status.value if equity_context else None),
        qualified_direction=(equity_context.qualified_direction if equity_context else None),
        company_name=(
            json.loads(equity_context.summary_json).get("company_name")
            if equity_context else None
        ),
        market_cap=(equity_context.market_cap if equity_context else None),
        shares_outstanding=(equity_context.shares_outstanding if equity_context else None),
        free_float=(equity_context.free_float if equity_context else None),
        dividend_yield=(equity_context.dividend_yield if equity_context else None),
        enterprise_value=(equity_context.enterprise_value if equity_context else None),
        ebitda=(equity_context.ebitda if equity_context else None),
        operating_income=(equity_context.operating_income if equity_context else None),
        free_cash_flow=(equity_context.free_cash_flow if equity_context else None),
        equity_reason_codes=(equity_context.reason_codes if equity_context else ()),
    )


class OptionStrategyContextRepository(PostgresRepository):
    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
        equity_context_repository: EquityEvidenceRepository | None = None,
    ) -> None:
        super().__init__(connection_factory)
        self.equity_context_repository = equity_context_repository or EquityEvidenceRepository(
            connection_factory
        )

    def build(
        self,
        matrix_id: UUID,
        underlyer: str,
        asset_type: AssetType,
        decision_context: DecisionContext,
        *,
        event_calendar_available: bool,
        policy_version: str,
        policy_sha256: str,
        equity_context_enabled: bool = False,
    ) -> StrategyContextSnapshot:
        with self._cursor() as cursor:
            cursor.execute(
                _CANONICAL_CLOSE_QUERY,
                (
                    underlyer, "1d", decision_context.market_time,
                    decision_context.observed_time, 100,
                ),
            )
            daily = tuple(reversed(cursor.fetchall()))
            cursor.execute(
                _CANONICAL_CLOSE_QUERY,
                (
                    underlyer, "1h", decision_context.market_time,
                    decision_context.observed_time, 20,
                ),
            )
            hourly = tuple(reversed(cursor.fetchall()))
            earnings_blackout = None
            fed_blackout = None
            if event_calendar_available:
                cursor.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (source, source_key)
                            event_type, affected_underlying, scheduled_time, status
                        FROM option_market_events
                        WHERE first_observed_at <= %s
                          AND COALESCE(revised_observed_at, first_observed_at) <= %s
                        ORDER BY source, source_key,
                                 COALESCE(revised_observed_at, first_observed_at) DESC
                    )
                    SELECT event_type, affected_underlying, scheduled_time
                    FROM latest
                    WHERE status NOT IN ('CANCELED')
                      AND scheduled_time BETWEEN %s - INTERVAL '1 day'
                                             AND %s + INTERVAL '72 hours'
                      AND (affected_underlying IS NULL OR affected_underlying = %s)
                    """,
                    (
                        decision_context.observed_time,
                        decision_context.observed_time,
                        decision_context.market_time,
                        decision_context.market_time,
                        underlyer,
                    ),
                )
                events = cursor.fetchall()
                earnings_blackout = any(row["event_type"] == "EARNINGS" for row in events)
                fed_blackout = any(row["event_type"] == "FED_RATE_DECISION" for row in events)
        equity_context = None
        if equity_context_enabled:
            equity_context = self.equity_context_repository.get_context_as_of(
                underlyer,
                "INTRADAY_30M",
                DecisionWatermark(
                    decision_context.market_time,
                    decision_context.observed_time,
                ),
            )
        return build_strategy_context(
            matrix_id,
            underlyer,
            asset_type,
            decision_context,
            daily,
            hourly,
            event_calendar_available=event_calendar_available,
            bar_observation_times_available=bool(daily and hourly),
            earnings_blackout=earnings_blackout,
            fed_blackout=fed_blackout,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
            equity_context=equity_context,
        )