from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from options.domain import AssetType, DecisionContext
from options.repositories.base import ConnectionFactory, PostgresRepository

from .domain import StrategyContextSnapshot, StrategyContextStatus


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
    )


class OptionStrategyContextRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

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
    ) -> StrategyContextSnapshot:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT datetime, close_price
                FROM stock_prices_daily
                WHERE ticker = %s
                  AND datetime <= %s
                ORDER BY datetime DESC
                LIMIT 100
                """,
                                (underlyer, decision_context.market_time),
            )
            daily = tuple(reversed(cursor.fetchall()))
            cursor.execute(
                """
                SELECT datetime, close_price
                FROM stock_prices_hourly
                WHERE ticker = %s
                  AND datetime <= %s
                ORDER BY datetime DESC
                LIMIT 20
                """,
                                (underlyer, decision_context.market_time),
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
        return build_strategy_context(
            matrix_id,
            underlyer,
            asset_type,
            decision_context,
            daily,
            hourly,
            event_calendar_available=event_calendar_available,
            bar_observation_times_available=False,
            earnings_blackout=earnings_blackout,
            fed_blackout=fed_blackout,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
        )