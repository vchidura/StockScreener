"""Pure composition policies for rich equity trade setups."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .technicals import EmaConfirmation, TradeSetupTechnicals


@dataclass(frozen=True, slots=True)
class DirectionComposition:
    direction: str
    conviction: str
    bull_signals: int
    bear_signals: int
    signal_reasons: tuple[str, ...]
    support_gaps: tuple[Mapping[str, Any], ...]
    resistance_gaps: tuple[Mapping[str, Any], ...]
    bull_fvgs: tuple[Mapping[str, Any], ...]
    bear_fvgs: tuple[Mapping[str, Any], ...]
    zones: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class TradeLevelComposition:
    entries: tuple[dict[str, Any], ...]
    targets: tuple[dict[str, Any], ...]
    stops: tuple[dict[str, Any], ...]


def compose_fibonacci_context(
    fibonacci: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not fibonacci:
        return None
    return {
        "scoring_role": "structural_context_only",
        "signal": fibonacci.get("signal"),
        "trend_direction": fibonacci.get("trend_direction"),
        "swing_basis": fibonacci.get("swing_basis"),
        "swing_detection_pct": fibonacci.get("swing_detection_pct"),
        "scope_bars": fibonacci.get("scope_bars"),
        "swing_high": fibonacci.get("swing_high"),
        "swing_low": fibonacci.get("swing_low"),
        "swing_high_date": fibonacci.get("swing_high_date"),
        "swing_low_date": fibonacci.get("swing_low_date"),
        "swing_size_pct": fibonacci.get("swing_size_pct"),
        "developing_pivot": fibonacci.get("developing_pivot"),
        "active_leg": fibonacci.get("active_leg"),
        "confirmed_legs": fibonacci.get("confirmed_legs", []),
        "nearest_level": fibonacci.get("nearest_level"),
        "nearest_level_price": fibonacci.get("nearest_level_price"),
        "distance_pct": fibonacci.get("distance_pct"),
        "retracement_pct": fibonacci.get("retracement_pct"),
        "progress_reached_pct": fibonacci.get("progress_reached_pct"),
        "progress_current_pct": fibonacci.get("progress_current_pct"),
        "target_kind": fibonacci.get("target_kind"),
        "retracement_levels": fibonacci.get("retracement_levels", []),
        "extension_levels": fibonacci.get("extension_levels", []),
        "target_levels": fibonacci.get("target_levels", []),
        "levels": [
            {"name": "23.6%", "price": fibonacci.get("fib_236")},
            {"name": "38.2%", "price": fibonacci.get("fib_382")},
            {"name": "50.0%", "price": fibonacci.get("fib_500")},
            {"name": "61.8%", "price": fibonacci.get("fib_618")},
            {"name": "78.6%", "price": fibonacci.get("fib_786")},
        ],
    }


def compose_setup_direction(
    *,
    interval: str,
    technicals: TradeSetupTechnicals,
    confirmation: EmaConfirmation,
    confirmation_interval: str,
    primary_retests: Sequence[Mapping[str, Any]],
    moving_average: Mapping[str, Any] | None,
    momentum_pullback: Mapping[str, Any] | None,
    bearish_bounce: Mapping[str, Any] | None,
    gaps: Sequence[Mapping[str, Any]],
    fair_value_gaps: Sequence[Mapping[str, Any]],
    golden_cross: Mapping[str, Any] | None,
    volume_pivot_zones: Sequence[Mapping[str, Any]] = (),
) -> DirectionComposition:
    last_close = float(technicals.close[-1])
    bull_signals = 0
    bear_signals = 0
    signal_reasons = []

    if moving_average:
        signal = moving_average["signal"]
        if signal in ("Bullish Crossover", "Recent Bullish", "Above MA"):
            bull_signals += 2 if "Crossover" in signal else 1
            signal_reasons.append(
                f'MA: {signal} ({moving_average.get("days_since_cross", "?")}d ago)'
            )
        elif signal in ("Bearish Crossover", "Recent Bearish", "Below MA"):
            bear_signals += 2 if "Crossover" in signal else 1
            signal_reasons.append(
                f'MA: {signal} ({moving_average.get("days_since_cross", "?")}d ago)'
            )
        higher_timeframe_signal = moving_average.get("weekly_signal")
        if interval not in ("1wk", "1mo"):
            if higher_timeframe_signal in (
                "W-Above", "W-Bullish Cross", "D-Above", "D-Bullish Cross",
            ):
                bull_signals += 1
                signal_reasons.append(f"Higher timeframe: {higher_timeframe_signal}")
            elif higher_timeframe_signal in (
                "W-Below", "W-Bearish Cross", "D-Below", "D-Bearish Cross",
            ):
                bear_signals += 1
                signal_reasons.append(f"Higher timeframe: {higher_timeframe_signal}")

    if technicals.ema_bullish_stack:
        bull_signals += 1
        signal_reasons.append("EMA Stack: 8 > 21 > 50 (bullish alignment)")
    elif technicals.ema_bearish_stack:
        bear_signals += 1
        signal_reasons.append("EMA Stack: 8 < 21 < 50 (bearish alignment)")

    if last_close > technicals.ema8[-1] * 1.005:
        bull_signals += 1
        signal_reasons.append(f"Price above 8 EMA by {technicals.distance_to_ema8}%")
    elif last_close < technicals.ema8[-1] * 0.995:
        bear_signals += 1
        signal_reasons.append(
            f"Price below 8 EMA by {abs(technicals.distance_to_ema8)}%"
        )

    if last_close > technicals.vwap:
        bull_signals += 1
        signal_reasons.append(f"Price above VWAP(20) ${technicals.vwap}")
    else:
        bear_signals += 1
        signal_reasons.append(f"Price below VWAP(20) ${technicals.vwap}")

    if confirmation.alignment:
        if (
            confirmation.alignment == "Bullish"
            and technicals.ema_alignment in ("Bullish Stack", "Short-term Bullish")
        ):
            bull_signals += 1
            signal_reasons.append(
                f"Multi-TF: {confirmation_interval} 8/21 EMA aligns bullish with {interval}"
            )
        elif (
            confirmation.alignment == "Bearish"
            and technicals.ema_alignment in ("Bearish Stack", "Short-term Bearish")
        ):
            bear_signals += 1
            signal_reasons.append(
                f"Multi-TF: {confirmation_interval} 8/21 EMA aligns bearish with {interval}"
            )

    for retest in primary_retests:
        if (
            retest["held"] and retest["bounce_pct"] > 0
            and retest["source"] in ("Moving Average", "Gap", "FVG")
        ):
            bull_signals += 1
            signal_reasons.append(
                f'Retest Held: {retest["level_name"]} (${retest["level_price"]}, '
                f'bounced +{retest["bounce_pct"]}%)'
            )
            break
    for retest in primary_retests:
        if (
            retest["held"] and retest["bounce_pct"] < 0
            and retest["source"] in ("Moving Average", "Gap", "FVG")
        ):
            bear_signals += 1
            signal_reasons.append(
                f'Retest Rejected: {retest["level_name"]} '
                f'(${retest["level_price"]}, rejected {retest["bounce_pct"]}%)'
            )
            break

    if momentum_pullback:
        grade = momentum_pullback.get("grade", "C")
        bull_signals += 2 if grade in ("A+", "A") else 1
        signal_reasons.append(
            f'Momentum Pullback: Grade {grade} '
            f'(score {momentum_pullback.get("score", 0)})'
        )
    if bearish_bounce:
        grade = bearish_bounce.get("grade", "C")
        bear_signals += 2 if grade in ("A+", "A") else 1
        signal_reasons.append(
            f'Bearish Bounce: Grade {grade} '
            f'(score {bearish_bounce.get("score", 0)})'
        )

    support_gaps = tuple(gap for gap in gaps if "Support" in gap.get("gap_type", ""))
    resistance_gaps = tuple(
        gap for gap in gaps if "Resistance" in gap.get("gap_type", "")
    )
    if support_gaps:
        bull_signals += 1
        nearest = min(
            support_gaps, key=lambda gap: abs(gap["last_close"] - gap["gap_high"])
        )
        signal_reasons.append(
            f'Gap Support: ${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}'
        )
    if resistance_gaps:
        bear_signals += 1
        nearest = min(
            resistance_gaps, key=lambda gap: abs(gap["last_close"] - gap["gap_low"])
        )
        signal_reasons.append(
            f'Gap Resistance: ${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}'
        )

    bull_fvgs = tuple(
        item for item in fair_value_gaps
        if item.get("fvg_type") == "Bullish FVG" and item.get("status") == "Unmitigated"
    )
    bear_fvgs = tuple(
        item for item in fair_value_gaps
        if item.get("fvg_type") == "Bearish FVG" and item.get("status") == "Unmitigated"
    )
    if bull_fvgs:
        bull_signals += 1
        signal_reasons.append(
            f"Bullish FVGs: {len(bull_fvgs)} unmitigated demand zone(s)"
        )
    if bear_fvgs:
        bear_signals += 1
        signal_reasons.append(
            f"Bearish FVGs: {len(bear_fvgs)} unmitigated supply zone(s)"
        )

    zones = []
    for gap in sorted(
        support_gaps, key=lambda item: abs(item["last_close"] - item["gap_high"])
    )[:3]:
        zones.append({
            "name": "Gap support", "low": round(float(gap["gap_low"]), 2),
            "high": round(float(gap["gap_high"]), 2), "source": "Gap",
            "qualifier": gap.get("gap_type", ""),
        })
    for gap in sorted(
        resistance_gaps, key=lambda item: abs(item["last_close"] - item["gap_low"])
    )[:3]:
        zones.append({
            "name": "Gap resistance", "low": round(float(gap["gap_low"]), 2),
            "high": round(float(gap["gap_high"]), 2), "source": "Gap",
            "qualifier": gap.get("gap_type", ""),
        })
    for item in sorted(
        bull_fvgs, key=lambda value: abs(last_close - value["fvg_high"])
    )[:3]:
        zones.append({
            "name": "FVG demand zone", "low": round(float(item["fvg_low"]), 2),
            "high": round(float(item["fvg_high"]), 2), "source": "FVG",
            "qualifier": "Unmitigated",
        })
    for item in sorted(
        bear_fvgs, key=lambda value: abs(last_close - value["fvg_low"])
    )[:3]:
        zones.append({
            "name": "FVG supply zone", "low": round(float(item["fvg_low"]), 2),
            "high": round(float(item["fvg_high"]), 2), "source": "FVG",
            "qualifier": "Unmitigated",
        })
    zones.extend(dict(zone) for zone in volume_pivot_zones)

    if technicals.rsi < 35:
        bull_signals += 1
        signal_reasons.append(f"RSI: {technicals.rsi} (oversold — bounce potential)")
    elif technicals.rsi > 65:
        bear_signals += 1
        signal_reasons.append(f"RSI: {technicals.rsi} (overbought — pullback risk)")

    if golden_cross:
        if golden_cross["type"] == "Golden Cross":
            bull_signals += 2
            signal_reasons.append(f'Golden Cross: {golden_cross["detail"]}')
        elif golden_cross["type"] == "Death Cross":
            bear_signals += 2
            signal_reasons.append(f'Death Cross: {golden_cross["detail"]}')

    total_signals = bull_signals + bear_signals
    if total_signals == 0:
        direction = "Neutral"
        conviction = "None"
    elif bull_signals > bear_signals:
        ratio = bull_signals / total_signals
        direction = "Bullish"
        conviction = "High" if ratio >= 0.75 else "Moderate" if ratio >= 0.6 else "Low"
    elif bear_signals > bull_signals:
        ratio = bear_signals / total_signals
        direction = "Bearish"
        conviction = "High" if ratio >= 0.75 else "Moderate" if ratio >= 0.6 else "Low"
    else:
        direction = "Neutral"
        conviction = "Conflicted"

    return DirectionComposition(
        direction=direction,
        conviction=conviction,
        bull_signals=bull_signals,
        bear_signals=bear_signals,
        signal_reasons=tuple(signal_reasons),
        support_gaps=support_gaps,
        resistance_gaps=resistance_gaps,
        bull_fvgs=bull_fvgs,
        bear_fvgs=bear_fvgs,
        zones=tuple(zones),
    )


def compose_trade_levels(
    *,
    interval: str,
    technicals: TradeSetupTechnicals,
    direction: DirectionComposition,
    primary_retests: Sequence[Mapping[str, Any]],
    momentum_pullback: Mapping[str, Any] | None,
    bearish_bounce: Mapping[str, Any] | None,
    fibonacci: Mapping[str, Any] | None,
    directional_brackets: bool = False,
) -> TradeLevelComposition:
    last_close = float(technicals.close[-1])
    entries = []
    if direction.direction == "Bullish":
        if abs(technicals.distance_to_ema8) < 0.5:
            entries.append({
                "strategy": "8 EMA Retest",
                "condition": (
                    f"Price at 8 EMA (${technicals.ema8_value}) — pullback entry in trend"
                ),
                "price_zone": f"${technicals.ema8_value}",
                "zone_low": technicals.ema8_value,
                "zone_high": technicals.ema8_value,
                "strength": "Strong" if technicals.ema_bullish_stack else "Moderate",
            })
        elif abs(technicals.distance_to_ema21) < 0.8:
            entries.append({
                "strategy": "21 EMA Retest",
                "condition": (
                    f"Price near 21 EMA (${technicals.ema21_value}) — deeper pullback entry"
                ),
                "price_zone": f"${technicals.ema21_value}",
                "zone_low": technicals.ema21_value,
                "zone_high": technicals.ema21_value,
                "strength": "Strong" if technicals.ema_bullish_stack else "Moderate",
            })
        if momentum_pullback and momentum_pullback.get("grade") in ("A+", "A", "B+"):
            ema21 = momentum_pullback.get("ema21")
            entries.append({
                "strategy": "Momentum Pullback",
                "condition": (
                    f'Stoch %K at {momentum_pullback.get("stoch_k", "?")} '
                    "(oversold in uptrend)"
                ),
                "price_zone": f"Near EMA21 ~${ema21:.2f}" if ema21 else "Near EMA21",
                "zone_low": round(float(ema21), 2) if ema21 else None,
                "zone_high": round(float(ema21), 2) if ema21 else None,
                "strength": momentum_pullback.get("grade", "?"),
            })
        if direction.bull_fvgs:
            nearest = max(direction.bull_fvgs, key=lambda item: item["fvg_low"])
            entries.append({
                "strategy": "FVG Demand Zone",
                "condition": (
                    f'Price enters ${nearest["fvg_low"]:.2f}–${nearest["fvg_high"]:.2f}'
                ),
                "price_zone": f'${nearest["fvg_low"]:.2f}–${nearest["fvg_high"]:.2f}',
                "zone_low": round(float(nearest["fvg_low"]), 2),
                "zone_high": round(float(nearest["fvg_high"]), 2),
                "strength": (
                    "Unmitigated" if nearest.get("trend_aligned") else "Counter-trend"
                ),
            })
        if direction.support_gaps:
            nearest = min(
                direction.support_gaps,
                key=lambda gap: abs(gap["last_close"] - gap["gap_high"]),
            )
            entries.append({
                "strategy": "Gap Support",
                "condition": (
                    f'Price tests gap zone at ${nearest["gap_low"]:.2f}–'
                    f'${nearest["gap_high"]:.2f}'
                ),
                "price_zone": f'${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}',
                "zone_low": round(float(nearest["gap_low"]), 2),
                "zone_high": round(float(nearest["gap_high"]), 2),
                "strength": (
                    "Unfilled" if "Unfilled" in nearest.get("gap_type", "")
                    else "Filled retest"
                ),
            })
        if fibonacci and fibonacci.get("trend_direction") == "uptrend_retracement":
            fib_targets = fibonacci.get("support_targets", [])
            if fib_targets:
                nearest = fib_targets[0]
                entries.append({
                    "strategy": "Fibonacci Support",
                    "condition": (
                        f'Near {nearest.get("level", "?")} at '
                        f'${nearest.get("price", 0):.2f}'
                    ),
                    "price_zone": f'${nearest.get("price", 0):.2f}',
                    "zone_low": round(float(nearest.get("price", 0)), 2),
                    "zone_high": round(float(nearest.get("price", 0)), 2),
                    "strength": nearest.get("level", "?"),
                })
    elif direction.direction == "Bearish":
        if abs(technicals.distance_to_ema8) < 0.5:
            entries.append({
                "strategy": "8 EMA Rejection",
                "condition": (
                    f"Price at 8 EMA (${technicals.ema8_value}) — "
                    "rejection/short entry in downtrend"
                ),
                "price_zone": f"${technicals.ema8_value}",
                "zone_low": technicals.ema8_value,
                "zone_high": technicals.ema8_value,
                "strength": "Strong" if technicals.ema_bearish_stack else "Moderate",
            })
        elif abs(technicals.distance_to_ema21) < 0.8:
            entries.append({
                "strategy": "21 EMA Rejection",
                "condition": (
                    f"Price near 21 EMA (${technicals.ema21_value}) — "
                    "deeper bounce rejection"
                ),
                "price_zone": f"${technicals.ema21_value}",
                "zone_low": technicals.ema21_value,
                "zone_high": technicals.ema21_value,
                "strength": "Strong" if technicals.ema_bearish_stack else "Moderate",
            })
        if bearish_bounce and bearish_bounce.get("grade") in ("A+", "A", "B+"):
            ema21 = bearish_bounce.get("ema21")
            entries.append({
                "strategy": "Bearish Bounce",
                "condition": (
                    f'Stoch %K at {bearish_bounce.get("stoch_k", "?")} '
                    "(overbought in downtrend)"
                ),
                "price_zone": f"Near EMA21 ~${ema21:.2f}" if ema21 else "Near EMA21",
                "zone_low": round(float(ema21), 2) if ema21 else None,
                "zone_high": round(float(ema21), 2) if ema21 else None,
                "strength": bearish_bounce.get("grade", "?"),
            })
        if direction.bear_fvgs:
            nearest = min(direction.bear_fvgs, key=lambda item: item["fvg_high"])
            entries.append({
                "strategy": "FVG Supply Zone",
                "condition": (
                    f'Price enters ${nearest["fvg_low"]:.2f}–${nearest["fvg_high"]:.2f}'
                ),
                "price_zone": f'${nearest["fvg_low"]:.2f}–${nearest["fvg_high"]:.2f}',
                "zone_low": round(float(nearest["fvg_low"]), 2),
                "zone_high": round(float(nearest["fvg_high"]), 2),
                "strength": (
                    "Unmitigated" if nearest.get("trend_aligned") else "Counter-trend"
                ),
            })
        if direction.resistance_gaps:
            nearest = min(
                direction.resistance_gaps,
                key=lambda gap: abs(gap["last_close"] - gap["gap_low"]),
            )
            entries.append({
                "strategy": "Gap Resistance",
                "condition": (
                    f'Price tests gap zone at ${nearest["gap_low"]:.2f}–'
                    f'${nearest["gap_high"]:.2f}'
                ),
                "price_zone": f'${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}',
                "zone_low": round(float(nearest["gap_low"]), 2),
                "zone_high": round(float(nearest["gap_high"]), 2),
                "strength": (
                    "Unfilled" if "Unfilled" in nearest.get("gap_type", "")
                    else "Filled retest"
                ),
            })
        if fibonacci and fibonacci.get("trend_direction") == "downtrend_retracement":
            fib_targets = fibonacci.get("resistance_targets", [])
            if fib_targets:
                nearest = fib_targets[0]
                entries.append({
                    "strategy": "Fibonacci Resistance",
                    "condition": (
                        f'Near {nearest.get("level", "?")} at '
                        f'${nearest.get("price", 0):.2f}'
                    ),
                    "price_zone": f'${nearest.get("price", 0):.2f}',
                    "zone_low": round(float(nearest.get("price", 0)), 2),
                    "zone_high": round(float(nearest.get("price", 0)), 2),
                    "strength": nearest.get("level", "?"),
                })

    for retest in primary_retests[:2]:
        if retest["held"] and retest["bars_ago"] <= 2:
            action = "bounced" if retest["bounce_pct"] > 0 else "rejected"
            entries.append({
                "strategy": f'Level Retest ({retest["source"]})',
                "condition": (
                    f'{retest["level_name"]} at ${retest["level_price"]} — '
                    f'{retest["touch_type"]}, {action} {abs(retest["bounce_pct"])}%'
                ),
                "price_zone": f'H:{retest["candle_high"]} / L:{retest["candle_low"]}',
                "zone_low": retest["level_price"],
                "zone_high": retest["level_price"],
                "strength": "Strong" if abs(retest["bounce_pct"]) > 0.5 else "Moderate",
            })

    targets = []
    stops = []
    if fibonacci:
        for target in fibonacci.get("resistance_targets", [])[:2]:
            targets.append({
                "level": target.get("level", "?"),
                "price": round(target.get("price", 0), 2),
                "source": "Fibonacci",
            })
        for stop in fibonacci.get("support_targets", [])[:2]:
            stops.append({
                "level": stop.get("level", "?"),
                "price": round(stop.get("price", 0), 2),
                "source": "Fibonacci",
            })
        for extension in fibonacci.get("upside_extensions", []):
            targets.append({
                "level": extension.get("level", "?"),
                "price": round(extension.get("price", 0), 2),
                "source": "Fib Extension",
            })
    if direction.resistance_gaps:
        nearest = min(
            direction.resistance_gaps,
            key=lambda gap: abs(gap["last_close"] - gap["gap_low"]),
        )
        targets.append({
            "level": "Gap Resistance",
            "price": round(nearest["gap_low"], 2),
            "source": "Gap",
        })
    if direction.support_gaps:
        nearest = min(
            direction.support_gaps,
            key=lambda gap: abs(gap["last_close"] - gap["gap_high"]),
        )
        stops.append({
            "level": "Gap Support",
            "price": round(nearest["gap_low"], 2),
            "source": "Gap",
        })

    high = technicals.high
    low = technicals.low
    prior_bar_count = min(252, len(high) - 1)
    if prior_bar_count > 0:
        prior_slice = slice(-(prior_bar_count + 1), -1)
        prior_high = float(max(high[prior_slice]))
        prior_low = float(min(low[prior_slice]))
        range_label = (
            "52-Week" if interval == "1d"
            else f"{prior_bar_count}-Month" if interval == "1mo"
            else f"{prior_bar_count}-Week" if interval == "1wk"
            else f"{prior_bar_count}-Bar"
        )
        if prior_high > last_close:
            targets.append({
                "level": f"Prior {range_label} High",
                "price": round(prior_high, 2),
                "source": "Price Action",
            })
        if prior_low < last_close:
            stops.append({
                "level": f"Prior {range_label} Low",
                "price": round(prior_low, 2),
                "source": "Price Action",
            })

    targets.append({
        "level": "ATR Target (2R)",
        "price": round(last_close + 2 * technicals.atr14, 2),
        "source": "ATR",
    })
    stops.append({
        "level": "ATR Stop (1R)",
        "price": round(last_close - technicals.atr14, 2),
        "source": "ATR",
    })
    ema_below = [
        (value, name) for value, name in (
            (technicals.ema8_value, "8 EMA"),
            (technicals.ema21_value, "21 EMA"),
            (technicals.ema50_value, "50 EMA"),
        ) if value < last_close
    ]
    if ema_below:
        value, name = max(ema_below, key=lambda item: item[0])
        stops.append({"level": name, "price": value, "source": "EMA"})
    sma_below = [
        (value, name) for value, name in (
            (technicals.ma50, "50 SMA"), (technicals.ma200, "200 SMA"),
        ) if value and value < last_close
    ]
    if sma_below:
        value, name = max(sma_below, key=lambda item: item[0])
        stops.append({"level": name, "price": round(value, 2), "source": "SMA"})
    swing_lookback = min(20, len(low) - 2)
    for index in range(len(low) - 1, max(0, len(low) - swing_lookback) - 1, -1):
        if (
            index > 0 and index < len(low) - 1
            and low[index] < low[index - 1] and low[index] < low[index + 1]
            and float(low[index]) < last_close
        ):
            stops.append({
                "level": f"Swing Low ({len(low) - 1 - index}b ago)",
                "price": round(float(low[index]), 2),
                "source": "Price Action",
            })
            break
    if (
        technicals.vwap < last_close
        and (last_close - technicals.vwap) / technicals.vwap * 100 > 0.5
    ):
        stops.append({
            "level": "VWAP(20)", "price": technicals.vwap, "source": "VWAP",
        })

    seen_prices = set()
    unique_stops = []
    for stop in stops:
        price_key = round(stop["price"], 2)
        if price_key not in seen_prices:
            seen_prices.add(price_key)
            unique_stops.append(stop)
    targets.sort(key=lambda target: target["price"])
    unique_stops.sort(key=lambda stop: stop["price"], reverse=True)
    if directional_brackets:
        displayed_close = round(last_close, 2)
        if direction.direction == "Bullish":
            targets = _deduplicate_levels(
                [target for target in targets if target["price"] > displayed_close],
                reverse=False,
            )
            unique_stops = _deduplicate_levels(
                [stop for stop in unique_stops if stop["price"] < displayed_close],
                reverse=True,
            )
        elif direction.direction == "Bearish":
            candidates = [
                level for level in (*targets, *unique_stops)
                if level.get("source") != "ATR"
            ]
            targets = [
                level for level in candidates
                if level["price"] < displayed_close
            ]
            unique_stops = [
                level for level in candidates
                if level["price"] > displayed_close
            ]
            targets.append({
                "level": "ATR Target (2R)",
                "price": round(last_close - 2 * technicals.atr14, 2),
                "source": "ATR",
            })
            unique_stops.append({
                "level": "ATR Stop (1R)",
                "price": round(last_close + technicals.atr14, 2),
                "source": "ATR",
            })
            targets = _deduplicate_levels(
                [target for target in targets if target["price"] < displayed_close],
                reverse=True,
            )
            unique_stops = _deduplicate_levels(
                [stop for stop in unique_stops if stop["price"] > displayed_close],
                reverse=False,
            )
        else:
            entries = []
            targets = []
            unique_stops = []
    return TradeLevelComposition(
        entries=tuple(entries),
        targets=tuple(targets),
        stops=tuple(unique_stops),
    )


def _deduplicate_levels(
    levels: Sequence[Mapping[str, Any]],
    *,
    reverse: bool,
) -> list[dict[str, Any]]:
    seen_prices = set()
    result = []
    for level in sorted(levels, key=lambda item: item["price"], reverse=reverse):
        price_key = round(level["price"], 2)
        if price_key not in seen_prices:
            seen_prices.add(price_key)
            result.append(dict(level))
    return result


def compose_setup_timing(
    *,
    technicals: TradeSetupTechnicals,
    moving_average: Mapping[str, Any] | None,
    primary_retests: Sequence[Mapping[str, Any]],
    momentum_pullback: Mapping[str, Any] | None,
    bearish_bounce: Mapping[str, Any] | None,
) -> dict[str, str]:
    if moving_average and moving_average.get("days_since_cross") is not None:
        days_since = moving_average["days_since_cross"]
        if days_since <= 3:
            return {
                "urgency": "Immediate",
                "detail": f"Fresh MA crossover {days_since}d ago — entry window open now",
            }
        if days_since <= 7:
            return {
                "urgency": "This Week",
                "detail": (
                    f"Recent crossover {days_since}d ago — still early but monitor price action"
                ),
            }
        return {
            "urgency": "Watchlist",
            "detail": (
                f"Crossover was {days_since}d ago — wait for pullback to MA or new catalyst"
            ),
        }
    if any(
        retest["held"] and retest["bars_ago"] <= 1
        for retest in primary_retests
    ):
        return {
            "urgency": "Immediate",
            "detail": "Active level retest — price testing key level right now",
        }
    if momentum_pullback and momentum_pullback.get("grade") in ("A+", "A"):
        return {
            "urgency": "Immediate",
            "detail": "A-grade pullback setup active — optimal entry window",
        }
    if bearish_bounce and bearish_bounce.get("grade") in ("A+", "A"):
        return {
            "urgency": "Immediate",
            "detail": "A-grade bearish bounce active — optimal short window",
        }
    if technicals.rsi < 30 or technicals.rsi > 70:
        return {
            "urgency": "Near-term",
            "detail": (
                f"RSI at extreme ({technicals.rsi}) — mean reversion likely within 1–5 days"
            ),
        }
    if abs(technicals.distance_to_ema8) < 0.3:
        return {
            "urgency": "This Week",
            "detail": "Price hugging 8 EMA — decision point imminent",
        }
    return {
        "urgency": "Watchlist",
        "detail": "No immediate catalyst — add to watchlist and wait for entry trigger",
    }


def compose_setup_duration(
    moving_average: Mapping[str, Any] | None,
) -> dict[str, str]:
    if moving_average and moving_average.get("ma_spread_pct") is not None:
        spread = abs(moving_average["ma_spread_pct"])
        if spread >= 3:
            return {
                "estimate": "Extended (weeks to months)",
                "detail": f"Wide MA spread ({spread:.1f}%) — strong trend likely continues",
            }
        if spread >= 1:
            return {
                "estimate": "Medium (days to weeks)",
                "detail": (
                    f"Moderate MA spread ({spread:.1f}%) — trend has room but watch for narrowing"
                ),
            }
        return {
            "estimate": "Short (1–5 days)",
            "detail": (
                f"Narrow MA spread ({spread:.1f}%) — crossover or reversal imminent"
            ),
        }
    return {
        "estimate": "Unknown",
        "detail": "Insufficient MA data to estimate duration",
    }


def compose_setup_confluence(signal_reasons: Sequence[str]) -> dict[str, Any]:
    count = len(signal_reasons)
    grade = (
        "A+" if count >= 7
        else "A" if count >= 5
        else "B+" if count >= 4
        else "B" if count >= 3
        else "B-" if count >= 2
        else "C"
    )
    return {"grade": grade, "count": count}
