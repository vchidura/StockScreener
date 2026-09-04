"""Pure finalized-bar equity feature, scanner, pattern, and setup materialization."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np
import pandas as pd
import exchange_calendars

from research.composite_scanners import build_all_scanner_events
from research.forming_patterns import detect_forming_patterns
from research.price_channels import detect_price_channel
from research.price_structures import analyze_price_structures
from screeners import (
    calculate_fibonacci_swing_pct,
    scan_bearish_bounce,
    scan_fair_value_gaps,
    scan_fibonacci,
    scan_gap_strategies,
    scan_momentum_pullback,
    scan_moving_average_crossover,
)

from .domain import (
    EquityBarRevision,
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from .polygon import canonical_json, sha256_json
from .setup_composition import (
    compose_setup_confluence,
    compose_setup_direction,
    compose_setup_duration,
    compose_setup_timing,
    compose_trade_levels,
    compose_fibonacci_context,
)
from .technicals import (
    EmaConfirmation,
    assess_momentum,
    compute_ema_confirmation,
    compute_trade_setup_technicals,
    detect_golden_cross,
    detect_level_retests,
    detect_setup_candlesticks,
)


FEATURE_VERSION = "equity_features_v1"
SCANNER_BUNDLE_VERSION = "equity_scanner_bundle_v4"
PATTERN_VERSION = "forming_patterns_v1"
CHANNEL_VERSION = "price_channels_v1"
SETUP_VERSION = "equity_setup_v13"
CONFIRMATION_VERSION = "ema_confirmation_1h_v2"
PERSISTED_CONFIRMATION_VERSIONS = {
    "1h": "ema_confirmation_1h_persisted_v1",
    "1d": "ema_confirmation_1d_persisted_v1",
    "1wk": "ema_confirmation_1wk_persisted_v1",
}
FUNDAMENTAL_SNAPSHOT_VERSION = "fundamental_snapshot_v2"
PAYLOAD_SCHEMA_VERSION = "1.0"
SCANNER_INTERVALS = frozenset(("30m", "1h", "1d", "1wk"))
SETUP_INTERVALS = frozenset(("30m", "1h", "1d", "1wk", "1mo"))
FUNDAMENTAL_INTERVALS = frozenset(("1d",))
PORTAL_STRATEGY_INTERVALS = frozenset(("30m", "1h", "1d", "1wk"))
GAP_STRATEGY_INTERVALS = frozenset(("30m", "1h", "1d"))
PORTAL_STRATEGY_VERSION = "portal_strategy_bundle_v3"


@dataclass(frozen=True, slots=True)
class PortalStrategyResults:
    gaps: tuple[dict[str, Any], ...]
    fair_value_gaps: tuple[dict[str, Any], ...]
    moving_average: dict[str, Any] | None
    momentum_pullback: dict[str, Any] | None
    bearish_bounce: dict[str, Any] | None
    fibonacci: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    evidence: tuple[EquityEvidence, ...]
    ema_direction: str | None
    setup_direction: str
    quality_codes: tuple[str, ...]


def bars_to_frame(bars: Sequence[EquityBarRevision]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    return pd.DataFrame(
        {
            "open": [float(row.open_price) for row in bars],
            "high": [float(row.high_price) for row in bars],
            "low": [float(row.low_price) for row in bars],
            "close": [float(row.close_price) for row in bars],
            "volume": [float(row.volume) for row in bars],
        },
        index=pd.DatetimeIndex([row.bar_start for row in bars]),
    ).sort_index()


def derive_session_hourly_frame(
    bars: Sequence[EquityBarRevision],
    *,
    calendar_name: str = "XNYS",
) -> tuple[pd.DataFrame, tuple[UUID, ...]]:
    columns = ["open", "high", "low", "close", "volume"]
    if not bars:
        return pd.DataFrame(columns=columns), ()
    if any(row.interval != "30m" for row in bars):
        raise ValueError("session-hour derivation requires 30m source bars")
    calendar = exchange_calendars.get_calendar(calendar_name)
    by_session: dict[date, list[EquityBarRevision]] = {}
    for row in bars:
        if row.is_final:
            by_session.setdefault(row.session_date, []).append(row)

    aggregates = []
    aggregate_starts = []
    source_ids = []
    for session_date, session_bars in sorted(by_session.items()):
        session = pd.Timestamp(session_date)
        if not calendar.is_session(session):
            continue
        session_open = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
        session_close = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        sources_by_start = {
            row.bar_start: row
            for row in session_bars
            if row.bar_start >= session_open and row.bar_end <= session_close
        }
        bucket_start = session_open
        while bucket_start < session_close:
            bucket_end = min(bucket_start + timedelta(hours=1), session_close)
            expected_starts = []
            source_start = bucket_start
            while source_start < bucket_end:
                expected_starts.append(source_start)
                source_start += timedelta(minutes=30)
            bucket_sources = [
                sources_by_start.get(source_start)
                for source_start in expected_starts
            ]
            if all(bucket_sources) and all(
                row.bar_end == min(row.bar_start + timedelta(minutes=30), bucket_end)
                for row in bucket_sources
            ):
                complete_sources = [row for row in bucket_sources if row is not None]
                aggregates.append({
                    "open": float(complete_sources[0].open_price),
                    "high": max(float(row.high_price) for row in complete_sources),
                    "low": min(float(row.low_price) for row in complete_sources),
                    "close": float(complete_sources[-1].close_price),
                    "volume": sum(float(row.volume) for row in complete_sources),
                })
                aggregate_starts.append(bucket_start)
                source_ids.extend(row.bar_revision_id for row in complete_sources)
            bucket_start += timedelta(hours=1)
    return (
        pd.DataFrame(aggregates, index=pd.DatetimeIndex(aggregate_starts), columns=columns),
        tuple(source_ids),
    )


def materialize_equity_evidence(
    *,
    analysis_run_id: UUID,
    security: SecurityReferenceRevision,
    interval: str,
    bars: Sequence[EquityBarRevision],
    confirmation_bars: Sequence[EquityBarRevision] = (),
    observed_at: datetime,
    fundamental_metrics: Mapping[str, Any] | None = None,
    fundamental_report_ids: Sequence[UUID] = (),
    robust_qualifications: Mapping[
        tuple[str, str, str | None, int | None], UUID
    ] | None = None,
) -> MaterializationResult:
    if not bars:
        return MaterializationResult((), None, "UNAVAILABLE", ("NO_FINAL_BARS",))
    observed_utc = _utc(observed_at)
    latest = bars[-1]
    if not latest.is_final or latest.bar_end > observed_utc:
        raise ValueError("materialization requires a latest finalized bar")
    if any(row.ticker != security.ticker or row.interval != interval for row in bars):
        raise ValueError("all bars must match the security ticker and interval")
    confirmation_interval = {
        "1mo": "1wk", "1wk": "1d", "1d": "1h", "1h": "1d",
    }.get(interval, "1h")
    if confirmation_bars and any(
        row.ticker != security.ticker or row.interval != confirmation_interval
        for row in confirmation_bars
    ):
        raise ValueError(
            "all confirmation bars must match the security and confirmation interval"
        )

    frame = bars_to_frame(bars)
    feature_payload, ema_direction, feature_quality = _feature_payload(
        frame, fundamental_metrics or {}
    )
    evidence: list[EquityEvidence] = [
        _evidence(
            analysis_run_id=analysis_run_id,
            security=security,
            interval=interval,
            evidence_type=EvidenceType.FEATURE_SNAPSHOT,
            evidence_role=EvidenceRole.REGIME,
            lifecycle_key=f"feature:{security.ticker}:{interval}",
            lifecycle_status=LifecycleStatus.SNAPSHOT,
            direction=_direction_value(ema_direction),
            strength=None,
            market_time=latest.bar_end,
            observed_at=observed_utc,
            source_name="EQUITY_FEATURES",
            source_version=FEATURE_VERSION,
            latest_bar_revision_id=latest.bar_revision_id,
            source_revision_ids=tuple(row.bar_revision_id for row in bars),
            fundamental_report_ids=tuple(fundamental_report_ids),
            quality_state=(
                QualityState.COMPLETE if not feature_quality else QualityState.DEGRADED
            ),
            quality_codes=feature_quality,
            payload=feature_payload,
        )
    ]

    if fundamental_metrics and interval in FUNDAMENTAL_INTERVALS:
        evidence.append(
            _evidence(
                analysis_run_id=analysis_run_id,
                security=security,
                interval=None,
                evidence_type=EvidenceType.FUNDAMENTAL_SNAPSHOT,
                evidence_role=EvidenceRole.RISK,
                lifecycle_key=f"fundamental:{security.ticker}",
                lifecycle_status=LifecycleStatus.SNAPSHOT,
                direction=None,
                strength=None,
                market_time=latest.bar_end,
                observed_at=observed_utc,
                source_name="FUNDAMENTAL_DERIVATION",
                source_version=FUNDAMENTAL_SNAPSHOT_VERSION,
                latest_bar_revision_id=latest.bar_revision_id,
                source_revision_ids=(security.security_revision_id,),
                fundamental_report_ids=tuple(fundamental_report_ids),
                quality_state=QualityState.COMPLETE,
                quality_codes=(),
                payload=dict(fundamental_metrics),
            )
        )

    scanner_rows = pd.DataFrame()
    if interval in SCANNER_INTERVALS:
        panel = frame.reset_index(names="date")
        panel.insert(0, "ticker", security.ticker)
        scanner_rows = build_all_scanner_events(panel, interval)
    if not scanner_rows.empty:
        latest_rows = scanner_rows[
            pd.to_datetime(scanner_rows["date"], utc=True) == pd.Timestamp(latest.bar_start)
        ]
        for _, row in latest_rows.iterrows():
            metadata = _json_dict(row.get("metadata"))
            scanner_payload = {
                "atr_at_signal": _finite_or_none(row.get("atr_at_signal")),
                "entry_price": _finite_or_none(row.get("entry_price")),
                "metadata": metadata,
                "reference_level": _finite_or_none(row.get("reference_level")),
                "setup_anchor": str(row.get("setup_anchor") or ""),
                "stop_price": _finite_or_none(row.get("stop_price")),
                "target_price": _finite_or_none(row.get("target_price")),
                "trigger_type": str(row.get("trigger_type") or ""),
            }
            qualification_id = (robust_qualifications or {}).get((
                str(row["scanner_name"]), str(row["scanner_version"]),
                interval, int(row["direction"]),
            ))
            evidence.append(
                _evidence(
                    analysis_run_id=analysis_run_id,
                    security=security,
                    interval=interval,
                    evidence_type=EvidenceType.SCANNER_RESULT,
                    evidence_role=EvidenceRole.DIRECTION,
                    lifecycle_key=(
                        f"scanner:{row['scanner_name']}:{row['scanner_version']}:"
                        f"{security.ticker}:{interval}:{row.get('setup_anchor')}"
                    ),
                    lifecycle_status=LifecycleStatus.MATCH,
                    direction=int(row["direction"]),
                    strength=None,
                    market_time=latest.bar_end,
                    observed_at=observed_utc,
                    source_name=str(row["scanner_name"]),
                    source_version=str(row["scanner_version"]),
                    latest_bar_revision_id=latest.bar_revision_id,
                    source_revision_ids=tuple(row.bar_revision_id for row in bars),
                    fundamental_report_ids=tuple(fundamental_report_ids),
                    quality_state=(
                        QualityState.COMPLETE
                        if qualification_id else QualityState.RESEARCH_ONLY
                    ),
                    quality_codes=(
                        () if qualification_id else ("UNQUALIFIED_DIRECTION",)
                    ),
                    payload=scanner_payload,
                    qualification_revision_id=qualification_id,
                )
            )

    portal_strategies = _portal_strategy_results(
        security.ticker, frame, interval
    )
    portal_evidence = (
        ("GAP_STRATEGIES", EvidenceRole.LOCATION, None, {"results": portal_strategies.gaps}),
        ("FAIR_VALUE_GAPS", EvidenceRole.LOCATION, None, {"results": portal_strategies.fair_value_gaps}),
        (
            "MOVING_AVERAGE_CROSSOVER", EvidenceRole.DIRECTION,
            _moving_average_direction(portal_strategies.moving_average),
            portal_strategies.moving_average,
        ),
        (
            "MOMENTUM_PULLBACK", EvidenceRole.DIRECTION, 1,
            portal_strategies.momentum_pullback,
        ),
        (
            "BEARISH_BOUNCE", EvidenceRole.DIRECTION, -1,
            portal_strategies.bearish_bounce,
        ),
        (
            "FIBONACCI", EvidenceRole.LOCATION, None,
            portal_strategies.fibonacci,
        ),
    )
    for source_name, role, direction, payload in portal_evidence:
        if not payload or payload == {"results": ()}:
            continue
        evidence.append(
            _evidence(
                analysis_run_id=analysis_run_id,
                security=security,
                interval=interval,
                evidence_type=EvidenceType.SCANNER_RESULT,
                evidence_role=role,
                lifecycle_key=(
                    f"portal-strategy:{source_name}:{security.ticker}:{interval}"
                ),
                lifecycle_status=LifecycleStatus.MATCH,
                direction=direction,
                strength=None,
                market_time=latest.bar_end,
                observed_at=observed_utc,
                source_name=source_name,
                source_version=PORTAL_STRATEGY_VERSION,
                latest_bar_revision_id=latest.bar_revision_id,
                source_revision_ids=tuple(row.bar_revision_id for row in bars),
                fundamental_report_ids=tuple(fundamental_report_ids),
                quality_state=QualityState.RESEARCH_ONLY,
                quality_codes=(
                    ("LOCATION_ONLY",)
                    if role is EvidenceRole.LOCATION
                    else ("UNQUALIFIED_DIRECTION",)
                ),
                payload=payload,
            )
        )

    confirmation_frame = pd.DataFrame()
    confirmation = EmaConfirmation(None, None, None)
    confirmation_evidence = None
    if interval == "30m":
        confirmation_frame, confirmation_source_ids = derive_session_hourly_frame(bars)
        confirmation = compute_ema_confirmation(confirmation_frame)
        confirmation_version = CONFIRMATION_VERSION
        confirmation_policy = "XNYS_SESSION_ANCHORED_1H_V1"
        confirmation_quality_codes = (
            "DERIVED_FROM_30M", "UNQUALIFIED_CONFIRMATION",
        )
    elif confirmation_bars:
        confirmation_frame = bars_to_frame(confirmation_bars)
        confirmation_source_ids = tuple(
            row.bar_revision_id for row in confirmation_bars
        )
        confirmation = compute_ema_confirmation(confirmation_frame)
        confirmation_version = PERSISTED_CONFIRMATION_VERSIONS[confirmation_interval]
        confirmation_policy = f"PERSISTED_{confirmation_interval.upper()}_V1"
        confirmation_quality_codes = (
            f"PERSISTED_{confirmation_interval.upper()}_INPUT",
            "UNQUALIFIED_CONFIRMATION",
        )
    else:
        confirmation_source_ids = ()
        confirmation_version = None
        confirmation_policy = None
        confirmation_quality_codes = ()
    if confirmation.alignment and confirmation_source_ids and confirmation_version:
            source_by_id = {
                row.bar_revision_id: row for row in (*bars, *confirmation_bars)
            }
            latest_confirmation_source = source_by_id[confirmation_source_ids[-1]]
            confirmation_evidence = _evidence(
                analysis_run_id=analysis_run_id,
                security=security,
                interval=confirmation_interval,
                evidence_type=EvidenceType.REGIME_SIGNAL,
                evidence_role=EvidenceRole.REGIME,
                lifecycle_key=(
                    f"ema-confirmation:{security.ticker}:1h:derived-30m"
                    if interval == "30m" else
                    f"ema-confirmation:{security.ticker}:{confirmation_interval}:for-{interval}"
                ),
                lifecycle_status=LifecycleStatus.SNAPSHOT,
                direction=_direction_value(confirmation.alignment),
                strength=None,
                market_time=latest_confirmation_source.bar_end,
                observed_at=observed_utc,
                source_name="EMA_CONFIRMATION",
                source_version=confirmation_version,
                latest_bar_revision_id=latest_confirmation_source.bar_revision_id,
                source_revision_ids=confirmation_source_ids,
                fundamental_report_ids=tuple(fundamental_report_ids),
                quality_state=QualityState.RESEARCH_ONLY,
                quality_codes=confirmation_quality_codes,
                payload=(
                    {
                        "aggregation_policy": confirmation_policy,
                        "alignment": confirmation.alignment,
                        "ema8": confirmation.ema8,
                        "ema21": confirmation.ema21,
                        "hourly_bar_count": len(confirmation_frame),
                        "source_bar_count": len(confirmation_source_ids),
                        "source_interval": "30m",
                    }
                    if interval == "30m" else {
                        "aggregation_policy": confirmation_policy,
                        "alignment": confirmation.alignment,
                        "bar_count": len(confirmation_frame),
                        "ema8": confirmation.ema8,
                        "ema21": confirmation.ema21,
                        "source_bar_count": len(confirmation_source_ids),
                        "source_interval": confirmation_interval,
                    }
                ),
            )
            evidence.append(confirmation_evidence)

    patterns = detect_forming_patterns(
        frame,
        input_includes_forming_bar=False,
    )
    for pattern in patterns:
        bias = pattern.get("bias")
        evidence.append(
            _evidence(
                analysis_run_id=analysis_run_id,
                security=security,
                interval=interval,
                evidence_type=EvidenceType.PATTERN_OBSERVATION,
                evidence_role=EvidenceRole.TRIGGER,
                lifecycle_key=(
                    f"pattern:{security.ticker}:{interval}:{pattern.get('type')}:"
                    f"{pattern.get('start_time')}"
                ),
                lifecycle_status=_pattern_status(pattern),
                direction=_direction_value(bias),
                strength=_pattern_strength(pattern),
                market_time=latest.bar_end,
                observed_at=observed_utc,
                source_name=str(pattern.get("type") or "UNKNOWN_PATTERN"),
                source_version=PATTERN_VERSION,
                latest_bar_revision_id=latest.bar_revision_id,
                source_revision_ids=tuple(row.bar_revision_id for row in bars),
                fundamental_report_ids=tuple(fundamental_report_ids),
                quality_state=QualityState.RESEARCH_ONLY,
                quality_codes=("UNQUALIFIED_PATTERN",),
                payload=pattern,
            )
        )

    channel = detect_price_channel(frame)
    if channel:
        evidence.append(
            _evidence(
                analysis_run_id=analysis_run_id,
                security=security,
                interval=interval,
                evidence_type=EvidenceType.PRICE_CHANNEL,
                evidence_role=EvidenceRole.LOCATION,
                lifecycle_key=(
                    f"channel:{security.ticker}:{interval}:"
                    f"{channel.get('type')}:{channel.get('start_time')}"
                ),
                lifecycle_status=LifecycleStatus.FORMING,
                direction=_direction_value(channel.get("direction")),
                strength=None,
                market_time=latest.bar_end,
                observed_at=observed_utc,
                source_name="PRICE_CHANNEL",
                source_version=CHANNEL_VERSION,
                latest_bar_revision_id=latest.bar_revision_id,
                source_revision_ids=tuple(row.bar_revision_id for row in bars),
                fundamental_report_ids=tuple(fundamental_report_ids),
                quality_state=QualityState.RESEARCH_ONLY,
                quality_codes=("LOCATION_ONLY",),
                payload=channel,
            )
        )

    direction_rows = [
        row for row in evidence
        if row.evidence_type is EvidenceType.SCANNER_RESULT and row.direction in (-1, 1)
    ]
    if interval not in SETUP_INTERVALS:
        return MaterializationResult(
            evidence=tuple(evidence),
            ema_direction=ema_direction,
            setup_direction="UNAVAILABLE",
            quality_codes=feature_quality,
        )
    setup_technicals = compute_trade_setup_technicals(
        frame,
        interval,
        input_includes_forming_bar=False,
    )
    technical_levels = []
    for price, name, source in (
        (setup_technicals.ma50, "50 SMA", "Moving Average"),
        (setup_technicals.ma200, "200 SMA", "Moving Average"),
        (setup_technicals.ema8_value, "8 EMA", "EMA"),
        (setup_technicals.ema21_value, "21 EMA", "EMA"),
        (setup_technicals.vwap, "VWAP(20)", "VWAP"),
    ):
        if price is not None:
            technical_levels.append({"price": price, "name": name, "source": source})
    for gap in portal_strategies.gaps[:5]:
        technical_levels.extend((
            {
                "price": gap["gap_high"],
                "name": f'Gap High ({gap["gap_type"][:3]})',
                "source": "Gap",
            },
            {
                "price": gap["gap_low"],
                "name": f'Gap Low ({gap["gap_type"][:3]})',
                "source": "Gap",
            },
        ))
    for item in portal_strategies.fair_value_gaps[:5]:
        technical_levels.extend((
            {
                "price": item["fvg_high"],
                "name": f'FVG High ({item["fvg_type"][:4]})',
                "source": "FVG",
            },
            {
                "price": item["fvg_low"],
                "name": f'FVG Low ({item["fvg_type"][:4]})',
                "source": "FVG",
            },
        ))
    if portal_strategies.fibonacci:
        for target_list in (
            portal_strategies.fibonacci.get("support_targets", []),
            portal_strategies.fibonacci.get("resistance_targets", []),
        ):
            for target in target_list[:3]:
                technical_levels.append({
                    "price": target.get("price", 0),
                    "name": f'Fib {target.get("level", "?")}',
                    "source": "Fibonacci",
                })
    primary_retests = detect_level_retests(
        setup_technicals.close,
        setup_technicals.high,
        setup_technicals.low,
        technical_levels,
        lookback=5,
        tolerance_pct=0.5,
    )
    confirmation_retests = []
    if len(confirmation_frame) >= 10:
        confirmation_retests = detect_level_retests(
            confirmation_frame["close"].values.astype(float),
            confirmation_frame["high"].values.astype(float),
            confirmation_frame["low"].values.astype(float),
            technical_levels,
            lookback=10,
            tolerance_pct=0.3,
        )
    golden_cross = detect_golden_cross(setup_technicals)
    structure_fibonacci_levels = []
    if portal_strategies.fibonacci:
        structure_fibonacci_levels.extend(
            portal_strategies.fibonacci.get("retracement_levels", [])
        )
        active_leg = portal_strategies.fibonacci.get("active_leg")
        if active_leg:
            structure_fibonacci_levels.extend(active_leg.get("levels", []))
    structure_analysis = analyze_price_structures(frame, structure_fibonacci_levels)
    direction_composition = compose_setup_direction(
        interval=interval,
        technicals=setup_technicals,
        confirmation=confirmation,
        confirmation_interval=confirmation_interval,
        primary_retests=primary_retests,
        moving_average=portal_strategies.moving_average,
        momentum_pullback=portal_strategies.momentum_pullback,
        bearish_bounce=portal_strategies.bearish_bounce,
        gaps=portal_strategies.gaps,
        fair_value_gaps=portal_strategies.fair_value_gaps,
        golden_cross=golden_cross,
        volume_pivot_zones=structure_analysis["volume_pivot_zones"],
    )
    setup_direction = (
        "CONFLICTED"
        if direction_composition.conviction == "Conflicted"
        else direction_composition.direction.upper()
    )
    trade_levels = compose_trade_levels(
        interval=interval,
        technicals=setup_technicals,
        direction=direction_composition,
        primary_retests=primary_retests,
        momentum_pullback=portal_strategies.momentum_pullback,
        bearish_bounce=portal_strategies.bearish_bounce,
        fibonacci=portal_strategies.fibonacci,
        directional_brackets=True,
    )
    timing = compose_setup_timing(
        technicals=setup_technicals,
        moving_average=portal_strategies.moving_average,
        primary_retests=primary_retests,
        momentum_pullback=portal_strategies.momentum_pullback,
        bearish_bounce=portal_strategies.bearish_bounce,
    )
    setup_payload = {
        "ticker": security.ticker,
        "interval": interval,
        "date": latest.bar_start.strftime("%Y-%m-%d"),
        "last_close": round(float(latest.close_price), 2),
        "technicals": setup_technicals.payload(),
        "candlestick_patterns": detect_setup_candlesticks(
            frame, input_includes_forming_bar=False
        ),
        "structural_patterns": structure_analysis["patterns"],
        "ema_alignment": {
            "primary": setup_technicals.ema_alignment,
            "primary_detail": setup_technicals.ema_alignment_detail,
            "confirm_interval": confirmation_interval,
            "confirm": confirmation.alignment,
            "confirm_ema8": confirmation.ema8,
            "confirm_ema21": confirmation.ema21,
            "multi_tf_agree": (
                (
                    confirmation.alignment == "Bullish"
                    and setup_technicals.ema_alignment in (
                        "Bullish Stack", "Short-term Bullish",
                    )
                )
                or (
                    confirmation.alignment == "Bearish"
                    and setup_technicals.ema_alignment in (
                        "Bearish Stack", "Short-term Bearish",
                    )
                )
                if confirmation.alignment else None
            ),
        },
        "golden_cross": golden_cross,
        "level_retests": {
            "primary": primary_retests,
            "confirm": confirmation_retests[:5],
            "confirm_interval": confirmation_interval,
        },
        "momentum": assess_momentum(setup_technicals),
        "direction": {
            "bias": direction_composition.direction,
            "conviction": direction_composition.conviction,
            "bull_signals": direction_composition.bull_signals,
            "bear_signals": direction_composition.bear_signals,
        },
        "signals": list(direction_composition.signal_reasons),
        "zones": list(direction_composition.zones),
        "entries": list(trade_levels.entries),
        "targets": list(trade_levels.targets[:5]),
        "stops": list(trade_levels.stops[:5]),
        "timing": timing,
        "duration": compose_setup_duration(portal_strategies.moving_average),
        "confluence": compose_setup_confluence(
            direction_composition.signal_reasons
        ),
        "strategy_results": {
            "ma_crossover": ({
                "signal": portal_strategies.moving_average["signal"],
                "spread_pct": portal_strategies.moving_average.get("ma_spread_pct"),
                "days_since_cross": portal_strategies.moving_average.get("days_since_cross"),
                "weekly_signal": portal_strategies.moving_average.get("weekly_signal"),
                "markers": portal_strategies.moving_average.get("markers", []),
            } if portal_strategies.moving_average else None),
            "momentum_pullback": ({
                "grade": portal_strategies.momentum_pullback.get("grade"),
                "score": portal_strategies.momentum_pullback.get("score"),
            } if portal_strategies.momentum_pullback else None),
            "bearish_bounce": ({
                "grade": portal_strategies.bearish_bounce.get("grade"),
                "score": portal_strategies.bearish_bounce.get("score"),
            } if portal_strategies.bearish_bounce else None),
            "gaps": ({
                "support_count": len(direction_composition.support_gaps),
                "resistance_count": len(direction_composition.resistance_gaps),
            } if portal_strategies.gaps else None),
            "fvg": ({
                "bull_unmitigated": len(direction_composition.bull_fvgs),
                "bear_unmitigated": len(direction_composition.bear_fvgs),
                "total": len(portal_strategies.fair_value_gaps),
            } if portal_strategies.fair_value_gaps else None),
            "fibonacci": compose_fibonacci_context(portal_strategies.fibonacci),
        },
        "direction_state": setup_direction,
        "ema_direction": ema_direction,
        "setup_policy_version": SETUP_VERSION,
        "setup_policy_sha256": sha256_json({
            "conflict_rule": "EQUAL_DIRECTIONAL_VOTES",
            "direction_precedence": "PORTAL_STRATEGY_COMPOSITION",
            "version": SETUP_VERSION,
        }),
        "feature_evidence_id": str(evidence[0].evidence_id),
        "fundamental_context": dict(fundamental_metrics or {}),
        "pattern_evidence_ids": [
            str(row.evidence_id)
            for row in evidence
            if row.evidence_type is EvidenceType.PATTERN_OBSERVATION
        ],
        "scanner_evidence_ids": [str(row.evidence_id) for row in direction_rows],
    }
    evidence.append(
        _evidence(
            analysis_run_id=analysis_run_id,
            security=security,
            interval=interval,
            evidence_type=EvidenceType.TRADE_SETUP,
            evidence_role=EvidenceRole.SETUP,
            lifecycle_key=f"setup:{security.ticker}:{interval}",
            lifecycle_status=(
                LifecycleStatus.CONFLICTED
                if setup_direction == "CONFLICTED"
                else LifecycleStatus.SNAPSHOT
            ),
            direction=_direction_value(setup_direction),
            strength=None,
            market_time=latest.bar_end,
            observed_at=observed_utc,
            source_name="EQUITY_SETUP",
            source_version=SETUP_VERSION,
            latest_bar_revision_id=latest.bar_revision_id,
            source_revision_ids=tuple(row.evidence_id for row in evidence),
            fundamental_report_ids=tuple(fundamental_report_ids),
            quality_state=QualityState.RESEARCH_ONLY,
            quality_codes=("UNQUALIFIED_SETUP",),
            payload=setup_payload,
        )
    )
    return MaterializationResult(
        evidence=tuple(evidence),
        ema_direction=ema_direction,
        setup_direction=setup_direction,
        quality_codes=feature_quality,
    )


def _portal_strategy_results(
    ticker: str,
    frame: pd.DataFrame,
    interval: str,
) -> PortalStrategyResults:
    if interval not in PORTAL_STRATEGY_INTERVALS:
        return PortalStrategyResults((), (), None, None, None)
    return PortalStrategyResults(
        gaps=(
            tuple(scan_gap_strategies(ticker, frame, interval=interval))
            if len(frame) >= 20 and interval in GAP_STRATEGY_INTERVALS else ()
        ),
        fair_value_gaps=(
            tuple(scan_fair_value_gaps(ticker, frame)) if len(frame) >= 20 else ()
        ),
        moving_average=(
            scan_moving_average_crossover(ticker, frame, interval=interval)
            if len(frame) >= 26 else None
        ),
        momentum_pullback=(
            scan_momentum_pullback(ticker, frame, interval=interval)
            if len(frame) >= 100 else None
        ),
        bearish_bounce=(
            scan_bearish_bounce(ticker, frame, interval=interval)
            if len(frame) >= 100 else None
        ),
        fibonacci=(
            scan_fibonacci(
                ticker,
                frame,
                min_swing_pct=calculate_fibonacci_swing_pct(frame, interval),
            )
            if len(frame) >= 50 else None
        ),
    )


def _moving_average_direction(result: Mapping[str, Any] | None) -> int | None:
    if not result:
        return None
    signal = str(result.get("signal") or "")
    if signal in ("Bullish Crossover", "Recent Bullish", "Above MA"):
        return 1
    if signal in ("Bearish Crossover", "Recent Bearish", "Below MA"):
        return -1
    return None


def _feature_payload(
    frame: pd.DataFrame,
    fundamental_metrics: Mapping[str, Any],
) -> tuple[dict[str, Any], str | None, tuple[str, ...]]:
    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    volume = frame["volume"].astype(float)
    ema8 = close.ewm(span=8, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    atr14 = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    delta = close.diff()
    average_gain = delta.clip(lower=0).rolling(14).mean()
    average_loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi14 = 100 - 100 / (1 + average_gain / average_loss.replace(0, np.nan))
    volume_mean = volume.shift(1).rolling(20).mean()
    volume_ratio = volume / volume_mean.replace(0, np.nan)
    ema_direction = (
        "BULLISH" if ema8.iloc[-1] > ema21.iloc[-1] > ema50.iloc[-1]
        else "BEARISH" if ema8.iloc[-1] < ema21.iloc[-1] < ema50.iloc[-1]
        else "NEUTRAL"
    )
    quality = []
    if len(frame) < 50:
        quality.append("FEATURE_HISTORY_SHORT")
    free_float = _finite_or_none(fundamental_metrics.get("free_float"))
    float_turnover = (
        float(volume.iloc[-1] / free_float)
        if free_float is not None and free_float > 0 else None
    )
    return {
        "atr14": _finite_or_none(atr14.iloc[-1]),
        "close": _finite_or_none(close.iloc[-1]),
        "ema8": _finite_or_none(ema8.iloc[-1]),
        "ema21": _finite_or_none(ema21.iloc[-1]),
        "ema50": _finite_or_none(ema50.iloc[-1]),
        "ema_direction": ema_direction,
        "float_turnover": float_turnover,
        "rsi14": _finite_or_none(rsi14.iloc[-1]),
        "sma20": _finite_or_none(close.rolling(20).mean().iloc[-1]),
        "sma50": _finite_or_none(close.rolling(50).mean().iloc[-1]),
        "sma200": _finite_or_none(close.rolling(200).mean().iloc[-1]),
        "volume": _finite_or_none(volume.iloc[-1]),
        "volume_ratio_20": _finite_or_none(volume_ratio.iloc[-1]),
    }, ema_direction, tuple(quality)


def _evidence(
    *,
    analysis_run_id: UUID,
    security: SecurityReferenceRevision,
    interval: str | None,
    evidence_type: EvidenceType,
    evidence_role: EvidenceRole,
    lifecycle_key: str,
    lifecycle_status: LifecycleStatus,
    direction: int | None,
    strength: float | None,
    market_time: datetime,
    observed_at: datetime,
    source_name: str,
    source_version: str,
    latest_bar_revision_id: UUID | None,
    source_revision_ids: tuple[UUID, ...],
    fundamental_report_ids: tuple[UUID, ...],
    quality_state: QualityState,
    quality_codes: tuple[str, ...],
    payload: Mapping[str, Any],
    qualification_revision_id: UUID | None = None,
) -> EquityEvidence:
    normalized_payload = _json_safe(dict(payload))
    payload_digest = sha256_json(normalized_payload)
    source_digest = sha256_json([str(value) for value in source_revision_ids])
    evidence_key = ":".join(
        [
            str(analysis_run_id),
            evidence_type.value,
            source_name,
            source_version,
            security.ticker,
            interval or "NONE",
            market_time.isoformat(),
            lifecycle_key,
            source_digest,
            payload_digest,
        ]
    )
    return EquityEvidence(
        evidence_id=uuid5(NAMESPACE_URL, f"equity-evidence:{evidence_key}"),
        evidence_key=evidence_key,
        lifecycle_key=lifecycle_key,
        evidence_type=evidence_type,
        evidence_role=evidence_role,
        security_id=security.security_id,
        ticker=security.ticker,
        interval=interval,
        direction=direction,
        lifecycle_status=lifecycle_status,
        strength=strength,
        market_time=market_time,
        observed_at=observed_at,
        valid_until=market_time + _validity(interval),
        source_name=source_name,
        source_version=source_version,
        payload_schema_version=PAYLOAD_SCHEMA_VERSION,
        analysis_run_id=analysis_run_id,
        latest_bar_revision_id=latest_bar_revision_id,
        security_revision_id=security.security_revision_id,
        fundamental_report_ids=fundamental_report_ids,
        source_revision_ids=source_revision_ids,
        quality_state=quality_state,
        quality_codes=quality_codes,
        qualification_revision_id=qualification_revision_id,
        payload_json=canonical_json(normalized_payload),
        payload_sha256=payload_digest,
    )


def _setup_direction(
    ema_direction: str | None,
    scanner_rows: Sequence[EquityEvidence],
) -> str:
    directions = {row.direction for row in scanner_rows if row.direction in (-1, 1)}
    if len(directions) > 1:
        return "CONFLICTED"
    scanner_direction = next(iter(directions), None)
    if scanner_direction is not None:
        scanner_label = "BULLISH" if scanner_direction == 1 else "BEARISH"
        if ema_direction in ("BULLISH", "BEARISH") and ema_direction != scanner_label:
            return "CONFLICTED"
        return scanner_label
    return ema_direction or "UNAVAILABLE"


def _pattern_status(pattern: Mapping[str, Any]) -> LifecycleStatus:
    readiness = pattern.get("readiness")
    if readiness == "AT_EDGE":
        return LifecycleStatus.AT_EDGE
    return LifecycleStatus.FORMING


def _pattern_strength(pattern: Mapping[str, Any]) -> float | None:
    distance = _finite_or_none(pattern.get("edge_distance_atr"))
    if distance is None:
        return None
    return float(max(0.0, min(1.0, 1.0 - distance / 2.0)))


def _direction_value(value: Any) -> int | None:
    normalized = value.upper() if isinstance(value, str) else value
    if normalized in (1, "BULLISH", "UP", "RISING"):
        return 1
    if normalized in (-1, "BEARISH", "DOWN", "FALLING"):
        return -1
    if normalized in (0, "NEUTRAL"):
        return 0
    return None


def _validity(interval: str | None) -> timedelta:
    return {
        "5m": timedelta(minutes=10),
        "15m": timedelta(minutes=30),
        "30m": timedelta(minutes=60),
        "1h": timedelta(hours=2),
        "1d": timedelta(days=2),
        "1wk": timedelta(days=8),
        "1mo": timedelta(days=35),
    }.get(interval, timedelta(days=2))


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return _json_safe(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return _json_safe(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _finite_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(timezone.utc)