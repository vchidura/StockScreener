"""Canonical compatibility reads for Stock Research and scanner APIs."""
from __future__ import annotations

import math
from typing import Any

from database import get_db_cursor


OUTCOME_ENTRY_MODEL = "NEXT_ACTIONABLE_BAR_OPEN_V1"


def qualification_report(interval: str | None = None) -> list[dict[str, Any]]:
    with get_db_cursor() as cursor:
        cursor.execute(
            r"""
            SELECT DISTINCT ON (
                revision.source_name, revision.source_version, revision.interval,
                revision.direction, revision.horizon_key,
                revision.outcome_policy_key
            ) revision.source_name, revision.source_version, revision.interval,
              revision.direction, revision.sample_size,
              revision.independent_periods, revision.mean_net_alpha,
              revision.outcome_policy_key,
              revision.alpha_t_stat, revision.alpha_fdr_q,
              revision.calibrated_probability, revision.probability_ci_low,
              revision.probability_ci_high, revision.brier_score,
              revision.brier_skill_score, revision.expected_calibration_error,
              revision.qualification_state, revision.metrics,
              (policy.horizons ->> revision.horizon_key)::INTEGER AS horizon_bars
            FROM equity_qualification_revisions AS revision
            JOIN LATERAL (
                SELECT candidate.horizons
                FROM equity_outcome_policies AS candidate
                WHERE candidate.policy_key = revision.outcome_policy_key
                  AND candidate.horizons ? revision.horizon_key
                                    AND candidate.benchmark_policy->>'primary' = 'SECTOR'
                ORDER BY candidate.effective_from DESC,
                         candidate.created_at DESC
                LIMIT 1
            ) AS policy ON TRUE
            WHERE revision.metrics->>'research_scope' = 'EQUITY_SIGNAL'
              AND revision.interval IS NOT NULL
              AND revision.source_name NOT LIKE 'CONTROL\_%%'
              AND (%s IS NULL OR revision.interval = %s)
              AND revision.effective_from <= NOW()
              AND (revision.effective_to IS NULL OR revision.effective_to > NOW())
            ORDER BY revision.source_name, revision.source_version,
              revision.interval, revision.direction, revision.horizon_key,
              revision.outcome_policy_key, revision.effective_from DESC,
              revision.created_at DESC
            """,
            (interval, interval),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    result = [_qualification_row(row) for row in rows]
    result.sort(key=lambda row: (
        row["scanner_name"], row["scanner_version"], row["interval"],
        row["direction"], row["horizon_bars"], row["outcome_policy_key"],
    ))
    return result


def event_summary(
    interval: str | None = None,
    discovery_state: str | None = None,
    min_independent_periods: int = 20,
) -> list[dict[str, Any]]:
    if discovery_state is not None:
        return []
    summaries = []
    for row in qualification_report(interval):
        if row["independent_periods"] < min_independent_periods:
            continue
        summaries.append({
            "scanner_name": row["scanner_name"],
            "scanner_version": row["scanner_version"],
            "interval": row["interval"],
            "discovery_state": None,
            "direction": row["direction"],
            "horizon_bars": row["horizon_bars"],
            "independent_periods": row["independent_periods"],
            "events": row["events"],
            "mean_net_return": row["mean_net_return"],
            "mean_net_alpha": row["mean_net_alpha"],
            "alpha_t_stat": row["alpha_t_stat"],
            "hit_rate": row["hit_rate"],
            "mean_mae_pct": row["mean_mae_pct"],
            "mean_mfe_pct": row["mean_mfe_pct"],
            "mean_mae_r": None,
            "mean_mfe_r": None,
            "stop_first_rate": row["stop_first_rate"],
            "target_first_rate": row["target_first_rate"],
            "promotion_status": _promotion_status(
                row["independent_periods"],
                row["mean_net_alpha"],
                row["alpha_t_stat"],
            ),
            "outcome_policy_key": row["outcome_policy_key"],
            "return_mode": row["return_mode"],
        })
    return summaries


def pending_outcome_counts() -> list[dict[str, Any]]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH subjects AS (
                SELECT evidence.evidence_id, evidence.interval,
                       policy.outcome_policy_id,
                       horizon.key AS horizon_key,
                       horizon.value::INTEGER AS horizon_bars
                FROM equity_evidence AS evidence
                JOIN equity_analysis_runs AS analysis
                  ON analysis.analysis_run_id = evidence.analysis_run_id
                 AND analysis.run_purpose = 'ORIGINAL'
                JOIN equity_outcome_policies AS policy
                  ON policy.evidence_type = evidence.evidence_type
                 AND policy.source_name = evidence.source_name
                 AND policy.source_version = evidence.source_version
                 AND policy.interval = evidence.interval
                 AND evidence.observed_at >= policy.effective_from
                 AND (policy.effective_to IS NULL
                      OR evidence.observed_at < policy.effective_to)
                CROSS JOIN LATERAL jsonb_each_text(policy.horizons)
                  AS horizon(key, value)
                WHERE evidence.evidence_type = 'SCANNER_RESULT'
                  AND evidence.direction IN (-1, 1)
            )
            SELECT subject.interval, subject.horizon_bars,
                   COUNT(*) FILTER (WHERE outcome.outcome_id IS NULL)::INTEGER
                     AS pending,
                   COUNT(*) FILTER (WHERE outcome.outcome_id IS NOT NULL)::INTEGER
                     AS evaluated
            FROM subjects AS subject
            LEFT JOIN LATERAL (
                SELECT candidate.outcome_id
                FROM equity_research_outcomes AS candidate
                WHERE candidate.subject_evidence_id = subject.evidence_id
                  AND candidate.outcome_policy_id = subject.outcome_policy_id
                  AND candidate.horizon_key = subject.horizon_key
                  AND candidate.is_stale = FALSE
                ORDER BY candidate.outcome_revision DESC,
                         candidate.created_at DESC
                LIMIT 1
            ) AS outcome ON TRUE
            GROUP BY subject.interval, subject.horizon_bars
            ORDER BY subject.interval, subject.horizon_bars
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def recent_events(
    interval: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _event_rows(interval=interval, ticker=None, limit=limit)


def ticker_events(
    ticker: str,
    limit: int = 100,
    daily_sessions: int = 21,
    hourly_sessions: int = 5,
) -> list[dict[str, Any]]:
    return _event_rows(
        interval=None,
        ticker=ticker.upper(),
        limit=limit,
        daily_sessions=daily_sessions,
        hourly_sessions=hourly_sessions,
    )


def latest_ticker_signals(
    interval: str | None = None,
    limit: int = 500,
    daily_session_lookback: int = 10,
    hourly_session_lookback: int = 2,
) -> list[dict[str, Any]]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH eligible AS (
                SELECT evidence.*, analysis.published_at,
                       (evidence.market_time AT TIME ZONE
                         'America/New_York')::DATE AS session_date
                FROM equity_evidence AS evidence
                JOIN equity_analysis_runs AS analysis
                  ON analysis.analysis_run_id = evidence.analysis_run_id
                WHERE evidence.evidence_type = 'SCANNER_RESULT'
                  AND evidence.direction IN (-1, 1)
                                    AND evidence.payload ? 'trigger_type'
                  AND analysis.run_purpose = 'ORIGINAL'
                  AND analysis.status IN ('COMPLETE', 'DEGRADED')
                  AND analysis.published_at IS NOT NULL
                  AND (%s IS NULL OR evidence.interval = %s)
            ), session_ranked AS (
                SELECT eligible.*,
                       DENSE_RANK() OVER (
                           PARTITION BY interval ORDER BY session_date DESC
                       ) AS session_rank
                FROM eligible
            ), ranked AS (
                SELECT evidence.evidence_id,
                       evidence.source_name AS scanner_name,
                       evidence.source_version AS scanner_version,
                       evidence.interval, evidence.ticker,
                       evidence.market_time AS signal_time,
                       evidence.direction,
                       evidence.payload ->> 'trigger_type' AS trigger_type,
                       discovery.state AS discovery_state,
                       discovery.state AS current_discovery_state,
                       discovery.evidence ->> 'trend_state' AS trend_state,
                       discovery.evidence ->> 'extension_risk' AS extension_risk,
                       discovery.evidence ->> 'reversal_trigger' AS reversal_trigger,
                       discovery.evidence ->> 'position_guidance' AS position_guidance,
                       'UNVALIDATED_TIMING'::TEXT AS validation_status,
                       selected.sector,
                       signal_bar.open_price AS signal_open_price,
                       COALESCE(
                           (evidence.payload ->> 'signal_reference_price')::DOUBLE PRECISION,
                           (evidence.payload ->> 'entry_price')::DOUBLE PRECISION,
                           (evidence.payload ->> 'last_close')::DOUBLE PRECISION
                       ) AS signal_close_price,
                       (evidence.payload ->> 'stop_price')::DOUBLE PRECISION
                         AS stop_price,
                       (evidence.payload ->> 'target_price')::DOUBLE PRECISION
                         AS target_price,
                       next_bar.open_price AS next_open_price,
                       next_bar.bar_start AS next_open_time,
                       ROW_NUMBER() OVER (
                           PARTITION BY evidence.ticker
                           ORDER BY evidence.market_time DESC,
                                    evidence.source_name,
                                    evidence.evidence_id
                       ) AS ticker_rank
                FROM session_ranked AS evidence
                LEFT JOIN equity_bar_revisions AS signal_bar
                  ON signal_bar.bar_revision_id = evidence.latest_bar_revision_id
                LEFT JOIN selected_tickers AS selected
                  ON selected.ticker = evidence.ticker
                LEFT JOIN LATERAL (
                    SELECT bar.open_price, bar.bar_start
                    FROM equity_bar_revisions AS bar
                    WHERE bar.ticker = evidence.ticker
                      AND bar.interval = evidence.interval
                      AND bar.is_final = TRUE
                      AND bar.bar_start > evidence.market_time
                    ORDER BY bar.bar_start, bar.system_observed_at DESC
                    LIMIT 1
                ) AS next_bar ON TRUE
                LEFT JOIN LATERAL (
                    SELECT state, current.evidence
                    FROM market_discovery_states AS current
                    WHERE current.ticker = evidence.ticker
                    ORDER BY current.trade_date DESC
                    LIMIT 1
                ) AS discovery ON TRUE
                WHERE evidence.session_rank <= CASE
                    WHEN evidence.interval IN ('1h', '30m') THEN %s ELSE %s END
            )
            SELECT * FROM ranked
            WHERE ticker_rank = 1
            ORDER BY signal_time DESC, ticker
            LIMIT %s
            """,
            (
                interval, interval, hourly_session_lookback,
                daily_session_lookback, limit,
            ),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    for row in rows:
        row["event_id"] = str(row.pop("evidence_id"))
        row.pop("ticker_rank", None)
        tier, reasons = _review_priority(
            row["interval"], row["direction"], row["discovery_state"]
        )
        row["review_priority_tier"] = tier
        row["review_priority_reasons"] = reasons
    return rows


def _event_rows(
    *,
    interval: str | None,
    ticker: str | None,
    limit: int,
    daily_sessions: int = 60,
    hourly_sessions: int = 5,
) -> list[dict[str, Any]]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH eligible AS (
                SELECT evidence.*,
                       (evidence.market_time AT TIME ZONE
                         'America/New_York')::DATE AS session_date
                FROM equity_evidence AS evidence
                JOIN equity_analysis_runs AS analysis
                  ON analysis.analysis_run_id = evidence.analysis_run_id
                WHERE evidence.evidence_type = 'SCANNER_RESULT'
                  AND evidence.direction IN (-1, 1)
                                    AND evidence.payload ? 'trigger_type'
                  AND analysis.run_purpose = 'ORIGINAL'
                  AND analysis.status IN ('COMPLETE', 'DEGRADED')
                  AND analysis.published_at IS NOT NULL
                  AND (%s IS NULL OR evidence.interval = %s)
                  AND (%s IS NULL OR evidence.ticker = %s)
            ), session_ranked AS (
                SELECT eligible.*,
                       DENSE_RANK() OVER (
                           PARTITION BY interval ORDER BY session_date DESC
                       ) AS session_rank
                FROM eligible
            )
            SELECT evidence.evidence_id AS event_id,
                   evidence.source_name AS scanner_name,
                   evidence.source_version AS scanner_version,
                   evidence.interval, evidence.ticker,
                   evidence.market_time AS signal_time,
                   evidence.observed_at AS last_seen_at,
                   1::INTEGER AS occurrence_count,
                   evidence.direction,
                   evidence.payload ->> 'trigger_type' AS trigger_type,
                   evidence.payload ->> 'discovery_state' AS discovery_state,
                   'UNVALIDATED_TIMING'::TEXT AS validation_status,
                   signal_bar.open_price AS signal_open_price,
                   COALESCE(
                       (evidence.payload ->> 'signal_reference_price')::DOUBLE PRECISION,
                       (evidence.payload ->> 'entry_price')::DOUBLE PRECISION,
                       (evidence.payload ->> 'last_close')::DOUBLE PRECISION
                   ) AS entry_price,
                   (evidence.payload ->> 'atr_at_signal')::DOUBLE PRECISION
                     AS atr_at_signal,
                   (evidence.payload ->> 'reference_level')::DOUBLE PRECISION
                     AS reference_level,
                   (evidence.payload ->> 'stop_price')::DOUBLE PRECISION
                     AS stop_price,
                   (evidence.payload ->> 'target_price')::DOUBLE PRECISION
                     AS target_price,
                   CASE
                     WHEN evidence.payload ? 'stop_price' THEN ABS(
                       COALESCE(
                         (evidence.payload ->> 'signal_reference_price')::DOUBLE PRECISION,
                         (evidence.payload ->> 'entry_price')::DOUBLE PRECISION,
                         (evidence.payload ->> 'last_close')::DOUBLE PRECISION
                       ) - (evidence.payload ->> 'stop_price')::DOUBLE PRECISION
                     ) ELSE NULL
                   END AS risk_per_share,
                   COALESCE(evidence.payload -> 'metadata', evidence.payload)
                     AS metadata,
                   COALESCE(outcomes.outcomes, '[]'::JSONB) AS outcomes
            FROM session_ranked AS evidence
            LEFT JOIN equity_bar_revisions AS signal_bar
              ON signal_bar.bar_revision_id = evidence.latest_bar_revision_id
            LEFT JOIN LATERAL (
                SELECT jsonb_agg(item.payload ORDER BY item.horizon_bars)
                         AS outcomes
                FROM (
                    SELECT DISTINCT ON (outcome.horizon_key)
                           (policy.horizons ->> outcome.horizon_key)::INTEGER
                             AS horizon_bars,
                           jsonb_build_object(
                             'horizon_bars',
                               (policy.horizons ->> outcome.horizon_key)::INTEGER,
                             'entry_time', outcome.entry_time,
                             'entry_price', outcome.entry_price,
                             'entry_model', policy.entry_model,
                             'exit_time', outcome.exit_time,
                             'exit_price', outcome.exit_price,
                             'net_signed_return', outcome.net_return,
                             'net_alpha_return', outcome.sector_net_alpha,
                             'mae_pct', outcome.mae_pct,
                             'mfe_pct', outcome.mfe_pct,
                             'mae_r', outcome.mae_r,
                             'mfe_r', outcome.mfe_r,
                             'first_hit', outcome.first_hit
                           ) AS payload
                    FROM equity_research_outcomes AS outcome
                    JOIN equity_outcome_policies AS policy
                      ON policy.outcome_policy_id = outcome.outcome_policy_id
                    WHERE outcome.subject_evidence_id = evidence.evidence_id
                      AND outcome.is_stale = FALSE
                    ORDER BY outcome.horizon_key,
                      (policy.policy_key LIKE '%%:RECOMMENDATION_PLAN:%%') DESC,
                      outcome.outcome_revision DESC, outcome.created_at DESC
                ) AS item
            ) AS outcomes ON TRUE
            WHERE evidence.session_rank <= CASE
                WHEN evidence.interval IN ('1h', '30m') THEN %s ELSE %s END
            ORDER BY evidence.market_time DESC, evidence.ticker,
                     evidence.source_name
            LIMIT %s
            """,
            (
                interval, interval, ticker, ticker,
                hourly_sessions, daily_sessions, limit,
            ),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    for row in rows:
        row["event_id"] = str(row["event_id"])
    return rows


def _qualification_row(row: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(row.get("metrics") or {})
    curve = list(metrics.get("calibration_curve") or [])
    state = str(row["qualification_state"])
    calibrated_periods = sum(int(point.get("count") or 0) for point in curve)
    calibrated = (
        state == "ROBUST_PASS"
        and calibrated_periods >= 100
        and row.get("brier_score") is not None
        and float(row["brier_score"]) < 0.25
        and row.get("expected_calibration_error") is not None
        and float(row["expected_calibration_error"]) <= 0.05
    )
    return _sanitize({
        "scanner_name": str(row["source_name"]).lower(),
        "scanner_version": str(row["source_version"]),
        "interval": str(row["interval"]),
        "direction": int(row["direction"]),
        "horizon_bars": int(row["horizon_bars"]),
        "outcome_policy_key": str(row["outcome_policy_key"]),
        "return_mode": (
            "RECOMMENDATION_PLAN"
            if ":RECOMMENDATION_PLAN" in str(row["outcome_policy_key"])
            else "DIRECTIONAL_HORIZON"
        ),
        "events": int(row["sample_size"]),
        "independent_periods": int(row["independent_periods"]),
        "mean_net_return": metrics.get("mean_net_return"),
        "mean_net_alpha": row.get("mean_net_alpha"),
        "alpha_t_stat": row.get("alpha_t_stat"),
        "alpha_p_value": metrics.get("alpha_p_value"),
        "alpha_fdr_q": row.get("alpha_fdr_q"),
        "alpha_ci_low": metrics.get("live_expected_alpha_ci_low"),
        "alpha_ci_high": metrics.get("live_expected_alpha_ci_high"),
        "early_alpha": metrics.get("early_alpha"),
        "late_alpha": metrics.get("late_alpha"),
        "hit_rate": metrics.get("hit_rate"),
        "hit_rate_ci_low": metrics.get("hit_rate_ci_low"),
        "hit_rate_ci_high": metrics.get("hit_rate_ci_high"),
        "mean_mae_pct": metrics.get("mean_mae_pct"),
        "mean_mfe_pct": metrics.get("mean_mfe_pct"),
        "stop_first_rate": metrics.get("stop_first_rate"),
        "target_first_rate": metrics.get("target_first_rate"),
        "stop_hit_rate": metrics.get("stop_hit_rate"),
        "target_hit_rate": metrics.get("target_hit_rate"),
        "mean_sector_alpha": metrics.get("mean_sector_net_alpha"),
        "sector_alpha_t_stat": metrics.get("sector_alpha_t_stat"),
        "distinct_tickers": metrics.get("distinct_tickers"),
        "top5_concentration": metrics.get("top5_concentration"),
        "first_signal_time": metrics.get("first_signal_time"),
        "last_signal_time": metrics.get("last_signal_time"),
        "regime_alpha": {
            regime: {"mean_alpha": None, "periods": 0}
            for regime in ("BULL", "BEAR", "CHOPPY")
        },
        "qualification_status": (
            "PRIMARY_PASS" if metrics.get("raw_pass") else "NOT_QUALIFIED"
        ),
        "evidence_status": state,
        "calibration_status": (
            "RESEARCH_CALIBRATED" if calibrated else "NOT_ELIGIBLE"
        ),
        "calibration_oos_periods": calibrated_periods,
        "calibrated_win_probability": (
            row.get("calibrated_probability") if calibrated else None
        ),
        "calibrated_win_probability_ci_low": (
            row.get("probability_ci_low") if calibrated else None
        ),
        "calibrated_win_probability_ci_high": (
            row.get("probability_ci_high") if calibrated else None
        ),
        "brier_score": row.get("brier_score"),
        "brier_skill_score_vs_50": row.get("brier_skill_score"),
        "expected_calibration_error": row.get("expected_calibration_error"),
        "live_expected_alpha": (
            metrics.get("live_expected_alpha") if calibrated else None
        ),
        "live_expected_alpha_ci_low": (
            metrics.get("live_expected_alpha_ci_low") if calibrated else None
        ),
        "live_expected_alpha_ci_high": (
            metrics.get("live_expected_alpha_ci_high") if calibrated else None
        ),
        "calibration_curve": curve,
    })


def _review_priority(
    interval: str,
    direction: int,
    discovery_state: str | None,
) -> tuple[str, list[str]]:
    if interval != "1h":
        return "UNRANKED", [
            "Review priority is not qualified for daily or weekly signals."
        ]
    state = discovery_state or "NEUTRAL"
    aligned = (
        {"CONTINUATION", "EMERGING_REVERSAL", "REVERSAL_CONFIRMED"}
        if direction == 1 else {"CONFLICT", "LAGGARD"}
    )
    opposed = (
        {"CONFLICT", "LAGGARD"}
        if direction == 1 else {
            "CONTINUATION", "REVERSAL_WATCH", "EMERGING_REVERSAL",
            "REVERSAL_CONFIRMED",
        }
    )
    if state in aligned:
        return "HIGHER", ["Direction aligns with current discovery state."]
    if state in opposed:
        return "LOWER", ["Direction opposes current discovery state."]
    return "STANDARD", ["Discovery-state context is neutral or inconclusive."]


def _promotion_status(
    periods: int,
    mean_alpha: float | None,
    alpha_t: float | None,
) -> str:
    if periods < 20:
        return "COLLECTING"
    if periods < 40:
        return "INSUFFICIENT_SAMPLE"
    if mean_alpha is None or mean_alpha <= 0 or alpha_t is None or alpha_t < 2:
        return "FAILED"
    return "PROMISING"


def _sanitize(value):
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value
