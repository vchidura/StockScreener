-- Migration 004: Daily Recommendations & Performance Tracking (ENHANCED)
-- Purpose: Create tables for daily high-probability bull/bear recommendations with pattern isolation,
--          historical priors, analog matching, and continuous performance tracking
-- Date: 2026-08-08
-- Timezone: America/New_York (ET)
--
-- ARCHITECTURE:
-- Layer 0: Screeners (existing)
-- Layer 1: opening_pattern_scores (NEW) - Independent 6-pattern scores computed 9:25 AM
-- Layer 2: pattern_win_rate_priors (NEW) - Historical win rates from past 60 days
-- Layer 3: pattern_analog_matches (NEW) - Similar historical patterns matched 9:30 AM
-- Layer 4: daily_recommendations (ENHANCED) - Uses all 3 layers for calibrated confidence
-- Layer 5: Performance tracking (existing)

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 1 (NEW): Opening Pattern Scores (9:25 AM)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opening_pattern_scores (
    score_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    sector VARCHAR(50),
    
    -- 6 independent pattern scores (0-100 each)
    breakout_score INT CHECK (breakout_score >= 0 AND breakout_score <= 100),
    vwap_score INT CHECK (vwap_score >= 0 AND vwap_score <= 100),
    volatility_score INT CHECK (volatility_score >= 0 AND volatility_score <= 100),
    trend_score INT CHECK (trend_score >= 0 AND trend_score <= 100),
    rs_score INT CHECK (rs_score >= 0 AND rs_score <= 100),
    calendar_score INT CHECK (calendar_score >= 0 AND calendar_score <= 100),
    
    -- Pattern firing indicators (score > 60 = fired)
    breakout_fired BOOLEAN DEFAULT FALSE,
    vwap_fired BOOLEAN DEFAULT FALSE,
    volatility_fired BOOLEAN DEFAULT FALSE,
    trend_fired BOOLEAN DEFAULT FALSE,
    rs_fired BOOLEAN DEFAULT FALSE,
    calendar_fired BOOLEAN DEFAULT FALSE,
    
    -- Market context at time of scoring
    primary_regime VARCHAR(20),  -- Bull/Neutral/Bear
    sector_regime VARCHAR(20),
    market_breadth_score INT CHECK (market_breadth_score >= 0 AND market_breadth_score <= 100),
    
    -- Analog matching results (filled by pattern_analog_matches)
    analog_match_count INT DEFAULT 0,
    analog_win_rate DECIMAL(5,2),  -- % of similar historical days that hit
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pattern_scores_date_ticker ON opening_pattern_scores(trade_date, ticker);
CREATE INDEX idx_pattern_scores_sector ON opening_pattern_scores(sector, trade_date DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 2 (NEW): Pattern Win-Rate Priors (Weekly - Friday)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pattern_win_rate_priors (
    prior_id BIGSERIAL PRIMARY KEY,
    effective_date DATE NOT NULL,
    pattern_name VARCHAR(50) NOT NULL,  -- 'breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar'
    
    -- Historical accuracy (computed from past 60 days)
    historical_win_rate DECIMAL(5,2),  -- % of times pattern fired and recommendation hit
    sample_size INT,  -- Number of past days analyzed
    lookback_days INT DEFAULT 60,
    
    -- Confidence adjustment multiplier
    confidence_multiplier DECIMAL(3,2),  -- 0.5 to 1.5 (neutral = 1.0)
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_priors_date_pattern ON pattern_win_rate_priors(effective_date DESC, pattern_name);

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 3 (NEW): Pattern Analog Matches (9:30 AM)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pattern_analog_matches (
    analog_id BIGSERIAL PRIMARY KEY,
    current_trade_date DATE NOT NULL,
    current_ticker VARCHAR(10) NOT NULL,
    
    -- Current pattern signature
    current_breakout_score INT,
    current_vwap_score INT,
    current_volatility_score INT,
    current_trend_score INT,
    current_rs_score INT,
    current_calendar_score INT,
    current_sector_regime VARCHAR(20),
    current_market_breadth INT,
    
    -- Historical analog results (past 100 days with same sector regime)
    analog_count INT DEFAULT 0,  -- How many similar days found
    analog_accuracy DECIMAL(5,2),  -- % of similar days that actually hit
    
    -- Match details (stored as JSON for flexibility)
    -- Example: [{date: '2026-08-01', distance: 8, actual_return: 0.45, hit: true}, ...]
    analog_details JSONB,
    
    -- Confidence adjustment from analog matching
    analog_confidence_boost INT DEFAULT 0,  -- +5 to -10 points
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_analogs_date_ticker ON pattern_analog_matches(current_trade_date, current_ticker);

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 4 (ENHANCED): Daily Recommendations (9:35 AM)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_recommendations (
    rec_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    sector VARCHAR(50),
    
    -- Recommendation details
    recommendation_type VARCHAR(20) NOT NULL CHECK (recommendation_type IN ('BULL', 'BEAR')),
    rank_in_category INT NOT NULL,  -- 1-10
    
    -- Predicted metrics (computed at 9:25 AM)
    predicted_return_pct DECIMAL(6,3),
    predicted_confidence_pct INT CHECK (predicted_confidence_pct >= 0 AND predicted_confidence_pct <= 100),
    signal_grade VARCHAR(20) CHECK (signal_grade IN ('Excellent', 'Good', 'Fair', 'Weak')),
    signal_score INT CHECK (signal_score >= 0 AND signal_score <= 100),
    
    -- Sector context at market open
    primary_index VARCHAR(10),  -- QQQ for tech, DIA for finance, etc.
    primary_regime VARCHAR(20),  -- Bull/Neutral/Bear
    sector_etf VARCHAR(10),  -- XLK, XLF, etc.
    sector_regime VARCHAR(20),  -- Bull/Neutral/Bear
    market_breadth_score INT CHECK (market_breadth_score >= 0 AND market_breadth_score <= 100),  -- 0-100
    
    -- Execution parameters (precomputed at 9:25 AM)
    recommended_position_size_pct DECIMAL(5,3),
    recommended_entry DECIMAL(12,4),
    recommended_stop DECIMAL(12,4),
    recommended_target_1 DECIMAL(12,4),
    recommended_target_2 DECIMAL(12,4),
    risk_reward_ratio DECIMAL(6,2),
    
    -- Pattern breakdown (0-100 scores, copied from opening_pattern_scores)
    breakout_score INT CHECK (breakout_score >= 0 AND breakout_score <= 100),
    vwap_score INT CHECK (vwap_score >= 0 AND vwap_score <= 100),
    volatility_score INT CHECK (volatility_score >= 0 AND volatility_score <= 100),
    trend_score INT CHECK (trend_score >= 0 AND trend_score <= 100),
    rs_score INT CHECK (rs_score >= 0 AND rs_score <= 100),
    calendar_score INT CHECK (calendar_score >= 0 AND calendar_score <= 100),
    
    -- Foreign keys to supporting tables (NEW)
    score_id BIGINT REFERENCES opening_pattern_scores(score_id),
    analog_id BIGINT REFERENCES pattern_analog_matches(analog_id),
    
    -- Confidence calibration tracing (NEW)
    pattern_priors_applied BOOLEAN DEFAULT FALSE,
    analog_matching_applied BOOLEAN DEFAULT FALSE,
    confidence_before_calibration INT,  -- Base confidence before priors/analogs (9:25 AM)
    confidence_after_calibration INT,   -- Final confidence after all adjustments (9:35 AM)
    calibration_sources TEXT,  -- Example: 'priors: ×1.15, analogs: +8'
    
    -- Execution tracking
    auto_trade_enabled BOOLEAN DEFAULT FALSE,
    was_executed BOOLEAN DEFAULT FALSE,
    execution_price DECIMAL(12,4),
    execution_time TIMESTAMPTZ,
    
    -- EOD tracking (filled at 4:05 PM close)
    actual_return_pct DECIMAL(6,3),
    actual_high_pct DECIMAL(6,3),
    actual_low_pct DECIMAL(6,3),
    hit_target_1 BOOLEAN,
    hit_target_2 BOOLEAN,
    stopped_out BOOLEAN,
    recommendation_correct BOOLEAN,  -- TRUE if |actual_return - predicted_return| < 0.5%
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT
);

-- Indexes for daily recommendations
CREATE INDEX idx_daily_rec_date_type ON daily_recommendations(trade_date, recommendation_type);
CREATE INDEX idx_daily_rec_ticker_date ON daily_recommendations(ticker, trade_date);
CREATE INDEX idx_daily_rec_accuracy ON daily_recommendations(recommendation_correct, trade_date DESC);
CREATE INDEX idx_daily_rec_sector ON daily_recommendations(sector, trade_date);
CREATE INDEX idx_daily_rec_score_id ON daily_recommendations(score_id);
CREATE INDEX idx_daily_rec_analog_id ON daily_recommendations(analog_id);


-- Table 5 (EXISTING): Daily Recommendation Performance Report (4:05 PM)
CREATE TABLE IF NOT EXISTS recommendation_performance_log (
    perf_id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    
    -- Daily performance stats (4:05 PM close)
    bull_recommendations_total INT,
    bull_recommendations_hit INT,
    bull_win_rate_pct DECIMAL(5,2),
    
    bear_recommendations_total INT,
    bear_recommendations_hit INT,
    bear_win_rate_pct DECIMAL(5,2),
    
    overall_recommendations_total INT,
    overall_recommendations_hit INT,
    overall_win_rate_pct DECIMAL(5,2),
    
    -- Rolling metrics
    win_rate_5day DECIMAL(5,2),
    win_rate_20day DECIMAL(5,2),
    win_rate_monthly DECIMAL(5,2),
    
    -- Pattern-level accuracy (computed from daily_recommendations where pattern_score > 60)
    breakout_pattern_accuracy DECIMAL(5,2),
    breakout_pattern_sample_size INT,
    
    vwap_pattern_accuracy DECIMAL(5,2),
    vwap_pattern_sample_size INT,
    
    volatility_pattern_accuracy DECIMAL(5,2),
    volatility_pattern_sample_size INT,
    
    trend_pattern_accuracy DECIMAL(5,2),
    trend_pattern_sample_size INT,
    
    rs_pattern_accuracy DECIMAL(5,2),
    rs_pattern_sample_size INT,
    
    calendar_pattern_accuracy DECIMAL(5,2),
    calendar_pattern_sample_size INT,
    
    -- Confidence calibration (are 75% signals actually 75% accurate?)
    signal_75_80_pct_accuracy DECIMAL(5,2),  -- Actual accuracy of signals predicted at 75-80%
    signal_75_80_pct_sample_size INT,
    
    signal_80_90_pct_accuracy DECIMAL(5,2),
    signal_80_90_pct_sample_size INT,
    
    signal_90_100_pct_accuracy DECIMAL(5,2),
    signal_90_100_pct_sample_size INT,
    
    -- Alerts and recommendations
    patterns_needing_adjustment TEXT,  -- JSON: [{"pattern": "breakout", "accuracy": 40, "threshold": 55}, ...]
    recommended_weight_changes TEXT,   -- JSON: [{"pattern": "breakout", "current": 30, "new": 25, "change": -5}, ...]
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT
);

CREATE INDEX idx_perf_log_date ON recommendation_performance_log(report_date DESC);


-- Table 6 (EXISTING): Sector-Specific Metrics (for multi-regime tracking)
CREATE TABLE IF NOT EXISTS sector_regime_daily (
    sector_regime_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    sector VARCHAR(50) NOT NULL,  -- 'tech', 'finance', 'consumer', etc.
    etf_symbol VARCHAR(10) NOT NULL,  -- XLK, XLF, XLY, etc.
    
    -- Market open (9:30 AM) regime assessment
    open_regime VARCHAR(20),  -- Bull/Neutral/Bear
    open_score INT CHECK (open_score >= 0 AND open_score <= 100),
    
    -- Intraday updates
    update_time TIMESTAMPTZ,
    current_regime VARCHAR(20),
    current_score INT CHECK (current_score >= 0 AND current_score <= 100),
    
    -- Session close
    close_regime VARCHAR(20),
    close_score INT CHECK (close_score >= 0 AND close_score <= 100),
    session_return_pct DECIMAL(6,3),
    
    -- Calculated metrics
    price_vs_sma40_pct DECIMAL(6,3),
    rsi_14 DECIMAL(5,2),
    atr_20day DECIMAL(12,4),
    volume_vs_sma_20 DECIMAL(6,3),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sector_regime_date_sector ON sector_regime_daily(trade_date DESC, sector);


-- Table 7 (EXISTING): Daily Market Context Summary (SPY, QQQ, DIA, IWM at open)
CREATE TABLE IF NOT EXISTS market_context_daily (
    context_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE,
    
    -- 9:30 AM (market open) assessment
    spy_regime VARCHAR(20),
    spy_score INT CHECK (spy_score >= 0 AND spy_score <= 100),
    
    qqq_regime VARCHAR(20),
    qqq_score INT CHECK (qqq_score >= 0 AND qqq_score <= 100),
    
    dia_regime VARCHAR(20),
    dia_score INT CHECK (dia_score >= 0 AND dia_score <= 100),
    
    iwm_regime VARCHAR(20),
    iwm_score INT CHECK (iwm_score >= 0 AND iwm_score <= 100),
    
    -- Composite scores
    broad_market_regime VARCHAR(20),  -- Bull/Neutral/Bear (SPY+DIA+IWM consensus)
    growth_market_regime VARCHAR(20),  -- Bull/Neutral/Bear (QQQ+XLK focus)
    
    breadth_score INT CHECK (breadth_score >= 0 AND breadth_score <= 100),  -- % indices in agreement
    market_consensus_pct INT,  -- How many indices aligned
    
    -- Divergence detection
    has_sector_divergence BOOLEAN,
    divergence_type VARCHAR(100),  -- 'tech_strength_vs_broad_weakness', etc.
    
    -- Session end summary
    spy_session_return_pct DECIMAL(6,3),
    qqq_session_return_pct DECIMAL(6,3),
    dia_session_return_pct DECIMAL(6,3),
    iwm_session_return_pct DECIMAL(6,3),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_market_context_date ON market_context_daily(trade_date DESC);


-- ═════════════════════════════════════════════════════════════════════════════
-- ARCHITECTURE & EXECUTION FLOW
-- ═════════════════════════════════════════════════════════════════════════════
--
-- LAYER 1: OPENING PATTERN SCORES (9:25 AM)
-- ─────────────────────────────────────────────────────────────────────────────
-- Script: backend/scripts/compute_opening_pattern_scores.py
-- Input:  5m/15m/30m candles from stock_prices_intraday
-- Output: 11 rows in opening_pattern_scores (one per tracked ticker)
-- Data:   6 independent pattern scores + fired flags + market context
--
-- LAYER 2: PATTERN WIN-RATE PRIORS (Weekly - Friday)
-- ─────────────────────────────────────────────────────────────────────────────
-- Script: backend/scripts/calibrate_pattern_priors.py
-- Input:  Past 60 days from daily_recommendations + recommendation_performance_log
-- Compute: For each pattern, if fired (score > 60), what % actually hit?
-- Output: 6 rows in pattern_win_rate_priors (one per pattern)
-- Data:   Historical win rate (e.g., 62%) → confidence_multiplier (e.g., 1.24)
--
-- LAYER 3: PATTERN ANALOG MATCHES (9:30 AM)
-- ─────────────────────────────────────────────────────────────────────────────
-- Script: backend/scripts/find_analog_matches.py
-- Input:  Current day's pattern scores + past 100 days with same sector regime
-- Compute: Find historical days with similar pattern scores (±10 point tolerance)
-- Output: 11 rows in pattern_analog_matches (one per tracked ticker)
-- Data:   Analog count, win rate of similar days, confidence boost (-10 to +10)
--
-- LAYER 4: DAILY RECOMMENDATIONS (9:35 AM)
-- ─────────────────────────────────────────────────────────────────────────────
-- Script: backend/scripts/daily_recommendations_generator.py (UPDATED)
-- Input:  Layers 1, 2, 3 + sector regime + market breadth
-- Process:
--   1. Load opening_pattern_scores (Layer 1)
--   2. Load pattern_win_rate_priors (Layer 2)
--   3. Load pattern_analog_matches (Layer 3)
--   4. Compute: confidence = base × priors_multiplier + analog_boost
--   5. Determine Bull/Bear based on trend_score
--   6. Rank: Sort by (confidence × pattern_fired_count)
--   7. Select: Top 10 Bull + Top 10 Bear
-- Output: 20 rows in daily_recommendations
-- Data:   Full recommendation + calibration trace + execution params
--
-- LAYER 5: EOD PERFORMANCE TRACKING (4:05 PM)
-- ─────────────────────────────────────────────────────────────────────────────
-- Script: backend/scripts/daily_recommendations_tracker.py (EXISTING)
-- Input:  20 morning recommendations + closing prices
-- Process:
--   1. Load daily_recommendations (20 rows from 9:35 AM)
--   2. Fetch close prices from stock_prices_daily
--   3. Compute actual_return = (close - entry) / entry
--   4. Determine HIT: |actual_return - predicted_return| < 0.5%
--   5. Update daily_recommendations.recommendation_correct
--   6. Aggregate to recommendation_performance_log
-- Output: Updated daily_recommendations + new row in recommendation_performance_log
-- Data:   Actual returns, HIT/MISS, pattern accuracy, win rates
--
-- DATA DEPENDENCIES:
-- ─────────────────────────────────────────────────────────────────────────────
-- opening_pattern_scores
--   ├─ DEPENDS ON: stock_prices_intraday (5m/15m/30m bars)
--   └─ USED BY: daily_recommendations (score_id FK)
--
-- pattern_win_rate_priors
--   ├─ DEPENDS ON: daily_recommendations (past 60 days)
--   └─ USED BY: daily_recommendations_generator (confidence calibration)
--
-- pattern_analog_matches
--   ├─ DEPENDS ON: daily_recommendations (past 100 days same regime)
--   └─ USED BY: daily_recommendations (analog_id FK)
--
-- daily_recommendations
--   ├─ DEPENDS ON: opening_pattern_scores, pattern_analog_matches, pattern_win_rate_priors
--   ├─ USED BY: daily_recommendations_tracker, recommendation_performance_log
--   └─ USED BY: frontend dashboard
--
-- recommendation_performance_log
--   ├─ DEPENDS ON: daily_recommendations
--   └─ USED BY: daily_recommendations_tracker
--


-- Grants for application user (if using role-based access)
-- GRANT SELECT, INSERT, UPDATE ON daily_recommendations TO stock_screener_app;
-- GRANT SELECT, INSERT ON recommendation_performance_log TO stock_screener_app;
-- GRANT SELECT ON sector_regime_daily TO stock_screener_app;
-- GRANT SELECT ON market_context_daily TO stock_screener_app;
