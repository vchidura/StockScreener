#!/usr/bin/env python3
"""
Daily Recommendations Generator (ENHANCED)
Generates Top 10 Bull + Top 10 Bear recommendations at 9:35 AM each trading day

NEW: Integrates 3-layer calibration:
  Layer 1: opening_pattern_scores (9:25 AM) - Independent 6-pattern scores
  Layer 2: pattern_win_rate_priors (Friday) - Historical win rates
  Layer 3: pattern_analog_matches (9:30 AM) - Similar historical patterns

Usage:
    python daily_recommendations_generator.py [--date 2026-08-08] [--dry-run]
    
Output:
    - Inserts 20 recommendations into daily_recommendations table
    - Includes calibration trace (confidence_before/after, sources)
    - Logs recommendation summary to console
    - Optionally auto-executes high-confidence recommendations (confidence >= 75%)
"""

import sys
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from database import get_db_cursor
from screeners import analyze_market_regime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Sector mapping: Top 30 tickers organized by sector with regime indices
SECTOR_MAPPING = {
    'tech': {
        'tickers': ['MSFT', 'META', 'AMD', 'DELL', 'ASML', 'SMH'],
        'primary_index': 'QQQ',
        'secondary_index': 'XLK',
        'context_indices': ['SPY', 'DIA'],
    },
    'semiconductors': {
        'tickers': ['MU', 'STX', 'WDC', 'AMAT'],
        'primary_index': 'QQQ',
        'secondary_index': 'XLK',
        'context_indices': ['SPY', 'SMH'],
    },
    'industrial': {
        'tickers': ['CAT', 'DE', 'CMI', 'PWR', 'TT', 'ROK', 'ETN', 'NOC', 'LMT'],
        'primary_index': 'DIA',
        'secondary_index': 'XLI',
        'context_indices': ['SPY', 'IWM'],
    },
    'healthcare': {
        'tickers': ['LLY', 'TMO', 'VRTX', 'GEV'],
        'primary_index': 'DIA',
        'secondary_index': 'XLV',
        'context_indices': ['SPY'],
    },
    'finance': {
        'tickers': ['GS', 'MA', 'MCO', 'BRK-B'],
        'primary_index': 'DIA',
        'secondary_index': 'XLF',
        'context_indices': ['SPY', 'QQQ'],
    },
    'etf_benchmarks': {
        'tickers': ['SPY', 'QQQ', 'DIA'],
        'primary_index': 'SPY',
        'secondary_index': 'SPY',
        'context_indices': ['SPY'],
    },
}

# Flatten tickers
TRACKED_TICKERS = []
for sector, config in SECTOR_MAPPING.items():
    TRACKED_TICKERS.extend(config['tickers'])

ALL_INDICES = ['SPY', 'QQQ', 'DIA', 'IWM', 'XLK', 'XLF', 'XLY', 'XLP', 'XLE', 'XLI', 'XLV']


def get_sector_for_ticker(ticker: str) -> str:
    """Lookup sector for a ticker"""
    for sector, config in SECTOR_MAPPING.items():
        if ticker in config['tickers']:
            return sector
    return 'other'


def load_pattern_scores(ticker: str, trade_date: datetime) -> Optional[Dict]:
    """
    LAYER 1: Load opening pattern scores (computed at 9:25 AM)
    
    Returns: {
        'breakout_score': int,
        'vwap_score': int,
        'volatility_score': int,
        'trend_score': int,
        'rs_score': int,
        'calendar_score': int,
        'score_id': int,
    }
    """
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                score_id,
                breakout_score, vwap_score, volatility_score,
                trend_score, rs_score, calendar_score
            FROM opening_pattern_scores
            WHERE trade_date = %s AND ticker = %s
        """, (trade_date.date(), ticker))
        
        row = cur.fetchone()
        
        if not row:
            logger.warning(f"  {ticker}: No pattern scores found (9:25 AM step not completed?)")
            return None
        
        return {
            'score_id': row['score_id'],
            'breakout_score': row['breakout_score'],
            'vwap_score': row['vwap_score'],
            'volatility_score': row['volatility_score'],
            'trend_score': row['trend_score'],
            'rs_score': row['rs_score'],
            'calendar_score': row['calendar_score'],
        }


def load_pattern_priors(trade_date: datetime) -> Dict:
    """
    LAYER 2: Load pattern win-rate priors in effect on trade_date.
    
    Returns: {
        'breakout': {'win_rate': 0.58, 'multiplier': 1.16},
        'vwap': {'win_rate': 0.62, 'multiplier': 1.24},
        ...
    }
    """
    with get_db_cursor() as cur:
        # DISTINCT ON keeps one row per pattern; a plain LIMIT 6 silently mixes
        # effective dates when the newest batch is not exactly six rows.
        cur.execute("""
            SELECT DISTINCT ON (pattern_name)
                   pattern_name, historical_win_rate, confidence_multiplier
            FROM pattern_win_rate_priors
            WHERE effective_date <= %s
            ORDER BY pattern_name, effective_date DESC
        """, (trade_date.date() if hasattr(trade_date, 'date') else trade_date,))
        
        rows = cur.fetchall()
        
        if not rows:
            logger.warning("No pattern priors found; using neutral multipliers")
            return {p: {'win_rate': 0.5, 'multiplier': 1.0} for p in 
                    ['breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar']}
        
        priors = {}
        for row in rows:
            pattern = row['pattern_name']
            priors[pattern] = {
                'win_rate': row['historical_win_rate'] / 100.0,
                'multiplier': row['confidence_multiplier'],
            }
        
        # Fill missing patterns with neutral
        for pattern in ['breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar']:
            if pattern not in priors:
                priors[pattern] = {'win_rate': 0.5, 'multiplier': 1.0}
        
        return priors


def load_analog_matches(ticker: str, trade_date: datetime) -> Optional[Dict]:
    """
    LAYER 3: Load pattern analog matches (computed at 9:30 AM)
    
    Returns: {
        'analog_id': int,
        'analog_count': int,
        'analog_accuracy': float (0.0-1.0),
        'confidence_boost': int (-10 to +10),
    }
    """
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                analog_id,
                analog_count,
                analog_accuracy,
                analog_confidence_boost
            FROM pattern_analog_matches
            WHERE current_trade_date = %s AND current_ticker = %s
        """, (trade_date.date(), ticker))
        
        row = cur.fetchone()
        
        if not row:
            logger.warning(f"  {ticker}: No analog matches found (9:30 AM step not completed?)")
            return None
        
        return {
            'analog_id': row['analog_id'],
            'analog_count': row['analog_count'],
            'analog_accuracy': row['analog_accuracy'],
            'confidence_boost': row['analog_confidence_boost'],
        }


def apply_calibration(base_confidence: int, pattern_scores: Dict, priors: Dict, analogs: Optional[Dict]) -> Tuple[int, str]:
    """
    Apply 3-layer calibration to base confidence.
    
    1. Load pattern win-rate priors (Layer 2)
    2. Apply prior multipliers based on which patterns fired
    3. Apply analog boost from Layer 3
    
    Returns:
      (final_confidence, calibration_sources_str)
    """
    # Step 1: Apply pattern priors
    # For patterns that fired (score > 60), apply their historical multipliers
    prior_adjustments = []
    prior_adjusted_confidence = base_confidence
    
    fired_patterns = []
    for pattern in ['breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar']:
        score_key = f'{pattern}_score'
        if pattern_scores.get(score_key, 0) > 60:
            fired_patterns.append(pattern)
            multiplier = priors.get(pattern, {}).get('multiplier', 1.0)
            prior_adjusted_confidence *= multiplier
            adjustment_points = int((multiplier - 1.0) * 100)
            prior_adjustments.append(f"{pattern[:3]}:{adjustment_points:+d}")
    
    # Clamp to 0-100
    prior_adjusted_confidence = max(0, min(100, int(prior_adjusted_confidence)))
    
    # Step 2: Apply analog boost
    analog_boost = 0
    analog_str = ""
    
    if analogs and analogs['analog_count'] > 0:
        analog_boost = analogs['confidence_boost']
        analog_str = f"analogs:{analog_boost:+d}"
    
    final_confidence = max(0, min(100, prior_adjusted_confidence + analog_boost))
    
    # Step 3: Build calibration sources string
    calibration_sources = ""
    if prior_adjustments:
        calibration_sources += "priors:" + ",".join(prior_adjustments)
    if analog_str:
        if calibration_sources:
            calibration_sources += f" | {analog_str}"
        else:
            calibration_sources = analog_str
    
    return final_confidence, calibration_sources


def get_sector_for_ticker(ticker: str) -> str:
    """Lookup sector for a ticker"""
    for sector, config in SECTOR_MAPPING.items():
        if ticker in config['tickers']:
            return sector
    return 'other'


def get_market_regime_scores(trade_date: datetime) -> Dict[str, Dict]:
    """
    Fetch market regime scores for all indices at market open (9:30 AM)
    
    Returns: {
        'SPY': {'regime': 'Bull', 'score': 75},
        'QQQ': {'regime': 'Neutral', 'score': 52},
        ...
        'breadth_score': 75,
        'market_consensus_pct': 75
    }
    """
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            # Try to fetch from market_context_daily if it exists
            cur.execute("""
                SELECT spy_regime, spy_score, qqq_regime, qqq_score, 
                       dia_regime, dia_score, iwm_regime, iwm_score,
                       broad_market_regime, growth_market_regime, breadth_score
                FROM market_context_daily
                WHERE trade_date = %s
            """, (trade_date.date(),))
            
            row = cur.fetchone()
            ctx.__exit__(None, None, None)
            
            if row:
                return {
                    'SPY': {'regime': row[0], 'score': row[1]},
                    'QQQ': {'regime': row[2], 'score': row[3]},
                    'DIA': {'regime': row[4], 'score': row[5]},
                    'IWM': {'regime': row[6], 'score': row[7]},
                    'broad_market_regime': row[8],
                    'growth_market_regime': row[9],
                    'breadth_score': row[10],
                }
    except Exception as e:
        logger.warning(f"Could not fetch market_context_daily: {e}")
    
    # Fallback: Compute from analyzer (simplified)
    logger.info("Computing market regime scores...")
    regimes = {}
    for index in ['SPY', 'QQQ', 'DIA', 'IWM']:
        regime = analyze_market_regime(index, trade_date)
        regimes[index] = {
            'regime': regime.get('regime', 'Neutral'),
            'score': regime.get('score', 50)
        }
    
    regimes['breadth_score'] = 75  # Placeholder
    regimes['broad_market_regime'] = 'Bull' if all(r['regime'] == 'Bull' for r in regimes.values()) else 'Neutral'
    regimes['growth_market_regime'] = 'Bull' if regimes['QQQ']['regime'] == 'Bull' else 'Neutral'
    
    return regimes


def get_sector_adjusted_regime(
    sector: str,
    primary_regime: str,
    secondary_regime: Optional[str],
    market_regimes: Dict
) -> Tuple[str, int]:
    """
    Adjust recommendation confidence based on sector alignment
    
    Returns: (adjusted_regime, confidence_adjustment)
    """
    if primary_regime == 'Bull' and secondary_regime == 'Bull':
        return 'Bull', +15  # Strong alignment
    elif primary_regime == 'Bull' and secondary_regime is None:
        return 'Bull', +5
    elif primary_regime == 'Bull' and secondary_regime == 'Neutral':
        return 'Bull', 0  # Mixed
    elif primary_regime == 'Bull' and secondary_regime == 'Bear':
        return 'Neutral', -20  # Conflicting - reduce confidence
    elif primary_regime == 'Neutral':
        return 'Neutral', -10
    elif primary_regime == 'Bear':
        return 'Bear', 0
    
    return primary_regime, 0


def get_opening_patterns(ticker: str, trade_date: datetime) -> Dict:
    """
    Fetch opening minute patterns for the day
    
    Returns pattern scores (0-100) or defaults if no data
    """
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            cur.execute("""
                SELECT 
                    breakout_type, vwap_pattern, volatility_ratio, trend_structure_type,
                    relative_strength_type, volume_expansion_ratio, price_vs_vwap_5m_pct
                FROM opening_minute_patterns
                WHERE ticker = %s AND trade_date = %s
                ORDER BY datetime DESC
                LIMIT 1
            """, (ticker, trade_date.date()))
            
            row = cur.fetchone()
            ctx.__exit__(None, None, None)
            
            if row:
                return {
                    'breakout_type': row[0],
                    'vwap_pattern': row[1],
                    'volatility_ratio': row[2],
                    'trend_structure': row[3],
                    'rs_type': row[4],
                    'volume_expansion': row[5],
                    'price_vs_vwap_pct': row[6],
                }
    except Exception as e:
        logger.warning(f"Could not fetch opening patterns for {ticker}: {e}")
    
    return {}


def compute_breakout_score(patterns: Dict, sector: str) -> int:
    """Compute breakout pattern score (0-100)"""
    if not patterns or 'breakout_type' not in patterns:
        return 50  # Default to neutral
    
    breakout_type = patterns['breakout_type']
    
    if breakout_type == 'above_prior_high':
        return 75  # Bullish breakout
    elif breakout_type == 'below_prior_low':
        return 25  # Bearish breakout
    else:
        return 50  # No breakout


def compute_vwap_score(patterns: Dict) -> int:
    """Compute VWAP pattern score (0-100)"""
    if not patterns or 'vwap_pattern' not in patterns:
        return 50
    
    vwap_pattern = patterns['vwap_pattern']
    
    if vwap_pattern == 'reclaimed':
        return 80  # Strong bullish
    elif vwap_pattern == 'above_vwap':
        return 70
    elif vwap_pattern == 'near_vwap':
        return 50
    elif vwap_pattern == 'below_vwap':
        return 30
    elif vwap_pattern == 'rejected':
        return 20
    
    return 50


def compute_volatility_score(patterns: Dict) -> int:
    """Compute volatility analysis score (0-100)"""
    if not patterns or 'volatility_ratio' not in patterns:
        return 50
    
    vol_ratio = float(patterns['volatility_ratio'] or 1.0)
    
    if vol_ratio < 0.8:
        return 70  # Compression - potential breakout
    elif vol_ratio > 1.5:
        return 30  # Expansion - potential reversal
    else:
        return 50  # Normal volatility


def compute_trend_score(patterns: Dict) -> int:
    """Compute trend structure score (0-100)"""
    if not patterns or 'trend_structure' not in patterns:
        return 50
    
    trend = patterns['trend_structure']
    
    if trend == 'higher_highs_and_lows':
        return 80  # Strong uptrend
    elif trend == 'lower_highs_and_lows':
        return 20  # Strong downtrend
    elif trend == 'breakout_up':
        return 75
    elif trend == 'breakout_down':
        return 25
    else:
        return 50  # Mixed


def compute_rs_score(patterns: Dict) -> int:
    """Compute relative strength score (0-100)"""
    if not patterns or 'rs_type' not in patterns:
        return 50
    
    rs_type = patterns['rs_type']
    
    if rs_type == 'outperforming_both':
        return 80  # Relative strength
    elif rs_type == 'outperforming_spy':
        return 65
    elif rs_type == 'underperforming':
        return 30
    
    return 50


def compute_signal_confidence(signal_score: int, market_breadth: int) -> int:
    """
    Compute confidence percentage (0-100)
    Based on signal strength and market alignment
    """
    base_confidence = max(50, signal_score)  # Min 50%
    breadth_bonus = (market_breadth - 50) * 0.4 if market_breadth > 50 else 0  # +0-20%
    
    confidence = int(base_confidence + breadth_bonus)
    return min(100, max(0, confidence))


def estimate_session_return(signal_score: int, confidence: int) -> float:
    """
    Estimate expected session return based on pattern strength
    """
    base_return = (signal_score - 50) * 0.02  # -1% to +1% range
    confidence_adjustment = (confidence - 50) * 0.005  # Small adjustment
    
    estimated = base_return + confidence_adjustment
    return round(estimated, 3)


def generate_daily_recommendations(trade_date: datetime, dry_run: bool = False) -> Tuple[List, List]:
    """
    Main function: Generate Top 10 Bull + Top 10 Bear recommendations
    
    NEW: Integrates 3 data layers:
      Layer 1: pattern_scores (9:25 AM)
      Layer 2: pattern_priors (Friday)
      Layer 3: analog_matches (9:30 AM)
    """
    logger.info(f"Generating daily recommendations for {trade_date.date()} (9:35 AM)")
    
    # 1. Load market regime scores
    market_regimes = get_market_regime_scores(trade_date)
    logger.info(f"Market regimes: SPY={market_regimes['SPY']['regime']}, "
                f"QQQ={market_regimes['QQQ']['regime']}, "
                f"Breadth={market_regimes['breadth_score']}")
    
    # 2. Load pattern priors (Layer 2) - for all patterns
    pattern_priors = load_pattern_priors(trade_date)
    
    # 3. Compute scores for each ticker
    recommendations = []
    
    for ticker in TRACKED_TICKERS:
        sector = get_sector_for_ticker(ticker)
        sector_config = SECTOR_MAPPING[sector]
        
        # LAYER 1: Load pattern scores (computed 9:25 AM)
        pattern_scores = load_pattern_scores(ticker, trade_date)
        if not pattern_scores:
            logger.warning(f"  {ticker}: Skipping (pattern scores missing)")
            continue
        
        # LAYER 3: Load analog matches (computed 9:30 AM)
        analogs = load_analog_matches(ticker, trade_date)
        if not analogs:
            logger.warning(f"  {ticker}: Proceeding without analogs (9:30 AM step may not be complete)")
            analogs = None
        
        # Get sector-adjusted regime
        primary_regime = market_regimes[sector_config['primary_index']]['regime']
        secondary_regime = market_regimes.get(sector_config['secondary_index'], {}).get('regime')
        adjusted_regime, regime_adjustment = get_sector_adjusted_regime(
            sector, primary_regime, secondary_regime, market_regimes
        )
        
        # Compute bullish signal score from pattern scores
        bullish_score = (
            pattern_scores['breakout_score'] +
            pattern_scores['trend_score'] +
            pattern_scores['rs_score']
        ) / 3
        bullish_score += regime_adjustment
        bullish_score = max(0, min(100, bullish_score))
        
        # Compute base confidence (before calibration)
        base_confidence = compute_signal_confidence(int(bullish_score), market_regimes['breadth_score'])
        
        # CALIBRATION: Apply priors + analogs
        final_confidence, calibration_sources = apply_calibration(
            base_confidence=base_confidence,
            pattern_scores=pattern_scores,
            priors=pattern_priors,
            analogs=analogs,
        )
        
        # Estimate return
        predicted_return = estimate_session_return(int(bullish_score), final_confidence)
        
        # Determine signal grade
        if final_confidence >= 80:
            signal_grade = 'Good'
        elif final_confidence >= 70:
            signal_grade = 'Fair'
        else:
            signal_grade = 'Weak'
        
        # Create recommendation
        rec_type = 'BULL' if bullish_score > 50 else 'BEAR'
        score_for_ranking = bullish_score if rec_type == 'BULL' else (100 - bullish_score)
        
        recommendations.append({
            'ticker': ticker,
            'sector': sector,
            'type': rec_type,
            'score': score_for_ranking,
            'base_confidence': base_confidence,
            'final_confidence': final_confidence,
            'calibration_sources': calibration_sources,
            'predicted_return': predicted_return,
            'pattern_scores': pattern_scores,
            'regime_adjustment': regime_adjustment,
            'primary_index': sector_config['primary_index'],
            'primary_regime': primary_regime,
            'secondary_index': sector_config['secondary_index'],
            'secondary_regime': secondary_regime,
            'signal_grade': signal_grade,
            'market_breadth': market_regimes['breadth_score'],
            'score_id': pattern_scores.get('score_id'),
            'analog_id': analogs.get('analog_id') if analogs else None,
        })
    
    # 4. Rank and select Top 10
    bull_recs = sorted(
        [r for r in recommendations if r['type'] == 'BULL'],
        key=lambda x: x['score'] * (x['final_confidence'] / 100),
        reverse=True
    )[:10]
    
    bear_recs = sorted(
        [r for r in recommendations if r['type'] == 'BEAR'],
        key=lambda x: x['score'] * (x['final_confidence'] / 100),
        reverse=True
    )[:10]
    
    logger.info(f"Top Bull Recommendations:")
    for rank, rec in enumerate(bull_recs, 1):
        logger.info(f"  {rank}. {rec['ticker']}: {rec['score']:.0f} | "
                   f"Confidence: {rec['base_confidence']}% → {rec['final_confidence']}% | "
                   f"Sources: {rec['calibration_sources']}")
    
    logger.info(f"Top Bear Recommendations:")
    for rank, rec in enumerate(bear_recs, 1):
        logger.info(f"  {rank}. {rec['ticker']}: {rec['score']:.0f} | "
                   f"Confidence: {rec['base_confidence']}% → {rec['final_confidence']}% | "
                   f"Sources: {rec['calibration_sources']}")
    
    # 5. Upsert to database
    if not dry_run:
        upsert_recommendations_to_db(trade_date, bull_recs + bear_recs)
    
    return bull_recs, bear_recs


def upsert_recommendations_to_db(trade_date: datetime, recommendations: List):
    """
    Insert/update daily recommendations in database with calibration tracking.
    
    NEW: Stores calibration trace:
      - score_id → references opening_pattern_scores (Layer 1)
      - analog_id → references pattern_analog_matches (Layer 3)
      - pattern_priors_applied / analog_matching_applied flags
      - confidence_before_calibration / confidence_after_calibration
      - calibration_sources (text trace of adjustments)
    """
    
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            for rank, rec in enumerate(recommendations, 1):
                # Determine rank in category
                cat_rank = rank if rank <= 10 else rank - 10
                
                cur.execute("""
                    INSERT INTO daily_recommendations (
                        trade_date, ticker, sector, recommendation_type, rank_in_category,
                        predicted_return_pct, predicted_confidence_pct, signal_grade, signal_score,
                        primary_index, primary_regime, sector_etf, sector_regime, market_breadth_score,
                        breakout_score, vwap_score, volatility_score, trend_score, rs_score, calendar_score,
                        score_id, analog_id,
                        pattern_priors_applied, analog_matching_applied,
                        confidence_before_calibration, confidence_after_calibration, calibration_sources,
                        recommended_position_size_pct, auto_trade_enabled
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (trade_date, ticker, recommendation_type)
                    DO UPDATE SET
                        predicted_return_pct = EXCLUDED.predicted_return_pct,
                        predicted_confidence_pct = EXCLUDED.predicted_confidence_pct,
                        signal_score = EXCLUDED.signal_score,
                        market_breadth_score = EXCLUDED.market_breadth_score,
                        confidence_before_calibration = EXCLUDED.confidence_before_calibration,
                        confidence_after_calibration = EXCLUDED.confidence_after_calibration,
                        calibration_sources = EXCLUDED.calibration_sources,
                        auto_trade_enabled = EXCLUDED.auto_trade_enabled,
                        updated_at = NOW()
                """, (
                    trade_date.date(), rec['ticker'], rec['sector'], rec['type'], cat_rank,
                    Decimal(str(rec['predicted_return'])),
                    rec['final_confidence'],
                    rec['signal_grade'],
                    int(rec['score']),
                    rec['primary_index'],
                    rec['primary_regime'],
                    rec['secondary_index'],
                    rec['secondary_regime'],
                    rec['market_breadth'],
                    rec['pattern_scores']['breakout_score'],
                    rec['pattern_scores']['vwap_score'],
                    rec['pattern_scores']['volatility_score'],
                    rec['pattern_scores']['trend_score'],
                    rec['pattern_scores']['rs_score'],
                    rec['pattern_scores']['calendar_score'],
                    rec['score_id'],
                    rec['analog_id'],
                    True,  # pattern_priors_applied
                    rec['analog_id'] is not None,  # analog_matching_applied
                    rec['base_confidence'],
                    rec['final_confidence'],
                    rec['calibration_sources'],
                    Decimal('0.015') if rec['final_confidence'] >= 75 else Decimal('0.01'),
                    rec['final_confidence'] >= 75 and rec['signal_grade'] == 'Good'
                ))
            
            ctx.connection.commit()
            ctx.__exit__(None, None, None)
            logger.info(f"✓ Inserted {len(recommendations)} recommendations (with calibration tracking)")
    
    except Exception as e:
        logger.error(f"❌ Error inserting recommendations: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate daily recommendations')
    parser.add_argument('--date', type=str, help='Trade date (YYYY-MM-DD)', default=None)
    parser.add_argument('--dry-run', action='store_true', help='Do not insert into database')
    
    args = parser.parse_args()
    
    if args.date:
        trade_date = datetime.strptime(args.date, '%Y-%m-%d')
    else:
        trade_date = datetime.now()
    
    try:
        bull_recs, bear_recs = generate_daily_recommendations(trade_date, dry_run=args.dry_run)
        logger.info(f"✅ Generated {len(bull_recs)} bull + {len(bear_recs)} bear recommendations")
    except Exception as e:
        logger.error(f"❌ Error generating recommendations: {e}")
        sys.exit(1)
