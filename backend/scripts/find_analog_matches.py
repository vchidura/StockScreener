#!/usr/bin/env python3
"""
Find Pattern Analog Matches (9:30 AM)

Purpose:
  For each tracked ticker, find historical days with SIMILAR pattern scores.
  Specifically:
    1. Filter past 100 days with SAME sector regime
    2. Find days where pattern scores are within ±10 points of current
    3. Check outcomes of those days (HIT or MISS)
    4. Compute analog accuracy and confidence boost
  
  Stores matches in pattern_analog_matches table for recommendation calibration.
  This implements Layer 3: Trend Similarity via Historical Analog Matching.

Execution:
  9:30 AM: python find_analog_matches.py [--date YYYY-MM-DD] [--dry-run]

Output:
  - 11 rows in pattern_analog_matches table (one per tracked ticker)
  - Analog count (how many similar days found)
  - Analog accuracy (% of similar days that hit)
  - Confidence boost (-10 to +10 points)
  - Console log with matches

Dependencies:
  - database.py: get_db_cursor
  - opening_pattern_scores table (populated by compute_opening_pattern_scores.py)
  - daily_recommendations table (past data)
"""

import sys
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path

# Add backend directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_db_cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("find-analog-matches")

ET = ZoneInfo("America/New_York")

TRACKED_TICKERS = [
    'ASML', 'LLY', 'GS', 'GEV', 'MU', 'CAT', 'STX', 'SPY', 'QQQ', 'PWR',
    'CMI', 'DE', 'TMO', 'META', 'LMT', 'SMH', 'NOC', 'MA', 'DIA', 'AMAT',
    'BRK-B', 'MSFT', 'VRTX', 'AMD', 'TT', 'MCO', 'DELL', 'ETN', 'ROK', 'WDC'
]

# Sector mapping for regime lookup
SECTOR_MAPPING = {
    'AAPL': ('tech', 'XLK'),
    'MSFT': ('tech', 'XLK'),
    'NVDA': ('tech', 'XLK'),
    'TSLA': ('tech', 'XLK'),
    'GOOGL': ('tech', 'XLK'),
    'AMZN': ('tech', 'XLK'),
    'META': ('tech', 'XLK'),
    'INTC': ('tech', 'XLK'),
    'JPM': ('finance', 'XLF'),
    'BAC': ('finance', 'XLF'),
    'MCD': ('consumer', 'XLY'),
}


def compute_pattern_distance(current_scores: dict, historical_scores: dict, tolerance: int = 10) -> float:
    """
    Compute Euclidean distance between current and historical pattern scores.
    
    Returns distance or None if too different (> tolerance).
    """
    patterns = ['breakout_score', 'vwap_score', 'volatility_score', 'trend_score', 'rs_score', 'calendar_score']
    
    distance_squared = 0
    for pattern in patterns:
        current = current_scores.get(pattern, 50)
        historical = historical_scores.get(pattern, 50)
        
        diff = current - historical
        distance_squared += diff ** 2
    
    distance = distance_squared ** 0.5
    
    # Return None if distance too large (patterns too dissimilar)
    if distance > (tolerance * 3):  # Rough threshold: patterns quite different
        return None
    
    return distance


def find_pattern_analogs(ticker: str, trade_date: str) -> dict:
    """
    Find historical days with similar patterns and same sector regime.
    
    Returns:
      {
        'analog_count': int,
        'analog_accuracy': float (0.0-1.0),
        'analog_details': [{date, distance, actual_return, hit}, ...],
        'confidence_boost': int (-10 to +10),
      }
    """
    with get_db_cursor() as cur:
        # Step 1: Get current pattern scores (from opening_pattern_scores)
        cur.execute("""
            SELECT 
                breakout_score, vwap_score, volatility_score, 
                trend_score, rs_score, calendar_score,
                sector_regime, market_breadth_score
            FROM opening_pattern_scores
            WHERE trade_date = %s AND ticker = %s
        """, (trade_date, ticker))
        
        current_row = cur.fetchone()
        if not current_row:
            logger.warning(f"  {ticker}: No pattern scores found for {trade_date}")
            return {
                'analog_count': 0,
                'analog_accuracy': 0.5,
                'analog_details': [],
                'confidence_boost': 0,
            }
        
        current_scores = {
            'breakout_score': current_row['breakout_score'],
            'vwap_score': current_row['vwap_score'],
            'volatility_score': current_row['volatility_score'],
            'trend_score': current_row['trend_score'],
            'rs_score': current_row['rs_score'],
            'calendar_score': current_row['calendar_score'],
        }
        current_sector_regime = current_row['sector_regime']
        
        # Step 2: Get historical pattern scores (past 100 days with SAME sector regime)
        sector, sector_etf = SECTOR_MAPPING.get(ticker, ('unknown', 'XLK'))
        
        lookback_days = 100
        start_date = (datetime.strptime(trade_date, '%Y-%m-%d') - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        cur.execute("""
            SELECT 
                ops.trade_date,
                ops.breakout_score, ops.vwap_score, ops.volatility_score,
                ops.trend_score, ops.rs_score, ops.calendar_score,
                dr.actual_return_pct, dr.recommendation_correct
            FROM opening_pattern_scores ops
            LEFT JOIN daily_recommendations dr 
              ON dr.trade_date = ops.trade_date 
              AND dr.ticker = ops.ticker
            WHERE ops.ticker = %s
              AND ops.trade_date >= %s
              AND ops.trade_date < %s
              AND ops.sector_regime = %s
            ORDER BY ops.trade_date DESC
        """, (ticker, start_date, trade_date, current_sector_regime))
        
        historical_rows = cur.fetchall()
        
        if not historical_rows:
            logger.warning(f"  {ticker}: No historical analogs found with regime {current_sector_regime}")
            return {
                'analog_count': 0,
                'analog_accuracy': 0.5,
                'analog_details': [],
                'confidence_boost': 0,
            }
        
        # Step 3: Find similar days (within ±10 points tolerance)
        analogs = []
        tolerance = 10
        
        for hist_row in historical_rows:
            hist_scores = {
                'breakout_score': hist_row['breakout_score'],
                'vwap_score': hist_row['vwap_score'],
                'volatility_score': hist_row['volatility_score'],
                'trend_score': hist_row['trend_score'],
                'rs_score': hist_row['rs_score'],
                'calendar_score': hist_row['calendar_score'],
            }
            
            distance = compute_pattern_distance(current_scores, hist_scores, tolerance)
            
            if distance is not None and distance < (tolerance * 2):  # Similar enough
                # Skip days with no resolved outcome; treating them as misses
                # biases analog accuracy downward.
                if hist_row['recommendation_correct'] is None:
                    continue
                actual_return = hist_row['actual_return_pct']
                hit = hist_row['recommendation_correct']
                
                analogs.append({
                    'date': hist_row['trade_date'].isoformat(),
                    'distance': round(distance, 2),
                    'actual_return': round(float(actual_return) if actual_return else 0, 3),
                    'hit': bool(hit),
                })
        
        # Step 4: Compute analog accuracy
        if not analogs:
            return {
                'analog_count': 0,
                'analog_accuracy': 0.5,
                'analog_details': [],
                'confidence_boost': 0,
            }
        
        hits = sum(1 for a in analogs if a['hit'])
        analog_accuracy = hits / len(analogs)
        
        # Step 5: Compute confidence boost
        # Analog accuracy 60% → +4 points
        # Analog accuracy 40% → -4 points
        # Formula: (analog_accuracy - 0.5) * 20 (range: -10 to +10)
        confidence_boost = int((analog_accuracy - 0.5) * 20)
        confidence_boost = max(-10, min(10, confidence_boost))
        
        return {
            'analog_count': len(analogs),
            'analog_accuracy': round(analog_accuracy, 3),
            'analog_details': analogs,
            'confidence_boost': confidence_boost,
        }


def insert_analog_matches(
    current_trade_date: str,
    ticker: str,
    current_scores: dict,
    sector_regime: str,
    market_breadth: int,
    results: dict,
) -> bool:
    """
    Insert analog matching results into pattern_analog_matches table.
    """
    with get_db_cursor() as cur:
        cur.execute("""
            INSERT INTO pattern_analog_matches (
                current_trade_date, current_ticker,
                current_breakout_score, current_vwap_score, current_volatility_score,
                current_trend_score, current_rs_score, current_calendar_score,
                current_sector_regime, current_market_breadth,
                analog_count, analog_accuracy, analog_details, analog_confidence_boost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (current_trade_date, current_ticker) DO UPDATE SET
                analog_count = EXCLUDED.analog_count,
                analog_accuracy = EXCLUDED.analog_accuracy,
                analog_details = EXCLUDED.analog_details,
                analog_confidence_boost = EXCLUDED.analog_confidence_boost
        """, (
            current_trade_date, ticker,
            current_scores['breakout_score'], current_scores['vwap_score'],
            current_scores['volatility_score'], current_scores['trend_score'],
            current_scores['rs_score'], current_scores['calendar_score'],
            sector_regime, market_breadth,
            results['analog_count'],
            results['analog_accuracy'],
            json.dumps(results['analog_details']),
            results['confidence_boost'],
        ))
        
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Find pattern analog matches (9:30 AM)")
    parser.add_argument('--date', type=str, help='Trade date (YYYY-MM-DD)', default=None)
    parser.add_argument('--dry-run', action='store_true', help='Print results without writing to DB')
    
    args = parser.parse_args()
    
    trade_date = args.date or datetime.now(ET).strftime('%Y-%m-%d')
    
    logger.info(f"Finding pattern analog matches for {trade_date}")
    
    # Process each ticker
    for ticker in TRACKED_TICKERS:
        sector, sector_etf = SECTOR_MAPPING[ticker]
        
        # Get current pattern scores and regime
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT 
                    breakout_score, vwap_score, volatility_score, 
                    trend_score, rs_score, calendar_score,
                    sector_regime, market_breadth_score
                FROM opening_pattern_scores
                WHERE trade_date = %s AND ticker = %s
            """, (trade_date, ticker))
            
            pattern_row = cur.fetchone()
            if not pattern_row:
                logger.warning(f"  {ticker}: No pattern scores found")
                continue
            
            current_scores = {
                'breakout_score': pattern_row['breakout_score'],
                'vwap_score': pattern_row['vwap_score'],
                'volatility_score': pattern_row['volatility_score'],
                'trend_score': pattern_row['trend_score'],
                'rs_score': pattern_row['rs_score'],
                'calendar_score': pattern_row['calendar_score'],
            }
            sector_regime = pattern_row['sector_regime']
            market_breadth = pattern_row['market_breadth_score']
        
        # Find analog matches
        results = find_pattern_analogs(ticker, trade_date)
        
        logger.info(f"  {ticker}: {results['analog_count']} analogs, "
                   f"{results['analog_accuracy']:.1%} accuracy, {results['confidence_boost']:+d} boost")
        
        # Insert results
        if not args.dry_run:
            insert_analog_matches(
                current_trade_date=trade_date,
                ticker=ticker,
                current_scores=current_scores,
                sector_regime=sector_regime,
                market_breadth=market_breadth,
                results=results,
            )
    
    logger.info(f"✓ Found analog matches for {len(TRACKED_TICKERS)} tickers")


if __name__ == '__main__':
    main()
