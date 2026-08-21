#!/usr/bin/env python3
"""
Calibrate Pattern Win-Rate Priors (Weekly - Friday)

Purpose:
  Analyze past 60 days of recommendations to compute win-rate for each pattern.
  For each pattern (breakout, vwap, volatility, trend, rs, calendar):
    1. Find days where pattern "fired" (score > 60)
    2. Check if recommendation was HIT (actually profitable)
    3. Compute win rate = hits / fires
    4. Convert to confidence_multiplier = win_rate / 0.5 (neutral)
    5. Clamp multiplier to 0.5-1.5 range
  
  Stores priors in pattern_win_rate_priors table for recommendation calibration.
  This implements Layer 2: Historical Win-Rate Foundation.

Execution:
  Friday end-of-day: python calibrate_pattern_priors.py [--date YYYY-MM-DD] [--dry-run]

Output:
  - 6 rows in pattern_win_rate_priors table (one per pattern)
  - Historical win rate (e.g., 62%)
  - Sample size (how many days analyzed)
  - Confidence multiplier (e.g., 1.24)
  - Console log with priors

Dependencies:
  - database.py: get_db_cursor
  - daily_recommendations table (past 60 days)
  - recommendation_performance_log table
"""

import sys
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

# Add backend directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_db_cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("calibrate-pattern-priors")

ET = ZoneInfo("America/New_York")

PATTERNS = ['breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar']

# Patterns whose score encodes a direction (>60 bullish, <40 bearish). The rest
# are magnitude-only and say nothing about which way price goes.
DIRECTIONAL_PATTERNS = {'breakout', 'vwap', 'trend', 'rs'}

# Pseudo-counts pulling a small sample back toward a 50% base rate.
PRIOR_STRENGTH = 20.0


def compute_pattern_win_rates(pattern_name: str, as_of: str, lookback_days: int = 60) -> dict:
    """
    Compute historical win rate for a pattern.
    
    For past 60 days:
      1. Find recommendations where pattern "fired" (score > 60)
      2. Check if recommendation was HIT (recommendation_correct = true)
      3. Win rate = hits / fires
    
    Returns:
      {
        'win_rate': float (0.0-1.0),
        'sample_size': int,
        'confidence_multiplier': float (0.5-1.5),
      }
    """
    with get_db_cursor() as cur:
        # Get past 60 days of recommendations
        col_name = f'{pattern_name}_score'
        
        cur.execute(f"""
            SELECT
                {col_name} AS score,
                recommendation_type,
                recommendation_correct
            FROM daily_recommendations
            WHERE trade_date < %s
              AND trade_date >= %s::date - make_interval(days => %s)
              AND {col_name} IS NOT NULL
              AND recommendation_correct IS NOT NULL
            ORDER BY trade_date DESC
        """, (as_of, as_of, lookback_days))
        
        rows = cur.fetchall()
        
        if not rows:
            logger.warning(f"  {pattern_name}: No historical data found")
            return {
                'win_rate': 0.5,
                'sample_size': 0,
                'confidence_multiplier': 1.0,
            }
        
        # A bullish score only counts as a win when the call it backed was BULL.
        # Pooling both sides credits the pattern for calls it argued against.
        if pattern_name in DIRECTIONAL_PATTERNS:
            fired_events = [
                r for r in rows
                if (r['score'] > 60 and r['recommendation_type'] == 'BULL')
                or (r['score'] < 40 and r['recommendation_type'] == 'BEAR')
            ]
        else:
            fired_events = [r for r in rows if r['score'] > 60]
        
        if not fired_events:
            logger.warning(f"  {pattern_name}: Pattern never fired in past {lookback_days} days")
            return {
                'win_rate': 0.5,
                'sample_size': 0,
                'confidence_multiplier': 1.0,
            }
        
        # Count hits
        hits = sum(1 for r in fired_events if r['recommendation_correct'])
        n = len(fired_events)
        win_rate = hits / n
        
        # Beta-Binomial shrinkage: at n=0 the multiplier is 1.0, and it only
        # approaches the raw win rate once the sample is large.
        alpha = PRIOR_STRENGTH / 2.0
        shrunk = (hits + alpha) / (n + PRIOR_STRENGTH)
        confidence_multiplier = max(0.5, min(1.5, shrunk / 0.5))
        
        return {
            'win_rate': round(win_rate, 4),
            'sample_size': n,
            'confidence_multiplier': round(confidence_multiplier, 2),
        }


def insert_pattern_prior(
    effective_date: str,
    pattern_name: str,
    win_rate: float,
    sample_size: int,
    confidence_multiplier: float,
) -> bool:
    """
    Insert pattern prior into database.
    """
    with get_db_cursor() as cur:
        cur.execute("""
            INSERT INTO pattern_win_rate_priors (
                effective_date, pattern_name,
                historical_win_rate, sample_size, lookback_days,
                confidence_multiplier
            ) VALUES (%s, %s, %s, %s, 60, %s)
            ON CONFLICT (effective_date, pattern_name) DO UPDATE SET
                historical_win_rate = EXCLUDED.historical_win_rate,
                sample_size = EXCLUDED.sample_size,
                confidence_multiplier = EXCLUDED.confidence_multiplier
        """, (
            effective_date,
            pattern_name,
            int(win_rate * 100),  # Store as percentage (0-100)
            sample_size,
            confidence_multiplier,
        ))
        
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Calibrate pattern win-rate priors (Friday)")
    parser.add_argument('--date', type=str, help='Effective date for priors (YYYY-MM-DD)', default=None)
    parser.add_argument('--dry-run', action='store_true', help='Print priors without writing to DB')
    
    args = parser.parse_args()
    
    # Effective date = next Monday (priors go live on Monday)
    if args.date:
        effective_date = args.date
    else:
        today = datetime.now(ET).date()
        # Find next Monday
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        effective_date = (today + timedelta(days=days_until_monday)).isoformat()
    
    logger.info(f"Calibrating pattern win-rate priors (effective {effective_date})")
    
    # Compute priors for each pattern
    for pattern in PATTERNS:
        results = compute_pattern_win_rates(pattern, as_of=effective_date, lookback_days=60)
        
        logger.info(f"  {pattern:12s}: {results['win_rate']:.1%} win rate "
                   f"({results['sample_size']:3d} samples) → ×{results['confidence_multiplier']:.2f}")
        
        # Insert to database
        if not args.dry_run:
            insert_pattern_prior(
                effective_date=effective_date,
                pattern_name=pattern,
                win_rate=results['win_rate'],
                sample_size=results['sample_size'],
                confidence_multiplier=results['confidence_multiplier'],
            )
    
    logger.info(f"✓ Calibrated priors for {len(PATTERNS)} patterns")


if __name__ == '__main__':
    main()
