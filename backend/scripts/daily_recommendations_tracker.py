#!/usr/bin/env python3
"""
Daily Recommendations Tracker
Tracks performance of daily recommendations at 4:05 PM market close
Compares predicted vs actual returns, computes win rates, identifies pattern anomalies

Usage:
    python daily_recommendations_tracker.py [--date 2026-08-08] [--lookback 5]
    
Output:
    - Updates daily_recommendations with actual return data
    - Inserts daily performance report into recommendation_performance_log
    - Prints daily stats and rolling metrics
    - Flags patterns needing weight adjustments
"""

import sys
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from database import get_db_cursor

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_daily_recommendations(trade_date: datetime) -> List[Dict]:
    """Fetch all recommendations for a specific date"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            cur.execute("""
                SELECT rec_id, ticker, recommendation_type, predicted_return_pct,
                       predicted_confidence_pct, breakout_score, vwap_score,
                       volatility_score, trend_score, rs_score, calendar_score,
                       recommended_entry, signal_grade
                FROM daily_recommendations
                WHERE trade_date = %s
                ORDER BY rank_in_category
            """, (trade_date.date(),))
            
            columns = [desc[0] for desc in cur.description]
            recs = [dict(zip(columns, row)) for row in cur.fetchall()]
            ctx.__exit__(None, None, None)
            
            return recs
    except Exception as e:
        logger.error(f"Error fetching recommendations: {e}")
        return []


def get_closing_prices(tickers: List[str], trade_date: datetime) -> Dict[str, Decimal]:
    """Fetch closing prices for tickers on a specific date"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            cur.execute("""
                SELECT ticker, close_price
                FROM stock_prices_daily
                WHERE ticker = ANY(%s) AND date_trunc('day', datetime AT TIME ZONE 'America/New_York') = %s
            """, (tickers, trade_date.date()))
            
            prices = {}
            for row in cur.fetchall():
                prices[row[0]] = row[1]
            
            ctx.__exit__(None, None, None)
            return prices
    except Exception as e:
        logger.error(f"Error fetching closing prices: {e}")
        return {}


def get_daily_open_prices(tickers: List[str], trade_date: datetime) -> Dict[str, Decimal]:
    """Fetch opening prices for tickers on a specific date"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            cur.execute("""
                SELECT ticker, open_price
                FROM stock_prices_daily
                WHERE ticker = ANY(%s) AND date_trunc('day', datetime AT TIME ZONE 'America/New_York') = %s
            """, (tickers, trade_date.date()))
            
            prices = {}
            for row in cur.fetchall():
                prices[row[0]] = row[1]
            
            ctx.__exit__(None, None, None)
            return prices
    except Exception as e:
        logger.error(f"Error fetching opening prices: {e}")
        return {}


def compute_actual_return(entry_price: Decimal, close_price: Decimal) -> float:
    """Compute actual return percentage"""
    if not entry_price or entry_price == 0:
        return 0.0
    return float((close_price - entry_price) / entry_price * 100)


def determine_hit(predicted_return: float, actual_return: float, threshold: float = 0.5) -> bool:
    """Determine if recommendation was a HIT (actual within threshold of predicted)"""
    return abs(actual_return - predicted_return) < threshold


def track_daily_performance(trade_date: datetime, lookback_days: int = 20) -> Dict:
    """
    Track daily recommendations performance and generate report
    
    Returns: {
        'bull_hits': int,
        'bear_hits': int,
        'overall_win_rate': float,
        'pattern_accuracy': {...},
        'patterns_needing_adjustment': [...]
    }
    """
    logger.info(f"Tracking daily recommendations for {trade_date.date()}")
    
    # 1. Fetch recommendations
    recommendations = get_daily_recommendations(trade_date)
    if not recommendations:
        logger.warning(f"No recommendations found for {trade_date.date()}")
        return {}
    
    # 2. Fetch closing prices
    tickers = list(set(r['ticker'] for r in recommendations))
    close_prices = get_closing_prices(tickers, trade_date)
    open_prices = get_daily_open_prices(tickers, trade_date)
    
    if not close_prices:
        logger.warning(f"No closing prices found for {trade_date.date()}")
        return {}
    
    # 3. Evaluate each recommendation
    bull_hits = 0
    bear_hits = 0
    pattern_performance = defaultdict(lambda: {'hits': 0, 'total': 0})
    confidence_bins = defaultdict(lambda: {'hits': 0, 'total': 0})
    
    for rec in recommendations:
        ticker = rec['ticker']
        close = close_prices.get(ticker)
        
        if not close:
            logger.warning(f"No close price for {ticker}")
            continue
        
        # Use recommended entry or open price
        entry = rec['recommended_entry'] if rec['recommended_entry'] else open_prices.get(ticker, close)
        
        # Compute actual return
        actual_return = compute_actual_return(Decimal(str(entry)), close)
        predicted_return = float(rec['predicted_return_pct'] or 0.0)
        
        # Determine if HIT
        hit = determine_hit(predicted_return, actual_return)
        
        # Count hits by recommendation type
        if rec['recommendation_type'] == 'BULL':
            if actual_return > 0:  # Session positive
                bull_hits += 1
        else:  # BEAR
            if actual_return < 0:  # Session negative
                bear_hits += 1
        
        # Track pattern accuracy
        for pattern_name in ['breakout', 'vwap', 'volatility', 'trend', 'rs', 'calendar']:
            score_key = f'{pattern_name}_score'
            score = rec.get(score_key, 0)
            if score and score > 60:  # Pattern fired
                pattern_performance[pattern_name]['hits'] += 1 if hit else 0
                pattern_performance[pattern_name]['total'] += 1
        
        # Track confidence calibration
        conf = rec['predicted_confidence_pct']
        if conf >= 90:
            bin_key = '90-100'
        elif conf >= 80:
            bin_key = '80-90'
        elif conf >= 75:
            bin_key = '75-80'
        else:
            bin_key = '<75'
        
        confidence_bins[bin_key]['hits'] += 1 if hit else 0
        confidence_bins[bin_key]['total'] += 1
        
        # Update database
        update_recommendation_performance(
            rec_id=rec['rec_id'],
            actual_return=Decimal(str(actual_return)),
            recommendation_correct=hit
        )
    
    # 4. Compute win rates
    total_bull = len([r for r in recommendations if r['recommendation_type'] == 'BULL'])
    total_bear = len([r for r in recommendations if r['recommendation_type'] == 'BEAR'])
    total_recs = total_bull + total_bear
    
    bull_win_rate = (bull_hits / total_bull * 100) if total_bull > 0 else None
    bear_win_rate = (bear_hits / total_bear * 100) if total_bear > 0 else None
    overall_win_rate = ((bull_hits + bear_hits) / total_recs * 100) if total_recs > 0 else None
    
    logger.info(f"Daily Performance:")
    logger.info(f"  Bull: {bull_hits}/{total_bull} HIT ({bull_win_rate:.1f}%)" if bull_win_rate else "  Bull: No recs")
    logger.info(f"  Bear: {bear_hits}/{total_bear} HIT ({bear_win_rate:.1f}%)" if bear_win_rate else "  Bear: No recs")
    logger.info(f"  Overall: {bull_hits + bear_hits}/{total_recs} HIT ({overall_win_rate:.1f}%)")
    
    # 5. Pattern accuracy analysis
    logger.info("Pattern Accuracy:")
    pattern_accuracy = {}
    patterns_needing_adjustment = []
    
    for pattern_name, perf in pattern_performance.items():
        if perf['total'] > 0:
            accuracy = perf['hits'] / perf['total'] * 100
            pattern_accuracy[pattern_name] = round(accuracy, 2)
            logger.info(f"  {pattern_name}: {accuracy:.1f}% ({perf['hits']}/{perf['total']})")
            
            if accuracy < 55:  # Underperforming
                patterns_needing_adjustment.append({
                    'pattern': pattern_name,
                    'current_accuracy': round(accuracy, 2),
                    'required_accuracy': 65.0,
                    'sample_size': perf['total'],
                    'adjustment_suggested': -5
                })
    
    # 6. Confidence calibration analysis
    logger.info("Confidence Calibration:")
    confidence_calibration = {}
    for bin_key in sorted(confidence_bins.keys()):
        perf = confidence_bins[bin_key]
        if perf['total'] > 0:
            accuracy = perf['hits'] / perf['total'] * 100
            confidence_calibration[bin_key] = {
                'accuracy': round(accuracy, 2),
                'sample_size': perf['total']
            }
            logger.info(f"  {bin_key}%: {accuracy:.1f}% ({perf['hits']}/{perf['total']})")
    
    # 7. Compute rolling metrics
    rolling_stats = compute_rolling_stats(trade_date, lookback_days)
    logger.info(f"Rolling Metrics:")
    logger.info(f"  5-Day: {rolling_stats['5_day']:.1f}%")
    logger.info(f"  20-Day: {rolling_stats['20_day']:.1f}%")
    
    # 8. Insert performance report
    insert_performance_log(
        trade_date=trade_date,
        bull_total=total_bull,
        bull_hits=bull_hits,
        bear_total=total_bear,
        bear_hits=bear_hits,
        pattern_accuracy=pattern_accuracy,
        confidence_calibration=confidence_calibration,
        rolling_stats=rolling_stats,
        patterns_needing_adjustment=patterns_needing_adjustment
    )
    
    return {
        'trade_date': trade_date.date(),
        'bull_win_rate': bull_win_rate,
        'bear_win_rate': bear_win_rate,
        'overall_win_rate': overall_win_rate,
        'pattern_accuracy': pattern_accuracy,
        'confidence_calibration': confidence_calibration,
        'rolling_stats': rolling_stats,
        'patterns_needing_adjustment': patterns_needing_adjustment,
    }


def update_recommendation_performance(rec_id: int, actual_return: Decimal, recommendation_correct: bool):
    """Update a single recommendation with actual performance data"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            cur.execute("""
                UPDATE daily_recommendations
                SET actual_return_pct = %s,
                    recommendation_correct = %s,
                    updated_at = NOW()
                WHERE rec_id = %s
            """, (actual_return, recommendation_correct, rec_id))
            
            ctx.connection.commit()
            ctx.__exit__(None, None, None)
    except Exception as e:
        logger.error(f"Error updating recommendation {rec_id}: {e}")


def compute_rolling_stats(trade_date: datetime, lookback_days: int = 20) -> Dict[str, float]:
    """Compute rolling win rates (5-day, 20-day)"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            results = {'5_day': None, '20_day': None}
            
            for window, key in [(5, '5_day'), (lookback_days, '20_day')]:
                start_date = trade_date.date() - timedelta(days=window)
                
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN recommendation_correct = true THEN 1 ELSE 0 END) as hits
                    FROM daily_recommendations
                    WHERE trade_date >= %s AND trade_date <= %s
                      AND recommendation_correct IS NOT NULL
                """, (start_date, trade_date.date()))
                
                row = cur.fetchone()
                if row and row[0] > 0:
                    results[key] = (row[1] / row[0] * 100) if row[1] else 0.0
            
            ctx.__exit__(None, None, None)
            return results
    except Exception as e:
        logger.error(f"Error computing rolling stats: {e}")
        return {'5_day': None, '20_day': None}


def insert_performance_log(
    trade_date: datetime,
    bull_total: int,
    bull_hits: int,
    bear_total: int,
    bear_hits: int,
    pattern_accuracy: Dict[str, float],
    confidence_calibration: Dict[str, Dict],
    rolling_stats: Dict[str, float],
    patterns_needing_adjustment: List[Dict]
):
    """Insert daily performance report into database"""
    try:
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            bull_win_rate = (bull_hits / bull_total * 100) if bull_total > 0 else None
            bear_win_rate = (bear_hits / bear_total * 100) if bear_total > 0 else None
            total_recs = bull_total + bear_total
            overall_win_rate = ((bull_hits + bear_hits) / total_recs * 100) if total_recs > 0 else None
            
            # Extract confidence bin accuracy
            conf_75_80 = confidence_calibration.get('75-80', {})
            conf_80_90 = confidence_calibration.get('80-90', {})
            conf_90_100 = confidence_calibration.get('90-100', {})
            
            cur.execute("""
                INSERT INTO recommendation_performance_log (
                    report_date, 
                    bull_recommendations_total, bull_recommendations_hit, bull_win_rate_pct,
                    bear_recommendations_total, bear_recommendations_hit, bear_win_rate_pct,
                    overall_recommendations_total, overall_recommendations_hit, overall_win_rate_pct,
                    win_rate_5day, win_rate_20day,
                    breakout_pattern_accuracy, breakout_pattern_sample_size,
                    vwap_pattern_accuracy, vwap_pattern_sample_size,
                    volatility_pattern_accuracy, volatility_pattern_sample_size,
                    trend_pattern_accuracy, trend_pattern_sample_size,
                    rs_pattern_accuracy, rs_pattern_sample_size,
                    calendar_pattern_accuracy, calendar_pattern_sample_size,
                    signal_75_80_pct_accuracy, signal_75_80_pct_sample_size,
                    signal_80_90_pct_accuracy, signal_80_90_pct_sample_size,
                    signal_90_100_pct_accuracy, signal_90_100_pct_sample_size,
                    patterns_needing_adjustment, recommended_weight_changes
                ) VALUES (
                    %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (report_date) DO UPDATE SET
                    overall_win_rate_pct = EXCLUDED.overall_win_rate_pct,
                    win_rate_5day = EXCLUDED.win_rate_5day,
                    patterns_needing_adjustment = EXCLUDED.patterns_needing_adjustment,
                    updated_at = NOW()
            """, (
                trade_date.date(),
                bull_total, bull_hits, bull_win_rate,
                bear_total, bear_hits, bear_win_rate,
                total_recs, bull_hits + bear_hits, overall_win_rate,
                rolling_stats.get('5_day'), rolling_stats.get('20_day'),
                # Pattern accuracy
                pattern_accuracy.get('breakout'), len([1 for r in {} if pattern_accuracy.get('breakout')]),
                pattern_accuracy.get('vwap'), len([1 for r in {} if pattern_accuracy.get('vwap')]),
                pattern_accuracy.get('volatility'), len([1 for r in {} if pattern_accuracy.get('volatility')]),
                pattern_accuracy.get('trend'), len([1 for r in {} if pattern_accuracy.get('trend')]),
                pattern_accuracy.get('rs'), len([1 for r in {} if pattern_accuracy.get('rs')]),
                pattern_accuracy.get('calendar'), len([1 for r in {} if pattern_accuracy.get('calendar')]),
                # Confidence calibration
                conf_75_80.get('accuracy'), conf_75_80.get('sample_size'),
                conf_80_90.get('accuracy'), conf_80_90.get('sample_size'),
                conf_90_100.get('accuracy'), conf_90_100.get('sample_size'),
                # Patterns needing adjustment
                json.dumps(patterns_needing_adjustment),
                json.dumps([{
                    'pattern': p['pattern'],
                    'current': p['current_accuracy'],
                    'new': p['current_accuracy'] + p['adjustment_suggested'],
                    'change': p['adjustment_suggested']
                } for p in patterns_needing_adjustment])
            ))
            
            ctx.connection.commit()
            ctx.__exit__(None, None, None)
            logger.info(f"✅ Inserted performance report for {trade_date.date()}")
    except Exception as e:
        logger.error(f"Error inserting performance log: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Track daily recommendation performance')
    parser.add_argument('--date', type=str, help='Trade date (YYYY-MM-DD)', default=None)
    parser.add_argument('--lookback', type=int, default=20, help='Lookback days for rolling stats')
    
    args = parser.parse_args()
    
    if args.date:
        trade_date = datetime.strptime(args.date, '%Y-%m-%d')
    else:
        trade_date = datetime.now()
    
    try:
        stats = track_daily_performance(trade_date, lookback_days=args.lookback)
        logger.info(f"✅ Daily tracking complete: {stats['overall_win_rate']:.1f}% win rate")
    except Exception as e:
        logger.error(f"❌ Error tracking performance: {e}")
        sys.exit(1)
