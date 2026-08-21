#!/usr/bin/env python3
"""
Compute Opening Pattern Scores (9:25 AM)

Purpose:
  For each tracked ticker, compute 6 independent pattern scores based on opening price action
  (5-min, 15-min, 30-min candles from stock_prices_intraday table)
  
  Stores raw scores in opening_pattern_scores table BEFORE recommendations are generated.
  This decouples pattern scoring from recommendation logic, enabling:
  - Independent pattern analysis
  - Historical win-rate calculation (Layer 2)
  - Analog matching (Layer 3)

Execution:
  9:25 AM: python compute_opening_pattern_scores.py [--date YYYY-MM-DD] [--dry-run]

Output:
  - 11 rows in opening_pattern_scores table (one per tracked ticker)
  - All 6 pattern scores (0-100)
  - Fired flags (score > 60)
  - Market context (sector regime, breadth)
  - Console log with scores

Dependencies:
  - database.py: get_db_cursor, get_selected_tickers
  - screeners.py: analyze_market_regime
  - stock_prices_intraday table
"""

import sys
import logging
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
from pathlib import Path

# Add backend directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_db_cursor, get_selected_tickers
from screeners import analyze_market_regime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("compute-opening-pattern-scores")

ET = ZoneInfo("America/New_York")

TRACKED_TICKERS = [
    'ASML', 'LLY', 'GS', 'GEV', 'MU', 'CAT', 'STX', 'SPY', 'QQQ', 'PWR',
    'CMI', 'DE', 'TMO', 'META', 'LMT', 'SMH', 'NOC', 'MA', 'DIA', 'AMAT',
    'BRK-B', 'MSFT', 'VRTX', 'AMD', 'TT', 'MCO', 'DELL', 'ETN', 'ROK', 'WDC'
]

# Sector mapping for recommendation type
SECTOR_MAPPING = {
    'AAPL': ('tech', 'QQQ', 'XLK'),
    'MSFT': ('tech', 'QQQ', 'XLK'),
    'NVDA': ('tech', 'QQQ', 'XLK'),
    'TSLA': ('tech', 'QQQ', 'XLK'),
    'GOOGL': ('tech', 'QQQ', 'XLK'),
    'AMZN': ('tech', 'QQQ', 'XLK'),
    'META': ('tech', 'QQQ', 'XLK'),
    'INTC': ('tech', 'QQQ', 'XLK'),
    'JPM': ('finance', 'DIA', 'XLF'),
    'BAC': ('finance', 'DIA', 'XLF'),
    'MCD': ('consumer', 'DIA', 'XLY'),
}


def get_intraday_patterns(ticker: str, trade_date: str) -> dict:
    """
    Load 5-min, 15-min, 30-min bars from stock_prices_intraday.
    Compute opening patterns (first 2 hours: 9:30 AM - 11:30 AM).
    Return pattern metrics needed for scoring.
    """
    with get_db_cursor() as cur:
        # Get intraday bars for the trade date (first 2 hours only)
        cur.execute("""
            SELECT 
                datetime AT TIME ZONE 'America/New_York' AS datetime_et,
                open_price, high, low, close_price, volume, interval
            FROM stock_prices_intraday
            WHERE ticker = %s 
              AND DATE(datetime AT TIME ZONE 'America/New_York') = %s
              AND EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') < 12
            ORDER BY interval ASC, datetime ASC
        """, (ticker, trade_date))
        
        rows = cur.fetchall()
        
        if not rows:
            logger.warning(f"  {ticker}: No intraday data found for {trade_date}")
            return {
                'breakout_score': 50,
                'vwap_score': 50,
                'volatility_score': 50,
                'trend_score': 50,
                'rs_score': 50,
                'calendar_score': 50,
                'error': 'No data',
            }
        
        # Convert to dict for easier access
        bars_by_interval = {'5m': [], '15m': [], '30m': []}
        for row in rows:
            interval = row['interval']
            if interval in bars_by_interval:
                bars_by_interval[interval].append(row)
        
        # Compute pattern metrics
        patterns = {
            'breakout_score': _compute_breakout_score(bars_by_interval['5m']),
            'vwap_score': _compute_vwap_score(bars_by_interval['5m']),
            'volatility_score': _compute_volatility_score(bars_by_interval['30m']),
            'trend_score': _compute_trend_score(bars_by_interval['15m']),
            'rs_score': _compute_rs_score(ticker, trade_date),
            'calendar_score': _compute_calendar_score(trade_date),
        }
        
        return patterns


def _compute_breakout_score(bars_5m: list) -> int:
    """
    Breakout Score: Did price break above/below opening range early?
    
    Logic:
      - Opening range = first 30 min (bars[0:6] at 5-min intervals)
      - If price breaks above range in minutes 31-120 → Bull score
      - If price breaks below range → Bear score
      - Score = how quickly + how far beyond range
    """
    if len(bars_5m) < 6:
        return 50  # Not enough data
    
    opening_range = bars_5m[:6]
    open_high = max(bar['high'] for bar in opening_range)
    open_low = min(bar['low'] for bar in opening_range)
    
    breakout_score = 50
    
    # Check if price breaks above range
    for bar in bars_5m[6:]:
        if bar['high'] > open_high:
            breakout_distance = (bar['high'] - open_high) / open_high * 100
            breakout_score = int(50 + min(50, breakout_distance * 10))  # 50-100
            break
        elif bar['low'] < open_low:
            breakout_distance = (open_low - bar['low']) / open_low * 100
            breakout_score = int(50 - min(50, breakout_distance * 10))  # 0-50
            break
    
    return max(0, min(100, breakout_score))


def _compute_vwap_score(bars_5m: list) -> int:
    """
    VWAP Score: Is price aligned with Volume-Weighted Average Price?
    
    Logic:
      - Compute VWAP of opening 5-min bars
      - If current price > VWAP + score up to 100
      - If current price < VWAP - score down to 0
    """
    if len(bars_5m) < 3:
        return 50
    
    total_vol = sum(bar['volume'] for bar in bars_5m)
    if total_vol == 0:
        return 50
    
    vwap = sum(
        (bar['close_price'] * bar['volume'])
        for bar in bars_5m
    ) / total_vol
    
    current_price = bars_5m[-1]['close_price']
    vwap_diff_pct = (current_price - vwap) / vwap * 100
    
    # -2% to +2% maps to 0-100
    vwap_score = 50 + (vwap_diff_pct * 25)
    
    return max(0, min(100, int(vwap_score)))


def _compute_volatility_score(bars_30m: list) -> int:
    """
    Volatility Score: Is there enough range to trade?
    
    Logic:
      - Compute ATR of 30-min bars (Average True Range)
      - High ATR = more opportunity (higher score)
      - Low ATR = choppy/tight market (lower score)
    """
    if len(bars_30m) < 3:
        return 50
    
    true_ranges = []
    prev_close = bars_30m[0]['open_price']
    
    for bar in bars_30m:
        high_low = bar['high'] - bar['low']
        high_prev = abs(bar['high'] - prev_close)
        low_prev = abs(bar['low'] - prev_close)
        
        tr = max(high_low, high_prev, low_prev)
        true_ranges.append(tr)
        prev_close = bar['close_price']
    
    atr = sum(true_ranges) / len(true_ranges)
    current_price = bars_30m[-1]['close_price']
    atr_pct = (atr / current_price) * 100
    
    # 0.5% ATR = low volatility (40), 2.0% ATR = high volatility (100)
    volatility_score = (atr_pct / 2.0) * 100
    
    return max(0, min(100, int(volatility_score)))


def _compute_trend_score(bars_15m: list) -> int:
    """
    Trend Score: Is price in an uptrend or downtrend?
    
    Logic:
      - Compute SMA of close prices
      - If current price > SMA 20 → Bull (higher score)
      - If current price < SMA 20 → Bear (lower score)
    """
    if len(bars_15m) < 5:
        return 50
    
    closes = [bar['close_price'] for bar in bars_15m[-5:]]
    sma = sum(closes) / len(closes)
    
    current_price = bars_15m[-1]['close_price']
    trend_pct = (current_price - sma) / sma * 100
    
    # -2% to +2% maps to 0-100
    trend_score = 50 + (trend_pct * 25)
    
    return max(0, min(100, int(trend_score)))


def _compute_rs_score(ticker: str, trade_date: str) -> int:
    """
    Relative Strength Score: How is this ticker performing vs its sector ETF?
    
    Logic:
      - Compare intraday return of ticker vs sector ETF
      - If ticker outperforming → higher score
    """
    with get_db_cursor() as cur:
        # Get ticker intraday return (today open to latest)
        cur.execute("""
            SELECT open_price, close_price FROM stock_prices_intraday
            WHERE ticker = %s AND DATE(datetime AT TIME ZONE 'America/New_York') = %s
            ORDER BY datetime DESC LIMIT 1
        """, (ticker, trade_date))
        
        ticker_row = cur.fetchone()
        if not ticker_row:
            return 50
        
        ticker_return = (ticker_row['close_price'] - ticker_row['open_price']) / ticker_row['open_price']
        
        # Get sector ETF intraday return
        sector, _, sector_etf = SECTOR_MAPPING.get(ticker, ('unknown', 'SPY', 'XLK'))
        
        cur.execute("""
            SELECT open_price, close_price FROM stock_prices_intraday
            WHERE ticker = %s AND DATE(datetime AT TIME ZONE 'America/New_York') = %s
            ORDER BY datetime DESC LIMIT 1
        """, (sector_etf, trade_date))
        
        sector_row = cur.fetchone()
        if not sector_row:
            return 50
        
        sector_return = (sector_row['close_price'] - sector_row['open_price']) / sector_row['open_price']
        
        # RS = ticker_return - sector_return (outperformance)
        rs_outperformance = ticker_return - sector_return
        
        # -2% to +2% maps to 0-100
        rs_score = 50 + (rs_outperformance * 2500)
        
        return max(0, min(100, int(rs_score)))


def _compute_calendar_score(trade_date: str) -> int:
    """
    Calendar Score: Day-of-week seasonality
    
    Logic:
      - Mondays historically strong = 70
      - Tuesdays neutral = 50
      - Wednesdays strong = 75
      - Thursdays neutral = 50
      - Fridays weak = 35
    """
    import datetime as dt
    
    date_obj = dt.datetime.strptime(trade_date, '%Y-%m-%d').date()
    weekday = date_obj.weekday()  # 0=Monday, 4=Friday
    
    calendar_scores = [70, 50, 75, 50, 35]  # Mon-Fri
    
    return calendar_scores[weekday]


def insert_pattern_scores(
    trade_date: str,
    ticker: str,
    scores: dict,
    sector: str,
    primary_regime: str,
    sector_regime: str,
    breadth_score: int,
) -> bool:
    """
    Insert computed pattern scores into opening_pattern_scores table.
    """
    with get_db_cursor() as cur:
        # Determine which patterns "fired" (score > 60)
        fired = {pattern: scores[pattern] > 60 for pattern in scores}
        
        cur.execute("""
            INSERT INTO opening_pattern_scores (
                trade_date, ticker, sector,
                breakout_score, vwap_score, volatility_score, trend_score, rs_score, calendar_score,
                breakout_fired, vwap_fired, volatility_fired, trend_fired, rs_fired, calendar_fired,
                primary_regime, sector_regime, market_breadth_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, ticker) DO UPDATE SET
                breakout_score = EXCLUDED.breakout_score,
                vwap_score = EXCLUDED.vwap_score,
                volatility_score = EXCLUDED.volatility_score,
                trend_score = EXCLUDED.trend_score,
                rs_score = EXCLUDED.rs_score,
                calendar_score = EXCLUDED.calendar_score,
                breakout_fired = EXCLUDED.breakout_fired,
                vwap_fired = EXCLUDED.vwap_fired,
                volatility_fired = EXCLUDED.volatility_fired,
                trend_fired = EXCLUDED.trend_fired,
                rs_fired = EXCLUDED.rs_fired,
                calendar_fired = EXCLUDED.calendar_fired,
                primary_regime = EXCLUDED.primary_regime,
                sector_regime = EXCLUDED.sector_regime,
                market_breadth_score = EXCLUDED.market_breadth_score
        """, (
            trade_date, ticker, sector,
            scores['breakout_score'], scores['vwap_score'], scores['volatility_score'],
            scores['trend_score'], scores['rs_score'], scores['calendar_score'],
            fired['breakout_score'], fired['vwap_score'], fired['volatility_score'],
            fired['trend_score'], fired['rs_score'], fired['calendar_score'],
            primary_regime, sector_regime, breadth_score,
        ))
        
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Compute opening pattern scores (9:25 AM)")
    parser.add_argument('--date', type=str, help='Trade date (YYYY-MM-DD)', default=None)
    parser.add_argument('--dry-run', action='store_true', help='Print scores without writing to DB')
    
    args = parser.parse_args()
    
    trade_date = args.date or datetime.now(ET).strftime('%Y-%m-%d')
    
    logger.info(f"Computing opening pattern scores for {trade_date}")
    
    with get_db_cursor() as cur:
        # Get market regime
        market_regime = analyze_market_regime(trade_date)
        breadth_score = market_regime.get('breadth_score', 50)
        
        logger.info(f"Market breadth score: {breadth_score}")
    
    # Compute scores for each ticker
    for ticker in TRACKED_TICKERS:
        sector, primary_index, sector_etf = SECTOR_MAPPING[ticker]
        
        # Get sector regime for this ticker
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT close_regime FROM sector_regime_daily
                WHERE trade_date = %s AND etf_symbol = %s
                LIMIT 1
            """, (trade_date, sector_etf))
            
            sector_row = cur.fetchone()
            sector_regime = sector_row['close_regime'] if sector_row else 'Neutral'
        
        # Compute pattern scores
        scores = get_intraday_patterns(ticker, trade_date)
        
        logger.info(f"  {ticker}: Breakout {scores['breakout_score']} | VWAP {scores['vwap_score']} | "
                   f"Vol {scores['volatility_score']} | Trend {scores['trend_score']} | "
                   f"RS {scores['rs_score']} | Cal {scores['calendar_score']}")
        
        # Insert to database
        if not args.dry_run:
            insert_pattern_scores(
                trade_date=trade_date,
                ticker=ticker,
                scores=scores,
                sector=sector,
                primary_regime='Bull' if market_regime.get('spy_regime') == 'Bull' else 'Neutral',
                sector_regime=sector_regime,
                breadth_score=breadth_score,
            )
    
    logger.info(f"✓ Computed pattern scores for {len(TRACKED_TICKERS)} tickers")


if __name__ == '__main__':
    main()
