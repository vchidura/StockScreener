# Intraday Strategies Design

This document shows the API and UI design for adding interval support to screeners.

## API Changes

### Current Endpoint (Daily Only)
```python
@app.get("/api/scan/ma-crossover")
async def scan_ma_crossover(
    tickers: Optional[str] = None,
    short_period: int = Query(default=9),
    long_period: int = Query(default=21),
    scan_date: Optional[str] = None,
):
```

### Updated Endpoint (Multi-Timeframe)
```python
@app.get("/api/scan/ma-crossover")
async def scan_ma_crossover(
    tickers: Optional[str] = None,
    short_period: int = Query(default=9),
    long_period: int = Query(default=21),
    scan_date: Optional[str] = None,
    interval: str = Query(default="1d", regex="^(1m|5m|15m|1h|1d)$"),  # NEW
):
    """
    Scan for moving average crossover signals.
    
    Args:
        interval: Timeframe for analysis
            - '1d': Daily candles (default, uses stock_prices_daily)
            - '1h': Hourly candles (uses stock_prices_hourly)
            - '15m': 15-minute candles (uses stock_prices_intraday)
            - '5m': 5-minute candles (uses stock_prices_intraday)
            - '1m': 1-minute candles (uses stock_prices_intraday)
    """
```

## Frontend UI Changes

### Current UI (No Interval Selector)
```tsx
// MAScreener.tsx - current
<button onClick={() => runScan()}>Run Scan</button>
```

### Updated UI (With Interval Selector)
```tsx
// MAScreener.tsx - with interval support
const [interval, setInterval] = useState('1d')

// Interval selector component
<div className="interval-selector">
  <label>Timeframe:</label>
  <div className="interval-buttons">
    {[
      { value: '1d', label: 'Daily', icon: '📅' },
      { value: '1h', label: 'Hourly', icon: '⏰' },
      { value: '15m', label: '15 Min', icon: '⚡' },
      { value: '5m', label: '5 Min', icon: '🚀' },
    ].map(opt => (
      <button
        key={opt.value}
        className={`interval-btn ${interval === opt.value ? 'active' : ''}`}
        onClick={() => setInterval(opt.value)}
      >
        {opt.icon} {opt.label}
      </button>
    ))}
  </div>
</div>

// API call with interval
const results = await scanMACrossover({
  shortPeriod: 9,
  longPeriod: 21,
  interval: interval,  // NEW
})
```

## Visual Design Mockup

```
┌─────────────────────────────────────────────────────────────────┐
│  Moving Average Crossover Screener                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Timeframe:  [📅 Daily] [⏰ Hourly] [⚡ 15 Min] [🚀 5 Min]      │
│              ─────────                                          │
│                 ▲                                               │
│              selected                                           │
│                                                                 │
│  MA Periods:   Short [  9  ]    Long [ 21  ]                   │
│                                                                 │
│  [ 🔍 Run Scan ]                                                │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  Results (Hourly Timeframe)                     53 signals      │
│                                                                 │
│  ┌─────────┬──────────┬─────────┬─────────┬──────────────────┐ │
│  │ Ticker  │ Signal   │ MA Spread│ Price   │ Cross Time      │ │
│  ├─────────┼──────────┼─────────┼─────────┼──────────────────┤ │
│  │ AAPL    │ Bullish  │ +2.3%   │ $178.50 │ 2 hours ago     │ │
│  │ NVDA    │ Bullish  │ +1.8%   │ $890.25 │ 4 hours ago     │ │
│  │ TSLA    │ Bearish  │ -1.5%   │ $245.00 │ 1 hour ago      │ │
│  └─────────┴──────────┴─────────┴─────────┴──────────────────┘ │
│                                                                 │
│  ⚠️ Intraday data: last 7 days only                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Strategy Adaptations by Timeframe

Different timeframes benefit from different MA periods:

| Strategy | Daily | Hourly | 15-Min | 5-Min |
|----------|-------|--------|--------|-------|
| **Fast MA** | 9 | 8 | 5 | 3 |
| **Slow MA** | 21 | 21 | 13 | 8 |
| **Trend MA** | 50 | 50 | 21 | 13 |
| **Long-term** | 200 | 100 | 50 | 21 |

## Data Availability Warning

Show users when intraday data is limited:

```tsx
{interval !== '1d' && (
  <div className="warning-banner">
    ⚠️ {interval} data available for last {
      interval === '1h' ? '90 days' :
      interval === '15m' ? '7 days' :
      '7 days'
    } only. Daily analysis uses full history.
  </div>
)}
```

## Multi-Timeframe Confirmation

Advanced feature: Show alignment across timeframes

```
┌────────────────────────────────────────────────────────────────┐
│  AAPL - Multi-Timeframe Analysis                               │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Daily:   ✅ Bullish (9/21 cross 3 days ago, +2.5% since)      │
│  Hourly:  ✅ Bullish (8/21 cross 4 hours ago, +0.8% since)     │
│  15-Min:  ⚠️ Neutral (MAs converging)                          │
│                                                                │
│  Alignment Score: 🟢 Strong (2/3 bullish)                      │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```
