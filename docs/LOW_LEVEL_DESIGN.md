# Stock Screener Portal - Low Level Design

## 1. Database Schema

### 1.1 Current Tables

```sql
-- Stock price data (existing)
CREATE TABLE stock_prices_hourly (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open_price REAL,
    high REAL,
    low REAL,
    close_price REAL,
    volume INTEGER,
    UNIQUE (ticker, datetime)
);

-- Indexes for performance
CREATE INDEX idx_ticker_hourly ON stock_prices_hourly (ticker);
CREATE INDEX idx_datetime_hourly ON stock_prices_hourly (datetime);
CREATE INDEX idx_ticker_datetime_hourly ON stock_prices_hourly (ticker, datetime DESC);

-- Gap scan results cache
CREATE TABLE gap_scan_results (
    id SERIAL PRIMARY KEY,
    scan_date TIMESTAMP DEFAULT NOW(),
    ticker TEXT NOT NULL,
    gap_type TEXT NOT NULL,
    gap_low REAL,
    gap_high REAL,
    last_close REAL,
    gap_diff REAL,
    gap_date DATE,
    UNIQUE (scan_date, ticker, gap_type)
);
```

### 1.2 Planned Tables

```sql
-- Watchlists
CREATE TABLE watchlists (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE watchlist_tickers (
    id SERIAL PRIMARY KEY,
    watchlist_id INTEGER REFERENCES watchlists(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    notes TEXT,
    added_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (watchlist_id, ticker)
);

-- Alert configurations
CREATE TABLE alert_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    strategy VARCHAR(50) NOT NULL,  -- 'gap', 'ma_crossover', 'rsi', 'volume'
    conditions JSONB NOT NULL,      -- {"rsi_below": 30, "volume_multiplier": 2}
    tickers TEXT[],                 -- NULL = all tickers
    notification_channels TEXT[],   -- ['telegram', 'email']
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Alert history
CREATE TABLE alert_history (
    id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES alert_rules(id),
    ticker VARCHAR(10) NOT NULL,
    signal_type VARCHAR(100) NOT NULL,
    signal_data JSONB,
    triggered_at TIMESTAMP DEFAULT NOW(),
    notified_at TIMESTAMP
);

-- Scan history for analytics
CREATE TABLE scan_history (
    id SERIAL PRIMARY KEY,
    strategy VARCHAR(50) NOT NULL,
    parameters JSONB,
    tickers_scanned INTEGER,
    signals_found INTEGER,
    duration_ms INTEGER,
    scanned_at TIMESTAMP DEFAULT NOW()
);

-- User preferences (future)
CREATE TABLE user_settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

---

## 2. API Specifications

### 2.1 Current Endpoints

#### GET /api/scan/gaps
Scan for gap strategies.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| tickers | string | null | Comma-separated tickers (null = all) |

**Response:**
```json
{
  "scan_datetime": "2026-03-21T09:30:00",
  "total_scanned": 500,
  "total_signals": 45,
  "results_by_type": {
    "At Support (Near Gap Up Highs)": [
      {
        "ticker": "AAPL",
        "gap_type": "At Support (Near Gap Up Highs)",
        "gap_low": 175.50,
        "gap_high": 178.25,
        "last_close": 176.80,
        "gap_diff": 2.75,
        "gap_date": "2026-03-15"
      }
    ]
  },
  "results": [...]
}
```

#### GET /api/scan/ma-crossover
**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| tickers | string | null | Comma-separated tickers |
| short_period | int | 9 | Short MA period (2-50) |
| long_period | int | 21 | Long MA period (5-200) |

#### GET /api/scan/rsi
**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| tickers | string | null | Comma-separated tickers |
| period | int | 14 | RSI period (5-50) |
| oversold | int | 30 | Oversold threshold (10-40) |
| overbought | int | 70 | Overbought threshold (60-90) |

#### GET /api/scan/volume-breakout
**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| tickers | string | null | Comma-separated tickers |
| volume_multiplier | float | 2.0 | Volume ratio threshold (1.5-10) |

#### GET /api/stock/{ticker}/chart
**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| period | string | "1y" | Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y) |
| interval | string | "1d" | Data interval (1m, 5m, 15m, 1h, 1d, 1wk) |

**Response:**
```json
[
  {
    "time": 1710979200,
    "open": 175.50,
    "high": 178.25,
    "low": 174.80,
    "close": 177.90,
    "volume": 52000000
  }
]
```

### 2.2 Planned Endpoints

#### Watchlists
```
GET    /api/watchlists              - List all watchlists
POST   /api/watchlists              - Create watchlist
GET    /api/watchlists/{id}         - Get watchlist details
PUT    /api/watchlists/{id}         - Update watchlist
DELETE /api/watchlists/{id}         - Delete watchlist
POST   /api/watchlists/{id}/tickers - Add ticker to watchlist
DELETE /api/watchlists/{id}/tickers/{ticker} - Remove ticker
```

#### Alerts
```
GET    /api/alerts                  - List alert rules
POST   /api/alerts                  - Create alert rule
PUT    /api/alerts/{id}             - Update alert rule
DELETE /api/alerts/{id}             - Delete alert rule
GET    /api/alerts/history          - Get alert history
POST   /api/alerts/test             - Test alert configuration
```

#### New Screeners
```
GET /api/scan/vwap                  - VWAP deviation scanner
GET /api/scan/fibonacci             - Fibonacci retracement levels
GET /api/scan/patterns              - Chart pattern detection
GET /api/scan/sector-rotation       - Sector strength analysis
```

---

## 3. Screener Algorithms

### 3.1 Gap Strategy Logic
```python
def identify_gap(df, gap_threshold=0.01):
    """
    Gap Up: current_open > previous_high * (1 + threshold)
    Gap Down: current_open < previous_low * (1 - threshold)
    """
    gaps = []
    for i in range(1, len(df)):
        prev_high = df.iloc[i-1]['high']
        prev_low = df.iloc[i-1]['low']
        curr_open = df.iloc[i]['open']
        
        # Gap Up
        if curr_open > prev_high * (1 + gap_threshold):
            gaps.append({
                'type': 'gap_up',
                'index': i,
                'gap_low': prev_high,
                'gap_high': df.iloc[i]['low']
            })
        # Gap Down
        elif curr_open < prev_low * (1 - gap_threshold):
            gaps.append({
                'type': 'gap_down', 
                'index': i,
                'gap_high': prev_low,
                'gap_low': df.iloc[i]['high']
            })
    return gaps
```

### 3.2 VWAP Screener (Planned)
```python
def calculate_vwap(df):
    """
    VWAP = Cumulative(Price * Volume) / Cumulative(Volume)
    Typical Price = (High + Low + Close) / 3
    """
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['tp_volume'] = df['typical_price'] * df['volume']
    df['cumulative_tp_vol'] = df['tp_volume'].cumsum()
    df['cumulative_vol'] = df['volume'].cumsum()
    df['vwap'] = df['cumulative_tp_vol'] / df['cumulative_vol']
    return df

def scan_vwap_deviation(ticker, df, deviation_threshold=0.02):
    """
    Signal when price deviates significantly from VWAP
    """
    df = calculate_vwap(df)
    last_close = df.iloc[-1]['close']
    last_vwap = df.iloc[-1]['vwap']
    deviation = (last_close - last_vwap) / last_vwap
    
    if abs(deviation) >= deviation_threshold:
        return {
            'ticker': ticker,
            'signal': 'Above VWAP' if deviation > 0 else 'Below VWAP',
            'vwap': round(last_vwap, 2),
            'last_close': round(last_close, 2),
            'deviation_pct': round(deviation * 100, 2)
        }
    return None
```

### 3.3 Fibonacci Retracement (Planned)
```python
FIBONACCI_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]

def calculate_fibonacci_levels(high, low, trend='up'):
    """
    Calculate Fibonacci retracement levels
    """
    diff = high - low
    levels = {}
    
    for fib in FIBONACCI_LEVELS:
        if trend == 'up':
            levels[f'{fib*100:.1f}%'] = high - (diff * fib)
        else:
            levels[f'{fib*100:.1f}%'] = low + (diff * fib)
    
    return levels

def find_nearest_fib_level(price, fib_levels, threshold=0.01):
    """
    Check if price is near any Fibonacci level
    """
    for level_name, level_price in fib_levels.items():
        if abs(price - level_price) / level_price <= threshold:
            return level_name, level_price
    return None, None
```

### 3.4 Pattern Recognition (Planned)
```python
def detect_double_top(df, lookback=20, tolerance=0.02):
    """
    Detect double top pattern
    - Two peaks at approximately same level
    - Valley between them
    """
    peaks = find_local_peaks(df['high'], lookback)
    
    if len(peaks) >= 2:
        peak1, peak2 = peaks[-2], peaks[-1]
        peak1_price = df.iloc[peak1]['high']
        peak2_price = df.iloc[peak2]['high']
        
        if abs(peak1_price - peak2_price) / peak1_price <= tolerance:
            return {
                'pattern': 'double_top',
                'peak1_date': df.index[peak1],
                'peak2_date': df.index[peak2],
                'resistance_level': max(peak1_price, peak2_price)
            }
    return None
```

---

## 4. Frontend Components

### 4.1 Component Hierarchy
```
App
├── Navbar
├── Routes
│   ├── Dashboard
│   │   ├── StatCard (x4)
│   │   ├── StrategyGrid
│   │   │   └── StrategyCard (x4)
│   │   └── RecentSignalsTable
│   │
│   ├── GapScreener
│   │   ├── ScanControls
│   │   ├── FilterBar
│   │   ├── SummaryStats
│   │   └── ResultsTable (grouped by gap_type)
│   │
│   ├── TickerDetail
│   │   ├── PriceHeader
│   │   ├── ChartControls
│   │   ├── CandlestickChart
│   │   │   ├── PriceSeries
│   │   │   ├── VolumeSeries
│   │   │   └── DrawingTools (planned)
│   │   └── StatsGrid
│   │
│   ├── Watchlists (planned)
│   │   ├── WatchlistSidebar
│   │   ├── TickerList
│   │   └── AddTickerModal
│   │
│   └── AlertsManager (planned)
│       ├── AlertRulesList
│       ├── CreateRuleForm
│       └── AlertHistoryTable
│
└── SharedComponents
    ├── LoadingSpinner
    ├── DataTable
    ├── Badge
    └── Button
```

### 4.2 State Management
```typescript
// Current: Component-level state with hooks

// Planned: Context or Zustand for global state
interface AppState {
  // Watchlists
  watchlists: Watchlist[];
  activeWatchlist: string | null;
  
  // Alerts
  alertRules: AlertRule[];
  alertHistory: Alert[];
  
  // Scan cache
  lastScanResults: {
    gaps: GapScanResponse | null;
    ma: MAScanResponse | null;
    rsi: RSIScanResponse | null;
    volume: VolumeScanResponse | null;
    timestamp: number;
  };
  
  // User preferences
  settings: {
    theme: 'light' | 'dark';
    defaultPeriod: string;
    defaultInterval: string;
  };
}
```

### 4.3 Chart Drawing Tools (Planned)
```typescript
interface DrawingTool {
  type: 'trendline' | 'horizontal' | 'fibonacci' | 'rectangle';
  points: { time: number; price: number }[];
  color: string;
  lineWidth: number;
}

// Integration with Lightweight Charts
function addTrendLine(chart: IChartApi, start: Point, end: Point) {
  const lineSeries = chart.addLineSeries({
    color: '#2563eb',
    lineWidth: 2,
    lineStyle: LineStyle.Solid,
  });
  lineSeries.setData([
    { time: start.time, value: start.price },
    { time: end.time, value: end.price },
  ]);
}
```

---

## 5. Real-Time Data Integration

### 5.1 Polygon.io WebSocket (Planned)
```python
import websockets
import json

POLYGON_WS_URL = "wss://socket.polygon.io/stocks"

async def connect_polygon(api_key: str, tickers: list[str]):
    async with websockets.connect(POLYGON_WS_URL) as ws:
        # Authenticate
        await ws.send(json.dumps({"action": "auth", "params": api_key}))
        
        # Subscribe to tickers
        await ws.send(json.dumps({
            "action": "subscribe",
            "params": ",".join([f"T.{t}" for t in tickers])
        }))
        
        # Process messages
        async for message in ws:
            data = json.loads(message)
            for event in data:
                if event['ev'] == 'T':  # Trade event
                    yield {
                        'ticker': event['sym'],
                        'price': event['p'],
                        'size': event['s'],
                        'timestamp': event['t']
                    }
```

### 5.2 Frontend WebSocket Client
```typescript
interface RealtimeQuote {
  ticker: string;
  price: number;
  change: number;
  changePercent: number;
  volume: number;
  timestamp: number;
}

function useRealtimeQuotes(tickers: string[]) {
  const [quotes, setQuotes] = useState<Map<string, RealtimeQuote>>(new Map());
  
  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/quotes');
    
    ws.onopen = () => {
      ws.send(JSON.stringify({ action: 'subscribe', tickers }));
    };
    
    ws.onmessage = (event) => {
      const quote = JSON.parse(event.data);
      setQuotes(prev => new Map(prev).set(quote.ticker, quote));
    };
    
    return () => ws.close();
  }, [tickers]);
  
  return quotes;
}
```

---

## 6. Alert System Design

### 6.1 Alert Rule Engine
```python
from dataclasses import dataclass
from typing import Any, Callable

@dataclass
class AlertCondition:
    field: str           # 'rsi', 'price', 'volume_ratio'
    operator: str        # 'lt', 'gt', 'eq', 'between'
    value: Any           # 30, [25, 35], etc.

class AlertEngine:
    def __init__(self):
        self.operators = {
            'lt': lambda a, b: a < b,
            'gt': lambda a, b: a > b,
            'eq': lambda a, b: a == b,
            'between': lambda a, b: b[0] <= a <= b[1],
        }
    
    def evaluate(self, data: dict, conditions: list[AlertCondition]) -> bool:
        for cond in conditions:
            value = data.get(cond.field)
            if value is None:
                return False
            if not self.operators[cond.operator](value, cond.value):
                return False
        return True
    
    def process_scan_results(self, results: list, rules: list) -> list:
        """Check all results against all rules, return triggered alerts"""
        alerts = []
        for rule in rules:
            for result in results:
                if self.evaluate(result, rule.conditions):
                    alerts.append({
                        'rule_id': rule.id,
                        'rule_name': rule.name,
                        'ticker': result['ticker'],
                        'signal': result,
                        'triggered_at': datetime.now()
                    })
        return alerts
```

### 6.2 Notification Channels
```python
import aiohttp
from abc import ABC, abstractmethod

class NotificationChannel(ABC):
    @abstractmethod
    async def send(self, message: str, metadata: dict) -> bool:
        pass

class TelegramChannel(NotificationChannel):
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
    
    async def send(self, message: str, metadata: dict) -> bool:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }) as resp:
                return resp.status == 200

class EmailChannel(NotificationChannel):
    def __init__(self, sendgrid_key: str, from_email: str):
        self.sendgrid_key = sendgrid_key
        self.from_email = from_email
    
    async def send(self, message: str, metadata: dict) -> bool:
        # SendGrid implementation
        pass
```

---

## 7. Caching Strategy

### 7.1 Redis Cache Keys
```
# Scan results (TTL: 5 minutes)
scan:gaps:{ticker_hash}:{timestamp}
scan:ma:{short}:{long}:{ticker_hash}
scan:rsi:{period}:{ticker_hash}

# Stock data (TTL: 1 hour for daily, 5 min for intraday)  
stock:{ticker}:daily:{date}
stock:{ticker}:intraday:{date}:{interval}

# Computed indicators (TTL: 15 minutes)
indicator:{ticker}:vwap:{date}
indicator:{ticker}:fibonacci:{date}

# Rate limiting
ratelimit:ip:{ip_address}
```

### 7.2 Cache Implementation
```python
import redis
import json
from functools import wraps

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def cache_result(prefix: str, ttl_seconds: int = 300):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key = f"{prefix}:{hash(str(args) + str(kwargs))}"
            
            # Check cache
            cached = redis_client.get(key)
            if cached:
                return json.loads(cached)
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Store in cache
            redis_client.setex(key, ttl_seconds, json.dumps(result))
            return result
        return wrapper
    return decorator

# Usage
@cache_result(prefix="scan:gaps", ttl_seconds=300)
async def scan_gaps(tickers: list[str]):
    # ... expensive operation
    pass
```

---

## 8. Error Handling

### 8.1 API Error Responses
```python
from fastapi import HTTPException
from pydantic import BaseModel

class ErrorResponse(BaseModel):
    error: str
    code: str
    details: dict | None = None

# Standard error codes
ERROR_CODES = {
    'TICKER_NOT_FOUND': 'The requested ticker does not exist',
    'INVALID_PERIOD': 'Invalid time period specified',
    'RATE_LIMITED': 'Too many requests, please try again later',
    'DATA_UNAVAILABLE': 'Market data temporarily unavailable',
    'SCAN_FAILED': 'Screener scan failed to complete',
}

def raise_api_error(code: str, details: dict = None):
    raise HTTPException(
        status_code=400,
        detail={
            'error': ERROR_CODES.get(code, 'Unknown error'),
            'code': code,
            'details': details
        }
    )
```

### 8.2 Frontend Error Handling
```typescript
interface ApiError {
  error: string;
  code: string;
  details?: Record<string, any>;
}

async function apiCall<T>(request: () => Promise<T>): Promise<T> {
  try {
    return await request();
  } catch (error) {
    if (axios.isAxiosError(error) && error.response) {
      const apiError = error.response.data as ApiError;
      // Display user-friendly error
      toast.error(apiError.error);
      throw apiError;
    }
    toast.error('Network error. Please try again.');
    throw error;
  }
}
```

---

## 9. Testing Strategy

### 9.1 Backend Tests
```python
# tests/test_screeners.py
import pytest
from screeners import scan_gap_strategies, scan_rsi_signals

@pytest.fixture
def sample_df():
    # Create sample OHLCV data
    pass

def test_gap_up_detection(sample_df):
    result = scan_gap_strategies('TEST', sample_df)
    assert result is not None
    assert result['gap_type'].startswith('At Support') or result['gap_type'].startswith('At Resistance')

def test_rsi_oversold(sample_df):
    result = scan_rsi_signals('TEST', sample_df, period=14, oversold=30)
    if result:
        assert result['rsi'] <= 30
        assert result['signal'] == 'Oversold'
```

### 9.2 Frontend Tests
```typescript
// src/__tests__/Dashboard.test.tsx
import { render, screen, waitFor } from '@testing-library/react';
import { Dashboard } from '../pages/Dashboard';

jest.mock('../services/api');

test('displays scan results', async () => {
  render(<Dashboard />);
  
  await waitFor(() => {
    expect(screen.getByText('Gap Strategies')).toBeInTheDocument();
  });
});
```

---

## 10. Deployment Configuration

### 10.1 Docker Compose (Planned)
```yaml
version: '3.8'
services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - DB_HOST=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis
  
  frontend:
    build: ./frontend
    ports:
      - "5173:5173"
    depends_on:
      - backend
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: stocks_db
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:7
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

---

## 11. Implementation Priority

| Priority | Feature | Effort | Dependencies |
|----------|---------|--------|--------------|
| P0 | Bug fixes & stability | Low | None |
| P1 | Watchlists | Medium | DB migration |
| P1 | VWAP screener | Low | None |
| P2 | Alert rules engine | High | DB migration |
| P2 | Telegram notifications | Medium | Alert engine |
| P3 | Real-time data (Polygon) | High | API subscription |
| P3 | Chart drawing tools | Medium | None |
| P4 | Backtesting | Very High | Historical data |
| P4 | Pattern recognition | High | ML models |

---

## 12. Revision History

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2026-03-21 | 1.0 | Initial LLD document | System |
