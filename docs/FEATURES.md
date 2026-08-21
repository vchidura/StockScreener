# Feature Tracker

Track all features, their status, and implementation notes.

## Legend
- ✅ Complete
- 🔨 In Progress  
- 🔲 Planned
- ❌ Blocked

---

## Core Screeners

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Gap Strategies Scanner | ✅ | Support/resistance gap detection | 2026-03-21 |
| MA Crossover Scanner | ✅ | Configurable short/long periods | 2026-03-21 |
| RSI Signal Scanner | ✅ | Oversold/overbought detection | 2026-03-21 |
| Volume Breakout Scanner | ✅ | Unusual volume detection | 2026-03-21 |
| VWAP Scanner | 🔲 | Price deviation from VWAP | - |
| Fibonacci Levels | 🔲 | Auto-detect key levels | - |
| Pattern Recognition | 🔲 | Double top/bottom, H&S | - |

---

## Charts & Visualization

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Candlestick Charts | ✅ | Lightweight Charts integration | 2026-03-21 |
| Volume Histogram | ✅ | Below price chart | 2026-03-21 |
| Multiple Timeframes | ✅ | 1m to weekly | 2026-03-21 |
| Trend Lines (Drawing) | 🔲 | User-drawn annotations | - |
| Horizontal Lines | 🔲 | Support/resistance marks | - |
| Fibonacci Drawing Tool | 🔲 | Manual retracement | - |
| MA Overlay | 🔲 | Show MAs on chart | - |
| RSI Indicator Panel | 🔲 | Separate RSI subplot | - |

---

## Alerts & Notifications

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Telegram Integration | 🔲 | Bot notifications | - |
| Email Alerts | 🔲 | SendGrid integration | - |
| Custom Alert Rules | 🔲 | Rule builder UI | - |
| Alert History | 🔲 | View past alerts | - |
| In-App Notifications | 🔲 | Toast/bell icon | - |

---

## Watchlists

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Create Watchlist | 🔲 | - | - |
| Add/Remove Tickers | 🔲 | - | - |
| Ticker Notes | 🔲 | Personal annotations | - |
| Watchlist Scan | 🔲 | Scan specific list | - |
| Default Watchlist | 🔲 | Load on startup | - |

---

## Real-Time Data

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Polygon.io Integration | 🔲 | WebSocket feed | - |
| Live Price Updates | 🔲 | Real-time quotes | - |
| Streaming Charts | 🔲 | Auto-update candles | - |
| Pre/Post Market Data | 🔲 | Extended hours | - |

---

## Backend Improvements

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Redis Caching | 🔲 | Scan result caching | - |
| Scheduled Scans | 🔲 | Cron job integration | - |
| Rate Limiting | 🔲 | API throttling | - |
| Batch Scanning | 🔲 | Parallel processing | - |
| Database Optimization | 🔲 | Query performance | - |

---

## UI/UX Enhancements

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Dark Theme | 🔲 | Toggle support | - |
| Mobile Responsive | 🔲 | Tablet/phone layouts | - |
| Keyboard Shortcuts | 🔲 | Power user features | - |
| Export to CSV | 🔲 | Download results | - |
| PDF Reports | 🔲 | Print-friendly | - |

---

## Infrastructure

| Feature | Status | Notes | Date |
|---------|--------|-------|------|
| Docker Setup | 🔲 | Containerization | - |
| Azure Deployment | 🔲 | Cloud hosting | - |
| CI/CD Pipeline | 🔲 | GitHub Actions | - |
| Monitoring | 🔲 | App Insights | - |
| Backup System | 🔲 | PostgreSQL backups | - |

---

## Implementation Notes

### Next Up (Priority)
1. **VWAP Scanner** - Simple addition, reuses existing patterns
2. **Watchlists** - Requires DB migration, moderate effort
3. **MA Overlay on Charts** - Frontend only, good UX improvement

### Known Issues
- Volume breakout scan limited to 50 tickers for performance
- Chart drawing tools not yet available
- No caching - each scan hits DB fresh

### Dependencies
- Real-time features require Polygon.io subscription ($29/mo)
- Email alerts require SendGrid account

---

## Changelog

### v1.0.0 (2026-03-21)
- Initial release
- 4 screeners: Gap, MA, RSI, Volume
- Interactive charts with candlestick & volume
- Dashboard with overview stats
- Ticker detail view
