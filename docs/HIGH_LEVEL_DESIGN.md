# Stock Screener Portal - High Level Design

## 1. Overview

The Stock Screener Portal is a full-stack web application for scanning stocks using various trading strategies, visualizing results, and providing actionable insights for traders.

### 1.1 Goals
- Provide real-time and historical stock screening capabilities
- Support multiple trading strategies (gaps, MA crossovers, RSI, volume)
- Interactive charting with technical analysis tools
- Alert system for strategy triggers
- Extensible architecture for adding new screeners

### 1.2 Non-Goals (Future Consideration)
- Order execution / trading integration
- Social features / sharing
- Mobile native apps (web-responsive only)

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              FRONTEND                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Dashboard  │  │  Screeners  │  │   Charts    │  │   Alerts    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │
│                              React + TypeScript                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ REST API / WebSocket
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              BACKEND                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  API Layer  │  │  Screeners  │  │  WebSocket  │  │   Alerts    │    │
│  │  (FastAPI)  │  │   Engine    │  │   Handler   │  │   Service   │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │
│                              Python 3.10+                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │  PostgreSQL │ │   Redis     │ │  External   │
            │  (Storage)  │ │  (Cache)    │ │    APIs     │
            └─────────────┘ └─────────────┘ └─────────────┘
                                            │
                                ┌───────────┴───────────┐
                                ▼                       ▼
                        ┌─────────────┐         ┌─────────────┐
                        │  yfinance   │         │  Polygon.io │
                        │  (Free)     │         │  (Premium)  │
                        └─────────────┘         └─────────────┘
```

---

## 3. Component Overview

### 3.1 Frontend Components

| Component | Description | Status |
|-----------|-------------|--------|
| Dashboard | Overview of all strategies, quick stats | ✅ Done |
| Gap Screener | Gap-up/down strategy results | ✅ Done |
| MA Screener | Moving average crossover signals | ✅ Done |
| RSI Screener | Oversold/overbought conditions | ✅ Done |
| Volume Screener | Unusual volume detection | ✅ Done |
| Ticker Detail | Interactive candlestick charts | ✅ Done |
| Watchlist | Custom ticker lists | 🔲 Planned |
| Alerts Manager | Configure & view alerts | 🔲 Planned |
| Backtesting | Historical strategy testing | 🔲 Planned |
| Settings | User preferences | 🔲 Planned |

### 3.2 Backend Services

| Service | Description | Status |
|---------|-------------|--------|
| REST API | FastAPI endpoints for screeners | ✅ Done |
| Screener Engine | Strategy implementations | ✅ Done |
| Database Layer | PostgreSQL CRUD operations | ✅ Done |
| WebSocket Server | Real-time data streaming | 🔲 Planned |
| Alert Service | Telegram/Email notifications | 🔲 Planned |
| Scheduler | Automated scans (cron) | 🔲 Planned |
| Cache Layer | Redis for performance | 🔲 Planned |

---

## 4. Data Flow

### 4.1 Screening Flow
```
User Request → API → Screener Engine → Data Source → Process → Response
                                            │
                                    ┌───────┴───────┐
                                    ▼               ▼
                              PostgreSQL       yfinance
                              (Historical)     (Live)
```

### 4.2 Real-Time Flow (Planned)
```
Polygon WebSocket → Message Queue → Processing → WebSocket → Frontend
                                        │
                                        ▼
                                  Alert Engine → Telegram/Email
```

---

## 5. Feature Roadmap

### Phase 1: Core Screeners ✅ (Current)
- [x] Gap strategies (support/resistance)
- [x] Moving average crossover
- [x] RSI oversold/overbought
- [x] Volume breakout detection
- [x] Interactive charts

### Phase 2: Enhanced Analysis
- [ ] VWAP screener
- [ ] Fibonacci retracement levels on charts
- [ ] Support/resistance line detection
- [ ] Pattern recognition (head & shoulders, double top/bottom)
- [ ] Sector/industry heatmaps

### Phase 3: Real-Time & Alerts
- [ ] Polygon.io WebSocket integration
- [ ] Real-time price updates
- [ ] Custom alert rules engine
- [ ] Telegram bot integration
- [ ] Email notifications (SendGrid)

### Phase 4: Advanced Features
- [ ] Watchlists with notes
- [ ] Backtesting engine
- [ ] Portfolio tracking
- [ ] Options flow scanner
- [ ] News sentiment integration

### Phase 5: Platform Enhancements
- [ ] User authentication
- [ ] Cloud deployment (Azure)
- [ ] Mobile-responsive design
- [ ] Export reports (PDF/CSV)
- [ ] API rate limiting & caching

---

## 6. Technology Stack

### Frontend
| Technology | Purpose | Version |
|------------|---------|---------|
| React | UI Library | 19.x |
| TypeScript | Type Safety | 5.x |
| Vite | Build Tool | 8.x |
| React Router | Routing | 7.x |
| Lightweight Charts | Charting | 5.x |
| Axios | HTTP Client | 1.x |

### Backend
| Technology | Purpose | Version |
|------------|---------|---------|
| Python | Runtime | 3.10+ |
| FastAPI | Web Framework | 0.109+ |
| uvicorn | ASGI Server | 0.27+ |
| psycopg2 | PostgreSQL Driver | 2.9+ |
| yfinance | Market Data | 0.2+ |
| pandas | Data Processing | 2.x |

### Infrastructure
| Technology | Purpose | Status |
|------------|---------|--------|
| PostgreSQL | Primary Database | ✅ Active |
| Redis | Caching | 🔲 Planned |
| Docker | Containerization | 🔲 Planned |
| Azure | Cloud Hosting | 🔲 Planned |

---

## 7. Security Considerations

| Area | Approach | Status |
|------|----------|--------|
| API Authentication | JWT tokens | 🔲 Planned |
| Database | Connection pooling, parameterized queries | ✅ Done |
| CORS | Restricted origins | ✅ Done |
| Rate Limiting | API throttling | 🔲 Planned |
| Secrets | Environment variables | ✅ Done |

---

## 8. Performance Targets

| Metric | Target | Current |
|--------|--------|---------|
| Scan 500 tickers | < 30 seconds | ~45 seconds |
| Chart load time | < 2 seconds | ~1.5 seconds |
| API response (cached) | < 100ms | N/A |
| Concurrent users | 10+ | Local only |

---

## 9. Monitoring & Observability (Planned)

- Application Insights for APM
- Structured logging (JSON)
- Health check endpoints
- Performance metrics dashboard

---

## 10. Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2026-03-21 | 1.0 | Initial design document |
