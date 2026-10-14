import { lazy, Suspense } from 'react'
import { Routes, Route, Navigate, useLocation } from 'react-router-dom'
import AppShell from './layout/AppShell'
import { PortalViewBoundary } from './layout/PortalViewBoundary'
import { SessionDateProvider } from './layout/sessionDate'

const Dashboard = lazy(() => import('./pages/Dashboard'))
const GapScreener = lazy(() => import('./pages/GapScreener'))
const MAScreener = lazy(() => import('./pages/MAScreener'))
const MomentumPullback = lazy(() => import('./pages/MomentumPullback'))
const BearishBounce = lazy(() => import('./pages/BearishBounce'))
const FibonacciScreener = lazy(() => import('./pages/FibonacciScreener'))
const DailyStrategyTicker = lazy(() => import('./pages/DailyStrategyTicker'))
const StrategyOverviewPage = lazy(() => import('./pages/StrategyOverviewPage'))
const TickersOverview = lazy(() => import('./pages/TickersOverview'))
const SectorIntelligence = lazy(() => import('./pages/SectorIntelligence'))
const MarketConditionsPage = lazy(() => import('./pages/MarketConditionsPage'))
const PersistencePage = lazy(() => import('./pages/PersistencePage'))
const PatternWatch = lazy(() => import('./pages/PatternWatch'))
const TickerWorkspace = lazy(() => import('./pages/ticker/TickerWorkspace'))
const OptionsActivityPage = lazy(() => import('./pages/OptionsActivityPage'))
const OptionsFlowPage = lazy(() => import('./pages/OptionsFlowPage'))
const OptionsScreenerPage = lazy(() => import('./pages/OptionsScreeningWorkspace'))
const StockScreenerPage = lazy(() => import('./pages/StockScreeningPage'))
const StockAlertsPage = lazy(() => import('./pages/StockAlertsPage'))
const OptionsAlertsPage = lazy(() => import('./pages/OptionsAlertsPage'))
const RealtimeCanaryPage = lazy(() => import('./pages/RealtimeCanaryPage'))
const Account = lazy(() => import('./pages/auth/Account'))
const SignIn = lazy(() => import('./pages/auth/SignIn'))
const SignUp = lazy(() => import('./pages/auth/SignUp'))

function RouteFallback() {
  const tickerRoute = useLocation().pathname.startsWith('/ticker/')
  return (
    <div className="loading" role="status" aria-busy="true">
      <div className="spinner" aria-hidden="true" />
      {tickerRoute ? 'Loading chart data...' : 'Loading workspace…'}
    </div>
  )
}

function PersistenceRedirect() {
  const location = useLocation()
  return <Navigate to={`/stocks/persistence${location.search}`} replace />
}

function PortalRoutes({ realtimeOnly, realtimeHome }: { realtimeOnly: boolean; realtimeHome: string }) {
  if (realtimeOnly) return (
    <Suspense fallback={<RouteFallback />}>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/overview" element={<TickersOverview />} />
        <Route path="/sector-intelligence" element={<SectorIntelligence />} />
        <Route path="/market-conditions" element={<MarketConditionsPage />} />
        <Route path="/pattern-watch" element={<PatternWatch />} />
        <Route path="/gaps" element={<GapScreener />} />
        <Route path="/ma-crossover" element={<MAScreener />} />
        <Route path="/momentum-pullback" element={<MomentumPullback />} />
        <Route path="/bearish-bounce" element={<BearishBounce />} />
        <Route path="/fibonacci" element={<FibonacciScreener />} />
        <Route path="/strategies/overview" element={<StrategyOverviewPage />} />
        <Route path="/strategy-research/:symbol" element={<DailyStrategyTicker />} />
        <Route path="/ticker/:symbol" element={<TickerWorkspace />} />
        <Route path="/ticker/:symbol/:view" element={<TickerWorkspace />} />
        <Route path="/stocks/screener" element={<StockScreenerPage />} />
        <Route path="/stocks/alerts" element={<StockAlertsPage />} />
        <Route path="/stocks/persistence" element={<PersistencePage />} />
        <Route path="/stock-research/*" element={<PersistenceRedirect />} />
        <Route path="/options/activity" element={<OptionsActivityPage />} />
        <Route path="/options/flow" element={<OptionsFlowPage />} />
        <Route path="/options/screener" element={<OptionsScreenerPage />} />
        <Route path="/options/alerts" element={<OptionsAlertsPage />} />
        <Route path="/options/*" element={<Navigate to="/options/screener" replace />} />
        <Route path="/account" element={<Account />} />
        <Route path="/scanner-results" element={<PersistenceRedirect />} />
        <Route path="/scanner-evaluation" element={<PersistenceRedirect />} />
        <Route path="/backtest" element={<PersistenceRedirect />} />
        <Route path="/realtime-canary" element={<RealtimeCanaryPage />} />
        <Route path="*" element={<Navigate to={realtimeHome} replace />} />
      </Routes>
    </Suspense>
  )

  return (
    <Suspense fallback={<RouteFallback />}>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/gaps" element={<GapScreener />} />
        <Route path="/ma-crossover" element={<MAScreener />} />
        <Route path="/momentum-pullback" element={<MomentumPullback />} />
        <Route path="/bearish-bounce" element={<BearishBounce />} />
        <Route path="/fibonacci" element={<FibonacciScreener />} />
        <Route path="/strategies/overview" element={<StrategyOverviewPage />} />
        <Route path="/strategy-research/:symbol" element={<DailyStrategyTicker />} />
        <Route path="/overview" element={<TickersOverview />} />
        <Route path="/sector-intelligence" element={<SectorIntelligence />} />
        <Route path="/market-conditions" element={<MarketConditionsPage />} />
        <Route path="/pattern-watch" element={<PatternWatch />} />
        <Route path="/stocks/persistence" element={<PersistencePage />} />
        <Route path="/stock-research/*" element={<PersistenceRedirect />} />
        <Route path="/options/activity" element={<OptionsActivityPage />} />
        <Route path="/options/flow" element={<OptionsFlowPage />} />
        <Route path="/options/screener" element={<OptionsScreenerPage />} />
        <Route path="/options/alerts" element={<OptionsAlertsPage />} />
        <Route path="/stocks/screener" element={<StockScreenerPage />} />
        <Route path="/stocks/alerts" element={<StockAlertsPage />} />
        <Route path="/realtime-canary" element={<RealtimeCanaryPage />} />
        <Route path="/options/*" element={<Navigate to="/options/screener" replace />} />
        <Route path="/account" element={<Account />} />
        <Route path="/scanner-results" element={<PersistenceRedirect />} />
        <Route path="/scanner-evaluation" element={<PersistenceRedirect />} />
        <Route path="/backtest" element={<PersistenceRedirect />} />
        <Route path="/ticker/:symbol" element={<TickerWorkspace />} />
        <Route path="/ticker/:symbol/:view" element={<TickerWorkspace />} />
      </Routes>
    </Suspense>
  )
}

function App() {
  const location = useLocation()
  const realtimeDeployment = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const realtimeCohortId = import.meta.env.VITE_REALTIME_COHORT_ID?.trim()
  if (realtimeDeployment && !realtimeCohortId) {
    throw new Error('Realtime-only portal requires VITE_REALTIME_COHORT_ID')
  }
  const realtimeCanary = `/realtime-canary?scope=equity${realtimeCohortId ? `&cohort_id=${encodeURIComponent(realtimeCohortId)}` : ''}`
  const realtimeHome = realtimeDeployment ? '/' : realtimeCanary
  const realtimeOnly = realtimeDeployment || location.pathname === '/realtime-canary'

  if (realtimeDeployment) return (
    <SessionDateProvider>
      <AppShell realtimeOnly realtimeHome={realtimeHome} realtimeCanary={realtimeCanary}>
        <PortalViewBoundary>
          <PortalRoutes realtimeOnly realtimeHome={realtimeHome} />
        </PortalViewBoundary>
      </AppShell>
    </SessionDateProvider>
  )

  return (
    <Suspense fallback={<RouteFallback />}>
      <Routes>
        <Route path="/signin" element={<SignIn />} />
        <Route path="/signup" element={<SignUp />} />
        <Route
          path="*"
          element={
            <SessionDateProvider>
              <AppShell realtimeOnly={realtimeOnly} realtimeHome={realtimeHome} realtimeCanary={realtimeCanary}>
                <PortalViewBoundary>
                  <PortalRoutes realtimeOnly={false} realtimeHome={realtimeHome} />
                </PortalViewBoundary>
              </AppShell>
            </SessionDateProvider>
          }
        />
      </Routes>
    </Suspense>
  )
}

export default App
