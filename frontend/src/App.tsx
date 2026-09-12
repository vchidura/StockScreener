import { lazy, Suspense } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import AppShell from './layout/AppShell'
import { SessionDateProvider } from './layout/sessionDate'

const Dashboard = lazy(() => import('./pages/Dashboard'))
const GapScreener = lazy(() => import('./pages/GapScreener'))
const MAScreener = lazy(() => import('./pages/MAScreener'))
const MomentumPullback = lazy(() => import('./pages/MomentumPullback'))
const BearishBounce = lazy(() => import('./pages/BearishBounce'))
const FibonacciScreener = lazy(() => import('./pages/FibonacciScreener'))
const TickersOverview = lazy(() => import('./pages/TickersOverview'))
const SectorIntelligence = lazy(() => import('./pages/SectorIntelligence'))
const ScannerResults = lazy(() => import('./pages/ScannerResults'))
const PatternWatch = lazy(() => import('./pages/PatternWatch'))
const TickerWorkspace = lazy(() => import('./pages/ticker/TickerWorkspace'))
const OptionsActivityPage = lazy(() => import('./pages/OptionsActivityPage'))
const OptionsFlowPage = lazy(() => import('./pages/OptionsFlowPage'))
const OptionsScreenerPage = lazy(() => import('./pages/OptionsScreenerPage'))
const OptionsResearchWorkspace = lazy(() => import('./pages/OptionsResearchWorkspace'))
const Account = lazy(() => import('./pages/auth/Account'))
const SignIn = lazy(() => import('./pages/auth/SignIn'))
const SignUp = lazy(() => import('./pages/auth/SignUp'))

function RouteFallback() {
  return (
    <div className="loading" role="status">
      <div className="spinner" aria-hidden="true" />
      Loading workspace…
    </div>
  )
}

function PortalRoutes() {
  return (
    <Suspense fallback={<RouteFallback />}>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/gaps" element={<GapScreener />} />
        <Route path="/ma-crossover" element={<MAScreener />} />
        <Route path="/momentum-pullback" element={<MomentumPullback />} />
        <Route path="/bearish-bounce" element={<BearishBounce />} />
        <Route path="/fibonacci" element={<FibonacciScreener />} />
        <Route path="/overview" element={<TickersOverview />} />
        <Route path="/sector-intelligence" element={<SectorIntelligence />} />
        <Route path="/pattern-watch" element={<PatternWatch />} />
        <Route path="/stock-research/*" element={<ScannerResults />} />
        <Route path="/options/activity" element={<OptionsActivityPage />} />
        <Route path="/options/flow" element={<OptionsFlowPage />} />
        <Route path="/options/screener" element={<OptionsScreenerPage />} />
        <Route path="/options/*" element={<OptionsResearchWorkspace />} />
        <Route path="/account" element={<Account />} />
        <Route path="/scanner-results" element={<Navigate to="/stock-research" replace />} />
        <Route path="/scanner-evaluation" element={<Navigate to="/stock-research/research" replace />} />
        <Route path="/backtest" element={<Navigate to="/stock-research/research" replace />} />
        <Route path="/ticker/:symbol" element={<TickerWorkspace />} />
        <Route path="/ticker/:symbol/:view" element={<TickerWorkspace />} />
      </Routes>
    </Suspense>
  )
}

function App() {
  return (
    <Suspense fallback={<RouteFallback />}>
      <Routes>
        <Route path="/signin" element={<SignIn />} />
        <Route path="/signup" element={<SignUp />} />
        <Route
          path="*"
          element={
            <SessionDateProvider>
              <AppShell>
                <PortalRoutes />
              </AppShell>
            </SessionDateProvider>
          }
        />
      </Routes>
    </Suspense>
  )
}

export default App
