import { Routes, Route, Navigate } from 'react-router-dom'
import AppShell from './layout/AppShell'
import { SessionDateProvider } from './layout/sessionDate'
import Dashboard from './pages/Dashboard'
import GapScreener from './pages/GapScreener'
import MAScreener from './pages/MAScreener'
import MomentumPullback from './pages/MomentumPullback'
import BearishBounce from './pages/BearishBounce'
import FibonacciScreener from './pages/FibonacciScreener'
import TickersOverview from './pages/TickersOverview'
import SectorIntelligence from './pages/SectorIntelligence'
import ScannerResults from './pages/ScannerResults'
import PatternWatch from './pages/PatternWatch'
import TickerWorkspace from './pages/ticker/TickerWorkspace'
import OptionsActivityPage from './pages/OptionsActivityPage'
import OptionsFlowPage from './pages/OptionsFlowPage'
import OptionsResearchWorkspace from './pages/OptionsResearchWorkspace'
import Account from './pages/auth/Account'
import SignIn from './pages/auth/SignIn'
import SignUp from './pages/auth/SignUp'

function PortalRoutes() {
  return (
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
      <Route path="/options/*" element={<OptionsResearchWorkspace />} />
      <Route path="/account" element={<Account />} />
      <Route path="/scanner-results" element={<Navigate to="/stock-research" replace />} />
      <Route path="/scanner-evaluation" element={<Navigate to="/stock-research/research" replace />} />
      <Route path="/backtest" element={<Navigate to="/stock-research/research" replace />} />
      <Route path="/ticker/:symbol" element={<TickerWorkspace />} />
      <Route path="/ticker/:symbol/:view" element={<TickerWorkspace />} />
    </Routes>
  )
}

function App() {
  return (
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
  )
}

export default App
