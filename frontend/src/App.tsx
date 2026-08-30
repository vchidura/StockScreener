import { useState, useEffect, useRef } from 'react'
import { Routes, Route, NavLink, Navigate, useNavigate } from 'react-router-dom'
import { FlaskConical, X } from 'lucide-react'
import Dashboard from './pages/Dashboard'
import GapScreener from './pages/GapScreener'
import MAScreener from './pages/MAScreener'
import MomentumPullback from './pages/MomentumPullback'
import BearishBounce from './pages/BearishBounce'
import FibonacciScreener from './pages/FibonacciScreener'
import TickerDetail from './pages/TickerDetail'
import TickersOverview from './pages/TickersOverview'
import SectorIntelligence from './pages/SectorIntelligence'
import ScannerResults from './pages/ScannerResults'
import PatternWatch from './pages/PatternWatch'
import OptionsResearchWorkspace from './pages/OptionsResearchWorkspace'
import { getTickers } from './services/api'

function TickerSearch() {
  const [query, setQuery] = useState('')
  const [tickers, setTickers] = useState<string[]>([])
  const [filtered, setFiltered] = useState<string[]>([])
  const [open, setOpen] = useState(false)
  const [highlightIdx, setHighlightIdx] = useState(-1)
  const navigate = useNavigate()
  const wrapperRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    getTickers().then(t => setTickers(t)).catch(() => {})
  }, [])

  useEffect(() => {
    if (!query.trim()) { setFiltered([]); setOpen(false); return }
    const q = query.toUpperCase()
    const exact: string[] = []
    const startsWith: string[] = []
    const contains: string[] = []
    for (const t of tickers) {
      if (t === q) exact.push(t)
      else if (t.startsWith(q)) startsWith.push(t)
      else if (t.includes(q)) contains.push(t)
    }
    setFiltered([...exact, ...startsWith, ...contains].slice(0, 12))
    setOpen(true)
    setHighlightIdx(-1)
  }, [query, tickers])

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const goToTicker = (t: string) => {
    navigate(`/ticker/${t}`)
    setQuery('')
    setOpen(false)
  }

  const clearSearch = () => {
    setQuery('')
    setOpen(false)
    setHighlightIdx(-1)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setHighlightIdx(i => Math.min(i + 1, filtered.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setHighlightIdx(i => Math.max(i - 1, 0)) }
    else if (e.key === 'Enter') {
      e.preventDefault()
      const exact = query.trim().toUpperCase()
      if (highlightIdx >= 0 && filtered[highlightIdx]) goToTicker(filtered[highlightIdx])
      else if (tickers.includes(exact)) goToTicker(exact)
      else if (filtered[0]) goToTicker(filtered[0])
      else if (exact) goToTicker(exact)
    }
    else if (e.key === 'Escape') setOpen(false)
  }

  return (
    <div ref={wrapperRef} style={{ position: 'relative' }}>
      <input
        type="text"
        aria-label="Search ticker"
        value={query}
        onChange={e => setQuery(e.target.value)}
        onFocus={() => query.trim() && filtered.length > 0 && setOpen(true)}
        onKeyDown={handleKeyDown}
        placeholder="Search ticker..."
        style={{
          padding: query ? '0.5rem 2.5rem 0.5rem 1rem' : '0.5rem 1rem',
          fontSize: '1.05rem',
          border: '1px solid #d1d5db',
          borderRadius: '0.375rem',
          width: '240px',
          outline: 'none',
        }}
      />
      {query && (
        <button
          type="button"
          aria-label="Clear ticker search"
          title="Clear search"
          onClick={clearSearch}
          style={{ position: 'absolute', top: '50%', right: 4, transform: 'translateY(-50%)', width: 30, height: 30, display: 'grid', placeItems: 'center', border: 0, background: 'transparent', color: '#64748b', cursor: 'pointer', padding: 0 }}
        >
          <X size={15} aria-hidden="true" />
        </button>
      )}
      {open && filtered.length > 0 && (
        <ul style={{
          position: 'absolute', top: '100%', left: 0, right: 0,
          background: '#fff', border: '1px solid #d1d5db', borderRadius: '0.375rem',
          marginTop: '0.25rem', padding: 0, listStyle: 'none',
          maxHeight: '280px', overflowY: 'auto', zIndex: 200,
          boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
        }}>
          {filtered.map((t, i) => (
            <li
              key={t}
              onClick={() => goToTicker(t)}
              style={{
                padding: '0.4rem 0.75rem', cursor: 'pointer', fontSize: '0.85rem', fontWeight: 600,
                background: i === highlightIdx ? '#eff6ff' : 'transparent',
                color: i === highlightIdx ? '#2563eb' : '#1e293b',
              }}
              onMouseEnter={() => setHighlightIdx(i)}
            >
              {t}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function App() {
  return (
    <div className="app">
      <nav className="navbar">
        <div style={{ display: 'flex', alignItems: 'center', gap: '3rem' }}>
          <NavLink to="/" className="navbar-brand">
            📊 Stock Screener
          </NavLink>
          <TickerSearch />
        </div>
        <ul className="navbar-nav">
          <li>
            <NavLink to="/gaps" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Gap Strategies
            </NavLink>
          </li>
          <li>
            <NavLink to="/ma-crossover" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              MA Crossover
            </NavLink>
          </li>
          <li>
            <NavLink to="/momentum-pullback" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Momentum Pullback
            </NavLink>
          </li>
          <li>
            <NavLink to="/bearish-bounce" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Bearish Bounce
            </NavLink>
          </li>
          <li>
            <NavLink to="/fibonacci" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Fibonacci
            </NavLink>
          </li>
          <li>
            <NavLink to="/overview" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              All Tickers
            </NavLink>
          </li>
          <li>
            <NavLink to="/sector-intelligence" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Sector Intelligence
            </NavLink>
          </li>
          <li>
            <NavLink to="/pattern-watch" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Pattern Watch
            </NavLink>
          </li>
          <li>
            <NavLink to="/scanner-results" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Scanner Results
            </NavLink>
          </li>
          <li>
            <NavLink to="/options" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              <FlaskConical size={15} aria-hidden="true" /> Options Research
            </NavLink>
          </li>
        </ul>
      </nav>
      
      <main className="main-content">
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
          <Route path="/scanner-results" element={<ScannerResults />} />
          <Route path="/options/*" element={<OptionsResearchWorkspace />} />
          <Route path="/scanner-evaluation" element={<Navigate to="/scanner-results" replace />} />
          <Route path="/backtest" element={<Navigate to="/scanner-results" replace />} />
          <Route path="/ticker/:symbol" element={<TickerDetail />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
