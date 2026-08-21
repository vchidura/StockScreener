import { useState, useEffect, useRef } from 'react'
import { Routes, Route, NavLink, Navigate, useNavigate } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import GapScreener from './pages/GapScreener'
import MAScreener from './pages/MAScreener'
import MomentumPullback from './pages/MomentumPullback'
import BearishBounce from './pages/BearishBounce'
import FibonacciScreener from './pages/FibonacciScreener'
import TickerDetail from './pages/TickerDetail'
import TickersOverview from './pages/TickersOverview'
import ScannerEvaluation from './pages/ScannerEvaluation'
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
    setFiltered(tickers.filter(t => t.includes(q)).slice(0, 12))
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

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setHighlightIdx(i => Math.min(i + 1, filtered.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setHighlightIdx(i => Math.max(i - 1, 0)) }
    else if (e.key === 'Enter') {
      e.preventDefault()
      if (highlightIdx >= 0 && filtered[highlightIdx]) goToTicker(filtered[highlightIdx])
      else if (query.trim()) goToTicker(query.trim().toUpperCase())
    }
    else if (e.key === 'Escape') setOpen(false)
  }

  return (
    <div ref={wrapperRef} style={{ position: 'relative' }}>
      <input
        type="text"
        value={query}
        onChange={e => setQuery(e.target.value)}
        onFocus={() => query.trim() && filtered.length > 0 && setOpen(true)}
        onKeyDown={handleKeyDown}
        placeholder="Search ticker..."
        style={{
          padding: '0.5rem 1rem',
          fontSize: '1.05rem',
          border: '1px solid #d1d5db',
          borderRadius: '0.375rem',
          width: '240px',
          outline: 'none',
        }}
      />
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
            <NavLink to="/" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Dashboard
            </NavLink>
          </li>
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
            <NavLink to="/scanner-evaluation" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
              Scanner Evaluation
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
          <Route path="/scanner-evaluation" element={<ScannerEvaluation />} />
          <Route path="/backtest" element={<Navigate to="/scanner-evaluation" replace />} />
          <Route path="/ticker/:symbol" element={<TickerDetail />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
