import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { scanRSI, RSIScanResponse } from '../services/api'

function RSIScreener() {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<RSIScanResponse | null>(null)
  const [filter, setFilter] = useState('')
  const [period, setPeriod] = useState(14)
  const [oversold, setOversold] = useState(30)
  const [overbought, setOverbought] = useState(70)
  const [selectedSignal, setSelectedSignal] = useState<string>('all')

  const runScan = async () => {
    setLoading(true)
    try {
      const result = await scanRSI(undefined, period, oversold, overbought)
      setData(result)
    } catch (err) {
      console.error('RSI scan failed:', err)
    }
    setLoading(false)
  }

  useEffect(() => {
    runScan()
  }, [])

  const getSignalColor = (signal: string): string => {
    if (signal === 'Oversold') return 'badge-success'
    if (signal === 'Overbought') return 'badge-danger'
    return 'badge-warning'
  }

  const filteredResults = data?.results.filter(r => {
    const matchesFilter = filter === '' || r.ticker.toLowerCase().includes(filter.toLowerCase())
    const matchesSignal = selectedSignal === 'all' || r.signal === selectedSignal
    return matchesFilter && matchesSignal
  }) ?? []

  return (
    <div>
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div>
          <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>RSI Signals</h1>
          <p style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
            Find oversold and overbought conditions using RSI indicator
          </p>
        </div>
        <button className="btn btn-primary" onClick={runScan} disabled={loading}>
          {loading ? 'Scanning...' : '🔄 Refresh'}
        </button>
      </div>

      {/* Parameters */}
      <div className="card">
        <h3 style={{ marginBottom: '1rem' }}>Parameters</h3>
        <div className="filter-bar" style={{ marginBottom: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <label>RSI Period:</label>
            <input
              type="number"
              value={period}
              onChange={(e) => setPeriod(Number(e.target.value))}
              min={5}
              max={50}
              style={{ width: '80px' }}
            />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <label>Oversold:</label>
            <input
              type="number"
              value={oversold}
              onChange={(e) => setOversold(Number(e.target.value))}
              min={10}
              max={40}
              style={{ width: '80px' }}
            />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <label>Overbought:</label>
            <input
              type="number"
              value={overbought}
              onChange={(e) => setOverbought(Number(e.target.value))}
              min={60}
              max={90}
              style={{ width: '80px' }}
            />
          </div>
          <button className="btn btn-secondary" onClick={runScan}>
            Apply
          </button>
        </div>
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning for RSI signals...</span>
        </div>
      )}

      {!loading && data && (
        <>
          {/* Summary Cards */}
          <div className="dashboard-grid">
            <div className="stat-card">
              <div className="stat-value">{data.total_scanned}</div>
              <div className="stat-label">Tickers Scanned</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: '#16a34a' }}>
                {data.results_by_signal['Oversold']?.length ?? 0}
              </div>
              <div className="stat-label">Oversold (Buy Signal)</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: '#dc2626' }}>
                {data.results_by_signal['Overbought']?.length ?? 0}
              </div>
              <div className="stat-label">Overbought (Sell Signal)</div>
            </div>
          </div>

          {/* Filters */}
          <div className="filter-bar">
            <input
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '200px' }}
            />
            <select value={selectedSignal} onChange={(e) => setSelectedSignal(e.target.value)}>
              <option value="all">All Signals</option>
              <option value="Oversold">Oversold Only</option>
              <option value="Overbought">Overbought Only</option>
            </select>
          </div>

          {/* Results Table */}
          <div className="card" style={{ padding: 0 }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Signal</th>
                  <th>RSI ({period})</th>
                  <th>Last Close</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                {filteredResults.length === 0 ? (
                  <tr>
                    <td colSpan={5} style={{ textAlign: 'center', padding: '2rem' }}>
                      No RSI signals found
                    </td>
                  </tr>
                ) : (
                  filteredResults.map((result, idx) => (
                    <tr key={idx}>
                      <td>
                        <span className="ticker" onClick={() => navigate(`/ticker/${result.ticker}`)}>
                          {result.ticker}
                        </span>
                      </td>
                      <td>
                        <span className={`badge ${getSignalColor(result.signal)}`}>
                          {result.signal}
                        </span>
                      </td>
                      <td style={{ 
                        fontWeight: 600,
                        color: result.rsi <= oversold ? '#16a34a' : result.rsi >= overbought ? '#dc2626' : 'inherit'
                      }}>
                        {result.rsi.toFixed(1)}
                      </td>
                      <td>${result.last_close.toFixed(2)}</td>
                      <td>{result.date}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

export default RSIScreener
