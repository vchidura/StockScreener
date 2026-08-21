import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { scanVolumeBreakout, VolumeScanResponse } from '../services/api'

function VolumeScreener() {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<VolumeScanResponse | null>(null)
  const [filter, setFilter] = useState('')
  const [volumeMultiplier, setVolumeMultiplier] = useState(2.0)

  const runScan = async () => {
    setLoading(true)
    try {
      const result = await scanVolumeBreakout(undefined, volumeMultiplier)
      setData(result)
    } catch (err) {
      console.error('Volume scan failed:', err)
    }
    setLoading(false)
  }

  useEffect(() => {
    runScan()
  }, [])

  const getSignalColor = (signal: string): string => {
    if (signal.includes('Bullish')) return 'badge-success'
    if (signal.includes('Bearish')) return 'badge-danger'
    return 'badge-info'
  }

  const formatVolume = (volume: number): string => {
    if (volume >= 1000000) return (volume / 1000000).toFixed(1) + 'M'
    if (volume >= 1000) return (volume / 1000).toFixed(0) + 'K'
    return volume.toString()
  }

  const filteredResults = data?.results.filter(r => 
    filter === '' || r.ticker.toLowerCase().includes(filter.toLowerCase())
  ) ?? []

  const bullishResults = filteredResults.filter(r => r.signal.includes('Bullish'))
  const bearishResults = filteredResults.filter(r => r.signal.includes('Bearish'))

  return (
    <div>
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div>
          <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>Volume Breakout</h1>
          <p style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
            Spot unusual volume spikes indicating potential price moves
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
            <label>Volume Multiplier:</label>
            <input
              type="number"
              value={volumeMultiplier}
              onChange={(e) => setVolumeMultiplier(Number(e.target.value))}
              min={1.5}
              max={10}
              step={0.5}
              style={{ width: '80px' }}
            />
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>
              (x average volume)
            </span>
          </div>
          <button className="btn btn-secondary" onClick={runScan}>
            Apply
          </button>
        </div>
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning for volume breakouts...</span>
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
              <div className="stat-value">{data.total_signals}</div>
              <div className="stat-label">Volume Breakouts</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: '#16a34a' }}>
                {data.results.filter(r => r.signal.includes('Bullish')).length}
              </div>
              <div className="stat-label">Bullish Breakouts</div>
            </div>
            <div className="stat-card">
              <div className="stat-value" style={{ color: '#dc2626' }}>
                {data.results.filter(r => r.signal.includes('Bearish')).length}
              </div>
              <div className="stat-label">Bearish Breakouts</div>
            </div>
          </div>

          {/* Filter */}
          <div className="filter-bar">
            <input
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '200px' }}
            />
          </div>

          {/* Categorized Results */}
          {filteredResults.length === 0 ? (
            <div className="card" style={{ textAlign: 'center', padding: '2rem' }}>
              No volume breakouts found
            </div>
          ) : (
            <>
              <div className="results-section">
                <h3>
                  Bullish Volume Breakouts
                  <span className="count">{bullishResults.length}</span>
                </h3>
                <div className="card" style={{ padding: 0 }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Ticker</th>
                        <th>Signal</th>
                        <th>Volume</th>
                        <th>Avg Volume</th>
                        <th>Vol Ratio</th>
                        <th>Price Change</th>
                        <th>Last Close</th>
                      </tr>
                    </thead>
                    <tbody>
                      {bullishResults.length === 0 ? (
                        <tr>
                          <td colSpan={7} style={{ textAlign: 'center', padding: '1.5rem' }}>
                            No bullish volume breakouts
                          </td>
                        </tr>
                      ) : (
                        bullishResults.map((result, idx) => (
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
                            <td style={{ fontWeight: 600 }}>{formatVolume(result.volume)}</td>
                            <td>{formatVolume(result.avg_volume)}</td>
                            <td>
                              <span style={{ 
                                color: result.volume_ratio >= 3 ? '#dc2626' : result.volume_ratio >= 2 ? '#ca8a04' : 'inherit',
                                fontWeight: 600
                              }}>
                                {result.volume_ratio.toFixed(1)}x
                              </span>
                            </td>
                            <td style={{ 
                              color: result.price_change_pct >= 0 ? '#16a34a' : '#dc2626',
                              fontWeight: 500
                            }}>
                              {result.price_change_pct >= 0 ? '+' : ''}{result.price_change_pct.toFixed(2)}%
                            </td>
                            <td>${result.last_close.toFixed(2)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="results-section">
                <h3>
                  Bearish Volume Breakouts
                  <span className="count">{bearishResults.length}</span>
                </h3>
                <div className="card" style={{ padding: 0 }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Ticker</th>
                        <th>Signal</th>
                        <th>Volume</th>
                        <th>Avg Volume</th>
                        <th>Vol Ratio</th>
                        <th>Price Change</th>
                        <th>Last Close</th>
                      </tr>
                    </thead>
                    <tbody>
                      {bearishResults.length === 0 ? (
                        <tr>
                          <td colSpan={7} style={{ textAlign: 'center', padding: '1.5rem' }}>
                            No bearish volume breakouts
                          </td>
                        </tr>
                      ) : (
                        bearishResults.map((result, idx) => (
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
                            <td style={{ fontWeight: 600 }}>{formatVolume(result.volume)}</td>
                            <td>{formatVolume(result.avg_volume)}</td>
                            <td>
                              <span style={{ 
                                color: result.volume_ratio >= 3 ? '#dc2626' : result.volume_ratio >= 2 ? '#ca8a04' : 'inherit',
                                fontWeight: 600
                              }}>
                                {result.volume_ratio.toFixed(1)}x
                              </span>
                            </td>
                            <td style={{ 
                              color: result.price_change_pct >= 0 ? '#16a34a' : '#dc2626',
                              fontWeight: 500
                            }}>
                              {result.price_change_pct >= 0 ? '+' : ''}{result.price_change_pct.toFixed(2)}%
                            </td>
                            <td>${result.last_close.toFixed(2)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </>
      )}
    </div>
  )
}

export default VolumeScreener
