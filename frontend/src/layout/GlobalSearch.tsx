import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Command, Search, X } from 'lucide-react'
import { getTickers } from '../services/api'
import { NAV_ITEMS } from './navigation'

type SearchResult = {
  kind: 'page' | 'ticker'
  label: string
  hint: string
  to: string
}

function rankTickers(tickers: string[], query: string): string[] {
  const exact: string[] = []
  const startsWith: string[] = []
  const contains: string[] = []
  for (const ticker of tickers) {
    if (ticker === query) exact.push(ticker)
    else if (ticker.startsWith(query)) startsWith.push(ticker)
    else if (ticker.includes(query)) contains.push(ticker)
  }
  return [...exact, ...startsWith, ...contains]
}

export default function GlobalSearch() {
  const [query, setQuery] = useState('')
  const [tickers, setTickers] = useState<string[]>([])
  const [open, setOpen] = useState(false)
  const [highlight, setHighlight] = useState(0)
  const navigate = useNavigate()
  const wrapperRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    getTickers().then(setTickers).catch(() => {})
  }, [])

  useEffect(() => {
    const focusOnCommandK = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === 'k' && (event.ctrlKey || event.metaKey)) {
        event.preventDefault()
        inputRef.current?.focus()
        inputRef.current?.select()
      }
    }
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener('keydown', focusOnCommandK)
    document.addEventListener('mousedown', closeOnOutsideClick)
    return () => {
      document.removeEventListener('keydown', focusOnCommandK)
      document.removeEventListener('mousedown', closeOnOutsideClick)
    }
  }, [])

  const results = useMemo<SearchResult[]>(() => {
    const trimmed = query.trim()
    if (!trimmed) return []
    const upper = trimmed.toUpperCase()
    const lower = trimmed.toLowerCase()
    const pages: SearchResult[] = NAV_ITEMS
      .filter(item => item.label.toLowerCase().includes(lower))
      .map(item => ({ kind: 'page', label: item.label, hint: 'Page', to: item.to }))
    const symbols: SearchResult[] = rankTickers(tickers, upper)
      .slice(0, 10)
      .map(ticker => ({ kind: 'ticker', label: ticker, hint: 'Ticker', to: `/ticker/${ticker}` }))
    const combined = [...pages.slice(0, 4), ...symbols]
    if (!combined.length && /^[A-Z.\-]{1,10}$/.test(upper)) {
      combined.push({ kind: 'ticker', label: upper, hint: 'Open ticker', to: `/ticker/${upper}` })
    }
    return combined.slice(0, 12)
  }, [query, tickers])

  useEffect(() => {
    setHighlight(0)
    setOpen(results.length > 0)
  }, [results])

  const go = (result: SearchResult) => {
    navigate(result.to)
    setQuery('')
    setOpen(false)
    inputRef.current?.blur()
  }

  const onKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      setHighlight(index => Math.min(index + 1, results.length - 1))
    } else if (event.key === 'ArrowUp') {
      event.preventDefault()
      setHighlight(index => Math.max(index - 1, 0))
    } else if (event.key === 'Enter') {
      event.preventDefault()
      const target = results[highlight] || results[0]
      if (target) go(target)
    } else if (event.key === 'Escape') {
      setOpen(false)
      inputRef.current?.blur()
    }
  }

  return (
    <div className="tm-search" ref={wrapperRef}>
      <Search className="tm-search__icon" size={14} aria-hidden="true" />
      <input
        ref={inputRef}
        type="text"
        role="combobox"
        aria-expanded={open}
        aria-controls="tm-search-results"
        aria-label="Search tickers and pages"
        value={query}
        placeholder="Search tickers & pages"
        onChange={event => setQuery(event.target.value)}
        onFocus={() => setOpen(results.length > 0)}
        onKeyDown={onKeyDown}
      />
      {query ? (
        <button type="button" className="tm-search__clear" aria-label="Clear search" onClick={() => setQuery('')}>
          <X size={13} aria-hidden="true" />
        </button>
      ) : (
        <span className="tm-search__hint" aria-hidden="true">
          <Command size={11} /> K
        </span>
      )}
      {open && (
        <ul className="tm-search__results" id="tm-search-results" role="listbox">
          {results.map((result, index) => (
            <li
              key={`${result.kind}-${result.to}`}
              role="option"
              aria-selected={index === highlight}
              className={index === highlight ? 'is-active' : undefined}
              onMouseEnter={() => setHighlight(index)}
              onMouseDown={event => {
                event.preventDefault()
                go(result)
              }}
            >
              <span>{result.label}</span>
              <small>{result.hint}</small>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
