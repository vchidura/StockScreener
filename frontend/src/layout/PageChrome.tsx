import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Settings2 } from 'lucide-react'
import './pageChrome.css'

/** Dense control strip that sits directly under the shell context bar. */
export function CommandBar({ children }: { children: ReactNode }) {
  return <div className="tm-commandbar">{children}</div>
}

export function CommandField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="tm-commandbar__field">
      <span>{label}</span>
      {children}
    </label>
  )
}

/** Non-label variant for groups of buttons, which must not be wrapped in a <label>. */
export function CommandGroup({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="tm-commandbar__field" role="group" aria-label={label}>
      <span>{label}</span>
      <div className="tm-commandbar__group">{children}</div>
    </div>
  )
}

export function CommandSpacer() {
  return <span className="tm-commandbar__spacer" />
}

export type ColumnSpec = {
  key: string
  label: string
  /** Identity columns that must always render. */
  locked?: boolean
  /** Hidden on first load so wide tables fit; the picker re-enables them. */
  hiddenByDefault?: boolean
}

const storageName = (key: string) => `alphascreener.columns.${key}`

function initialHidden(storageKey: string, columns: ColumnSpec[]): Set<string> {
  const fallback = new Set(columns.filter(column => column.hiddenByDefault).map(column => column.key))
  try {
    const raw = localStorage.getItem(storageName(storageKey))
    if (!raw) return fallback
    const stored = JSON.parse(raw) as string[]
    const known = new Set(columns.map(column => column.key))
    return new Set(stored.filter(key => known.has(key)))
  } catch {
    return fallback
  }
}

export function useColumnPreferences(storageKey: string, columns: ColumnSpec[]) {
  const [hidden, setHidden] = useState<Set<string>>(() => initialHidden(storageKey, columns))

  useEffect(() => {
    try {
      localStorage.setItem(storageName(storageKey), JSON.stringify([...hidden]))
    } catch {
      // Private browsing can block storage; the choice still holds for this session.
    }
  }, [hidden, storageKey])

  const toggle = useCallback((key: string) => setHidden(previous => {
    const next = new Set(previous)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    return next
  }), [])

  const showAll = useCallback(() => setHidden(new Set()), [])
  const reset = useCallback(
    () => setHidden(new Set(columns.filter(column => column.hiddenByDefault).map(column => column.key))),
    [columns],
  )

  const isVisible = useCallback((key: string) => !hidden.has(key), [hidden])
  const visibleColumns = useMemo(() => columns.filter(column => !hidden.has(column.key)), [columns, hidden])

  return { hidden, toggle, showAll, reset, isVisible, visibleColumns }
}

export function ColumnPicker({
  columns,
  hidden,
  onToggle,
  onShowAll,
  onReset,
}: {
  columns: ColumnSpec[]
  hidden: Set<string>
  onToggle: (key: string) => void
  onShowAll: () => void
  onReset: () => void
}) {
  const [open, setOpen] = useState(false)
  const container = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const close = (event: MouseEvent) => {
      if (!container.current?.contains(event.target as Node)) setOpen(false)
    }
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', close)
    document.addEventListener('keydown', escape)
    return () => {
      document.removeEventListener('mousedown', close)
      document.removeEventListener('keydown', escape)
    }
  }, [open])

  const hiddenCount = columns.filter(column => hidden.has(column.key)).length
  const title = `Choose columns${hiddenCount ? ` (${hiddenCount} hidden)` : ''}`

  return (
    <div className="tm-columns" ref={container}>
      <button
        type="button"
        className={`tm-columns__button${open ? ' is-open' : ''}`}
        onClick={() => setOpen(value => !value)}
        aria-label={title}
        aria-expanded={open}
        title={title}
      >
        <Settings2 size={15} strokeWidth={2} aria-hidden="true" />
        {hiddenCount > 0 && <i>{hiddenCount}</i>}
      </button>
      {open && (
        <div className="tm-columns__menu">
          <div className="tm-columns__actions">
            <button type="button" onClick={onShowAll}>Show all</button>
            <button type="button" onClick={onReset}>Reset to fit</button>
          </div>
          {columns.map(column => (
            <label key={column.key} className={column.locked ? 'is-locked' : undefined}>
              <input
                type="checkbox"
                checked={!hidden.has(column.key)}
                disabled={column.locked}
                onChange={() => onToggle(column.key)}
              />
              {column.label}
            </label>
          ))}
        </div>
      )}
    </div>
  )
}
