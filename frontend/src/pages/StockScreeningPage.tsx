import { Fragment, useEffect, useEffectEvent, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Check, CircleHelp, Download, Eye as ChevronDown, Eye as ChevronRight, RefreshCw, RotateCcw, Save, SlidersHorizontal, X } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { ColumnPicker, type ColumnSpec } from '../layout/PageChrome'
import { getGapDetails, getHourlyDetails, getScreeningCatalog, queryScreening } from '../services/screening'
import { appliedScreeningColumns, comparisonDate, compileDraft, DEFAULT_COLUMNS, downloadText, emptyDraft, emptyPredicate, filterLabel, formatValue, hasDraftFilter, hourlyContextTiming, hourlyObservedBehavior, hourlyTimestamp, newComparisonReason, newOnlyRequest, removeDraftFilter, resultCsv, ruleIdentity, screeningColumns, screeningColumnGroup, screeningColumnPresets, screeningDetailFacts, screeningDisplayColumns, screeningFieldHelp, screeningTimeframeLabel, screeningValueClass, selectedScreeningColumnPreset, type Draft, type FieldHelpScope, type FieldSpec, type GapDetails, type HourlyDetails, type ScreeningCatalog, type ScreeningRequest, type ScreeningResult, type ScreeningRow } from './screeningModel'
import { useScreenLibrary } from './useScreenLibrary'
import { dailyCoverageNotice } from './screeningModel'
import ScreenLibrary, { Modal } from './ScreenLibrary'
import './StockScreeningPage.css'

export default function StockScreeningPage() {
  const [draft, setDraft] = useState<Draft>(emptyDraft)
  const [request, setRequest] = useState<ScreeningRequest>(() => ({ predicate: emptyPredicate(), generation: null, view: 'CURRENT', new_only: false, sort: 'ticker', descending: false, offset: 0, limit: 100 }))
  const [showNewMarkers, setShowNewMarkers] = useState(true)
  const [columnSelection, setColumns] = useState(DEFAULT_COLUMNS)
  const selectedColumns = screeningColumns(columnSelection)
  const columns = screeningDisplayColumns(selectedColumns, request.predicate)
  const [filtersOpen, setFiltersOpen] = useState(false)
  const [mobile, setMobile] = useState(() => matchMedia('(max-width: 760px)').matches)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [fieldSearch, setFieldSearch] = useState('')
  const [applyOpen, setApplyOpen] = useState(false)
  const [applyMode, setApplyMode] = useState<'APPLY_ONLY' | 'SAVE_NEW' | 'UPDATE'>('APPLY_ONLY')
  const [applyName, setApplyName] = useState('')
  const drawer = useRef<HTMLDialogElement>(null)
  const catalogQuery = useQuery({ queryKey: ['screening-catalog'], queryFn: ({ signal }) => getScreeningCatalog(signal), staleTime: 60_000, refetchInterval: 60_000, refetchOnWindowFocus: false })
  const catalog = catalogQuery.data
  const library = useScreenLibrary({ catalog, draft, request, columns: selectedColumns, setDraft, setRequest, setColumns })
  const dailyRequest = { ...request, generation: null }
  const query = useQuery({ queryKey: ['screening-query', dailyRequest], queryFn: ({ signal }) => queryScreening(dailyRequest, signal), enabled: !!catalog?.publications.length && library.ready, staleTime: 60_000, gcTime: 60_000, refetchInterval: 60_000, refetchOnWindowFocus: false })
  const result = query.data
  const coverageNotice = dailyCoverageNotice(result?.generation)
  const expandedRow = result?.rows.find(row => row.security_id === expanded)
  const gapQuery = useQuery({ queryKey: ['screening-gap-details', result?.generation.generation, expandedRow?.security_id, request.predicate.gap || null],
    queryFn: ({ signal }) => getGapDetails(result!.generation.generation, expandedRow!.security_id, request.predicate.gap || null, signal),
    enabled: !!catalog?.gaps && !!expandedRow?.gap_summary, staleTime: 60_000, gcTime: 60_000, refetchOnWindowFocus: false })
  const hourlyQuery = useQuery({ queryKey: ['screening-hourly-details', result?.generation.generation, expandedRow?.security_id],
    queryFn: ({ signal }) => getHourlyDetails(result!.generation.generation, expandedRow!.security_id, signal),
    enabled: !!catalog?.hourly && !!expandedRow?.hourly, staleTime: 60_000, gcTime: 60_000, refetchOnWindowFocus: false })
  const hourlySource = result?.generation.hourly_source
  const comparison = result?.comparison
  const resultCount = result?.result_count ?? result?.matched_count ?? 0
  const compiled = catalog ? compileDraft(draft, catalog) : null
  const hasErrors = !!compiled && Object.keys(compiled.errors).length > 0
  const draftChanged = !!compiled && (hasErrors || ruleIdentity(compiled.predicate) !== ruleIdentity(request.predicate))
  const hasHourly = columns.includes('hourly')
  const baseLabels: Record<string, string> = { ticker: 'Stock', price: 'Close', change: 'Session change', volume: 'Volume', patterns: 'Candle occurrences', gaps: 'Daily gap', hourly: '1h values', hourly_behavior: '1h filter context' }
  const specs: ColumnSpec[] = screeningColumns([...Object.keys(catalog?.fields || {}), 'patterns', ...(catalog?.gaps ? ['gaps'] : []), ...(catalog?.hourly ? ['hourly'] : []), ...(request.predicate.hourly ? ['hourly_behavior'] : [])]).map(key => {
    const spec = catalog?.fields[key]
    const label = baseLabels[key] || (spec?.unit === 'USD' || spec?.unit === 'USD/day' || spec?.unit === 'shares' ? `${spec.label} (${spec.unit})` : spec?.label || key)
    return { key, group: screeningColumnGroup(key, spec), locked: DEFAULT_COLUMNS.includes(key) || !!request.predicate.hourly && ['hourly', 'hourly_behavior'].includes(key), label: screeningTimeframeLabel(key, label, hasHourly) }
  })
  const optionalColumns = selectedColumns.slice(DEFAULT_COLUMNS.length)
  const columnPresets = screeningColumnPresets(catalog, library.builtIn?.columns || DEFAULT_COLUMNS, library.builtIn ? request.predicate : undefined)
  const columnPreset = selectedScreeningColumnPreset(selectedColumns, columnPresets)
  const groups = [...new Set(Object.values(catalog?.fields || {}).map(spec => spec.group))]
  const activeGroupCounts = Object.fromEntries(groups.map(group => [group, Object.entries(draft.fields).filter(([field, input]) => catalog?.fields[field]?.group === group && hasDraftFilter(input)).length]))
  const filterPanelKey = `${library.workspace.active_tab || 'all'}:${filtersOpen}`
  const units = (unit: string) => unit === 'fraction' ? '%' : unit === 'category' ? '' : unit
  usePublishPageContext({ eyebrow: 'Daily screening', title: 'Stock Screener', status: [{ label: 'Session', value: result?.generation.session || 'Awaiting publication' }] })

  useEffect(() => {
    const media = matchMedia('(max-width: 760px)')
    const change = () => setMobile(media.matches)
    media.addEventListener('change', change)
    return () => media.removeEventListener('change', change)
  }, [])
  useEffect(() => {
    if (!mobile || !drawer.current) return
    if (filtersOpen && !drawer.current.open) drawer.current.showModal()
    if (!filtersOpen && drawer.current.open) drawer.current.close()
  }, [mobile, filtersOpen])
  useEffect(() => { setFieldSearch('') }, [library.workspace.active_tab])

  function toggleFilters() {
    if (!filtersOpen) setFieldSearch('')
    setFiltersOpen(!filtersOpen)
  }

  function apply() {
    if (!compiled || hasErrors) return
    setApplyMode('APPLY_ONLY')
    setApplyName(library.active ? `${library.active.name.slice(0, 73)} (copy)` : library.builtIn?.name || 'My screen')
    setApplyOpen(true)
  }
  async function confirmApply() {
    if (!compiled || hasErrors || library.busy) return
    if (applyMode === 'APPLY_ONLY') {
      setColumns(previous => appliedScreeningColumns(previous, compiled.predicate))
      setRequest(previous => ({ ...previous, predicate: compiled.predicate, generation: null, offset: 0, new_only: compiled.predicate.hourly ? false : previous.new_only }))
    } else {
      if (!library.writable || applyMode === 'UPDATE' && !library.active) return
      const saved = await library.save(applyMode === 'UPDATE' ? library.active!.name : applyName, applyMode === 'SAVE_NEW')
      if (!saved) return
    }
    setExpanded(null)
    setApplyOpen(false)
    setFiltersOpen(false)
  }
  function removeFilter(field: string) {
    const predicate = { ...request.predicate, filters: request.predicate.filters.filter(filter => filter.field !== field), patterns: field === 'patterns' ? [] : request.predicate.patterns, gap: field === 'gap' ? undefined : request.predicate.gap, hourly: field === 'hourly' ? undefined : request.predicate.hourly }
    setRequest(previous => ({ ...previous, predicate, offset: 0 }))
    setDraft(previous => removeDraftFilter(previous, field))
  }
  function editField(field: string, values: Partial<Draft['fields'][string]>) {
    setDraft(previous => ({ ...previous, fields: { ...previous.fields, [field]: { ...(previous.fields[field] || { min: '', max: '', values: [] }), ...values } } }))
  }
  function editGapField(field: string, values: Partial<Draft['fields'][string]>) {
    setDraft(previous => ({ ...previous, gap: { enabled: true, fields: { ...previous.gap?.fields,
      [field]: { ...(previous.gap?.fields[field] || { min: '', max: '', values: [] }), ...values } } } }))
  }
  function moveOptionalColumn(index: number, direction: -1 | 1) {
    setColumns(previous => {
      const extra = screeningColumns(previous).slice(DEFAULT_COLUMNS.length)
      if (index + direction < 0 || index + direction >= extra.length) return screeningColumns(previous)
      ;[extra[index], extra[index + direction]] = [extra[index + direction], extra[index]]
      return screeningColumns(extra)
    })
  }
  function editHourlyField(field: string, bound: 'min' | 'max', value: string) {
    setDraft(previous => ({ ...previous, hourly: { enabled: true, fields: { ...previous.hourly?.fields,
      [field]: { ...(previous.hourly?.fields[field] || { min: '', max: '', values: [] }), [bound]: value } } } }))
  }
  const columnOrder = optionalColumns.length > 0 && <details className="sw-column-order"><summary>Optional column order</summary><ol>{optionalColumns.map((key, index) => <li key={key}><span>{specs.find(spec => spec.key === key)?.label || key}</span><button type="button" className="sw-icon" title="Move column earlier" aria-label={`Move ${key} column earlier`} disabled={index === 0} onClick={() => moveOptionalColumn(index, -1)}><ArrowUp size={13} /></button><button type="button" className="sw-icon" title="Move column later" aria-label={`Move ${key} column later`} disabled={index === optionalColumns.length - 1} onClick={() => moveOptionalColumn(index, 1)}><ArrowDown size={13} /></button></li>)}</ol></details>
  const filters = <>
    <header className="sw-rail-head"><strong>Filters</strong><button type="button" className="sw-icon" title="Close filters" aria-label="Close filters" onClick={() => setFiltersOpen(false)}><X size={16} /></button></header>
    <div className="sw-filter-body"><label className="sw-search">Find a filter<input value={fieldSearch} onChange={event => setFieldSearch(event.target.value)} type="search" /></label>
      {groups.map(group => <details key={`${filterPanelKey}:${group}`} open={!!fieldSearch || activeGroupCounts[group] > 0 || undefined} className="sw-group"><summary>{group}{activeGroupCounts[group] > 0 && <span className="sw-selected-count">{activeGroupCounts[group]} selected</span>}</summary>
        {Object.entries(catalog?.fields || {}).filter(([, spec]) => spec.group === group && spec.label.toLowerCase().includes(fieldSearch.toLowerCase())).map(([field, spec]) => <div className="sw-field" key={field} data-active={hasDraftFilter(draft.fields[field]) || undefined}>
          <div className="sw-field-heading"><label htmlFor={spec.type === 'number' ? `${field}-min` : undefined}>{spec.label} {units(spec.unit) && <small>({units(spec.unit)})</small>}</label><FieldHelpButton field={field} spec={spec} /></div>
          {spec.type === 'category' ? <div className="sw-categories">{spec.options.map(value => <label key={value}><input type="checkbox" checked={draft.fields[field]?.values.includes(value) || false} onChange={event => editField(field, { values: event.target.checked ? [...(draft.fields[field]?.values || []), value] : draft.fields[field].values.filter(item => item !== value) })} />{formatValue(value)}</label>)}</div> : <div className="sw-range">
            <input id={`${field}-min`} aria-label={`${spec.label} minimum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[field]} inputMode="decimal" maxLength={64} placeholder="Min" value={draft.fields[field]?.min || ''} onChange={event => editField(field, { min: event.target.value })} />
            <span>to</span><input aria-label={`${spec.label} maximum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[field]} inputMode="decimal" maxLength={64} placeholder="Max" value={draft.fields[field]?.max || ''} onChange={event => editField(field, { max: event.target.value })} />
          </div>}{compiled?.errors[field] && <small role="alert" className="sw-error">{compiled.errors[field]}</small>}
        </div>)}
      </details>)}
      <details key={`${filterPanelKey}:patterns`} className="sw-group" open={!!fieldSearch || draft.patterns.length > 0 || undefined}><summary>Candle observations{draft.patterns.length > 0 && <span className="sw-selected-count">{draft.patterns.length} selected</span>}</summary><div className="sw-field" data-active={draft.patterns.length > 0 || undefined}><label>Selection<select value={draft.pattern_mode} onChange={event => setDraft(previous => ({ ...previous, pattern_mode: event.target.value as Draft['pattern_mode'] }))}><option value="ANY">Any selected</option><option value="ALL">All selected</option><option value="NONE">None selected</option></select></label>
        {Object.entries(catalog?.patterns || {}).filter(([, spec]) => spec.label.toLowerCase().includes(fieldSearch.toLowerCase())).map(([id, spec]) => <div className="sw-candle-field" key={id}><label className="sw-pattern"><input type="checkbox" checked={draft.patterns.includes(id)} onChange={event => setDraft(previous => ({ ...previous, patterns: event.target.checked ? [...previous.patterns, id] : previous.patterns.filter(value => value !== id) }))} />{spec.label}<span>{spec.direction > 0 ? <ArrowUp size={13} /> : spec.direction < 0 ? <ArrowDown size={13} /> : null}</span></label><FieldHelpButton field={id} scope="pattern" /></div>)}
      </div></details>
      <details key={`${filterPanelKey}:gap`} className="sw-group" open={!!draft.gap?.enabled || !!fieldSearch || undefined}><summary>Daily gap context{draft.gap?.enabled && <span className="sw-selected-count">Selected</span>}</summary>
        <div className="sw-field"><label className="sw-pattern" title="All conditions must match one gap formed within 0-20 trading sessions; formation requires an open at least 1% beyond the previous high or low"><input type="checkbox" checked={!!draft.gap?.enabled} disabled={!catalog?.gaps} onChange={event => setDraft(previous => ({ ...previous, gap: { enabled: event.target.checked, fields: previous.gap?.fields || {} } }))} />Has matching daily gap</label>{!catalog?.gaps && <small>Gap publication unavailable</small>}{compiled?.errors.gap && <small role="alert" className="sw-error">{compiled.errors.gap}</small>}</div>
        {draft.gap?.enabled && Object.entries(catalog?.gaps?.fields || {}).map(([field, spec]) => <div className="sw-field" key={field} data-active={hasDraftFilter(draft.gap?.fields[field]) || undefined}>
          <div className="sw-field-heading"><label htmlFor={spec.type === 'number' ? `gap-${field}-min` : undefined}>{spec.label} {units(spec.unit) && <small>({units(spec.unit)})</small>}</label><FieldHelpButton field={field} scope="gap" spec={spec} /></div>
          {spec.type === 'category' ? <div className="sw-categories">{spec.options.map(value => <label key={value}><input type="checkbox" aria-label={`${spec.label}: ${formatValue(value)}`} checked={draft.gap?.fields[field]?.values.includes(value) || false} onChange={event => editGapField(field, { values: event.target.checked ? [...(draft.gap?.fields[field]?.values || []), value] : (draft.gap?.fields[field]?.values || []).filter(item => item !== value) })} />{formatValue(value)}</label>)}</div> : <div className="sw-range">
            <input id={`gap-${field}-min`} aria-label={`${spec.label} minimum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[`gap.${field}`]} inputMode={field === 'formation_age' ? 'numeric' : 'decimal'} maxLength={64} placeholder="Min" value={draft.gap?.fields[field]?.min || ''} onChange={event => editGapField(field, { min: event.target.value })} />
            <span>to</span><input aria-label={`${spec.label} maximum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[`gap.${field}`]} inputMode={field === 'formation_age' ? 'numeric' : 'decimal'} maxLength={64} placeholder={field === 'formation_age' ? '20' : 'Max'} value={draft.gap?.fields[field]?.max || ''} onChange={event => editGapField(field, { max: event.target.value })} />
          </div>}{compiled?.errors[`gap.${field}`] && <small role="alert" className="sw-error">{compiled.errors[`gap.${field}`]}</small>}
        </div>)}
      </details>
      <details key={`${filterPanelKey}:hourly`} className="sw-group" open={!!draft.hourly?.enabled || !!fieldSearch || undefined}><summary>Hourly context (1h){draft.hourly?.enabled && <span className="sw-selected-count">{Object.values(draft.hourly.fields).filter(hasDraftFilter).length} selected</span>}</summary>
        <div className="sw-field"><label className="sw-pattern"><input type="checkbox" checked={!!draft.hourly?.enabled} disabled={!catalog?.hourly} onChange={event => setDraft(previous => ({ ...previous, hourly: { enabled: event.target.checked, fields: previous.hourly?.fields || {} } }))} />Filter hourly context</label>
          <small>Snapshot: {hourlyTimestamp(hourlySource?.market_time)}</small>{compiled?.errors.hourly && <small className="sw-error" role="alert">{compiled.errors.hourly}</small>}</div>
        {draft.hourly?.enabled && Object.entries(catalog?.hourly?.fields || {}).map(([field, spec]) => <div className="sw-field" key={field} data-active={hasDraftFilter(draft.hourly?.fields[field]) || undefined}>
          <div className="sw-field-heading"><label htmlFor={`hourly-${field}-min`}>{spec.label} <small>({units(spec.unit)})</small></label><FieldHelpButton field={field} scope="hourly" spec={spec} /></div>
          <div className="sw-range"><input id={`hourly-${field}-min`} aria-label={`${spec.label} minimum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[`hourly.${field}`]} inputMode="decimal" maxLength={64} placeholder="Min" value={draft.hourly?.fields[field]?.min || ''} onChange={event => editHourlyField(field, 'min', event.target.value)} />
            <span>to</span><input aria-label={`${spec.label} maximum ${units(spec.unit)}`} aria-invalid={!!compiled?.errors[`hourly.${field}`]} inputMode="decimal" maxLength={64} placeholder="Max" value={draft.hourly?.fields[field]?.max || ''} onChange={event => editHourlyField(field, 'max', event.target.value)} /></div>
          {compiled?.errors[`hourly.${field}`] && <small className="sw-error" role="alert">{compiled.errors[`hourly.${field}`]}</small>}
        </div>)}
      </details>
      <details className="sw-group"><summary>Unavailable capabilities</summary><dl className="sw-deferred">{Object.entries(catalog?.deferred || {}).map(([field, reason]) => <div key={field}><dt>{field.replace(/_/g, ' ')}</dt><dd>{reason}</dd></div>)}</dl></details>
    </div>
    <footer className="sw-filter-actions"><div><button type="button" className="sw-primary" disabled={!compiled || hasErrors} onClick={apply}><Check size={15} />Apply{draftChanged ? ' changes' : ''}</button><button type="button" title="Clear draft filters" onClick={() => setDraft(emptyDraft())}><RotateCcw size={14} />Clear</button></div></footer>
  </>

  return <div className="screening-workspace">
    <ScreenLibrary library={library} catalog={catalog} onNewScreenStarted={() => { setExpanded(null); setFieldSearch(''); setApplyOpen(false); setFiltersOpen(true) }} filterControl={<button type="button" className="sw-icon" title={filtersOpen ? 'Hide filters' : 'Show filters'} aria-label="Toggle filters" aria-expanded={filtersOpen} onClick={toggleFilters}><SlidersHorizontal size={16} /></button>} tools={<>
      <label className="sw-column-preset" title="Display layout only; filters and matches stay unchanged"><select aria-label="Column preset" value={columnPreset} onChange={event => { const preset = columnPresets[event.target.value]; if (preset) setColumns([...preset.columns]) }}><option value="" disabled>Custom columns</option>{Object.entries(columnPresets).map(([key, preset]) => <option key={key} value={key}>{preset.label}</option>)}</select></label>
      <ColumnPicker columns={specs} hidden={new Set(specs.filter(spec => !columns.includes(spec.key)).map(spec => spec.key))} onToggle={key => { if (!specs.find(spec => spec.key === key)?.locked) setColumns(previous => screeningColumns(previous.includes(key) ? previous.filter(value => value !== key) : [...previous, key])) }} onShowAll={() => setColumns(screeningColumns(specs.filter(spec => spec.key !== 'hourly_behavior').map(spec => spec.key)))} onReset={() => setColumns([...columnPresets.screen.columns])}><label className="sw-marker-option"><input type="checkbox" checked={showNewMarkers} onChange={event => setShowNewMarkers(event.target.checked)} />New markers</label>{columnOrder}</ColumnPicker>
      <button type="button" className="sw-icon" title="Export displayed rows with session and generation" aria-label="Export displayed rows" disabled={!result?.rows.length} onClick={() => result && downloadText(resultCsv(result, columns, request.predicate), `screening-${result.generation.session}-page-${request.offset / 100 + 1}.csv`, 'text/csv;charset=utf-8')}><Download size={16} /></button>
      <button type="button" className="sw-icon" title="Refresh published facts" aria-label="Refresh published facts" disabled={query.isFetching} onClick={() => { void catalogQuery.refetch(); void query.refetch() }}><RefreshCw size={16} /></button>
    </>} />
    {library.error && <div className="sw-warning" role="alert"><AlertTriangle size={16} />{library.error}</div>}
    {library.recovered && <div className="sw-warning" role="status">Recovered unsaved filters. <button type="button" onClick={library.discardDraft}>Discard recovered draft</button></div>}
    {applyOpen && <Modal title="Apply filters" close={() => { if (!library.busy) setApplyOpen(false) }}><form onSubmit={event => { event.preventDefault(); void confirmApply() }}>
      <fieldset className="sw-apply-options"><legend>{library.active?.name || library.builtIn?.name || 'All equities'}</legend>
        <label><input type="radio" name="apply-action" value="APPLY_ONLY" checked={applyMode === 'APPLY_ONLY'} disabled={library.busy} onChange={() => setApplyMode('APPLY_ONLY')} />Apply without saving</label>
        <label><input type="radio" name="apply-action" value="SAVE_NEW" checked={applyMode === 'SAVE_NEW'} disabled={!library.writable || library.busy} onChange={() => setApplyMode('SAVE_NEW')} />Save as a new screen</label>
        {library.active && <label><input type="radio" name="apply-action" value="UPDATE" checked={applyMode === 'UPDATE'} disabled={!library.writable || library.busy} onChange={() => setApplyMode('UPDATE')} />Update saved screen</label>}
      </fieldset>
      {applyMode === 'SAVE_NEW' && <label>Screen name<input required maxLength={80} value={applyName} disabled={library.busy} onChange={event => setApplyName(event.target.value)} /></label>}
      {library.error && <p className="sw-error" role="alert">{library.error}</p>}
      <footer><button type="button" disabled={library.busy} onClick={() => setApplyOpen(false)}>Cancel</button><button type="submit" disabled={hasErrors || library.busy || applyMode !== 'APPLY_ONLY' && (!library.writable || applyMode === 'SAVE_NEW' && !applyName.trim())}>{applyMode === 'APPLY_ONLY' ? <Check size={15} /> : <Save size={15} />}{applyMode === 'APPLY_ONLY' ? 'Apply' : 'Save and apply'}</button></footer>
    </form></Modal>}
    <div className={`sw-layout ${filtersOpen && !mobile ? 'sw-with-rail' : ''}`}>
      {mobile ? <dialog ref={drawer} className="sw-drawer" aria-label="Screening filters" onCancel={() => setFiltersOpen(false)} onClose={() => setFiltersOpen(false)}>{filters}</dialog> : filtersOpen && <aside className="sw-rail" aria-label="Screening filters">{filters}</aside>}
      <main id="sw-result-panel" className="sw-results" aria-label="Screening results">
        <div className="sw-results-bar"><div><strong>{result?.matched_count ?? '...'} matches</strong><span>{result?.unknown_count ?? '...'} unknown</span><label className="sw-new-only" title="New matches under the same rules since the previous completed session"><input type="checkbox" checked={!!request.new_only} disabled={query.isFetching || !request.new_only && comparison?.status !== 'READY'} onChange={event => { setRequest(previous => newOnlyRequest(previous, event.target.checked)); setExpanded(null) }} />New only{comparison?.status === 'READY' && <span>({comparison.new_count})</span>}</label>{comparison?.status === 'READY' ? <span className="sw-comparison-date" title={`${comparison.current_session} vs ${comparison.previous_session}`}>{comparisonDate(comparison.current_session)} vs {comparisonDate(comparison.previous_session)}</span> : result && <span title={newComparisonReason(comparison?.status)}>New comparison unavailable</span>}{comparison?.status === 'READY' && comparison.unavailable_count > 0 && <span title="Current matches without a verified comparison; excluded from New only">{comparison.unavailable_count} comparisons unavailable</span>}</div><div className="sw-sort"><label>Sort<select aria-label="Sort results" value={request.sort} onChange={event => setRequest(previous => ({ ...previous, sort: event.target.value, offset: 0 }))}><option value="ticker">Stock</option>{Object.entries(catalog?.fields || {}).map(([key, spec]) => <option key={key} value={key}>{spec.label}</option>)}</select></label><button type="button" className="sw-icon" title={request.descending ? 'Descending order' : 'Ascending order'} aria-label="Reverse sort order" onClick={() => setRequest(previous => ({ ...previous, descending: !previous.descending, offset: 0 }))}>{request.descending ? <ArrowDown size={15} /> : <ArrowUp size={15} />}</button></div></div>
        <div className="sw-chips" aria-label="Applied filters">{result && catalog && <>
          {request.predicate.filters.map(filter => <span key={filter.field}>{filterLabel(filter, catalog)}<button type="button" aria-label={`Remove ${catalog.fields[filter.field].label} filter`} onClick={() => removeFilter(filter.field)}><X size={12} /></button></span>)}
          {request.predicate.patterns.length > 0 && <span>{request.predicate.pattern_mode}: {request.predicate.patterns.map(pattern => catalog.patterns[pattern.id].label).join(', ')}<button type="button" aria-label="Remove pattern filters" onClick={() => removeFilter('patterns')}><X size={12} /></button></span>}
          {request.predicate.gap && catalog.gaps && <span aria-label="Applied daily gap conditions" title="All conditions apply to the same gap episode">Daily gap: {request.predicate.gap.filters.length ? request.predicate.gap.filters.map(filter => filterLabel(filter, { ...catalog, fields: catalog.gaps!.fields })).join('; ') : 'Any within 0-20 sessions'}<button type="button" aria-label="Remove daily gap conditions" onClick={() => removeFilter('gap')}><X size={12} /></button></span>}
          {request.predicate.hourly && catalog.hourly && <span aria-label="Applied hourly conditions">{request.predicate.hourly.filters.map(filter => filterLabel(filter, { ...catalog, fields: catalog.hourly!.fields })).join('; ')}<button type="button" aria-label="Remove hourly conditions" onClick={() => removeFilter('hourly')}><X size={12} /></button></span>}
          {!request.predicate.filters.length && !request.predicate.patterns.length && !request.predicate.gap && !request.predicate.hourly && <small>All eligible instruments</small>}
        </>}{draftChanged && <small className="sw-draft-status">Unapplied changes</small>}</div>
        {hourlySource && (columns.includes('hourly') || request.predicate.hourly) && <div className="sw-hourly-status" role="status">{hourlyContextTiming(result?.generation.market_time, hourlySource)}{result?.hourly_stale || hourlySource.status !== 'READY' ? ' / Stale or unavailable' : ''}{request.predicate.hourly && ' / New comparison unavailable for hourly rules'}</div>}
        {result?.stale && <div className="sw-warning" role="status"><AlertTriangle size={16} />Stale screening publication: {result.generation.session}</div>}
        {coverageNotice && <div className="sw-warning" role="status" aria-label="Daily source coverage"><AlertTriangle size={16} /><span>{coverageNotice}</span></div>}
        {(catalogQuery.isError || query.isError) && <div role="alert" className="sw-warning"><AlertTriangle size={16} />Screening publication unavailable. <button type="button" onClick={() => { void catalogQuery.refetch(); void query.refetch() }}>Retry</button></div>}
        {catalog?.status === 'AWAITING_FIRST_PUBLICATION' ? <div className="sw-empty">Awaiting first daily screening publication</div> : query.isFetching && !result ? <div className="sw-empty" role="status">Loading published screening facts...</div> : result && <>
          <div className="sw-table-scroll"><table><thead><tr><th aria-label="Match explanation" />{columns.map(key => <th key={key} data-column={key}><span className="sw-column-heading"><span>{specs.find(spec => spec.key === key)?.label || key}</span><FieldHelpButton field={key} spec={catalog?.fields[key]} /></span></th>)}</tr></thead><tbody>
            {result.rows.map(row => <Fragment key={row.security_id}><tr><td><button type="button" className="sw-icon" aria-label={`Explain ${row.ticker}`} aria-expanded={expanded === row.security_id} onClick={() => setExpanded(expanded === row.security_id ? null : row.security_id)}>{expanded === row.security_id ? <ChevronDown size={15} /> : <ChevronRight size={15} />}</button></td>{columns.map(key => <td key={key} data-column={key}>{key === 'ticker' ? <><span className="sw-ticker-label"><Link to={`/ticker/${encodeURIComponent(row.ticker)}`} title="Open stock chart">{row.ticker}</Link>{showNewMarkers && row.new_status === 'NEW' && <span className="sw-new-marker" title={`New match since ${comparison?.previous_session}; same applied rules`}>New</span>}</span><small title={row.company_name || ''}>{row.company_name}</small></> : key === 'hourly' ? <HourlySummary hourly={row.hourly} predicate={request.predicate} /> : key === 'hourly_behavior' ? <p className="sw-hourly-behavior">{hourlyObservedBehavior(row, request.predicate, !!result.hourly_stale)}</p> : key === 'gaps' ? <div className="sw-gap-summary">{row.gap_summary?.representative ? <><span>{formatValue(row.gap_summary.representative.values.direction)} / {formatValue(row.gap_summary.representative.values.state)}</span><small>{row.gap_summary.representative.values.formation_age} sessions / {formatValue(row.gap_summary.representative.values.fill_fraction, { unit: 'fraction' })} filled</small><small>{formatValue(row.gap_summary.representative.values.distance_fraction, { unit: 'fraction' })} from edge / {row.gap_summary.matching_count} matching</small></> : <span title={row.gap_summary?.reason}>{row.gap_summary?.status === 'READY' ? 'No matching gap' : 'Unavailable'}</span>}</div> : key === 'patterns' ? Object.entries(row.patterns).filter(([, observation]) => observation.present).map(([id]) => catalog?.patterns[id]?.label).join(', ') || (Object.keys(row.patterns).length ? 'None' : 'Unavailable') : <span className={screeningValueClass(key, row.values[key])} title={row.missing[key] || (key === 'price' ? 'Completed daily close in USD' : key === 'volume' ? 'Completed session volume in shares' : undefined)}>{key === 'price' && typeof row.values[key] === 'number' ? row.values[key].toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : formatValue(row.values[key], catalog?.fields[key])}</span>}</td>)}</tr>
              {expanded === row.security_id && <tr className="sw-explanation"><td colSpan={columns.length + 1}><div><strong>{row.ticker} match explanation</strong><dl><div><dt>Source session</dt><dd>{row.session}</dd></div><div><dt>Generation</dt><dd>{result.generation.generation}</dd></div><div><dt>Security / bar revision</dt><dd>{row.security_id} / {row.source_bar_id}</dd></div><div><dt>Quality</dt><dd>{row.quality.join(', ')}</dd></div><div><dt>New comparison</dt><dd>{newComparisonReason(row.new_reason)}{comparison?.previous_session && ` (${comparison.previous_session})`}</dd></div><div><dt>Prior generation</dt><dd>{comparison?.previous_generation || 'Unavailable'}</dd></div></dl>{!row.explanations.length && <p>Eligible instrument with a valid completed daily bar. No additional predicates.</p>}{row.explanations.map(explanation => <p key={explanation.field}><b>{explanation.field === 'gap' ? 'Daily gap' : explanation.field === 'hourly' ? 'Hourly context' : catalog?.fields[explanation.field]?.label || 'Patterns'}</b>: {explanation.state} {explanation.conditions ? explanation.conditions.map(condition => `${catalog?.hourly?.fields[condition.field]?.label}: ${formatValue(condition.value, catalog?.hourly?.fields[condition.field])} (${condition.state})`).join('; ') : explanation.field === 'gap' ? explanation.reason : explanation.observations ? explanation.observations.map(observation => `${catalog?.patterns[observation.field]?.label}: ${observation.state}`).join('; ') : `${formatValue(explanation.value, catalog?.fields[explanation.field])} (${explanation.reason})`}</p>)}{row.hourly && <HourlyContextDetails data={hourlyQuery.data} fields={catalog?.hourly?.fields || {}} loading={hourlyQuery.isFetching && !hourlyQuery.data} error={hourlyQuery.isError} retry={() => void hourlyQuery.refetch()} />}{row.gap_summary && <GapEpisodeDetails data={gapQuery.data} loading={gapQuery.isFetching && !gapQuery.data} error={gapQuery.isError} retry={() => void gapQuery.refetch()} />}</div></td></tr>}
            </Fragment>)}
          </tbody></table></div>{!result.rows.length && <div className="sw-empty">{request.new_only ? comparison?.status === 'READY' ? 'No verified new matches' : 'New comparison unavailable' : 'No matching stocks'}{result.unknown_count > 0 && <span>{result.unknown_count} instruments have unknown required data</span>}</div>}
          <footer className="sw-pager"><span>{result.rows.length ? `${request.offset + 1}-${request.offset + result.rows.length} of ${resultCount}${request.new_only ? ' new matches' : ''}` : request.new_only ? '0 new matches' : '0 results'} / {result.generation.session}</span><div><button type="button" className="sw-icon" aria-label="Previous page" title="Previous page" disabled={request.offset === 0} onClick={() => setRequest(previous => ({ ...previous, offset: Math.max(0, previous.offset - 100) }))}><ArrowLeft size={16} /></button><button type="button" className="sw-icon" aria-label="Next page" title="Next page" disabled={request.offset + result.rows.length >= resultCount} onClick={() => setRequest(previous => ({ ...previous, offset: previous.offset + 100 }))}><ArrowRight size={16} /></button></div></footer>
          <details className="sw-diagnostics"><summary>Publication details</summary><dl><div><dt>Source cutoff</dt><dd>{result.generation.source_cutoff}</dd></div><div><dt>Published</dt><dd>{result.generation.observed_at}</dd></div><div><dt>Capture</dt><dd>{result.generation.capture_mode}</dd></div><div><dt>Momentum rank population</dt><dd>{result.generation.rank_population} eligible instruments before filtering</dd></div><div><dt>Persistence</dt><dd>{result.generation.persistence_status}</dd></div></dl><ul>{Object.entries(result.generation.field_coverage).map(([field, count]) => <li key={field}>{catalog?.fields[field]?.label}: {count} / {result.universe_count}</li>)}</ul></details>
        </>}
      </main>
    </div>
    {expandedRow && catalog && result && <ScreeningDetailDrawer row={expandedRow} catalog={catalog} generation={result.generation} comparison={comparison} hourlyStale={!!result.hourly_stale} gap={gapQuery.data} gapLoading={gapQuery.isFetching && !gapQuery.data} gapError={gapQuery.isError} retryGap={() => void gapQuery.refetch()} onClose={() => setExpanded(null)} />}
  </div>
}

function ScreeningDetailDrawer({ row, catalog, generation, comparison, hourlyStale, gap, gapLoading, gapError, retryGap, onClose }: {
  row: ScreeningRow
  catalog: ScreeningCatalog
  generation: ScreeningResult['generation']
  comparison?: ScreeningResult['comparison']
  hourlyStale: boolean
  gap?: GapDetails
  gapLoading: boolean
  gapError: boolean
  retryGap: () => void
  onClose: () => void
}) {
  const closeOnEscape = useEffectEvent(onClose)
  const groups = screeningDetailFacts(row, catalog)
  const patterns = Object.entries(row.patterns).filter(([, observation]) => observation.present)
  const gapEpisode = gap?.matching_episodes.find(episode => episode.episode_id === row.gap_summary?.representative?.episode_id)
    || gap?.matching_episodes[0]
  useEffect(() => {
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const keydown = (event: KeyboardEvent) => { if (event.key === 'Escape') closeOnEscape() }
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      document.querySelector<HTMLButtonElement>(`button[aria-label="Explain ${row.ticker}"]`)?.focus()
      document.removeEventListener('keydown', keydown)
    }
  }, [row.ticker])
  const explanationText = (explanation: ScreeningRow['explanations'][number]) => explanation.conditions
    ? explanation.conditions.map(condition => `${catalog.hourly?.fields[condition.field]?.label || condition.field}: ${formatValue(condition.value, catalog.hourly?.fields[condition.field])} (${readableState(condition.state)})`).join('; ')
    : explanation.field === 'gap' ? readableState(explanation.reason || explanation.state)
      : explanation.observations ? explanation.observations.map(observation => `${catalog.patterns[observation.field]?.label || observation.field}: ${readableState(observation.state)}`).join('; ')
        : `${formatValue(explanation.value, catalog.fields[explanation.field])} (${readableState(explanation.reason || explanation.state)})`
  return <div className="sw-detail-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}><aside className="sw-detail-drawer" role="dialog" aria-modal="true" aria-labelledby="sw-detail-title">
    <header><div><span>Daily screening snapshot</span><h2 id="sw-detail-title"><Link to={`/ticker/${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link></h2><p>{row.company_name || 'Company name unavailable'} / {row.session}</p></div><button type="button" className="sw-detail-close" aria-label="Close stock details" title="Close stock details" autoFocus onClick={onClose}><X size={18} /></button></header>
    <div className="sw-detail-body">
      {groups.map(group => <section key={group.title}><h3>{group.title}</h3><div className="sw-detail-grid">{group.facts.map(fact => <div key={fact.field}><span>{fact.label}</span><strong className={fact.tone}>{fact.value}</strong>{fact.missing && <small>{readableState(fact.missing)}</small>}</div>)}</div></section>)}
      {row.hourly && <section><h3>Hourly behavior</h3><p className="sw-detail-note">{generation.hourly_source ? hourlyContextTiming(generation.market_time, generation.hourly_source) : 'Hourly timing unavailable'}{hourlyStale ? ' / Stale' : ''}</p><div className="sw-detail-grid">{Object.entries(catalog.hourly?.fields || {}).map(([field, spec]) => <div key={field}><span>{spec.label}</span><strong className={field === 'close' ? '' : screeningValueClass('change', row.hourly!.values[field])}>{field === 'close' && row.hourly!.values[field] != null ? `$${formatValue(row.hourly!.values[field])}` : formatValue(row.hourly!.values[field], spec)}</strong>{row.hourly!.missing[field] && <small>{readableState(row.hourly!.missing[field])}</small>}</div>)}</div></section>}
      <section><h3>Setups and observations</h3><div className="sw-detail-grid"><div><span>Candle observations</span><strong>{patterns.length ? patterns.map(([id]) => catalog.patterns[id]?.label || id).join(', ') : 'None'}</strong></div><div><span>Daily gap</span><strong>{row.gap_summary?.representative ? `${formatValue(row.gap_summary.representative.values.direction)} / ${formatValue(row.gap_summary.representative.values.state)}` : row.gap_summary?.status === 'READY' ? 'No matching gap' : 'Unavailable'}</strong>{row.gap_summary?.representative && <small>{row.gap_summary.representative.values.formation_age} sessions old / {formatValue(row.gap_summary.representative.values.fill_fraction, { unit: 'fraction' })} filled</small>}</div>{gapEpisode && <><div><span>Gap zone</span><strong>${formatValue(gapEpisode.zone_lower)} to ${formatValue(gapEpisode.zone_upper)}</strong></div><div><span>Price vs gap</span><strong>{formatValue(gapEpisode.values.price_location)} / {formatValue(gapEpisode.values.distance_fraction, { unit: 'fraction' })} from edge</strong></div></>}</div>{gapLoading && <p className="sw-detail-note" role="status">Loading retained gap details...</p>}{gapError && <p className="sw-detail-note" role="alert">Gap details unavailable. <button type="button" onClick={retryGap}>Retry</button></p>}</section>
      <section><h3>Why this stock matched</h3>{row.explanations.length ? <ul className="sw-detail-reasons">{row.explanations.map(explanation => <li key={explanation.field}><strong>{explanation.field === 'gap' ? 'Daily gap' : explanation.field === 'hourly' ? 'Hourly context' : catalog.fields[explanation.field]?.label || 'Candle observations'}</strong><span>{explanationText(explanation)}</span></li>)}</ul> : <p className="sw-detail-note">Eligible instrument with a valid completed daily bar; no additional predicates were applied.</p>}<div className="sw-detail-grid"><div><span>Screen membership</span><strong>{row.new_status === 'NEW' ? 'New match' : row.new_status === 'NOT_NEW' ? 'Continuing match' : 'Comparison unavailable'}</strong><small>{newComparisonReason(row.new_reason)}</small></div><div><span>Comparison</span><strong>{comparison?.status === 'READY' ? `${comparison.current_session} vs ${comparison.previous_session}` : 'Unavailable'}</strong></div></div></section>
    </div>
  </aside></div>
}

const readableState = (value: string) => value.toLowerCase().replace(/_/g, ' ')

function FieldHelpButton({ field, scope = 'daily', spec }: { field: string; scope?: FieldHelpScope; spec?: FieldSpec }) {
  const [open, setOpen] = useState(false)
  const trigger = useRef<HTMLButtonElement>(null)
  const restoreFocus = useRef(false)
  const help = screeningFieldHelp(field, scope)
  useEffect(() => {
    if (!open && restoreFocus.current) {
      trigger.current?.focus()
      restoreFocus.current = false
    }
  }, [open])
  if (!help) return null
  function close() {
    restoreFocus.current = true
    setOpen(false)
  }
  return <>
    <button ref={trigger} type="button" className="sw-field-help" aria-label={`Explain ${help.title}`} title={help.purpose} aria-haspopup="dialog" onClick={() => setOpen(true)}><CircleHelp size={14} aria-hidden="true" /></button>
    {open && <Modal title={`About ${help.title}`} close={close}><div className="sw-help-content">
      <dl className="sw-help-purpose"><dt>Why track it?</dt><dd>{help.purpose}</dd></dl>
      <dl><dt>How to read it</dt><dd>{help.example || help.meaning}</dd></dl>
      <details className="sw-help-calculation"><summary>Calculation details</summary>
        <p>{help.meaning}</p>
        {help.formula && <dl><dt>Formula</dt><dd><code>{help.formula}</code></dd></dl>}
        <dl><dt>Interpretation and limits</dt><dd>{help.note}</dd></dl>
        {spec && <dl className="sw-help-facts"><div><dt>Timeframe</dt><dd>{scope === 'hourly' ? 'Completed regular-session hourly slots' : 'Completed daily sessions'}</dd></div><div><dt>Required history</dt><dd>{spec.warmup_sessions === 0 ? 'Dated instrument reference' : `${spec.warmup_sessions} valid ${scope === 'hourly' ? 'slots' : 'sessions'}`}</dd></div></dl>}
        {spec?.unit === 'fraction' && <p className="sw-help-note">Percentage inputs use percent units: enter 5 for 5%, not 0.05. Bounds are inclusive; an empty bound is unrestricted.</p>}
        <p className="sw-help-note">Unavailable is not zero. Missing history, identity issues or known corporate actions can prevent a value from being calculated. Price-based fields use retained unadjusted prices; they are not dividend-reinvested total returns.</p>
      </details>
    </div><footer><button type="button" onClick={close}>Close</button></footer></Modal>}
  </>
}

function HourlySummary({ hourly, predicate }: { hourly?: ScreeningRow['hourly']; predicate: ScreeningRequest['predicate'] }) {
  if (!hourly) return <span>Unavailable</span>
  const labels: Record<string, string> = { close: '1h close', change: '1h change', vs_ema20: '1h vs EMA20', ema20_change_3: '1h EMA20 / 3 bars' }
  const fields = predicate.hourly ? predicate.hourly.filters.map(filter => filter.field) : Object.keys(labels)
  return <dl className="sw-hourly-summary">{fields.map(field => <div key={field}><dt>{labels[field]}</dt><dd title={hourly.missing[field]} className={field === 'close' ? '' : screeningValueClass('change', hourly.values[field])}>{field === 'close' && hourly.values[field] != null ? `$${formatValue(hourly.values[field])}` : formatValue(hourly.values[field], { unit: 'fraction' })}</dd></div>)}</dl>
}

function HourlyContextDetails({ data, fields, loading, error, retry }: { data?: HourlyDetails; fields: Record<string, FieldSpec>; loading: boolean; error: boolean; retry: () => void }) {
  return <section className="sw-hourly-details" aria-label="Hourly context details"><h3>Hourly context (1h)</h3>
    {loading && <p role="status">Loading retained hourly context...</p>}
    {error && <p role="alert">Hourly details unavailable. <button type="button" onClick={retry}>Retry</button></p>}
    {data && <><p>Hourly: {hourlyTimestamp(data.source.market_time)} / {data.source.slot_minutes}-minute slot</p>
      <dl>{Object.entries(fields).map(([field, spec]) => <div key={field}><dt className="sw-field-heading"><span>{spec.label}</span><FieldHelpButton field={field} scope="hourly" spec={spec} /></dt><dd>{formatValue(data.values[field], spec)}{data.missing[field] && ` (${data.missing[field].replace(/_/g, ' ').toLowerCase()})`}</dd></div>)}</dl>
      <details><summary>Hourly sources</summary><dl><div><dt>Source publication</dt><dd>{data.source.source_publication_id || 'Unavailable'}</dd></div><div><dt>Source availability cutoff</dt><dd>{data.source.source_cutoff || 'Unavailable'}</dd></div><div><dt>Daily snapshot cutoff</dt><dd>{data.source.snapshot_cutoff}</dd></div><div><dt>Published latest bar</dt><dd>{data.lineage.selected_bar_id || 'Unavailable'}</dd></div><div><dt>Retained hourly slots</dt><dd>{data.lineage.bars.length}</dd></div><div><dt>Reconstructed from retained 30m</dt><dd>{Object.keys(data.lineage.reconstructed_from_30m).length} slots</dd></div></dl><ul>{Object.entries(data.lineage.reconstructed_from_30m).map(([identity, sources]) => <li key={identity}>{identity}: {sources.join(', ')}</li>)}</ul></details>
    </>}
  </section>
}

function GapEpisodeDetails({ data, loading, error, retry }: { data?: GapDetails; loading: boolean; error: boolean; retry: () => void }) {
  return <section className="sw-gap-details" aria-label="Daily gap episodes"><h3>Daily gap episodes</h3>
    {loading && <p role="status">Loading retained gap episodes...</p>}
    {error && <p role="alert">Gap details unavailable. <button type="button" onClick={retry}>Retry</button></p>}
    {data && <>{data.status === 'READY' && <p>{data.matching_episodes.length} matching / {data.episode_count} observed / {data.session}</p>}
      {data.status !== 'READY' ? <p>Gap coverage unavailable: {data.reason.replace(/_/g, ' ').toLowerCase()}</p> : !data.matching_episodes.length && <p>No matching gaps within 0-20 sessions</p>}
      <ul>{data.matching_episodes.map(episode => <li key={episode.episode_id}><h4>{episode.formation_session} / {formatValue(episode.values.direction)} / <span title="Current fill state; FAILED is not permanent invalidation">{formatValue(episode.values.state)}</span></h4><dl>
        <div><dt>Formation age</dt><dd>{episode.values.formation_age} trading sessions</dd></div>
        <div><dt>Opening gap filled</dt><dd>{formatValue(episode.values.fill_fraction, { unit: 'fraction' })}</dd></div>
        <div><dt>Fixed zone</dt><dd>${formatValue(episode.zone_lower)} to ${formatValue(episode.zone_upper)}</dd></div>
        <div><dt>Zone basis</dt><dd>{episode.zone_basis === 'FORMATION_RANGE_GAP' ? 'Formation range gap' : 'Open to previous close'}</dd></div>
        <div><dt>Price vs zone</dt><dd>{formatValue(episode.values.price_location)}</dd></div>
        <div><dt>Nearest zone edge</dt><dd>{episode.nearest_boundary.toLowerCase()} / ${formatValue(episode.boundary_price)} / {formatValue(episode.values.distance_fraction, { unit: 'fraction' })} away</dd></div>
        <div><dt>Opening price</dt><dd>${formatValue(episode.opening_price)}</dd></div>
        <div><dt>Fill target (previous close)</dt><dd>${formatValue(episode.fill_target)}</dd></div>
        <div><dt>First fill session</dt><dd>{episode.first_fill_session || 'Not filled'}</dd></div>
      </dl><details><summary>Episode sources</summary><dl><div><dt>Episode ID</dt><dd>{episode.episode_id}</dd></div><div><dt>Formation bar</dt><dd>{episode.formation_bar_id}</dd></div><div><dt>Previous bar</dt><dd>{episode.previous_bar_id}</dd></div><div><dt>Source cutoff</dt><dd>{data.source_cutoff}</dd></div></dl></details></li>)}</ul>
    </>}
  </section>
}