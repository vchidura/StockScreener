import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowUp, Check, CircleHelp, RefreshCw, RotateCcw, SlidersHorizontal, X } from 'lucide-react'
import { getOptionDiscoveryCatalog } from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import { Modal } from './ScreenLibrary'
import { OptionsScreenLibrary, OptionsChainResults } from './OptionsScreenLibrary.tsx'
import { independentScreenQuery, optionFilterGroups, optionRuleLabels } from './optionsScreenLibrary'
import './StockScreeningPage.css'
import './OptionsScreenerPage.css'

const filterFields = (params: URLSearchParams) => Object.fromEntries([...params].filter(([key]) => key === 'type' || key === 'underlyer' || key.startsWith('range_')))

export default function OptionsScreeningWorkspace() {
  const [params, setParams] = useSearchParams()
  const queryClient = useQueryClient()
  const catalogQuery = useQuery({ queryKey: ['options', 'discovery-catalog'], queryFn: getOptionDiscoveryCatalog, staleTime: 60_000, retry: false })
  const catalog = catalogQuery.data
  const [filtersOpen, setFiltersOpen] = useState(false)
  const [mobile, setMobile] = useState(() => matchMedia('(max-width: 760px)').matches)
  const drawer = useRef<HTMLDialogElement>(null)
  const [search, setSearch] = useState('')
  const [draft, setDraft] = useState<Record<string, string>>(() => filterFields(params))
  const [applyOpen, setApplyOpen] = useState(false)
  const [help, setHelp] = useState<string | null>(null)
  const appliedFilters = JSON.stringify(filterFields(params))
  const sessionDate = params.get('session_date') || ''
  let applied: Record<string, string> = { view: 'chain' }
  let invalid = ''
  try { if (catalog) applied = independentScreenQuery(params, catalog) } catch (failure) { invalid = failure instanceof Error ? failure.message : 'Invalid screen' }
  const draftQuery = { ...Object.fromEntries(Object.entries(applied).filter(([key]) => key !== 'type' && key !== 'underlyer' && !key.startsWith('range_'))), ...draft }
  let draftError = ''
  try { if (catalog) independentScreenQuery(new URLSearchParams(draftQuery), catalog) } catch (failure) { draftError = failure instanceof Error ? failure.message : 'Invalid filters' }
  const draftDirty = JSON.stringify(Object.entries(draft).sort()) !== JSON.stringify(Object.entries(JSON.parse(appliedFilters)).sort())

  useEffect(() => { setDraft(JSON.parse(appliedFilters)) }, [appliedFilters])
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
  usePublishPageContext({ eyebrow: 'Options', title: 'Options Screener', status: [{ label: 'Session', value: sessionDate || 'Latest stored' }] })
  function edit(key: string, value: string) { setDraft(previous => { const next = { ...previous }; if (value === '') delete next[key]; else next[key] = value; return next }) }
  function update(key: string, value: string) { const next = new URLSearchParams(window.location.search); if (value) next.set(key, value); else next.delete(key); next.delete('offset'); setParams(next) }
  function remove(field: string) {
    const next = new URLSearchParams(window.location.search)
    const keys = field === 'type' || field === 'underlyer' ? [field] : [`range_${field}_min`, `range_${field}_max`]
    keys.forEach(key => next.delete(key)); next.delete('offset'); setParams(next)
    setDraft(previous => { const result = { ...previous }; keys.forEach(key => delete result[key]); return result })
  }
  const matchedGroups = Object.entries(optionFilterGroups).map(([group, keys]) => [group, keys.filter(key => catalog?.contract_filters?.[key]?.label.toLowerCase().includes(search.toLowerCase()))] as const)
  const filters = <>
    <header className="sw-rail-head"><strong>Filters</strong><button type="button" className="sw-icon" title="Close filters" aria-label="Close filters" onClick={() => setFiltersOpen(false)}><X size={16} /></button></header>
    <div className="sw-filter-body"><label className="sw-search">Find a filter<input aria-label="Find a filter" type="search" value={search} onChange={event => setSearch(event.target.value)} /></label>
      {(!search || /underlying|contract|type|calls|puts/i.test(search)) && <details className="sw-group" open><summary>Contract</summary>
        <div className="sw-field" data-active={!!draft.underlyer}><label>Underlying<input aria-label="Underlying" maxLength={15} placeholder="All tracked" value={draft.underlyer || ''} onChange={event => edit('underlyer', event.target.value.toUpperCase())} /></label></div>
        <div className="sw-field" data-active={!!draft.type}><label>Type<select aria-label="Contract type" value={draft.type || 'ALL'} onChange={event => edit('type', event.target.value === 'ALL' ? '' : event.target.value)}><option value="ALL">Calls + puts</option><option value="CALL">Calls</option><option value="PUT">Puts</option></select></label></div>
      </details>}
      {matchedGroups.filter(([, keys]) => keys.length).map(([group, keys]) => <details className="sw-group" key={`${group}:${params.get('builtin') || params.get('screen') || 'all'}:${search}`} open={!!search || keys.some(key => draft[`range_${key}_min`] != null || draft[`range_${key}_max`] != null) || group === 'Expiration & price'}>
        <summary>{group}</summary>{keys.map(key => { const field = catalog!.contract_filters![key]; const active = draft[`range_${key}_min`] != null || draft[`range_${key}_max`] != null
          return <div key={key} className="sw-field" data-active={active}><div className="sw-field-heading"><label htmlFor={`option-${key}-min`}>{field.label} <small>({field.unit === 'fraction' ? '%' : field.unit})</small></label><button type="button" className="sw-field-help" title={`${field.label} details`} aria-label={`${field.label} details`} onClick={() => setHelp(key)}><CircleHelp size={14} /></button></div>
            <div className="sw-range"><input id={`option-${key}-min`} aria-label={`Minimum ${field.label}`} inputMode={field.integer ? 'numeric' : 'decimal'} maxLength={32} placeholder="Min" value={draft[`range_${key}_min`] || ''} onChange={event => edit(`range_${key}_min`, event.target.value)} /><span>to</span><input aria-label={`Maximum ${field.label}`} inputMode={field.integer ? 'numeric' : 'decimal'} maxLength={32} placeholder="Max" value={draft[`range_${key}_max`] || ''} onChange={event => edit(`range_${key}_max`, event.target.value)} /></div>
          </div>
        })}
      </details>)}
      <details className="sw-group"><summary>Unavailable screening fields</summary><dl className="sw-deferred"><dt>Quote spread / size</dt><dd>Usable NBBO is not published for this retained source.</dd><dt>Sweep-like / smile residual</dt><dd>No independent screening field is published.</dd><dt>IV rank / percentile</dt><dd>Comparable historical IV readiness is required.</dd></dl></details>
      {draftError && <p className="sw-error os-draft-error" role="alert">{draftError}</p>}
    </div><footer className="sw-filter-actions"><div><button type="button" className="sw-primary" disabled={!catalog || !!draftError || !!invalid} onClick={() => { if (mobile) setFiltersOpen(false); setApplyOpen(true) }}><Check size={15} />Apply{draftDirty ? ' changes' : ''}</button><button type="button" title="Clear draft filters" onClick={() => setDraft({})}><RotateCcw size={14} />Clear</button></div></footer>
  </>
  return <div className="screening-workspace options-screening-workspace">
    <OptionsScreenLibrary catalog={catalog} params={params} setParams={setParams} draftQuery={draftQuery} draftDirty={draftDirty} applyOpen={applyOpen} onApplyClose={() => setApplyOpen(false)} onLoaded={() => { setDraft(filterFields(new URLSearchParams(window.location.search))); setFiltersOpen(false); setSearch('') }} onNew={() => { setDraft({}); setFiltersOpen(true) }}
      filterControl={<button type="button" className="sw-icon" title={filtersOpen ? 'Hide filters' : 'Show filters'} aria-label="Toggle filters" aria-expanded={filtersOpen} onClick={() => setFiltersOpen(value => !value)}><SlidersHorizontal size={16} /></button>}
      tools={<button type="button" className="sw-icon" title="Refresh stored contracts" aria-label="Refresh stored contracts" onClick={() => { void catalogQuery.refetch(); void queryClient.invalidateQueries({ queryKey: ['options', 'eligible-chain'] }) }}><RefreshCw size={16} /></button>} />
    {catalogQuery.isError && <p className="sw-warning" role="alert"><AlertTriangle size={16} />Contract screening catalog unavailable.</p>}
    <div className={`sw-layout ${filtersOpen && !mobile ? 'sw-with-rail' : ''}`}>
      {mobile ? <dialog ref={drawer} className="sw-drawer" aria-label="Screening filters" onCancel={() => setFiltersOpen(false)} onClose={() => setFiltersOpen(false)} onKeyDown={event => { if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); drawer.current?.close(); setFiltersOpen(false) } }}>{filters}</dialog> : filtersOpen && <aside className="sw-rail" aria-label="Screening filters">{filters}</aside>}
      <main className="sw-results" id="os-result-panel" aria-label="Screening results">
        <div className="sw-results-bar"><div><label className="os-session-field">Options session<input aria-label="Options session" type="date" value={sessionDate} onChange={event => update('session_date', event.target.value)} /></label>{sessionDate && <button type="button" className="sw-icon" title="Latest stored options" aria-label="Latest stored options" onClick={() => update('session_date', '')}><RotateCcw size={15} /></button>}<span>Delayed contract observations</span></div><div className="sw-sort"><label>Sort<select aria-label="Sort results" value={params.get('sort') || 'day_volume'} onChange={event => update('sort', event.target.value)}>{Object.entries(catalog?.contract_filters || {}).map(([key, field]) => <option key={key} value={key}>{field.label}</option>)}</select></label><button type="button" className="sw-icon" title={params.get('descending') === '0' ? 'Ascending order' : 'Descending order'} aria-label="Reverse sort order" onClick={() => update('descending', params.get('descending') === '0' ? '1' : '0')}>{params.get('descending') === '0' ? <ArrowUp size={15} /> : <ArrowDown size={15} />}</button></div></div>
        <div className="sw-chips" aria-label="Applied filters">{catalog && !invalid && optionRuleLabels(applied, catalog).map(label => {
          const field = Object.keys(catalog.contract_filters || {}).find(key => label.startsWith(`${catalog.contract_filters![key].label}:`)) || (label.startsWith('Underlying:') ? 'underlyer' : label === 'Calls' || label === 'Puts' ? 'type' : '')
          return field ? <span key={label}>{label}<button type="button" aria-label={`Remove ${catalog.contract_filters?.[field]?.label || field} filter`} onClick={() => remove(field)}><X size={12} /></button></span> : <small key={label}>{label}</small>
        })}{draftDirty && <small className="sw-draft-status">Unapplied changes</small>}</div>
        {!invalid && <OptionsChainResults catalog={catalog} params={params} setParams={setParams} sessionDate={sessionDate || undefined} resultsOnly />}
      </main>
    </div>
    {help && catalog?.contract_filters?.[help] && <Modal title={`${catalog.contract_filters[help].label} details`} close={() => setHelp(null)}><div className="sw-help-content"><dl><dt>Units</dt><dd>{catalog.contract_filters[help].unit === 'fraction' ? 'Percent; enter 25 for 25%' : catalog.contract_filters[help].unit}</dd><dt>Matching</dt><dd>Inclusive minimum and maximum on the same stored contract. Missing values do not match a numeric bound.</dd><dt>Source</dt><dd>Retained model-eligible contract snapshot, not an executable quote or alert signal.</dd></dl>{help === 'volume_open_interest_ratio' && <p>Volume divided by snapshot open interest. Zero or missing OI is unavailable. The ratio does not identify opening activity or buyer/seller direction.</p>}{help === 'absolute_delta' && <p>Absolute model delta, not a calibrated probability of profit.</p>}</div></Modal>}
  </div>
}