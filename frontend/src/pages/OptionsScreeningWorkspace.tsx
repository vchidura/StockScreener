import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowUp, Check, CircleHelp, RefreshCw, RotateCcw, SlidersHorizontal, X } from 'lucide-react'
import { getOptionDiscoveryCatalog } from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import { Modal } from './ScreenLibrary'
import { OptionsScreenLibrary, OptionsChainResults } from './OptionsScreenLibrary.tsx'
import { independentScreenQuery, isScreenFieldKey, SCREEN_EDIT_KEY, optionFilterGroups, optionRuleLabels, optionTemplates, screenChangeLabels, screenTierLabel, serverScreenDefaults, serverScreenFields, serverScreenFor } from './optionsScreenLibrary'
import './StockScreeningPage.css'
import './OptionsScreenerPage.css'

const filterFields = (params: URLSearchParams) => Object.fromEntries([...params].filter(([key]) => key === 'type' || key === 'underlyer' || key.startsWith('range_') || isScreenFieldKey(key)))

export default function OptionsScreeningWorkspace() {
  const [params, setParams] = useSearchParams()
  const queryClient = useQueryClient()
  const catalogQuery = useQuery({ queryKey: ['options', 'discovery-catalog'], queryFn: getOptionDiscoveryCatalog, staleTime: 60_000, retry: false })
  const catalog = catalogQuery.data
  const builtinScreen = serverScreenFor(params.get('builtin'), catalog)
  const screenFields = builtinScreen?.criteria ? serverScreenFields(builtinScreen) : null
  const screenDefaults: Record<string, string> = builtinScreen ? Object.fromEntries(Object.entries({ ...(builtinScreen.contract_type ? { type: builtinScreen.contract_type } : {}), ...serverScreenDefaults(builtinScreen, catalog) }).filter(([key]) => key !== SCREEN_EDIT_KEY)) : {}
  // Nov 10: the opened tab's purpose replaces the list of its rules above the results (the rules are in the fields).
  const template = optionTemplates(catalog).find(item => item.id === params.get('builtin'))
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
  const draftQuery = { ...Object.fromEntries(Object.entries(applied).filter(([key]) => key !== 'type' && key !== 'underlyer' && !key.startsWith('range_') && !isScreenFieldKey(key))), ...draft }
  let draftError = ''
  try { if (catalog) independentScreenQuery(new URLSearchParams(draftQuery), catalog) } catch (failure) { draftError = failure instanceof Error ? failure.message : 'Invalid filters' }
  const draftDirty = JSON.stringify(Object.entries(draft).sort()) !== JSON.stringify(Object.entries(JSON.parse(appliedFilters)).sort())

  useEffect(() => { setDraft(JSON.parse(appliedFilters)) }, [appliedFilters])
  useEffect(() => {
    // Nov 10: a built-in tab opened before its fields were published carries none of them; fill them once.
    if (!builtinScreen?.criteria || params.get(SCREEN_EDIT_KEY) === '1') return
    const next = new URLSearchParams(window.location.search)
    for (const [key, value] of Object.entries(serverScreenDefaults(builtinScreen, catalog))) if (!next.has(key)) next.set(key, value)
    setParams(next, { replace: true })
  }, [builtinScreen, catalog, params, setParams])
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
  // Nov 13 (user): the Options Screener has no session picker; the header shows the latest stored session as a fixed date.
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
    <div className="sw-filter-body">{builtinScreen && screenFields && <div className="os-builtin-rules" aria-label="Built-in screen"><strong>{builtinScreen.name}</strong><p className="sw-hint">Every field marked Built-in is filled from this screen; change any of them to change what it returns.</p><button type="button" onClick={() => setDraft({ ...screenDefaults })}><RotateCcw size={13} />Reset to built-in</button></div>}<label className="sw-search">Find a filter<input aria-label="Find a filter" type="search" value={search} onChange={event => setSearch(event.target.value)} /></label>
      {(!search || /underlying|contract|type|calls|puts/i.test(search)) && <details className="sw-group" open><summary>Contract</summary>
        <div className="sw-field" data-active={!!draft.underlyer}><label>Underlying<input aria-label="Underlying" maxLength={15} placeholder="All tracked" value={draft.underlyer || ''} onChange={event => edit('underlyer', event.target.value.toUpperCase())} /></label></div>
        <div className="sw-field" data-active={!!draft.type}><label>Type<select aria-label="Contract type" value={draft.type || 'ALL'} onChange={event => edit('type', event.target.value === 'ALL' ? '' : event.target.value)}><option value="ALL">Calls + puts</option><option value="CALL">Calls</option><option value="PUT">Puts</option></select></label></div>
      </details>}
      {matchedGroups.filter(([, keys]) => keys.length).map(([group, keys]) => <details className="sw-group" key={`${group}:${params.get('builtin') || params.get('screen') || 'all'}:${search}`} open={!!search || keys.some(key => draft[`range_${key}_min`] != null || draft[`range_${key}_max`] != null || !!screenFields?.tiers[key]) || group === 'Expiration & price' || !!screenFields && group === 'Volume & open interest'}>
        <summary>{group}</summary>{keys.map(key => { const field = catalog!.contract_filters![key]; const tiers = screenFields?.tiers[key]; const builtin = !!screenFields && (screenFields.minimums.has(key) || !!tiers || key === 'calendar_dte' && screenFields.excludeDte); const active = draft[`range_${key}_min`] != null || draft[`range_${key}_max`] != null || builtin
          return <div key={key} className="sw-field" data-active={active}><div className="sw-field-heading"><label htmlFor={`option-${key}-min`}>{field.label} <small>({field.unit === 'fraction' ? '%' : field.unit})</small>{builtin && <span className="os-builtin-tag" title={`Set by the built-in screen ${builtinScreen?.name}`}>Built-in</span>}</label><button type="button" className="sw-field-help" title={`${field.label} details`} aria-label={`${field.label} details`} onClick={() => setHelp(key)}><CircleHelp size={14} /></button></div>
            <div className="sw-range"><input id={`option-${key}-min`} aria-label={`Minimum ${field.label}`} inputMode={field.integer ? 'numeric' : 'decimal'} maxLength={32} placeholder={tiers ? 'Min (all tiers)' : 'Min'} value={draft[`range_${key}_min`] || ''} onChange={event => edit(`range_${key}_min`, event.target.value)} /><span>to</span><input aria-label={`Maximum ${field.label}`} inputMode={field.integer ? 'numeric' : 'decimal'} maxLength={32} placeholder="Max" value={draft[`range_${key}_max`] || ''} onChange={event => edit(`range_${key}_max`, event.target.value)} /></div>
            {tiers && <TierMinimums field={key} tiers={tiers} draft={draft} edit={edit} />}
            {key === 'calendar_dte' && screenFields?.excludeDte && <label className="os-builtin-extra">Exclude DTE<input aria-label="Exclude DTE" inputMode="numeric" maxLength={120} placeholder="e.g. 3, 4" value={draft.exclude_calendar_dte || ''} onChange={event => edit('exclude_calendar_dte', event.target.value)} /></label>}
          </div>
        })}
      </details>)}
      {screenFields?.tiers.day_premium && (!search || /premium/i.test(search)) && <details className="sw-group" open><summary>Premium</summary><div className="sw-field" data-active><div className="sw-field-heading"><label>Day premium <small>(USD)</small><span className="os-builtin-tag">Built-in</span></label></div><TierMinimums field="day_premium" tiers={screenFields.tiers.day_premium} draft={draft} edit={edit} /></div></details>}
      {screenFields?.caps && (!search || /limit|result/i.test(search)) && <details className="sw-group" open><summary>Result limits</summary><div className="sw-field" data-active><div className="os-tier-grid"><label>Per underlying<input aria-label="Limit per underlying" inputMode="numeric" maxLength={3} placeholder="No limit" value={draft.cap_per_underlying || ''} onChange={event => edit('cap_per_underlying', event.target.value)} /></label><label>In all<input aria-label="Limit in all" inputMode="numeric" maxLength={3} placeholder="No limit" value={draft.cap_total || ''} onChange={event => edit('cap_total', event.target.value)} /></label></div></div></details>}
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
        <div className="sw-results-bar"><div>{sessionDate && <button type="button" className="sw-icon" title="Latest stored options" aria-label="Latest stored options" onClick={() => update('session_date', '')}><RotateCcw size={15} /></button>}<span>Delayed contract observations</span></div><div className="sw-sort"><label>Sort<select aria-label="Sort results" value={params.get('sort') || 'day_volume'} onChange={event => update('sort', event.target.value)}>{Object.entries(catalog?.contract_filters || {}).map(([key, field]) => <option key={key} value={key}>{field.label}</option>)}</select></label><button type="button" className="sw-icon" title={params.get('descending') === '0' ? 'Ascending order' : 'Descending order'} aria-label="Reverse sort order" onClick={() => update('descending', params.get('descending') === '0' ? '1' : '0')}>{params.get('descending') === '0' ? <ArrowUp size={15} /> : <ArrowDown size={15} />}</button></div></div>
        {template && !invalid && <p className="os-screen-purpose"><strong>{template.name}</strong> {template.summary}</p>}
        {template && !invalid ? <div className="sw-chips" aria-label="Changes from the built-in screen">{screenChangeLabels(applied, { ...template.query, ...screenDefaults }, catalog).map(label => <small key={label}>{label}</small>)}{draftDirty && <small className="sw-draft-status">Unapplied changes</small>}</div>
        : <div className="sw-chips" aria-label="Applied filters">{catalog && !invalid && optionRuleLabels(applied, catalog).map(label => {
          const field = Object.keys(catalog.contract_filters || {}).find(key => label.startsWith(`${catalog.contract_filters![key].label}:`)) || (label.startsWith('Underlying:') ? 'underlyer' : label === 'Calls' || label === 'Puts' ? 'type' : '')
          return field ? <span key={label}>{label}<button type="button" aria-label={`Remove ${catalog.contract_filters?.[field]?.label || field} filter`} onClick={() => remove(field)}><X size={12} /></button></span> : <small key={label}>{label}</small>
        })}{draftDirty && <small className="sw-draft-status">Unapplied changes</small>}</div>}
        {!invalid && <OptionsChainResults catalog={catalog} params={params} setParams={setParams} sessionDate={sessionDate || undefined} resultsOnly />}
      </main>
    </div>
    {help && catalog?.contract_filters?.[help] && <Modal title={`${catalog.contract_filters[help].label} details`} close={() => setHelp(null)}><div className="sw-help-content"><dl><dt>Units</dt><dd>{catalog.contract_filters[help].unit === 'fraction' ? 'Percent; enter 25 for 25%' : catalog.contract_filters[help].unit}</dd><dt>Matching</dt><dd>Inclusive minimum and maximum on the same stored contract. Missing values do not match a numeric bound.</dd><dt>Source</dt><dd>Retained model-eligible contract snapshot, not an executable quote or alert signal.</dd></dl>{help === 'volume_open_interest_ratio' && <p>Volume divided by snapshot open interest. Zero or missing OI is unavailable. The ratio does not identify opening activity or buyer/seller direction.</p>}{help === 'absolute_delta' && <p>Absolute model delta, not a calibrated probability of profit.</p>}</div></Modal>}
  </div>
}

/** Nov 10: a minimum that differs by underlying tier (Unusual-activity Volume and Day premium), one box per tier. */
function TierMinimums({ field, tiers, draft, edit }: { field: string; tiers: string[]; draft: Record<string, string>; edit: (key: string, value: string) => void }) {
  return <div className="os-tier-grid" aria-label={`${field} minimum by tier`}>{tiers.map(tier => <label key={tier}>{screenTierLabel(tier)}<input aria-label={`${field} minimum for ${screenTierLabel(tier)}`} inputMode="decimal" maxLength={16} placeholder="0" value={draft[`tier_${field}_${tier}`] || ''} onChange={event => edit(`tier_${field}_${tier}`, event.target.value)} /></label>)}</div>
}
