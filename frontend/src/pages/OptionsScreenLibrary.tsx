import { useEffect, useRef, useState, type ReactNode, type KeyboardEvent } from 'react'
import { Link, type SetURLSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Copy, Download, Eye, FolderOpen, Pencil, Plus, Save, Search, Trash2, Upload, RotateCcw, Check, ChevronDown, X } from 'lucide-react'
import { ColumnPicker } from '../layout/PageChrome'
import { getOptionCandidate, getOptionCandidates, queryEligibleChain, type EligibleChainRow, type EligibleChainRequest, type OptionCandidateRow, type OptionDiscoveryCatalog } from '../services/api'
import { Modal } from './ScreenLibrary'
import { chainFilterRanges, columnsForView, independentScreenQuery, makeOptionsScreen, optionColumnPresets, optionContractDetailSections, optionRuleLabels, optionTemplates, OPTIONS_LIBRARY_KEY, readOptionsLibrary, screenQuery, visibleOptionsColumns, writeOptionsLibrary, type OptionsLibrary, type OptionsScreen, type OptionsView } from './optionsScreenLibrary'
import { optionAlertMoney, optionAlertPercent, optionAlertTime } from './optionsAlertPresentation'
import './StockScreeningPage.css'

type ControlsProps = { catalog?: OptionDiscoveryCatalog; params: URLSearchParams; setParams: SetURLSearchParams }
const readable = (value: string) => value.replace(/_/g, ' ')
const money = (value: string | number | null | undefined) => value == null ? 'Unavailable' : Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD' })
const timestamp = (value: string) => new Date(value).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
function download(content: string, name: string, type: string) {
  const url = URL.createObjectURL(new Blob([content], { type }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = name
  anchor.click()
  URL.revokeObjectURL(url)
}

type LibraryProps = ControlsProps & {
  filterControl?: ReactNode
  tools?: ReactNode
  draftQuery?: Record<string, string>
  draftDirty?: boolean
  applyOpen?: boolean
  onApplyClose?: () => void
  onLoaded?: () => void
  onNew?: () => void
}

export function OptionsScreenLibrary({ catalog, params, setParams, filterControl, tools, draftQuery, draftDirty = false, applyOpen = false, onApplyClose, onLoaded, onNew }: LibraryProps) {
  const [library, setLibrary] = useState<OptionsLibrary>({ schema_version: 1, screens: [] })
  const [writable, setWritable] = useState(false)
  const [error, setError] = useState('')
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [collection, setCollection] = useState('BUILTIN')
  const [preview, setPreview] = useState('builtin:all')
  const [applyMode, setApplyMode] = useState('APPLY_ONLY')
  const [applyName, setApplyName] = useState('')
  const fileInput = useRef<HTMLInputElement>(null)
  const newScreenAfterLoad = useRef(false)
  const closeAfterLoad = useRef<string | null>(null)
  const [naming, setNaming] = useState<{ action: 'SAVE' | 'UPDATE' | 'RENAME' | 'COPY'; screen?: OptionsScreen } | null>(null)
  const [name, setName] = useState('')
  const [deleting, setDeleting] = useState<OptionsScreen | null>(null)
  const [pending, setPending] = useState<Record<string, string> | null>(null)
  const [afterSave, setAfterSave] = useState<Record<string, string> | null>(null)
  const [baseline, setBaseline] = useState(() => {
    try { return catalog ? JSON.stringify(independentScreenQuery(params, catalog)) : '{"view":"chain"}' } catch { return '{}' }
  })
  const active = library.screens.find(item => item.id === params.get('screen'))
  const view: OptionsView = 'chain'
  const columns = columnsForView(view)
  const visible = visibleOptionsColumns(view, params.get('columns')).map(column => column.key)
  const presets = optionColumnPresets(view)
  const selectedPreset = Object.entries(presets).find(([, keys]) => columns.every(column => visible.includes(column.key) === keys.includes(column.key)))?.[0] || ''
  let query: Record<string, string> = {}
  let queryError = ''
  try { if (catalog) query = independentScreenQuery(params, catalog) } catch (failure) { queryError = failure instanceof Error ? failure.message : 'Invalid filters' }
  const identity = JSON.stringify(query)
  const dirty = !queryError && (draftDirty || identity !== (active ? JSON.stringify(active.query) : baseline || identity))
  const activeKey = active ? `saved:${active.id}` : params.get('builtin') && params.get('builtin') !== 'all' ? `builtin:${params.get('builtin')}` : null
  const choices = [
    ...optionTemplates(catalog).map(item => ({ ...item, key: `builtin:${item.id}`, kind: 'BUILTIN', saved: undefined as OptionsScreen | undefined })),
    ...library.screens.map(item => {
      let unavailable: string | undefined
      try { if (catalog) independentScreenQuery(new URLSearchParams(item.query), catalog) } catch (failure) { unavailable = failure instanceof Error ? failure.message : 'Unsupported screen' }
      return { ...item, key: `saved:${item.id}`, kind: 'SAVED', summary: 'Saved contract filters and columns.', unavailable, saved: item }
    }),
  ]
  const visibleChoices = choices.filter(item => item.kind === collection && `${item.name} ${item.summary}`.toLowerCase().includes(search.toLowerCase()))
  const selectedChoice = visibleChoices.find(item => item.key === preview)
  const tabKeys = [...new Set([...(library.tabs || []), ...(activeKey ? [activeKey] : [])])]
  const tabs = tabKeys.map(key => choices.find(item => item.key === key)).filter((item): item is typeof choices[number] => !!item && !item.unavailable && item.id !== 'all')
  const shown = tabs.slice(0, 3)
  const currentTab = tabs.find(item => item.key === activeKey)
  if (currentTab && !shown.includes(currentTab)) shown[shown.length - 1] = currentTab
  const overflow = tabs.filter(item => !shown.includes(item))

  useEffect(() => {
    if (!catalog) return
    const hydrate = () => {
      try { setLibrary(readOptionsLibrary(localStorage, catalog)); setWritable(true); setError('') }
      catch (failure) { setWritable(false); setError(`Library unavailable: ${failure instanceof Error ? failure.message : 'storage error'}. Stored definitions were preserved.`) }
    }
    hydrate()
    const changed = (event: StorageEvent) => { if (event.key === OPTIONS_LIBRARY_KEY) hydrate() }
    window.addEventListener('storage', changed)
    return () => window.removeEventListener('storage', changed)
  }, [catalog])
  useEffect(() => {
    if (!dirty) return
    const beforeUnload = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = '' }
    window.addEventListener('beforeunload', beforeUnload)
    return () => window.removeEventListener('beforeunload', beforeUnload)
  }, [dirty])
  useEffect(() => { if (applyOpen) { setApplyMode('APPLY_ONLY'); setApplyName(active?.name || '') } }, [applyOpen, active?.name])

  function commit(transform: (latest: OptionsLibrary) => OptionsLibrary) {
    if (!catalog || !writable) return false
    try {
      const next = transform(readOptionsLibrary(localStorage, catalog))
      writeOptionsLibrary(localStorage, next, catalog)
      setLibrary(next); setError('')
      return true
    } catch (failure) { setError(`Not saved: ${failure instanceof Error ? failure.message : 'storage failed'}`); return false }
  }
  function load(next: Record<string, string>) {
    if (!catalog) return
    try { independentScreenQuery(new URLSearchParams(next), catalog) } catch (failure) { setError(failure instanceof Error ? failure.message : 'Unsupported screen'); return }
    const nextParams = new URLSearchParams(next)
    const selectedSession = new URLSearchParams(window.location.search).get('session_date')
    if (selectedSession) nextParams.set('session_date', selectedSession)
    setBaseline(JSON.stringify(screenQuery(nextParams, catalog)))
    const key = next.screen ? `saved:${next.screen}` : next.builtin && next.builtin !== 'all' ? `builtin:${next.builtin}` : null
    if (closeAfterLoad.current && writable) {
      const closing = closeAfterLoad.current
      if (!commit(latest => ({ ...latest, tabs: (latest.tabs || []).filter(tab => tab !== closing) }))) return
      closeAfterLoad.current = null
    }
    if (key && writable && !commit(latest => ({ ...latest, tabs: [...new Set([...(latest.tabs || []), key])] }))) return
    setParams(nextParams); setOpen(false); setPending(null); onLoaded?.()
    if (newScreenAfterLoad.current) { newScreenAfterLoad.current = false; onNew?.() }
  }
  function select(next: Record<string, string>) { if (dirty) setPending(next); else load(next) }
  function startNaming(action: 'SAVE' | 'UPDATE' | 'RENAME' | 'COPY', screen?: OptionsScreen) {
    setName(action === 'COPY' ? `${screen?.name.slice(0, 73)} (copy)` : screen?.name || '')
    setNaming({ action, screen })
  }
  function save() {
    if (!naming || !catalog) return
    try {
      const original = naming.screen
      const saved = makeOptionsScreen(name, naming.action === 'RENAME' || naming.action === 'COPY' ? original!.query : independentScreenQuery(new URLSearchParams(draftQuery || Object.fromEntries(new URLSearchParams(window.location.search))), catalog), catalog,
        naming.action === 'UPDATE' || naming.action === 'RENAME' ? original : undefined)
      if (!commit(latest => ({ ...latest, screens: [...latest.screens.filter(item => item.id !== saved.id), saved] }))) return
      if (afterSave) { load(afterSave); setAfterSave(null) }
      else if (naming.action === 'SAVE' || naming.action === 'UPDATE') {
        const next = new URLSearchParams({ ...saved.query, screen: saved.id })
        const selectedSession = new URLSearchParams(window.location.search).get('session_date')
        if (selectedSession) next.set('session_date', selectedSession)
        setParams(next)
      }
      setNaming(null)
      onLoaded?.()
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Save failed') }
  }
  async function importFile(file?: File) {
    if (!file || !catalog) return
    try {
      if (file.size > 250_000) throw new Error('Import exceeds size limit')
      const raw = await file.text()
      const imported = readOptionsLibrary({ getItem: () => raw }, catalog)
      const copies = imported.screens.map(item => makeOptionsScreen(item.name, item.query, catalog))
      commit(latest => ({ ...latest, screens: [...latest.screens, ...copies] }))
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Import failed') }
  }
  function openPicker() { setCollection(active ? 'SAVED' : 'BUILTIN'); setPreview(activeKey || 'builtin:all'); setSearch(''); setOpen(true) }
  function openChoice(choice: typeof choices[number]) { if (!choice.unavailable) select({ ...choice.query, ...(choice.saved ? { screen: choice.id } : { builtin: choice.id }) }) }
  function closeTab(key: string) {
    if (key === activeKey && dirty) { closeAfterLoad.current = key; setPending({ view: 'chain' }); return }
    if (commit(latest => ({ ...latest, tabs: (latest.tabs || []).filter(item => item !== key) })) && key === activeKey) load({ view: 'chain' })
  }
  function tabKey(event: KeyboardEvent<HTMLButtonElement>) {
    const elements = Array.from(event.currentTarget.closest('[role=tablist]')!.querySelectorAll<HTMLButtonElement>('[role=tab]'))
    const index = elements.indexOf(event.currentTarget)
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? elements.length - 1 : event.key === 'ArrowRight' ? (index + 1) % elements.length : event.key === 'ArrowLeft' ? (index + elements.length - 1) % elements.length : null
    if (next !== null) { event.preventDefault(); elements[next].focus(); elements[next].click() }
  }
  function applyDraft() {
    if (!catalog || !draftQuery) return
    try {
      const validated = independentScreenQuery(new URLSearchParams(draftQuery), catalog)
      const next = new URLSearchParams(validated)
      for (const key of ['screen', 'builtin', 'session_date']) if (params.has(key)) next.set(key, params.get(key)!)
      if (applyMode !== 'APPLY_ONLY') {
        const saved = makeOptionsScreen(applyMode === 'UPDATE' ? active!.name : applyName, validated, catalog, applyMode === 'UPDATE' ? active : undefined)
        if (!commit(latest => ({ ...latest, screens: [...latest.screens.filter(item => item.id !== saved.id), saved], tabs: [...new Set([...(latest.tabs || []), `saved:${saved.id}`])] }))) return
        next.set('screen', saved.id); next.delete('builtin')
      }
      setParams(next); onApplyClose?.(); onLoaded?.()
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Invalid draft') }
  }
  const selectedName = active?.name || choices.find(item => item.key === activeKey)?.name || 'All options'
  return <>
    <section className="sw-toolbar" aria-label="Screen controls"><div className="sw-screen-navigation">{filterControl}
      <button type="button" className="sw-screen-picker" aria-label="Choose screener" title={selectedName} aria-haspopup="dialog" aria-expanded={open} onClick={openPicker} disabled={!catalog}><FolderOpen size={18} /><span>Screen library</span><ChevronDown size={14} /></button>
      <div className="sw-screen-tabs"><nav className="sw-desktop-pins" role="tablist" aria-label="Open screeners">
        <button type="button" role="tab" aria-selected={!activeKey} tabIndex={!activeKey ? 0 : -1} onKeyDown={tabKey} onClick={() => select({ view: 'chain' })}>All options{!activeKey && dirty ? ' *' : ''}</button>
        {shown.map(item => <span key={item.key}><button type="button" role="tab" className="sw-pin-name" aria-selected={activeKey === item.key} tabIndex={activeKey === item.key ? 0 : -1} onKeyDown={tabKey} title={item.name} onClick={() => openChoice(item)}>{item.name}{activeKey === item.key && dirty ? ' *' : ''}</button><button type="button" className="sw-unpin" aria-label={`Close ${item.name} tab`} title={`Close ${item.name} tab`} onClick={() => closeTab(item.key)}><X size={12} /></button></span>)}
        {!!overflow.length && <select aria-label="More open screeners" value="" onChange={event => { const choice = choices.find(item => item.key === event.target.value); if (choice) openChoice(choice) }}><option value="">More ({overflow.length})</option>{overflow.map(item => <option key={item.key} value={item.key}>{item.name}</option>)}</select>}
      </nav><div className="sw-mobile-picker"><select aria-label="Open screeners" value={activeKey || ''} onChange={event => { const choice = choices.find(item => item.key === event.target.value); if (choice) openChoice(choice); else select({ view: 'chain' }) }}><option value="">All options</option>{tabs.map(item => <option key={item.key} value={item.key}>{item.name}</option>)}</select>{activeKey && <button type="button" className="sw-icon" aria-label="Close active screener tab" title="Close active tab" onClick={() => closeTab(activeKey)}><X size={15} /></button>}<button type="button" className="sw-icon" aria-label="Choose another screener" title="Choose screener" onClick={openPicker}><Plus size={16} /></button></div></div>
      </div><div className="sw-tools"><label title="Display layout only"><select aria-label="Column preset" value={selectedPreset} onChange={event => { const next = new URLSearchParams(window.location.search); next.set('columns', presets[event.target.value].join(',')); setParams(next) }}>
        <option value="" disabled>Custom columns</option>{Object.keys(presets).map(key => <option key={key}>{key}</option>)}
      </select></label>
      <ColumnPicker columns={columns} hidden={new Set(columns.filter(column => !visible.includes(column.key)).map(column => column.key))}
        onToggle={key => { const next = new URLSearchParams(window.location.search); const current = visibleOptionsColumns(view, next.get('columns')).map(column => column.key); next.set('columns', columns.filter(column => column.locked || (column.key === key ? !current.includes(key) : current.includes(column.key))).map(column => column.key).join(',')); setParams(next) }}
        onShowAll={() => { const next = new URLSearchParams(window.location.search); next.set('columns', columns.map(column => column.key).join(',')); setParams(next) }} onReset={() => { const next = new URLSearchParams(window.location.search); next.delete('columns'); setParams(next) }} />
      <button type="button" className="sw-icon" title="Save as new screen" aria-label="Save as new screen" disabled={!writable || !!queryError} onClick={() => startNaming('SAVE')}><Save size={16} /></button>{tools}</div></section>
    {(error || queryError) && <div className="screener-state is-warning" role="alert">{error || queryError}</div>}
    {open && <Modal title="Screen library" close={() => setOpen(false)} wide>
      <div className="sw-library-toolbar"><label className="sw-library-search"><Search size={16} /><input aria-label="Search screens" type="search" placeholder="Find a screener" value={search} onChange={event => setSearch(event.target.value)} /></label><button type="button" className="sw-icon" title="Import copies" aria-label="Import definitions" disabled={!writable} onClick={() => fileInput.current?.click()}><Upload size={16} /></button><button type="button" className="sw-icon" title="Export library definitions" aria-label="Export library definitions" disabled={!library.screens.length} onClick={() => download(JSON.stringify(library, null, 2), 'options-screen-library.json', 'application/json')}><Download size={16} /></button><input ref={fileInput} type="file" accept="application/json,.json" hidden onChange={event => { void importFile(event.target.files?.[0]); event.target.value = '' }} /></div>
      <div className="sw-library-modes" role="group" aria-label="Screen collection">{[['BUILTIN', 'Built-in screens'], ['SAVED', 'My saved screens']].map(([key, label]) => <button type="button" key={key} aria-pressed={collection === key} onClick={() => { setCollection(key); setPreview('') }}>{label}</button>)}</div>
      {error && <p role="alert" className="sw-error">{error}</p>}
      <div className="sw-library-browser"><div className="sw-choice-list" aria-label="Screen choices">{visibleChoices.map(item => <button type="button" key={item.key} className="sw-choice" aria-pressed={preview === item.key} aria-label={`Preview ${item.name}`} onClick={() => setPreview(item.key)}><span><strong>{item.name}</strong><small>{item.kind === 'SAVED' ? 'Saved' : 'Recipe'} / Options{item.unavailable ? ' / unavailable' : ''}</small></span>{tabKeys.includes(item.key) && <Check size={14} aria-label="Open tab" />}</button>)}{!visibleChoices.length && <p className="sw-library-empty">No matching screeners</p>}</div>
      <section className="sw-choice-detail" aria-label="Selected screener">{selectedChoice ? <><div className="sw-choice-title"><span className="sw-choice-kind">{selectedChoice.kind === 'SAVED' ? 'Your screen' : 'Built-in'}</span><h3>{selectedChoice.name}</h3><p>{selectedChoice.summary}</p></div>{selectedChoice.unavailable && <p className="sw-warning">{selectedChoice.unavailable}</p>}<ul className="sw-rule-list">{!selectedChoice.unavailable && catalog && optionRuleLabels(selectedChoice.query, catalog).map(label => <li key={label}>{label}</li>)}</ul><dl className="sw-screen-facts"><div><dt>Sort</dt><dd>{catalog?.contract_filters?.[selectedChoice.query.sort || 'day_volume']?.label || 'Volume'} / {selectedChoice.query.descending === '0' ? 'ascending' : 'descending'}</dd></div><div><dt>Source</dt><dd>Retained contract facts / selected options session</dd></div></dl>
      {selectedChoice.saved && <div className="sw-row-tools" aria-label="Manage selected screen"><button type="button" className="sw-icon" title="Rename screen" aria-label="Rename selected screen" onClick={() => startNaming('RENAME', selectedChoice.saved)}><Pencil size={15} /></button><button type="button" className="sw-icon" title="Duplicate screen" aria-label="Duplicate selected screen" onClick={() => startNaming('COPY', selectedChoice.saved)}><Copy size={15} /></button><button type="button" className="sw-icon" title="Export definition" aria-label="Export selected screen" onClick={() => download(JSON.stringify({ schema_version: 1, screens: [selectedChoice.saved] }, null, 2), 'options-screen.json', 'application/json')}><Download size={15} /></button><button type="button" className="sw-icon" title="Delete local definition" aria-label="Delete selected screen" onClick={() => setDeleting(selectedChoice.saved!)}><Trash2 size={15} /></button></div>}</> : <p className="sw-library-empty">Select a screener</p>}</section></div>
      <footer className="sw-library-footer"><small>Browser-local / {window.location.host}</small>{collection === 'SAVED' && <button type="button" disabled={!catalog} onClick={() => { newScreenAfterLoad.current = true; select({ view: 'chain' }) }}><Plus size={15} />New screen</button>}<button type="button" className="sw-primary" disabled={!selectedChoice || !!selectedChoice.unavailable} onClick={() => selectedChoice && openChoice(selectedChoice)}><FolderOpen size={15} />{selectedChoice && tabKeys.includes(selectedChoice.key) ? 'Go to tab' : 'Open tab'}</button></footer>
    </Modal>}
    {applyOpen && <Modal title="Apply filters" close={() => onApplyClose?.()}><form onSubmit={event => { event.preventDefault(); applyDraft() }}><fieldset className="sw-apply-options"><legend>{selectedName}</legend><label><input type="radio" name="apply-action" checked={applyMode === 'APPLY_ONLY'} onChange={() => setApplyMode('APPLY_ONLY')} />Apply without saving</label><label><input type="radio" name="apply-action" checked={applyMode === 'SAVE_NEW'} disabled={!writable} onChange={() => setApplyMode('SAVE_NEW')} />Save as a new screen</label>{active && <label><input type="radio" name="apply-action" checked={applyMode === 'UPDATE'} disabled={!writable} onChange={() => setApplyMode('UPDATE')} />Update saved screen</label>}</fieldset>{applyMode === 'SAVE_NEW' && <label>Screen name<input aria-label="Screen name" required maxLength={80} value={applyName} onChange={event => setApplyName(event.target.value)} /></label>}{error && <p role="alert" className="sw-error">{error}</p>}<footer><button type="button" onClick={() => onApplyClose?.()}>Cancel</button><button type="submit" disabled={applyMode === 'SAVE_NEW' && !applyName.trim()}>{applyMode === 'APPLY_ONLY' ? <Check size={15} /> : <Save size={15} />}{applyMode === 'APPLY_ONLY' ? 'Apply' : 'Save and apply'}</button></footer></form></Modal>}
    {naming && <Modal title={naming.action === 'RENAME' ? 'Rename options screen' : naming.action === 'COPY' ? 'Duplicate options screen' : 'Save options screen'} close={() => { setNaming(null); setAfterSave(null) }}>
      <form className="os-library-content" onSubmit={event => { event.preventDefault(); save() }}><label>Screen name<input autoFocus aria-label="Screen name" maxLength={80} value={name} onChange={event => setName(event.target.value)} /></label>
        {error && <p role="alert">{error}</p>}<footer><button type="button" onClick={() => { setNaming(null); setAfterSave(null) }}>Cancel</button><button type="submit" disabled={!name.trim()}><Save size={15} />Save</button></footer></form>
    </Modal>}
    {deleting && <Modal title="Delete options screen" close={() => setDeleting(null)}><div className="os-library-content"><p>Delete {deleting.name}?</p><footer><button type="button" onClick={() => setDeleting(null)}>Cancel</button><button type="button" onClick={() => {
      if (commit(latest => ({ ...latest, screens: latest.screens.filter(item => item.id !== deleting.id) }))) { if (active?.id === deleting.id) { const next = new URLSearchParams(params); next.delete('screen'); setParams(next); setBaseline(identity) } setDeleting(null) }
    }}><Trash2 size={15} />Delete</button></footer></div></Modal>}
    {pending && <Modal title="Unsaved screen changes" close={() => { setPending(null); closeAfterLoad.current = null; newScreenAfterLoad.current = false }}><div className="os-library-content"><p>Save or discard changes before opening another screen.</p><footer>
      <button type="button" onClick={() => { setPending(null); closeAfterLoad.current = null; newScreenAfterLoad.current = false }}>Cancel</button><button type="button" onClick={() => load(pending)}>Discard changes</button>
      <button type="button" onClick={() => { setAfterSave(pending); setPending(null); startNaming(active ? 'UPDATE' : 'SAVE', active) }}><Save size={15} />Save changes</button>
    </footer></div></Modal>}
  </>
}

function OptionContractDetail({ row, close }: { row: EligibleChainRow; close: () => void }) {
  const detailTone = (key: string, value: unknown) => {
    if (key === 'contract_type') return value === 'CALL' ? 'os-positive' : value === 'PUT' ? 'os-negative' : ''
    if (['model_mark', 'display_mark'].includes(key)) return 'os-option-price'
    if (!['local_delta', 'local_theta_per_day', 'local_vega_per_vol_point', 'local_rho_per_rate_point'].includes(key)) return ''
    const numeric = Number(value)
    return !Number.isFinite(numeric) || numeric === 0 ? '' : numeric > 0 ? 'os-positive' : 'os-negative'
  }
  function fieldValue(field: ReturnType<typeof optionContractDetailSections>[number]['fields'][number]): string {
    const value = field.value
    if (value == null || typeof value === 'string' && !value.trim()) return 'Unavailable'
    if (field.format === 'money') return optionAlertMoney(value)
    if (field.format === 'percent') return optionAlertPercent(value)
    if (field.format === 'time') return optionAlertTime(typeof value === 'string' ? value : null)
    if (field.format === 'boolean') return value === true ? 'Yes' : value === false ? 'No' : 'Unavailable'
    if (field.format === 'flags') return Array.isArray(value) ? value.length ? value.map(readable).join(' / ') : 'None recorded' : 'Unavailable'
    if (field.format === 'number') return typeof value !== 'boolean' && !Array.isArray(value) && Number.isFinite(Number(value))
      ? Number(value).toLocaleString('en-US', { maximumFractionDigits: 6 }) : 'Unavailable'
    return String(value)
  }
  return <Modal title="Option contract details" close={close}><div className="os-contract-detail">
    <div className="os-contract-heading"><h3>{row.underlying} / <span className={row.contract_type === 'CALL' ? 'os-positive' : 'os-negative'}>{row.contract_type}</span> {optionAlertMoney(row.strike)}</h3><p>{row.contract_ticker}</p></div>
    <p className="os-contract-notice">Retained contract snapshot / Delayed marks / Not a fill or an alert package</p>
    {optionContractDetailSections(row).map(section => <section key={section.title} aria-label={section.title}>
      <h3>{section.title}</h3><dl className="os-contract-facts">{section.fields.map(field => <div key={field.key}>
        <dt>{field.label}</dt><dd className={detailTone(field.key, field.value)} title={typeof field.value === 'string' ? field.value : undefined}>{fieldValue(field)}</dd>
      </div>)}</dl>
    </section>)}
    <p className="os-contract-notice">No strategy selection, stop/target plan, calibrated probability or execution permission is attached to this screening row.</p>
    <details className="os-contract-raw"><summary>Retained snapshot fields</summary><pre>{JSON.stringify(row, null, 2)}</pre></details>
  </div></Modal>
}

export function OptionsChainResults({ catalog, params, setParams, sessionDate, resultsOnly = false }: ControlsProps & { sessionDate?: string; resultsOnly?: boolean }) {
  const [search, setSearch] = useState('')
  const [filtersOpen, setFiltersOpen] = useState(false)
  const [selectedContract, setSelectedContract] = useState<EligibleChainRow | null>(null)
  const contractTrigger = useRef<HTMLButtonElement | null>(null)
  useEffect(() => {
    if (!selectedContract) contractTrigger.current?.focus({ preventScroll: true })
  }, [selectedContract])
  let invalid = ''
  let ranges: EligibleChainRequest['filters'] = []
  try { if (catalog) ranges = chainFilterRanges(independentScreenQuery(params, catalog), catalog) } catch (failure) { invalid = failure instanceof Error ? failure.message : 'Invalid filters' }
  const rawOffset = Number(params.get('offset') || 0)
  const offset = Number.isInteger(rawOffset) && rawOffset >= 0 ? rawOffset : 0
  const request: EligibleChainRequest = {
    session_date: sessionDate, underlyer: params.get('underlyer') || undefined,
    contract_type: params.get('type') === 'CALL' ? 'CALL' : params.get('type') === 'PUT' ? 'PUT' : undefined,
    filters: ranges, sort: params.get('sort') || 'day_volume', descending: params.get('descending') !== '0', limit: 100, offset,
  }
  const query = useQuery({ queryKey: ['options', 'eligible-chain', request], queryFn: () => queryEligibleChain(request), enabled: !!catalog?.contract_filters && !invalid, refetchInterval: 60_000 })
  const data = query.data?.data
  const rows = invalid ? [] : data?.rows || []
  const columns = visibleOptionsColumns('chain', params.get('columns'))
  const tone = (value: unknown) => { const numeric = Number(value); return !Number.isFinite(numeric) || numeric === 0 ? '' : numeric > 0 ? 'os-positive' : 'os-negative' }
  function update(key: string, value: string) { const next = new URLSearchParams(window.location.search); if (value === '') next.delete(key); else next.set(key, value); if (key !== 'offset') next.delete('offset'); setParams(next) }
  function cell(row: EligibleChainRow, key: string): ReactNode {
    if (key === 'contract') return <><strong className={row.contract_type === 'CALL' ? 'os-positive' : 'os-negative'}>{row.strike} {row.contract_type}</strong><small>{row.expiration_date}</small><small title={row.contract_ticker}>{row.contract_ticker}</small></>
    if (key === 'market_data_time') return <span title={`Observed ${timestamp(row.first_observed_at)} ET / ${readable(row.mark_source)}`}>{timestamp(row.market_data_time)}</span>
    if (key === 'underlying') return <Link to={`/ticker/${encodeURIComponent(row.underlying)}`} title={`Open ${row.underlying} ticker page`}>{row.underlying}</Link>
    if (key === 'mark_market_data_time' || key === 'first_observed_at') return optionAlertTime(row[key])
    if (key === 'mark_source') return row.mark_source ? readable(row.mark_source) : 'Unavailable'
    if (key === 'quality_flags') return row.quality_flags?.length ? row.quality_flags.map(readable).join(' / ') : 'None recorded'
    if (key === 'bid_ask') return row.bid == null || row.ask == null ? 'Unavailable' : `${optionAlertMoney(row.bid)} / ${optionAlertMoney(row.ask)}`
    const value = row[key as keyof EligibleChainRow]
    if (value == null || value === '' || !Number.isFinite(Number(value))) return 'Unavailable'
    if (key === 'spot') return optionAlertMoney(value)
    if (key === 'model_mark') return <span className="os-option-price">{optionAlertMoney(value)}</span>
    if (key === 'local_iv' || key === 'otm_fraction') return optionAlertPercent(value)
    const formatted = Number(value).toLocaleString('en-US', { maximumFractionDigits: ['day_volume', 'open_interest', 'calendar_dte'].includes(key) ? 0 : key === 'volume_open_interest_ratio' ? 2 : 4 })
    const displayed = key === 'volume_open_interest_ratio' ? `${formatted}x` : formatted
    return ['local_delta', 'local_theta_per_day', 'local_vega_per_vol_point', 'local_rho_per_rate_point'].includes(key)
      ? <span className={tone(value)}>{displayed}</span> : displayed
  }
  return <>
    {!resultsOnly && <><section className="screener-discovery-controls" aria-label="Eligible chain controls">
      <label>Underlying<input aria-label="Chain underlying" maxLength={15} placeholder="All tracked" value={params.get('underlyer') || ''} onChange={event => update('underlyer', event.target.value.toUpperCase())} /></label>
      <label>Type<select aria-label="Chain contract type" value={params.get('type') || 'ALL'} onChange={event => update('type', event.target.value === 'ALL' ? '' : event.target.value)}><option value="ALL">Calls + puts</option><option value="CALL">Calls</option><option value="PUT">Puts</option></select></label>
      <label>Sort<select aria-label="Chain sort" value={request.sort} onChange={event => update('sort', event.target.value)}>{Object.entries(catalog?.contract_filters || {}).map(([key, field]) => <option key={key} value={key}>{field.label}</option>)}</select></label>
      <label>Order<select aria-label="Chain sort order" value={request.descending ? '1' : '0'} onChange={event => update('descending', event.target.value)}><option value="1">Highest first</option><option value="0">Lowest first</option></select></label>
    </section>
    <details className="os-chain-filters" open={filtersOpen} onToggle={event => setFiltersOpen(event.currentTarget.open)}>
      <summary>Contract filters {ranges.length > 0 ? `(${ranges.length})` : ''}</summary>
      <label className="os-filter-search">Find a filter<input type="search" aria-label="Find a contract filter" value={search} onChange={event => setSearch(event.target.value)} /></label>
      <div className="os-filter-grid">{Object.entries(catalog?.contract_filters || {}).filter(([, field]) => field.label.toLowerCase().includes(search.toLowerCase())).map(([key, field]) => <div className="os-range-field" key={key}>
        <strong>{field.label} <small>({field.unit === 'fraction' ? '%' : field.unit})</small></strong><div className="screener-range">
          {(['min', 'max'] as const).map(bound => <input key={bound} inputMode={field.integer ? 'numeric' : 'decimal'} aria-label={`${bound === 'min' ? 'Minimum' : 'Maximum'} ${field.label}`} maxLength={32} placeholder={bound === 'min' ? 'Min' : 'Max'} value={params.get(`range_${key}_${bound}`) || ''} onChange={event => update(`range_${key}_${bound}`, event.target.value)} />)}
        </div>
      </div>)}</div>
      <button type="button" onClick={() => { const next = new URLSearchParams(window.location.search); for (const key of [...next.keys()]) if (key.startsWith('range_') || key === 'offset') next.delete(key); setParams(next) }}><RotateCcw size={14} />Clear ranges</button>
    </details></>}
    <section className="screener-table-panel os-package-panel os-chain-panel">
      <header><div><h2>Eligible retained chain</h2><span>{invalid ? 0 : data?.total ?? 0} matched / {rows.length} shown</span></div><button type="button" title="Export displayed chain" aria-label="Export displayed chain" disabled={!rows.length} onClick={() => download(JSON.stringify({ source: 'ELIGIBLE_RETAINED_CHAIN', policy: query.data?.policy_sha256, request, coverage: data?.coverage, rows }, null, 2), 'eligible-option-chain.json', 'application/json')}><Download size={16} /></button></header>
      {query.isLoading && <p role="status">Loading eligible contracts...</p>}{query.isError && <p role="alert">Eligible-chain data unavailable.</p>}
      {data && <details className="os-chain-coverage"><summary>Source coverage: {data.coverage.length} / {data.requested_underlyers?.length ?? 0} underlyings</summary>
        {!!data.missing_underlyers?.length && <p>Missing active-policy matrix: {data.missing_underlyers.join(', ')}</p>}
        <div className="os-coverage-grid">{data.coverage.map(row => <div key={row.underlying}><strong>{row.underlying}</strong><span>{timestamp(row.market_time)} ET</span><span>{row.eligible} eligible / {row.retained} retained / {row.received} received</span></div>)}</div>
      </details>}
      {!invalid && query.data && !query.data.available && <p role="status">{readable(query.data.reason || 'NO_ELIGIBLE_CHAIN')}</p>}
      <div className="screener-table-wrap" tabIndex={0} role="region" aria-label="Eligible option contracts"><table className="os-package-table"><thead><tr><th className="os-contract-view" scope="col">View</th>{columns.map(column => <th key={column.key} scope="col" title={column.tip}>{column.label}</th>)}</tr></thead><tbody>
        {rows.map(row => <tr key={row.snapshot_id}><td className="os-contract-view"><button type="button" className="sw-icon" title="View retained contract details" aria-label={`View ${row.contract_ticker} details`} onClick={event => { contractTrigger.current = event.currentTarget; setSelectedContract(structuredClone(row)) }}><Eye size={16} /></button></td>{columns.map(column => <td key={column.key}>{cell(row, column.key)}</td>)}</tr>)}
        {!query.isLoading && rows.length === 0 && <tr><td colSpan={columns.length + 1}>{invalid || (query.isError ? 'Data unavailable' : 'No eligible contracts on this page.')}</td></tr>}
      </tbody></table></div>
      <footer><span>Retained policy-filtered chain / delayed model marks</span><div><button type="button" disabled={offset === 0} onClick={() => update('offset', String(Math.max(0, offset - 100)))}>Previous</button><button type="button" disabled={offset + 100 >= (data?.total || 0)} onClick={() => update('offset', String(offset + 100))}>Next</button></div></footer>
    </section>
    {selectedContract && <OptionContractDetail row={selectedContract} close={() => setSelectedContract(null)} />}
  </>
}

export function OptionsPackageResults({ catalog, params, setParams, sessionDate }: ControlsProps & { sessionDate?: string }) {
  const [selected, setSelected] = useState<string | null>(null)
  let invalid = ''
  try { if (catalog) screenQuery(params, catalog) } catch (failure) { invalid = failure instanceof Error ? failure.message : 'Invalid filters' }
  const category = catalog?.categories.find(item => item.id === params.get('category'))?.id
  const args = { session_date: sessionDate, structured_only: true, status: 'SELECTED' as const, category,
    strategy: params.get('model') || undefined, underlyer: params.get('underlyer') || undefined,
    minimum_dte: Number(params.get('min_dte') || 0), maximum_dte: Number(params.get('max_dte') || 60),
    maximum_capital: params.has('max_capital') ? Number(params.get('max_capital')) : undefined,
    sort: (params.get('sort') || 'DEFAULT') as 'DEFAULT' | 'CAPITAL_ASC' | 'DTE_ASC', limit: 100, offset: Number(params.get('offset') || 0) }
  const query = useQuery({ queryKey: ['options', 'package-screen', args], queryFn: () => getOptionCandidates(args), enabled: !!catalog && !invalid, refetchInterval: 60_000 })
  const detail = useQuery({ queryKey: ['options', 'candidate', selected], queryFn: () => getOptionCandidate(selected!), enabled: !!selected })
  const rows = query.data?.data.rows || []
  const columns = columnsForView('packages').filter(column => column.locked || !params.has('columns') || params.get('columns')!.split(',').includes(column.key))
  const value = (row: OptionCandidateRow, key: string): ReactNode => {
    if (key === 'legs') return row.legs.map(leg => <div key={leg.leg_index}>{leg.side} {leg.ratio} {leg.contract_ticker}<small>Multiplier {leg.multiplier} / mark {money(leg.model_mark)}</small></div>)
    if (key === 'structure_type') return <><strong>{readable(row.structure_type)}</strong><small>{row.expiration_date}</small></>
    if (key === 'eligibility') return <span title={row.reason_codes.map(readable).join('; ')}>{row.execution_eligibility || 'Not eligible'}</span>
    if (key === 'net_premium') return row.net_premium == null ? 'Unavailable' : `${money(Math.abs(Number(row.net_premium)))} ${Number(row.net_premium) >= 0 ? 'credit' : 'debit'}`
    if (['capital_at_risk', 'collateral_required', 'maximum_profit', 'maximum_loss'].includes(key)) return money(row[key as keyof OptionCandidateRow] as string | null)
    if (key === 'breakevens') return row.breakevens.map(amount => money(amount)).join(', ') || 'Unavailable'
    if (key === 'market_data_time') return timestamp(row.market_data_time)
    if (key === 'structure_risk_class') return readable(row.structure_risk_class)
    return String(row[key as keyof OptionCandidateRow] ?? 'Unavailable')
  }
  const page = (offset: number) => { const next = new URLSearchParams(params); next.set('offset', String(offset)); setParams(next) }
  return <section className="screener-table-panel os-package-panel">
    <header><div><h2>Complete strategy packages</h2><span>{query.data?.data.total ?? 0} matched / {rows.length} shown</span></div><button type="button" title="Export displayed packages" aria-label="Export displayed packages" disabled={!rows.length} onClick={() => download(JSON.stringify({ source: 'DELAYED_RESEARCH', as_of: query.data?.as_of, filters: args, rows }, null, 2), 'option-packages.json', 'application/json')}><Download size={16} /></button></header>
    {query.isLoading && <p role="status">Loading packages...</p>}{query.isError && <p role="alert">Package data unavailable.</p>}
    {query.data?.data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && rows.length > 0 && <p role="status">Historical previous-policy evidence</p>}
    <div className="screener-table-wrap"><table className="os-package-table"><thead><tr>{columns.map(column => <th key={column.key}>{column.label}</th>)}<th>Evidence</th></tr></thead><tbody>
      {!invalid && rows.map(row => <tr key={row.candidate_id}>{columns.map(column => <td key={column.key}>{value(row, column.key)}</td>)}<td><button type="button" title={`Inspect ${row.underlying} package`} aria-label={`Inspect package ${row.candidate_id}`} onClick={() => setSelected(row.candidate_id)}><Eye size={16} /></button></td></tr>)}
      {!query.isLoading && !rows.length && <tr><td colSpan={columns.length + 1}>{invalid || 'No complete packages match this screen.'}</td></tr>}
    </tbody></table></div>
    <footer><span>Delayed model marks / not executable quotes</span><div><button type="button" disabled={args.offset === 0} onClick={() => page(Math.max(0, args.offset - 100))}>Previous</button><button type="button" disabled={args.offset + rows.length >= (query.data?.data.total || 0)} onClick={() => page(args.offset + 100)}>Next</button></div></footer>
    {selected && <Modal title="Package evidence" close={() => setSelected(null)} wide><div className="os-library-content os-package-detail">
      {detail.isLoading && <p>Loading evidence...</p>}{detail.isError && <p role="alert">Evidence unavailable.</p>}
      {detail.data?.data && <><h3>{detail.data.data.candidate.underlying} / {readable(detail.data.data.candidate.structure_type)}</h3>
        <p>Source {timestamp(detail.data.data.candidate.market_data_time)} ET / observed {timestamp(detail.data.data.candidate.observed_time)} ET</p>
        {detail.data.data.legs.map(leg => <p key={leg.leg_index}>{leg.side} {leg.ratio} {leg.contract_ticker} / multiplier {leg.multiplier} / mark {money(leg.model_mark)}</p>)}
        <h3>Execution gates</h3>{detail.data.data.execution_gates.map(gate => <p key={gate.gate_name}><strong>{readable(gate.gate_name)}: {gate.verdict}</strong> {gate.reason_codes.map(readable).join('; ')}</p>)}
        <h3>Context</h3><p>Trend: {detail.data.data.candidate.trend_state || 'Unavailable'} / earnings: {detail.data.data.candidate.earnings_blackout_state || 'Unavailable'} / FOMC: {detail.data.data.candidate.fed_blackout_state || 'Unavailable'}</p>
        <h3>Spot / IV / time scenarios</h3><div className="screener-table-wrap"><table className="os-package-table"><thead><tr><th>Spot shock</th><th>IV shock</th><th>Time remaining</th><th>Modeled P/L</th></tr></thead><tbody>{detail.data.data.scenarios.map(scenario => <tr key={scenario.scenario_result_id}><td>{(scenario.spot_shock_fraction * 100).toFixed(1)}%</td><td>{(scenario.iv_shock_fraction * 100).toFixed(1)}%</td><td>{(scenario.time_fraction_remaining * 100).toFixed(1)}%</td><td>{money(scenario.profit_loss)}</td></tr>)}</tbody></table></div>
        <h3>Management policy</h3>{Object.entries(detail.data.data.candidate.management_policy).map(([key, item]) => <p key={key}>{readable(key)}: {typeof item === 'object' ? JSON.stringify(item) : String(item)}</p>)}
        <p>Candidate {selected}</p><p>Policy {detail.data.data.candidate.policy_sha256}</p></>}
    </div></Modal>}
  </section>
}