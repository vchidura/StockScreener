import { useEffect, useRef, useState, type ReactNode, type KeyboardEvent } from 'react'
import { ArrowDown, ArrowUp, Check, ChevronDown, Copy, Download, FolderOpen, Pencil, Pin, PinOff, Plus, Save, Search, Trash2, Upload, X } from 'lucide-react'
import { canonicalScreenTab, downloadText, exportDefinitions, filterLabel, getBuiltInScreens, importDefinitions, MAX_IMPORT_BYTES, type SavedScreen, type ScreeningCatalog } from './screeningModel'
import type { ScreenLibraryState } from './useScreenLibrary'

export function Modal({ title, close, children, wide = false }: { title: string; close: () => void; children: ReactNode; wide?: boolean }) {
  const dialog = useRef<HTMLDialogElement>(null)
  useEffect(() => { dialog.current?.showModal() }, [])
  return <dialog ref={dialog} className={`sw-modal ${wide ? 'sw-library-modal' : ''}`} aria-label={title} onCancel={event => { event.preventDefault(); event.stopPropagation(); close() }} onKeyDown={event => { if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close() } }}><header><h2>{title}</h2><button type="button" className="sw-icon" aria-label={`Close ${title}`} title="Close" onClick={close}><X size={16} /></button></header>{children}</dialog>
}

export default function ScreenLibrary({ library, catalog, filterControl, tools, onNewScreenStarted }: { library: ScreenLibraryState; catalog?: ScreeningCatalog; filterControl: ReactNode; tools: ReactNode; onNewScreenStarted: () => void }) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [category, setCategory] = useState('BUILTIN')
  const [selected, setSelected] = useState<string | null>(null)
  const [naming, setNaming] = useState<{ action: 'RENAME' | 'DUPLICATE'; screen: SavedScreen } | null>(null)
  const [name, setName] = useState('')
  const [pendingName, setPendingName] = useState('')
  const [deleting, setDeleting] = useState<SavedScreen | null>(null)
  const [imported, setImported] = useState<SavedScreen[] | null>(null)
  const file = useRef<HTMLInputElement>(null)
  const templates = getBuiltInScreens(catalog)
  const choices = [
    ...library.workspace.screens.map(screen => ({ key: `saved:${screen.id}`, name: screen.name, type: 'SAVED', detail: `Revision ${screen.predicate_revision}`, summary: '', qualification: '', unavailable: null as string | null, predicate: screen.predicate, columns: screen.columns, sort: screen.sort, saved: screen })),
    ...templates.map(screen => ({ key: `builtin:${screen.id}`, name: screen.name, type: 'BUILTIN', detail: screen.group === 'DEFAULT' ? 'Default' : 'Recipe', summary: screen.summary, qualification: screen.qualification, unavailable: screen.unavailable, predicate: screen.predicate, columns: screen.columns, sort: screen.sort, saved: undefined })),
  ]
  const visible = choices.filter(screen => category === screen.type && `${screen.name} ${screen.summary}`.toLowerCase().includes(search.trim().toLowerCase()))
  const selectedChoice = visible.find(screen => screen.key === selected)
  const tabs = library.workspace.tabs.filter(key => canonicalScreenTab(key) !== null).map(key => choices.find(screen => screen.key === key)).filter((screen): screen is typeof choices[number] => !!screen)
  const shown = tabs.slice(0, 4)
  const activeTab = tabs.find(screen => screen.key === library.workspace.active_tab)
  if (activeTab && !shown.some(screen => screen.key === activeTab.key)) shown[shown.length - 1] = activeTab
  const overflow = tabs.filter(screen => !shown.some(item => item.key === screen.key))
  const activeName = library.active?.name || library.builtIn?.name || library.defaultName
  const activeKind = library.active ? `Saved / revision ${library.active.predicate_revision}` : library.builtIn ? 'Built-in' : 'Unsaved'
  const hasTab = (key: string | null) => canonicalScreenTab(key) === null || library.workspace.tabs.includes(key!)
  function showPicker() { setSelected(library.workspace.active_tab || 'builtin:all'); setSearch(''); setCategory(library.active ? 'SAVED' : 'BUILTIN'); setOpen(true) }
  useEffect(() => { if (library.pending) setPendingName(library.active?.name || library.defaultName) }, [library.pending])
  const nameDialog = (action: NonNullable<typeof naming>['action'], screen?: SavedScreen) => {
    if (!screen) return
    setName(action === 'DUPLICATE' ? `${screen.name.slice(0, 73)} (copy)` : screen.name)
    setNaming({ action, screen })
  }
  const exportScreens = (screens: SavedScreen[]) => downloadText(exportDefinitions(screens), 'stock-screen-definitions.json', 'application/json')
  async function submitName() {
    if (!naming) return
    const success = naming.action === 'RENAME' ? library.rename(naming.screen.id, name) : await library.duplicate(naming.screen, name)
    if (success) setNaming(null)
  }
  async function readImport(selected: File | undefined) {
    if (!selected || !catalog) return
    try {
      if (selected.size > MAX_IMPORT_BYTES) throw new Error('Import exceeds 256 KiB')
      const screens = await importDefinitions(await selected.text(), catalog)
      setImported(screens)
    } catch (failure) { library.setError(`Import: ${failure instanceof Error ? failure.message : 'invalid file'}`) }
    finally { if (file.current) file.current.value = '' }
  }
  function openSelected() {
    if (!selectedChoice?.predicate || selectedChoice.unavailable) return
    library.activateTab(selectedChoice.key)
    setOpen(false)
  }
  function startNew() {
    library.startNewScreen(() => {
      setOpen(false)
      onNewScreenStarted()
    })
  }
  function tabKey(event: KeyboardEvent<HTMLButtonElement>) {
    const elements = Array.from(event.currentTarget.closest('[role=tablist]')!.querySelectorAll<HTMLButtonElement>('[role=tab]'))
    const index = elements.indexOf(event.currentTarget)
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? elements.length - 1 : event.key === 'ArrowRight' ? (index + 1) % elements.length : event.key === 'ArrowLeft' ? (index + elements.length - 1) % elements.length : null
    if (next !== null) { event.preventDefault(); elements[next].focus(); elements[next].click() }
  }

  return <>
    <section className="sw-toolbar" aria-label="Screen controls"><div className="sw-screen-navigation">{filterControl}<button type="button" className="sw-screen-picker" aria-label="Choose screener" title={`${activeName} / ${activeKind}${library.dirty ? ' / edited' : ''}`} aria-haspopup="dialog" aria-expanded={open} onClick={showPicker}><FolderOpen size={18} /><span>Screen library</span><ChevronDown size={14} /></button>
    <div className="sw-screen-tabs">
      <nav className="sw-desktop-pins" role="tablist" aria-label="Open screeners"><button type="button" role="tab" aria-selected={!library.workspace.active_tab} aria-controls="sw-result-panel" tabIndex={!library.workspace.active_tab ? 0 : -1} onKeyDown={tabKey} onClick={() => library.open(null)}>All equities{!library.workspace.active_tab && library.dirty ? ' *' : ''}</button>{shown.map(screen => <span key={screen.key}><button type="button" role="tab" className="sw-pin-name" aria-selected={library.workspace.active_tab === screen.key} aria-controls="sw-result-panel" tabIndex={library.workspace.active_tab === screen.key ? 0 : -1} onKeyDown={tabKey} title={screen.name} onClick={() => library.activateTab(screen.key)}>{screen.name}{library.workspace.active_tab === screen.key && library.dirty ? ' *' : ''}</button><button type="button" className="sw-unpin" title={`Close ${screen.name} tab`} aria-label={`Close ${screen.name} tab`} onClick={() => library.closeTab(screen.key)}><X size={12} /></button></span>)}{overflow.length > 0 && <select aria-label="More open screeners" value="" onChange={event => event.target.value && library.activateTab(event.target.value)}><option value="">More ({overflow.length})</option>{overflow.map(screen => <option key={screen.key} value={screen.key}>{screen.name}</option>)}</select>}</nav>
      <div className="sw-mobile-picker"><select aria-label="Open screeners" value={library.workspace.active_tab || ''} onChange={event => library.activateTab(event.target.value || null)}><option value="">All equities</option>{tabs.map(screen => <option key={screen.key} value={screen.key}>{screen.name}</option>)}</select>{library.workspace.active_tab && <button type="button" className="sw-icon" aria-label="Close active screener tab" title="Close active tab" onClick={() => library.closeTab(library.workspace.active_tab!)}><X size={15} /></button>}<button type="button" className="sw-icon" aria-label="Choose another screener" title="Choose screener" onClick={showPicker}><Plus size={16} /></button></div>
    </div>
    </div><div className="sw-tools">{tools}</div></section>
    {open && <Modal title="Screen library" close={() => setOpen(false)} wide><div className="sw-library-toolbar"><label className="sw-library-search"><Search size={16} /><input aria-label="Search screens" type="search" placeholder="Find a screener" value={search} onChange={event => setSearch(event.target.value)} /></label><button type="button" className="sw-icon" title="Import saved screen rules and columns from JSON" aria-label="Import definitions" onClick={() => file.current?.click()}><Upload size={16} /></button><button type="button" className="sw-icon" title="Export saved screen rules and columns as JSON" aria-label="Export library definitions" disabled={!library.workspace.screens.length} onClick={() => exportScreens(library.workspace.screens)}><Download size={16} /></button><input ref={file} type="file" accept="application/json,.json" hidden onChange={event => void readImport(event.target.files?.[0])} /></div>
      <div className="sw-library-modes" role="group" aria-label="Screen collection">{[['BUILTIN', 'Built-in screens'], ['SAVED', 'My saved screens']].map(([key, label]) => <button type="button" key={key} aria-pressed={category === key} onClick={() => setCategory(key)}>{label}</button>)}</div>
      {library.error && <p className="sw-error" role="alert">{library.error}</p>}
      <div className="sw-library-browser"><div className="sw-choice-list" aria-label="Screen choices">{visible.map(screen => <button type="button" key={screen.key} className="sw-choice" aria-pressed={selected === screen.key} aria-label={`Preview ${screen.name}`} onClick={() => setSelected(screen.key)}><span><strong>{screen.name}</strong><small>{screen.type === 'SAVED' ? 'Saved' : screen.detail} / Daily{screen.unavailable ? ' / unavailable' : ''}</small></span>{hasTab(screen.key) ? <Check size={14} aria-label="Open tab" /> : null}</button>)}{!visible.length && <p className="sw-library-empty">No matching screeners</p>}</div>
        <section className="sw-choice-detail" aria-label="Selected screener">{selectedChoice ? <><div className="sw-choice-title"><span className="sw-choice-kind">{selectedChoice.type === 'SAVED' ? 'Your screen' : 'Built-in'}</span><h3>{selectedChoice.name}</h3><p>{selectedChoice.summary || `Daily / ${selectedChoice.detail}`}</p></div>{selectedChoice.unavailable && <p className="sw-warning">{selectedChoice.unavailable}</p>}
          <ul className="sw-rule-list">{selectedChoice.predicate?.filters.map(filter => <li key={filter.field}>{catalog?.fields[filter.field] ? filterLabel(filter, catalog) : filter.field}</li>)}{selectedChoice.predicate?.patterns.map(pattern => <li key={pattern.id}>{selectedChoice.predicate?.pattern_mode}: {catalog?.patterns[pattern.id]?.label || pattern.id}</li>)}{selectedChoice.predicate?.gap && <li>Daily gap (same episode, 0-20 sessions): {selectedChoice.predicate.gap.filters.length ? selectedChoice.predicate.gap.filters.map(filter => catalog?.gaps ? filterLabel(filter, { ...catalog, fields: catalog.gaps.fields }) : filter.field).join('; ') : 'Any'}</li>}{selectedChoice.predicate && !selectedChoice.predicate.filters.length && !selectedChoice.predicate.patterns.length && !selectedChoice.predicate.gap && !selectedChoice.predicate.hourly && <li>All eligible common stocks, ETFs and ETVs</li>}</ul>
          {selectedChoice.predicate?.hourly && <p>Hourly context (1h): {selectedChoice.predicate.hourly.filters.map(filter => catalog?.hourly ? filterLabel(filter, { ...catalog, fields: catalog.hourly.fields }) : filter.field).join('; ')}</p>}
          <dl className="sw-screen-facts"><div><dt>Sort</dt><dd>{catalog?.fields[selectedChoice.sort.field]?.label || 'Stock'} / {selectedChoice.sort.descending ? 'descending' : 'ascending'}</dd></div><div><dt>Time</dt><dd>Latest complete daily session{selectedChoice.predicate?.hourly && ' / frozen hourly context'}</dd></div></dl>{selectedChoice.qualification && <p className="sw-recipe-note">{selectedChoice.qualification}</p>}
          {selectedChoice.saved && <div className="sw-row-tools" aria-label="Manage selected screen"><button type="button" className="sw-icon" title="Pin or unpin" aria-label={`${library.workspace.pins.includes(selectedChoice.saved.id) ? 'Unpin' : 'Pin'} selected screen`} onClick={() => library.pin(selectedChoice.saved!.id)}>{library.workspace.pins.includes(selectedChoice.saved.id) ? <PinOff size={15} /> : <Pin size={15} />}</button><button type="button" className="sw-icon" title="Rename screen" aria-label="Rename selected screen" onClick={() => nameDialog('RENAME', selectedChoice.saved)}><Pencil size={15} /></button><button type="button" className="sw-icon" title="Duplicate screen" aria-label="Duplicate selected screen" onClick={() => nameDialog('DUPLICATE', selectedChoice.saved)}><Copy size={15} /></button><button type="button" className="sw-icon" title="Export definition" aria-label="Export selected screen" onClick={() => exportScreens([selectedChoice.saved!])}><Download size={15} /></button><button type="button" className="sw-icon" title="Delete local definition" aria-label="Delete selected screen" onClick={() => setDeleting(selectedChoice.saved!)}><Trash2 size={15} /></button>{library.workspace.pins.includes(selectedChoice.saved.id) && <><button type="button" className="sw-icon" title="Move pin earlier" aria-label="Move pin earlier" disabled={library.workspace.pins.indexOf(selectedChoice.saved.id) === 0} onClick={() => library.movePin(selectedChoice.saved!.id, -1)}><ArrowUp size={15} /></button><button type="button" className="sw-icon" title="Move pin later" aria-label="Move pin later" disabled={library.workspace.pins.indexOf(selectedChoice.saved.id) === library.workspace.pins.length - 1} onClick={() => library.movePin(selectedChoice.saved!.id, 1)}><ArrowDown size={15} /></button></>}</div>}
        </> : <p className="sw-library-empty">Select a screener</p>}</section>
      </div><footer className="sw-library-footer"><small>Browser-local / {window.location.host}</small>{category === 'SAVED' && <button type="button" disabled={!library.ready || !library.writable || library.busy} onClick={startNew}><Plus size={15} />New screen</button>}<button type="button" className="sw-primary" disabled={!library.ready || !library.writable || !selectedChoice?.predicate || !!selectedChoice?.unavailable} onClick={openSelected}><FolderOpen size={15} />{selectedChoice && hasTab(selectedChoice.key) ? 'Go to tab' : 'Open tab'}</button></footer>
    </Modal>}
    {naming && <Modal title={naming.action === 'RENAME' ? 'Rename screen' : 'Duplicate screen'} close={() => setNaming(null)}><form onSubmit={event => { event.preventDefault(); void submitName() }}><label>Screen name<input autoFocus required maxLength={80} value={name} onChange={event => setName(event.target.value)} /></label>{library.error && <p role="alert" className="sw-error">{library.error}</p>}<footer><button type="button" onClick={() => setNaming(null)}>Cancel</button><button type="submit" disabled={library.busy || !name.trim()}><Save size={14} />Save</button></footer></form></Modal>}
    {deleting && <Modal title="Delete local screen" close={() => setDeleting(null)}><p>Delete &quot;{deleting.name}&quot; from this browser? Published snapshots and market data will remain unchanged.</p><footer><button type="button" onClick={() => setDeleting(null)}>Cancel</button><button type="button" onClick={() => { library.remove(deleting.id); setDeleting(null) }}><Trash2 size={14} />Delete</button></footer></Modal>}
    {imported && <Modal title="Import screen definitions" close={() => setImported(null)}><p>{imported.length} definitions / {imported.filter(screen => library.workspace.screens.some(existing => existing.id === screen.id)).length} existing IDs</p><footer><button type="button" onClick={() => setImported(null)}>Cancel</button><button type="button" onClick={() => { library.importScreens(imported, 'COPY'); setImported(null) }}>Create copies</button><button type="button" onClick={() => { library.importScreens(imported, 'REPLACE'); setImported(null) }}>Replace conflicting IDs</button></footer></Modal>}
    {library.pending && <Modal title="Unsaved filter changes" close={() => void library.finishPending('CANCEL', pendingName)}><p>Save changes to {library.active?.name || library.defaultName} before leaving?</p><label>Screen name<input maxLength={80} value={pendingName} onChange={event => setPendingName(event.target.value)} /></label>{library.error && <p role="alert" className="sw-error">{library.error}</p>}<footer><button type="button" onClick={() => void library.finishPending('CANCEL', pendingName)}>Cancel</button><button type="button" onClick={() => void library.finishPending('DISCARD', pendingName)}>Discard</button><button type="button" disabled={library.busy || !library.writable || !pendingName.trim()} onClick={() => void library.finishPending('SAVE_AS', pendingName)}><Copy size={14} />Save as</button><button type="button" disabled={library.busy || !library.writable || !pendingName.trim()} onClick={() => void library.finishPending('SAVE', pendingName)}><Save size={14} />Save</button></footer></Modal>}
  </>
}