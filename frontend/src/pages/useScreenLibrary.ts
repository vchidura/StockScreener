import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { appliedScreeningColumns, canonicalScreenTab, closeScreenTab, compileDraft, DEFAULT_COLUMNS, DRAFT_KEY, emptyPredicate, emptyWorkspace, getBuiltInScreens, makeScreen, MAX_IMPORT_BYTES, mergeDefinitions, openScreenTab, readWorkspace, ruleIdentity, toDraft, validateGapDraft, validateHourlyDraft, validatePredicate, writeWorkspace,
  type Draft, type Predicate, type SavedScreen, type ScreeningCatalog, type ScreeningRequest, type ScreenWorkspace, type ScreenLayout } from './screeningModel'

type Inputs = {
  catalog?: ScreeningCatalog; draft: Draft; request: ScreeningRequest; columns: string[]
  setDraft: Dispatch<SetStateAction<Draft>>; setRequest: Dispatch<SetStateAction<ScreeningRequest>>; setColumns: Dispatch<SetStateAction<string[]>>
}

export function useScreenLibrary({ catalog, draft, request, columns, setDraft, setRequest, setColumns }: Inputs) {
  const [workspace, setWorkspace] = useState<ScreenWorkspace>(emptyWorkspace)
  const current = useRef(workspace)
  const hydrated = useRef(false)
  const [ready, setReady] = useState(false)
  const [writable, setWritable] = useState(false)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [recovered, setRecovered] = useState(false)
  const [pending, setPending] = useState<(() => void) | null>(null)
  const [defaultName, setDefaultName] = useState('All equities')
  const [templateId, setTemplateId] = useState<string | null>(null)
  const navigate = useNavigate()
  const location = useLocation()
  const active = workspace.screens.find(screen => screen.id === workspace.active_id)
  const builtIn = templateId ? getBuiltInScreens(catalog).find(screen => screen.id === templateId) : undefined
  const compiled = catalog ? compileDraft(draft, catalog) : null
  const baseline = active?.predicate || builtIn?.predicate || emptyPredicate()
  const dirty = ready && !!compiled && (Object.keys(compiled.errors).length > 0 || ruleIdentity(compiled.predicate) !== ruleIdentity(baseline) || ruleIdentity(request.predicate) !== ruleIdentity(baseline))

  function commit(next: ScreenWorkspace): boolean {
    if (!writable) { setError('Browser storage is unavailable. Existing definitions were not overwritten.'); return false }
    try {
      writeWorkspace(localStorage, next)
      current.current = next
      setWorkspace(next)
      setError('')
      return true
    } catch (failure) { setError(`Browser storage: ${failure instanceof Error ? failure.message : 'write failed'}. Changes were not saved.`); return false }
  }
  function load(screen?: SavedScreen, predicate = screen?.predicate || emptyPredicate(), name = 'All equities', layout?: ScreenLayout, selectedTemplate: string | null = null) {
    if (!catalog) return
    const sort = screen?.sort || layout?.sort || { field: 'ticker', descending: false }
    setDraft(toDraft(predicate, catalog))
    setRequest(previous => ({ ...previous, predicate, generation: null, offset: 0, sort: sort.field, descending: sort.descending, view: 'CURRENT', new_only: false }))
    setColumns([...(screen?.columns || layout?.columns || DEFAULT_COLUMNS)])
    setDefaultName(screen?.name || name)
    setTemplateId(selectedTemplate)
    setRecovered(false)
  }
  function guard(action: () => void) { if (dirty) setPending(() => action); else action() }
  function loadTab(next: ScreenWorkspace) {
    const selected = next.active_tab?.startsWith('builtin:') ? getBuiltInScreens(catalog).find(screen => `builtin:${screen.id}` === next.active_tab) : undefined
    if (selected?.predicate && !selected.unavailable) load(undefined, selected.predicate, selected.name, { ...selected, columns: next.builtin_columns?.[selected.id] || selected.columns }, selected.id)
    else if (next.active_id) load(next.screens.find(screen => screen.id === next.active_id))
    else load(undefined, emptyPredicate(), 'All equities', { columns: next.builtin_columns?.all || DEFAULT_COLUMNS, sort: { field: 'ticker', descending: false } })
  }
  function activateTab(key: string | null) {
    key = canonicalScreenTab(key)
    if (key === current.current.active_tab) return
    const selected = key?.startsWith('builtin:') ? getBuiltInScreens(catalog).find(screen => `builtin:${screen.id}` === key) : undefined
    if (selected?.unavailable) { setError(selected.unavailable); return }
    guard(() => { const next = openScreenTab(current.current, key); if (commit(next)) loadTab(next) })
  }
  function open(id: string | null) {
    if (id === null && current.current.active_tab === null && dirty) guard(() => load())
    else activateTab(id ? `saved:${id}` : null)
  }
  function startNewScreen(onStarted: () => void) {
    if (!catalog || !ready || !writable || busy) return
    guard(() => {
      const next = openScreenTab(current.current, null)
      if (!commit(next)) return
      load()
      onStarted()
    })
  }
  function template(_name: string, _predicate: Predicate, _layout?: ScreenLayout, selectedTemplate?: string) {
    if (selectedTemplate) activateTab(`builtin:${selectedTemplate}`)
  }
  function closeTab(key: string) {
    const action = () => {
      const wasActive = current.current.active_tab === key
      const next = closeScreenTab(current.current, key)
      if (commit(next) && wasActive) loadTab(next)
    }
    if (current.current.active_tab === key) guard(action); else action()
  }
  async function save(name: string, copy = false): Promise<boolean> {
    if (!catalog || !compiled || Object.keys(compiled.errors).length) { setError('Correct invalid filters before saving.'); return false }
    setBusy(true)
    try {
      const nextColumns = appliedScreeningColumns(columns, compiled.predicate)
      const saved = await makeScreen(name, compiled.predicate, nextColumns, { field: request.sort, descending: request.descending }, copy ? undefined : active)
      const next = openScreenTab({ ...current.current, screens: [...current.current.screens.filter(screen => screen.id !== saved.id), saved] }, `saved:${saved.id}`)
      if (next.screens.length > 100) throw new Error('Browser library is limited to 100 screens')
      if (!commit(next)) return false
      setColumns(nextColumns)
      setRequest(previous => ({ ...previous, predicate: saved.predicate, offset: 0, new_only: saved.predicate.hourly ? false : previous.new_only }))
      setDraft(toDraft(saved.predicate, catalog))
      setTemplateId(null)
      setRecovered(false)
      return true
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Save failed'); return false }
    finally { setBusy(false) }
  }
  async function finishPending(choice: 'SAVE' | 'SAVE_AS' | 'DISCARD' | 'CANCEL', name: string) {
    if (choice === 'CANCEL') { setPending(null); return }
    if ((choice === 'SAVE' || choice === 'SAVE_AS') && !await save(name, choice === 'SAVE_AS')) return
    if (choice === 'DISCARD') {
      try { localStorage.removeItem(DRAFT_KEY) } catch { setError('Browser storage could not remove the unsaved draft.'); return }
    }
    const action = pending
    setPending(null)
    action?.()
  }
  function discardDraft() {
    try {
      localStorage.removeItem(DRAFT_KEY)
      loadTab(current.current)
    } catch { setError('Browser storage could not remove the unsaved draft.') }
  }
  function pin(id: string) {
    const pins = current.current.pins.includes(id) ? current.current.pins.filter(value => value !== id) : [...current.current.pins, id]
    const key = `saved:${id}`
    const tabs = pins.includes(id) && !current.current.tabs.includes(key) ? [...current.current.tabs, key] : current.current.tabs
    const pinnedTabs = pins.map(value => `saved:${value}`)
    commit({ ...current.current, pins, tabs: [...pinnedTabs, ...tabs.filter(tab => !pinnedTabs.includes(tab))] })
  }
  function movePin(id: string, direction: number) {
    const pins = [...current.current.pins]
    const index = pins.indexOf(id)
    if (index < 0 || index + direction < 0 || index + direction >= pins.length) return
    ;[pins[index], pins[index + direction]] = [pins[index + direction], pins[index]]
    const pinnedTabs = pins.map(value => `saved:${value}`)
    commit({ ...current.current, pins, tabs: [...pinnedTabs, ...current.current.tabs.filter(tab => !pinnedTabs.includes(tab))] })
  }
  function rename(id: string, name: string) {
    if (!name.trim() || name.length > 80) { setError('Screen name must contain 1-80 characters'); return false }
    return commit({ ...current.current, screens: current.current.screens.map(screen => screen.id === id ? { ...screen, name: name.trim(), updated_at: new Date().toISOString() } : screen) })
  }
  async function duplicate(screen: SavedScreen, name: string) {
    try {
      const copy = await makeScreen(name, screen.predicate, screen.columns, screen.sort)
      if (current.current.screens.length >= 100) throw new Error('Browser library is limited to 100 screens')
      return commit({ ...current.current, screens: [...current.current.screens, copy] })
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Duplicate failed'); return false }
  }
  function remove(id: string) {
    const action = () => {
      const wasActive = current.current.active_id === id
      const closed = closeScreenTab(current.current, `saved:${id}`)
      const next = { ...closed, screens: closed.screens.filter(screen => screen.id !== id) }
      if (commit(next) && wasActive) loadTab(next)
    }
    if (active?.id === id) guard(action); else action()
  }
  function importScreens(screens: SavedScreen[], conflict: 'REPLACE' | 'COPY') {
    guard(() => {
      try {
        const next = mergeDefinitions(current.current, screens, conflict)
        if (commit(next) && conflict === 'REPLACE' && screens.some(screen => screen.id === next.active_id)) load(next.screens.find(screen => screen.id === next.active_id))
      } catch (failure) { setError(failure instanceof Error ? failure.message : 'Import failed') }
    })
  }

  useEffect(() => {
    if (!catalog || hydrated.current) return
    hydrated.current = true
    void (async () => {
      try {
        const stored = await readWorkspace(localStorage, catalog)
        current.current = stored
        setWorkspace(stored)
        loadTab(stored)
        const raw = localStorage.getItem(DRAFT_KEY)
        if (raw) {
          if (new TextEncoder().encode(raw).length > MAX_IMPORT_BYTES) throw new Error('Draft recovery record exceeds size limit')
          const recovery = JSON.parse(raw)
          if (recovery.schema_version !== 1 || recovery.active_id !== stored.active_id || !recovery.draft || !Array.isArray(recovery.draft.patterns) || !['ANY', 'ALL', 'NONE'].includes(recovery.draft.pattern_mode) || !recovery.draft.fields || typeof recovery.draft.fields !== 'object') throw new Error('Incompatible unsaved draft record')
          if (Object.keys(recovery.draft.fields).length > 30 || recovery.draft.patterns.length > 10) throw new Error('Draft recovery exceeds field limits')
          for (const [field, input] of Object.entries(recovery.draft.fields)) {
            const value = input as Draft['fields'][string]
            if (!Object.prototype.hasOwnProperty.call(catalog.fields, field) || !value || typeof value.min !== 'string' || typeof value.max !== 'string' || value.min.length > 64 || value.max.length > 64 || !Array.isArray(value.values) || value.values.length > 20 || value.values.some(item => !catalog.fields[field].options.includes(item))) throw new Error('Invalid unsaved draft field')
          }
          if (recovery.draft.patterns.some((id: unknown) => typeof id !== 'string' || !Object.prototype.hasOwnProperty.call(catalog.patterns, id))) throw new Error('Unsupported unsaved pattern')
          if (recovery.draft.gap != null) validateGapDraft(recovery.draft.gap, catalog)
          if (recovery.draft.hourly != null) validateHourlyDraft(recovery.draft.hourly, catalog)
          const applied = validatePredicate(recovery.applied, catalog)
          const restoredTemplate = recovery.template_id == null ? undefined : getBuiltInScreens(catalog).find(screen => screen.id === recovery.template_id)
          if (recovery.template_id != null && (!restoredTemplate?.predicate || restoredTemplate.unavailable || stored.active_id !== null)) throw new Error('Unsupported recovered default screener')
          const restoredBaseline = stored.screens.find(screen => screen.id === stored.active_id)?.predicate || restoredTemplate?.predicate || emptyPredicate()
          const restoredDraft = compileDraft(recovery.draft, catalog)
          setDraft(recovery.draft)
          setRequest(previous => ({ ...previous, predicate: applied, generation: null, offset: 0, sort: restoredTemplate?.sort.field || previous.sort, descending: restoredTemplate?.sort.descending ?? previous.descending }))
          if (restoredTemplate) {
            setTemplateId(restoredTemplate.id === 'all' ? null : restoredTemplate.id)
            setDefaultName(restoredTemplate.name)
            setColumns(stored.builtin_columns?.[restoredTemplate.id] || restoredTemplate.columns)
            const migrated = openScreenTab(stored, `builtin:${restoredTemplate.id}`)
            current.current = migrated
            setWorkspace(migrated)
          }
          setRecovered(Object.keys(restoredDraft.errors).length > 0 || ruleIdentity(restoredDraft.predicate) !== ruleIdentity(restoredBaseline) || ruleIdentity(applied) !== ruleIdentity(restoredBaseline))
        }
        setWritable(true)
      } catch (failure) { setError(`Browser recovery: ${failure instanceof Error ? failure.message : 'unavailable'}. Stored definitions were not overwritten.`) }
      finally { setReady(true) }
    })()
  }, [catalog])

  useEffect(() => {
    if (!ready || !writable) return
    try {
      if (dirty || templateId) localStorage.setItem(DRAFT_KEY, JSON.stringify({ schema_version: 1, active_id: workspace.active_id, template_id: templateId, draft, applied: request.predicate }))
      else localStorage.removeItem(DRAFT_KEY)
    } catch { setError('Unsaved draft recovery could not be written to browser storage.') }
  }, [ready, writable, dirty, draft, request.predicate, workspace.active_id, templateId])

  useEffect(() => {
    if (!ready || !writable) return
    if (active) {
      if (JSON.stringify([active.columns, active.sort]) === JSON.stringify([columns, { field: request.sort, descending: request.descending }])) return
      commit({ ...current.current, screens: current.current.screens.map(screen => screen.id === active.id ? { ...screen, columns: [...columns], sort: { field: request.sort, descending: request.descending }, updated_at: new Date().toISOString() } : screen) })
    } else {
      const id = templateId || 'all'
      if (JSON.stringify(current.current.builtin_columns?.[id]) === JSON.stringify(columns)) return
      commit({ ...current.current, builtin_columns: { ...current.current.builtin_columns, [id]: [...columns] }, builtin_columns_version: 2 })
    }
  }, [ready, writable, active?.id, templateId, columns, request.sort, request.descending])

  useEffect(() => {
    if (!dirty) return
    const beforeUnload = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = '' }
    const click = (event: MouseEvent) => {
      if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
      const anchor = (event.target as Element).closest('a[href]') as HTMLAnchorElement | null
      if (!anchor || anchor.target === '_blank' || anchor.download || anchor.origin !== window.location.origin) return
      const target = anchor.pathname + anchor.search + anchor.hash
      if (target === location.pathname + location.search + location.hash) return
      event.preventDefault(); event.stopPropagation()
      setPending(() => () => navigate(target))
    }
    window.addEventListener('beforeunload', beforeUnload)
    document.addEventListener('click', click, true)
    return () => { window.removeEventListener('beforeunload', beforeUnload); document.removeEventListener('click', click, true) }
  }, [dirty, location, navigate])

  useEffect(() => {
    if (!dirty) return
    const originalIndex = history.state?.idx
    let restoring = false
    let proceeding = false
    const pop = (event: PopStateEvent) => {
      if (restoring) { restoring = false; return }
      if (proceeding || typeof originalIndex !== 'number' || typeof event.state?.idx !== 'number') return
      const delta = event.state.idx - originalIndex
      if (!delta) return
      event.stopImmediatePropagation()
      restoring = true
      history.go(-delta)
      setPending(() => () => { proceeding = true; history.go(delta) })
    }
    window.addEventListener('popstate', pop, true)
    return () => window.removeEventListener('popstate', pop, true)
  }, [dirty, location])

  return { workspace, active, builtIn, ready, writable, error, setError, busy, dirty, recovered, pending, defaultName, open, startNewScreen, template, activateTab, closeTab,
    save, finishPending, discardDraft, pin, movePin, rename, duplicate, remove, importScreens }
}

export type ScreenLibraryState = ReturnType<typeof useScreenLibrary>