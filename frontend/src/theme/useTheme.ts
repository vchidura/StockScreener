import { useCallback, useEffect, useState } from 'react'

export type ThemeName = 'dark' | 'light'

export const THEME_STORAGE_KEY = 'alphascreener.theme'

export function storedTheme(): ThemeName {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) === 'light' ? 'light' : 'dark'
  } catch {
    return 'dark'
  }
}

export function useTheme() {
  const [theme, setTheme] = useState<ThemeName>(storedTheme)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    try {
      localStorage.setItem(THEME_STORAGE_KEY, theme)
    } catch {
      // Storage can be unavailable in private browsing; the applied theme still holds for the session.
    }
  }, [theme])

  const toggleTheme = useCallback(
    () => setTheme(current => (current === 'dark' ? 'light' : 'dark')),
    [],
  )

  return { theme, toggleTheme }
}
