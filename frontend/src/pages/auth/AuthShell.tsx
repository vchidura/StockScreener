import type { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import { AlertTriangle, ChevronLeft, LockKeyhole } from 'lucide-react'
import { BRAND_MARK, BRAND_NAME, BRAND_SUFFIX, BRAND_TAGLINE } from '../../layout/navigation'
import type { AccountEnvelope } from '../../services/api'
import './auth.css'

export function AccountNotice({ envelope, failed }: { envelope: AccountEnvelope | null; failed: boolean }) {
  if (failed) {
    return (
      <div className="auth-notice auth-notice--error" role="alert">
        <strong>
          <AlertTriangle size={13} aria-hidden="true" />
          Request failed
        </strong>
        <span>The account service could not be reached. The portal itself remains available without an account.</span>
      </div>
    )
  }
  if (!envelope) return null
  return (
    <div className="auth-notice auth-notice--info" role="status">
      <strong>
        <LockKeyhole size={13} aria-hidden="true" />
        Not implemented
      </strong>
      <span>{envelope.reason}</span>
      {envelope.planned_capabilities.length > 0 && (
        <ul>
          {envelope.planned_capabilities.map(capability => (
            <li key={capability}>{capability}</li>
          ))}
        </ul>
      )}
    </div>
  )
}

export function AuthField({
  id,
  label,
  type,
  value,
  autoComplete,
  error,
  hint,
  onChange,
}: {
  id: string
  label: string
  type: 'text' | 'email' | 'password'
  value: string
  autoComplete: string
  error?: string
  hint?: string
  onChange: (value: string) => void
}) {
  return (
    <label className={`auth-field${error ? ' auth-field--invalid' : ''}`} htmlFor={id}>
      <span>{label}</span>
      <input
        id={id}
        type={type}
        value={value}
        autoComplete={autoComplete}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `${id}-error` : hint ? `${id}-hint` : undefined}
        onChange={event => onChange(event.target.value)}
      />
      {error ? (
        <small id={`${id}-error`} role="alert">
          {error}
        </small>
      ) : hint ? (
        <small id={`${id}-hint`} className="auth-field__hint">
          {hint}
        </small>
      ) : null}
    </label>
  )
}

export default function AuthShell({
  eyebrow,
  title,
  detail,
  children,
  footer,
}: {
  eyebrow: string
  title: string
  detail: string
  children: ReactNode
  footer: ReactNode
}) {
  return (
    <div className="auth-page">
      <aside className="auth-page__brand">
        <NavLink to="/" className="auth-brand">
          <span className="auth-brand__mark">{BRAND_MARK}</span>
          <span className="auth-brand__text">
            <strong>{BRAND_NAME}</strong>
            <small>
              {BRAND_SUFFIX} · {BRAND_TAGLINE}
            </small>
          </span>
        </NavLink>
        <h2>Equity and options research, evidence first.</h2>
        <ul>
          <li>Scanner qualification with explicit robustness gates rather than back-fitted scores.</li>
          <li>Delayed options research that states its entitlement limits on every surface.</li>
          <li>Read-only throughout. No recommendations, no order routing.</li>
        </ul>
        <NavLink to="/" className="auth-page__back">
          <ChevronLeft size={13} aria-hidden="true" />
          Back to the portal
        </NavLink>
      </aside>
      <main className="auth-page__panel">
        <div className="auth-card">
          <span className="auth-card__eyebrow">{eyebrow}</span>
          <h1>{title}</h1>
          <p className="auth-card__detail">{detail}</p>
          {children}
          <div className="auth-card__footer">{footer}</div>
        </div>
      </main>
    </div>
  )
}
