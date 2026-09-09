import { useState } from 'react'
import { Link } from 'react-router-dom'
import { EMAIL_PATTERN, MINIMUM_PASSWORD_LENGTH } from '../../auth/accountState'
import { requestSignUp, type AccountEnvelope } from '../../services/api'
import AuthShell, { AccountNotice, AuthField } from './AuthShell'

type FieldErrors = { displayName?: string; email?: string; password?: string; confirm?: string }

export default function SignUp() {
  const [displayName, setDisplayName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [errors, setErrors] = useState<FieldErrors>({})
  const [submitting, setSubmitting] = useState(false)
  const [envelope, setEnvelope] = useState<AccountEnvelope | null>(null)
  const [failed, setFailed] = useState(false)

  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const nextErrors: FieldErrors = {}
    if (!displayName.trim()) nextErrors.displayName = 'Enter a display name.'
    if (!EMAIL_PATTERN.test(email.trim())) nextErrors.email = 'Enter a valid email address.'
    if (password.length < MINIMUM_PASSWORD_LENGTH) {
      nextErrors.password = `Use at least ${MINIMUM_PASSWORD_LENGTH} characters.`
    }
    if (confirm !== password) nextErrors.confirm = 'Passwords do not match.'
    setErrors(nextErrors)
    if (Object.keys(nextErrors).length > 0) return

    setSubmitting(true)
    setFailed(false)
    try {
      setEnvelope(
        await requestSignUp({
          email: email.trim().toLowerCase(),
          display_name: displayName.trim(),
          password,
        }),
      )
    } catch {
      setEnvelope(null)
      setFailed(true)
    } finally {
      setPassword('')
      setConfirm('')
      setSubmitting(false)
    }
  }

  return (
    <AuthShell
      eyebrow="Account access"
      title="Create an account"
      detail="Registration is not open yet. Nothing you enter is stored; the request only verifies the endpoint contract."
      footer={
        <>
          Already registered? <Link to="/signin">Sign in</Link> · Continue without an account on the{' '}
          <Link to="/">portal</Link>.
        </>
      }
    >
      <form className="auth-form" onSubmit={submit} noValidate>
        <AuthField
          id="signup-name"
          label="Display name"
          type="text"
          value={displayName}
          autoComplete="nickname"
          error={errors.displayName}
          onChange={setDisplayName}
        />
        <AuthField
          id="signup-email"
          label="Email"
          type="email"
          value={email}
          autoComplete="username"
          error={errors.email}
          onChange={setEmail}
        />
        <AuthField
          id="signup-password"
          label="Password"
          type="password"
          value={password}
          autoComplete="new-password"
          error={errors.password}
          hint={`At least ${MINIMUM_PASSWORD_LENGTH} characters.`}
          onChange={setPassword}
        />
        <AuthField
          id="signup-confirm"
          label="Confirm password"
          type="password"
          value={confirm}
          autoComplete="new-password"
          error={errors.confirm}
          onChange={setConfirm}
        />
        <button type="submit" className="auth-submit" disabled={submitting}>
          {submitting ? 'Checking…' : 'Create account'}
        </button>
        <AccountNotice envelope={envelope} failed={failed} />
      </form>
    </AuthShell>
  )
}
