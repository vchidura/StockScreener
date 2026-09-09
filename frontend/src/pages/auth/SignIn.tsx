import { useState } from 'react'
import { Link } from 'react-router-dom'
import { EMAIL_PATTERN } from '../../auth/accountState'
import { requestSignIn, type AccountEnvelope } from '../../services/api'
import AuthShell, { AccountNotice, AuthField } from './AuthShell'

export default function SignIn() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [errors, setErrors] = useState<{ email?: string; password?: string }>({})
  const [submitting, setSubmitting] = useState(false)
  const [envelope, setEnvelope] = useState<AccountEnvelope | null>(null)
  const [failed, setFailed] = useState(false)

  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const nextErrors: { email?: string; password?: string } = {}
    if (!EMAIL_PATTERN.test(email.trim())) nextErrors.email = 'Enter a valid email address.'
    if (!password) nextErrors.password = 'Enter your password.'
    setErrors(nextErrors)
    if (Object.keys(nextErrors).length > 0) return

    setSubmitting(true)
    setFailed(false)
    try {
      setEnvelope(await requestSignIn({ email: email.trim().toLowerCase(), password }))
    } catch {
      setEnvelope(null)
      setFailed(true)
    } finally {
      setPassword('')
      setSubmitting(false)
    }
  }

  return (
    <AuthShell
      eyebrow="Account access"
      title="Sign in"
      detail="Accounts are not live yet. Submitting confirms the endpoint contract and returns the server's explicit unavailable response."
      footer={
        <>
          No account? <Link to="/signup">Request one</Link> · Continue without an account on the{' '}
          <Link to="/">portal</Link>.
        </>
      }
    >
      <form className="auth-form" onSubmit={submit} noValidate>
        <AuthField
          id="signin-email"
          label="Email"
          type="email"
          value={email}
          autoComplete="username"
          error={errors.email}
          onChange={setEmail}
        />
        <AuthField
          id="signin-password"
          label="Password"
          type="password"
          value={password}
          autoComplete="current-password"
          error={errors.password}
          onChange={setPassword}
        />
        <button type="submit" className="auth-submit" disabled={submitting}>
          {submitting ? 'Checking…' : 'Sign in'}
        </button>
        <AccountNotice envelope={envelope} failed={failed} />
      </form>
    </AuthShell>
  )
}
