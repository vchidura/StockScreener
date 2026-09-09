import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { LockKeyhole } from 'lucide-react'
import { ACCOUNT_UNAVAILABLE_REASON } from '../../auth/accountState'
import { usePublishPageContext } from '../../layout/pageContext'
import { getAccountSession } from '../../services/api'
import './auth.css'

export default function Account() {
  const session = useQuery({ queryKey: ['account', 'session'], queryFn: getAccountSession, retry: false })

  const status = session.data?.status ?? (session.isError ? 'UNREACHABLE' : 'Loading')
  const reason = session.isError
    ? 'The account service could not be reached. Portal research pages remain available without an account.'
    : session.data?.reason ?? ACCOUNT_UNAVAILABLE_REASON
  const planned = session.data?.planned_capabilities ?? []

  usePublishPageContext({
    eyebrow: 'Account access',
    title: 'Account',
    detail: 'The portal runs without an account today. This page reports the state the API declares for the account subsystem.',
    status: [{ label: 'Subsystem', value: status }],
  })

  return (
    <div className="account-page">
      <section className="account-card">
        <span className="account-status">
          <LockKeyhole size={12} aria-hidden="true" />
          {status}
        </span>
        <h2>Account access is not implemented</h2>
        <p>{reason}</p>
        {planned.length > 0 && (
          <ul>
            {planned.map(capability => (
              <li key={capability}>{capability}</li>
            ))}
          </ul>
        )}
        <div className="account-actions">
          <Link className="is-primary" to="/signin">
            Sign in
          </Link>
          <Link className="is-secondary" to="/signup">
            Create account
          </Link>
        </div>
      </section>
    </div>
  )
}
