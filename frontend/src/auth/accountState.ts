/** Single source of truth for the placeholder account state.
 *  No session, no token, and no client-side "signed in" flag exists yet. */
export const ACCOUNT_STATUS = 'DISABLED' as const

export const ACCOUNT_UNAVAILABLE_REASON =
  'Account access is not available yet. Sign-in, sign-up, watchlists, and alerts are planned but not implemented.'

export const EMAIL_PATTERN = /^[^@\s]+@[^@\s.]+(\.[^@\s.]+)+$/

/** Client-side only. The server deliberately applies no password constraint, because a rejected
 *  constraint would place the secret in a validation error body. */
export const MINIMUM_PASSWORD_LENGTH = 12
