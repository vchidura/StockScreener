"""Shared pooled HTTP session for outbound market-data calls.

Bare ``requests.get()`` builds a throwaway Session per call: a new TCP connection and
a new DNS lookup every time. Across a full universe run that is thousands of sockets
and resolver queries, which can exhaust the OS resolver and produce spurious
``getaddrinfo failed`` errors unrelated to the upstream API.

Always call ``get_session()`` at the call site rather than caching the returned object.
The scheduler runs resident for days, so the session is recycled periodically to pick up
DNS changes and to drop connections a peer silently dropped.
"""

import threading
import time
from http.cookiejar import DefaultCookiePolicy

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONNECT_RETRIES = 2
BACKOFF_FACTOR = 1.0
RETRY_STATUSES = (500, 502, 503, 504)
# 429 is deliberately absent: callers implement their own rate-limit backoff.

SESSION_MAX_AGE = 1800  # seconds; bounds how long a pooled connection can pin a stale IP

_lock = threading.Lock()
_session: requests.Session | None = None
_created_at = 0.0


def build_session(pool_maxsize: int = 8) -> requests.Session:
    """Create a keep-alive session that retries transient connection failures."""
    session = requests.Session()
    # Callers previously used one-shot requests.get(), so no cookie ever survived a
    # call. Reject cookies to keep that behaviour and to keep the jar thread-safe.
    session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
    adapter = HTTPAdapter(
        pool_connections=4,
        pool_maxsize=pool_maxsize,
        max_retries=Retry(
            total=CONNECT_RETRIES,
            connect=CONNECT_RETRIES,
            read=CONNECT_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUSES,
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        ),
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_session() -> requests.Session:
    """Return the shared session, rebuilding it once it exceeds SESSION_MAX_AGE."""
    global _session, _created_at
    now = time.monotonic()
    with _lock:
        if _session is None or now - _created_at > SESSION_MAX_AGE:
            # The old session is not closed: another thread may still be mid-request on
            # it. Dropping the reference lets it close once its last user releases it.
            _session = build_session()
            _created_at = now
        return _session
