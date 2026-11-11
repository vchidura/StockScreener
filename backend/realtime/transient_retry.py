"""Bounded in-process retry for transient errors (G102 R21 layer 2, V3 D9; Nov 11).

A transient error (a SERIALIZABLE conflict, a deadlock, a dropped database connection, a provider timeout or 5xx/429)
is retried a few times with a growing delay instead of failing the stage or exiting the process and relying on the
container restart (R19: three lifecycle restarts on one conflict). Policy refusals, invalid data and anything else are
not retried: they are raised at once, as before.

Rules for callers:
- `operation` must be a whole unit of work (one transaction, or one provider request with its own ledger reservation);
  a retry repeats it from the start, so nothing half-done may survive a failed attempt.
- Provider requests: each attempt must reserve and finish its own ledger slot (the R12 share counts used slots only).

FUTURE (adoption, one image at a time, each with a rehearsal):
- `scripts/run_advanced_options_v3_lifecycle.py` `_reconcile_with_retry` (R19) does the same for the reconcile; fold it
  into `retry_transient` at the next lifecycle image.
- Options capture pages (G21): retry a failed page with its own reservation instead of spending a capture round.
- Equity and Options stage transactions in the window services, the Stock Alerts batch write, breadth capture
  requests and the API's read transactions.
"""
from __future__ import annotations

import time
from typing import Callable, TypeVar

from .fault_journal import emit_fault

T = TypeVar("T")

# PostgreSQL: serialization failure, deadlock, admin/crash shutdown, cannot connect now, connection exceptions.
TRANSIENT_PGCODES = frozenset({
    "40001", "40P01", "57P01", "57P02", "57P03", "08000", "08001", "08003", "08004", "08006",
})
TRANSIENT_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
_TRANSIENT_NAMES = frozenset({
    "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "ChunkedEncodingError", "RemoteDisconnected",
    "ConnectionResetError", "ConnectionAbortedError", "BrokenPipeError", "IncompleteRead", "TimeoutError",
})


def is_transient(error: BaseException) -> bool:
    pgcode = str(getattr(error, "pgcode", "") or "")
    if pgcode:
        return pgcode in TRANSIENT_PGCODES
    status = getattr(error, "status", None)
    if status is None:
        status = getattr(getattr(error, "response", None), "status_code", None)
    if isinstance(status, int):
        return status in TRANSIENT_HTTP_STATUSES
    if type(error).__name__ in _TRANSIENT_NAMES:
        return True
    # psycopg2.OperationalError without a code is a lost connection ("server closed the connection unexpectedly").
    return type(error).__name__ in {"OperationalError", "InterfaceError"} and not pgcode


def retry_transient(
    operation: Callable[[], T],
    *,
    name: str,
    attempts: int = 5,
    base_delay_seconds: float = 0.1,
    maximum_delay_seconds: float = 5.0,
    wait: Callable[[float], object] = time.sleep,
    classify: Callable[[BaseException], bool] = is_transient,
    service: str | None = None,
) -> T:
    """Run `operation`; on a transient error wait (0.1, 0.2, 0.4 ... s, capped) and run it again, at most `attempts`
    times. Reports RECOVERED when a retry succeeded and FATAL when the last attempt failed, then raises it."""
    if attempts < 1:
        raise ValueError("retry attempts must be positive")
    errors: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            result = operation()
        except Exception as error:  # noqa: BLE001 - re-raised unless transient
            if not classify(error):
                raise
            errors.append(f"{type(error).__name__}:{getattr(error, 'pgcode', '') or ''}")
            if attempt >= attempts:
                emit_fault(kind="RETRY_EXHAUSTED", severity="FATAL", recoverable=False, action="RAISED",
                           service=service, key=name, message=f"{name}: {attempts} attempts failed: {error}",
                           context={"operation": name, "errors": errors})
                raise
            wait(min(maximum_delay_seconds, base_delay_seconds * (2 ** (attempt - 1))))
            continue
        if errors:
            emit_fault(kind="RETRY", severity="RECOVERED", recoverable=True, action="RETRIED", service=service,
                       key=name, message=f"{name}: succeeded on attempt {attempt}",
                       context={"operation": name, "errors": errors})
        return result
    raise AssertionError("unreachable")
