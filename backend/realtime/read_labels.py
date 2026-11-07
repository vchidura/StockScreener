"""G102 A2 (design 6.1): the labels a page read carries when one of its inputs changed after publication.

A page is served as it was published. When a security reference revision or corporate-action evidence arrives after
its publication, the read labels that input instead of refusing the page, and the next window publishes over it.
Integrity failures (a stored hash or identity that does not match) still refuse; this module is only for later input
changes. Pure: it formats what the repositories found.
"""
from __future__ import annotations

from collections.abc import Iterable


def input_change_labels(changed_reference_tickers: Iterable[str], action_drift: bool) -> dict[str, object] | None:
    """`reference_status`, `action_status` and `section_warnings` for a read whose inputs changed; None when none did."""
    tickers = sorted(set(changed_reference_tickers))
    if not tickers and not action_drift:
        return None
    warnings = {}
    if tickers:
        warnings["reference"] = (
            f"Security reference changed after this publication ({', '.join(tickers)}); "
            "showing it as published until the next window."
        )
    if action_drift:
        warnings["actions"] = (
            "Corporate-action evidence changed after this publication; showing it as published until the next window."
        )
    return dict(
        reference_status="CHANGED" if tickers else "CURRENT",
        action_status="CHANGED" if action_drift else "CURRENT",
        changed_reference_tickers=tickers,
        section_warnings=warnings,
    )
