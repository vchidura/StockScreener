"""Corporate-action drift by content, not by arrival time (Nov 16, 2026).

The daily dividend/split coverage refresh (the rollover, due after 23:30 UTC for the next session) re-fetches each
member's action history. It re-inserts revisions byte-identical to known ones (same security, source, action type,
source key and payload) and writes coverage rows whose provider response equals the previous one. The readers asked
"was ANY revision or coverage row received after this publication?", so every refresh made every publication look
changed: on Nov 16 the strategy read answered 503 "Strategy action evidence changed" with 136 of 136 revisions and 90
of 90 coverage responses unchanged, and the daily products' input identity changed with every refresh.

Corporate-action evidence for a set of securities changed in (since, until] only when, received in that period:
- a revision whose content (security, source, action type, source key, payload) was not already known at `since`; or
- a coverage row whose provider response differs from the latest coverage of the same security and action type known
  at `since` (or no coverage of it was known then).

`corporate_action_drift_sql` is one boolean SQL expression; `corporate_action_drift_params` gives its parameters in
order. Both tables are small (about 1,300 rows each on Nov 16, ~150 more per refresh) and every lookup is scoped to the
publication's members.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

_REVISION_CHANGED = (
    "EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS drift_action "
    "WHERE drift_action.security_id{scope} AND drift_action.received_at>%s AND drift_action.received_at<=%s "
    "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS known_action "
    "WHERE known_action.security_id=drift_action.security_id AND known_action.source=drift_action.source "
    "AND known_action.action_type=drift_action.action_type "
    "AND known_action.source_key IS NOT DISTINCT FROM drift_action.source_key "
    "AND known_action.payload_sha256=drift_action.payload_sha256 AND known_action.received_at<=%s))"
)
_COVERAGE_CHANGED = (
    "EXISTS (SELECT 1 FROM ref.corporate_action_coverage AS drift_coverage "
    "WHERE drift_coverage.security_id{scope} AND drift_coverage.received_at>%s AND drift_coverage.received_at<=%s "
    "AND drift_coverage.response_sha256 IS DISTINCT FROM (SELECT known_coverage.response_sha256 "
    "FROM ref.corporate_action_coverage AS known_coverage "
    "WHERE known_coverage.security_id=drift_coverage.security_id "
    "AND known_coverage.action_type=drift_coverage.action_type AND known_coverage.received_at<=%s "
    "ORDER BY known_coverage.received_at DESC, known_coverage.coverage_id DESC LIMIT 1))"
)


def corporate_action_drift_sql(*, single: bool = False) -> str:
    """The predicate for `security_id=%s` (`single`) or `security_id=ANY(%s::uuid[])`; parameters from
    `corporate_action_drift_params`. It is `EXISTS (...) OR EXISTS (...)`, as the readers' select lists had it (and
    their test doubles match `SELECT EXISTS`): wrap it in parentheses before combining it with AND."""
    scope = "=%s" if single else "=ANY(%s::uuid[])"
    return _REVISION_CHANGED.format(scope=scope) + " OR " + _COVERAGE_CHANGED.format(scope=scope)


def corporate_action_drift_params(securities, since, until) -> tuple:
    """`securities`: one security id (string) for the single form, else a list of security id strings."""
    return (securities, since, until, since, securities, since, until, since)


_ACTION_CONTENT = (
    "security_id", "ticker", "source", "action_type", "source_key", "effective_date", "split_from", "split_to",
    "cash_amount", "ex_date", "new_ticker", "payload_sha256",
)


def corporate_action_content(actions: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """The content of loaded action rows without their arrival (action id, received/observed times, tie counts):
    a re-inserted identical revision gives the same list."""
    seen: dict[str, dict[str, object]] = {}
    for row in actions:
        content = {key: row[key] for key in _ACTION_CONTENT if key in row}
        seen[repr(sorted((key, str(value)) for key, value in content.items()))] = content
    return [seen[key] for key in sorted(seen)]


def corporate_action_coverage_content(coverage: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Per (security, action type) the latest coverage's provider response (count and hash), without coverage ids,
    arrival times or the moving window bounds: a re-check with the same response gives the same list."""
    latest: dict[tuple[str, str], Mapping[str, object]] = {}
    for row in coverage:
        key = (str(row["security_id"]), str(row["action_type"]))
        current = latest.get(key)
        if current is None or (row["received_at"], str(row.get("coverage_id", ""))) > (
            current["received_at"], str(current.get("coverage_id", ""))
        ):
            latest[key] = row
    return [
        {
            "security_id": security_id,
            "action_type": action_type,
            "response_action_count": latest[(security_id, action_type)].get("response_action_count"),
            "response_sha256": latest[(security_id, action_type)].get("response_sha256"),
        }
        for security_id, action_type in sorted(latest)
    ]
