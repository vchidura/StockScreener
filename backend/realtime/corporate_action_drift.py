"""Corporate-action drift by content, not by arrival time (Nov 16, 2026; R84b Nov 18).

The daily dividend/split coverage refresh (the rollover, due after 23:30 UTC for the next session) re-fetches each
member's action history. It re-inserts revisions byte-identical to known ones (same security, source, action type,
source key and payload) and writes coverage rows whose provider response equals the previous one. The readers asked
"was ANY revision or coverage row received after this publication?", so every refresh made every publication look
changed: on Nov 16 the strategy read answered 503 "Strategy action evidence changed" with 136 of 136 revisions and 90
of 90 coverage responses unchanged, and the daily products' input identity changed with every refresh.

R84b (found Nov 17 06:44): R84 still compared raw hashes. After XOM's ex-date (Nov 16) the provider re-sent the same
dividend with two fields added (`historical_adjustment_factor`, `split_adjusted_cash_amount`); ex-date and cash were
unchanged, but the payload hash and the coverage's response hash changed, so `/api/strategies/daily` answered 503
overnight again, as it would after every member's ex-date. An action's CONTENT is what the readers derive from it:
type, effective date, ex-date, pay date, cash amount, split ratio, new ticker, and the payload's currency and
distribution type (the strategy's dividend handling, `strategy_action_context`). Identity: security, source, action
type, source key (the provider's action id).

Corporate-action evidence for a set of securities changed in (since, until] only when, received in that period:
- a revision whose content was not already known at `since` for the same identity (a known revision with the same
  payload hash counts as known, as in R84, so this flags a subset of what R84 flagged); or
- a coverage row whose response differs from the latest coverage of the same security and action type known at
  `since` (or no coverage of it was known then). Same response hash = unchanged (R84); otherwise unchanged only when
  the action count is the same and the receipt's actions (source key + content, the revisions written with the
  coverage: same security, action type, source and clocks) are the same set, all of them found. A dropped or added
  action is a change.

`corporate_action_drift_sql` is one boolean SQL expression; `corporate_action_drift_params` gives its parameters in
order. Both tables are small (about 1,300 rows each on Nov 16, ~150 more per refresh) and every lookup is scoped to the
publication's members.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence


def _content_sql(alias: str) -> str:
    """One action's content as a jsonb array (numeric columns compare by value; an absent payload field is null)."""
    return (
        f"jsonb_build_array({alias}.effective_date,{alias}.ex_date,{alias}.pay_date,{alias}.cash_amount,"
        f"{alias}.split_from,{alias}.split_to,{alias}.new_ticker,{alias}.raw_payload->'currency',"
        f"{alias}.raw_payload->'distribution_type')"
    )


def _receipt_sql(coverage: str, alias: str) -> str:
    """The actions of one coverage receipt (written in the same request: same security, type, source and clocks),
    as a sorted array of (source key, content)."""
    return (
        f"ARRAY(SELECT jsonb_build_array({alias}.source_key,{_content_sql(alias)}) "
        f"FROM ref.corporate_action_revisions AS {alias} "
        f"WHERE {alias}.security_id={coverage}.security_id AND {alias}.action_type={coverage}.action_type "
        f"AND {alias}.source={coverage}.source AND {alias}.received_at={coverage}.received_at "
        f"AND {alias}.observed_at={coverage}.observed_at ORDER BY 1)"
    )


_REVISION_CHANGED = (
    "EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS drift_action "
    "WHERE drift_action.security_id{scope} AND drift_action.received_at>%s AND drift_action.received_at<=%s "
    "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS known_action "
    "WHERE known_action.security_id=drift_action.security_id AND known_action.source=drift_action.source "
    "AND known_action.action_type=drift_action.action_type "
    "AND known_action.source_key IS NOT DISTINCT FROM drift_action.source_key "
    "AND (known_action.payload_sha256=drift_action.payload_sha256 "
    "OR " + _content_sql("known_action") + "=" + _content_sql("drift_action") + ") "
    "AND known_action.received_at<=%s))"
)
_COVERAGE_CHANGED = (
    "EXISTS (SELECT 1 FROM ref.corporate_action_coverage AS drift_coverage "
    "WHERE drift_coverage.security_id{scope} AND drift_coverage.received_at>%s AND drift_coverage.received_at<=%s "
    "AND NOT EXISTS (SELECT 1 FROM (SELECT known_coverage.security_id,known_coverage.action_type,"
    "known_coverage.source,known_coverage.response_action_count,known_coverage.response_sha256,"
    "known_coverage.observed_at,known_coverage.received_at "
    "FROM ref.corporate_action_coverage AS known_coverage "
    "WHERE known_coverage.security_id=drift_coverage.security_id "
    "AND known_coverage.action_type=drift_coverage.action_type AND known_coverage.received_at<=%s "
    "ORDER BY known_coverage.received_at DESC, known_coverage.coverage_id DESC LIMIT 1) AS latest_coverage "
    "WHERE latest_coverage.response_sha256=drift_coverage.response_sha256 "
    "OR (latest_coverage.response_action_count=drift_coverage.response_action_count "
    "AND cardinality(" + _receipt_sql("drift_coverage", "drift_receipt") + ")=drift_coverage.response_action_count "
    "AND " + _receipt_sql("drift_coverage", "drift_receipt") + "="
    + _receipt_sql("latest_coverage", "known_receipt") + ")))"
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


_ACTION_IDENTITY = ("security_id", "ticker", "source", "action_type", "source_key")
_ACTION_FIELDS = ("effective_date", "ex_date", "pay_date", "cash_amount", "split_from", "split_to", "new_ticker")
_PAYLOAD_FIELDS = ("currency", "distribution_type")


def _action_content(row: Mapping[str, object]) -> dict[str, object]:
    """One loaded action row's content (the SQL predicate's fields that the row carries). Without its payload loaded,
    the payload hash stands in for the payload fields (R84's rule)."""
    content = {key: row[key] for key in _ACTION_IDENTITY + _ACTION_FIELDS if key in row}
    raw = row.get("raw_payload")
    if isinstance(raw, Mapping):
        content.update({f"payload_{key}": raw.get(key) for key in _PAYLOAD_FIELDS})
    elif "payload_sha256" in row:
        content["payload_sha256"] = row["payload_sha256"]
    return content


def _content_key(content: Mapping[str, object]) -> str:
    return repr(sorted((key, str(value)) for key, value in content.items()))


def corporate_action_content(actions: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """The content of loaded action rows without their arrival (action id, received/observed times, tie counts) or
    their raw payload hash: a re-sent action whose content is unchanged gives the same list (R84b)."""
    seen: dict[str, dict[str, object]] = {}
    for row in actions:
        content = _action_content(row)
        seen[_content_key(content)] = content
    return [seen[key] for key in sorted(seen)]


def corporate_action_coverage_content(
    coverage: Sequence[Mapping[str, object]],
    actions: Sequence[Mapping[str, object]] = (),
) -> list[dict[str, object]]:
    """Per (security, action type) the latest coverage's response, without coverage ids, arrival times or the moving
    window bounds: a re-check with the same response gives the same list. The response enters as its action count and
    the content of its actions (the loaded `actions` received with it) when all of them are loaded, else as its hash
    (R84's rule), so a re-sent response whose actions' content is unchanged keeps the same list (R84b)."""
    latest: dict[tuple[str, str], Mapping[str, object]] = {}
    for row in coverage:
        key = (str(row["security_id"]), str(row["action_type"]))
        current = latest.get(key)
        if current is None or (row["received_at"], str(row.get("coverage_id", ""))) > (
            current["received_at"], str(current.get("coverage_id", ""))
        ):
            latest[key] = row
    receipts: dict[tuple[str, str, object], list[str]] = {}
    for row in actions:
        if "received_at" in row:
            receipts.setdefault(
                (str(row["security_id"]), str(row["action_type"]), row["received_at"]), []
            ).append(_content_key(_action_content(row)))
    content = []
    for security_id, action_type in sorted(latest):
        row = latest[(security_id, action_type)]
        count = row.get("response_action_count")
        receipt = sorted(receipts.get((security_id, action_type, row["received_at"]), ()))
        entry = {"security_id": security_id, "action_type": action_type, "response_action_count": count}
        if len(receipt) == count:
            entry["response_actions"] = receipt
        else:
            entry["response_sha256"] = row.get("response_sha256")
        content.append(entry)
    return content
