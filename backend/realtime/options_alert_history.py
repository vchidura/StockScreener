"""G102 OA3 (Nov 16): past sessions on the Options Alerts page, read from durable rows.

The realtime Options Alerts page reads the Options page release of the selected session's cohort. Its index
(`AdvancedOptionsAlertsPagePublication.datasets`) listed only that release's own session, and a past session could not
be read either: page payloads live in `rt.portal_snapshot_hot`, a hot table kept 3 calendar days
(`009_realtime_partition_maintenance.sql`, HOT_TTL), so by Monday every session but Friday's was gone (live Nov 16:
Nov 10-12 releases pinned, payloads pruned; the API answered DETECTOR_ALERTS_UNAVAILABLE).

What stays: every Options window's dataset (`rt.detector_datasets`: cohort, session, lanes), its evaluations
(`rt.detector_evaluations`: payload, status), the selected packages (`rt.option_packages` + legs) and, since Nov 10,
the session alert list (`rt.alert_current`, schema options-alert-session-v1). From these this module rebuilds, for a
past session, what its last window's Alerts page held (`evaluation_rows`, `alert_rows`, `candidate_details`, `run`,
`lanes`, with the page builder's own row functions) and serves it through the page publication's own read methods, so
Day History, Latest Run and Daily model results read a past session as they read the current one. The index lists every
session from the first session alert list on (earlier sessions have no durable alert list).

Read-only; nothing here writes or changes a window, a release or a stored row.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Mapping, Sequence
from uuid import UUID

from .advanced_options_projection import (
    OPTIONS_SESSION_ALERT_SCHEMA,
    AdvancedOptionsAlertsPagePublication,
    _alert_evaluation_row,
    _alert_run_row,
    _candidate_detail,
    _current_mark_row,
    _one_alert_per_contract,
    _run_summary,
)
from .contracts import canonical_sha256
from .preflight import DetectorFamily

OPTIONS_ALERT_HISTORY_CONTRACT = "options-alert-session-history-v1"
OPTIONS_ALERT_HISTORY_POLICY_SHA256 = canonical_sha256({"contract": OPTIONS_ALERT_HISTORY_CONTRACT})
MAXIMUM_LISTED_SESSIONS = 60
HISTORY_CONTRACTS = frozenset({"datasets", "detector_runs", "evaluations", "candidate"})


def _record(row, names):
    return dict(row) if isinstance(row, Mapping) else dict(zip(names, row, strict=True))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Options alert history time must be timezone-aware")
    return value.astimezone(timezone.utc)


_SESSIONS_SQL = (
    "SELECT DISTINCT ON (dataset.session_date) dataset.session_date,dataset.cohort_id::text AS cohort_id,"
    "dataset.dataset_id::text AS dataset_id,dataset.started_at "
    "FROM rt.detector_datasets AS dataset "
    "WHERE dataset.provider_tier='MASSIVE_ADVANCED' AND dataset.started_at<=%s "
    "AND dataset.session_date>=(SELECT min((alert.projection->>'session_date')::date) FROM rt.alert_current AS alert "
    "WHERE alert.alert_kind='OPTION' AND alert.projection_schema=%s) "
    "ORDER BY dataset.session_date DESC,dataset.started_at DESC,dataset.dataset_id DESC LIMIT %s"
)


def load_options_alert_sessions_cursor(cursor, *, cutoff: datetime, limit: int = MAXIMUM_LISTED_SESSIONS):
    """Sessions with durable Options Alerts history, oldest first: one row per session with its last Options window
    (dataset) and that window's cohort. Sessions before the first session alert list are not listed."""
    cursor.execute(_SESSIONS_SQL, (_utc(cutoff), OPTIONS_SESSION_ALERT_SCHEMA, int(limit)))
    names = ("session_date", "cohort_id", "dataset_id", "started_at")
    rows = [_record(row, names) for row in cursor.fetchall()]
    return [
        {"session_date": str(row["session_date"]), "cohort_id": str(row["cohort_id"]),
         "dataset_id": str(row["dataset_id"]), "cutoff": row["started_at"]}
        for row in reversed(rows)
    ]


def with_past_sessions(index: Mapping[str, object], sessions: Sequence[Mapping[str, object]]) -> dict:
    """The page index (`datasets` data) with the durable sessions before its current session added; the current
    session, its dataset and every other field stay as they are."""
    data = deepcopy(dict(index))
    current = data.get("current_session_date")
    past = [row for row in sessions if current is None or str(row["session_date"]) < str(current)]
    if not past:
        return data
    data["sessions"] = sorted({*(str(value) for value in data.get("sessions") or ()),
                               *(str(row["session_date"]) for row in past)})
    data["session_datasets"] = {**{str(row["session_date"]): str(row["dataset_id"]) for row in past},
                                **dict(data.get("session_datasets") or {})}
    data["dataset_sessions"] = {**{str(row["dataset_id"]): [str(row["session_date"])] for row in past},
                                **dict(data.get("dataset_sessions") or {})}
    data["datasets"] = [*(str(row["dataset_id"]) for row in past),
                        *(value for value in data.get("datasets") or () if value not in {str(row["dataset_id"]) for row in past})]
    return data


def history_index(sessions: Sequence[Mapping[str, object]]) -> dict | None:
    """The page index built only from durable sessions (the current release's page is not stored any more)."""
    if not sessions:
        return None
    latest = sessions[-1]
    return with_past_sessions({
        "storage_ready": True,
        "datasets": [latest["dataset_id"]],
        "dataset_sessions": {latest["dataset_id"]: [latest["session_date"]]},
        "sessions": [latest["session_date"]],
        "session_datasets": {latest["session_date"]: latest["dataset_id"]},
        "current_session_date": latest["session_date"],
        "current_dataset_id": latest["dataset_id"],
        "default_dataset_id": latest["dataset_id"],
        "configured_dataset_id": latest["dataset_id"],
        "configured_effective_from": _utc(latest["cutoff"]).isoformat(),
    }, sessions)


class AdvancedOptionsAlertsHistoryPublication(AdvancedOptionsAlertsPagePublication):
    """A past session's Alerts page rebuilt from durable rows. Its cohort and session were matched when it was loaded
    (`load_options_alert_history_page_cursor`), so the stored-page identity check does not apply."""

    def _stored(self, identity):
        return self._payload


def _latest_dataset(cursor, *, cohort_id, session_date, cutoff):
    cursor.execute(
        "SELECT dataset_id::text AS dataset_id,session_date,started_at,completed_at,summary "
        "FROM rt.detector_datasets WHERE provider_tier='MASSIVE_ADVANCED' AND cohort_id=%s "
        "AND (%s::date IS NULL OR session_date=%s::date) AND started_at<=%s "
        "ORDER BY started_at DESC,dataset_id DESC LIMIT 1",
        (str(cohort_id), session_date, session_date, _utc(cutoff)),
    )
    row = cursor.fetchone()
    return None if row is None else _record(
        row, ("dataset_id", "session_date", "started_at", "completed_at", "summary"),
    )


def _window_marks(cursor, dataset_id, evaluations) -> dict:
    """The package marks the window sealed into its Alerts page (`load_advanced_options_page_source_cursor`): each
    selected package's mark from its flow-package evidence, keyed by candidate. A package recorded without a mark (or
    an unreadable one) is left out, so its row reads UNAVAILABLE as on the page. Empty when the reader may not read
    that evidence (the API role, Nov 16 copy test)."""
    candidate_ids = sorted({str(row.package.candidate_id) for row in evaluations if row.package is not None})
    if not candidate_ids:
        return {}
    # The window's evidence table is the runtime's; a reader without that grant (the API role) serves the rows
    # without the sealed marks and the API's read-time marks fill them from stored quotes.
    cursor.execute(
        "SELECT has_table_privilege(current_user,'rt.advanced_options_flow_package_evidence','SELECT') AS allowed"
    )
    allowed = cursor.fetchone()
    if allowed is None or not _record(allowed, ("allowed",))["allowed"]:
        return {}
    cursor.execute(
        "SELECT candidate_id,evidence_document->'package_mark' AS package_mark "
        "FROM rt.advanced_options_flow_package_evidence WHERE dataset_id=%s AND candidate_id=ANY(%s::uuid[]) "
        "AND selection_status='SELECTED' ORDER BY candidate_id",
        (str(dataset_id), candidate_ids),
    )
    marks = {}
    for row in (_record(value, ("candidate_id", "package_mark")) for value in cursor.fetchall()):
        if row["package_mark"] is None:
            continue
        try:
            mark = _current_mark_row(row)
        except ValueError:
            continue
        marks[str(mark["candidate_id"])] = mark
    return marks


def load_options_alert_history_page_cursor(cursor, *, cohort_id, session_date=None, cutoff: datetime):
    """The Alerts page of a session's last Options window, rebuilt from durable rows; None when the cohort has no
    Options window for that session (by `cutoff`). `session_date` None = the cohort's latest session."""
    from .option_portal_repository import load_realtime_option_packages_cursor

    dataset = _latest_dataset(
        cursor, cohort_id=cohort_id,
        session_date=None if session_date is None else str(session_date), cutoff=cutoff,
    )
    if dataset is None:
        return None
    run_cutoff = _utc(dataset["started_at"])
    published_at = _utc(dataset["completed_at"] or dataset["started_at"])
    cursor.execute(
        "SELECT evaluation_id::text AS evaluation_id,event_id::text AS event_id,detector_id,underlyer,"
        "selection_status,observed_at,payload FROM rt.detector_evaluations WHERE dataset_id=%s "
        "ORDER BY observed_at,evaluation_id",
        (dataset["dataset_id"],),
    )
    names = ("evaluation_id", "event_id", "detector_id", "underlyer", "selection_status", "observed_at", "payload")
    rows = [_record(row, names) for row in cursor.fetchall()]
    selected_events = sorted({row["event_id"] for row in rows if row["selection_status"] == "SELECTED"})
    packages_by_event = {}
    if selected_events:
        cursor.execute(
            "SELECT event_id::text AS event_id,candidate_id::text AS candidate_id FROM rt.option_packages "
            "WHERE cohort_id=%s AND event_id=ANY(%s::uuid[]) ORDER BY event_id,created_at,candidate_id",
            (str(cohort_id), selected_events),
        )
        first_by_event = {}
        for row in (_record(value, ("event_id", "candidate_id")) for value in cursor.fetchall()):
            first_by_event.setdefault(row["event_id"], UUID(row["candidate_id"]))
        if first_by_event:
            packages = load_realtime_option_packages_cursor(
                cursor, tuple(first_by_event.values()), cohort_id=UUID(str(cohort_id)),
            )
            by_candidate = {row.candidate_id: row for row in packages}
            packages_by_event = {event: by_candidate.get(candidate) for event, candidate in first_by_event.items()}
    evaluations = []
    for row in rows:
        try:
            detector = DetectorFamily(str(row["detector_id"]))
        except ValueError:
            continue
        evaluations.append(SimpleNamespace(
            evaluation_id=UUID(row["evaluation_id"]), dataset_id=UUID(dataset["dataset_id"]),
            event_id=UUID(row["event_id"]), detector=detector, underlyer=str(row["underlyer"]),
            selection_status=str(row["selection_status"]), observed_at=_utc(row["observed_at"]),
            payload=dict(row["payload"] or {}),
            package=(packages_by_event.get(row["event_id"]) if row["selection_status"] == "SELECTED" else None),
        ))
    cycle = SimpleNamespace(dataset_id=UUID(dataset["dataset_id"]), evaluations=tuple(evaluations))
    current_marks = _window_marks(cursor, dataset["dataset_id"], evaluations)
    alert_rows = _one_alert_per_contract([
        (evaluation, stored)
        for evaluation in evaluations
        if (stored := _alert_run_row(evaluation, run_cutoff, current_marks)) is not None
    ])
    summary = dict(dataset["summary"] or {})
    session = str(dataset["session_date"])
    return AdvancedOptionsAlertsHistoryPublication({
        "contract": OPTIONS_ALERT_HISTORY_CONTRACT,
        "artifact_kind": "ALERTS",
        "cohort_id": str(cohort_id),
        "provider_tier": "MASSIVE_ADVANCED",
        "scope_sha256": OPTIONS_ALERT_HISTORY_POLICY_SHA256,
        "policy_sha256": OPTIONS_ALERT_HISTORY_POLICY_SHA256,
        "dataset_id": dataset["dataset_id"],
        "session_date": session,
        "published_at": published_at.isoformat(),
        "run": _run_summary(cycle, run_cutoff),
        "evaluation_rows": [_alert_evaluation_row(evaluation, run_cutoff) for evaluation in evaluations],
        "alert_rows": alert_rows,
        "lanes": [dict(lane) for lane in summary.get("lanes") or ()],
        "candidate_details": {
            str(evaluation.package.candidate_id): detail
            for evaluation in evaluations
            if evaluation.package is not None
            if (detail := _candidate_detail(evaluation, OPTIONS_ALERT_HISTORY_POLICY_SHA256)) is not None
        },
    })


def history_release(publication, *, checked_at: datetime, release_id=None) -> dict:
    """The release fields an Options page envelope carries (`_retained_options_envelope`), for a rebuilt page."""
    stored = publication._payload
    return {
        "release_id": None if release_id is None else str(release_id),
        "cutoff": datetime.fromisoformat(stored["run"]["scheduled_cycle"]),
        "retained_session_date": stored["session_date"],
        "valid_until": stored["published_at"],
        "checked_at": _utc(checked_at).isoformat(),
        "stale": False,
        "staleness_seconds": 0,
        "serving_latest_available": False,
    }


__all__ = [
    "AdvancedOptionsAlertsHistoryPublication", "HISTORY_CONTRACTS", "OPTIONS_ALERT_HISTORY_CONTRACT",
    "history_index", "history_release", "load_options_alert_history_page_cursor",
    "load_options_alert_sessions_cursor", "with_past_sessions",
]
