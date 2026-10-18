from __future__ import annotations

from datetime import datetime, timezone

from .contracts import canonical_sha256


def classify_reference_identity_continuity(prior, latest, cutoff):
    direct = (
        prior["effective_from"] <= cutoff
        and (
            prior["effective_to"] is None
            or prior["effective_to"] > cutoff
        )
        and prior["observed_at"] <= cutoff
    )
    if direct:
        return "DIRECT_AT_CUTOFF"
    if (
        latest["revision_id"] == prior["revision_id"]
        or latest["observed_at"] <= cutoff
        or not latest["active"]
        or str(latest["security_id"]) != str(prior["security_id"])
        or latest["ticker"] != prior["ticker"]
    ):
        raise ValueError(
            "late identity continuity lacks distinct matching post-cutoff evidence"
        )
    return "LATE_OBSERVED_CONTINUITY"


def load_reference_continuity_rows_cursor(
    cursor,
    *,
    manifest_id,
    cutoff,
    known_by,
):
    cursor.execute(
        "SELECT instrument.instrument_id,instrument.security_id "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "WHERE member.manifest_id=%s AND member.channel='AM' "
        "AND instrument.asset_class='EQUITY' "
        "ORDER BY instrument.instrument_id",
        (str(manifest_id),),
    )
    members = tuple(cursor.fetchall())
    selected_rows = []
    evidence = []
    for member in members:
        instrument_id = int(member["instrument_id"])
        security_id = member["security_id"]
        cursor.execute(
            "SELECT revision.*,revision.revision_id "
            "AS security_revision_id "
            "FROM ref.security_revisions AS revision "
            "WHERE revision.security_id=%s "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.observed_at DESC,"
            "revision.revision_id DESC LIMIT 1",
            (str(security_id), cutoff),
        )
        prior = cursor.fetchone()
        cursor.execute(
            "SELECT revision.*,revision.revision_id "
            "AS security_revision_id "
            "FROM ref.security_revisions AS revision "
            "WHERE revision.security_id=%s "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.observed_at DESC,"
            "revision.revision_id DESC LIMIT 1",
            (str(security_id), known_by),
        )
        latest = cursor.fetchone()
        if prior is None or latest is None:
            raise ValueError(
                "late identity continuity requires pre/post revisions"
            )
        mode = classify_reference_identity_continuity(
            prior,
            latest,
            cutoff,
        )
        cursor.execute(
            "SELECT count(*) AS count FROM ref.security_revisions "
            "WHERE security_id=%s AND observed_at>%s "
            "AND observed_at<%s AND ticker<>%s",
            (
                str(security_id),
                prior["observed_at"],
                latest["observed_at"],
                latest["ticker"],
            ),
        )
        if int(cursor.fetchone()["count"]) != 0:
            raise ValueError(
                "late identity continuity contains conflicting ticker evidence"
            )
        selected = dict(
            prior if mode == "DIRECT_AT_CUTOFF" else latest
        )
        selected["instrument_id"] = instrument_id
        selected_rows.append(selected)
        evidence.append((
            instrument_id,
            selected["ticker"],
            mode,
            str(prior["revision_id"]),
            prior["payload_sha256"],
            prior["observed_at"],
            prior["effective_from"],
            prior["effective_to"],
            str(latest["revision_id"]),
            latest["payload_sha256"],
            latest["observed_at"],
            latest["effective_from"],
            latest["effective_to"],
        ))
    return tuple(selected_rows), tuple(sorted(evidence))


def reference_continuity_manifest_fields(inputs):
    if inputs.identity_continuity_sha256 is None:
        return {}
    return {
        "identity_continuity_sha256": inputs.identity_continuity_sha256,
        "known_by": inputs.recorded_by.isoformat(),
    }


def source_manifest_identity_cursor(
    cursor,
    manifest,
    *,
    manifest_id,
    cohort_manifest_sha256,
    history_binding_sha256,
    cutoff,
):
    continuity_sha256 = manifest.get("identity_continuity_sha256")
    known_by_value = manifest.get("known_by")
    if continuity_sha256 is None and known_by_value is None:
        return (
            canonical_sha256({
                "manifest_sha256": cohort_manifest_sha256,
                "history_binding_sha256": history_binding_sha256,
            })
            if history_binding_sha256 is not None
            else cohort_manifest_sha256
        ), None
    if continuity_sha256 is None or known_by_value is None:
        raise ValueError("late identity continuity manifest is incomplete")
    known_by = (
        known_by_value
        if isinstance(known_by_value, datetime)
        else datetime.fromisoformat(known_by_value)
    )
    if (
        known_by.tzinfo is None
        or known_by.utcoffset() is None
        or known_by.astimezone(timezone.utc) < cutoff
    ):
        raise ValueError("late identity continuity known-by is invalid")
    rows, evidence = load_reference_continuity_rows_cursor(
        cursor,
        manifest_id=manifest_id,
        cutoff=cutoff,
        known_by=known_by.astimezone(timezone.utc),
    )
    if canonical_sha256(evidence) != continuity_sha256:
        raise ValueError("late identity continuity digest changed")
    return canonical_sha256({
        "manifest_sha256": cohort_manifest_sha256,
        "history_binding_sha256": history_binding_sha256,
        "identity_continuity": evidence,
    }), rows
