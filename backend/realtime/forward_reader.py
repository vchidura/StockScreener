from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from options.calendar import OptionExchangeCalendar

from .contracts import canonical_sha256
from .forward_storage import decode_forward_artifact


class ForwardPublicationUnavailable(LookupError):
    pass


def load_forward_release_cursor(cursor, *, cohort_id, available_by, artifact_kind, release_id=None):
    if available_by.utcoffset() is None:
        raise ValueError("forward serving clock must be aware")
    cursor.execute("SELECT release.* FROM rt.developer_forward_releases AS release "
        "JOIN rt.provider_tier_cohorts AS tier ON tier.cohort_id=release.cohort_id AND tier.provider_tier=release.provider_tier "
        "WHERE release.cohort_id=%s AND release.provider_tier='MASSIVE_DEVELOPER' AND release.published_at<=%s "
        "AND ((%s::uuid IS NOT NULL AND release.release_id=%s::uuid) OR (%s::uuid IS NULL AND EXISTS ("
        "SELECT 1 FROM rt.developer_forward_current AS current WHERE current.cohort_id=release.cohort_id "
        "AND current.provider_tier=release.provider_tier AND current.release_id=release.release_id)))",
        (str(cohort_id), available_by, str(release_id) if release_id else None,
         str(release_id) if release_id else None, str(release_id) if release_id else None))
    row = cursor.fetchone()
    if row is None:
        return None
    row = dict(row)
    manifest = row["manifest"]
    if (canonical_sha256(manifest) != row["manifest_sha256"] or manifest["version"] != "developer_forward_release_v1"
            or manifest["cohort_id"] != str(cohort_id) or str(row["cohort_id"]) != str(cohort_id)
            or row["provider_tier"] != "MASSIVE_DEVELOPER" or manifest["provider_tier"] != row["provider_tier"]
            or manifest["scope_sha256"] != row["scope_sha256"]
            or release_id is not None and str(row["release_id"]) != str(release_id)):
        raise ForwardPublicationUnavailable("forward release immutable identity/hash drift")
    if available_by >= row["valid_until"]:
        raise ForwardPublicationUnavailable("forward release serving expiry elapsed")
    cutoff = datetime.fromisoformat(manifest["cutoff"])
    expected = OptionExchangeCalendar().latest_delayed_slot(available_by, interval=timedelta(minutes=15),
        provider_delay=timedelta(minutes=15), publication_grace=timedelta(0))
    if expected is not None and cutoff > expected or cutoff > available_by - timedelta(minutes=15):
        raise ForwardPublicationUnavailable("forward release is ahead of Developer-qualified source watermark")
    references = manifest.get("references", ())
    for contract in references:
        cursor.execute("SELECT payload_sha256 FROM ref.option_contract_versions WHERE contract_id=%s "
            "AND effective_from<=%s AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
            "ORDER BY observed_at DESC,version_id DESC", (contract["contract_id"], cutoff, cutoff, available_by))
        versions = cursor.fetchall()
        if len(versions) != 1 or versions[0]["payload_sha256"] != contract["payload_sha256"]:
            raise ForwardPublicationUnavailable("forward option reference drift; explicit republish required")
    ids = manifest.get("security_ids", ())
    cursor.execute("SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions WHERE security_id=ANY(%s::uuid[]) "
        "AND received_at>%s AND received_at<=%s) OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
        "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) OR EXISTS ("
        "SELECT 1 FROM ref.security_revisions WHERE security_id=ANY(%s::uuid[]) AND observed_at>%s AND observed_at<=%s) AS drift",
        (list(ids), row["published_at"], available_by, list(ids), row["published_at"], available_by,
         list(ids), row["published_at"], available_by))
    if cursor.fetchone()["drift"]:
        raise ForwardPublicationUnavailable("forward security/action revision drift")
    member = manifest["artifacts"].get(artifact_kind)
    if member is None:
        raise ForwardPublicationUnavailable("forward original contract is absent from retained release")
    cursor.execute("SELECT * FROM rt.developer_forward_artifacts WHERE artifact_id=%s AND input_id=%s "
        "AND cohort_id=%s AND provider_tier='MASSIVE_DEVELOPER'", (member["artifact_id"], str(row["input_id"]), str(cohort_id)))
    artifact = cursor.fetchone()
    if (not artifact or artifact["artifact_kind"] != artifact_kind or artifact["scope_sha256"] != row["scope_sha256"]
            or artifact["payload_sha256"] != member["payload_sha256"] or artifact["computed_at"] > row["published_at"]):
        raise ForwardPublicationUnavailable("forward retained artifact membership/hash/clock drift")
    value = decode_forward_artifact(artifact["payload_text"], artifact["payload_sha256"])
    return value, row