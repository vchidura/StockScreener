from __future__ import annotations

import base64
import importlib
import json
from dataclasses import dataclass
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel
from psycopg2.extras import Json

from .contracts import canonical_sha256


FORWARD_INPUT_VERSION = "developer_forward_inputs_v1"
MAX_FORWARD_INPUT_BYTES = 67108864
_INPUT_MODULES = frozenset({
    "realtime.contracts", "realtime.preflight", "realtime.option_discovery_scope",
    "realtime.option_session_manifest", "realtime.forward_ingestion", "realtime.option_package_context",
    "realtime.option_flow_projection", "realtime.option_discovery_repository",
    "equity.domain", "equity.behavior", "options.domain", "options.event_calendar",
    "options.data.normalizer", "options.data.polygon_chain_discovery", "options.repositories.daily_facts",
    "options.analytics.analysis_engine", "options.analytics.chain_analysis", "options.analytics.gamma_exposure",
    "options.analytics.marks", "options.model_inputs",
    "realtime.forward_runtime",
    "realtime.forward_launcher", "realtime.option_pre_session_producer", "realtime.option_pre_session_target",
    "realtime.option_pre_session_staged_runner", "realtime.option_manifest_cohort_repository",
    "realtime.option_screener_projection", "realtime.option_alert_projection",
    "options.intraday_participation", "options.detector_collection", "options.dual_origin",
})
_SENSITIVE_FIELDS = frozenset({"password", "api_key", "token", "secret", "dsn", "database_url", "authorization"})


def _contract_type(name):
    module, class_name = name.rsplit(":", 1)
    if module not in _INPUT_MODULES or class_name.startswith("_"):
        raise ValueError(f"forward input type is not an approved domain contract: {name}")
    contract = getattr(importlib.import_module(module), class_name, None)
    if not isinstance(contract, type) or not (is_dataclass(contract) or issubclass(contract, (Enum, BaseModel))):
        raise ValueError("forward input requires a typed domain contract")
    return contract


def _encode(value):
    if isinstance(value, Enum):
        return {"type": f"{type(value).__module__}:{type(value).__name__}", "enum": value.value}
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ValueError("forward input clocks must be aware")
        return {"datetime": value.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("forward input Decimal must be finite")
        return {"decimal": str(value)}
    if isinstance(value, UUID):
        return {"uuid": str(value)}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    if is_dataclass(value) and not isinstance(value, type):
        name = f"{type(value).__module__}:{type(value).__name__}"
        _contract_type(name)
        return {"type": name, "fields": {field.name: _encode(getattr(value, field.name)) for field in fields(value)
            if not (name == "realtime.option_session_manifest:BoundedOptionManifestPolicy"
                and field.name == "liquidity_rule" and value.liquidity_rule == "OR")}}
    if isinstance(value, BaseModel):
        name = f"{type(value).__module__}:{type(value).__name__}"
        _contract_type(name)
        return {"type": name, "model": value.model_dump(mode="json")}
    if isinstance(value, tuple):
        return {"tuple": [_encode(item) for item in value]}
    if isinstance(value, list):
        return {"list": [_encode(item) for item in value]}
    if hasattr(value, "items"):
        if any(str(key).lower() in _SENSITIVE_FIELDS for key in value):
            raise ValueError("credentials cannot be retained in forward inputs")
        return {"mapping": {str(key): _encode(item) for key, item in value.items()}}
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise TypeError("unsupported forward input value")


def _decode(value):
    if not isinstance(value, dict):
        return value
    if "type" in value:
        contract = _contract_type(value["type"])
        if set(value) == {"type", "enum"} and issubclass(contract, Enum):
            return contract(value["enum"])
        if set(value) == {"type", "model"} and issubclass(contract, BaseModel):
            return contract.model_validate(value["model"])
        if set(value) == {"type", "fields"} and is_dataclass(contract):
            expected_fields = {field.name for field in fields(contract)}
            if (value["type"] == "realtime.option_session_manifest:BoundedOptionManifestPolicy"
                    and "liquidity_rule" not in value["fields"]):
                expected_fields.remove("liquidity_rule")
            if set(value["fields"]) != expected_fields:
                raise ValueError("forward input domain fields changed")
            return contract(**{key: _decode(item) for key, item in value["fields"].items()})
        raise ValueError("forward input contract encoding changed")
    if len(value) != 1:
        raise ValueError("forward input encoding must be exact")
    key, item = next(iter(value.items()))
    readers = {"datetime": datetime.fromisoformat, "date": date.fromisoformat, "decimal": Decimal,
        "uuid": UUID, "bytes": lambda data: base64.b64decode(data, validate=True),
        "tuple": lambda rows: tuple(_decode(row) for row in rows), "list": lambda rows: [_decode(row) for row in rows],
        "mapping": lambda rows: {name: _decode(row) for name, row in rows.items()}}
    if key not in readers:
        raise ValueError("unknown forward input encoding")
    return readers[key](item)


def encode_forward_inputs(scope, sources, *, detector_source=None, chain_evidence=(), package_sources=()):
    payload = dict(version=FORWARD_INPUT_VERSION, scope=_encode(scope),
        sources=_encode(tuple(sorted(sources, key=lambda row: row.underlyer))),
        detector_source=_encode(detector_source),
        chain_evidence=_encode(tuple(sorted(chain_evidence, key=lambda row: row.discovery.result.batch.underlyer))),
        package_sources=_encode(tuple(sorted(package_sources, key=lambda row: row.underlyer))))
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(serialized.encode("ascii")) > MAX_FORWARD_INPUT_BYTES:
        raise ValueError("forward input payload exceeds reviewed byte budget")
    return serialized, canonical_sha256(payload)


def decode_forward_inputs(payload_text, payload_sha256):
    if len(payload_text.encode("ascii")) > MAX_FORWARD_INPUT_BYTES:
        raise ValueError("forward input payload exceeds reviewed byte budget")
    payload = json.loads(payload_text)
    if (set(payload) != {"version", "scope", "sources", "detector_source", "chain_evidence", "package_sources"}
            or payload["version"] != FORWARD_INPUT_VERSION or canonical_sha256(payload) != payload_sha256):
        raise ValueError("immutable forward input hash/version/fields drift")
    return {key: _decode(value) for key, value in payload.items() if key != "version"}


def encode_forward_artifact(value):
    payload = _encode(value)
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(text.encode("ascii")) > MAX_FORWARD_INPUT_BYTES:
        raise ValueError("forward artifact exceeds byte budget")
    return text, canonical_sha256(payload)


def decode_forward_artifact(text, digest):
    payload = json.loads(text)
    if len(text.encode("ascii")) > MAX_FORWARD_INPUT_BYTES or canonical_sha256(payload) != digest:
        raise ValueError("immutable forward artifact checksum drift")
    return _decode(payload)


def _row(value):
    return dict(value) if value is not None else None


def persist_forward_inputs_cursor(cursor, scope, sources, *, received_at, detector_source=None,
                                  chain_evidence=(), package_sources=()):
    from .forward_ingestion import admit_developer_matrix
    from .preflight import ProviderTier

    if (scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or received_at.utcoffset() is None or not scope.available_after <= received_at <= scope.deadline):
        raise ValueError("durable forward inputs require timely Developer scope")
    if len({source.underlyer for source in sources}) != len(sources):
        raise ValueError("durable forward sources are ambiguous")
    for source in sources:
        admit_developer_matrix(scope, source)
        if source.completed_at > received_at:
            raise ValueError("forward input receipt precedes source completion")
    cursor.execute("SELECT manifest.option_underlying_count,manifest.manifest_sha256,tier.provider_tier FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.provider_tier_cohorts AS tier USING(cohort_id) WHERE cohort.cohort_id=%s",
        (str(scope.cohort_id),))
    enrollment = _row(cursor.fetchone())
    expected = (len(scope.underlyings), "MASSIVE_DEVELOPER", scope.selection.manifest.sha256)
    actual = (enrollment["option_underlying_count"], enrollment["provider_tier"], enrollment["manifest_sha256"]) if enrollment else None
    if actual != expected:
        raise ValueError("forward inputs require an approved option-bearing Developer enrollment")
    text, digest = encode_forward_inputs(scope, sources, detector_source=detector_source,
        chain_evidence=chain_evidence, package_sources=package_sources)
    input_id = uuid5(NAMESPACE_URL, f"{FORWARD_INPUT_VERSION}:{scope.sha256}")
    cursor.execute("INSERT INTO rt.developer_forward_inputs (input_id,cohort_id,provider_tier,scope_sha256,"
        "selection_sha256,configuration_sha256,reference_sha256,action_sha256,cutoff,available_after,deadline,received_at,"
        "payload_text,payload_sha256) VALUES (%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (input_id) DO NOTHING", (str(input_id), str(scope.cohort_id), scope.sha256,
        scope.selection.selection_sha256, scope.configuration_sha256, scope.reference_sha256, scope.action_sha256,
        scope.cutoff, scope.available_after, scope.deadline, received_at, text, digest))
    cursor.execute("SELECT payload_text,payload_sha256,scope_sha256 FROM rt.developer_forward_inputs WHERE input_id=%s", (str(input_id),))
    stored = _row(cursor.fetchone())
    if not stored or (stored["payload_text"], stored["payload_sha256"], stored["scope_sha256"]) != (text, digest, scope.sha256):
        raise ValueError("immutable complete forward input retry conflicts")
    return input_id


def load_forward_inputs_cursor(cursor, input_id):
    cursor.execute("SELECT * FROM rt.developer_forward_inputs WHERE input_id=%s", (str(input_id),))
    row = _row(cursor.fetchone())
    if not row:
        raise LookupError("retained complete forward inputs unavailable")
    values = decode_forward_inputs(row["payload_text"], row["payload_sha256"])
    scope = values["scope"]
    if (row["provider_tier"] != "MASSIVE_DEVELOPER" or str(row["cohort_id"]) != str(scope.cohort_id)
            or row["scope_sha256"] != scope.sha256 or row["configuration_sha256"] != scope.configuration_sha256
            or row["reference_sha256"] != scope.reference_sha256 or row["action_sha256"] != scope.action_sha256):
        raise ValueError("retained forward input scope drift")
    if (row["cutoff"] != scope.cutoff or row["available_after"] != scope.available_after or row["deadline"] != scope.deadline
            or row["selection_sha256"] != scope.selection.selection_sha256
            or not scope.available_after <= row["received_at"] <= scope.deadline):
        raise ValueError("retained forward input causal clock/manifest drift")
    return values


@dataclass(frozen=True, slots=True)
class ForwardWorkClaim:
    input_id: UUID
    owner_id: UUID
    owner_epoch: int
    lease_expires_at: datetime


def _database_clock(cursor):
    cursor.execute("SELECT clock_timestamp() AS checked_at")
    return _row(cursor.fetchone())["checked_at"]


def enqueue_forward_work_cursor(cursor, input_id):
    cursor.execute("INSERT INTO rt.developer_forward_work (input_id) VALUES (%s) ON CONFLICT (input_id) DO NOTHING", (str(input_id),))


def reconcile_forward_work_cursor(cursor, *, cohort_id):
    cursor.execute("UPDATE rt.developer_forward_work AS work SET status='BLOCKED',reason='DEADLINE_OR_ATTEMPT_LIMIT',"
        "completed_at=clock_timestamp() FROM rt.developer_forward_inputs AS input "
        "WHERE work.input_id=input.input_id AND input.cohort_id=%s AND work.status IN ('PENDING','CLAIMED') "
        "AND (input.deadline<=clock_timestamp() OR work.attempts>=3 AND "
        "(work.status='PENDING' OR work.lease_expires_at<=clock_timestamp()))", (str(cohort_id),))
    return cursor.rowcount


def claim_forward_work_cursor(cursor, *, cohort_id, owner_id, lease_seconds=90):
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 90 or not isinstance(owner_id, UUID):
        raise ValueError("forward work lease/owner budget invalid")
    reconcile_forward_work_cursor(cursor, cohort_id=cohort_id)
    cursor.execute("UPDATE rt.developer_forward_work AS work SET status='CLAIMED',owner_id=%s,"
        "owner_epoch=work.owner_epoch+1,attempts=work.attempts+1,"
        "lease_expires_at=LEAST(input.deadline,clock_timestamp()+%s*INTERVAL '1 second') "
        "FROM rt.developer_forward_inputs AS input WHERE work.input_id=input.input_id AND work.input_id=("
        "SELECT pending.input_id FROM rt.developer_forward_work AS pending JOIN rt.developer_forward_inputs AS source USING(input_id) "
        "WHERE source.cohort_id=%s AND source.available_after<=clock_timestamp() AND source.deadline>clock_timestamp() "
        "AND pending.attempts<3 AND (pending.status='PENDING' OR pending.status='CLAIMED' AND pending.lease_expires_at<=clock_timestamp()) "
        "ORDER BY source.cutoff,pending.input_id FOR UPDATE OF pending SKIP LOCKED LIMIT 1) "
        "RETURNING work.input_id,work.owner_id,work.owner_epoch,work.lease_expires_at",
        (str(owner_id), lease_seconds, str(cohort_id)))
    row = _row(cursor.fetchone())
    return ForwardWorkClaim(UUID(str(row["input_id"])), UUID(str(row["owner_id"])), row["owner_epoch"], row["lease_expires_at"]) if row else None


def _claim_fence(cursor, claim):
    cursor.execute("SELECT work.*,input.deadline FROM rt.developer_forward_work AS work "
        "JOIN rt.developer_forward_inputs AS input USING(input_id) WHERE work.input_id=%s FOR UPDATE OF work", (str(claim.input_id),))
    row = _row(cursor.fetchone())
    now = _database_clock(cursor)
    if (not row or row["status"] != "CLAIMED" or str(row["owner_id"]) != str(claim.owner_id)
            or row["owner_epoch"] != claim.owner_epoch or now >= min(row["lease_expires_at"], row["deadline"])):
        raise ValueError("forward work lost its database owner/epoch/deadline fence")
    return now


def complete_forward_work_cursor(cursor, claim, scope, artifacts):
    now = _claim_fence(cursor, claim)
    identifiers = {}
    for kind, value in sorted(artifacts.items()):
        text, digest = encode_forward_artifact(value)
        identity = uuid5(NAMESPACE_URL, f"forward-artifact:{claim.input_id}:{kind}:BUNDLE")
        cursor.execute("INSERT INTO rt.developer_forward_artifacts (artifact_id,input_id,cohort_id,provider_tier,"
            "artifact_kind,artifact_key,scope_sha256,payload_text,payload_sha256,computed_at) "
            "VALUES (%s,%s,%s,'MASSIVE_DEVELOPER',%s,'BUNDLE',%s,%s,%s,%s) ON CONFLICT (artifact_id) DO NOTHING",
            (str(identity), str(claim.input_id), str(scope.cohort_id), kind, scope.sha256, text, digest, now))
        cursor.execute("SELECT payload_text,payload_sha256 FROM rt.developer_forward_artifacts WHERE artifact_id=%s", (str(identity),))
        stored = _row(cursor.fetchone())
        if not stored or (stored["payload_text"], stored["payload_sha256"]) != (text, digest):
            raise ValueError("immutable complete-matrix artifact conflict")
        identifiers[kind] = str(identity)
    _claim_fence(cursor, claim)
    cursor.execute("UPDATE rt.developer_forward_work SET status='COMPLETE',completed_at=clock_timestamp(),reason=NULL "
        "WHERE input_id=%s AND owner_id=%s AND owner_epoch=%s", (str(claim.input_id), str(claim.owner_id), claim.owner_epoch))
    return identifiers


def publish_forward_release_cursor(cursor, input_id, *, valid_until):
    inputs = load_forward_inputs_cursor(cursor, input_id)
    scope = inputs["scope"]
    now = _database_clock(cursor)
    if valid_until.utcoffset() is None or not now < valid_until:
        raise ValueError("forward publication serving expiry must follow database publication time")
    cursor.execute("SELECT * FROM rt.developer_forward_work WHERE input_id=%s", (str(input_id),))
    work = _row(cursor.fetchone())
    if not work or work["status"] != "COMPLETE" or now > scope.deadline:
        raise ValueError("forward release requires a completed timely compute cycle")
    cursor.execute("SELECT * FROM rt.developer_forward_artifacts WHERE input_id=%s ORDER BY artifact_kind", (str(input_id),))
    rows = tuple(dict(row) for row in cursor.fetchall())
    by_kind = {row["artifact_kind"]: row for row in rows}
    if set(by_kind) != {"MATRICES", "CONTEXT", "PACKAGES", "FLOW", "SCREENER", "ALERTS"}:
        raise ValueError("forward release requires one dependency-complete artifact group")
    for row in rows:
        if row["scope_sha256"] != scope.sha256 or str(row["cohort_id"]) != str(scope.cohort_id):
            raise ValueError("forward release artifact scope drift")
        decode_forward_artifact(row["payload_text"], row["payload_sha256"])
    alert_artifact = by_kind["ALERTS"]
    alert_value = decode_forward_artifact(alert_artifact["payload_text"], alert_artifact["payload_sha256"])
    alert_payload = alert_value.read(cohort_id=scope.cohort_id, provider_tier=alert_value.provider_tier, scope_sha256=scope.sha256)
    from options.analytics.alert_selection import DetectorSelectionEvidence, O3CreditEvaluationEvidence, load_evaluation_evidence

    for cycle in alert_payload.get("native_cycles", ()):
        for payload in cycle["record_payloads"]:
            record = load_evaluation_evidence(payload)
            if record.selection_status != "SELECTED" or record.scheduled_cycle != scope.cutoff:
                continue
            if isinstance(record, DetectorSelectionEvidence):
                deadline = record.package.entry_deadline
            elif isinstance(record, O3CreditEvaluationEvidence):
                deadline = record.observation.valid_until
            else:
                raise ValueError("unsupported selected native alert evidence")
            if deadline <= now:
                raise ValueError("native alert entry deadline elapsed before durable publication")
    manifest = dict(version="developer_forward_release_v1", input_id=str(input_id), scope_sha256=scope.sha256,
        cohort_id=str(scope.cohort_id), provider_tier="MASSIVE_DEVELOPER", cutoff=scope.cutoff.isoformat(),
        reference_sha256=scope.reference_sha256, action_sha256=scope.action_sha256,
        selection_sha256=scope.selection.selection_sha256, configuration_sha256=scope.configuration_sha256,
        references=tuple(dict(contract_id=row.contract_id, payload_sha256=row.payload_sha256) for row in scope.catalog),
        security_ids=tuple(str(row.security_id) for row in inputs["detector_source"].securities) if inputs["detector_source"] else (),
        artifacts={kind: dict(artifact_id=str(row["artifact_id"]), payload_sha256=row["payload_sha256"]) for kind, row in by_kind.items()})
    digest = canonical_sha256(manifest)
    release_id = uuid5(NAMESPACE_URL, f"forward-release:{input_id}")
    cursor.execute("INSERT INTO rt.developer_forward_releases (release_id,input_id,cohort_id,provider_tier,"
        "scope_sha256,manifest,manifest_sha256,published_at,valid_until) VALUES (%s,%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,%s) "
        "ON CONFLICT (release_id) DO NOTHING", (str(release_id), str(input_id), str(scope.cohort_id), scope.sha256,
            Json(manifest), digest, now, valid_until))
    cursor.execute("SELECT * FROM rt.developer_forward_releases WHERE release_id=%s", (str(release_id),))
    retained = _row(cursor.fetchone())
    if not retained or retained["manifest_sha256"] != digest or retained["valid_until"] != valid_until:
        raise ValueError("immutable release retry conflicts with original publication")
    cursor.execute("INSERT INTO rt.developer_forward_current (cohort_id,provider_tier,release_id,cutoff,published_at) "
        "VALUES (%s,'MASSIVE_DEVELOPER',%s,%s,%s) ON CONFLICT (cohort_id,provider_tier) DO UPDATE "
        "SET release_id=EXCLUDED.release_id,cutoff=EXCLUDED.cutoff,published_at=EXCLUDED.published_at "
        "WHERE rt.developer_forward_current.cutoff<EXCLUDED.cutoff OR rt.developer_forward_current.release_id=EXCLUDED.release_id",
        (str(scope.cohort_id), str(release_id), scope.cutoff, retained["published_at"]))
    if cursor.rowcount != 1:
        raise ValueError("forward release cannot replace a newer immutable current pointer")
    return release_id