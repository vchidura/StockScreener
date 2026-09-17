"""Immutable, publication-bound context sidecars; independent of alert selection."""
import json
import os
from pathlib import Path
import tempfile

from research.stock_alert_context import shadow_event_decision, utc
from research.stock_idea_engine import digest


VERSION = "stock_alert_publication_context_v1"
MAX_BYTES = 2_000_000
FACTORS = ("market", "stock_daily", "sector", "native30m", "earnings", "fomc", "financials",
    "spy", "qqq", "sector_rotation", "stock_relative_rotation", "stock_divergence", "tracked_breadth",
    "spy_realized_volatility", "vix", "credit", "conditions_score", "market_volume", "sector_volume", "event_shadow")
BINDING_FIELDS = ("alert_id", "run_id", "security_id", "ticker", "model", "interval", "direction",
    "trigger_price", "stop", "target", "triggered_at", "published_at", "policy_version")


def binding(row):
    result = {field: row.get(field) for field in BINDING_FIELDS}
    for field in ("triggered_at", "published_at"):
        result[field] = utc(result[field]).isoformat()
    return result


def context_path(directory, source_id, run_id):
    return Path(directory) / (digest([VERSION, source_id, run_id]) + ".json")


def validate_context(bundle):
    if not isinstance(bundle, dict) or bundle.get("schema") != VERSION or bundle.get("bundle_sha256") != digest({key: value for key, value in bundle.items() if key != "bundle_sha256"}):
        raise ValueError("context schema or hash mismatch")
    cutoff = utc(bundle["input_cutoff"])
    if not cutoff <= utc(bundle["publication_at"]) <= utc(bundle["assembled_at"]):
        raise ValueError("context publication clocks mismatch")
    if (bundle["capture_mode"] != "RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS"
            or not isinstance(bundle["rows"], dict) or len(bundle["rows"]) > 1000):
        raise ValueError("unsupported context capture")
    for alert_id, row in bundle["rows"].items():
        if not isinstance(row, dict) or not isinstance(row.get("binding"), dict) or not isinstance(row.get("factors"), dict):
            raise ValueError("invalid context row")
        plan = row["binding"]
        if plan["alert_id"] != alert_id or plan["run_id"] != bundle["run_id"] or utc(plan["published_at"]) != utc(bundle["publication_at"]):
            raise ValueError("context plan identity mismatch")
        for factor in row["factors"].values():
            if not isinstance(factor, dict) or (factor.get("value") is not None and not isinstance(factor["value"], dict)):
                raise ValueError("invalid context factor")
            for field in ("release_at", "observed_at", "created_at", "available_at"):
                if factor.get(field) is not None and utc(factor[field]) > cutoff:
                    raise ValueError("context includes post-cutoff evidence")
            if factor["status"] == "READY" and (factor.get("value") is None or not factor.get("source_revision_ids")):
                raise ValueError("ready context lacks evidence")
    return bundle


def read_context(path):
    with Path(path).open("rb") as handle:
        payload = handle.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("context exceeds bounded reader size")
    return validate_context(json.loads(payload))


def freeze_publication_context(directory, publication, annotations, shared, *, assembled_at, manifest_sha256):
    if not annotations:
        return "EMPTY"
    rows = {}
    for annotation in annotations:
        alert_id = annotation["alert_id"]
        if (alert_id not in publication["selected"] or alert_id in rows
                or annotation["source_id"] != publication["policy_version"]
                or annotation["run_id"] != publication["window_key"]
                or utc(annotation["input_cutoff"]) != utc(publication["input_deadline"])):
            raise ValueError("annotation does not match retained publication")
        candidate = publication["candidates"][alert_id]
        if candidate["security_id"] != annotation["security_id"] or candidate["ticker"] != annotation["ticker"]:
            raise ValueError("annotation security mismatch")
        plan = dict(candidate, alert_id=alert_id, run_id=publication["window_key"],
            trigger_price=candidate["price"], triggered_at=candidate["trigger_at"], published_at=publication["actual_publication_at"])
        factors = dict(annotation["factors"], market=shared["market"])
        rows[alert_id] = dict(binding=binding(plan), factors={key: factors[key] for key in FACTORS if key in factors})
    if set(rows) != set(publication["selected"]):
        raise ValueError("publication context is incomplete")
    bundle = dict(schema=VERSION, source_id=publication["policy_version"], run_id=publication["window_key"],
        source_publication_sha256=digest(publication), input_cutoff=publication["input_deadline"],
        publication_at=publication["actual_publication_at"], assembled_at=assembled_at,
        capture_mode="RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS", readiness_manifest_sha256=manifest_sha256, rows=rows)
    bundle = json.loads(json.dumps(bundle, default=str, allow_nan=False))
    bundle["bundle_sha256"] = digest(bundle)
    validate_context(bundle)
    path = context_path(directory, bundle["source_id"], bundle["run_id"])
    return write_once_bundle(path, bundle, read_context)


def write_once_bundle(path, bundle, reader):
    payload = json.dumps(bundle, sort_keys=True, allow_nan=False).encode("utf-8")
    if len(payload) > MAX_BYTES:
        raise ValueError("context exceeds bounded storage size")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as handle:
            temporary = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = reader(path)
            if existing["source_publication_sha256"] != bundle["source_publication_sha256"]:
                raise ValueError("retained publication changed; context cannot be replaced")
            return "PRESERVED"
    finally:
        if temporary:
            Path(temporary).unlink(missing_ok=True)
    return "CREATED"


def read_event_shadow(path):
    with Path(path).open("rb") as handle:
        payload = handle.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("shadow record exceeds read bound")
    record = json.loads(payload)
    if (record.get("schema") != "stock_alert_event_shadow_v1" or record.get("live_gate_enabled") is not False
            or record.get("record_sha256") != digest({key: value for key, value in record.items() if key != "record_sha256"})):
        raise ValueError("invalid shadow record")
    return record


def freeze_event_shadow(directory, publication, annotations, *, assembled_at, manifest_sha256):
    indexed = {row["alert_id"]: row for row in annotations}
    if len(indexed) != len(annotations) or set(indexed) != set(publication["candidates"]):
        raise ValueError("shadow diagnostics require all retained candidates")
    dispositions = {row["episode_id"]: row for row in publication.get("dispositions", [])}
    rows = []
    for alert_id, candidate in sorted(publication["candidates"].items()):
        annotation = indexed[alert_id]
        if (annotation["security_id"] != candidate["security_id"] or annotation["ticker"] != candidate["ticker"]
                or annotation["run_id"] != publication["window_key"] or annotation["source_id"] != publication["policy_version"]
                or utc(annotation["input_cutoff"]) != utc(publication["input_deadline"])):
            raise ValueError("shadow candidate binding mismatch")
        factors = {name: annotation["factors"][name] for name in ("earnings", "fomc")}
        for factor in factors.values():
            for clock in ("available_at", "created_at", "observed_at"):
                if factor.get(clock) and utc(factor[clock]) > utc(publication["input_deadline"]):
                    raise ValueError("shadow factor after original cutoff")
        baseline = dispositions.get(alert_id, {})
        rows.append(dict(alert_id=alert_id, security_id=candidate["security_id"], ticker=candidate["ticker"],
            candidate_sha256=digest(candidate), baseline_selected=alert_id in publication["selected"],
            baseline_selection=baseline.get("selection"), baseline_reason=baseline.get("reason"),
            factors=factors, decision=shadow_event_decision(factors)))
    record = dict(schema="stock_alert_event_shadow_v1", source_id=publication["policy_version"], run_id=publication["window_key"],
        source_publication_sha256=digest(publication), readiness_manifest_sha256=manifest_sha256,
        input_cutoff=publication["input_deadline"], publication_at=publication["actual_publication_at"], assembled_at=assembled_at,
        live_gate_enabled=False, preselection_latency_validated=False,
        scope="ALL_RETAINED_PUBLICATION_CANDIDATES_NOT_FULL_UNIVERSE", quota_refill_simulated=False, rows=rows)
    record = json.loads(json.dumps(record, default=str, allow_nan=False))
    record["record_sha256"] = digest(record)
    return write_once_bundle(context_path(directory, record["source_id"], record["run_id"]), record, read_event_shadow)


def attach_publication_context(page, directory):
    if page["source"] != "SHADOW":
        return page
    cache, records = {}, []
    budget = 10_000_000
    for original in page["rows"]:
        run_id = original["run_id"]
        if run_id not in cache:
            path = context_path(directory, page["source_id"], run_id)
            try:
                size = path.stat().st_size
                if size > budget or size > MAX_BYTES:
                    raise ValueError("context page read budget exceeded")
                budget -= size
                bundle = read_context(path)
                if bundle["source_id"] != page["source_id"] or bundle["run_id"] != run_id:
                    raise ValueError("context source/run mismatch")
                cache[run_id] = (bundle, None)
            except FileNotFoundError:
                cache[run_id] = (None, "NO_SAVED_PUBLICATION_CONTEXT")
            except (OSError, ValueError, KeyError, TypeError, OverflowError, RecursionError):
                cache[run_id] = (None, "INVALID_SAVED_PUBLICATION_CONTEXT")
        bundle, reason = cache[run_id]
        context = dict(status="UNAVAILABLE", reason=reason, factors={})
        if bundle:
            stored = bundle["rows"].get(original["alert_id"])
            try:
                matches = stored is not None and stored["binding"] == binding(original)
            except (ValueError, TypeError):
                matches = False
            if matches:
                context = dict(status="AVAILABLE", reason=None, factors=stored["factors"],
                    input_cutoff=bundle["input_cutoff"], publication_at=bundle["publication_at"],
                    assembled_at=bundle["assembled_at"], capture_mode=bundle["capture_mode"],
                    bundle_sha256=bundle["bundle_sha256"], source_publication_sha256=bundle["source_publication_sha256"])
            else:
                context["reason"] = "CONTEXT_PLAN_BINDING_MISMATCH"
        records.append(dict(original, context=context))
    return dict(page, rows=records)