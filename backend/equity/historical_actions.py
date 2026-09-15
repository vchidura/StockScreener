"""Response-bound corporate-action evidence for reconstructed security histories."""
from dataclasses import dataclass
from datetime import date, datetime
import json
from uuid import UUID

from .polygon import sha256_json


def validate_action_coverage(coverage, actions):
    count = coverage.get("response_action_count")
    digest = coverage.get("response_sha256")
    if count is None or digest is None:
        raise ValueError("LEGACY_SCOPE_ONLY_COVERAGE: response membership is not pinned")
    if coverage.get("security_id") is None:
        raise ValueError("coverage must identify its security")
    if type(count) is not int or count < 0:
        raise ValueError("invalid coverage response count")
    scope = {key: coverage[key] for key in ("ticker", "action_type")}
    scope.update(window_start=coverage["window_start"].isoformat(), window_end=coverage["window_end"].isoformat())
    if coverage["action_type"] not in ("SPLIT", "DIVIDEND") or sha256_json(scope) != coverage["payload_sha256"]:
        raise ValueError("invalid corporate-action coverage scope")
    identifiers = [UUID(str(row["corporate_action_id"])) for row in actions]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("duplicate linked corporate actions")
    source_keys = [row["source_key"] for row in actions]
    if len(set(source_keys)) != len(source_keys):
        raise ValueError("conflicting versions of one provider action in coverage response")
    hashes = []
    for row in actions:
        if UUID(str(row["security_id"])) != UUID(str(coverage["security_id"])):
            raise ValueError("covered action security identity conflict")
        if row["ticker"] != coverage["ticker"] or row["action_type"] != coverage["action_type"] \
                or not coverage["window_start"] <= row["effective_date"] <= coverage["window_end"]:
            raise ValueError("linked corporate action is outside its coverage scope")
        if sha256_json(row["raw_payload"]) != row["payload_sha256"]:
            raise ValueError("corporate-action payload checksum mismatch")
        hashes.append(row["payload_sha256"])
    if count != len(actions) or sha256_json(sorted(hashes)) != digest:
        raise ValueError("coverage response membership count or checksum mismatch")


@dataclass(frozen=True)
class HistoricalActionRead:
    actions_by_session_security: dict
    manifest: dict


def select_historical_actions(coverage_rows, actions_by_coverage, *, scopes, action_types,
                              source_cutoff, identity_evidence_sha256, pinned_coverage_ids=None):
    if source_cutoff.utcoffset() is None:
        raise ValueError("action source cutoff must be timezone-aware")
    if len(identity_evidence_sha256) != 64 or any(value not in "0123456789abcdef" for value in identity_evidence_sha256):
        raise ValueError("action selection requires an identity evidence hash")
    types = tuple(sorted(set(action_types)))
    if not types or set(types) - {"SPLIT", "DIVIDEND"}:
        raise ValueError("action coverage supports only explicit SPLIT/DIVIDEND requests")
    windows = sorted((row["ticker"], UUID(str(row["security_id"])), row["window_start"], row["window_end"]) for row in scopes)
    if not windows or any(start > end for _, _, start, end in windows):
        raise ValueError("nonempty valid dated security scopes are required")
    for position, (ticker, identity, start, end) in enumerate(windows):
        if any((ticker == previous[0] or identity == previous[1]) and start <= previous[3] and end >= previous[2]
               for previous in windows[:position]):
            raise ValueError("overlapping or ambiguous dated security action scopes")
    keys = [(ticker, identity, start, end, kind) for ticker, identity, start, end in windows for kind in types]
    pins = None if pinned_coverage_ids is None else {key: UUID(str(value)) for key, value in pinned_coverage_ids.items()}
    if pins is not None and set(pins) != set(keys):
        raise ValueError("coverage pins must cover every requested scope/type")
    indexed = {}
    for coverage in coverage_rows:
        indexed.setdefault((coverage["ticker"], coverage["action_type"]), []).append(coverage)
    linked = {UUID(str(key)): value for key, value in actions_by_coverage.items()}
    manifests = []
    selected_actions = {}
    for key in keys:
        ticker, identity, start, end, kind = key
        candidates = []
        for row in indexed.get((ticker, kind), []):
            if row["source"] != "POLYGON_CORPORATE_ACTIONS_V1" or row["availability_mode"] != "HISTORICAL_RECONSTRUCTED" \
                    or row["window_start"] > start or row["window_end"] < end:
                continue
            if pins is not None and UUID(str(row["coverage_id"])) != pins[key]:
                continue
            clocks = [row.get(field) for field in ("first_observed_at", "created_at", "replay_available_at")]
            if any(value is None or value.utcoffset() is None for value in clocks):
                raise ValueError("coverage availability clocks are missing or naive")
            if max(clocks) > source_cutoff:
                continue
            if row["replay_available_at"] > row["first_observed_at"]:
                raise ValueError("coverage replay availability exceeds observation")
            candidates.append(row)
        if not candidates:
            raise ValueError(f"ACTION_COVERAGE_MISSING: {ticker} {kind} {start}..{end}")
        candidates.sort(key=lambda row: (row["first_observed_at"], row["created_at"]), reverse=True)
        coverage = candidates[0]
        if len(candidates) > 1 and (coverage["first_observed_at"], coverage["created_at"]) == (
            candidates[1]["first_observed_at"], candidates[1]["created_at"],
        ):
            raise ValueError("ambiguous action coverage observation")
        coverage_id = UUID(str(coverage["coverage_id"]))
        if coverage_id not in linked:
            raise ValueError("coverage membership was not loaded")
        actions = linked[coverage_id]
        validate_action_coverage(coverage, actions)
        if UUID(str(coverage["security_id"])) != identity:
            raise ValueError("action coverage security identity conflict")
        for action in actions:
            if action["source"] != coverage["source"] or action["availability_mode"] != "HISTORICAL_RECONSTRUCTED":
                raise ValueError("action and coverage source contracts disagree")
            clocks = [action.get(field) for field in ("first_observed_at", "created_at", "replay_available_at")]
            if action.get("revised_observed_at") is not None:
                clocks.append(action["revised_observed_at"])
            if any(value is None or value.utcoffset() is None for value in clocks) or max(clocks) > source_cutoff:
                raise ValueError("linked action was not available by source cutoff")
            if action["first_observed_at"] > coverage["first_observed_at"]:
                raise ValueError("linked action was observed after its coverage response")
            if action["replay_available_at"] > action["first_observed_at"]:
                raise ValueError("action replay availability exceeds observation")
            if start <= action["effective_date"] <= end:
                selected_actions[UUID(str(action["corporate_action_id"]))] = dict(action)
        manifests.append({
            "scope": {"ticker": ticker, "security_id": str(identity), "window_start": start.isoformat(),
                      "window_end": end.isoformat(), "action_type": kind},
            "coverage": json.loads(json.dumps(dict(coverage), default=str)),
            "actions": [json.loads(json.dumps(dict(action), default=str)) for action in sorted(actions, key=lambda row: str(row["corporate_action_id"]))],
        })
    by_session = {}
    for action in sorted(selected_actions.values(), key=lambda row: (row["effective_date"], str(row["corporate_action_id"]))):
        by_session.setdefault((action["effective_date"], UUID(str(action["security_id"]))), []).append(action)
    manifest = {"contract": "RESPONSE_BOUND_ACTIONS_V1", "source_cutoff": source_cutoff.isoformat(),
                "identity_evidence_sha256": identity_evidence_sha256, "action_types": list(types), "selections": manifests}
    manifest["sha256"] = sha256_json(manifest)
    return HistoricalActionRead({key: tuple(value) for key, value in by_session.items()}, manifest)


def read_historical_actions(cursor, *, scopes, action_types, source_cutoff, identity_evidence_sha256, pinned_coverage_ids=None):
    if not scopes:
        raise ValueError("dated security action scopes are required")
    cursor.execute(
        """SELECT * FROM equity_corporate_action_coverage
           WHERE ticker = ANY(%s::TEXT[]) AND action_type = ANY(%s::TEXT[])
             AND window_start <= %s AND window_end >= %s
             AND first_observed_at <= %s AND created_at <= %s
             AND availability_mode = 'HISTORICAL_RECONSTRUCTED'
             AND (%s::UUID[] IS NULL OR coverage_id = ANY(%s::UUID[]))""",
        (sorted({row["ticker"] for row in scopes}), list(action_types), max(row["window_end"] for row in scopes),
         min(row["window_start"] for row in scopes), source_cutoff, source_cutoff,
         None if pinned_coverage_ids is None else [str(value) for value in pinned_coverage_ids.values()],
         None if pinned_coverage_ids is None else [str(value) for value in pinned_coverage_ids.values()]),
    )
    coverage = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """SELECT member.coverage_id, action.* FROM equity_corporate_action_coverage_members AS member
           LEFT JOIN equity_corporate_actions AS action USING (corporate_action_id)
           WHERE member.coverage_id = ANY(%s::UUID[])""", ([str(row["coverage_id"]) for row in coverage],),
    )
    actions = {UUID(str(row["coverage_id"])): [] for row in coverage}
    for record in cursor.fetchall():
        row = dict(record)
        coverage_id = UUID(str(row.pop("coverage_id")))
        if row.get("corporate_action_id") is None:
            raise ValueError("coverage references a missing action row")
        actions[coverage_id].append(row)
    return select_historical_actions(coverage, actions, scopes=scopes, action_types=action_types,
                                      source_cutoff=source_cutoff, identity_evidence_sha256=identity_evidence_sha256,
                                      pinned_coverage_ids=pinned_coverage_ids)


def reread_historical_actions(cursor, manifest):
    if manifest.get("contract") != "RESPONSE_BOUND_ACTIONS_V1" \
            or sha256_json({key: value for key, value in manifest.items() if key != "sha256"}) != manifest.get("sha256"):
        raise ValueError("invalid historical action manifest contract or checksum")
    scopes = {}
    pins = {}
    for selection in manifest["selections"]:
        row = selection["scope"]
        key = (row["ticker"], UUID(row["security_id"]), date.fromisoformat(row["window_start"]), date.fromisoformat(row["window_end"]))
        scope_key = (*key, row["action_type"])
        if scope_key in pins:
            raise ValueError("duplicate action coverage pin")
        scopes[key] = dict(ticker=key[0], security_id=key[1], window_start=key[2], window_end=key[3])
        pins[scope_key] = UUID(selection["coverage"]["coverage_id"])
    result = read_historical_actions(cursor, scopes=list(scopes.values()), action_types=manifest["action_types"],
                                     source_cutoff=datetime.fromisoformat(manifest["source_cutoff"]),
                                     identity_evidence_sha256=manifest["identity_evidence_sha256"], pinned_coverage_ids=pins)
    if result.manifest != manifest:
        raise ValueError("historical action manifest does not match retained evidence")
    return result