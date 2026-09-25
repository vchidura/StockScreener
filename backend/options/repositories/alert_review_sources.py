from __future__ import annotations

from uuid import UUID
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from options.alert_qualification import retained_candidate
from .base import PostgresRepository


def configured_detector_launch(*, backend_dir=None, environ=None):
    import os
    from pathlib import Path
    from dotenv import dotenv_values
    from options.detector_launch import decode_detector_forward_launch

    root = (backend_dir or Path(__file__).resolve().parents[2]).resolve()
    if environ is None:
        pinned = dotenv_values(root / ".env")
        environment = dict(os.environ,
            **{key: value for key, value in pinned.items()
               if key in {"OPTION_TECHNICAL_FORWARD_LAUNCH_FILE",
                          "OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256"} and value is not None})
    else:
        environment = environ
    source = environment.get("OPTION_TECHNICAL_FORWARD_LAUNCH_FILE")
    checksum = environment.get("OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256")
    if not source and not checksum:
        return None
    if not source or not checksum:
        raise ValueError("configured detector result version is incomplete")
    path = (root / source).resolve()
    if not path.is_relative_to(root) or path.stat().st_size > 262144:
        raise ValueError("configured detector result manifest path or size invalid")
    launch = decode_detector_forward_launch(path.read_text(encoding="utf-8"))
    if launch.sha256 != checksum:
        raise ValueError("configured detector result manifest checksum mismatch")
    return launch


def configured_detector_dataset(*, backend_dir=None, environ=None):
    launch = configured_detector_launch(backend_dir=backend_dir, environ=environ)
    return launch.dataset_id if launch else None


def detector_attempt_status(*, launch, as_of, completed=(), backend_dir=None):
    import json
    from pathlib import Path

    root = backend_dir or Path(__file__).resolve().parents[2]
    path = root / "backups/options-worker/detector-status.json"
    unavailable = dict(available=False, attempts=[])
    try:
        if path.stat().st_size > 131072:
            return unavailable
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (payload["version"] != "option_detector_operational_status_v1" or payload["dataset_id"] != launch.dataset_id
                or payload["configuration_sha256"] != launch.configuration_sha256
                or not isinstance(payload["attempts"], list) or len(payload["attempts"]) > 96):
            return unavailable
        attempts = []
        seen = set()
        for row in payload["attempts"]:
            cycle, started = (datetime.fromisoformat(row[key]) for key in ("scheduled_cycle", "started_at"))
            finished = datetime.fromisoformat(row["finished_at"]) if row.get("finished_at") else None
            if (cycle.utcoffset() is None or started.utcoffset() is None or finished and finished.utcoffset() is None
                    or not launch.effective_from <= cycle <= started <= as_of
                    or finished and not started <= finished <= as_of or cycle in seen
                    or row["status"] not in ("RUNNING", "FAILED", "RECORDED", "INCOMPLETE")
                    or row["status"] != "RUNNING" and finished is None):
                return unavailable
            seen.add(cycle)
            if cycle.date() != as_of.astimezone(ZoneInfo("America/New_York")).date() or cycle in completed:
                continue
            status = row["status"]
            if status == "RECORDED" or status == "RUNNING" and as_of - started > timedelta(minutes=30):
                status = "UNVERIFIED"
            reason = row.get("reason")
            if reason not in (None, "DETECTOR_SOURCE_TIMEOUT", "DETECTOR_EVALUATION_FAILED", "PIPELINE_EXECUTION_FAILED", "DETECTOR_NOT_EVALUATED"):
                reason = "DETECTOR_EVALUATION_FAILED"
            attempts.append(dict(scheduled_cycle=cycle.isoformat(), started_at=started.isoformat(),
                finished_at=finished.isoformat() if finished else None, status=status, reason=reason))
        return dict(available=True, attempts=sorted(attempts, key=lambda row: row["scheduled_cycle"]))
    except (OSError, ValueError, TypeError, KeyError):
        return unavailable


def detector_alert_schedule(*, as_of, effective_from, settings, completed=(), calendar=None):
    import exchange_calendars
    import pandas as pd

    if as_of.utcoffset() is None or effective_from.utcoffset() is None or settings.slot_seconds < 60:
        raise ValueError("alert schedule requires aware clocks and bounded cadence")
    calendar = calendar or exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(pd.Timestamp(max(as_of.astimezone(ZoneInfo("America/New_York")).date(),
        effective_from.astimezone(ZoneInfo("America/New_York")).date())), direction="next")
    interval = timedelta(seconds=settings.slot_seconds)
    delay = timedelta(seconds=settings.provider_delay_seconds + settings.publication_grace_seconds)
    completed = set(completed)
    missing = []
    for _ in range(2):
        opened = calendar.session_open(session).to_pydatetime()
        closed = calendar.session_close(session).to_pydatetime()
        cycle = opened + interval
        while cycle <= closed:
            start, end = cycle + delay, cycle + delay + interval
            if cycle >= effective_from:
                if end <= as_of and cycle not in completed:
                    missing.append(cycle)
                elif end > as_of and cycle not in completed:
                    return dict(dataset_timing="EXPECTED_CHECK_WINDOW_NOT_COMPLETION_DEADLINE", as_of=as_of.isoformat(),
                        scheduled_cycle=cycle.isoformat(), window_start=start.isoformat(), window_end=end.isoformat(),
                        status="DUE" if start <= as_of else "UPCOMING", warning=bool(missing),
                        unpublished_windows=len(missing), last_unpublished_cycle=missing[-1].isoformat() if missing else None)
            cycle += interval
        session = calendar.next_session(session)
    raise ValueError("next alert schedule unavailable")


class OptionAlertReviewSourceRepository(PostgresRepository):
    def window_completions(self, *, dataset_id, session_date, as_of):
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SELECT scheduled_cycle FROM option_board_publications "
                "WHERE selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND status='COMPLETE' "
                "AND selection_evidence->>'dataset_id'=%s AND as_of_session=%s "
                "AND published_at<=%s AND created_at<=%s ORDER BY scheduled_cycle LIMIT 1001",
                (dataset_id, session_date, as_of, as_of))
            records = [row["scheduled_cycle"] for row in cursor.fetchall()]
        if len(records) > 1000:
            raise ValueError("alert schedule completion bound exceeded")
        return records

    def dataset_index(self, *, as_of):
        if as_of.utcoffset() is None:
            raise ValueError("dataset index requires an aware cutoff")
        start = as_of.astimezone(ZoneInfo("America/New_York")).date() - timedelta(days=60)
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SELECT to_regclass('option_detector_evaluations') IS NOT NULL AS ready")
            if not cursor.fetchone()["ready"]:
                return dict(storage_ready=False, datasets=[])
            cursor.execute(
                "SELECT DISTINCT dataset_id FROM ("
                "SELECT dataset_id FROM option_detector_evaluations WHERE session_date>=%s AND selected_at<=%s AND recorded_at<=%s "
                "UNION SELECT selection_evidence->>'dataset_id' AS dataset_id FROM option_board_publications "
                "WHERE selector_version IN ('option_detector_run_v1','option_detector_run_v2') AND status='COMPLETE' "
                "AND as_of_session>=%s AND published_at<=%s AND created_at<=%s"
                ") AS versions ORDER BY dataset_id LIMIT 1001", (start, as_of, as_of, start, as_of, as_of),
            )
            datasets = [row["dataset_id"] for row in cursor.fetchall()]
        if len(datasets) > 1000:
            raise ValueError("dataset index exceeds bound")
        return dict(storage_ready=True, datasets=datasets)

    def original_packages(self, records, *, as_of):
        records = tuple(records)
        if len(records) > 200 or as_of.utcoffset() is None:
            raise ValueError("alert source display requires a bounded page and aware cutoff")
        packages = {record.candidate_id: record.package for record in records if record.candidate_id is not None}
        if not packages:
            return {}
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                "SELECT * FROM option_strategy_candidates WHERE candidate_id=ANY(%s::uuid[]) "
                "AND created_at<=%s AND observed_time<=%s ORDER BY candidate_id",
                (list(packages), as_of, as_of),
            )
            candidates = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT leg.*,snapshot.snapshot_id AS matched_snapshot_id,snapshot.underlying AS snapshot_underlying, "
                "snapshot.contract_id AS snapshot_contract_id,snapshot.batch_id,snapshot.normalized_payload_sha256, "
                "snapshot.model_mark AS snapshot_mark,snapshot.spot AS snapshot_spot,snapshot.local_iv AS snapshot_iv, "
                "snapshot.valuation_policy_sha256 AS snapshot_valuation_sha256,snapshot.day_volume,snapshot.open_interest, "
                "snapshot.first_observed_at,snapshot.revised_observed_at,snapshot.bid AS quote_bid,snapshot.ask AS quote_ask "
                "FROM option_candidate_legs AS leg LEFT JOIN option_chain_snapshots AS snapshot "
                "ON snapshot.snapshot_id=leg.snapshot_id AND snapshot.contract_id=leg.contract_id "
                "AND snapshot.created_at<=%s AND snapshot.first_observed_at<=%s "
                "WHERE leg.candidate_id=ANY(%s::uuid[]) ORDER BY leg.candidate_id,leg.leg_index LIMIT 401",
                (as_of, as_of, list(packages)),
            )
            legs = [dict(row) for row in cursor.fetchall()]
        if len(legs) > 400:
            raise ValueError("alert original-leg read exceeds page bound")
        by_candidate = {}
        for leg in legs:
            by_candidate.setdefault(leg["candidate_id"], []).append(leg)
        result = {}
        for row in candidates:
            package = packages[row["candidate_id"]]
            source_legs = by_candidate.get(row["candidate_id"], ())
            if (row["candidate_identity"] != package.candidate_identity_sha256 or row["matrix_id"] != package.matrix_id
                    or row["underlying"] != package.underlyer or row["observed_time"] > package.decision_at
                    or row["created_at"] > package.decision_at or not 1 <= len(source_legs) <= 2):
                raise ValueError("alert original candidate does not match frozen membership")
            candidate = retained_candidate(row, source_legs)
            if any(leg["matched_snapshot_id"] is None or UUID(str(leg["matched_snapshot_id"])) != leg["snapshot_id"]
                    or leg["snapshot_contract_id"] != leg["contract_id"] or leg["snapshot_underlying"] != package.underlyer
                    or leg["snapshot_mark"] != leg["model_mark"] or leg["snapshot_spot"] != leg["spot"]
                    or leg["snapshot_iv"] != leg["local_iv"] or leg["snapshot_valuation_sha256"] != leg["valuation_policy_sha256"]
                    or leg["first_observed_at"] > candidate.observed_time
                    or leg["revised_observed_at"] is not None for leg in source_legs):
                raise ValueError("alert original snapshot binding is unavailable or mismatched")
            fields = ("leg_index", "snapshot_id", "contract_id", "contract_ticker", "side", "ratio", "multiplier",
                "expiration_date", "strike", "contract_type", "spot", "model_mark", "local_iv", "local_delta",
                "local_gamma", "local_theta_per_day", "local_vega_per_vol_point", "local_rho_per_rate_point",
                "time_to_expiration_years", "risk_free_rate", "dividend_yield", "source_market_time", "mark_source",
                "model_version", "quality_flags", "valuation_policy_version", "valuation_policy_sha256", "batch_id",
                "normalized_payload_sha256", "day_volume", "open_interest", "quote_bid", "quote_ask")
            result[str(candidate.candidate_id)] = dict(status="AVAILABLE", basis="ORIGINAL_CANDIDATE_SNAPSHOTS",
                structure_type=candidate.structure_type.value, expiration_date=candidate.expiration_date,
                calendar_dte=(candidate.expiration_date - candidate.market_data_time.date()).days,
                market_data_time=candidate.market_data_time, observed_time=candidate.observed_time,
                net_premium=candidate.net_premium, capital_at_risk=candidate.capital_at_risk,
                maximum_loss=candidate.maximum_loss, maximum_profit=candidate.maximum_profit,
                breakevens=candidate.breakevens, legs=[{field: leg[field] for field in fields} for leg in source_legs])
        return result