"""Target-local production and stored-only, publication-pinned Dashboard reads."""
from copy import deepcopy
from datetime import datetime, timedelta

import exchange_calendars

from equity.calendar import latest_expected_market_time, staleness_state
from equity.ticker_contracts import setup_summary_row

from .contracts import canonical_sha256
from .dashboard_projection import (
    DASHBOARD_KEY, DASHBOARD_VERSION, DAILY_SCANNER_CONTRACTS, MEMBER_LISTS, SCANNER_BASIS,
    member_set_identities, produce_realtime_dashboard,
)
from .database_access import RealtimeDatabaseRole
from .lineage_head import require_member_set
from .portal_compatibility_repository import SNAPSHOT_RELATION, _RealtimeCompatibilityRepository, _utc
from .portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE, RETENTION_HOT_AND_KEPT, persist_portal_snapshot_cursor,
    portal_session_date, publish_portal_release_cursor,
)
from .screening_repository import load_realtime_screening_input
from .worker_database import RealtimeProcessDatabase


class DashboardPublicationUnavailable(LookupError):
    pass


class DashboardQueryRejected(ValueError):
    pass


def materialize_realtime_dashboard(database: RealtimeProcessDatabase,
                                   publication_database: RealtimeProcessDatabase, *,
                                   receipt=None, session_close=False, **scope):
    snapshot = compute_realtime_dashboard(database, **scope)
    with publication_database.transaction() as cursor:
        published, kept = persist_realtime_dashboard_cursor(
            cursor,
            snapshot,
            session_close=session_close,
        )
        if receipt is not None:
            publish_portal_release_cursor(cursor, receipt, published)
            if session_close:
                publish_portal_release_cursor(
                    cursor,
                    receipt,
                    kept,
                    release_class=RELEASE_CLASS_SESSION_CLOSE,
                    session_date=portal_session_date(snapshot),
                )
    return snapshot


def compute_realtime_dashboard(database: RealtimeProcessDatabase, **scope):
    if not isinstance(database, RealtimeProcessDatabase) or database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime Dashboard production requires the target-local COMPUTE role")
    with database.transaction(dict_cursor=True, read_only=True, isolation_level="REPEATABLE READ") as cursor:
        inputs = load_realtime_screening_input(
            cursor,
            include_portal_history=True,
            include_strategy_action_details=True,
            **scope,
        )
        return produce_realtime_dashboard(inputs)


def persist_realtime_dashboard_cursor(cursor, snapshot, *, session_close=False):
    if session_close:
        kept = persist_portal_snapshot_cursor(
            cursor,
            snapshot,
            retention=RETENTION_HOT_AND_KEPT,
        )
        return (snapshot,), (kept,)
    persist_portal_snapshot_cursor(cursor, snapshot)
    return (snapshot,), ()


class RealtimeDashboardRepository(_RealtimeCompatibilityRepository):
    def read(self, contract, *, available_by, cohort_id, snapshot_id=None, scan_date=None,
             min_swing_pct=5.0, interval="1d", short_period=9, long_period=21, lookback=50):
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise DashboardQueryRejected("Dashboard requires an explicit realtime cohort")
        if min_swing_pct != 5.0:
            raise DashboardQueryRejected("Only the stored default 5% Fibonacci publication is supported")
        if contract == "setup_summary" and interval != "1d":
            raise DashboardQueryRejected("All Tickers retains only the published daily setup summary")
        scanner_key = DAILY_SCANNER_CONTRACTS.get(contract)
        if scanner_key:
            if snapshot_id is None:
                raise DashboardQueryRejected("Daily scanners require an explicit Dashboard snapshot identity")
            if interval != "1d" or short_period != 9 or long_period != 21 or lookback != 50 or scan_date is not None:
                raise DashboardQueryRejected("Only latest-publication daily defaults are retained; historical/parameter variants are unavailable")
        with self._cursor() as cursor:
            self._begin(cursor)
            pinned = str(snapshot_id) if snapshot_id else None
            if pinned is None:
                release = self._release(cursor, cohort_id)
                entry = release.entry(DASHBOARD_VERSION, DASHBOARD_KEY) if release is not None else None
                if entry is None:
                    raise DashboardPublicationUnavailable("Dashboard has no retained publication for this cohort")
                pinned = str(entry["snapshot_id"])
            cursor.execute(
                "SELECT snapshot.*, manifest.manifest_sha256 AS cohort_manifest_sha256, manifest.stock_count "
                "FROM " + SNAPSHOT_RELATION + " "
                "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                "JOIN rt.screening_current AS screening_current ON screening_current.cohort_id=cohort.cohort_id "
                "JOIN rt.screening_generations AS screening ON screening.generation_id=screening_current.generation_id "
                "AND screening.cohort_id=screening_current.cohort_id AND screening.status='COMPLETE' "
                "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
                "AND manifest.created_at<=%s AND screening.generated_at<=%s "
                "AND snapshot.provider_tier=screening.provider_tier "
                "AND snapshot.snapshot_id=%s",
                (str(cohort_id), DASHBOARD_VERSION, DASHBOARD_KEY, cutoff, cutoff, cutoff, cutoff, cutoff, pinned),
            )
            stored = cursor.fetchone()
            if stored is None or stored["payload"] is None:
                raise DashboardPublicationUnavailable("Dashboard has no retained publication for this cohort")
            manifest, payload = stored["source_manifest"], stored["payload"]
            tickers, references = payload["tickers"], payload["references"]
            if (
                canonical_sha256(manifest) != stored["source_manifest_sha256"]
                or canonical_sha256(payload) != stored["payload_sha256"]
                or manifest["version"] != DASHBOARD_VERSION or payload["version"] != DASHBOARD_VERSION
                or str(stored["cohort_id"]) != str(cohort_id)
                or (snapshot_id is not None and str(stored["snapshot_id"]) != str(snapshot_id))
                or manifest["cohort_id"] != str(cohort_id) or payload["cohort_id"] != str(cohort_id)
                or manifest["source_manifest_sha256"] != stored["cohort_manifest_sha256"]
                or manifest["provider_tier"] != stored["provider_tier"]
                or manifest["source_generation"] != stored["source_generation"]
                or manifest["generated_at"] != stored["generated_at"].isoformat()
                or payload["generated_at"] != manifest["generated_at"]
                or payload["session"] != manifest["session"] or payload["market_time"] != manifest["market_time"]
                or manifest["expected_members"] != stored["stock_count"]
                or len(tickers) != stored["stock_count"] or len(set(tickers)) != len(tickers)
                or [row["ticker"] for row in payload["overview"]] != tickers
                or [row["ticker"] for row in payload["context"]["rows"]] != tickers
                or [row["ticker"] for row in references] != tickers
                or payload["scan"]["total_scanned"] != len(tickers)
                or payload["scan"]["scan_datetime"] != payload["generated_at"]
                or payload["provider_fetch_performed"] is not False or payload["execution_permission"] is not False
            ):
                raise ValueError("Dashboard stored publication identity/checksum/clock drift")
            # The manifest is a constant-size head; the member lists live in the payload and must match it.
            for identity, computed in member_set_identities(payload).items():
                require_member_set(manifest[identity], computed, name=f"Dashboard {identity}")
            if scan_date is not None and str(scan_date) != manifest["session"]:
                raise DashboardPublicationUnavailable("Requested Dashboard session is not in this retained publication")
            if scanner_key:
                scanners = payload.get("scanners", {})
                required = ("SCAN_GAPS_1D", "SCAN_FVG_1D_50") if contract in {"gaps", "fvg"} else (scanner_key,)
                if (
                    any(key not in scanners or key not in manifest.get("scanner_contracts", ()) for key in required)
                    or payload.get("scanner_basis") != SCANNER_BASIS
                    or manifest.get("scanner_basis") != SCANNER_BASIS
                ):
                    raise DashboardPublicationUnavailable("Default daily scanner contracts/basis are not retained in this publication")
                combined_keys = {
                    "SCAN_GAPS_1D": "gaps", "SCAN_MA_1D_9_21": "ma_crossover",
                    "SCAN_MOMENTUM_1D": "momentum_pullback", "SCAN_BEARISH_1D": "bearish_bounce",
                    "SCAN_FIBONACCI_1D_5": "fibonacci",
                }
                for key in required:
                    scanner = scanners[key]
                    rows = scanner["results"]
                    if (
                        scanner["scan_datetime"] != payload["generated_at"]
                        or scanner["interval"] != "1d" or scanner["total_scanned"] != len(tickers)
                        or scanner["total_signals"] != len(rows)
                        or any(row["ticker"] not in tickers for row in rows)
                        or (key in combined_keys and (
                            rows != payload["scan"][combined_keys[key]]["results"]
                            or scanner["total_signals"] != payload["scan"][combined_keys[key]]["total_signals"]
                        ))
                        or (key == "SCAN_FVG_1D_50" and scanner["lookback"] != 50)
                        or (key == "SCAN_FVG_1D_50" and any(
                            not {"ticker", "fvg_type", "fvg_low", "fvg_high", "fvg_size", "fvg_pct",
                                 "last_close", "gap_date", "status", "proximity", "streak_count", "trend"} <= row.keys()
                            for row in rows
                        ))
                        or (key == "SCAN_MA_1D_9_21" and (scanner["short_period"], scanner["long_period"]) != (9, 21))
                        or (key == "SCAN_FIBONACCI_1D_5" and scanner["min_swing_pct"] != 5.0)
                    ):
                        raise ValueError("Stored daily scanner shape/identity/default drift")
                    group_key = "results_by_signal" if key == "SCAN_MA_1D_9_21" else "results_by_type"
                    if key in {"SCAN_GAPS_1D", "SCAN_FVG_1D_50", "SCAN_MA_1D_9_21"}:
                        discriminator = {"SCAN_GAPS_1D": "gap_type", "SCAN_FVG_1D_50": "fvg_type",
                                         "SCAN_MA_1D_9_21": "signal"}[key]
                        groups = scanner[group_key]
                        if (
                            set(groups) != {row[discriminator] for row in rows}
                            or any(sorted(canonical_sha256(row) for row in grouped) != sorted(
                                canonical_sha256(row) for row in rows if row[discriminator] == label
                            ) for label, grouped in groups.items())
                        ):
                            raise ValueError("Stored daily scanner grouping drift")
            if contract == "setup_summary":
                summary = payload.get("setup_summary")
                evidence = payload.get("setup_evidence")
                if summary is None or evidence is None:
                    raise DashboardPublicationUnavailable("Daily setup summary has no retained publication")
                header_drift = (
                    summary["interval"] != "1d" or summary["source"] != "MATERIALIZED_CURRENT_PROJECTION"
                    or summary["computed_at"] != payload["generated_at"]
                    or summary["analysis_run_id"] != manifest["setup_analysis_run_id"]
                    or summary["count"] != len(tickers)
                    or [row["ticker"] for row in summary["results"]] != tickers
                    or [row["ticker"] for row in evidence] != tickers
                )
                if header_drift:
                    raise ValueError("All Tickers stored setup evidence/summary header drift")
                for row, reference, flattened in zip(evidence, references, summary["results"]):
                    drift = [
                        name for name, mismatched in (
                            ("payload_sha256", canonical_sha256(row["payload"]) != row["payload_sha256"]),
                            ("market_time", row["market_time"] != summary["market_time"]),
                            ("observed_at", row["observed_at"] != payload["generated_at"]),
                            ("analysis_run_id", row["analysis_run_id"] != summary["analysis_run_id"]),
                            ("security_revision_id", row["security_revision_id"]
                             != str(reference["security_revision_id"])),
                            ("flattened", setup_summary_row(row) != flattened),
                        )
                        if mismatched
                    ]
                    if drift:
                        raise ValueError(
                            "All Tickers stored setup evidence/summary row drift: "
                            f"{row['ticker']} fields={','.join(drift)}"
                        )
            deadline_expired = stored["valid_until"] is not None and cutoff >= stored["valid_until"]
            # A frozen session-close release stands on its own embedded hashes; only a live
            # (hot) publication must still match current reference and action data.
            if stored.get("snapshot_class") != "SESSION_CLOSE":
                for reference in references:
                    cursor.execute(
                        "SELECT revision_id, payload_sha256 FROM ref.security_revisions "
                        "WHERE security_id=%s AND ticker=%s AND effective_from<=%s "
                        "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                        "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                        (str(reference["security_id"]), reference["ticker"], cutoff, cutoff, cutoff),
                    )
                    revisions = cursor.fetchall()
                    if (len(revisions) != 1
                        or str(revisions[0]["revision_id"]) != str(reference["security_revision_id"])
                        or revisions[0]["payload_sha256"] != reference["payload_sha256"]):
                        raise DashboardPublicationUnavailable("Dashboard reference changed; republish required")
                ids = [str(reference["security_id"]) for reference in references]
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions "
                    "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) "
                    "OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
                    "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) AS action_drift",
                    (ids, stored["generated_at"], cutoff, ids, stored["generated_at"], cutoff),
                )
                if cursor.fetchone()["action_drift"]:
                    raise DashboardPublicationUnavailable("Dashboard action evidence changed; republish required")
        delay = 15 if stored["provider_tier"] == "MASSIVE_DEVELOPER" else 0
        actual = _utc(datetime.fromisoformat(payload["market_time"]), "Dashboard market time")
        session_close = exchange_calendars.get_calendar("XNYS").session_close(
            str(payload["session"])
        ).to_pydatetime()
        availability_interval = (
            "1d"
            if actual == session_close
            else "5m"
        )
        expected = latest_expected_market_time(
            cutoff - timedelta(minutes=delay),
            availability_interval,
        )
        if actual > expected:
            raise DashboardPublicationUnavailable("Dashboard is ahead of the provider-qualified watermark")
        state = staleness_state(actual, expected, is_fresh=actual == expected)
        if deadline_expired:
            state = dict(state, status="EXPIRED", is_serveable=False)
        if contract == "materialization_status":
            stale = state["status"] != "READY"
            return dict(
                status=state["status"], checked_at=cutoff.isoformat(),
                max_serveable_stale_sessions=state["max_serveable_stale_sessions"],
                staleness_sessions=state["staleness_sessions"],
                last_market_time=payload["market_time"],
                oldest_stale_market_time=payload["market_time"] if stale else None,
                stale_intervals=["1d"] if stale else [], pipeline_active=False,
                active_intervals=[], active_runs=[],
                intervals={"1d": dict(
                    market_time=payload["market_time"], published_at=payload["generated_at"],
                    expected_market_time=expected.isoformat(), **state,
                )},
            )
        result = deepcopy({key: value for key, value in payload.items() if key not in MEMBER_LISTS})
        publication = dict(
            cohort_id=str(stored["cohort_id"]),
            snapshot_id=str(stored["snapshot_id"]), payload_sha256=stored["payload_sha256"],
            status=state["status"], session=payload["session"], market_time=payload["market_time"],
            generated_at=payload["generated_at"], read_at=cutoff.isoformat(),
            expected_market_time=expected.isoformat(), scope_note=payload["scope_note"],
            price_basis=payload["price_basis"], missing_producers=payload["missing_producers"],
            strategy_ticker_available=(
                payload.get("scanner_basis") == SCANNER_BASIS
                and manifest.get("scanner_basis") == SCANNER_BASIS
                and all(
                    key in payload.get("scanners", {})
                    and key in manifest.get("scanner_contracts", ())
                    for key in DAILY_SCANNER_CONTRACTS.values()
                )
                and payload.get("setup_summary") is not None
                and payload.get("setup_evidence") is not None
            ),
        )
        if scanner_key:
            return dict(result["scanners"][scanner_key], publication=publication,
                        scanner_basis=deepcopy(payload["scanner_basis"]))
        if contract == "latest":
            return dict(latest_date=payload["session"], publication=publication)
        if contract == "overview":
            return result["overview"]
        if contract == "setup_summary":
            return dict(result["setup_summary"], is_fresh=state["status"] == "READY",
                        staleness_sessions=state["staleness_sessions"], publication=publication)
        if contract == "scan":
            return dict(result["scan"], publication=publication)
        if contract == "context":
            context = result["context"]
            context["cohort"].update(snapshot_id=publication["snapshot_id"], payload_sha256=publication["payload_sha256"],
                                     status=state["status"])
            context["status"] = state["status"]
            for row in context["rows"]:
                if row["daily"]["status"] == "READY":
                    row["daily"]["status"] = state["status"]
            return context
        raise DashboardQueryRejected("Unsupported stored Dashboard contract")
