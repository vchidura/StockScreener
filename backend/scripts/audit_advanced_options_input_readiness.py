#!/usr/bin/env python3
"""Bounded read-only audit of canonical Advanced Options input readiness."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_reference_repository import (  # noqa: E402
    decode_advanced_options_reference_state,
)
from realtime.advanced_options_reference_state import (  # noqa: E402
    ReferenceAvailability,
)
from realtime.advanced_options_rolling_repository import (  # noqa: E402
    decode_advanced_options_rolling_state,
)
from realtime.advanced_options_rolling_producer import (  # noqa: E402
    advanced_options_consumer_checkpoint_sha256,
    advanced_options_unresolved_gaps_sha256,
)
from realtime.advanced_options_rolling_state import (  # noqa: E402
    RollingAvailability,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402
from realtime.option_flow.volume import COMPLETE_BASELINES  # noqa: E402


UTC = timezone.utc
ADVANCED_NBBO_MAXIMUM_AGE_SECONDS = 1
SOURCE_ANCHORS = (
    "backend/realtime/advanced_options_postgres.py",
    "backend/realtime/advanced_options_loaders.py",
    "backend/realtime/advanced_options_input.py",
    "backend/realtime/advanced_options_mark_producer.py",
    "backend/realtime/advanced_options_native_loader.py",
    "backend/realtime/advanced_options_reference_state.py",
    "backend/realtime/advanced_options_rolling_producer.py",
    "backend/realtime/advanced_options_rolling_state.py",
    "backend/realtime/option_session_manifest.py",
)
REQUIRED_CATEGORIES = (
    "active_contracts",
    "active_slot_set",
    "dated_open_interest",
    "coherent_nbbo_marks",
    "treasury_inputs",
    "dividend_inputs",
    "model_inputs",
    "equity_input_receipt",
    "s1_finalized_window",
    "s2_finalized_window",
    "market_state_checkpoint",
    "flow_checkpoint",
    "unresolved_gaps",
    "source_clocks",
    "reference_generation",
    "rolling_state_generation",
    "options_input_ready_closure",
)
ALLOWED_STATEMENT_PREFIXES = ("SELECT", "WITH", "SHOW", "SET LOCAL")
FORBIDDEN_STATEMENT_PATTERN = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CALL|DO|ALTER|CREATE|DROP|"
    r"TRUNCATE|COPY|GRANT|REVOKE)\b"
)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include an explicit UTC offset"
        )
    return parsed.astimezone(UTC)


def _json(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return value.as_posix()
    raise TypeError(f"unsupported audit JSON value: {type(value).__name__}")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--session-date", type=date.fromisoformat, required=True)
    value.add_argument("--cutoff-utc", type=_timestamp, required=True)
    value.add_argument("--observed-at-utc", type=_timestamp, required=True)
    value.add_argument("--cohort-id", type=UUID)
    value.add_argument("--output", type=Path, required=True)
    value.add_argument("--allow-admin-read-only", action="store_true")
    return value


def _dsn(*, allow_admin_read_only: bool) -> tuple[str, str]:
    values = dotenv_values(BACKEND_DIR / ".env")
    api_dsn = str(
        os.getenv("REALTIME_V2_API_DSN")
        or values.get("REALTIME_V2_API_DSN")
        or ""
    ).strip()
    if not allow_admin_read_only:
        if not api_dsn:
            raise RuntimeError(
                "Options input-readiness audit requires REALTIME_V2_API_DSN"
            )
        return api_dsn, "API_READ_ONLY"
    admin_dsn = str(
        os.getenv("REALTIME_V2_ADMIN_DSN")
        or values.get("REALTIME_V2_ADMIN_DSN")
        or ""
    ).strip()
    if admin_dsn:
        return admin_dsn, "ADMIN_READ_ONLY"
    settings = {
        name: str(os.getenv(name) or values.get(name) or "").strip()
        for name in (
            "DB_HOST",
            "DB_PORT",
            "POSTGRES_ADMIN_USER",
            "POSTGRES_ADMIN_PASSWORD",
        )
    }
    missing = [name for name, setting in settings.items() if not setting]
    if missing:
        raise RuntimeError(
            "Options admin read-only audit requires: " + ", ".join(missing)
        )
    return make_dsn(
        dbname=EXPECTED_V2_DATABASE_NAME,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
    ), "ADMIN_READ_ONLY"


def _normalized_statement(statement: str) -> str:
    return re.sub(r"\s+", " ", statement).strip().upper()


class ReadOnlyCursor:
    def __init__(self, cursor) -> None:
        self._cursor = cursor
        self.statement_count = 0

    def execute(self, statement: str, parameters=()):
        normalized = _normalized_statement(statement)
        if (
            not normalized.startswith(ALLOWED_STATEMENT_PREFIXES)
            or FORBIDDEN_STATEMENT_PATTERN.search(normalized)
        ):
            raise RuntimeError(
                "Options readiness audit rejected a non-read-only statement"
            )
        self.statement_count += 1
        return self._cursor.execute(statement, parameters)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


def _source_evidence() -> dict[str, object]:
    anchors = {}
    for relative in SOURCE_ANCHORS:
        path = REPOSITORY_DIR / Path(relative)
        body = path.read_bytes()
        anchors[relative] = {
            "sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body),
        }
    scans = {
        "reference_state_builder_production_calls": (
            "build_advanced_options_reference_state("
        ),
        "rolling_state_builder_production_calls": (
            "build_advanced_options_rolling_state("
        ),
        "postgres_runtime_scope_production_calls": (
            "AdvancedOptionsPostgresRuntimeScope("
        ),
        "native_input_loader_production_calls": (
            "PostgresAdvancedOptionsNativeInputLoader("
        ),
        "mark_producer_production_calls": (
            "PostgresAdvancedOptionsMarkProducer("
        ),
    }
    counts = {key: 0 for key in scans}
    matches = {key: [] for key in scans}
    definition_files = {
        "reference_state_builder_production_calls": (
            "advanced_options_reference_state.py"
        ),
        "rolling_state_builder_production_calls": (
            "advanced_options_rolling_state.py"
        ),
        "postgres_runtime_scope_production_calls": (
            "advanced_options_postgres.py"
        ),
        "native_input_loader_production_calls": (
            "advanced_options_native_loader.py"
        ),
        "mark_producer_production_calls": (
            "advanced_options_mark_producer.py"
        ),
    }
    for root in (BACKEND_DIR / "realtime", BACKEND_DIR / "scripts"):
        for path in sorted(root.rglob("*.py")):
            if path == Path(__file__).resolve():
                continue
            text = path.read_text(encoding="utf-8")
            relative = path.relative_to(REPOSITORY_DIR).as_posix()
            for key, token in scans.items():
                count = text.count(token)
                if path.name == definition_files[key]:
                    count = max(0, count - 1)
                if count:
                    counts[key] += count
                    matches[key].append(relative)
    return {
        "anchors": anchors,
        "production_wiring_counts": counts,
        "production_wiring_matches": matches,
        "native_inputs_contract": {
            "scope_field": (
                "AdvancedOptionsPostgresRuntimeScope.native_inputs_loader"
            ),
            "canonical_loader": "not present outside tests",
            "reason_code": "CANONICAL_NATIVE_INPUT_LOADER_UNWIRED",
        },
        "mark_contract": {
            "policy": "option_valuation_advanced_nbbo_v1",
            "maximum_source_age_seconds": (
                ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
            ),
            "package_quote_skew_milliseconds": 250,
            "reason_code": "CANONICAL_MARK_PRODUCER_UNWIRED",
        },
    }


def _cohorts(cursor: ReadOnlyCursor, session_date: date):
    cursor.execute(
        """
        SELECT
            cohort.cohort_id::text,
            cohort.cohort_sha256,
            cohort.manifest_id::text,
            cohort.model_bundle_sha256,
            cohort.valuation_sha256,
            cohort.mode,
            cohort.effective_at,
            manifest.manifest_sha256,
            manifest.universe_sha256,
            manifest.profile_id::text,
            manifest.option_underlying_count,
            current.version AS active_slot_version,
            current.activated_at AS slot_activated_at,
            version.member_sha256 AS slot_member_sha256,
            version.primary_contract_count,
            version.reserve_contract_count
        FROM rt.cohorts AS cohort
        JOIN rt.subscription_manifests AS manifest
          ON manifest.manifest_id=cohort.manifest_id
        LEFT JOIN rt.slot_set_current AS current
          ON current.cohort_id=cohort.cohort_id
        LEFT JOIN rt.slot_set_versions AS version
          ON version.cohort_id=current.cohort_id
         AND version.version=current.version
        WHERE manifest.manifest_kind='MARKET_OPTIONS'
          AND manifest.session_date=%s
        ORDER BY cohort.effective_at,cohort.cohort_id
        """,
        (session_date,),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _select_cohort(
    rows: tuple[dict[str, object], ...],
    requested: UUID | None,
) -> tuple[dict[str, object] | None, tuple[str, ...]]:
    if requested is not None:
        selected = tuple(
            row for row in rows if row["cohort_id"] == str(requested)
        )
        if not selected:
            return None, ("REQUESTED_MARKET_OPTIONS_COHORT_ABSENT",)
        return selected[0], ()
    if not rows:
        return None, ("MARKET_OPTIONS_COHORT_ABSENT",)
    if len(rows) != 1:
        return None, ("MARKET_OPTIONS_COHORT_AMBIGUOUS",)
    return rows[0], ()


def _slot_members(cursor: ReadOnlyCursor, cohort_id: str):
    cursor.execute(
        """
        SELECT
            member.instrument_id,
            member.channel,
            member.purpose,
            member.priority,
            member.catalog_version_id,
            contract.contract_id,
            contract.contract_ticker,
            version.expiration,
            version.call_put,
            version.strike,
            version.eligibility,
            version.effective_from,
            version.effective_to
        FROM rt.slot_set_current AS current
        JOIN rt.slot_set_members AS member
          ON member.cohort_id=current.cohort_id
         AND member.version=current.version
        JOIN rt.instruments AS instrument
          ON instrument.instrument_id=member.instrument_id
         AND instrument.asset_class='OPTION'
        JOIN ref.option_contracts AS contract
          ON contract.contract_id=instrument.contract_id
        JOIN ref.option_contract_versions AS version
          ON version.version_id=member.catalog_version_id
         AND version.contract_id=contract.contract_id
        WHERE current.cohort_id=%s
        ORDER BY contract.contract_ticker,member.channel
        """,
        (cohort_id,),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _reference_rows(cursor: ReadOnlyCursor, session_date: date):
    cursor.execute(
        """
        SELECT
            state_id::text,
            state_sha256,
            reference_generation,
            known_at,
            valid_until,
            reference_policy_sha256,
            contract_scope_sha256,
            underlyings,
            member_count,
            state_document
        FROM rt.advanced_options_reference_states
        WHERE session_date=%s
        ORDER BY reference_generation DESC
        """,
        (session_date,),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _rolling_rows(
    cursor: ReadOnlyCursor,
    session_date: date,
    cohort_id: str | None,
):
    if cohort_id is None:
        cursor.execute(
            """
            SELECT
                state_id::text,
                state_sha256,
                cohort_id::text,
                journal_generation::text,
                owner_epoch,
                committed_offset,
                committed_at,
                market_state_checkpoint_sha256,
                flow_checkpoint_sha256,
                state_document
            FROM rt.advanced_options_rolling_checkpoints
            WHERE session_date=%s
            ORDER BY owner_epoch DESC,committed_offset DESC
            """,
            (session_date,),
        )
    else:
        cursor.execute(
            """
            SELECT
                state_id::text,
                state_sha256,
                cohort_id::text,
                journal_generation::text,
                owner_epoch,
                committed_offset,
                committed_at,
                market_state_checkpoint_sha256,
                flow_checkpoint_sha256,
                state_document
            FROM rt.advanced_options_rolling_checkpoints
            WHERE session_date=%s AND cohort_id=%s
            ORDER BY owner_epoch DESC,committed_offset DESC
            """,
            (session_date, cohort_id),
        )
    return tuple(dict(row) for row in cursor.fetchall())


def _source_flows(cursor: ReadOnlyCursor, cohort_id: str | None):
    if cohort_id is None:
        return ()
    cursor.execute(
        """
        SELECT
            source_flow_id::text,
            source_flow_sha256,
            journal_generation::text,
            stream_session_ids::text[],
            owner_name,
            condition_semantics_sha256,
            flow_policy_sha256,
            created_at
        FROM rt.option_source_flows
        WHERE source_cohort_id=%s
        ORDER BY created_at,source_flow_id
        """,
        (cohort_id,),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _quotes(cursor: ReadOnlyCursor, instrument_ids: tuple[int, ...]):
    if not instrument_ids:
        return ()
    cursor.execute(
        """
        SELECT
            instrument_id,
            source_session,
            event_at,
            received_at,
            bid,
            ask,
            bid_size,
            ask_size,
            quality,
            reason_codes,
            session_id::text,
            journal_generation::text,
            source_offset,
            payload_sha256
        FROM rt.quote_current
        WHERE provider='massive' AND instrument_id=ANY(%s)
        ORDER BY instrument_id
        """,
        (list(instrument_ids),),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _marks(
    cursor: ReadOnlyCursor,
    instrument_ids: tuple[int, ...],
    cutoff: datetime,
):
    if not instrument_ids:
        return ()
    cursor.execute(
        """
        SELECT
            evidence_id::text,
            instrument_id,
            contract_id,
            catalog_version_id,
            journal_generation::text,
            stream_session_id::text,
            source_offset,
            market_time,
            observed_time,
            bid,
            ask,
            mark,
            payload_sha256
        FROM rt.option_mark_evidence
        WHERE provider_tier='MASSIVE_ADVANCED'
          AND source_kind='ADVANCED_NBBO'
          AND mark_source='ADVANCED_NBBO_MIDPOINT'
          AND instrument_id=ANY(%s)
          AND observed_time<=%s
        ORDER BY instrument_id,observed_time DESC,evidence_id
        """,
        (list(instrument_ids), cutoff),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _receipts(
    cursor: ReadOnlyCursor,
    cohort_id: str | None,
    cutoff: datetime,
):
    if cohort_id is None:
        return ()
    cursor.execute(
        """
        SELECT
            receipt.receipt_id::text,
            receipt.window_id::text,
            receipt.cohort_id::text,
            receipt.receipt_kind,
            receipt.cutoff,
            receipt.reference_generation,
            receipt.universe_sha256,
            receipt.input_sha256,
            receipt.build_id,
            receipt.created_at,
            barrier.barrier_id::text,
            barrier.cohort_id::text AS barrier_cohort_id,
            barrier.universe_sha256 AS barrier_universe_sha256,
            barrier.state AS barrier_state,
            barrier.expected_count,
            barrier.completed_at,
            COUNT(member.member_key) AS barrier_member_count,
            COUNT(*) FILTER (
                WHERE member.outcome='COMPLETE'
                  AND member.outcome_input_sha256 IS NOT NULL
            ) AS complete_member_count,
            COUNT(*) FILTER (
                WHERE member.member_key='EQUITY_INPUT_READY'
                  AND member.job_id IS NULL
            ) AS equity_member_count,
            COUNT(job.job_id) AS materialization_job_count,
            COUNT(*) FILTER (
                WHERE job.bundle_sha256=receipt.input_sha256
                  AND job.build_id=receipt.build_id
            ) AS exact_materialization_job_count
        FROM rt.cycle_receipts AS receipt
        LEFT JOIN rt.job_barriers AS barrier
          ON barrier.window_id=receipt.window_id
         AND barrier.barrier_kind='OPTIONS_INPUT_READY'
        LEFT JOIN rt.job_barrier_members AS member
          ON member.barrier_id=barrier.barrier_id
        LEFT JOIN rt.materialization_jobs AS job
          ON job.job_id=member.job_id
        WHERE receipt.cutoff=%s AND (
            receipt.receipt_kind='INPUT_READY'
            OR (
                receipt.cohort_id=%s
                AND receipt.receipt_kind='OPTIONS_INPUT_READY'
            )
          )
        GROUP BY
            receipt.receipt_id,receipt.window_id,receipt.cohort_id,
            receipt.receipt_kind,
            receipt.cutoff,receipt.reference_generation,
            receipt.universe_sha256,receipt.input_sha256,receipt.build_id,
            receipt.created_at,barrier.barrier_id,barrier.state,
            barrier.cohort_id,barrier.universe_sha256,
            barrier.expected_count,barrier.completed_at
        ORDER BY receipt.receipt_kind,receipt.window_id,receipt.receipt_id
        """,
        (cutoff, cohort_id),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _bar_windows(
    cursor: ReadOnlyCursor,
    cohort_id: str | None,
    source_cohort_id: str | None,
    session_date: date,
    cutoff: datetime,
):
    if cohort_id is None or source_cohort_id is None:
        return ()
    cursor.execute(
        """
        WITH latest_security AS (
            SELECT DISTINCT ON (revision.security_id)
                revision.security_id,
                revision.ticker
            FROM ref.security_revisions AS revision
            WHERE revision.effective_from<=%s
            ORDER BY
                revision.security_id,
                revision.effective_from DESC,
                revision.observed_at DESC
        ),
        option_underlyings AS (
            SELECT DISTINCT
                instrument.instrument_id,
                instrument.security_id,
                security.ticker
            FROM rt.cohorts AS cohort
            JOIN rt.subscription_members AS member
              ON member.manifest_id=cohort.manifest_id
             AND member.option_underlying
            JOIN rt.instruments AS instrument
              ON instrument.instrument_id=member.instrument_id
            JOIN latest_security AS security
              ON security.security_id=instrument.security_id
            WHERE cohort.cohort_id=%s
        )
        SELECT
            underlying.ticker,
            COUNT(*) FILTER (
                WHERE revision.interval='1d'
                  AND revision.session_date<%s
            ) AS finalized_daily_count,
            MAX(revision.bar_end) FILTER (
                WHERE revision.interval='1d'
                  AND revision.session_date<%s
            ) AS latest_daily_bar_end,
            COUNT(*) FILTER (
                WHERE revision.interval='5m'
                  AND revision.session_date=%s
                  AND revision.bar_end<=%s
            ) AS finalized_intraday_count,
            MAX(revision.bar_end) FILTER (
                WHERE revision.interval='5m'
                  AND revision.session_date=%s
                  AND revision.bar_end<=%s
            ) AS latest_intraday_bar_end
        FROM option_underlyings AS underlying
        LEFT JOIN rt.bar_current AS current
          ON current.cohort_id=%s
         AND current.instrument_id=underlying.instrument_id
        LEFT JOIN rt.bar_revisions AS revision
          ON revision.bar_revision_id=current.bar_revision_id
         AND revision.observed_at=current.observed_at
         AND revision.cohort_id=current.cohort_id
         AND revision.provider_tier=current.provider_tier
         AND revision.instrument_id=current.instrument_id
         AND revision.interval=current.interval
         AND revision.bar_start=current.bar_start
         AND revision.session_scope=current.session_scope
         AND revision.adjustment_basis=current.adjustment_basis
         AND revision.is_final
         AND revision.availability_mode='LIVE_OBSERVED'
         AND revision.adjustment_basis='RAW_UNADJUSTED'
         AND revision.session_scope='RTH'
        GROUP BY underlying.ticker
        ORDER BY underlying.ticker
        """,
        (
            cutoff,
            cohort_id,
            session_date,
            session_date,
            session_date,
            cutoff,
            session_date,
            cutoff,
            source_cohort_id,
        ),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _native_stock_decisions(
    cursor: ReadOnlyCursor,
    *,
    equity_cohort_id: str | None,
    options_cohort_id: str | None,
    cutoff: datetime,
):
    if equity_cohort_id is None or options_cohort_id is None:
        return ()
    cursor.execute(
        """
        SELECT
            version.detector_id,
            event.event_id::text,
            revision.ticker,
            event.disposition,
            event.trigger_at,
            event.decision_at,
            event.detector_sha256,
            COUNT(input.input_key) FILTER (
                WHERE input.source_kind='BAR_REVISION'
            ) AS retained_bar_count,
            CASE
                WHEN event.disposition='DETECTED'
                THEN (
                    SELECT COUNT(*)
                    FROM rt.option_packages AS package
                    WHERE package.cohort_id=%s
                      AND package.event_id=(
                          event.payload->>'signal_id'
                      )::uuid
                )
                ELSE 0
            END AS package_count
        FROM rt.detector_events AS event
        JOIN rt.detector_versions AS version
          ON version.detector_sha256=event.detector_sha256
        JOIN rt.instruments AS instrument
          ON instrument.instrument_id=event.instrument_id
        JOIN LATERAL (
            SELECT ticker
            FROM ref.security_revisions
            WHERE security_id=instrument.security_id
              AND effective_from<=%s
            ORDER BY effective_from DESC,observed_at DESC
            LIMIT 1
        ) AS revision ON TRUE
        LEFT JOIN rt.detector_event_inputs AS input
          ON input.event_id=event.event_id
        WHERE event.cohort_id=%s
          AND event.trigger_at=%s
          AND version.detector_id IN (
              'ADVANCED_EQUITY_S1',
              'ADVANCED_EQUITY_S2'
          )
        GROUP BY
            version.detector_id,event.event_id,revision.ticker,
            event.disposition,event.trigger_at,event.decision_at,
            event.detector_sha256,event.payload
        ORDER BY version.detector_id,revision.ticker,event.event_id
        """,
        (options_cohort_id, cutoff, equity_cohort_id, cutoff),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _checkpoints(
    cursor: ReadOnlyCursor,
    cohort_id: str | None,
    generation: str | None,
):
    if cohort_id is None or generation is None:
        return ()
    cursor.execute(
        """
        SELECT
            consumer_name,
            journal_generation::text,
            shard_id,
            shard_map_sha256,
            owner_epoch,
            committed_offset,
            checkpoint_id::text,
            state_hash,
            committed_at,
            updated_at
        FROM rt.consumer_checkpoints
        WHERE cohort_id=%s AND journal_generation=%s
          AND consumer_name IN (
              'market-state-materializer-v1',
              'option_flow_stream_v1'
          )
        ORDER BY consumer_name,shard_id
        """,
        (cohort_id, generation),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _gaps(cursor: ReadOnlyCursor, session_ids: tuple[str, ...]):
    if not session_ids:
        return ()
    cursor.execute(
        """
        SELECT
            gap_id::text,
            session_id::text,
            instrument_id,
            channel,
            gap_start,
            gap_end,
            journal_generation::text,
            last_durable_offset,
            reason,
            recovery_status,
            recorded_at
        FROM rt.stream_gaps
        WHERE session_id=ANY(%s::uuid[])
          AND recovery_status IN ('UNRESOLVED','WARMING')
        ORDER BY gap_start,gap_id
        """,
        (list(session_ids),),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _source_watermarks(
    cursor: ReadOnlyCursor,
    session_ids: tuple[str, ...],
):
    if not session_ids:
        return ()
    cursor.execute(
        """
        SELECT
            session_id::text,
            manifest_id::text,
            owner_epoch,
            status,
            journal_generation::text,
            last_raw_offset,
            last_event_offset,
            event_time_watermark,
            heartbeat_at,
            started_at,
            subscribed_at,
            ended_at
        FROM rt.stream_sessions
        WHERE session_id=ANY(%s::uuid[])
        ORDER BY session_id
        """,
        (list(session_ids),),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _result(
    status: str,
    reasons: tuple[str, ...],
    *,
    evidence: Mapping[str, object],
    observed_at: datetime,
    source_clocks: Mapping[str, object] | None = None,
    freshness: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if status not in {"AVAILABLE", "COMPLETE_ZERO", "BLOCKED", "UNAVAILABLE"}:
        raise ValueError("invalid readiness classification")
    if status in {"AVAILABLE", "COMPLETE_ZERO"} and reasons:
        raise ValueError("ready classification cannot contain reason codes")
    if status in {"BLOCKED", "UNAVAILABLE"} and not reasons:
        raise ValueError("non-ready classification requires reason codes")
    return {
        "status": status,
        "reason_codes": list(dict.fromkeys(reasons)),
        "observed_at_utc": observed_at,
        "source_clocks": dict(source_clocks or {}),
        "freshness": dict(
            freshness or {
                "status": "NOT_APPLICABLE",
                "age_seconds": None,
                "maximum_age_seconds": None,
            }
        ),
        "evidence": dict(evidence),
    }


def _blocked(
    reasons: tuple[str, ...],
    *,
    evidence: Mapping[str, object],
    observed_at: datetime,
) -> dict[str, object]:
    return _result(
        "BLOCKED",
        reasons,
        evidence=evidence,
        observed_at=observed_at,
        freshness={
            "status": "ABSENT",
            "age_seconds": None,
            "maximum_age_seconds": None,
        },
    )


def _classify(
    snapshot: Mapping[str, object],
    source: Mapping[str, object],
    *,
    session_date: date,
    cutoff: datetime,
    observed_at: datetime,
) -> dict[str, dict[str, object]]:
    categories: dict[str, dict[str, object]] = {}
    cohort = snapshot["cohort"]
    cohort_reasons = tuple(snapshot["cohort_reason_codes"])
    slots = tuple(snapshot["slot_members"])
    by_contract: dict[int, list[Mapping[str, object]]] = {}
    for row in slots:
        by_contract.setdefault(int(row["contract_id"]), []).append(row)
    active_contract_ids = tuple(sorted(by_contract))
    invalid_contracts = tuple(
        rows[0]["contract_ticker"]
        for rows in by_contract.values()
        if (
            {row["channel"] for row in rows} != {"T", "Q"}
            or {row["catalog_version_id"] for row in rows}
            != {rows[0]["catalog_version_id"]}
            or rows[0]["eligibility"] != "VALIDATED_ACTIVE"
            or rows[0]["expiration"] < session_date
            or rows[0]["effective_from"] > cutoff
            or (
                rows[0]["effective_to"] is not None
                and rows[0]["effective_to"] <= cutoff
            )
        )
    )
    slot_evidence = {
        "table": "rt.slot_set_current/rt.slot_set_members",
        "cohort_id": None if cohort is None else cohort["cohort_id"],
        "row_count": len(slots),
        "contract_count": len(active_contract_ids),
        "invalid_contracts": invalid_contracts,
    }
    if cohort is None:
        categories["active_slot_set"] = _blocked(
            cohort_reasons,
            evidence=slot_evidence,
            observed_at=observed_at,
        )
        categories["active_contracts"] = _blocked(
            cohort_reasons + ("ACTIVE_SLOT_SET_ABSENT",),
            evidence=slot_evidence,
            observed_at=observed_at,
        )
    elif cohort["active_slot_version"] is None or not slots:
        categories["active_slot_set"] = _blocked(
            ("ACTIVE_SLOT_SET_ABSENT",),
            evidence=slot_evidence,
            observed_at=observed_at,
        )
        categories["active_contracts"] = _blocked(
            ("ACTIVE_CONTRACT_SET_ABSENT",),
            evidence=slot_evidence,
            observed_at=observed_at,
        )
    else:
        expected = int(cohort["primary_contract_count"]) + int(
            cohort["reserve_contract_count"]
        )
        if len(active_contract_ids) != expected:
            categories["active_slot_set"] = _blocked(
                ("ACTIVE_SLOT_MEMBER_COUNT_MISMATCH",),
                evidence={**slot_evidence, "expected_contract_count": expected},
                observed_at=observed_at,
            )
        else:
            categories["active_slot_set"] = _result(
                "AVAILABLE",
                (),
                evidence={**slot_evidence, "expected_contract_count": expected},
                observed_at=observed_at,
                source_clocks={
                    "activated_at": cohort["slot_activated_at"],
                },
            )
        if invalid_contracts:
            categories["active_contracts"] = _blocked(
                ("ACTIVE_CONTRACT_IDENTITY_OR_CHANNEL_INVALID",),
                evidence=slot_evidence,
                observed_at=observed_at,
            )
        else:
            categories["active_contracts"] = _result(
                "AVAILABLE",
                (),
                evidence=slot_evidence,
                observed_at=observed_at,
            )

    reference = snapshot["reference_state"]
    reference_error = snapshot["reference_identity_error"]
    reference_evidence = {
        "table": "rt.advanced_options_reference_states",
        "row_count": len(snapshot["reference_rows"]),
        "state_id": None if reference is None else reference.state_id,
        "state_sha256": None if reference is None else reference.state_sha256,
        "reference_generation": (
            None if reference is None else reference.reference_generation
        ),
        "identity_error": reference_error,
        "production_builder_calls": source["production_wiring_counts"][
            "reference_state_builder_production_calls"
        ],
    }
    reference_reasons = []
    if reference is None:
        reference_reasons.append(
            "REFERENCE_STATE_IDENTITY_INVALID"
            if reference_error
            else "REFERENCE_STATE_ABSENT"
        )
    elif not reference.known_at <= cutoff < reference.valid_until:
        reference_reasons.append("REFERENCE_STATE_STALE_OR_NONCAUSAL")
    if not active_contract_ids:
        reference_reasons.append("ACTIVE_CONTRACT_SCOPE_ABSENT")
    if (
        source["production_wiring_counts"][
            "reference_state_builder_production_calls"
        ]
        == 0
    ):
        reference_reasons.append("CANONICAL_REFERENCE_STATE_PRODUCER_UNWIRED")
    if reference_reasons:
        categories["reference_generation"] = _blocked(
            tuple(reference_reasons),
            evidence=reference_evidence,
            observed_at=observed_at,
        )
    else:
        categories["reference_generation"] = _result(
            "AVAILABLE",
            (),
            evidence=reference_evidence,
            observed_at=observed_at,
            source_clocks={
                "known_at": reference.known_at,
                "valid_until": reference.valid_until,
            },
            freshness={
                "status": "FRESH",
                "age_seconds": (
                    observed_at - reference.known_at
                ).total_seconds(),
                "maximum_age_seconds": (
                    reference.valid_until - reference.known_at
                ).total_seconds(),
            },
        )

    if reference is None or not active_contract_ids:
        categories["dated_open_interest"] = _blocked(
            tuple(reference_reasons) or ("DATED_OPEN_INTEREST_ABSENT",),
            evidence={
                **reference_evidence,
                "available_member_count": 0,
                "unavailable_member_count": 0,
            },
            observed_at=observed_at,
        )
        categories["treasury_inputs"] = _blocked(
            tuple(reference_reasons) or ("TREASURY_INPUTS_ABSENT",),
            evidence=reference_evidence,
            observed_at=observed_at,
        )
        categories["dividend_inputs"] = _blocked(
            tuple(reference_reasons) or ("DIVIDEND_INPUTS_ABSENT",),
            evidence=reference_evidence,
            observed_at=observed_at,
        )
        categories["model_inputs"] = _blocked(
            tuple(reference_reasons) or ("MODEL_INPUTS_ABSENT",),
            evidence=reference_evidence,
            observed_at=observed_at,
        )
    else:
        oi_by_contract = {
            row.contract_id: row for row in reference.open_interest
        }
        missing_oi = tuple(
            contract_id
            for contract_id in active_contract_ids
            if contract_id not in oi_by_contract
        )
        unavailable_oi = tuple(
            contract_id
            for contract_id in active_contract_ids
            if contract_id in oi_by_contract
            and oi_by_contract[contract_id].availability
            is ReferenceAvailability.UNAVAILABLE
        )
        oi_evidence = {
            **reference_evidence,
            "active_contract_count": len(active_contract_ids),
            "oi_member_count": len(reference.open_interest),
            "missing_contract_ids": missing_oi,
            "unavailable_contract_ids": unavailable_oi,
        }
        if missing_oi or unavailable_oi:
            categories["dated_open_interest"] = _blocked(
                tuple(
                    code
                    for present, code in (
                        (missing_oi, "DATED_OPEN_INTEREST_MISSING"),
                        (unavailable_oi, "DATED_OPEN_INTEREST_UNAVAILABLE"),
                    )
                    if present
                ),
                evidence=oi_evidence,
                observed_at=observed_at,
            )
        else:
            categories["dated_open_interest"] = _result(
                "AVAILABLE",
                (),
                evidence=oi_evidence,
                observed_at=observed_at,
                source_clocks={
                    "latest_observed_at": max(
                        row.observed_at for row in reference.open_interest
                    ),
                    "latest_received_at": max(
                        row.received_at for row in reference.open_interest
                    ),
                },
            )
        valuations = reference.valuation_inputs
        rates = tuple(
            rate for valuation in valuations
            for rate in valuation.rate_observations
        )
        treasury_reasons = ()
        if not rates:
            treasury_reasons = ("TREASURY_INPUTS_ABSENT",)
        categories["treasury_inputs"] = (
            _blocked(
                treasury_reasons,
                evidence={"valuation_count": len(valuations), "rate_count": 0},
                observed_at=observed_at,
            )
            if treasury_reasons
            else _result(
                "AVAILABLE",
                (),
                evidence={
                    "valuation_count": len(valuations),
                    "rate_count": len(rates),
                    "observation_ids": tuple(
                        str(row.observation_id) for row in rates
                    ),
                },
                observed_at=observed_at,
                source_clocks={
                    "latest_observed_at": max(
                        row.observed_at for row in rates
                    ),
                    "latest_received_at": max(
                        row.received_at for row in rates
                    ),
                },
            )
        )
        coverages = tuple(
            row.dividend_coverage for row in valuations
        )
        if not coverages:
            categories["dividend_inputs"] = _blocked(
                ("DIVIDEND_INPUTS_ABSENT",),
                evidence={"valuation_count": len(valuations)},
                observed_at=observed_at,
            )
        else:
            dividend_status = (
                "COMPLETE_ZERO"
                if all(
                    row.status == "KNOWN_NO_ANNOUNCED_DIVIDEND"
                    for row in coverages
                )
                else "AVAILABLE"
            )
            categories["dividend_inputs"] = _result(
                dividend_status,
                (),
                evidence={
                    "coverage_count": len(coverages),
                    "coverage_ids": tuple(
                        str(row.coverage_id) for row in coverages
                    ),
                    "statuses": tuple(row.status for row in coverages),
                },
                observed_at=observed_at,
                source_clocks={
                    "latest_observed_at": max(
                        row.observed_at for row in coverages
                    ),
                    "latest_received_at": max(
                        row.received_at for row in coverages
                    ),
                },
            )
        model_reasons = []
        if not reference.historical_volatility:
            model_reasons.append("HISTORICAL_VOLATILITY_INPUTS_ABSENT")
        if not reference.equity_metrics:
            model_reasons.append("EQUITY_MODEL_INPUTS_ABSENT")
        if cohort is not None and any(
            row.model_bundle_sha256 != cohort["model_bundle_sha256"]
            or row.valuation_policy_sha256 != cohort["valuation_sha256"]
            for row in valuations
        ):
            model_reasons.append("MODEL_OR_VALUATION_IDENTITY_MISMATCH")
        categories["model_inputs"] = (
            _blocked(
                tuple(model_reasons),
                evidence=reference_evidence,
                observed_at=observed_at,
            )
            if model_reasons
            else _result(
                "AVAILABLE",
                (),
                evidence={
                    **reference_evidence,
                    "historical_volatility_count": len(
                        reference.historical_volatility
                    ),
                    "equity_metric_count": len(reference.equity_metrics),
                    "valuation_count": len(valuations),
                },
                observed_at=observed_at,
            )
        )

    source_flows = tuple(snapshot["source_flows"])
    rolling = snapshot["rolling_state"]
    rolling_error = snapshot["rolling_identity_error"]
    rolling_evidence = {
        "table": "rt.advanced_options_rolling_checkpoints",
        "row_count": len(snapshot["rolling_rows"]),
        "state_id": None if rolling is None else rolling.state_id,
        "state_sha256": None if rolling is None else rolling.state_sha256,
        "journal_generation": (
            None if rolling is None else rolling.journal_generation
        ),
        "owner_epoch": None if rolling is None else rolling.owner_epoch,
        "committed_offset": (
            None if rolling is None else rolling.committed_offset
        ),
        "identity_error": rolling_error,
        "production_builder_calls": source["production_wiring_counts"][
            "rolling_state_builder_production_calls"
        ],
    }
    rolling_reasons = []
    binding = None if rolling is None else rolling.producer_binding
    if rolling is None:
        rolling_reasons.append(
            "ROLLING_STATE_IDENTITY_INVALID"
            if rolling_error
            else "ROLLING_STATE_ABSENT"
        )
    elif rolling.committed_at > cutoff:
        rolling_reasons.append("ROLLING_STATE_FUTURE")
    elif binding is None:
        rolling_reasons.append("ROLLING_PRODUCER_BINDING_ABSENT")
    else:
        if (
            reference is None
            or binding.reference_state_id != reference.state_id
            or binding.reference_state_sha256 != reference.state_sha256
            or binding.reference_generation != reference.reference_generation
        ):
            rolling_reasons.append(
                "ROLLING_REFERENCE_GENERATION_MISMATCH"
            )
        if not binding.known_at <= cutoff < binding.valid_until:
            rolling_reasons.append("ROLLING_PRODUCER_CLOCKS_INVALID")
        if any(
            row.availability is not RollingAvailability.READY
            for row in rolling.underlying_ticks
        ):
            rolling_reasons.append("ROLLING_UNDERLYING_TICKS_INCOMPLETE")
        if any(
            row.completeness not in COMPLETE_BASELINES
            for row in rolling.contract_volumes
        ):
            rolling_reasons.append(
                "ROLLING_VOLUME_CHECKPOINTS_INCOMPLETE"
            )
        if (
            len(rolling.order_windows.members)
            > rolling.order_windows.maximum_members
        ):
            rolling_reasons.append("ROLLING_CLUSTER_WINDOW_UNBOUNDED")
    if len(source_flows) != 1:
        rolling_reasons.append(
            "SOURCE_FLOW_IDENTITY_ABSENT"
            if not source_flows
            else "SOURCE_FLOW_IDENTITY_AMBIGUOUS"
        )
    elif (
        rolling is not None
        and str(rolling.journal_generation)
        != source_flows[0]["journal_generation"]
    ):
        rolling_reasons.append("ROLLING_SOURCE_GENERATION_MISMATCH")
    if (
        binding is not None
        and len(source_flows) == 1
        and (
            str(binding.source_flow_id)
            != source_flows[0]["source_flow_id"]
            or binding.source_flow_sha256
            != source_flows[0]["source_flow_sha256"]
        )
    ):
        rolling_reasons.append("ROLLING_SOURCE_FLOW_IDENTITY_MISMATCH")
    if (
        source["production_wiring_counts"][
            "rolling_state_builder_production_calls"
        ]
        == 0
    ):
        rolling_reasons.append("CANONICAL_ROLLING_STATE_PRODUCER_UNWIRED")
    if rolling_reasons:
        categories["rolling_state_generation"] = _blocked(
            tuple(rolling_reasons),
            evidence=rolling_evidence,
            observed_at=observed_at,
        )
    else:
        categories["rolling_state_generation"] = _result(
            "AVAILABLE",
            (),
            evidence=rolling_evidence,
            observed_at=observed_at,
            source_clocks={"committed_at": rolling.committed_at},
        )

    checkpoints = tuple(snapshot["checkpoints"])
    for category, owner, absent_reason in (
        (
            "market_state_checkpoint",
            "market-state-materializer-v1",
            "MARKET_STATE_CHECKPOINT_ABSENT",
        ),
        (
            "flow_checkpoint",
            "option_flow_stream_v1",
            "FLOW_CHECKPOINT_ABSENT",
        ),
    ):
        rows = tuple(
            row for row in checkpoints if row["consumer_name"] == owner
        )
        reasons = list(rolling_reasons)
        if not rows:
            reasons.append(absent_reason)
        elif rolling is not None and min(
            int(row["committed_offset"]) for row in rows
        ) < rolling.committed_offset:
            reasons.append(f"{owner.upper().replace('-', '_')}_LAGS_ROLLING")
        elif rolling is not None and any(
            int(row["committed_offset"]) != rolling.committed_offset
            or int(row["owner_epoch"]) != rolling.owner_epoch
            or str(row["journal_generation"])
            != str(rolling.journal_generation)
            for row in rows
        ):
            reasons.append(
                f"{owner.upper().replace('-', '_')}_OFFSET_DIVERGED"
            )
        elif rolling is not None and rows:
            expected_digest = getattr(
                rolling,
                f"{category}_sha256",
            )
            if advanced_options_consumer_checkpoint_sha256(rows) != (
                expected_digest
            ):
                reasons.append(
                    f"{owner.upper().replace('-', '_')}_DIGEST_MISMATCH"
                )
        evidence = {
            "table": "rt.consumer_checkpoints",
            "consumer_name": owner,
            "row_count": len(rows),
            "committed_offsets": tuple(
                int(row["committed_offset"]) for row in rows
            ),
            "rolling_digest": (
                None
                if rolling is None
                else getattr(rolling, f"{category}_sha256")
            ),
        }
        categories[category] = (
            _blocked(
                tuple(reasons),
                evidence=evidence,
                observed_at=observed_at,
            )
            if reasons
            else _result(
                "AVAILABLE",
                (),
                evidence=evidence,
                observed_at=observed_at,
                source_clocks={
                    "latest_committed_at": max(
                        row["committed_at"] for row in rows
                    )
                },
            )
        )

    gaps = tuple(snapshot["gaps"])
    gap_digest = advanced_options_unresolved_gaps_sha256(gaps)
    if len(source_flows) != 1:
        categories["unresolved_gaps"] = _blocked(
            (
                "SOURCE_FLOW_IDENTITY_ABSENT"
                if not source_flows
                else "SOURCE_FLOW_IDENTITY_AMBIGUOUS",
            ),
            evidence={
                "table": "rt.stream_gaps",
                "unresolved_count": len(gaps),
            },
            observed_at=observed_at,
        )
    elif gaps:
        categories["unresolved_gaps"] = _blocked(
            ("UNRESOLVED_OR_WARMING_STREAM_GAPS",),
            evidence={
                "table": "rt.stream_gaps",
                "unresolved_count": len(gaps),
                "gaps": gaps,
            },
            observed_at=observed_at,
        )
    elif (
        binding is not None
        and binding.unresolved_gaps_sha256 != gap_digest
    ):
        categories["unresolved_gaps"] = _blocked(
            ("UNRESOLVED_GAP_DIGEST_MISMATCH",),
            evidence={
                "table": "rt.stream_gaps",
                "unresolved_count": 0,
                "digest": gap_digest,
            },
            observed_at=observed_at,
        )
    else:
        categories["unresolved_gaps"] = _result(
            "COMPLETE_ZERO",
            (),
            evidence={
                "table": "rt.stream_gaps",
                "unresolved_count": 0,
                "stream_session_ids": source_flows[0][
                    "stream_session_ids"
                ],
            },
            observed_at=observed_at,
        )

    quotes = tuple(snapshot["quotes"])
    quote_by_instrument = {
        int(row["instrument_id"]): row for row in quotes
    }
    invalid_quotes = []
    quote_ages = {}
    generation = (
        None if len(source_flows) != 1
        else source_flows[0]["journal_generation"]
    )
    for instrument_id in tuple(
        sorted({int(row["instrument_id"]) for row in slots})
    ):
        quote = quote_by_instrument.get(instrument_id)
        if quote is None:
            invalid_quotes.append((instrument_id, "OPTION_NBBO_ABSENT"))
            continue
        age = (observed_at - quote["received_at"]).total_seconds()
        quote_ages[instrument_id] = age
        if (
            quote["source_session"] != session_date
            or quote["event_at"] > quote["received_at"]
            or quote["received_at"] > observed_at
        ):
            invalid_quotes.append((instrument_id, "OPTION_NBBO_CLOCK_INVALID"))
        elif age > ADVANCED_NBBO_MAXIMUM_AGE_SECONDS:
            invalid_quotes.append((instrument_id, "OPTION_NBBO_STALE"))
        elif (
            quote["quality"] != "GOOD"
            or quote["bid"] is None
            or quote["ask"] is None
            or quote["bid_size"] is None
            or quote["ask_size"] is None
            or quote["bid"] <= 0
            or quote["ask"] <= 0
            or quote["ask"] < quote["bid"]
            or quote["bid_size"] <= 0
            or quote["ask_size"] <= 0
        ):
            invalid_quotes.append((instrument_id, "OPTION_NBBO_UNUSABLE"))
        elif generation is None or quote["journal_generation"] != generation:
            invalid_quotes.append(
                (instrument_id, "OPTION_NBBO_GENERATION_MISMATCH")
            )
    mark_reasons = []
    if not active_contract_ids:
        mark_reasons.append("ACTIVE_CONTRACT_SCOPE_ABSENT")
    mark_reasons.extend(reason for _, reason in invalid_quotes)
    mark_rows = tuple(snapshot.get("marks", ()))
    latest_marks = {}
    for row in mark_rows:
        latest_marks.setdefault(int(row["instrument_id"]), row)
    slot_catalog = {
        int(row["instrument_id"]): int(row["catalog_version_id"])
        for row in slots
    }
    invalid_marks = []
    for instrument_id in tuple(sorted(slot_catalog)):
        mark = latest_marks.get(instrument_id)
        quote = quote_by_instrument.get(instrument_id)
        if mark is None:
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_MARK_ABSENT")
            )
            continue
        age = (cutoff - mark["observed_time"]).total_seconds()
        if int(mark["catalog_version_id"]) != slot_catalog[instrument_id]:
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_CATALOG_MISMATCH")
            )
        elif generation is None or mark["journal_generation"] != generation:
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_GENERATION_MISMATCH")
            )
        elif (
            mark["market_time"] > mark["observed_time"]
            or mark["observed_time"] > cutoff
            or not 0 <= age <= ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
        ):
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_MARK_STALE_OR_NONCAUSAL")
            )
        elif (
            mark["bid"] is None
            or mark["ask"] is None
            or mark["mark"] is None
            or mark["bid"] <= 0
            or mark["ask"] < mark["bid"]
            or mark["mark"] != (
                mark["bid"] + mark["ask"]
            ) / Decimal(2)
            or mark["mark"] <= 0
        ):
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_MARK_INVALID")
            )
        elif (
            rolling is None
            or int(mark["source_offset"]) > rolling.committed_offset
        ):
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_MARK_AHEAD_OF_ROLLING")
            )
        elif quote is None or (
            mark["market_time"] != quote["event_at"]
            or mark["observed_time"] != quote["received_at"]
            or mark["bid"] != quote["bid"]
            or mark["ask"] != quote["ask"]
            or mark["source_offset"] != quote["source_offset"]
            or mark["stream_session_id"] != quote["session_id"]
        ):
            invalid_marks.append(
                (instrument_id, "ADVANCED_NBBO_SOURCE_QUOTE_MISMATCH")
            )
    mark_times = tuple(
        row["market_time"] for row in latest_marks.values()
    )
    if mark_times and (
        max(mark_times) - min(mark_times)
    ).total_seconds() * 1000 > 250:
        invalid_marks.append((None, "PACKAGE_QUOTE_TIME_SKEW_EXCEEDED"))
    mark_reasons.extend(reason for _, reason in invalid_marks)
    if (
        source["production_wiring_counts"].get(
            "mark_producer_production_calls",
            0,
        )
        == 0
    ):
        mark_reasons.append("CANONICAL_MARK_PRODUCER_UNWIRED")
    mark_evidence = {
            "table": "rt.quote_current",
            "mark_table": "rt.option_mark_evidence",
            "active_instrument_count": len(
                {int(row["instrument_id"]) for row in slots}
            ),
            "quote_count": len(quotes),
            "mark_count": len(mark_rows),
            "invalid_quotes": tuple(invalid_quotes),
            "invalid_marks": tuple(invalid_marks),
            "maximum_source_age_seconds": (
                ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
            ),
            "package_quote_skew_milliseconds": 250,
            "mark_source": "ADVANCED_NBBO_MIDPOINT",
            "production_mark_producer_calls": source[
                "production_wiring_counts"
            ].get("mark_producer_production_calls", 0),
        }
    categories["coherent_nbbo_marks"] = (
        _blocked(
            tuple(mark_reasons),
            evidence=mark_evidence,
            observed_at=observed_at,
        )
        if mark_reasons
        else _result(
            "AVAILABLE",
            (),
            evidence=mark_evidence,
            observed_at=observed_at,
            source_clocks={
                "cutoff": cutoff,
                "latest_mark_observed_at": max(
                    row["observed_time"] for row in latest_marks.values()
                ),
            },
        )
    )

    bars = tuple(snapshot["bar_windows"])
    native_decisions = tuple(snapshot.get("native_stock_decisions", ()))
    for category, needed, detector_id in (
        ("s1_finalized_window", 2, "ADVANCED_EQUITY_S1"),
        ("s2_finalized_window", 3, "ADVANCED_EQUITY_S2"),
    ):
        missing = tuple(
            row["ticker"]
            for row in bars
            if int(row["finalized_daily_count"]) < 15
            or int(row["finalized_intraday_count"]) < needed
        )
        reasons = []
        if cohort is None or not bars:
            reasons.append("EQUITY_FINALIZED_BAR_SCOPE_ABSENT")
        if missing:
            reasons.append(
                f"{category[:2].upper()}_FINALIZED_WINDOW_INCOMPLETE"
            )
        decisions = tuple(
            row
            for row in native_decisions
            if row["detector_id"] == detector_id
        )
        by_ticker: dict[str, list[Mapping[str, object]]] = {}
        for row in decisions:
            by_ticker.setdefault(str(row["ticker"]), []).append(row)
        expected_tickers = {str(row["ticker"]) for row in bars}
        if (
            set(by_ticker) != expected_tickers
            or any(len(rows) != 1 for rows in by_ticker.values())
        ):
            reasons.append(
                f"{category[:2].upper()}_DECISION_SCOPE_INCOMPLETE"
            )
        elif any(
            row["disposition"] not in {"DETECTED", "COMPLETE_ZERO"}
            or row["decision_at"] > cutoff
            or int(row["retained_bar_count"]) < 15 + needed
            or (
                row["disposition"] == "DETECTED"
                and int(row["package_count"]) != 1
            )
            or (
                row["disposition"] == "COMPLETE_ZERO"
                and int(row["package_count"]) != 0
            )
            for row in decisions
        ):
            reasons.append(
                f"{category[:2].upper()}_DECISION_EVIDENCE_INVALID"
            )
        if (
            source["production_wiring_counts"].get(
                "native_input_loader_production_calls",
                0,
            )
            == 0
        ):
            reasons.append("CANONICAL_NATIVE_INPUT_LOADER_UNWIRED")
        lane_evidence = {
                "table": "rt.bar_current/rt.bar_revisions",
                "required_daily_bars": 15,
                "required_intraday_bars": needed,
                "members": bars,
                "decision_table": (
                    "rt.detector_events/rt.detector_event_inputs/"
                    "rt.option_packages"
                ),
                "decisions": decisions,
                "production_loader_calls": source[
                    "production_wiring_counts"
                ].get("native_input_loader_production_calls", 0),
            }
        categories[category] = (
            _blocked(
                tuple(reasons),
                evidence=lane_evidence,
                observed_at=observed_at,
            )
            if reasons
            else _result(
                "AVAILABLE",
                (),
                evidence=lane_evidence,
                observed_at=observed_at,
                source_clocks={
                    "cutoff": cutoff,
                    "latest_decision_at": max(
                        row["decision_at"] for row in decisions
                    ),
                },
            )
        )

    receipts = tuple(snapshot["receipts"])
    equity = tuple(
        row for row in receipts if row["receipt_kind"] == "INPUT_READY"
    )
    options = tuple(
        row
        for row in receipts
        if row["receipt_kind"] == "OPTIONS_INPUT_READY"
    )
    equity_identities = {
        (
            row["cohort_id"],
            row["cutoff"],
            int(row["reference_generation"]),
            row["universe_sha256"],
            row["input_sha256"],
            row["build_id"],
        )
        for row in equity
    }
    if not equity:
        categories["equity_input_receipt"] = _blocked(
            ("EXACT_EQUITY_INPUT_READY_RECEIPT_ABSENT",),
            evidence={
                "table": "rt.cycle_receipts",
                "cutoff": cutoff,
                "row_count": 0,
            },
            observed_at=observed_at,
        )
    elif len(equity_identities) != 1:
        categories["equity_input_receipt"] = _blocked(
            ("EQUITY_INPUT_READY_RECEIPT_IDENTITY_DISAGREEMENT",),
            evidence={
                "table": "rt.cycle_receipts",
                "cutoff": cutoff,
                "row_count": len(equity),
            },
            observed_at=observed_at,
        )
    else:
        categories["equity_input_receipt"] = _result(
            "AVAILABLE",
            (),
            evidence={
                "table": "rt.cycle_receipts",
                "cutoff": cutoff,
                "row_count": len(equity),
                "receipt_ids": tuple(row["receipt_id"] for row in equity),
            },
            observed_at=observed_at,
            source_clocks={
                "latest_created_at": max(row["created_at"] for row in equity),
            },
        )

    source_clock_reasons = []
    if len(source_flows) != 1:
        source_clock_reasons.append("SOURCE_FLOW_IDENTITY_ABSENT")
    if reference is None:
        source_clock_reasons.append("REFERENCE_CLOCKS_ABSENT")
    if rolling is None:
        source_clock_reasons.append("ROLLING_CLOCKS_ABSENT")
    watermarks = tuple(snapshot.get("source_watermarks", ()))
    watermark_digest = canonical_sha256(tuple(
        tuple(row.values()) for row in watermarks
    ))
    if binding is not None and (
        not watermarks
        or binding.source_watermarks_sha256 != watermark_digest
    ):
        source_clock_reasons.append("SOURCE_WATERMARK_DIGEST_MISMATCH")
    categories["source_clocks"] = (
        _blocked(
            tuple(source_clock_reasons),
            evidence={
                "source_flow_count": len(source_flows),
                "reference_state_count": len(snapshot["reference_rows"]),
                "rolling_state_count": len(snapshot["rolling_rows"]),
                "source_watermark_count": len(watermarks),
                "cutoff": cutoff,
            },
            observed_at=observed_at,
        )
        if source_clock_reasons
        else _result(
            "AVAILABLE",
            (),
            evidence={
                "source_flow_id": source_flows[0]["source_flow_id"],
                "journal_generation": source_flows[0][
                    "journal_generation"
                ],
                "source_watermarks_sha256": watermark_digest,
            },
            observed_at=observed_at,
            source_clocks={
                "reference_known_at": reference.known_at,
                "reference_valid_until": reference.valid_until,
                "rolling_committed_at": rolling.committed_at,
                "cutoff": cutoff,
            },
        )
    )

    dependency_blockers = tuple(
        key
        for key in REQUIRED_CATEGORIES
        if key != "options_input_ready_closure"
        and key in categories
        and categories[key]["status"] not in {"AVAILABLE", "COMPLETE_ZERO"}
    )
    closure_reasons = []
    if not options:
        closure_reasons.append("OPTIONS_INPUT_READY_RECEIPT_ABSENT")
    elif len(options) != 1:
        closure_reasons.append("OPTIONS_INPUT_READY_RECEIPT_AMBIGUOUS")
    else:
        receipt = options[0]
        if (
            receipt["barrier_state"] != "COMPLETE"
            or receipt["expected_count"] is None
            or int(receipt["barrier_member_count"])
            != int(receipt["expected_count"])
            or int(receipt["complete_member_count"])
            != int(receipt["expected_count"])
        ):
            closure_reasons.append("OPTIONS_INPUT_READY_BARRIER_INCOMPLETE")
        if (
            receipt["barrier_cohort_id"] != receipt["cohort_id"]
            or receipt["barrier_universe_sha256"]
            != receipt["universe_sha256"]
        ):
            closure_reasons.append(
                "OPTIONS_INPUT_READY_BARRIER_SCOPE_MISMATCH"
            )
        expected_jobs = (
            None
            if receipt["expected_count"] is None
            else int(receipt["expected_count"]) - 1
        )
        if (
            int(receipt["equity_member_count"]) != 1
            or expected_jobs is None
            or int(receipt["materialization_job_count"])
            != expected_jobs
        ):
            closure_reasons.append(
                "OPTIONS_INPUT_READY_MEMBER_IDENTITY_MISMATCH"
            )
        elif (
            int(receipt["exact_materialization_job_count"])
            != expected_jobs
        ):
            closure_reasons.append(
                "OPTIONS_INPUT_READY_BUILD_OR_CLOSURE_MISMATCH"
            )
        if (
            reference is None
            or int(receipt["reference_generation"])
            != reference.reference_generation
        ):
            closure_reasons.append(
                "OPTIONS_INPUT_READY_REFERENCE_GENERATION_MISMATCH"
            )
    if dependency_blockers:
        closure_reasons.append("OPTIONS_INPUT_READY_DEPENDENCIES_BLOCKED")
    categories["options_input_ready_closure"] = (
        _blocked(
            tuple(closure_reasons),
            evidence={
                "table": "rt.cycle_receipts/rt.job_barriers/"
                "rt.job_barrier_members",
                "cutoff": cutoff,
                "receipt_count": len(options),
                "dependency_blockers": dependency_blockers,
                "receipts": options,
            },
            observed_at=observed_at,
        )
        if closure_reasons
        else _result(
            "AVAILABLE",
            (),
            evidence={
                "receipt_id": options[0]["receipt_id"],
                "input_sha256": options[0]["input_sha256"],
                "reference_generation": options[0][
                    "reference_generation"
                ],
            },
            observed_at=observed_at,
            source_clocks={
                "receipt_created_at": options[0]["created_at"],
                "barrier_completed_at": options[0]["completed_at"],
            },
        )
    )
    if set(categories) != set(REQUIRED_CATEGORIES):
        raise RuntimeError("readiness audit omitted a required input category")
    return categories


def _snapshot(
    cursor: ReadOnlyCursor,
    *,
    session_date: date,
    cutoff: datetime,
    observed_at: datetime,
    requested_cohort_id: UUID | None,
) -> dict[str, object]:
    cohorts = _cohorts(cursor, session_date)
    cohort, cohort_reasons = _select_cohort(cohorts, requested_cohort_id)
    cohort_id = None if cohort is None else str(cohort["cohort_id"])
    slots = _slot_members(cursor, cohort_id) if cohort_id else ()
    references = _reference_rows(cursor, session_date)
    reference = None
    reference_error = None
    if references:
        row = references[0]
        try:
            reference = decode_advanced_options_reference_state(
                state_id=UUID(row["state_id"]),
                state_sha256=str(row["state_sha256"]),
                document=row["state_document"],
            )
        except (TypeError, ValueError, KeyError) as error:
            reference_error = f"{type(error).__name__}:{error}"
    rolling_rows = _rolling_rows(cursor, session_date, cohort_id)
    rolling = None
    rolling_error = None
    if rolling_rows:
        row = rolling_rows[0]
        try:
            rolling = decode_advanced_options_rolling_state(
                state_id=UUID(row["state_id"]),
                state_sha256=str(row["state_sha256"]),
                document=row["state_document"],
            )
        except (TypeError, ValueError, KeyError) as error:
            rolling_error = f"{type(error).__name__}:{error}"
    flows = _source_flows(cursor, cohort_id)
    generation = (
        flows[0]["journal_generation"] if len(flows) == 1 else None
    )
    session_ids = (
        tuple(flows[0]["stream_session_ids"]) if len(flows) == 1 else ()
    )
    instrument_ids = tuple(
        sorted({int(row["instrument_id"]) for row in slots})
    )
    receipts = _receipts(cursor, cohort_id, cutoff)
    equity_cohort_ids = tuple(sorted({
        str(row["cohort_id"])
        for row in receipts
        if row["receipt_kind"] == "INPUT_READY"
    }))
    equity_cohort_id = (
        equity_cohort_ids[0] if len(equity_cohort_ids) == 1 else None
    )
    return {
        "cohorts": cohorts,
        "cohort": cohort,
        "cohort_reason_codes": cohort_reasons,
        "slot_members": slots,
        "reference_rows": references,
        "reference_state": reference,
        "reference_identity_error": reference_error,
        "rolling_rows": rolling_rows,
        "rolling_state": rolling,
        "rolling_identity_error": rolling_error,
        "source_flows": flows,
        "quotes": _quotes(cursor, instrument_ids),
        "marks": _marks(cursor, instrument_ids, cutoff),
        "receipts": receipts,
        "bar_windows": _bar_windows(
            cursor,
            cohort_id,
            equity_cohort_id,
            session_date,
            cutoff,
        ),
        "native_stock_decisions": _native_stock_decisions(
            cursor,
            equity_cohort_id=equity_cohort_id,
            options_cohort_id=cohort_id,
            cutoff=cutoff,
        ),
        "checkpoints": _checkpoints(cursor, cohort_id, generation),
        "gaps": _gaps(cursor, session_ids),
        "source_watermarks": _source_watermarks(cursor, session_ids),
        "observed_at": observed_at,
    }


def _next_actionable_blocker(
    categories: Mapping[str, Mapping[str, object]],
) -> dict[str, str] | None:
    order = (
        "MARKET_OPTIONS_COHORT_ABSENT",
        "ACTIVE_SLOT_SET_ABSENT",
        "CANONICAL_REFERENCE_STATE_PRODUCER_UNWIRED",
        "CANONICAL_ROLLING_STATE_PRODUCER_UNWIRED",
        "CANONICAL_NATIVE_INPUT_LOADER_UNWIRED",
        "CANONICAL_MARK_PRODUCER_UNWIRED",
        "OPTIONS_INPUT_READY_RECEIPT_ABSENT",
    )
    present = {
        reason
        for category in categories.values()
        for reason in category["reason_codes"]
    }
    actions = {
        "MARKET_OPTIONS_COHORT_ABSENT": (
            "Apply the separately authorized exact MARKET_OPTIONS cohort action."
        ),
        "ACTIVE_SLOT_SET_ABSENT": (
            "Apply the separately authorized exact active T/Q slot action."
        ),
        "CANONICAL_REFERENCE_STATE_PRODUCER_UNWIRED": (
            "Wire and preflight the canonical reference-state producer."
        ),
        "CANONICAL_ROLLING_STATE_PRODUCER_UNWIRED": (
            "Wire and preflight the canonical rolling-state producer."
        ),
        "CANONICAL_NATIVE_INPUT_LOADER_UNWIRED": (
            "Wire the canonical native S1/S2 input loader without activating "
            "production stages."
        ),
        "CANONICAL_MARK_PRODUCER_UNWIRED": (
            "Wire the exact canonical NBBO midpoint mark producer."
        ),
        "OPTIONS_INPUT_READY_RECEIPT_ABSENT": (
            "Produce the immutable OPTIONS_INPUT_READY closure only after all "
            "input categories are exact."
        ),
    }
    for reason in order:
        if reason in present:
            return {"reason_code": reason, "action": actions[reason]}
    return None


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.cutoff_utc.date() != args.session_date:
        raise ValueError("Options readiness cutoff must match session date")
    if args.observed_at_utc < args.cutoff_utc:
        raise ValueError("Options readiness observation cannot predate cutoff")
    output = args.output.resolve()
    if not output.parent.is_dir():
        raise FileNotFoundError(
            "Options readiness report output parent is absent"
        )
    source = _source_evidence()
    dsn, database_access = _dsn(
        allow_admin_read_only=args.allow_admin_read_only
    )
    with psycopg2.connect(dsn) as connection:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor(cursor_factory=RealDictCursor) as raw_cursor:
            cursor = ReadOnlyCursor(raw_cursor)
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute(
                "SELECT current_database() AS database,"
                "clock_timestamp() AS database_observed_at,"
                "current_setting('transaction_read_only') AS read_only,"
                "current_setting('transaction_isolation') AS isolation"
            )
            database_metadata = dict(cursor.fetchone())
            if database_metadata["database"] != EXPECTED_V2_DATABASE_NAME:
                raise RuntimeError(
                    "Options readiness audit targets the wrong database"
                )
            if database_metadata["read_only"] != "on":
                raise RuntimeError(
                    "Options readiness audit transaction is not read-only"
                )
            snapshot = _snapshot(
                cursor,
                session_date=args.session_date,
                cutoff=args.cutoff_utc,
                observed_at=args.observed_at_utc,
                requested_cohort_id=args.cohort_id,
            )
            statement_count = cursor.statement_count
        connection.rollback()
    categories = _classify(
        snapshot,
        source,
        session_date=args.session_date,
        cutoff=args.cutoff_utc,
        observed_at=args.observed_at_utc,
    )
    blocking_categories = tuple(
        name
        for name in REQUIRED_CATEGORIES
        if categories[name]["status"] == "BLOCKED"
    )
    status = "AVAILABLE" if not blocking_categories else "BLOCKED"
    next_actionable_blocker = _next_actionable_blocker(categories)
    report = {
        "version": "advanced-options-input-readiness-audit-v1",
        "status": status,
        "scope": {
            "session_date": args.session_date,
            "cutoff_utc": args.cutoff_utc,
            "observed_at_utc": args.observed_at_utc,
            "requested_cohort_id": args.cohort_id,
            "database": EXPECTED_V2_DATABASE_NAME,
            "database_access": database_access,
        },
        "database_snapshot": {
            **database_metadata,
            "market_options_cohort_count": len(snapshot["cohorts"]),
            "reference_state_count": len(snapshot["reference_rows"]),
            "rolling_state_count": len(snapshot["rolling_rows"]),
            "source_flow_count": len(snapshot["source_flows"]),
        },
        "source_evidence": source,
        "required_inputs": categories,
        "accepted_unavailable_lanes": {
            "O2": {
                "status": "UNAVAILABLE",
                "reason_codes": ["O2_MODEL_NOT_ACCEPTED"],
            },
            "IV_SURFACE": {
                "status": "UNAVAILABLE",
                "reason_codes": ["O2_MODEL_NOT_ACCEPTED"],
            },
            "DARK_POOL": {
                "status": "UNAVAILABLE",
                "reason_codes": ["DARK_POOL_OWNER_NOT_ACCEPTED"],
            },
        },
        "blocking_categories": blocking_categories,
        "blocker_order": [
            "MARKET_OPTIONS_COHORT_ABSENT",
            "ACTIVE_SLOT_SET_ABSENT",
            "CANONICAL_REFERENCE_STATE_PRODUCER_UNWIRED",
            "CANONICAL_ROLLING_STATE_PRODUCER_UNWIRED",
            "CANONICAL_NATIVE_INPUT_LOADER_UNWIRED",
            "CANONICAL_MARK_PRODUCER_UNWIRED",
            "OPTIONS_INPUT_READY_RECEIPT_ABSENT",
        ],
        "next_actionable_blocker": next_actionable_blocker,
        "zero_effect_proof": {
            "transaction_read_only": database_metadata["read_only"],
            "transaction_isolation": database_metadata["isolation"],
            "transaction_ended_with": "ROLLBACK",
            "executed_statement_count": statement_count,
            "allowed_statement_prefixes": ALLOWED_STATEMENT_PREFIXES,
            "write_statements": 0,
            "ddl_statements": 0,
            "provider_requests": 0,
            "provider_sockets": 0,
            "service_or_process_activations": 0,
            "stages_v4_executions": 0,
            "stored_read_flags_enabled": 0,
            "publication_release_pointer_outbox_delivery_effects": 0,
            "repair_backfill_replay_effects": 0,
            "archive_retention_effects": 0,
        },
    }
    body = (
        json.dumps(report, indent=2, sort_keys=True, default=_json) + "\n"
    ).encode("utf-8")
    with output.open("xb") as handle:
        handle.write(body)
    summary = {
        "status": status,
        "output": str(output),
        "sha256": hashlib.sha256(body).hexdigest(),
        "blocking_categories": blocking_categories,
        "next_actionable_blocker": report["next_actionable_blocker"],
        "zero_effect_proof": report["zero_effect_proof"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True, default=_json))
    return 0 if status == "AVAILABLE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
