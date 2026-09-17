"""Bounded daily FRED observations with explicit configuration and receipt clocks."""
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path

import exchange_calendars
from dotenv import dotenv_values
from requests import RequestException

from http_client import get_session
from research.stock_alert_context import observation, utc
from research.stock_idea_engine import digest


SERIES = {"vix": "VIXCLS", "credit": "BAMLH0A0HYM2"}
VERSION = "market_context_fred_daily_v1"
FRED_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def fred_settings():
    local = dotenv_values(FRED_ENV_PATH) if FRED_ENV_PATH.exists() else {}
    return {key: (local[key] or "").strip() if key in local else os.getenv(key, "").strip()
        for key in ("FRED_API_KEY", "FRED_CREDIT_INTERNAL_USE_APPROVED")}


def source_configuration(settings=None):
    settings = fred_settings() if settings is None else settings
    key_ready = bool(settings["FRED_API_KEY"])
    credit_approved = settings["FRED_CREDIT_INTERNAL_USE_APPROVED"].lower() in ("1", "true", "yes")
    return dict(api_key_configured=key_ready, credit_internal_use_approved=credit_approved,
        series={name: ("NEEDS_CONFIGURATION" if not key_ready else "NEEDS_APPROVAL" if name == "credit" and not credit_approved else "CONFIGURED")
            for name in SERIES})


def fred_context(record, cutoff, *, include_history=False):
    cutoff = utc(cutoff)
    series = record["series_id"]
    if series not in SERIES.values():
        raise ValueError("unsupported context series")
    received = utc(record["received_at"])
    result = observation("UNAVAILABLE", "FRED_RESPONSE_AFTER_CUTOFF", source="FRED", series_id=series,
        source_url="https://fred.stlouisfed.org/series/" + series,
        history_mode="RECONSTRUCTED_AT_RECEIPT_NOT_HISTORICAL_AVAILABILITY",
        units="INDEX_POINTS" if series == "VIXCLS" else "BASIS_POINTS", last_attempt_at=record["received_at"])
    if received > cutoff:
        return result
    if record["response_sha256"] != digest(record["observations"]):
        raise ValueError("FRED response hash mismatch")
    calendar = exchange_calendars.get_calendar("XNYS")
    expected = calendar.date_to_session(cutoff.date(), direction="previous")
    if calendar.session_close(expected).to_pydatetime() > cutoff:
        expected = calendar.previous_session(expected)
    values = {}
    for row in record["observations"]:
        date = row["date"]
        if date > str(expected.date()) or not calendar.is_session(date) or row["value"] == ".":
            continue
        value = float(row["value"])
        if date in values or not math.isfinite(value) or value < 0 or (series == "VIXCLS" and value == 0):
            raise ValueError("invalid or duplicate daily FRED observation")
        values[date] = value
    result.update(source_revision_ids=[record["response_sha256"]], observed_at=received.isoformat(),
        created_at=received.isoformat(), available_at=received.isoformat())
    if not values:
        return result | dict(reason_codes=["NO_ELIGIBLE_FRED_OBSERVATIONS"])
    latest = max(values)
    session = calendar.date_to_session(latest)
    dates = [str(item.date()) for item in calendar.sessions_in_range(calendar.session_offset(session, -252), calendar.previous_session(session))]
    history = [values[date] for date in dates if date in values]
    current = values[latest]
    previous = values.get(str(calendar.previous_session(session).date()))
    sufficient = len(history) >= 202 and len(set(history)) > 1
    percentile = (sum(value < current for value in history) + .5 * sum(value == current for value in history)) / len(history) if sufficient else None
    multiplier = 1 if series == "VIXCLS" else 100
    stale = latest < str(expected.date())
    result.update(status="STALE" if stale else "READY" if sufficient else "INSUFFICIENT_HISTORY",
        reason_codes=["SOURCE_CLOSE_BEHIND_EXPECTED_SESSION"] if stale else [] if sufficient else ["PRIOR_252_SESSION_SAMPLE_OR_VARIANCE_INSUFFICIENT"],
        session=latest, expected_session=str(expected.date()), market_time=calendar.session_close(session).isoformat(),
        expected_observations=252, timely_observations=len(history), coverage_fraction=len(history) / 252,
        value=dict(level=current * multiplier, change=(current - previous) * multiplier if previous is not None else None,
            percentile=percentile, percentile_status="READY" if sufficient else "INSUFFICIENT_HISTORY"))
    if include_history:
        result["history"] = [dict(session=date, value=values[date] * multiplier) for date in sorted(values)]
    return result


def retained_oas_reference(path):
    if not source_configuration()["credit_internal_use_approved"]:
        return None, "NEEDS_APPROVAL"
    if not path.exists():
        return None, "NEEDS_REFERENCE_HISTORY"
    if path.stat().st_size > 1_000_000:
        raise ValueError("retained OAS response exceeds bound")
    record = json.loads(path.read_text(encoding="utf-8"))
    if record.get("source") != "FRED" or record.get("version") != VERSION or record.get("series_id") != SERIES["credit"]:
        raise ValueError("incorrect retained OAS source")
    return record, "APPROVED_RETAINED"


def collect_fred_context(directory, *, fetch=False, now=None):
    now = utc(now or datetime.now(timezone.utc))
    settings = fred_settings()
    config = source_configuration(settings)
    contexts = {}
    for name, series in SERIES.items():
        reason = "FRED_API_KEY_NOT_CONFIGURED" if not config["api_key_configured"] else "CREDIT_INTERNAL_USE_APPROVAL_REQUIRED"
        if config["series"][name] != "CONFIGURED":
            contexts[name] = observation(config["series"][name], reason, series_id=series, source="FRED", last_attempt_at=None)
            continue
        path = directory / (series + ".latest.json")
        retained = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
        recent = retained and utc(retained["received_at"]) <= now and now - utc(retained["received_at"]) < timedelta(hours=4)
        failure = None
        if fetch and not recent:
            try:
                response = get_session().get("https://api.stlouisfed.org/fred/series/observations", params=dict(
                    series_id=series, api_key=settings["FRED_API_KEY"], file_type="json", sort_order="asc", limit=600,
                    observation_start=(now.date() - timedelta(days=550)).isoformat(), observation_end=now.date().isoformat()),
                    timeout=(5, 25), allow_redirects=False)
                if response.status_code != 200:
                    failure = "FRED_HTTP_" + str(response.status_code)
                elif len(response.content) > 1_000_000:
                    failure = "FRED_RESPONSE_EXCEEDS_BOUND"
                else:
                    payload = response.json()
                    rows = [{key: row[key] for key in ("date", "value", "realtime_start", "realtime_end") if key in row}
                        for row in payload["observations"]]
                    if len(rows) > 600 or payload.get("count") != len(rows):
                        raise ValueError("incomplete FRED response")
                    record = dict(version=VERSION, series_id=series, received_at=datetime.now(timezone.utc).isoformat(),
                        source="FRED", observations=rows, response_sha256=digest(rows))
                    fred_context(record, record["received_at"])
                    directory.mkdir(parents=True, exist_ok=True)
                    archive = directory / (series + "." + digest(record) + ".json")
                    with archive.open("x", encoding="utf-8") as stream:
                        json.dump(record, stream, indent=2, allow_nan=False)
                    temporary = path.with_suffix("." + digest(record) + ".tmp")
                    try:
                        temporary.write_bytes(archive.read_bytes())
                        os.replace(temporary, path)
                    finally:
                        temporary.unlink(missing_ok=True)
                    retained = record
            except RequestException:
                failure = "FRED_REQUEST_FAILED"
            except (ValueError, KeyError, TypeError):
                failure = "FRED_RESPONSE_INVALID"
        if retained:
            contexts[name] = fred_context(retained, max(now, utc(retained["received_at"])))
        else:
            contexts[name] = observation("FETCH_FAILED" if failure else "NEEDS_SOURCE", failure or "FRED_SERIES_NOT_CAPTURED",
                source="FRED", series_id=series)
        contexts[name]["last_attempt_at"] = datetime.now(timezone.utc).isoformat() if fetch and not recent else retained["received_at"] if retained else None
        if failure:
            contexts[name]["last_fetch_error"] = failure
    return contexts