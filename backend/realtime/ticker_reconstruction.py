"""Explicit research reconstruction with separate market, knowledge and publication clocks."""
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone

import exchange_calendars

from .portal_projection import build_portal_snapshot
from .ticker_projection import (
    produce_realtime_pattern_watch_publication,
    produce_realtime_ticker_workspace_summary,
)


def _clock(value):
    if not isinstance(value, (str, datetime)):
        raise ValueError("Ticker reconstruction clock is invalid")
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Ticker reconstruction clocks must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def publication_source_cutoff(generated_at, reconstruction=None, *, session_date=None, market_time=None):
    generated = _clock(generated_at)
    if reconstruction is None:
        return generated
    if not isinstance(reconstruction, dict) or reconstruction.get("kind") != "RECONSTRUCTED_RESEARCH":
        raise ValueError("Ticker reconstruction provenance is invalid")
    if not {"market_cutoff", "input_cutoff", "knowledge_cutoff", "published_at", "session"} <= reconstruction.keys():
        raise ValueError("Ticker reconstruction provenance is incomplete")
    market = _clock(reconstruction["market_cutoff"])
    input_cutoff = _clock(reconstruction["input_cutoff"])
    knowledge = _clock(reconstruction["knowledge_cutoff"])
    published = _clock(reconstruction["published_at"])
    session = date.fromisoformat(reconstruction["session"])
    calendar = exchange_calendars.get_calendar("XNYS")
    if (published != generated or not market <= input_cutoff <= knowledge <= published
            or str(session_date) != str(session) or not calendar.is_session(str(session))
            or calendar.session_close(str(session)).to_pydatetime() != market
            or market_time is not None and _clock(market_time) != market):
        raise ValueError("Ticker reconstruction clock/session identity drift")
    return input_cutoff


def reconstruct_ticker_closing_cycle(cycle, *, published_at, source_publications):
    inputs = cycle.inputs
    published = _clock(published_at)
    cutoff = _clock(inputs.cutoff)
    knowledge = _clock(inputs.recorded_by)
    calendar = exchange_calendars.get_calendar("XNYS")
    market = calendar.session_close(str(inputs.session)).to_pydatetime()
    if any(row["bar_end"] > market for row in inputs.bars):
        raise ValueError("Ticker closing reconstruction contains bars after the selected market close")
    if set(source_publications) != {snapshot.snapshot_key for snapshot in cycle.projection.snapshots}:
        raise ValueError("Ticker reconstruction requires one original publication per member")
    common = dict(kind="RECONSTRUCTED_RESEARCH", session=str(inputs.session),
        market_cutoff=market.isoformat(), input_cutoff=cutoff.isoformat(), knowledge_cutoff=knowledge.isoformat(),
        published_at=published.isoformat())
    publication_source_cutoff(published, common, session_date=inputs.session, market_time=market)

    def stamp(snapshot, payload, provenance):
        return build_portal_snapshot(
            cohort_id=snapshot.cohort_id, provider_tier=snapshot.provider_tier,
            snapshot_type=snapshot.snapshot_type, snapshot_key=snapshot.snapshot_key,
            source_generation=snapshot.source_generation,
            source_manifest=dict(snapshot.source_manifest, reconstruction=provenance),
            payload=dict(payload, session=str(inputs.session), reconstruction=provenance),
            generated_at=published, valid_until=snapshot.valid_until,
        )

    snapshots = []
    for snapshot in cycle.projection.snapshots:
        payload = deepcopy(snapshot.payload)
        if payload["quote"]["trade_date"] != str(inputs.session) or _clock(payload["quote"]["as_of"]) != market:
            raise ValueError("Ticker reconstruction must retain the exact original session close")
        if "1mo" not in payload["chart_refs"] or "1mo" not in payload["patterns"]:
            raise ValueError("Ticker monthly reconstruction is incomplete")
        payload["multi"]["as_of"] = published.isoformat()
        for setup in payload["multi"]["setups"].values():
            setup["computed_at"] = published.isoformat()
        for metric in payload["multi"]["setup_read_metrics"].values():
            if _clock(metric["observed_at"]) > knowledge:
                raise ValueError("Ticker reconstruction evidence exceeds its knowledge cutoff")
            metric.update(process_started_at=knowledge.isoformat(), published_at=published.isoformat())
        for response in (*payload["patterns"].values(), *payload["channels"].values(), payload["cross_patterns"]):
            response["computed_at"] = published.isoformat()
        payload["context"]["as_of"] = published.isoformat()
        payload["context"]["cohort"]["available_at"] = published.isoformat()
        for frame in payload["context"]["rows"][0]["frames"].values():
            frame["available_at"] = published.isoformat()
        provenance = dict(common, original_publication=deepcopy(source_publications[snapshot.snapshot_key]))
        snapshots.append(stamp(snapshot, payload, provenance))

    publication_inputs = replace(inputs, cutoff=published, known_by=published)
    summary = produce_realtime_ticker_workspace_summary(publication_inputs, snapshots, cycle.projection.summary_charts)
    patterns = produce_realtime_pattern_watch_publication(publication_inputs, snapshots)
    if patterns.payload["views"]["1mo"]["status"] != "COMPLETE":
        raise ValueError("Ticker reconstruction requires complete cohort monthly Pattern Watch coverage")
    provenance = dict(common, original_publications=deepcopy(source_publications))
    return replace(cycle, projection=replace(cycle.projection, snapshots=tuple(snapshots)),
        summary=stamp(summary, summary.payload, provenance),
        pattern_publication=stamp(patterns, patterns.payload, provenance))
