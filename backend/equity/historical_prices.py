"""Explicit, reproducible price selection for reconstructed security histories."""
from dataclasses import dataclass
from datetime import date, datetime
import math
from typing import Mapping
from uuid import UUID

import pandas as pd
import exchange_calendars

from .polygon import sha256_json


@dataclass(frozen=True)
class HistoricalPriceRead:
    frames_by_security: Mapping[UUID, pd.DataFrame]
    manifest: dict


def select_historical_prices(rows, *, identity_by_ticker_session, adjusted, source_cutoff,
                             identity_evidence_sha256, pinned_bar_ids=None):
    if source_cutoff.utcoffset() is None:
        raise ValueError("price source cutoff must be timezone-aware")
    if len(identity_evidence_sha256) != 64 or any(value not in "0123456789abcdef" for value in identity_evidence_sha256):
        raise ValueError("price selection requires an identity evidence hash")
    expected = {(ticker, session): UUID(str(identity))
                for (ticker, session), identity in identity_by_ticker_session.items()}
    if not expected:
        raise ValueError("an explicit dated security identity map is required")
    calendar = exchange_calendars.get_calendar("XNYS")
    if any(not calendar.is_session(pd.Timestamp(session)) for _, session in expected):
        raise ValueError("requested price date is not an XNYS session")
    if len({(identity, session) for (_, session), identity in expected.items()}) != len(expected):
        raise ValueError("multiple source tickers for one security/session")
    pins = None if pinned_bar_ids is None else {key: UUID(str(value)) for key, value in pinned_bar_ids.items()}
    if pins is not None and set(pins) != set(expected):
        raise ValueError("price revision pins must cover every requested ticker/session")
    candidates = {}
    for row in rows:
        key = (row["ticker"], row["session_date"])
        if key not in expected:
            continue
        if row["adjusted"] != adjusted or row["interval"] != "1d" or row["session_scope"] != "RTH" \
                or not row["is_final"] or row["availability_mode"] != "HISTORICAL_RECONSTRUCTED":
            continue
        if "GROUPED_DAILY_EXACT_TICKER_V2" not in row["quality_codes"]:
            continue
        clocks = [row[field] for field in ("system_observed_at", "created_at", "bar_end", "replay_available_at")]
        if any(clock is None or clock.utcoffset() is None for clock in clocks):
            raise ValueError("selected price lineage has missing or naive availability clocks")
        if max(clocks) > source_cutoff:
            continue
        revision_id = UUID(str(row["bar_revision_id"]))
        if pins is not None and pins[key] != revision_id:
            continue
        candidates.setdefault(key, []).append(row)
    selected = []
    for key, identity in sorted(expected.items()):
        available = candidates.get(key, [])
        if not available:
            raise ValueError(f"missing visible price revision: {key[0]} {key[1]}")
        available.sort(key=lambda row: (row["system_observed_at"], row["created_at"]), reverse=True)
        row = available[0]
        if len(available) > 1 and (pins is not None or (
            row["system_observed_at"], row["created_at"]
        ) == (available[1]["system_observed_at"], available[1]["created_at"])):
            raise ValueError(f"ambiguous visible price revision: {key[0]} {key[1]}")
        if UUID(str(row["security_id"])) != identity:
            raise ValueError(f"price security identity conflict: {key[0]} {key[1]}")
        if row["bar_end"] != calendar.session_close(pd.Timestamp(key[1])).to_pydatetime() \
                or row["replay_available_at"] != row["bar_end"] \
                or row["system_observed_at"] < row["bar_end"]:
            raise ValueError(f"invalid reconstructed price availability: {key[0]} {key[1]}")
        prices = [float(row[column]) for column in ("open", "high", "low", "close", "volume")]
        if not all(math.isfinite(value) for value in prices) or min(prices[:4]) <= 0 or prices[4] < 0 \
                or prices[1] < max(prices[0], prices[2], prices[3]) or prices[2] > min(prices[0], prices[1], prices[3]):
            raise ValueError(f"invalid selected OHLCV: {key[0]} {key[1]}")
        selected.append(dict(row, security_id=identity, source_ticker=row["ticker"],
                             bar_revision_id=UUID(str(row["bar_revision_id"]))))
    frames = {}
    for identity, group in pd.DataFrame(selected).groupby("security_id", sort=False):
        frame = group.set_index("session_date").sort_index()
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="raise")
        frames[identity] = frame
    manifest = {
        "contract": "EXACT_DATED_SECURITY_PRICES_V1", "adjusted": adjusted,
        "source_cutoff": source_cutoff.isoformat(),
        "identity_evidence_sha256": identity_evidence_sha256,
        "revisions": [{"ticker": row["source_ticker"], "session": row["session_date"].isoformat(),
                       "security_id": str(row["security_id"]), "bar_revision_id": str(row["bar_revision_id"]),
                       "payload_sha256": row["payload_sha256"]} for row in selected],
    }
    manifest["sha256"] = sha256_json(manifest)
    return HistoricalPriceRead(frames, manifest)


def read_historical_prices(cursor, *, identity_by_ticker_session, adjusted, source_cutoff,
                           identity_evidence_sha256, pinned_bar_ids=None):
    keys = sorted(identity_by_ticker_session)
    cursor.execute(
        """
        WITH requested AS (
            SELECT * FROM unnest(%s::TEXT[], %s::DATE[]) AS request(ticker, session_date)
        )
        SELECT bar.ticker, bar.session_date, bar.security_id, bar.bar_revision_id,
               bar.open_price AS open, bar.high_price AS high, bar.low_price AS low,
               bar.close_price AS close, bar.volume, bar.bar_end, bar.replay_available_at,
               bar.system_observed_at, bar.created_at, bar.payload_sha256,
               bar.interval, bar.session_scope, bar.adjusted, bar.is_final,
               bar.availability_mode, bar.quality_codes
        FROM equity_bar_revisions AS bar JOIN requested USING (ticker, session_date)
        WHERE bar.interval = '1d' AND bar.session_scope = 'RTH' AND bar.adjusted = %s AND bar.is_final
          AND bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
          AND bar.quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
          AND bar.system_observed_at <= %s AND bar.created_at <= %s
          AND (%s::UUID[] IS NULL OR bar.bar_revision_id = ANY(%s::UUID[]))
        """, ([ticker for ticker, _ in keys], [session for _, session in keys], adjusted, source_cutoff, source_cutoff,
              None if pinned_bar_ids is None else [str(value) for value in pinned_bar_ids.values()],
              None if pinned_bar_ids is None else [str(value) for value in pinned_bar_ids.values()]),
    )
    return select_historical_prices(
        [dict(row) for row in cursor.fetchall()], identity_by_ticker_session=identity_by_ticker_session,
        adjusted=adjusted, source_cutoff=source_cutoff, identity_evidence_sha256=identity_evidence_sha256,
        pinned_bar_ids=pinned_bar_ids,
    )


def reread_historical_prices(cursor, manifest):
    if manifest.get("contract") != "EXACT_DATED_SECURITY_PRICES_V1" \
            or type(manifest.get("adjusted")) is not bool \
            or sha256_json({key: value for key, value in manifest.items() if key != "sha256"}) != manifest.get("sha256"):
        raise ValueError("invalid historical price manifest contract or checksum")
    identities = {}
    pins = {}
    for row in manifest["revisions"]:
        key = (row["ticker"], date.fromisoformat(row["session"]))
        if key in identities:
            raise ValueError("duplicate ticker/session in historical price manifest")
        identities[key] = UUID(row["security_id"])
        pins[key] = UUID(row["bar_revision_id"])
    result = read_historical_prices(
        cursor, identity_by_ticker_session=identities, adjusted=manifest["adjusted"],
        source_cutoff=datetime.fromisoformat(manifest["source_cutoff"]),
        identity_evidence_sha256=manifest["identity_evidence_sha256"], pinned_bar_ids=pins,
    )
    if result.manifest != manifest:
        raise ValueError("historical price manifest does not match retained source revisions")
    return result