from __future__ import annotations

import hashlib
import json
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from equity.domain import (
    BarAvailabilityMode,
    EquityCorporateActionCoverage,
)
from equity.polygon import sha256_json


def build_corporate_action_coverage(
    tickers,
    *,
    start,
    end,
    observed_at: datetime,
    availability_mode: BarAvailabilityMode = (
        BarAvailabilityMode.LIVE_OBSERVED
    ),
    responses=None,
    action_types=("SPLIT", "DIVIDEND"),
) -> tuple[EquityCorporateActionCoverage, ...]:
    action_types = tuple(action_types)
    if (
        not action_types
        or len(set(action_types)) != len(action_types)
        or set(action_types) - {"SPLIT", "DIVIDEND"}
    ):
        raise ValueError("coverage requires distinct supported action types")
    if responses is not None and set(responses) != set(action_types):
        raise ValueError(
            "coverage requires every requested completed response"
        )
    rows = []
    for ticker in sorted(tickers):
        for action_type in action_types:
            payload = {
                "action_type": action_type,
                "ticker": ticker,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
            }
            canonical = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            )
            digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
            source_key = (
                f"polygon:{action_type.lower()}:{ticker}:{start}:{end}"
            )
            response_fields = {}
            revision_key = (
                f"{source_key}:{observed_at.isoformat()}:{digest}"
            )
            if responses is not None:
                hashes = sorted(
                    sha256_json(row)
                    for row in responses[action_type]
                    if str(row.get("ticker") or "").upper() == ticker
                )
                response_fields = {
                    "security_id": tickers[ticker],
                    "response_action_count": len(hashes),
                    "response_sha256": sha256_json(hashes),
                }
                source_key += ":response-v2"
                revision_key = (
                    f"{source_key}:{observed_at.isoformat()}:{digest}:"
                    f"{tickers[ticker]}:"
                    f"{response_fields['response_sha256']}"
                )
            rows.append(EquityCorporateActionCoverage(
                coverage_id=uuid5(
                    NAMESPACE_URL,
                    f"equity-action-coverage:{revision_key}",
                ),
                action_type=action_type,
                ticker=ticker,
                window_start=start,
                window_end=end,
                source="POLYGON_CORPORATE_ACTIONS_V1",
                source_key=source_key,
                first_observed_at=observed_at,
                availability_mode=availability_mode,
                replay_available_at=(
                    observed_at
                    if availability_mode
                    is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
                    else None
                ),
                payload_sha256=digest,
                **response_fields,
            ))
    return tuple(rows)
