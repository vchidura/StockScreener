"""Bounded daily gap facts prepared offline from a verified contiguous price window."""
from uuid import NAMESPACE_URL, uuid5

import pandas as pd

from screeners import identify_gap_up, identify_gap_down, _gap_lifecycle
from research.screening import GAP_VERSION, GAP_WINDOW


def project_gaps(frame, security_id):
    if len(frame) != GAP_WINDOW:
        raise ValueError("Daily gap projection requires 22 verified consecutive sessions")
    source = frame.copy()
    source.index = pd.DatetimeIndex(pd.to_datetime(source.session_date))
    close = float(source.close.iloc[-1])
    episodes = []
    for direction, detector in (("UP", identify_gap_up), ("DOWN", identify_gap_down)):
        for gap in detector(source, gap_threshold=.01):
            formation_index = gap["index"]
            lifecycle = _gap_lifecycle(source, gap, direction)
            if gap["range_gap_survived"]:
                lower, upper = (gap["prev_high"], gap["gap_low"]) if direction == "UP" else (gap["gap_high"], gap["prev_low"])
                zone_basis = "FORMATION_RANGE_GAP"
            else:
                lower, upper = sorted((gap["previous_close"], gap["gap_open"]))
                zone_basis = "OPEN_TO_PREVIOUS_CLOSE"
            boundary_name, boundary = min((("LOWER", lower), ("UPPER", upper)), key=lambda item: (abs(close - item[1]), item[0]))
            formation_session = str(source.index[formation_index].date())
            episodes.append(dict(
                episode_id=str(uuid5(NAMESPACE_URL, f"{GAP_VERSION}:{security_id}:1d:{formation_session}:{direction}")),
                version=GAP_VERSION, interval="1d", formation_session=formation_session,
                formation_bar_id=str(source.bar_revision_id.iloc[formation_index]),
                previous_bar_id=str(source.bar_revision_id.iloc[formation_index - 1]),
                zone_lower=float(lower), zone_upper=float(upper), zone_basis=zone_basis,
                nearest_boundary=boundary_name, boundary_price=float(boundary),
                opening_price=float(gap["gap_open"]), fill_target=float(gap["previous_close"]),
                first_fill_session=lifecycle["first_fill_date"],
                values=dict(direction=direction, state=lifecycle["gap_lifecycle"],
                    formation_age=lifecycle["gap_age_sessions"], fill_fraction=lifecycle["fill_pct"] / 100,
                    price_location="BELOW" if close < lower else "ABOVE" if close > upper else "INSIDE",
                    distance_fraction=abs(close - boundary) / boundary)))
    return sorted(episodes, key=lambda episode: (episode["formation_session"], episode["episode_id"]), reverse=True)