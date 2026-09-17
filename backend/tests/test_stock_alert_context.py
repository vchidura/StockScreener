from copy import deepcopy
from datetime import timedelta

import exchange_calendars
import pytest

from research.stock_alert_context import daily_price_context, utc


def split_fixture():
	calendar = exchange_calendars.get_calendar("XNYS")
	sessions = calendar.sessions_in_range(calendar.session_offset("2026-09-16", -252), "2026-09-16")
	bars = []
	for index, session in enumerate(sessions):
		multiplier = 2 if str(session.date()) < "2025-12-05" else 1
		close = (100 + index) * multiplier
		bars.append(dict(security_id="XLY-id", ticker="XLY", session=str(session.date()), revision_id=str(index),
			bar_start=calendar.session_open(session).isoformat(), bar_end=calendar.session_close(session).isoformat(),
			system_observed_at=(calendar.session_close(session) + timedelta(minutes=15)).isoformat(),
			created_at=(calendar.session_close(session) + timedelta(minutes=16)).isoformat(),
			open=close, high=close + multiplier, low=close - multiplier, close=close, volume=1000 / multiplier,
			source_kind="DERIVED", adjusted=False))
	action = dict(security_id="XLY-id", ticker="XLY", action_type="SPLIT", effective_date="2025-12-05",
		revision_id="split", payload_sha256="action-hash", split_from=1, split_to=2,
		first_observed_at="2026-09-03T12:00:00Z", created_at="2026-09-03T12:01:00Z")
	review = dict(security_id="XLY-id", ticker="XLY", effective_date="2025-12-05", action_revision_id="split",
		action_payload_sha256="action-hash", split_from=1, split_to=2, reviewed_at="2026-09-17T00:00:00Z",
		evidence_sha256="e" * 64, evidence_observed_at="2026-09-16T23:00:00Z",
		status="REVIEWED", evidence_url="https://example.test/issuer-split")
	return bars, action, review


def test_reviewed_split_prices_and_volume_match_continuous_history_without_source_changes():
	bars, action, review = split_fixture()
	original = deepcopy((bars, action, review))
	args = ("XLY-id", "2026-09-16", "2026-09-17T01:00:00Z")
	assert daily_price_context(bars, *args, [action])["status"] == "UNAVAILABLE"
	result = daily_price_context(bars, *args, [action], split_reviews=[review], include_history=True)
	continuous = [dict(bar, **{field: bar[field] / 2 for field in ("open", "high", "low", "close")}, volume=bar["volume"] * 2)
		if bar["session"] < "2025-12-05" else bar for bar in bars]
	baseline = daily_price_context(continuous, *args)
	assert result["status"] == "READY" and result["value"] == baseline["value"]
	assert result["price_basis"] == "REVIEWED_SPLIT_ADJUSTED_PRICE_V1"
	assert result["history"][0]["volume"] == 1000 and result["history"][-1]["close"] == bars[-1]["close"]
	assert result["split_adjustments"][0]["price_factor"] == .5
	assert "split" in result["source_revision_ids"] and result["available_at"] == review["reviewed_at"].replace("Z", "+00:00")
	assert (bars, action, review) == original
	assert daily_price_context(bars, *args, [action], split_reviews=[])["status"] == "UNAVAILABLE"


@pytest.mark.parametrize("change", [dict(action_revision_id="other"), dict(action_payload_sha256="revised"),
	dict(security_id="other"), dict(ticker="OTHER"), dict(effective_date="2025-12-04"), dict(split_to=3),
	dict(reviewed_at="2026-09-18T00:00:00Z"), dict(evidence_url=None), dict(status="PENDING"),
	dict(evidence_sha256=""), dict(evidence_observed_at="2026-09-18T00:00:00Z")])
def test_split_review_rejects_unreviewed_terms_identity_and_future_review(change):
	bars, action, review = split_fixture()
	result = daily_price_context(bars, "XLY-id", "2026-09-16", "2026-09-17T01:00:00Z", [action], split_reviews=[review | change])
	assert result["status"] == "UNAVAILABLE" and result["value"] is None


@pytest.mark.parametrize("action_change", [dict(action_type="MERGER"), dict(split_to=0), dict(split_to=float("nan")), dict(payload_sha256="changed")])
def test_reviewed_split_does_not_approve_other_actions_or_invalid_source(action_change):
	bars, action, review = split_fixture()
	assert daily_price_context(bars, "XLY-id", "2026-09-16", "2026-09-17T01:00:00Z", [action | action_change], split_reviews=[review])["status"] == "UNAVAILABLE"


def test_split_is_effective_only_before_its_session_and_never_double_adjusts():
	bars, action, review = split_fixture()
	from research.stock_alert_context import reviewed_split_history
	adjusted, _ = reviewed_split_history(bars, [action], [review], utc("2026-09-17T01:00:00Z"))
	boundary = next(index for index, bar in enumerate(bars) if bar["session"] == action["effective_date"])
	assert adjusted[boundary]["close"] == bars[boundary]["close"]
	assert adjusted[boundary - 1]["close"] == bars[boundary - 1]["close"] / 2
	with pytest.raises(ValueError):
		reviewed_split_history([bar | dict(adjusted=True) for bar in bars], [action], [review], utc("2026-09-17T01:00:00Z"))
	with pytest.raises(ValueError):
		reviewed_split_history(bars, [action, action], [review], utc("2026-09-17T01:00:00Z"))
	with pytest.raises(ValueError):
		reviewed_split_history(adjusted, [action], [review], utc("2026-09-17T01:00:00Z"))


def test_reviewed_rotation_has_consistent_price_basis_and_preserves_raw_baseline():
	from research.stock_rotation import build_rotation_snapshot, compact_rotation_lineage, split_basis_comparison, verify_rotation_snapshot
	from research.stock_idea_engine import digest
	bars, action, review = split_fixture()
	refs = [dict(ticker=ticker, security_id=ticker + "-id", revision_id=ticker + "-ref", security_type="ETF" if ticker != "ABC" else "CS",
		sic_code="5311", active=True, effective_from="2025-01-01T00:00:00Z", observed_at="2025-01-01T00:00:00Z", created_at="2025-01-01T00:00:00Z")
		for ticker in ("XLY", "SPY", "QQQ", "ABC")]
	continuous = [dict(bar, **{field: bar[field] / 2 for field in ("open", "high", "low", "close")})
		if bar["session"] < "2025-12-05" else bar for bar in bars]
	facts = dict(bars=bars + [dict(bar, ticker=ticker, security_id=ticker + "-id", revision_id=ticker + bar["revision_id"])
		for ticker in ("SPY", "QQQ", "ABC") for bar in continuous], references=refs, actions=[action])
	members = [dict(ticker="ABC", security_id="ABC-id")]
	original = deepcopy(facts)
	raw = build_rotation_snapshot(members, facts, "2026-09-17T01:00:00Z")
	reviewed = build_rotation_snapshot(members, facts | dict(split_reviews=[review]), "2026-09-17T01:00:00Z")
	assert next(row for row in raw["sectors"] if row["ticker"] == "XLY")["rotation"]["status"] == "UNAVAILABLE"
	assert next(row for row in reviewed["sectors"] if row["ticker"] == "XLY")["rotation"]["status"] == "READY"
	assert reviewed["stocks"][0]["relative"]["status"] == "READY" and facts == original
	assert reviewed["benchmarks"]["SPY"]["value"] == raw["benchmarks"]["SPY"]["value"]
	assert split_basis_comparison(raw, reviewed)["benchmark_values_unchanged"]
	reviewed.update(database_snapshot=dict(read_only="on", isolation="repeatable read"), original_publications_unchanged=True)
	reviewed["snapshot_sha256"] = digest(reviewed)
	compacted = compact_rotation_lineage(reviewed)
	assert verify_rotation_snapshot(compacted)["status"] == "VERIFIED"
	broken = deepcopy(compacted)
	next(row for row in broken["sectors"] if row["ticker"] == "XLY")["context"]["split_adjustments"][0]["price_factor"] = .25
	broken["snapshot_sha256"] = digest({key: value for key, value in broken.items() if key != "snapshot_sha256"})
	with pytest.raises(ValueError, match="lineage"):
		verify_rotation_snapshot(broken)


def test_pinned_four_proxy_review_loads_and_cannot_be_used_before_review(tmp_path):
	import json
	from pathlib import Path
	from scripts.prepare_stock_alert_context import load_context_split_reviews
	path = Path(__file__).resolve().parents[1] / "research/stock_sector_split_reviews.json"
	reviews = load_context_split_reviews(path, utc("2026-09-17T07:00:00Z"))
	assert {row["ticker"] for row in reviews} == {"XLB", "XLE", "XLU", "XLY"}
	for review in reviews:
		bars, action, _ = split_fixture()
		bars = [bar | dict(security_id=review["security_id"], ticker=review["ticker"]) for bar in bars]
		action.update(security_id=review["security_id"], ticker=review["ticker"], revision_id=review["action_revision_id"],
			payload_sha256=review["action_payload_sha256"])
		assert daily_price_context(bars, review["security_id"], "2026-09-16", "2026-09-17T07:00:00Z", [action], split_reviews=reviews)["status"] == "READY"
	with pytest.raises(ValueError, match="not available"):
		load_context_split_reviews(path, utc("2026-09-17T06:37:00Z"))
	record = json.loads(path.read_text())
	record["reviews"][0]["split_to"] = 3
	altered = tmp_path / "altered.json"
	altered.write_text(json.dumps(record))
	with pytest.raises(ValueError):
		load_context_split_reviews(altered, utc("2026-09-17T07:00:00Z"))


def test_review_policy_change_refreshes_without_changing_source_window_cadence():
	from research.stock_rotation import rotation_refresh_due
	snapshot = dict(additional_context={}, as_of="2026-09-17T07:00:00Z")
	args = (snapshot, "2026-09-16", "2026-09-16T20:00:00Z", "2026-09-17T07:00:01Z")
	assert not rotation_refresh_due(*args)
	assert rotation_refresh_due(*args, split_review_sha256="approved")
	snapshot["split_review_sha256"] = "approved"
	assert not rotation_refresh_due(*args, split_review_sha256="approved")
	assert rotation_refresh_due(*args, split_review_sha256="another-review")
