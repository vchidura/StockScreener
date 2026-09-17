from copy import deepcopy

import pytest

from research.stock_alerts import alert_page, price_return_fields, risk_fields, session_dates


def snapshot():
    return dict(source="REPLAY", source_id="test", source_label="Historical replay", as_of="2026-09-13T00:00:00Z",
        sessions=session_dates("2026-09-03"), hit_coverage="RETAINED_VALID_CANDIDATES",
        publications=[dict(run_id="one", session="2026-09-02", published_at="2026-09-02T14:17:00Z"),
                      dict(run_id="empty", session="2026-09-02", published_at="2026-09-02T14:47:00Z")],
        alerts=[dict(alert_id="plan", run_id="one", security_id="A", ticker="A", model="resumption", interval="30m",
                     direction=1, lane="TRADE", status="CLOSED", published_at="2026-09-02T14:17:00Z",
                     trigger_price=100., entry_price=101., stop=99., target=105., latest_price=110., paper_return=.02)],
        observations=[dict(security_id="A", direction=direction, session=session, run_id=run, at=at, models=[model], intervals=[interval], valid=valid)
            for direction, session, run, at, model, interval, valid in [
                (1, "2026-09-01", "prior", "2026-09-01T14:17:00Z", "resumption", "30m", True),
                (1, "2026-09-02", "one", "2026-09-02T14:17:00Z", "resumption", "30m", True),
                (1, "2026-09-02", "one", "2026-09-02T14:17:00Z", "failure", "1h", True),
                (-1, "2026-09-02", "short", "2026-09-02T14:47:00Z", "failure", "1h", True),
                (1, "2026-09-02", "stale", "2026-09-02T15:17:00Z", "failure", "30m", False),
                (1, "2026-09-03", "future", "2026-09-03T14:17:00Z", "resumption", "30m", True)]])


def test_latest_publication_never_falls_back_to_a_nonempty_run():
    data = snapshot()
    result = alert_page(data, session="2026-09-02")
    assert result["run"]["run_id"] == "empty" and not result["rows"]
    assert alert_page(data, session="2026-09-02", run="one")["total"] == 1
    assert alert_page(data, session="2026-09-03")["run"] is None


def combined_fixture():
    from research.stock_alert_results import combine_snapshots, namespace_snapshot, strategy_instance
    sources, instances = [], []
    for stream in ("intraday", "swing"):
        instance = strategy_instance(dict(policy_version=stream), dict(enrolled_at="2026-09-01T00:00:00Z", members=["A"]), stream)
        data = dict(snapshot(), source="SHADOW", source_id=stream)
        if stream == "swing":
            data["publications"] = data["publications"][:1]
            data["alerts"][0].update(trade_style="SWING", hold="21 sessions", status="OPEN")
        sources.append(namespace_snapshot(instance, data))
        instances.append(instance)
    return combine_snapshots(sources, instances), sources


def test_combined_latest_is_per_strategy_and_does_not_fall_back_to_nonempty():
    combined, sources = combined_fixture()
    page = alert_page(combined, session="2026-09-02")
    assert page["total"] == 1 and page["rows"][0]["trade_style"] == "SWING"
    assert len(page["latest_runs"]) == 2 and len({row["run_id"] for row in combined["publications"]}) == 3
    assert sources[0]["alerts"][0]["alert_id"] != sources[1]["alerts"][0]["alert_id"]
    history = alert_page(combined, session="2026-09-02", view="history")
    assert history["total"] == 1 and history["rows"][0]["original_source_id"] == "intraday"


def test_combined_recurrence_and_filters_are_instance_scoped_and_pagination_is_global():
    combined, _ = combined_fixture()
    for row in combined["alerts"]:
        row["status"] = "OPEN"
    page = alert_page(combined, session="2026-09-02", view="open", limit=1)
    assert page["total"] == 2 and len(page["rows"]) == 1
    assert page["rows"][0]["hits"] == 2
    assert alert_page(combined, session="2026-09-02", view="open", trade_type="SWING")["total"] == 1
    assert alert_page(combined, session="2026-09-02", view="open", trade_type="INTRADAY")["total"] == 1
    combined["alerts"][0]["run_id"] = "older-than-navigation"
    assert alert_page(combined, view="open")["total"] == 2


def test_combined_missing_stream_retains_other_results_and_never_nets_opposed_plans():
    from research.stock_alert_results import combine_snapshots
    combined, sources = combined_fixture()
    partial = combine_snapshots(sources[:1], [dict(stream="intraday"), dict(stream="swing", error="MISSING")])
    assert partial["status"] == "PARTIAL" and len(partial["alerts"]) == 1
    for row, direction in zip(combined["alerts"], [1, -1]):
        row.update(status="OPEN", direction=direction)
    page = alert_page(combined, view="open")
    assert page["total"] == 2 and all(row["opposing_exposure"] for row in page["rows"])


def test_combined_context_uses_original_instance_ids_without_changing_rows(monkeypatch):
    from contextlib import contextmanager
    import equity.stock_alert_results as repository
    from research.stock_alert_annotations import binding
    from research.stock_idea_engine import digest
    combined, _ = combined_fixture()
    row = combined["alerts"][1]
    row.update(triggered_at="2026-09-02T14:00:00Z", policy_version="swing")
    original = row | dict(alert_id=row["original_alert_id"], run_id=row["original_run_id"])
    bundle = dict(schema="stock_alert_publication_context_v1", source_id="swing", run_id="one",
        input_cutoff="2026-09-02T14:16:00Z", publication_at=row["published_at"], assembled_at="2026-09-02T15:00:00Z",
        capture_mode="RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS", source_publication_sha256="source",
        rows={"plan": dict(binding=binding(original), factors={})})
    bundle["bundle_sha256"] = digest(bundle)
    class Cursor:
        def execute(self, query, params=None):
            if params:
                assert params == (row["strategy_instance_id"], "one")
        def fetchone(self):
            return dict(payload=bundle)
    @contextmanager
    def cursor():
        yield Cursor()
    monkeypatch.setattr(repository, "get_db_cursor", cursor)
    page = dict(combined=True, rows=[row])
    result = repository.attach_shared_context(page)
    assert result["rows"][0]["context"]["status"] == "AVAILABLE"
    assert page["rows"][0] == row and "context" not in row
    bundle["source_id"] = "wrong"
    result = repository.attach_shared_context(page)
    assert result["rows"][0]["context"]["status"] == "UNAVAILABLE" and len(result["rows"]) == 1


def test_shared_result_contract_preserves_plan_and_terminal_outcome_but_allows_live_quote_changes():
    from research.stock_alert_results import validate_result_transition, namespace_snapshot, strategy_instance
    data = dict(snapshot(), source="SHADOW", source_id="policy")
    original = deepcopy(data)
    instance = strategy_instance(dict(policy_version="policy"), dict(enrolled_at="2026-09-01T00:00:00Z", members=["A"]), "intraday")
    namespace_snapshot(instance, data)
    assert data == original
    row = data["alerts"][0]
    validate_result_transition(row, row | dict(latest_price=120.))
    for change in (dict(stop=80.), dict(paper_return=.9), dict(status="OPEN"), dict(hold="21 sessions")):
        with pytest.raises(ValueError):
            validate_result_transition(row, row | change)
    opened = row | dict(status="OPEN", entry_at="2026-09-02T14:30:00Z", exit_due_at="2026-09-02T20:00:00Z")
    for change in (dict(entry_price=102.), dict(entry_at=None), dict(exit_due_at="2026-09-03T20:00:00Z")):
        with pytest.raises(ValueError):
            validate_result_transition(opened, opened | change)


def test_result_source_capture_preserves_source_files_and_rejects_altered_plan(tmp_path):
    import json
    from datetime import datetime, timezone
    from research.stock_idea_forward import ForwardStore, forward_config
    from research.stock_idea_engine import candidate_record
    from test_stock_idea_engine import candidate
    from equity.stock_alert_results import capture_result_source
    policy = forward_config(quality_version=2)
    store = ForwardStore(tmp_path / "forward.sqlite", policy)
    plan = candidate()
    data = dict(snapshot(), source="SHADOW", source_id=policy["policy_version"], enrolled_at="2026-09-01T00:00:00Z")
    row = data["alerts"][0]
    row.update(**{field: getattr(plan, field) for field in ("security_id", "ticker", "model", "interval", "direction", "stop", "target", "policy_version")},
        trigger_price=plan.price, triggered_at=plan.trigger_at.isoformat())
    store.save(dict(enrolled_at=data["enrolled_at"], members=["A"], positions={"plan": dict(candidate=candidate_record(plan))}))
    import sqlite3
    with sqlite3.connect(store.path) as connection:
        for run in data["publications"]:
            publication = dict(window_key=run["run_id"], selected=["plan"] if run["run_id"] == "one" else [], actual_publication_at=run["published_at"])
            connection.execute("INSERT INTO forward_publications VALUES(?,?)", (run["run_id"], store.encode(publication)))
    view = tmp_path / "alerts-view.json"
    view.write_text(json.dumps(data))
    before = (store.path.read_bytes(), view.read_bytes())
    capture = capture_result_source(tmp_path, "intraday", datetime(2026, 9, 17, tzinfo=timezone.utc))
    assert capture["snapshot"] == data and (store.path.read_bytes(), view.read_bytes()) == before
    data["alerts"][0]["stop"] = 12.
    view.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="retained ledger"):
        capture_result_source(tmp_path, "intraday", datetime(2026, 9, 17, tzinfo=timezone.utc))


def annotation_fixture():
    from research.stock_alert_context import observation
    data = dict(snapshot(), source="SHADOW")
    row = data["alerts"][0]
    row.update(triggered_at="2026-09-02T14:00:00Z", policy_version="quality2")
    candidate = dict(row, price=row["trigger_price"], trigger_at=row["triggered_at"])
    cutoff = "2026-09-02T14:16:00Z"
    publication = dict(policy_version="test", window_key="one", selected=["plan"], input_deadline=cutoff,
        actual_publication_at=row["published_at"], candidates={"plan": candidate})
    factor = observation("READY")
    factor.update(value=dict(direction="MIXED"), source_revision_ids=["evidence"],
        observed_at="2026-09-02T13:00:00Z", created_at="2026-09-02T13:01:00Z", available_at="2026-09-02T13:01:00Z")
    annotation = dict(alert_id="plan", source_id="test", run_id="one", input_cutoff=cutoff,
        security_id="A", ticker="A", factors=dict(earnings=observation("UNAVAILABLE", "EVENT_COVERAGE_UNKNOWN")))
    return data, publication, annotation, dict(market=factor)


def test_publication_context_is_immutable_optional_and_plan_bound(tmp_path):
    from research.stock_alert_annotations import attach_publication_context, context_path, freeze_publication_context
    data, publication, annotation, shared = annotation_fixture()
    original = deepcopy((data, publication, annotation, shared))
    args = (tmp_path, publication, [annotation], shared)
    kwargs = dict(assembled_at="2026-09-03T00:00:00Z", manifest_sha256="manifest")
    page = alert_page(data, session="2026-09-02", run="one")
    missing = attach_publication_context(page, tmp_path)
    assert missing["rows"][0]["context"]["reason"] == "NO_SAVED_PUBLICATION_CONTEXT"
    assert freeze_publication_context(*args, **kwargs) == "CREATED"
    path = context_path(tmp_path, "test", "one")
    retained = path.read_bytes()
    shared["market"]["value"]["direction"] = "DOWN"
    assert freeze_publication_context(*args, **(kwargs | dict(assembled_at="2026-09-04T00:00:00Z"))) == "PRESERVED"
    assert path.read_bytes() == retained
    result = attach_publication_context(page, tmp_path)
    assert result["rows"][0]["context"]["factors"]["market"]["value"]["direction"] == "MIXED"
    assert result["rows"][0]["context"]["factors"]["earnings"]["status"] == "UNAVAILABLE"
    assert {key: value for key, value in result["rows"][0].items() if key != "context"} == page["rows"][0]
    for change in (dict(security_id="B"), dict(stop=80.), dict(published_at="2026-09-02T14:18:00Z"), dict(policy_version="other")):
        changed = dict(page, rows=[page["rows"][0] | change])
        assert attach_publication_context(changed, tmp_path)["rows"][0]["context"]["status"] == "UNAVAILABLE"
    assert attach_publication_context(page | dict(source="REPLAY"), tmp_path) == page | dict(source="REPLAY")
    assert (data, publication, annotation) == original[:3]


@pytest.mark.parametrize("corrupt", ['{"broken":true}', 'null', '[]', '{'])
def test_publication_context_rejects_future_evidence_and_corruption_without_losing_alert(tmp_path, corrupt):
    from research.stock_alert_annotations import attach_publication_context, context_path, freeze_publication_context
    data, publication, annotation, shared = annotation_fixture()
    kwargs = dict(assembled_at="2026-09-03T00:00:00Z", manifest_sha256="manifest")
    shared["market"]["created_at"] = "2026-09-03T00:00:00Z"
    with pytest.raises(ValueError, match="post-cutoff"):
        freeze_publication_context(tmp_path, publication, [annotation], shared, **kwargs)
    assert not list(tmp_path.iterdir())
    page = alert_page(data, session="2026-09-02", run="one")
    path = context_path(tmp_path, "test", "one")
    path.write_text(corrupt, encoding="utf-8")
    result = attach_publication_context(page, tmp_path)
    assert result["total"] == page["total"] == 1
    assert result["rows"][0]["context"]["reason"] == "INVALID_SAVED_PUBLICATION_CONTEXT"
    assert result["rows"][0]["paper_return"] == page["rows"][0]["paper_return"]


def test_context_api_attachment_respects_source_path_and_is_read_only(tmp_path, monkeypatch):
    from equity.stock_alert_views import attach_alert_context
    from research.stock_alert_annotations import freeze_publication_context
    data, publication, annotation, shared = annotation_fixture()
    page = alert_page(data, session="2026-09-02", run="one")
    monkeypatch.setenv("STOCK_ALERT_SHADOW_VIEW", str(tmp_path / "alerts-view.json"))
    original = deepcopy(page)
    freeze_publication_context(tmp_path / "alert-context", publication, [annotation], shared,
        assembled_at="2026-09-03T00:00:00Z", manifest_sha256="manifest")
    before = {path.name: path.read_bytes() for path in (tmp_path / "alert-context").iterdir()}
    assert attach_alert_context(page)["rows"][0]["context"]["status"] == "AVAILABLE"
    assert page == original
    assert before == {path.name: path.read_bytes() for path in (tmp_path / "alert-context").iterdir()}


def test_verified_artifact_to_frozen_context_rejects_changed_evidence_and_publications(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from research.stock_alert_context import READINESS_VERSION, VERSION, utc
    from research.stock_alert_annotations import attach_publication_context
    from research.stock_idea_engine import digest
    from scripts import prepare_stock_alert_context as capture
    data, publication, annotation, shared = annotation_fixture()
    members = [dict(security_id="A", ticker="A")]
    member = dict(members[0], factors=annotation["factors"])
    bundle = dict(shared, spy=shared["market"], qqq=shared["market"], cutoff=annotation["input_cutoff"],
        prior_session="2026-09-01", members=[member])
    manifest = dict(schema_version=READINESS_VERSION, database_snapshot=dict(read_only="on", isolation="repeatable read"),
        original_publications_unchanged=True, observations={"one": bundle}, universe_sha256=digest(members),
        baseline_policy_sha256=digest({}), company_context_only=True, session="2026-09-02",
        publication_hashes={"one": digest(publication)})
    annotations = dict(schema_version=VERSION, readiness_manifest_sha256=digest(manifest), annotations_only=True,
        publication_contexts={"one": {key: value for key, value in bundle.items() if key != "members"}},
        rows=[annotation | dict(market_ref="one", capture_mode="RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS")])
    output = tmp_path / "capture.json"
    output.write_text(json.dumps(manifest), encoding="utf-8")
    output.with_suffix(".annotations.json").write_text(json.dumps(annotations), encoding="utf-8")
    monkeypatch.setattr(capture, "read_enrollment", lambda *args: (dict(members=members), {}, [publication], {}))
    args = SimpleNamespace(output=output, state_dir=tmp_path, session=utc("2026-09-02T00:00:00Z").date())
    capture.freeze_alert_context(args)
    page = alert_page(data, session="2026-09-02", run="one")
    assert attach_publication_context(page, tmp_path / "alert-context")["rows"][0]["context"]["status"] == "AVAILABLE"
    annotations["rows"][0]["factors"]["earnings"]["reason_codes"] = ["UNSUPPORTED_CHANGE"]
    output.with_suffix(".annotations.json").write_text(json.dumps(annotations), encoding="utf-8")
    with pytest.raises(AssertionError, match="annotation facts"):
        capture.freeze_alert_context(args)
    annotations["rows"][0]["factors"]["earnings"]["reason_codes"] = ["EVENT_COVERAGE_UNKNOWN"]
    output.with_suffix(".annotations.json").write_text(json.dumps(annotations), encoding="utf-8")
    publication["candidates"]["plan"]["stop"] = 80.
    with pytest.raises(ValueError, match="publication hash"):
        capture.freeze_alert_context(args)


def test_forward_reader_preserves_readiness_window_without_fabricating_a_fixed_time():
    data = dict(snapshot(), source="SHADOW", publication_mode="SOURCE_READINESS", next_publication_at=None,
        publication_window_start="2026-09-14T16:15:00Z", publication_deadline="2026-09-14T16:29:55Z")
    result = alert_page(data)
    assert result["next_publication_at"] is None and result["publication_mode"] == "SOURCE_READINESS"
    assert result["publication_window_start"] == data["publication_window_start"]
    assert result["publication_deadline"] == data["publication_deadline"]


def test_event_shadow_retains_suppressed_candidates_and_cannot_change_baseline(tmp_path):
    from research.stock_alert_annotations import context_path, freeze_event_shadow, read_event_shadow
    _, publication, annotation, _ = annotation_fixture()
    publication["candidates"]["suppressed"] = dict(publication["candidates"]["plan"], security_id="B", ticker="B")
    publication["dispositions"] = [dict(episode_id="suppressed", selection="SUPPRESSED", reason="MODEL_QUOTA")]
    unknown = dict(status="UNAVAILABLE", reason_codes=["EVENT_COVERAGE_UNKNOWN"], value=None)
    annotation["factors"]["fomc"] = unknown
    second = dict(annotation, alert_id="suppressed", security_id="B", ticker="B")
    rows = [annotation, second]
    original = deepcopy(publication)
    kwargs = dict(assembled_at="2026-09-03T00:00:00Z", manifest_sha256="manifest")
    assert freeze_event_shadow(tmp_path, publication, rows, **kwargs) == "CREATED"
    path = context_path(tmp_path, "test", "one")
    record = read_event_shadow(path)
    assert len(record["rows"]) == 2 and not record["live_gate_enabled"] and not record["preselection_latency_validated"]
    assert record["rows"][1]["baseline_reason"] == "MODEL_QUOTA"
    assert record["rows"][1]["decision"]["disposition"] == "UNKNOWN"
    assert freeze_event_shadow(tmp_path, publication, rows, **kwargs) == "PRESERVED" and publication == original
    with pytest.raises(ValueError, match="all retained candidates"):
        freeze_event_shadow(tmp_path, publication, [annotation], **kwargs)


def test_context_worker_is_prospective_and_preserves_existing_bundles(tmp_path):
    from scripts.run_stock_alert_context_worker import pending_publications
    from research.stock_alert_annotations import freeze_publication_context
    _, publication, annotation, shared = annotation_fixture()
    activation = dict(activated_at="2026-09-02T14:16:30Z")
    earlier = dict(publication, window_key="earlier", actual_publication_at="2026-09-02T14:16:00Z")
    empty = dict(publication, window_key="empty", selected=[])
    assert pending_publications([earlier, publication, empty], activation, tmp_path) == [publication]
    freeze_publication_context(tmp_path, publication, [annotation], shared,
        assembled_at="2026-09-03T00:00:00Z", manifest_sha256="manifest")
    assert pending_publications([publication], activation, tmp_path) == []
    publication["candidates"]["plan"]["stop"] = 80.
    with pytest.raises(ValueError, match="publication changed"):
        pending_publications([publication], activation, tmp_path)


@pytest.mark.parametrize("model,interval", [("resumption", "30m"), ("discovery", "1d")])
@pytest.mark.parametrize("shadow_only", [False, True])
def test_context_worker_builds_nonempty_evidence_once_without_mutating_ledger(tmp_path, monkeypatch, model, interval, shadow_only):
    from contextlib import contextmanager
    from datetime import timedelta
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import database
    import options.config
    from research.stock_idea_engine import candidate_record
    from research.stock_idea_forward import forward_config
    from research.stock_alert_annotations import context_path, read_context, read_event_shadow
    from scripts import run_stock_alert_context_worker as worker
    from test_stock_idea_engine import NOW, candidate
    plan, policy = candidate(model=model, interval=interval), forward_config()
    state = dict(enrolled_at=(NOW - timedelta(hours=1)).isoformat(), members=[dict(security_id=plan.security_id, ticker=plan.ticker)])
    publication = dict(window_key=NOW.isoformat(), session=NOW.date().isoformat(), input_deadline=NOW.isoformat(),
        actual_publication_at=NOW.isoformat(), market_time=plan.trigger_at.isoformat(), policy_version=policy["policy_version"],
        selected=[plan.episode_id], candidates={plan.episode_id: candidate_record(plan)})
    activation = dict(activated_at=(NOW - timedelta(seconds=1)).isoformat(), ledger_identity=worker.ledger_identity(state, policy))
    if shadow_only:
        publication["selected"] = []
    before = deepcopy((state, policy, publication))
    cursor = MagicMock()
    @contextmanager
    def database_cursor():
        yield cursor
    monkeypatch.setattr(database, "get_db_cursor", database_cursor)
    monkeypatch.setattr(options.config, "load_option_runtime_configuration", lambda: SimpleNamespace(
        settings=SimpleNamespace(event_calendar_max_age_seconds=43200, underlyers=()), settlement_valuation_policy=None))
    facts = dict(bars=[], references=[], actions=[], events=[], event_coverage=[], iv=[], activity=[], fundamentals=[],
        transaction=dict(read_only="on", isolation="repeatable read"))
    captured = MagicMock(return_value=facts)
    monkeypatch.setattr(worker, "capture", captured)
    monkeypatch.setattr(worker, "read_enrollment", lambda *args: (state, policy, [publication], {}))
    status = worker.produce_cycle(tmp_path, activation, NOW, shadow_activation=activation if shadow_only else None)
    assert status["status"] == ("CONTEXT_EMPTY" if shadow_only else "CONTEXT_CREATED") and status["alerts"] == (0 if shadow_only else 1)
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    assert captured.call_args.kwargs == dict(company_only=True)
    path = context_path(tmp_path / ("event-shadow" if shadow_only else "alert-context"), publication["policy_version"], publication["window_key"])
    retained = path.read_bytes()
    if shadow_only:
        record = read_event_shadow(path)
        assert len(record["rows"]) == 1 and not record["rows"][0]["baseline_selected"] and not record["live_gate_enabled"]
        assert worker.produce_cycle(tmp_path, activation, NOW, shadow_activation=activation)["pending_publications"] == 0
        assert captured.call_count == 1 and path.read_bytes() == retained and (state, policy, publication) == before
        return
    bundle = read_context(path)
    assert bundle["rows"][plan.episode_id]["factors"]["financials"]["status"] == "UNAVAILABLE"
    if model == "discovery":
        assert bundle["rows"][plan.episode_id]["factors"]["earnings"]["status"] == "NOT_APPLICABLE"
    assert worker.produce_cycle(tmp_path, activation, NOW)["status"] == "WAITING_FOR_SELECTED_PUBLICATION"
    assert captured.call_count == 1 and path.read_bytes() == retained
    assert (state, policy, publication) == before


def test_context_worker_empty_cycles_never_capture_and_changed_enrollment_blocks(tmp_path, monkeypatch):
    from unittest.mock import MagicMock
    from scripts import run_stock_alert_context_worker as worker
    state = dict(enrolled_at="2026-09-02T13:00:00Z", members=[dict(security_id="A", ticker="A")])
    activation = dict(activated_at="2026-09-02T14:16:30Z", ledger_identity=worker.ledger_identity(state, {}))
    monkeypatch.setattr(worker, "read_enrollment", lambda *args: (state, {}, [], {}))
    builder = MagicMock()
    status = worker.produce_cycle(tmp_path, activation, worker.utc("2026-09-02T15:00:00Z"), builder=builder)
    assert status["status"] == "WAITING_FOR_SELECTED_PUBLICATION" and not status["provider_requests"] and not status["ledger_writes"]
    builder.assert_not_called()
    state["members"].append(dict(security_id="B", ticker="B"))
    with pytest.raises(ValueError, match="enrollment or policy changed"):
        worker.produce_cycle(tmp_path, activation, worker.utc("2026-09-02T15:00:00Z"), builder=builder)


def test_context_progression_review_is_read_only_and_keeps_baseline_suppressions(tmp_path, monkeypatch):
    from scripts import run_stock_alert_context_worker as worker
    _, publication, annotation, _ = annotation_fixture()
    publication["selected"] = []
    publication["dispositions"] = [dict(episode_id="plan", selection="SUPPRESSED", reason="EXPIRED")]
    annotation["factors"].update(fomc=dict(status="UNAVAILABLE"), spy=dict(status="NOT_COVERED"))
    original = deepcopy(publication)
    monkeypatch.setattr(worker, "read_enrollment", lambda *args: ({}, {}, [publication], {}))
    calls = []
    def builder(*args, **kwargs):
        calls.append(kwargs)
        return {}, dict(rows=[annotation])
    result = worker.review_session(tmp_path, "2026-09-02", builder=builder)
    assert result["summary"] == dict(candidate_occurrences=1, unique_candidates=1, baseline_selected=0,
        shadow_decisions=dict(UNKNOWN=1), archived_context_present=0)
    assert result["rows"][0]["baseline_reason"] == "EXPIRED" and result["original_publications_unchanged"]
    assert calls == [dict(include_candidates=True)] and publication == original and not list(tmp_path.iterdir())


def test_history_keeps_frozen_plan_and_distinct_valid_window_hits():
    data = snapshot()
    original = deepcopy(data)
    result = alert_page(data, session="2026-09-02", view="history")
    row = result["rows"][0]
    assert row["hits"] == 2 and row["hit_intervals"] == ["1h", "30m"]
    assert row["entry_price"] == 101. and row["latest_price"] == 110. and row["paper_return"] == .02
    assert data == original


def test_history_withholds_latest_run_before_filters_then_releases_it_after_next_run():
    data = dict(snapshot(), source="SHADOW")
    data["publications"] = data["publications"][:1]
    data["alerts"][0]["triggered_at"] = "2026-09-02T14:00:00Z"
    original = deepcopy(data)
    assert alert_page(data, session="2026-09-02", view="history")["rows"] == []
    assert data == original
    data["publications"].append(dict(run_id="two", session="2026-09-02", published_at="2026-09-02T14:47:00Z"))
    data["alerts"].append(dict(data["alerts"][0], alert_id="new", run_id="two", ticker="B", security_id="B"))
    history = alert_page(data, session="2026-09-02", view="history", search="B", run="one")
    assert not history["rows"] and history["withheld_run"]["run_id"] == "two"
    assert [row["run_id"] for row in history["runs"]] == ["one"]
    assert [row["alert_id"] for row in alert_page(data, session="2026-09-02", view="history")["rows"]] == ["plan"]
    data["publications"].append(dict(run_id="three-empty", session="2026-09-02", published_at="2026-09-02T15:17:00Z"))
    assert alert_page(data, session="2026-09-02", view="history")["total"] == 2
    assert alert_page(data, session="2026-09-02")["total"] == 0
    assert data["alerts"][0] == original["alerts"][0]


def test_history_preserves_older_same_ticker_alerts_and_all_previous_session_runs():
    data = dict(snapshot(), source="SHADOW")
    data["alerts"].append(dict(data["alerts"][0], alert_id="latest-plan", run_id="empty"))
    history = alert_page(data, session="2026-09-02", view="history", search="A")
    assert [row["alert_id"] for row in history["rows"]] == ["plan"]
    data["publications"].append(dict(run_id="next-day", session="2026-09-03", published_at="2026-09-03T14:17:00Z"))
    history = alert_page(data, session="2026-09-02", view="history")
    assert history["total"] == 2 and history["withheld_run"] is None


def test_history_defaults_to_actual_trigger_time_descending_not_publication_or_string_order():
    data = snapshot()
    baseline = data["alerts"][0]
    data["alerts"] = [dict(baseline, alert_id=key, triggered_at=stamp) for key, stamp in [
        ("earlier", "2026-09-02T10:00:00-04:00"), ("newer", "2026-09-02 14:30:00+00:00"),
        ("missing", None), ("invalid", "bad-time")]]
    history = alert_page(data, session="2026-09-02", view="history", limit=1)
    assert history["sort"] == "triggered_at" and history["descending"] is True
    assert history["rows"][0]["alert_id"] == "newer" and history["total"] == 4
    assert alert_page(data, session="2026-09-02", view="history", offset=1, limit=1)["rows"][0]["alert_id"] == "earlier"
    ascending = alert_page(data, session="2026-09-02", view="history", sort="triggered_at", descending=False)
    assert [row["alert_id"] for row in ascending["rows"]] == ["earlier", "newer", "invalid", "missing"]


def test_frozen_replay_history_includes_its_final_run_without_waiting_for_another():
    data = snapshot()
    data["alerts"].append(dict(data["alerts"][0], alert_id="final-plan", run_id="empty"))
    original = deepcopy(data)
    result = alert_page(data, session="2026-09-02", view="history")
    assert result["total"] == 2 and result["withheld_run"] is None
    assert len(result["runs"]) == 2 and data == original


def test_day_history_routes_pre_enrollment_dates_to_frozen_replay_without_relabeling(monkeypatch):
    from equity import stock_alert_views as views
    replay = snapshot()
    original_replay = deepcopy(replay)
    shadow = dict(snapshot(), source="SHADOW", sessions=session_dates("2026-09-14"),
        enrolled_at="2026-09-14T14:50:00Z", publications=[], alerts=[])
    original_shadow = deepcopy(shadow)
    calls = []
    def load(source):
        calls.append(source)
        return replay
    monkeypatch.setattr(views, "load_alert_view", load)
    result = views.history_snapshot_for_date(shadow, "2026-09-02")
    assert result["source"] == "REPLAY" and result["as_of"] == replay["as_of"]
    assert result["sessions"] == shadow["sessions"]
    assert result["alerts"] == replay["alerts"] and result["publications"] == replay["publications"]
    assert views.current_history_prices(result, "2026-09-02") is result
    assert views.history_snapshot_for_date(shadow, "2026-09-14") is shadow
    assert views.history_snapshot_for_date(shadow) is shadow
    assert views.history_snapshot_for_date(shadow, "2026-08-03") is shadow
    assert views.history_snapshot_for_date(replay, "2026-09-02") is replay
    assert calls == ["REPLAY"] and shadow == original_shadow and replay == original_replay


def test_day_history_never_substitutes_backtests_for_real_empty_runs_or_uncovered_dates(monkeypatch):
    from equity import stock_alert_views as views
    replay = snapshot()
    shadow = dict(snapshot(), source="SHADOW", sessions=session_dates("2026-09-14"),
        enrolled_at="2026-09-14T14:50:00Z", alerts=[])
    monkeypatch.setattr(views, "load_alert_view", lambda source: replay)
    assert views.history_snapshot_for_date(shadow, "2026-09-02") is shadow
    assert views.history_snapshot_for_date(shadow, "2026-09-10") is shadow
    assert views.history_snapshot_for_date(shadow, "2026-09-14") is shadow


@pytest.mark.parametrize("direction,current,expected", [(1, 110., .1), (-1, 110., -.1), (-1, 90., .1), (1, 100., 0.)])
def test_price_return_is_available_without_a_paper_entry(direction, current, expected):
    row = dict(lane="TRADE", direction=direction, trigger_price=100., latest_price=current,
        triggered_at="2026-09-14T15:30:00Z", latest_price_at="2026-09-14T16:15:00Z",
        status="PENDING", entry_price=None, paper_return=None)
    original = deepcopy(row)
    result = price_return_fields(row)
    assert result["price_return"] == pytest.approx(expected)
    assert result["price_return_status"] == "AVAILABLE" and row == original


@pytest.mark.parametrize("changes", [dict(latest_price=None), dict(trigger_price=0), dict(latest_price=float("nan")),
    dict(direction=0), dict(lane="WATCH"), dict(latest_price_at=None), dict(latest_price_at="not a date"),
    dict(latest_price_at="2026-09-14T15:00:00Z"), dict(price_comparison_block="SPLIT"), dict(reason="IDENTITY_UNAVAILABLE")])
def test_price_return_keeps_missing_and_incomparable_inputs_unavailable(changes):
    row = dict(lane="TRADE", direction=1, trigger_price=100., latest_price=101.,
        triggered_at="2026-09-14T15:30:00Z", latest_price_at="2026-09-14T16:15:00Z", status="PENDING")
    assert price_return_fields(row | changes)["price_return"] is None


def test_live_price_comparison_never_rewrites_closed_paper_results_and_is_sortable():
    data = snapshot()
    data["alerts"][0].update(triggered_at="2026-09-02T14:00:00Z", latest_price_at="2026-09-03T20:00:00Z")
    original = deepcopy(data)
    result = alert_page(data, session="2026-09-02", view="history", sort="price_return")
    assert result["rows"][0]["price_return"] == pytest.approx(.1)
    assert result["rows"][0]["paper_return"] == .02
    assert data == original


def test_current_history_prices_are_identity_bound_read_only_and_do_not_mutate_snapshot(monkeypatch):
    from contextlib import contextmanager
    from datetime import datetime, timedelta, timezone
    from equity import stock_alert_views as views
    now = datetime(2026, 9, 14, 16, 30, tzinfo=timezone.utc)
    data = dict(snapshot(), source="SHADOW")
    data["alerts"][0].update(triggered_at="2026-09-02T14:00:00Z", latest_price_at="2026-09-03T20:00:00Z")
    original = deepcopy(data)
    queries = []
    class Cursor:
        def execute(self, query, parameters=None):
            queries.append((query, parameters))
        def fetchall(self):
            if "equity_corporate_actions" in queries[-1][0]:
                return [dict(security_id="A", effective_date=now.date(), action_type="SPLIT")]
            return [dict(security_id="A", latest_price=112., latest_price_at=now - timedelta(minutes=15),
                latest_price_revision_id="fresh", latest_price_interval="5m",
                latest_price_observed_at=now, latest_price_created_at=now)]
    @contextmanager
    def cursor():
        yield Cursor()
    monkeypatch.setattr(views, "get_db_cursor", cursor)
    refreshed = views.current_history_prices(data, "2026-09-02", now=now)
    row = refreshed["alerts"][0]
    assert row["latest_price"] == 112. and row["latest_price_interval"] == "5m"
    assert row["paper_return"] == .02 and row["price_comparison_block"] == "KNOWN_CORPORATE_ACTION"
    assert price_return_fields(row)["price_return"] is None and data == original
    assert "READ ONLY" in queries[0][0]
    assert "bar.security_id=requested.security_id::uuid" in queries[2][0]
    assert queries[2][1][-2:] == (now, now)
    assert queries[2][1][-3] <= now - timedelta(minutes=15)
    assert views.current_history_prices(original | dict(source="REPLAY"), "2026-09-02", now=now)["alerts"][0]["latest_price"] == 110.
    assert len(queries) == 4


def test_current_price_failure_preserves_alerts_and_paper_results(monkeypatch):
    from contextlib import contextmanager
    from psycopg2 import OperationalError
    from equity import stock_alert_views as views
    @contextmanager
    def unavailable():
        raise OperationalError("test unavailable")
        yield
    monkeypatch.setattr(views, "get_db_cursor", unavailable)
    data = dict(snapshot(), source="SHADOW")
    result = views.current_history_prices(data, "2026-09-02")
    assert result["alerts"][0]["paper_return"] == .02
    assert result["alerts"][0]["latest_price"] is None
    assert data["alerts"][0]["latest_price"] == 110.


def test_history_navigation_contains_21_exchange_sessions_without_purging():
    data = snapshot()
    assert len(data["sessions"]) == 21
    assert len(session_dates("2026-09-08")) == 21 and "2026-09-07" not in session_dates("2026-09-08")
    with pytest.raises(ValueError, match="21-session"):
        alert_page(data, session="2026-01-01")
    with pytest.raises(ValueError, match="publication"):
        alert_page(data, session="2026-09-03", run="one")


def test_risk_is_directional_distance_not_a_confidence_rating():
    assert risk_fields(100., 98., 104., 1) == dict(risk_pct=.02, reward_risk=2.)
    assert risk_fields(100., 102., 96., -1) == dict(risk_pct=.02, reward_risk=2.)
    assert risk_fields(100., None, None, 0) == dict(risk_pct=None, reward_risk=None)


def test_source_reader_never_labels_replay_as_shadow(tmp_path):
    import json
    from research.stock_alerts import load_snapshot, SCHEMA
    path = tmp_path / "view.json"
    path.write_text(json.dumps(dict(snapshot(), schema=SCHEMA)))
    with pytest.raises(ValueError, match="source mismatch"):
        load_snapshot(path, "SHADOW")
    assert load_snapshot(path, "REPLAY")["source"] == "REPLAY"
    assert load_snapshot(None, "SHADOW")["status"] == "AWAITING_PUBLICATION"


def test_shadow_reader_uses_quality_v2_store_without_legacy_fallback(monkeypatch, tmp_path):
    from pathlib import Path
    from equity import stock_alert_views as views
    from scripts.run_stock_idea_worker import QUALITY_ROOT
    calls = []
    monkeypatch.delenv("STOCK_ALERT_SHADOW_VIEW", raising=False)
    monkeypatch.setattr(views, "load_snapshot", lambda path, source: calls.append((Path(path), source)) or {"status": "AWAITING_PUBLICATION"})
    assert views.load_alert_view("SHADOW", combined=False)["status"] == "AWAITING_PUBLICATION"
    assert calls == [(QUALITY_ROOT / "alerts-view.json", "SHADOW")]
    custom = tmp_path / "approved-view.json"
    monkeypatch.setenv("STOCK_ALERT_SHADOW_VIEW", str(custom))
    views.load_alert_view("SHADOW", combined=False)
    assert calls[-1] == (custom, "SHADOW")


def test_replay_builder_retains_watch_and_frozen_trade_return():
    from research.stock_alerts import replay_snapshot
    from test_stock_idea_engine import candidate, NOW
    from research.stock_idea_engine import candidate_record
    from test_stock_idea_replay import CONFIG
    trade = candidate()
    publication = dict(arm="PRIORITY", window_key="2026-08-03T14:00:00+00:00", deadline=NOW.isoformat(), session="2026-08-03",
        expected_members=["A"], missing_members=[], coverage="PUBLISHED", selected=[trade.episode_id, "watch"],
        dispositions=[dict(episode_id=trade.episode_id, security_id="A", direction=1, model="resumption", interval="30m", reason=None),
                      dict(episode_id="watch", security_id="A", direction=1, model="discovery", interval="1d", reason=None)],
        outcomes={trade.episode_id: dict(candidate=candidate_record(trade), state="CLOSED", net_by_cost_bps={"10": .02},
                                       entry_price=100., exit_price=102.1)})
    result = replay_snapshot([publication], CONFIG, NOW.isoformat(), "test")
    assert len(result["alerts"]) == 2
    assert result["alerts"][0]["paper_return"] == .02
    assert result["alerts"][1]["lane"] == "WATCH" and result["alerts"][1]["paper_return"] is None


def test_alert_view_route_defaults_to_shadow_and_never_calls_capture(monkeypatch):
    from fastapi import FastAPI, HTTPException
    from equity.stock_discovery_api import router, alert_view
    import equity.stock_alert_views as views
    from research.stock_alerts import empty_snapshot
    monkeypatch.setattr(views, "load_alert_view", lambda source: snapshot() if source == "REPLAY" else empty_snapshot(source))
    app = FastAPI()
    app.include_router(router)
    args = dict(run=None, search="", offset=0, limit=100)
    assert alert_view(**args)["source"] == "SHADOW"
    assert alert_view(**args, source="REPLAY", session_date="2026-09-02", view="history")["total"] == 1
    with pytest.raises(HTTPException) as error:
        alert_view(**args, source="REPLAY", session_date="2026-01-01")
    assert error.value.status_code == 422
    parameters = app.openapi()["paths"]["/api/stocks/alert-view"]["get"]["parameters"]
    assert next(parameter for parameter in parameters if parameter["name"] == "source")["schema"]["default"] == "SHADOW"


@pytest.mark.parametrize("direction,expected_status", [("-1", 200), ("0", 200), ("1", 200), ("2", 422), ("-2", 422), ("1.5", 422), ("short", 422)])
def test_alert_direction_query_parses_http_values_and_rejects_invalid_sides(monkeypatch, direction, expected_status):
    import asyncio
    import json
    from urllib.parse import urlencode
    from fastapi import FastAPI
    from equity.stock_discovery_api import router
    import equity.stock_alert_views as views

    monkeypatch.setattr(views, "load_alert_view", lambda source: snapshot())
    app = FastAPI()
    app.include_router(router)
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    query = urlencode(dict(source="REPLAY", view="history", session_date="2026-09-02", direction=direction))
    scope = dict(type="http", http_version="1.1", method="GET", scheme="http", path="/api/stocks/alert-view",
                 root_path="", query_string=query.encode(), headers=[], server=("test", 80), client=("test", 123))
    asyncio.run(app(scope, receive, send))
    assert next(message["status"] for message in messages if message["type"] == "http.response.start") == expected_status
    if expected_status == 200:
        data = json.loads(b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body"))
        assert len(data["sessions"]) == 21
        assert data["total"] == (1 if direction == "1" else 0)
        assert all(row["direction"] == int(direction) for row in data["rows"])


def test_enrichment_is_prefix_causal_and_does_not_reprice_closed_plans():
    from research.stock_alerts import enrich_replay
    from test_stock_idea_replay import full_fixture
    fixture, config = full_fixture()
    data = snapshot()
    data["alerts"][0].update(triggered_at="2026-08-03T14:00:00+00:00", published_at="2026-08-03T14:17:00+00:00", indicators={})
    original = enrich_replay(deepcopy(data), fixture, config)
    changed = deepcopy(fixture)
    last = [bar for bar in changed["bars"] if bar["interval"] == "30m"][-1]
    last.update(close=200., high=201.)
    enriched = enrich_replay(deepcopy(data), changed, config)
    assert enriched["alerts"][0]["indicators"] == original["alerts"][0]["indicators"]
    assert enriched["alerts"][0]["latest_price"] == 200.
    assert enriched["alerts"][0]["paper_return"] == .02
    assert enriched["alerts"][0]["stop"] == 99. and enriched["alerts"][0]["target"] == 105.


def test_filtering_and_indicator_sort_never_change_the_original_run():
    data = snapshot()
    data["alerts"][0]["indicators"] = dict(rsi=55.)
    data["alerts"].append(dict(data["alerts"][0], alert_id="second", ticker="B", security_id="B", indicators=dict(rsi=70.)))
    result = alert_page(data, session="2026-09-02", view="history", sort="rsi")
    assert [row["ticker"] for row in result["rows"]] == ["B", "A"]
    assert not alert_page(data, session="2026-09-02", search="A")["rows"]
    assert alert_page(data, session="2026-09-02", view="history", direction=-1)["total"] == 0