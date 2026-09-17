from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.stock_idea_engine import candidate_record
from research.stock_idea_replay import available_at, derive_hours, execution_times, mark_position, publication_windows, session_windows
from test_stock_idea_engine import NOW, candidate


CONFIG = json.loads((Path(__file__).resolve().parents[2] / "backend/research/inputs/stock_idea_pilot_config.json").read_text())


def bars(session="2026-08-03"):
    return [dict(security_id="A", ticker="A", interval="30m", session=session,
                 bar_start=start.isoformat(), bar_end=end.isoformat(), open=100., high=101., low=99.,
                 close=100., volume=1000., revision_id=f"{session}-{index}",
                 system_observed_at=(end + timedelta(minutes=15)).isoformat(),
                 created_at=(end + timedelta(minutes=15)).isoformat())
            for index, (start, end) in enumerate(session_windows(session))]


def test_exchange_windows_cover_dst_half_day_and_shortened_hour():
    assert session_windows("2026-03-06")[0][0].hour == 14
    assert session_windows("2026-03-09")[0][0].hour == 13
    assert len(session_windows("2026-11-27")) == 7
    assert len(session_windows("2026-11-27", "1h")) == 4
    assert session_windows("2026-11-27", "1h")[-1][1] - session_windows("2026-11-27", "1h")[-1][0] == timedelta(minutes=30)
    assert len(publication_windows(CONFIG)) == 24 * 13


def test_hourly_requires_every_underlying_window():
    source = bars()
    hourly = derive_hours(source, CONFIG)
    assert len(hourly) == 7
    assert hourly[-1]["volume"] == 1000
    assert len(derive_hours(source[1:], CONFIG)) == 6
    assert hourly[0]["source_revision_ids"] == [source[0]["revision_id"], source[1]["revision_id"]]


def test_delay_enters_strictly_after_publication_and_exit_cap_is_fixed():
    original = candidate(stop=90., target=120.)
    opening, ending = execution_times(original, NOW, CONFIG)
    assert opening.hour == 14 and opening.minute == 30
    assert ending - opening == timedelta(minutes=240)
    source = bars()
    position = dict(state="PENDING", candidate=candidate_record(original), publication_at=NOW.isoformat())
    result = mark_position(position, source, [], ending + timedelta(minutes=15), CONFIG)
    assert result["state"] == "CLOSED" and result["entry_at"] == opening.isoformat()
    assert result["net_by_cost_bps"]["10"] == -.001
    assert mark_position(result, [], [], ending + timedelta(days=10), CONFIG) == result
    missing = mark_position(position, source[:2] + source[3:], [], ending + timedelta(minutes=15), CONFIG)
    assert missing["state"] == "UNRESOLVED" and "gross" not in missing


def test_actual_retained_availability_and_close_expiry():
    recent = bars("2026-09-02")[0]
    recent["created_at"] = "2026-09-03T00:00:00+00:00"
    assert available_at(recent, CONFIG).day == 3
    closing = session_windows("2026-08-03")[-1][1]
    original = replace(candidate(), trigger_at=closing, expires_at=closing)
    position = dict(state="PENDING", candidate=candidate_record(original), publication_at=(closing + timedelta(minutes=17)).isoformat())
    result = mark_position(position, bars(), [], closing + timedelta(hours=1), CONFIG)
    assert result["state"] == "NO_FILL" and result["gross"] == 0 and not result["cost_applies"]


def test_no_fill_has_no_cost_and_same_bar_ambiguity_uses_stop():
    original = candidate()
    position = dict(state="PENDING", candidate=candidate_record(original), publication_at=NOW.isoformat())
    source = bars()
    source[2].update(high=120., low=90.)
    result = mark_position(position, source, [], NOW + timedelta(hours=2), CONFIG)
    assert result["reason"] == "STOP_FIRST_AMBIGUOUS" and result["exit_price"] == 98.
    source[2]["volume"] = 0
    result = mark_position(position, source, [], NOW + timedelta(hours=2), CONFIG)
    assert result["state"] == "NO_FILL" and not result["cost_applies"]


def model_fixture():
    import exchange_calendars
    from research.stock_idea_engine import digest
    calendar = exchange_calendars.get_calendar("XNYS")
    source = [bar for session in calendar.sessions_in_range("2026-06-15", "2026-08-03") for bar in bars(str(session.date()))]
    for index, bar in enumerate(source):
        bar.update(open=100., close=100., high=100.1, low=99.9, volume=1_000_000.)
        if 195 <= index < 200:
            bar.update(high=100.025, low=99.975)
        if index == 200:
            bar.update(open=100., close=100.2, high=100.25, low=100.)
        if index == 201:
            bar.update(open=100.2, close=100.16, high=100.25, low=100.1)
    return dict(bars=source, actions=[], memberships=[], covered_security_ids=["A"], source_cutoff="2026-09-13T00:00:00+00:00",
                config_sha256=digest(CONFIG), bars_sha256=digest(source), memberships_sha256=digest([]),
                actions_sha256=digest([]), action_coverage="KNOWN_ACTIONS_ONLY_NOT_CERTIFIED")


def test_intraday_adapter_prefix_causality_and_reset_identity():
    import pandas as pd
    from research.stock_idea_models import feature_frames, intraday_observations
    fixture = model_fixture()
    frame = feature_frames(fixture, CONFIG)[("A", "30m")]
    context = dict(rs63=.1, rs_percentile=.9, close=100., ema50=99., ema50_prior10=98.,
                   momentum=.2, liquidity=100_000_000., visible_at=pd.Timestamp("2026-06-01T20:15:00Z"), rank_revision_ids=["daily-rank"])
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS")
    contexts = {("A", str(calendar.previous_session(session).date())): context for session in frame.session.unique()}
    prefix = intraday_observations(frame.iloc[:202], "30m", contexts, CONFIG)
    full = intraday_observations(frame, "30m", contexts, CONFIG)
    assert prefix == full[:202]
    assert any(item.model == "acceptance" for item in prefix[-1]["candidates"])
    absent = intraday_observations(frame.iloc[:202], "30m", {}, CONFIG)
    assert not any(item["candidates"] for item in absent)


def test_complete_higher_context_cannot_see_unfinished_month():
    import pandas as pd
    import exchange_calendars
    from research.stock_idea_models import higher_context_observations
    calendar = exchange_calendars.get_calendar("XNYS")
    frame = pd.DataFrame([dict(session=str(session.date()), security_id="A", close=100. + index,
                               bar_end=calendar.session_close(session), visible_at=calendar.session_close(session) + pd.Timedelta(minutes=15),
                               clock_valid=True, segment=1, revision_id=str(session.date()))
                          for index, session in enumerate(calendar.sessions_in_range("2026-07-01", "2026-08-14"))])
    result = higher_context_observations(frame, CONFIG)
    monthly = [item for item in result if item["interval"] == "1mo"]
    assert len(monthly) == 1 and monthly[0]["market_time"].date().isoformat() == "2026-07-31"
    assert not monthly[0]["ready"]


def test_replay_packet_order_atomic_restart_and_random_size_matching(tmp_path):
    from research.stock_idea_engine import PublicationLedger, digest
    from research.stock_idea_replay import replay_observations
    fixture = model_fixture()
    fixture["memberships"] = [dict(session="2026-08-03", security_id="A")]
    config = dict(CONFIG, end="2026-08-03")
    boundary = NOW - timedelta(minutes=17)
    packets = [dict(security_id="A", interval="30m", market_time=boundary, available_at=NOW - timedelta(minutes=2),
                    ready=True, candidates=[candidate(model=model)], updates=[], revision_ids=[model])
               for model in ("resumption", "acceptance", "failure")]
    first = replay_observations(fixture, config, packets, PublicationLedger(tmp_path / "first.sqlite"))
    second = replay_observations(fixture, config, list(reversed(packets)), PublicationLedger(tmp_path / "second.sqlite"))
    assert first == second
    restart = replay_observations(fixture, config, packets, PublicationLedger(tmp_path / "first.sqlite"))
    assert first == restart
    assert len(first[0]) == 13 * 6
    initial = [publication for publication in first[0] if publication["window_key"] == boundary.isoformat()]
    assert all(len(publication["selected"]) == 3 for publication in initial)


def full_fixture():
    import exchange_calendars
    from research.stock_idea_engine import digest
    calendar = exchange_calendars.get_calendar("XNYS")
    fixture = model_fixture()
    sessions = calendar.sessions_in_range("2025-05-01", "2026-08-03")
    for ticker, base in (("A", 80.), ("SPY", 400.)):
        for index, session in enumerate(sessions):
            start, end = session_windows(str(session.date()), "1d")[0]
            price = base + index * .02
            fixture["bars"].append(dict(security_id=ticker, ticker=ticker, interval="1d", session=str(session.date()),
                bar_start=start.isoformat(), bar_end=end.isoformat(), open=price, high=price + .1,
                low=price - .1, close=price, volume=1_000_000., revision_id=f"{ticker}-{session.date()}",
                system_observed_at=(end + timedelta(minutes=15)).isoformat(),
                created_at=(end + timedelta(minutes=15)).isoformat()))
    fixture["memberships"] = [dict(session=str(session.date()), security_id="A", ticker="A", universe_run_id=str(session.date()),
                                    observed_at="2026-09-12T00:00:00+00:00", created_at="2026-09-12T00:00:00+00:00") for session in sessions]
    config = dict(CONFIG, end="2026-08-03")
    fixture.update(config_sha256=digest(config), memberships_sha256=digest(fixture["memberships"]), bars_sha256=digest(fixture["bars"]))
    return fixture, config


def test_full_replay_worker_parity_and_immutable_artifacts(tmp_path):
    from research.stock_idea_replay import run_replay, validate_inputs
    fixture, config = full_fixture()
    assert validate_inputs(fixture, config)["status"] == "VALIDATED"
    first = run_replay(fixture, config, tmp_path / "serial", workers=1)
    second = run_replay(fixture, config, tmp_path / "parallel", workers=4)
    assert first == second
    for name in ("publications.json", "candidate_outcomes.json", "daily_rank_coverage.json", "manifest.json", "evaluation.json"):
        assert (tmp_path / "serial" / name).read_bytes() == (tmp_path / "parallel" / name).read_bytes()
    import pytest
    with pytest.raises(ValueError, match="unrelated nonempty"):
        run_replay(fixture, config, tmp_path)


def test_validation_rejects_unpinned_revisions_and_changed_contract():
    from research.stock_idea_replay import validate_inputs
    from research.stock_idea_engine import digest
    fixture, config = full_fixture()
    fixture["bars"][0]["created_at"] = "2026-09-14T00:00:00+00:00"
    result = validate_inputs(fixture, config)
    assert any("beyond source cutoff" in error for error in result["errors"])
    assert "pinned bar checksum mismatch" in result["errors"]
    config["provider_delay_minutes"] = 0
    fixture["config_sha256"] = digest(config)
    assert any("timing/indicator" in error for error in validate_inputs(fixture, config)["errors"])


def test_coverage_does_not_equate_stored_bars_with_deadline_visibility():
    from research.stock_idea_engine import digest
    from research.stock_idea_replay import validate_inputs
    fixture, config = full_fixture()
    config["retained_live_from"] = "2026-08-03T00:00:00+00:00"
    current = next(bar for bar in fixture["bars"] if bar["interval"] == "30m" and bar["session"] == "2026-08-03")
    current["created_at"] = "2026-08-04T00:00:00+00:00"
    fixture.update(config_sha256=digest(config), bars_sha256=digest(fixture["bars"]))
    result = validate_inputs(fixture, config)
    assert result["status"] == "VALIDATED"
    assert result["coverage"][0]["complete_covered"] == 1
    assert result["coverage"][0]["deadline_complete_covered"] == 0
    assert result["deadline_visible_security_windows"] == 12


def test_offline_plan_does_not_import_database():
    import os
    import subprocess
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("DB_", "POSTGRES_"))}
    script = Path(__file__).resolve().parents[1] / "scripts/run_stock_idea_replay.py"
    result = subprocess.run([sys.executable, str(script), "--plan"], cwd=script.parent,
                            env=environment, capture_output=True, text=True, check=True)
    plan = json.loads(result.stdout)
    assert plan["windows"] == 312 and not plan["writes_database"] and not plan["downloads"]


EVALUATION = json.loads((Path(__file__).resolve().parents[2] / "backend/research/inputs/stock_idea_evaluation_plan.json").read_text())


def test_holm_preserves_the_full_declared_family_including_missing_claims():
    from research.stock_idea_evaluation import holm_adjust
    assert holm_adjust([.01, .03, None, .04]) == [.04, .09, 1., .09]
    assert holm_adjust([None] * 72) == [1.] * 72


def test_short_pilot_does_not_gain_independent_blocks_from_more_trades():
    from research.stock_idea_evaluation import block_statistics
    sessions = sorted({session for session, _, _ in publication_windows(CONFIG)})
    observations = [(session, .01) for session in sessions for _ in range(1000)]
    intraday = block_statistics(observations, sessions, 5, EVALUATION)
    daily = block_statistics(observations, sessions, 21, EVALUATION)
    assert intraday["populated_blocks"] == 4 and daily["populated_blocks"] == 1
    assert intraday["raw_p"] is None and daily["interval95"] is None
    assert not intraday["estimable"] and intraday["observations"] == 24000


def test_block_inference_uses_calendar_blocks_and_is_reproducible():
    from research.stock_idea_evaluation import block_statistics
    sessions = [str(index) for index in range(210)]
    observations = [(session, .01 + .002 * ((index // 5) % 3 - 1)) for index, session in enumerate(sessions)]
    plan = dict(EVALUATION, bootstrap_replicates=999)
    result = block_statistics(observations, sessions, 5, plan)
    assert result["estimable"] and result["interval95"][0] > 0
    assert result["raw_p"] <= .01
    assert result == block_statistics(observations, sessions, 5, plan)


def test_independent_evaluation_compares_momentum_and_all_random_seeds_directly(tmp_path):
    from research.stock_idea_engine import PublicationLedger
    from research.stock_idea_replay import replay_observations
    from research.stock_idea_evaluation import evaluate_publications
    fixture = model_fixture()
    fixture["memberships"] = [dict(session="2026-08-03", security_id="A")]
    config = dict(CONFIG, end="2026-08-03")
    packets = [dict(security_id="A", interval="30m", market_time=NOW - timedelta(minutes=17),
                    available_at=NOW - timedelta(minutes=2), ready=True, candidates=[candidate()], updates=[], revision_ids=["bar"])]
    publications, _ = replay_observations(fixture, config, packets, PublicationLedger(tmp_path / "comparison.sqlite"))
    gross = {"ALL": .005, "PRIORITY": .02, "MOMENTUM": .03, "RANDOM_1729": .011, "RANDOM_2718": .012, "RANDOM_31415": .013}
    for publication in publications:
        publication["old_alpha_qualification"] = "ROBUST_PASS"
        for position in publication["outcomes"].values():
            position.update(state="CLOSED", gross=gross[publication["arm"]], cost_applies=True)
    result = evaluate_publications(publications, config, EVALUATION, fixture["source_cutoff"])
    assert result["publication_audit"]["valid"]
    assert result["primary_family_size"] == 72 and not result["inherited_qualification_ids"]
    claims = {item["claim"]: item for item in result["claims"] if item["model"] == "resumption" and item["interval"] == "30m" and item["direction"] == 1}
    assert abs(claims["VS_MOMENTUM"]["statistics"]["mean"] + .01) < 1e-12
    assert abs(claims["VS_RANDOM_MEAN"]["statistics"]["mean"] - .008) < 1e-12
    assert all(cell["status"] != "PASS_RESEARCH_ONLY" for cell in result["cells"])
    import copy
    missing = copy.deepcopy(publications)
    peer = next(publication for publication in missing if publication["arm"] == "RANDOM_1729" and publication["selected"])
    peer["outcomes"].clear()
    incomplete = evaluate_publications(missing, config, EVALUATION, fixture["source_cutoff"])
    cell = next(cell for cell in incomplete["cells"] if cell["model"] == "resumption" and cell["interval"] == "30m" and cell["direction"] == 1)
    assert cell["status"] == "INSUFFICIENT_OUTCOME_COVERAGE"
    assert cell["coverage"]["RANDOM_1729"]["unaccounted"] == 1
    random_claim = next(claim for claim in incomplete["claims"] if claim["model"] == "resumption" and claim["interval"] == "30m" and claim["direction"] == 1 and claim["claim"] == "VS_RANDOM_MEAN")
    assert random_claim["statistics"]["mean"] is None
    from research.stock_idea_evaluation import audit_publications
    corrupted = copy.deepcopy(publications)
    corrupted.append(corrupted[0])
    assert "DUPLICATE_OR_UNDECLARED_PUBLICATION" in audit_publications(corrupted, config)["problem_counts"]
    corrupted = copy.deepcopy(publications)
    filled = next(position for publication in corrupted for position in publication["outcomes"].values())
    filled.update(state="NO_FILL", gross=.02, cost_applies=True)
    assert "NO_FILL_EXPOSURE_OR_COST" in audit_publications(corrupted, config)["problem_counts"]


def test_daily_maturity_is_not_conditioned_on_an_early_winning_exit():
    from research.stock_idea_evaluation import window_measure
    from research.stock_idea_replay import utc
    closing = session_windows("2026-08-03")[-1][1]
    original = replace(candidate(), interval="1d", horizon="DAILY_21", trigger_at=closing)
    publication = dict(deadline=(closing + timedelta(minutes=17)).isoformat(),
        dispositions=[dict(selection="SELECTED", model="resumption", direction=1, interval="1d", security_id="A", episode_id=original.episode_id)],
        outcomes={original.episode_id: dict(state="CLOSED", candidate=candidate_record(original), gross=.1, cost_applies=True)})
    measure = window_measure(publication, dict(model="resumption", direction=1, interval="1d"), CONFIG, EVALUATION, utc("2026-08-05T00:00:00Z"))
    assert measure["pending"] == 1 and measure["mature"] == 0 and measure["value"] is None


def test_rank_readiness_cli_does_not_run_detection_or_evaluate_returns(tmp_path, monkeypatch, capsys):
    from scripts.run_stock_idea_replay import main
    from research.stock_idea_replay import write_once
    import research.stock_idea_models as models
    fixture, config = full_fixture()
    write_once(tmp_path / "inputs.json", fixture)
    write_once(tmp_path / "config.json", config)
    monkeypatch.setattr(models, "daily_observations", lambda *args: (_ for _ in ()).throw(AssertionError("no detection")))
    assert main(["--rank-readiness", "--inputs", str(tmp_path / "inputs.json"), "--config", str(tmp_path / "config.json")]) == 0
    report = json.loads((tmp_path / "inputs.rank-readiness.json").read_text())
    assert report["status"] == "READINESS_ONLY" and not report["returns_evaluated"]
    assert report["rows"][0]["covered_with_context"] == 1


def test_evaluation_does_not_allow_inherited_qualifications_or_reduced_testing_family():
    import pytest
    from research.stock_idea_evaluation import validate_plan
    with pytest.raises(ValueError, match="independent"):
        validate_plan(dict(EVALUATION, inherit_previous_qualifications=True), CONFIG)
    with pytest.raises(ValueError, match="72"):
        validate_plan(dict(EVALUATION, trigger_intervals=["30m"]), CONFIG)
    with pytest.raises(ValueError, match="guardrails"):
        validate_plan(dict(EVALUATION, minimum_populated_blocks=2), CONFIG)


def test_supported_synthetic_history_can_pass_only_the_exact_research_cell():
    import exchange_calendars
    from research.stock_idea_evaluation import evaluate_publications
    calendar = exchange_calendars.get_calendar("XNYS")
    start = "2025-01-02"
    end = str(calendar.session_offset(start, 209).date())
    config = dict(CONFIG, start=start, end=end)
    arms = ["ALL", "PRIORITY", "MOMENTUM"] + [f"RANDOM_{seed}" for seed in config["random_seeds"]]
    publications = []
    day = -1
    for session, boundary, deadline in publication_windows(config):
        first_window = boundary == session_windows(session)[0][1]
        if first_window:
            day += 1
        options = [replace(candidate(str(day % 7)), anchor=f"{session}:{arm}", trigger_at=boundary,
                           available_at=deadline, expires_at=boundary + timedelta(minutes=30)) for arm in arms] if first_window else []
        for index, arm in enumerate(arms):
            selected = options[index] if options else None
            rows = [dict(episode_id=option.episode_id, security_id=option.security_id, model="resumption", interval="30m",
                         direction=1, selection="SELECTED" if option == selected else "SUPPRESSED") for option in options]
            gross = .025 + .002 * (day % 3 - 1) if arm == "PRIORITY" else .002
            outcomes = {selected.episode_id: dict(state="CLOSED", candidate=candidate_record(selected),
                entry_at=(boundary + timedelta(minutes=30)).isoformat(), gross=gross, cost_applies=True)} if selected else {}
            publications.append(dict(arm=arm, session=session, window_key=boundary.isoformat(), deadline=deadline.isoformat(),
                expected_members=[str(value) for value in range(7)], dispositions=rows,
                selected=[selected.episode_id] if selected else [], outcomes=outcomes,
                display=[dict(episode_ids=[selected.episode_id])] if selected else []))
    result = evaluate_publications(publications, config, EVALUATION, "2026-09-13T00:00:00+00:00")
    assert result["publication_audit"]["valid"]
    passing = [cell for cell in result["cells"] if cell["status"] == "PASS_RESEARCH_ONLY"]
    assert [(cell["model"], cell["interval"], cell["direction"]) for cell in passing] == [("resumption", "30m", 1)]
    assert not result["live_activation_authorized"] and not result["canonical_qualification_publication"]