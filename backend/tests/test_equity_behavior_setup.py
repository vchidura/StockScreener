from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from equity.behavior import BehaviorSource
from equity.behavior_setup import bind_stock_setup
from research.stock_idea_engine import candidate_record, digest
from test_stock_idea_engine import NOW, candidate


def setup_inputs(**changes):
    stock = candidate(
        str(uuid4()), model="acceptance", interval="1h", ticker="AAPL",
        policy_version="range_breakout_acceptance_intraday_v1",
        reference=99., stop=98., target=104., price=100., revision_ids=(str(uuid4()),),
    )
    stock = replace(stock, **changes)
    lifecycle = dict(setup="TRIGGERED", health="READY",
                     candidate={**candidate_record(stock), "episode_id": stock.episode_id},
                     source_revision_ids=list(stock.revision_ids))
    source = BehaviorSource(
        evidence_id=uuid4(), security_id=stock.security_id, interval=stock.interval,
        payload_sha256=digest(lifecycle), policy_sha256="a" * 64,
        market_time=stock.trigger_at, observed_at=stock.available_at,
        recorded_at=NOW, received_at=NOW, valid_until=stock.expires_at,
        price_basis="RAW_ACTION_GATED", availability_mode="PROSPECTIVE_RECEIPT",
    )
    return lifecycle, source


def test_setup_adapter_preserves_episode_geometry_and_causal_receipt():
    lifecycle, source = setup_inputs()
    setup = bind_stock_setup(lifecycle, source)
    assert setup.candidate.episode_id == lifecycle["candidate"]["episode_id"]
    assert setup.candidate.target == 104.
    assert setup.available_at(NOW, source.market_time)
    assert not setup.available_at(NOW - timedelta(seconds=1), source.market_time)
    assert not setup.available_at(source.valid_until, source.market_time)
    assert not setup.available_at(NOW, source.market_time - timedelta(seconds=1))
    assert setup.execution_permission is False


@pytest.mark.parametrize("mutation", ["hash", "episode", "revisions", "state", "identity", "basis", "expired", "version"])
def test_setup_adapter_rejects_unbound_or_unavailable_evidence(mutation):
    lifecycle, source = setup_inputs()
    if mutation == "hash":
        lifecycle["candidate"]["target"] += 1
    if mutation == "episode":
        lifecycle["candidate"]["episode_id"] = "0" * 64
    if mutation == "revisions":
        lifecycle["source_revision_ids"] = []
    if mutation == "state":
        lifecycle["setup"] = "SETUP"
    if mutation == "version":
        lifecycle["candidate"]["policy_version"] = "unknown"
    changes = {} if mutation == "hash" else {"payload_sha256": digest(lifecycle)}
    if mutation == "identity":
        changes["security_id"] = uuid4()
    if mutation == "basis":
        changes.update(price_basis="PROVIDER_SPLIT_ADJUSTED", source_manifest_sha256="b" * 64)
    if mutation == "expired":
        changes["received_at"] = source.valid_until
    source = BehaviorSource.model_validate({**source.model_dump(), **changes})
    with pytest.raises(ValueError):
        bind_stock_setup(lifecycle, source)


def publication_inputs(model="acceptance"):
    from equity.behavior_setup import PublishedSetupSourcePolicy
    from research.stock_alert_results import strategy_instance

    stock = candidate(str(uuid4()), model=model, interval="1h", ticker="AAPL",
        policy_version="relative_trend_resumption_intraday_v1" if model == "resumption" else "range_breakout_acceptance_intraday_v2",
        revision_ids=(str(uuid4()),))
    config = {"policy_version": "stock_ideas_forward_quality_v2"}
    state = {"enrolled_at": (NOW - timedelta(days=1)).isoformat(), "members": [{"security_id": stock.security_id}]}
    instance = strategy_instance(config, state, "intraday") | {"policy": config, "enrollment": state["members"]}
    runtime = {name: "c" * 64 for name in ("research/stock_idea_models.py", "research/stock_idea_engine.py",
        "equity/stock_idea_forward_source.py", "research/stock_idea_forward.py", "research/stock_idea_replay.py",
        "scripts/run_stock_idea_worker.py")}
    publication = dict(window_key=stock.trigger_at.isoformat(), actual_publication_at=(NOW - timedelta(seconds=2)).isoformat(),
        policy_hash="a" * 64, runtime_sources=runtime, policy_version=config["policy_version"], source="SHADOW", coverage="PUBLISHED",
        input_deadline=(NOW - timedelta(seconds=3)).isoformat(), latest_dispatch_at=NOW.isoformat(),
        candidates={stock.episode_id: candidate_record(stock)}, expected_members=[stock.security_id], missing_members=[],
        dispositions=[dict(episode_id=stock.episode_id, security_id=stock.security_id, model=stock.model, interval=stock.interval,
            direction=stock.direction, selection="SELECTED", reason=None)])
    policy = PublishedSetupSourcePolicy(instance_id=instance["instance_id"], instance_policy_sha256=digest(config),
        publication_policy_sha256=publication["policy_hash"], runtime_sources=tuple(sorted(runtime.items())))
    return stock, instance, publication, policy


def test_direct_resumption_uses_separate_policy_without_acceptance_relabeling():
    from uuid import UUID
    from equity.behavior_setup import DirectResumptionSourcePolicy, DirectStockResumptionEvidence, bind_direct_stock_setup

    stock, instance, publication, shared_policy = publication_inputs("resumption")
    payload = direct_policy(shared_policy).model_dump()
    payload.update(version="stock_resumption_direct_source_v1", detector_version="relative_trend_resumption_intraday_v1")
    policy = DirectResumptionSourcePolicy.model_validate(payload)
    arguments = dict(instance=instance, publication=publication, record_id=publication["window_key"],
        payload_sha256=digest(publication), received_at=NOW, episode_id=stock.episode_id,
        security_id=UUID(stock.security_id), ticker=stock.ticker)
    result = bind_direct_stock_setup(**arguments, policy=policy)
    assert type(result) is DirectStockResumptionEvidence
    assert result.candidate == stock and result.candidate.model == "resumption"
    assert result.available_at(NOW, stock.trigger_at)
    with pytest.raises(ValueError, match="detector/episode/identity"):
        bind_direct_stock_setup(**arguments, policy=direct_policy(shared_policy))
    with pytest.raises(ValueError, match="receipt"):
        bind_direct_stock_setup(**{**arguments, "received_at": stock.expires_at}, policy=policy)


@pytest.mark.parametrize("reason", [None, "MODEL_QUOTA", "ACTIVE_POSITION_CAP"])
def test_published_setup_binds_original_candidate_not_portfolio_selection(reason):
    from equity.behavior_setup import bind_published_stock_setup
    from uuid import UUID

    stock, instance, publication, policy = publication_inputs()
    publication["dispositions"][0].update(selection="SUPPRESSED" if reason else "SELECTED", reason=reason)
    result = bind_published_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
        payload_sha256=digest(publication), imported_at=NOW - timedelta(seconds=1), received_at=NOW,
        episode_id=stock.episode_id, security_id=UUID(stock.security_id), ticker=stock.ticker, policy=policy)
    assert result.candidate == stock
    assert result.available_at(NOW, stock.trigger_at)
    assert result.schema_version == "stock_setup_publication_evidence_v1"
    assert not result.available_at(NOW - timedelta(seconds=1), stock.trigger_at)


@pytest.mark.parametrize("mutation", ["hash", "instance", "runtime", "policy", "missing", "expired", "blocked", "late", "identity"])
def test_published_setup_rejects_untrusted_late_or_mismatched_source(mutation):
    from equity.behavior_setup import bind_published_stock_setup
    from uuid import UUID

    stock, instance, publication, policy = publication_inputs()
    if mutation == "instance": instance["policy"]["unexpected"] = True
    if mutation == "runtime": publication["runtime_sources"]["research/stock_idea_models.py"] = "d" * 64
    if mutation == "policy": publication["policy_hash"] = "d" * 64
    if mutation == "missing": publication["missing_members"] = [stock.security_id]
    if mutation == "blocked": publication["dispositions"][0].update(selection="SUPPRESSED", reason="DATA_RISK")
    if mutation == "late": publication["actual_publication_at"] = (NOW + timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError):
        bind_published_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
            payload_sha256="0" * 64 if mutation == "hash" else digest(publication),
            imported_at=NOW - timedelta(seconds=1), received_at=stock.expires_at if mutation == "expired" else NOW,
            episode_id=stock.episode_id, security_id=uuid4() if mutation == "identity" else UUID(stock.security_id),
            ticker=stock.ticker, policy=policy)


@pytest.mark.parametrize("mutation", [None, "schema", "missing", "oversize", "future", "receipt"])
def test_published_setup_reader_is_exact_bounded_read_only(mutation):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from uuid import UUID
    from equity.stock_alert_results import read_published_stock_setup

    stock, instance, publication, policy = publication_inputs()
    if mutation == "future":
        publication["market_time"] = (NOW + timedelta(seconds=1)).isoformat()
    row = dict(instance=instance, publication=publication, payload_sha256=digest(publication),
               imported_at=NOW - timedelta(seconds=1))
    if mutation == "oversize": row["publication"] = None
    cursor = MagicMock()
    cursor.fetchone.side_effect = [{"ready": mutation != "schema"}, None if mutation == "missing" else row]

    @contextmanager
    def factory():
        yield cursor

    arguments = dict(policy=policy, record_id=publication["window_key"], payload_sha256=digest(publication),
        episode_id=stock.episode_id, security_id=UUID(stock.security_id), ticker=stock.ticker,
        market_cutoff=stock.trigger_at, observed_cutoff=NOW, cursor_factory=factory,
        clock=lambda: NOW - timedelta(seconds=1) if mutation == "receipt" else NOW)
    if mutation in ("oversize", "future", "receipt"):
        with pytest.raises(ValueError):
            read_published_stock_setup(**arguments)
    else:
        result = read_published_stock_setup(**arguments)
        assert (result is None) == (mutation in ("schema", "missing"))
        if result:
            assert result.candidate == stock and result.source.received_at == NOW
    sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "READ ONLY" in sql and "statement_timeout='5s'" in sql
    assert "INSERT" not in sql and "UPDATE" not in sql
    if mutation != "schema":
        query, parameters = cursor.execute.call_args.args
        assert "record.record_id=%s" in query and "LIMIT 1" in query
        assert parameters == (policy.instance_id, publication["window_key"], digest(publication), NOW, NOW)


def test_setup_publication_inventory_never_approves_historical_receipt():
    from equity.behavior_setup import summarize_setup_publications

    stock, instance, publication, policy = publication_inputs()
    report = summarize_setup_publications([dict(instance=instance, publication=publication,
        record_id=publication["window_key"], payload_sha256=digest(publication), imported_at=NOW)],
        ("AAPL", "SPY"), stock.expires_at + timedelta(seconds=1))
    assert report["status"] == "DIAGNOSTIC_NOT_APPROVED"
    assert report["counts"]["published_before_expiry"] == 1
    assert report["counts"]["unexpired_at_read_receipt"] == 0
    assert report["missing_hourly_acceptance_tickers"] == ["SPY"]
    assert report["source_policy_approved"] is False
    assert report["source_identity_groups"][0]["instance_id"] == policy.instance_id
    assert report["distinct_hourly_acceptance_episodes"] == 1
    assert report["samples"][0]["publication_slack_seconds"] == (stock.expires_at - NOW).total_seconds() + 2
    assert report["samples"][0]["projection_lag_seconds"] == 2


def test_setup_inventory_excludes_hash_mismatch_without_repair():
    from equity.behavior_setup import summarize_setup_publications

    stock, instance, publication, _ = publication_inputs()
    row = dict(instance=instance, publication=publication, record_id=publication["window_key"],
               payload_sha256="0" * 64, imported_at=NOW)
    report = summarize_setup_publications([row], ("AAPL",), NOW)
    assert report["distinct_hourly_acceptance_episodes"] == 0
    assert report["excluded_records"][0]["record_id"] == publication["window_key"]
    assert report["excluded_records"][0]["resolved_sha256"] == digest(publication)
    assert row["payload_sha256"] == "0" * 64


def test_setup_inventory_uses_bounded_read_only_current_stream():
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from equity.stock_alert_results import read_setup_publication_inventory

    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.return_value = []
    @contextmanager
    def factory():
        yield cursor
    assert read_setup_publication_inventory(limit=12, cursor_factory=factory, clock=lambda: NOW) == ((), NOW)
    sql, parameters = cursor.execute.call_args.args
    assert "stream.stream='intraday'" in sql and "LIMIT %s" in sql
    assert parameters == (NOW, 12, NOW)
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    with pytest.raises(ValueError):
        read_setup_publication_inventory(limit=49, cursor_factory=factory)


@pytest.mark.parametrize("mutation", ["none", "signed_zero", "number", "type", "source", "policy"])
def test_original_comparison_distinguishes_normalization_and_mutation(mutation):
    from copy import deepcopy
    from equity.behavior_setup import compare_original_setup_publication

    _, instance, original, policy = publication_inputs()
    original["diagnostic"] = -0.0
    shared = deepcopy(original)
    if mutation == "signed_zero": shared["diagnostic"] = 0.0
    if mutation == "number": shared["diagnostic"] = 1.0
    if mutation == "type": shared["diagnostic"] = 0
    row = dict(record_id=original["window_key"], payload_sha256=digest(original), publication=shared, instance=instance)
    if mutation == "source": original["diagnostic"] = 2.0
    result = compare_original_setup_publication(row, original,
        "0" * 64 if mutation == "policy" else policy.instance_policy_sha256)
    expected = {"none": "EXACT_MATCH", "signed_zero": "JSONB_SIGNED_ZERO_NORMALIZATION",
        "number": "SHARED_PAYLOAD_DIFFERS", "type": "SHARED_PAYLOAD_DIFFERS",
        "source": "ORIGINAL_CHECKSUM_MISMATCH", "policy": "ORIGINAL_INSTANCE_POLICY_MISMATCH"}
    assert result["status"] == expected[mutation]
    assert result["writes_performed"] is False
    assert result["historical_receipt_verified"] is False


def test_original_setup_ledger_reader_is_query_only_and_preserves_bytes(tmp_path):
    import hashlib
    import json
    import sqlite3
    import zlib
    from equity.stock_alert_results import read_original_setup_publications

    _, instance, publication, _ = publication_inputs()
    path = tmp_path / "source.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript("CREATE TABLE forward_manifest(singleton INTEGER, policy_hash TEXT, payload TEXT);"
                             "CREATE TABLE forward_publications(window_key TEXT PRIMARY KEY, payload BLOB);")
    connection.execute("INSERT INTO forward_manifest VALUES(1,?,?)", (instance["policy_hash"], json.dumps(instance["policy"])))
    connection.execute("INSERT INTO forward_publications VALUES(?,?)", (publication["window_key"], zlib.compress(json.dumps(publication).encode())))
    connection.commit()
    connection.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    policy_hash, rows = read_original_setup_publications(path, [publication["window_key"], "absent"])
    assert rows == {publication["window_key"]: json.loads(json.dumps(publication))}
    assert policy_hash == instance["policy_hash"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    with pytest.raises(ValueError):
        read_original_setup_publications(tmp_path / "missing.sqlite", [publication["window_key"]])
    assert not (tmp_path / "missing.sqlite").exists()


def direct_policy(publication_policy):
    from equity.behavior_setup import DirectSetupSourcePolicy
    return DirectSetupSourcePolicy(**publication_policy.model_dump(exclude={"version"}))


def test_direct_setup_preserves_signed_zero_without_inventing_recording_clock():
    from uuid import UUID
    from equity.behavior_setup import bind_direct_stock_setup

    stock, instance, publication, shared_policy = publication_inputs()
    publication["candidates"][stock.episode_id]["room_risk"] = -0.0
    policy = direct_policy(shared_policy)
    result = bind_direct_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
        payload_sha256=digest(publication), received_at=NOW, episode_id=stock.episode_id,
        security_id=UUID(stock.security_id), ticker=stock.ticker, policy=policy)
    assert '"room_risk":-0.0' in result.candidate_payload_text
    assert "recorded_at" not in result.source.model_dump()
    assert result.source.policy_sha256 != shared_policy.sha256
    assert result.source.observed_at < result.source.received_at
    assert result.available_at(NOW, stock.trigger_at)


@pytest.mark.parametrize("mutation", ["shared_policy", "expired", "hash"])
def test_direct_setup_never_falls_back_or_backdates(mutation):
    from uuid import UUID
    from equity.behavior_setup import bind_direct_stock_setup

    stock, instance, publication, shared_policy = publication_inputs()
    with pytest.raises(ValueError):
        bind_direct_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
            payload_sha256="0" * 64 if mutation == "hash" else digest(publication),
            received_at=stock.expires_at if mutation == "expired" else NOW, episode_id=stock.episode_id,
            security_id=UUID(stock.security_id), ticker=stock.ticker,
            policy=shared_policy if mutation == "shared_policy" else direct_policy(shared_policy))


def direct_ledger_fixture(tmp_path, model="acceptance"):
    import json
    import sqlite3
    import zlib

    stock, instance, publication, shared_policy = publication_inputs(model)
    publication["candidates"][stock.episode_id]["room_risk"] = -0.0
    path = tmp_path / "direct.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript("CREATE TABLE forward_manifest(singleton INTEGER, policy_hash TEXT, payload TEXT);"
                                 "CREATE TABLE forward_publications(window_key TEXT PRIMARY KEY, payload BLOB);"
                                 "CREATE TABLE forward_checkpoint(singleton INTEGER, payload BLOB);")
        connection.execute("INSERT INTO forward_manifest VALUES(1,?,?)", (instance["policy_hash"], json.dumps(instance["policy"])))
        connection.execute("INSERT INTO forward_checkpoint VALUES(1,?)", (zlib.compress(json.dumps({
            "enrolled_at": instance["enrolled_at"], "members": instance["enrollment"]}).encode()),))
    connection.close()
    policy = direct_policy(shared_policy)
    if model == "resumption":
        from equity.behavior_setup import DirectResumptionSourcePolicy

        policy = DirectResumptionSourcePolicy.model_validate({**policy.model_dump(),
            "version": "stock_resumption_direct_source_v1", "detector_version": "relative_trend_resumption_intraday_v1"})
    return path, stock, publication, policy


@pytest.mark.parametrize("model", ["acceptance", "resumption"])
def test_direct_reader_observes_only_committed_publications_without_source_writes(tmp_path, model):
    import hashlib
    import json
    import sqlite3
    import zlib
    from uuid import UUID
    from equity.stock_alert_results import read_direct_stock_setup

    path, stock, publication, policy = direct_ledger_fixture(tmp_path, model)
    arguments = dict(policy=policy, record_id=publication["window_key"], payload_sha256=digest(publication),
        episode_id=stock.episode_id, security_id=UUID(stock.security_id), ticker=stock.ticker,
        market_cutoff=stock.trigger_at, clock=lambda: NOW)
    writer = sqlite3.connect(path)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO forward_publications VALUES(?,?)", (publication["window_key"], zlib.compress(json.dumps(publication).encode())))
        assert read_direct_stock_setup(path, **arguments) is None
        writer.commit()
    finally:
        writer.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    evidence = read_direct_stock_setup(path, **arguments)
    assert evidence.candidate.model == model
    assert '"room_risk":-0.0' in evidence.candidate_payload_text
    assert evidence.source.received_at == NOW
    assert not evidence.available_at(NOW - timedelta(seconds=1), stock.trigger_at)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert read_direct_stock_setup(tmp_path / "missing.sqlite", **arguments) is None
    assert not (tmp_path / "missing.sqlite").exists()


@pytest.mark.parametrize("model", ["acceptance", "resumption"])
def test_direct_batch_reader_preserves_committed_source_and_expiry(tmp_path, model):
    import hashlib
    import json
    import sqlite3
    import zlib
    from equity.stock_alert_results import read_direct_stock_setups

    path, stock, publication, policy = direct_ledger_fixture(tmp_path, model)
    arguments = dict(policies=(policy,), underlyers=(stock.ticker,), market_cutoff=stock.trigger_at, as_of=NOW, clock=lambda: NOW)
    with sqlite3.connect(path) as writer:
        writer.execute("INSERT INTO forward_publications VALUES(?,?)", (publication["window_key"], zlib.compress(json.dumps(publication).encode())))
        assert read_direct_stock_setups(path, **arguments) == ()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    results = read_direct_stock_setups(path, **arguments)
    assert len(results) == 1 and results[0].candidate.model == model
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert read_direct_stock_setups(path, **{**arguments, "clock": lambda: stock.expires_at}) == ()


@pytest.mark.parametrize("mutation", ["hash", "clock", "expiry", "cutoff", "schema"])
def test_direct_reader_rejects_invalid_source_or_late_receipt(tmp_path, mutation):
    import json
    import sqlite3
    import zlib
    from uuid import UUID
    from equity.stock_alert_results import read_direct_stock_setup

    path, stock, publication, policy = direct_ledger_fixture(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute("INSERT INTO forward_publications VALUES(?,?)", (publication["window_key"],
            zlib.compress(json.dumps(publication).encode()) if mutation != "schema" else zlib.compress(b'[]')))
    connection.close()
    moments = iter([NOW, NOW - timedelta(seconds=1) if mutation == "clock" else stock.expires_at if mutation == "expiry" else NOW])
    with pytest.raises(ValueError):
        read_direct_stock_setup(path, policy=policy, record_id=publication["window_key"],
            payload_sha256="0" * 64 if mutation == "hash" else digest(publication), episode_id=stock.episode_id,
            security_id=UUID(stock.security_id), ticker=stock.ticker,
            market_cutoff=stock.trigger_at - timedelta(seconds=1) if mutation == "cutoff" else stock.trigger_at,
            clock=lambda: next(moments))