from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import timedelta

import pytest

from realtime.advanced_options_acquisition_runtime import (
    acquisition_binding_payload,
)
from realtime.advanced_options_p8_runtime import (
    LegacyArtifactSessionStateAdapter,
    P8DownstreamBindingStore,
    P8ExecutionBundleStore,
    build_p8_waiting_execution_bundle,
    p8_downstream_binding_payload,
    p8_preopen_session_binding_from_payload,
)
from realtime.advanced_options_session_state import (
    LEGACY_DIAGNOSTIC_STATE_SOURCE,
    POSTGRES_STATE_SOURCE,
    PostgresContinuousSessionStateRepository,
    select_options_session_state,
)
from realtime import advanced_options_session_state as state_module
from tests.realtime.test_advanced_options_acquisition_runtime import (
    _binding,
    _downstream_binding,
    _waiting_package,
)


class _Cursor:
    def __init__(self, provider):
        self.provider = provider
        self.sql = ""
        self.rowcount = 0
        self.chain_snapshot = None
        self.parameters = None

    def execute(self, sql, parameters=None):
        if self.provider.failure is not None:
            raise self.provider.failure
        self.sql = sql
        self.parameters = parameters
        self.provider.queries.append(sql)
        if "WITH captured_head AS MATERIALIZED" in sql:
            self.chain_snapshot = tuple(self.provider.bindings)
            if self.provider.append_during_chain_read is not None:
                self.provider.bindings.append(
                    self.provider.append_during_chain_read
                )
                self.provider.append_during_chain_read = None

    def fetchall(self):
        if "advanced_options_continuous_sessions" in self.sql:
            return [self.provider.session_row]
        if "WITH captured_head AS MATERIALIZED" in self.sql:
            bindings = self.chain_snapshot
            if not bindings:
                return [(0, None, None, None, None)]
            head = bindings[-1]
            return [
                (
                    head.sequence,
                    head.binding_sha256,
                    binding.sequence,
                    binding.binding_sha256,
                    p8_downstream_binding_payload(binding),
                )
                for binding in bindings
            ]
        raise AssertionError(f"unexpected fetchall: {self.sql}")

    def fetchone(self):
        if "advanced_options_downstream_heads" in self.sql:
            if not self.provider.bindings:
                return 0, None
            binding = self.provider.bindings[-1]
            return binding.sequence, binding.binding_sha256
        if (
            "SELECT binding_document" in self.sql
            and "advanced_options_downstream_bindings" in self.sql
        ):
            sequence = int(self.parameters[1])
            digest = str(self.parameters[2])
            binding = next(
                (
                    row
                    for row in self.provider.bindings
                    if row.sequence == sequence
                    and row.binding_sha256 == digest
                ),
                None,
            )
            return (
                None
                if binding is None
                else (p8_downstream_binding_payload(binding),)
            )
        raise AssertionError(f"unexpected fetchone: {self.sql}")


class _Unit:
    def __init__(self, provider):
        self.cursor = _Cursor(provider)


class _Provider:
    def __init__(self, session_row, bindings=()):
        self.session_row = session_row
        self.bindings = list(bindings)
        self.queries = []
        self.failure = None
        self.append_during_chain_read = None

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield _Unit(self)


def _state_fixture(tmp_path):
    acquisition = _binding()
    package = _waiting_package(acquisition)
    package_path = tmp_path / "package.json"
    bundle_path = tmp_path / "bundle.json"
    acquisition_path = tmp_path / "acquisition.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    acquisition_path.write_text(
        json.dumps(acquisition_binding_payload(acquisition)),
        encoding="utf-8",
    )
    bundle = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=tmp_path,
    ).write_waiting(
        acquisition,
        written_at=acquisition.authority.prepared_at,
    )
    session = p8_preopen_session_binding_from_payload(
        bundle["session_binding"]
    )
    provider = _Provider((
        str(session.binding_id),
        acquisition.authority.session_date,
        package,
        {},
        acquisition_binding_payload(acquisition),
        bundle["session_binding"],
        bundle,
    ))
    return (
        acquisition,
        package,
        bundle,
        session,
        provider,
        package_path,
        bundle_path,
        acquisition_path,
    )


def test_postgres_state_has_exact_legacy_typed_parity(tmp_path):
    (
        acquisition,
        package,
        bundle,
        session,
        provider,
        package_path,
        bundle_path,
        acquisition_path,
    ) = _state_fixture(tmp_path)
    legacy = LegacyArtifactSessionStateAdapter(
        package_path=package_path,
        bundle_path=bundle_path,
        acquisition_binding_path=acquisition_path,
    )
    postgres = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
    )

    assert postgres.load_package() == legacy.load_package() == package
    assert (
        postgres.load_execution_bundle()
        == legacy.load_execution_bundle()
        == bundle
    )
    assert (
        postgres.load_acquisition_binding()
        == legacy.load_acquisition_binding()
        == acquisition
    )
    assert (
        postgres.load_preopen_session_binding(bundle)
        == legacy.load_preopen_session_binding(bundle)
        == session
    )
    assert len([
        sql
        for sql in provider.queries
        if "advanced_options_continuous_sessions" in sql
    ]) == 1


def test_waiting_bundle_pure_builder_matches_file_store(tmp_path):
    (
        acquisition,
        package,
        bundle,
        _session,
        _provider,
        package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)

    assert build_p8_waiting_execution_bundle(
        package=package,
        acquisition_binding=acquisition,
        written_at=acquisition.authority.prepared_at,
        package_path_reference=str(package_path),
    ) == bundle


def test_postgres_head_poll_discovers_binding_on_bounded_cadence(
    tmp_path,
):
    (
        acquisition,
        _package,
        _bundle,
        session,
        provider,
        _package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)
    ticks = iter((10.0, 10.5, 11.1))
    state = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        head_poll_interval_seconds=1.0,
        monotonic=lambda: next(ticks),
    )

    assert state.load_latest_downstream_binding(session) is None
    binding = _downstream_binding(session, 1)
    provider.bindings.append(binding)
    assert state.load_latest_downstream_binding(session) is None
    assert state.load_latest_downstream_binding(session) == binding

    restarted = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        monotonic=lambda: 20.0,
    )
    assert restarted.load_acquisition_binding() == acquisition
    assert restarted.load_latest_downstream_binding(session) == binding


def test_expired_unchanged_head_poll_skips_document_fetch(
    tmp_path,
    monkeypatch,
):
    (
        acquisition,
        _package,
        _bundle,
        session,
        provider,
        _package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)
    binding = _downstream_binding(session, 1)
    provider.bindings.append(binding)
    ticks = iter((10.0, 12.0))
    parses = []
    parser = state_module.p8_downstream_binding_from_payload
    monkeypatch.setattr(
        state_module,
        "p8_downstream_binding_from_payload",
        lambda value: parses.append(value) or parser(value),
    )
    state = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        head_poll_interval_seconds=1.0,
        monotonic=lambda: next(ticks),
    )

    assert state.load_latest_downstream_binding(session) == binding
    assert len(parses) == 1
    provider.queries.clear()

    assert state.load_latest_downstream_binding(session) == binding
    assert len(parses) == 1
    assert len(provider.queries) == 1
    assert "advanced_options_downstream_heads" in provider.queries[0]
    assert "binding_document" not in provider.queries[0]


def test_database_interruption_is_visible_without_losing_cached_session(
    tmp_path,
):
    (
        acquisition,
        package,
        _bundle,
        session,
        provider,
        _package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)
    state = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        monotonic=lambda: 10.0,
    )
    assert state.load_package() == package
    provider.failure = ConnectionError("injected database interruption")

    with pytest.raises(
        ConnectionError,
        match="database interruption",
    ):
        state.load_latest_downstream_binding(session)
    assert state.load_package() == package

    provider.failure = None
    binding = _downstream_binding(session, 1)
    provider.bindings.append(binding)
    assert state.load_latest_downstream_binding(session) == binding


def test_postgres_export_matches_legacy_artifact_payloads(tmp_path):
    (
        acquisition,
        package,
        bundle,
        session,
        provider,
        _package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)
    first = _downstream_binding(session, 1)
    second = _downstream_binding(
        session,
        2,
        previous_binding_sha256=first.binding_sha256,
    )
    provider.bindings.extend((first, second))
    state = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        monotonic=lambda: 10.0,
    )
    output = tmp_path / "export"

    report = state.export_artifacts(output)

    assert json.loads(
        (output / "package.json").read_text(encoding="utf-8")
    ) == package
    assert json.loads(
        (output / "execution-bundle.json").read_text(
            encoding="utf-8"
        )
    ) == bundle
    assert json.loads(
        (output / "acquisition-binding.json").read_text(
            encoding="utf-8"
        )
    ) == acquisition_binding_payload(acquisition)
    legacy = P8DownstreamBindingStore(
        output / "downstream-bindings",
        session_binding=session,
    )
    assert legacy.bindings() == (first, second)
    assert report["downstream_binding_count"] == 2


def test_chain_export_uses_one_captured_head_during_append(tmp_path):
    (
        acquisition,
        _package,
        _bundle,
        session,
        provider,
        _package_path,
        _bundle_path,
        _acquisition_path,
    ) = _state_fixture(tmp_path)
    first = _downstream_binding(session, 1)
    second = _downstream_binding(
        session,
        2,
        previous_binding_sha256=first.binding_sha256,
    )
    provider.bindings.append(first)
    provider.append_during_chain_read = second
    state = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        monotonic=lambda: 10.0,
    )
    output = tmp_path / "raced-export"

    report = state.export_artifacts(output)

    exported = P8DownstreamBindingStore(
        output / "downstream-bindings",
        session_binding=session,
    )
    assert exported.bindings() == (first,)
    assert report["downstream_binding_count"] == 1
    assert provider.bindings == [first, second]
    restarted = PostgresContinuousSessionStateRepository(
        provider,
        session_date=acquisition.authority.session_date,
        monotonic=lambda: 20.0,
    )
    assert restarted.load_downstream_bindings(session) == (
        first,
        second,
    )


def test_state_source_selection_is_strict(tmp_path):
    provider = _Provider(None)
    postgres = select_options_session_state(
        environment={},
        provider=provider,
        state_source=POSTGRES_STATE_SOURCE,
    )
    assert isinstance(
        postgres,
        PostgresContinuousSessionStateRepository,
    )
    with pytest.raises(ValueError, match="cannot mix artifact"):
        select_options_session_state(
            environment={},
            provider=provider,
            package_path=tmp_path / "package.json",
            state_source=POSTGRES_STATE_SOURCE,
        )
    with pytest.raises(ValueError, match="cannot mix PostgreSQL"):
        select_options_session_state(
            environment={},
            provider=provider,
            package_path=tmp_path / "package.json",
            state_source=LEGACY_DIAGNOSTIC_STATE_SOURCE,
        )
    legacy = select_options_session_state(
        environment={},
        package_path=tmp_path / "package.json",
        state_source=LEGACY_DIAGNOSTIC_STATE_SOURCE,
    )
    assert isinstance(legacy, LegacyArtifactSessionStateAdapter)
    with pytest.raises(ValueError, match="must select"):
        select_options_session_state(environment={})
