"""Byte-identity oracle for the Ticker Workspace projection on the nine-name fixture.

The stored workspace payloads (without the screening-code-dependent `context.cohort.generation` digest), the
manifest identity (everything except the source-code hash, which changes with any edit of the producer), every
persisted evidence row and the inputs of the summary and Pattern Watch producers are pinned by digest. A
performance change to the projection must leave all of them untouched. When an intentional change to the
analysis or provenance identity (not a speed-up) alters them, regenerate with
`python -B backend/tests/realtime/test_ticker_projection_golden.py` from the repository root and review the diff.
"""
import json
import sys
from copy import deepcopy
from pathlib import Path

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.ticker_projection import (  # noqa: E402
    produce_realtime_pattern_watch_publication, produce_realtime_ticker_workspace_summary,
)
from tests.realtime.test_ticker_vertical_slice import ticker_fixture, ticker_projection  # noqa: E402

GOLDEN = {
    "screening": {
        "hourly_lineage": "aab9ac46d1949935175b55add8cd9cf9cefd691bd081a52a4455a45d0c063bc8",
        "input_sha256": "22b4fb44f175e03b2dd1a4b3a3fcc98e60b448e92f47dd9a82d57be9dd5cc4e0",
        "lineage": "f2b3c22ccd879dc85871d376abe26696f3320100360af7a890a08b9834a4ca92",
        "rows": "1951c5b76fb3c0d0c7af186937459dc840081dceef4b07235af97c0edf3591af"
    },
    "pattern_watch": "432b0026d4478a13aa0e741f13117a0bb19a33c86c46cdec3cfb49ee301ba3e4",
    "summary": "fc9ca6ddaa41d4c9830a33db77147398155d046e1d3057fd0515c19d37332000",
    "summary_charts": "b915396d4c175aa1bade66211327b530ecba96362fe89cd445d4b7ec5cb748c7",
    "workspaces": {
        "AAPL": {
            "evidence": "96a6d3b1515786829953f36d1f518aba8a31b1edfd00c4508b8655c4cc83c646",
            "manifest": "2cf04ddda27d807f851d8baa9ae612ce2034d248836a8840d585e164ad858ea7",
            "payload": "3f06d9fb0bf58f7ca89e40db139b90948041bbf69ddbea7fbe1f61fed0523c8e"
        },
        "AMZN": {
            "evidence": "739f430e320c429907c6d88b832cc8f20ef6be9974fba11e20bddc86c187a8be",
            "manifest": "d6f72685d00ebfe107de3d19df0fef51c25e8cd466c80dd89946158033adb00f",
            "payload": "a0c73fba3722692d2f5523c9eaac5ac297c56d94fac449707a06efa89038e710"
        },
        "IWM": {
            "evidence": "79dadd2b670be84379e7f508648e8ba27b5bd902fe93bb5cb32b5f487a9850ee",
            "manifest": "0b1bb5f8960a70a8f1103c9aac8151a5cca3708b0b61484e2c9b358fd2c3686b",
            "payload": "969bac58f58be7c30307c43c2e7ac8574728e01200a837229bfeb0003ca5bc45"
        },
        "MSFT": {
            "evidence": "2e82d59436134841aa79fe38a345db6d00ba2a987e0590f3358c7fd9c71532ff",
            "manifest": "8348fd6bc19b7f6f9f5a67433dbdf5788f671a4d1208735c6145e31cfb7ee808",
            "payload": "d88c12f431fc1cdf9a3ff9d2a30e171737afa24e62b4a3a2a16575c7025b69bb"
        },
        "NFLX": {
            "evidence": "5b3ecc43b9a2c481278aa8d25186307e03035850ebcf85a2e37ae9690a10109e",
            "manifest": "92b5a33fc536895eefa0c91178f0eecc07478c9ec1058576ab3d00e72da4e8d5",
            "payload": "be1007716fc120a02ee713b31c2c6079ddc0b6d37aa9cd8d7b94063a18f82978"
        },
        "NVDA": {
            "evidence": "0657f19426e6fbfcfc338b24429e28043ddf8648d8ce60eee3711c69e06a5884",
            "manifest": "316b8796b997539fadd35cf8e85edab6fa1cb80abf034abe9681aea1da1e2b94",
            "payload": "56acdd74d66ea97a04ef61b271241e4491b0456b11d149f6bd19304495ba5b40"
        },
        "QQQ": {
            "evidence": "75a78eb2d1694abab594481f608911178435cd12f2cce67a9d43dba9e540363a",
            "manifest": "aa5b0c7b6544559d19ed898eee18c1ab3286ae7f99753feff3c3d84c299ef090",
            "payload": "f48011e3f63c2d206f71cacaff7d520c1bd111e475738a5dcacea99b52f1bc1e"
        },
        "SPY": {
            "evidence": "00b44c93490a3a987fa70d100115b517b68950cc1da5f43191c9fc48f22294c9",
            "manifest": "65558d1354cb43af63a239bf38a7d6b78c864588f69e866c9fb33b80a9bd11ee",
            "payload": "1affbc8bdf6b36e9016d1bd3ab75c5fb169a5458226f4ec5070ce9e08e7a3db3"
        },
        "TSLA": {
            "evidence": "fc9bdbe7d5b7de5bdf0b2922f5c4ee284f30802431950f76bd80c991b6157546",
            "manifest": "0b0a1e659b22e376c3fa77e74390233b80f18903fee08de89f9bf60213bc8447",
            "payload": "bc135e3f8d979fe5f42785e1cde500cbfe0fa0e962cf84d1fea4fbe2136d419e"
        }
    }
}


def payload_without_generation(snapshot):
    """The stored payload minus `context.cohort.generation`: that one value is the screening manifest digest,
    which embeds the source hash of the screening modules, so it moves with any edit of them (it is checked
    against the projection's own screening generation by test_ticker_screening_generation)."""
    payload = deepcopy(snapshot.payload)
    generation = payload["context"]["cohort"].pop("generation")
    assert len(generation) == 64
    return canonical_sha256(payload)


def fingerprint(projection, inputs):
    evidence = {}
    for result in (*projection.materializations, *projection.pattern_materializations):
        for row in result.materialization.evidence:
            evidence.setdefault(row.ticker, []).append((str(row.evidence_id), row.payload_sha256))
    summary = produce_realtime_ticker_workspace_summary(inputs, projection.snapshots, projection.summary_charts)
    pattern = produce_realtime_pattern_watch_publication(inputs, projection.snapshots)
    generation = projection.screening_generation
    return dict(
        # The screening generation the workspaces name: its rows and the compact lineage heads and input identity
        # of its manifest (everything of it that does not embed the source hash of a code file).
        screening=dict(
            rows=canonical_sha256([(row.ticker, row.payload_sha256) for row in generation.rows]),
            lineage=canonical_sha256(generation.manifest["lineage"]),
            hourly_lineage=canonical_sha256(generation.manifest["hourly_lineage"]),
            input_sha256=generation.manifest["input_sha256"],
        ),
        workspaces={
            snapshot.snapshot_key: dict(
                payload=payload_without_generation(snapshot),
                manifest=canonical_sha256({
                    key: value for key, value in snapshot.source_manifest.items() if key != "code_sha256"}),
                evidence=canonical_sha256(sorted(evidence[snapshot.snapshot_key])),
            ) for snapshot in projection.snapshots
        },
        summary_charts=canonical_sha256(projection.summary_charts),
        summary=canonical_sha256([summary.payload["overview"], summary.payload["setup"]]),
        pattern_watch=canonical_sha256(pattern.payload["views"]),
    )


def test_projection_output_is_byte_identical_to_the_pinned_digests():
    assert GOLDEN, "no digests pinned: run this file as a script"
    assert fingerprint(ticker_projection(), ticker_fixture()) == GOLDEN


if __name__ == "__main__":
    print(json.dumps(fingerprint(ticker_projection(), ticker_fixture()), indent=4, sort_keys=True))
