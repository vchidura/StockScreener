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
            "evidence": "156f7013e4cf7f5c1f48172c0470f674c362350a2ac53219acab8a87463c6682",
            "manifest": "2cf04ddda27d807f851d8baa9ae612ce2034d248836a8840d585e164ad858ea7",
            "payload": "574f0ae1162cb31c79dd4d1f42865e920ac0da2b74eb11e6979f44b6cd363229"
        },
        "AMZN": {
            "evidence": "afca336209adf5406ea758100e11e25b8cbbce7cc65f7086aefc976ac7964694",
            "manifest": "d6f72685d00ebfe107de3d19df0fef51c25e8cd466c80dd89946158033adb00f",
            "payload": "1c5400b2158a4818e2b74c1d370073d2d9639da4b1adaae25281d064b0315a4f"
        },
        "IWM": {
            "evidence": "51706b0cd81518dbe097ef88915ac097c75b087835d465856fda7b55d80082ca",
            "manifest": "0b1bb5f8960a70a8f1103c9aac8151a5cca3708b0b61484e2c9b358fd2c3686b",
            "payload": "2e3b869eaa7996d0ccb006e52f93b200cd3db0393fb8c460f8850650b5f2ff87"
        },
        "MSFT": {
            "evidence": "1d109f1756afa5614ad1f8533e4292f811ea3bd9ccb5f63115e623255449ee02",
            "manifest": "8348fd6bc19b7f6f9f5a67433dbdf5788f671a4d1208735c6145e31cfb7ee808",
            "payload": "730b9cca4454ffe7d8c2626797643e7c474ee570fe9ff955c88862ec2cb7d5d4"
        },
        "NFLX": {
            "evidence": "04e4da18594049fcd3cdc9cac69a54081cf72bd1dc291faddb6a0bc57429dc47",
            "manifest": "92b5a33fc536895eefa0c91178f0eecc07478c9ec1058576ab3d00e72da4e8d5",
            "payload": "3492aec941a6b77ec59aed41dd60a5cfba8928ac25648ffcc728399a7b9a4461"
        },
        "NVDA": {
            "evidence": "4a408418b13d51099020bb2c8667793d6fb266a5a3b9481d13c2d0f295fd12d5",
            "manifest": "316b8796b997539fadd35cf8e85edab6fa1cb80abf034abe9681aea1da1e2b94",
            "payload": "399b8988cf2fde3594696c64a7adbc4c1d30ea98672a1475e1f1b1d445904d02"
        },
        "QQQ": {
            "evidence": "ca6d4799a3c33005c430df3cb4567c823b49e2127c8436a9f9d5d9a04ec8068b",
            "manifest": "aa5b0c7b6544559d19ed898eee18c1ab3286ae7f99753feff3c3d84c299ef090",
            "payload": "cd89fee0b803334a0f6ca044fa2d498e402cbdec1bbeca74fc6b7b5abcc8aab8"
        },
        "SPY": {
            "evidence": "6759505e0160a26d0a3707d97fe2814facb8756c0b2ea426f38b0004f47df249",
            "manifest": "65558d1354cb43af63a239bf38a7d6b78c864588f69e866c9fb33b80a9bd11ee",
            "payload": "b2297af7b9b7488866ee226f9f480412d0f2d06e002959d8626cc1987c51c98d"
        },
        "TSLA": {
            "evidence": "ea45053f410447492272b0d91ba8c116f9f624110b20f0cfa5312446f1e82c81",
            "manifest": "0b0a1e659b22e376c3fa77e74390233b80f18903fee08de89f9bf60213bc8447",
            "payload": "3388e18d8268d260db701d3c146a51bb78f1df1afe13d1788406082a830fbdd6"
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
