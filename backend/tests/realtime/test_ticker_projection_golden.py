"""Byte-identity oracle for the Ticker Workspace projection on the nine-name fixture.

The stored workspace payloads (without the screening-code-dependent `context.cohort.generation` digest), the
manifest identity (everything except the source-code hash, which changes with any edit of the producer), every
persisted evidence row and the inputs of the summary and Pattern Watch producers are pinned by digest. A
performance change to the projection must leave all of them untouched. When an intentional change to the
analysis (not a speed-up) alters them, regenerate with
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
    "pattern_watch": "12ecf98cb5af35be77947ce87c3defcad92a9bdb97950da737eadb3fd9069791",
    "summary": "fc9ca6ddaa41d4c9830a33db77147398155d046e1d3057fd0515c19d37332000",
    "summary_charts": "b915396d4c175aa1bade66211327b530ecba96362fe89cd445d4b7ec5cb748c7",
    "workspaces": {
        "AAPL": {
            "evidence": "e6de568ff0ae4f5f065c435f536da11b5ad9d28611e7174bcfd4a91e92257ec3",
            "manifest": "2cf04ddda27d807f851d8baa9ae612ce2034d248836a8840d585e164ad858ea7",
            "payload": "3816aa10f9dbeb7ae01f2e405d7c352674b0a5558a7296467df98fec2a2e8e93"
        },
        "AMZN": {
            "evidence": "d067de47138224251f647ad32141bed47ba96c35637ff3e9b4120a07a3b52056",
            "manifest": "d6f72685d00ebfe107de3d19df0fef51c25e8cd466c80dd89946158033adb00f",
            "payload": "fb3f6dc5a19a5e172c4222ddfd9448c1f179058ad823d74b1c9134f3acb4e472"
        },
        "IWM": {
            "evidence": "316054b8f8399b639f09732fcfbc4be96f7b96e71d5b31cffcff5df98c8d1c71",
            "manifest": "0b1bb5f8960a70a8f1103c9aac8151a5cca3708b0b61484e2c9b358fd2c3686b",
            "payload": "73766b6daf60b318992dc27b285b82b759b66406ca13f6ccc74727afe8e27633"
        },
        "MSFT": {
            "evidence": "a76a820aa967d0f548365639c00a57e8959c0f9e5cd80e34a4d347def9fcac4e",
            "manifest": "8348fd6bc19b7f6f9f5a67433dbdf5788f671a4d1208735c6145e31cfb7ee808",
            "payload": "ba8989c8fe440bf2b7358fc987e265c5e84ce2791e24ee16b79ad21d6643f61a"
        },
        "NFLX": {
            "evidence": "825c85046c42ad67f7ecb6eeb1dfbedc7c2135265e6d441f320de649614783c5",
            "manifest": "92b5a33fc536895eefa0c91178f0eecc07478c9ec1058576ab3d00e72da4e8d5",
            "payload": "6d858b2e3c41d937599d98649878f3174db113b30a056c49a654d4c121a030d2"
        },
        "NVDA": {
            "evidence": "629dab8551373a749308e04a64b2197b46f4a80b57fc4222bf1b8e6908a663a1",
            "manifest": "316b8796b997539fadd35cf8e85edab6fa1cb80abf034abe9681aea1da1e2b94",
            "payload": "158ef572c1176a6d1e3eb6c304cd46a910fc4717fb1f1c5ff778f7a3f6cbf360"
        },
        "QQQ": {
            "evidence": "ad3c3a2c41639e90d34939cddc578268beba1a95a034bbe775d9367381d5018a",
            "manifest": "aa5b0c7b6544559d19ed898eee18c1ab3286ae7f99753feff3c3d84c299ef090",
            "payload": "f9d343d7a4df02164a87789c144c4a4331c1d8c9355aa83e3e598223f15f4b1b"
        },
        "SPY": {
            "evidence": "40d2ec3dc60aa028351c9faf5fc366949f861167edb3d40fd81fc5af70ea4187",
            "manifest": "65558d1354cb43af63a239bf38a7d6b78c864588f69e866c9fb33b80a9bd11ee",
            "payload": "1779cc404c208b79e2994c0ad7056c9e53ff347ca80e75bea6196c309fbb9ed2"
        },
        "TSLA": {
            "evidence": "e3c87830a279c9dae86b8ae49347ea61c998dcbd3a9ccf6b2bfaa77370b81e8d",
            "manifest": "0b0a1e659b22e376c3fa77e74390233b80f18903fee08de89f9bf60213bc8447",
            "payload": "9b6a5519322508bddc0dfcf8a290caecc94bdcba01298b0b9ac6133ad669a8c7"
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
