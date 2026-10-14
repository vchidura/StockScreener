"""Evidence identity of the options pipeline: which lineage produced the evidence a gate is about to trust.

The v1 forward lineage gates its evidence on the physical database name `stocks_realtime_v1` plus the
Developer tier. That literal is a v1 fact, not a V2 one: loosening it would admit unvalidated V2 evidence
into the v1 forward code, and renaming a database must never change what a gate accepts. V2 evidence
therefore carries an explicit, separate identity: lineage `advanced_realtime_v2`, which is a logical name,
distinct from any physical database name (a disposable `test_realtime_v2_*` database or the future
`stocks_realtime_v2` produce the same lineage).

Rules the gates built on this module follow:
- The caller chooses the lineage (an `OptionsEvidenceLineage` member, never a string) at the entry point.
- A v1 object has no identity and the exact v1 database literal; a V2 object has the V2 identity and a
  physical `database_name` that is non-blank and is not the v1 literal. Neither is accepted by the other's gate.
- A blank, mistyped or wrongly typed lineage is accepted by neither gate.
- V2 carriers are distinct subclasses of the v1 carriers: v1 classes keep their exact field set, because the
  immutable v1 forward-input encoding serializes every dataclass field and decodes by exact field names.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

V1_FORWARD_DATABASE_NAME = "stocks_realtime_v1"
ADVANCED_REALTIME_V2_LINEAGE = "advanced_realtime_v2"


class OptionsEvidenceLineage(Enum):
    V1_FORWARD = "v1_forward"
    ADVANCED_REALTIME_V2 = ADVANCED_REALTIME_V2_LINEAGE


@dataclass(frozen=True, slots=True)
class OptionsEvidenceIdentity:
    """What a V2 evidence or source object carries. It is validated by `is_v2_options_evidence_identity`,
    not at construction, so a gate (and its tests) can see a blank or mistyped lineage and reject it."""

    lineage: str


V2_OPTIONS_EVIDENCE_IDENTITY = OptionsEvidenceIdentity(ADVANCED_REALTIME_V2_LINEAGE)


def require_options_lineage(lineage: object) -> OptionsEvidenceLineage:
    if type(lineage) is not OptionsEvidenceLineage:
        raise TypeError("options evidence lineage must be an explicit OptionsEvidenceLineage member, not a string")
    return lineage


def is_v2_options_evidence_identity(identity: object) -> bool:
    return (type(identity) is OptionsEvidenceIdentity and type(identity.lineage) is str
            and identity.lineage == ADVANCED_REALTIME_V2_LINEAGE)


def carried_identity(carrier: object) -> object:
    """The identity a carrier holds; v1 carriers have none."""
    return getattr(carrier, "identity", None)


def options_evidence_admitted(lineage: OptionsEvidenceLineage, *, database_name: object, identity: object) -> bool:
    if require_options_lineage(lineage) is OptionsEvidenceLineage.V1_FORWARD:
        return identity is None and database_name == V1_FORWARD_DATABASE_NAME
    return (is_v2_options_evidence_identity(identity) and type(database_name) is str
            and database_name != "" and database_name.strip() == database_name
            and database_name != V1_FORWARD_DATABASE_NAME)


def require_options_evidence(lineage: OptionsEvidenceLineage, *, database_name: object, identity: object,
                             what: str) -> None:
    if not options_evidence_admitted(lineage, database_name=database_name, identity=identity):
        raise ValueError(f"{what} evidence identity is not the requested {lineage.value} lineage; fallback prohibited")


def identity_binding(carrier: object) -> dict[str, object]:
    """Extra fields for a content hash: empty for v1 carriers (their hashes stay byte-identical), the
    identity for V2 carriers, so a V2 input hash can never equal a v1 one."""
    identity = carried_identity(carrier)
    return {} if identity is None else {"evidence_identity": identity}
