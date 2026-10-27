"""
subject_ids.py
--------------
Mapping FHIR Patient logical ids to the integer ``subject_id`` MEDS requires.
"""

import hashlib
from collections.abc import Iterable

# Patient.identifier systems whose (integer) value is used as the MEDS subject_id, e.g. MIMIC-IV's subject_id.
DEFAULT_SUBJECT_ID_SYSTEM_SUFFIXES = ("/identifier/patient",)


def stable_subject_id(patient_id: str) -> int:
    """Deterministically map a FHIR Patient logical id to a non-negative int64 subject_id.

    Uses the first 8 bytes of SHA-256, so the mapping is stable across runs, machines and library versions
    (unlike
    Python's ``hash`` or polars' ``Expr.hash``).

    Examples:
        >>> stable_subject_id("b426b062-8273-4b93-a907-de3176c0567d")
        7338863945208448917
        >>> 0 <= stable_subject_id("anything") < 2**63
        True
    """
    digest = hashlib.sha256(patient_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & (2**63 - 1)


def subject_id_from_identifiers(
    identifiers: Iterable[dict] | None, system_suffixes: Iterable[str] = DEFAULT_SUBJECT_ID_SYSTEM_SUFFIXES
) -> int | None:
    """Return an integer Patient.identifier value from one of ``system_suffixes``, if any.

    Examples:
        >>> mimic = "http://mimic.mit.edu/fhir/mimic/identifier/patient"
        >>> subject_id_from_identifiers([{"system": mimic, "value": "10000032"}])
        10000032
        >>> synthea = "https://github.com/synthetichealth/synthea"
        >>> subject_id_from_identifiers([{"system": synthea, "value": "abc"}]) is None
        True
        >>> subject_id_from_identifiers([{"system": "x/identifier/patient", "value": "not-an-int"}]) is None
        True
    """
    for ident in identifiers or []:
        system = ident.get("system") or ""
        if any(system.endswith(suffix) for suffix in system_suffixes):
            try:
                return int(ident["value"])
            except (KeyError, TypeError, ValueError):
                continue
    return None


def build_subject_id_map(patient_ids: list[str], identifier_subject_ids: list[int | None]) -> dict[str, int]:
    """Map every Patient logical id to an integer MEDS subject_id.

    An integer identifier (e.g. MIMIC-IV's ``subject_id``) is used when present; otherwise the id is derived
    with
    :func:`stable_subject_id` (e.g. Synthea, whose ids are UUIDs). Collisions are an error, never a silent
    merge.

    Examples:
        >>> build_subject_id_map(["a", "b"], [7, None]) == {"a": 7, "b": stable_subject_id("b")}
        True
        >>> build_subject_id_map(["a", "b"], [7, 7])
        Traceback (most recent call last):
            ...
        ValueError: Patient id mapping is not one-to-one; refusing to merge distinct patients.
    """
    mapping = {
        pid: (sid if sid is not None else stable_subject_id(pid))
        for pid, sid in zip(patient_ids, identifier_subject_ids)
    }
    if len(set(mapping.values())) != len(mapping):
        raise ValueError("Patient id mapping is not one-to-one; refusing to merge distinct patients.")
    return mapping
