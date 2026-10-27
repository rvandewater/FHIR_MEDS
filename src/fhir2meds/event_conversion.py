"""
event_conversion.py
-------------------
Config-driven conversion of FHIR resources (as dicts) to MEDS event dicts.
"""

import hashlib
import logging
import re
from collections.abc import Iterable

from .fhir_parser import get_patient_id

logger = logging.getLogger(__name__)

# Patient.identifier systems whose (integer) value is used as the MEDS subject_id, e.g. MIMIC-IV's subject_id.
DEFAULT_SUBJECT_ID_SYSTEM_SUFFIXES = ("/identifier/patient",)


def stable_subject_id(patient_id: str) -> int:
    """Deterministically map a FHIR Patient logical id to a non-negative int64 subject_id.

    Uses the first 8 bytes of SHA-256, so the mapping is stable across runs, machines and library versions
    (unlike Python's ``hash`` or polars' ``Expr.hash``).

    Examples:
        >>> stable_subject_id("b426b062-8273-4b93-a907-de3176c0567d")
        7338863945208448917
        >>> stable_subject_id("x") == stable_subject_id("x")
        True
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


def build_patient_id_map(
    patients: Iterable[dict], system_suffixes: Iterable[str] = DEFAULT_SUBJECT_ID_SYSTEM_SUFFIXES
) -> dict[str, int]:
    """Map every Patient logical id to an integer MEDS subject_id.

    An integer identifier from one of ``system_suffixes`` (MIMIC-IV's ``subject_id``) is used when present;
    otherwise the id is derived deterministically with :func:`stable_subject_id` (e.g. Synthea, whose ids are
    UUIDs).

    Examples:
        >>> build_patient_id_map([
        ...     {"resourceType": "Patient", "id": "a",
        ...      "identifier": [{"system": "x/identifier/patient", "value": "7"}]},
        ...     {"resourceType": "Patient", "id": "b"},
        ... ]) == {"a": 7, "b": stable_subject_id("b")}
        True
    """
    system_suffixes = tuple(system_suffixes)
    uuid_to_int = {}
    for patient in patients:
        pid = patient.get("id")
        if pid is None:
            continue
        subject_id = subject_id_from_identifiers(patient.get("identifier"), system_suffixes)
        uuid_to_int[pid] = subject_id if subject_id is not None else stable_subject_id(pid)
    if len(set(uuid_to_int.values())) != len(uuid_to_int):
        raise ValueError("Patient id mapping is not one-to-one; refusing to merge distinct patients.")
    return uuid_to_int


def resolve_medication_references(resources: list[dict], medications: list[dict]) -> int:
    """Copy ``Medication.code`` onto resources that only reference a Medication.

    MedicationRequest/Administration/Dispense/Statement may carry ``medicationReference`` instead of
    ``medicationCodeableConcept`` (the MIMIC-IV FHIR demo does this for most MedicationRequests). Resolving
    the reference lets those resources share one code config. Resources are updated in place; returns how many
    were resolved.

    Examples:
        >>> meds = [{"resourceType": "Medication", "id": "m1", "code": {"coding": [{"code": "123"}]}}]
        >>> reqs = [{"medicationReference": {"reference": "Medication/m1"}},
        ...         {"medicationReference": {"reference": "Medication/m2"}}]
        >>> resolve_medication_references(reqs, meds)
        1
        >>> reqs[0]["medicationCodeableConcept"]
        {'coding': [{'code': '123'}]}
        >>> "medicationCodeableConcept" in reqs[1]
        False
    """
    codes = {m.get("id"): m.get("code") for m in medications if m.get("code")}
    n_resolved = 0
    for resource in resources:
        if resource.get("medicationCodeableConcept") is not None:
            continue
        ref = (resource.get("medicationReference") or {}).get("reference") or ""
        med_id = ref.split("/")[-1] if ref.startswith("Medication/") else ref.removeprefix("urn:uuid:")
        if med_id in codes:
            resource["medicationCodeableConcept"] = codes[med_id]
            n_resolved += 1
    return n_resolved


def safe_str(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def extract_path(resource, path):
    """
    Resolve a dotted path like 'code.coding[0].code' on a FHIR resource object or dict.

    Examples:
        >>> r = {"code": {"coding": [{"code": "1234-5"}]}, "collection": {"collectedDateTime": "2020"}}
        >>> extract_path(r, "code.coding[0].code")
        '1234-5'
        >>> extract_path(r, "code[coding][0][code]")
        '1234-5'
        >>> extract_path(r, "collection.collectedDateTime")
        '2020'
        >>> extract_path(r, "code.coding[3].code") is None
        True
    """
    parts = re.split(r"\.|\[|\]", path)
    obj = resource
    for part in parts:
        if not part:
            continue
        # Check if part is a numeric index for list access
        if part.isdigit():
            try:
                obj = obj[int(part)]
            except (IndexError, TypeError, KeyError):
                return None
        elif isinstance(obj, dict):
            obj = obj.get(part)
        else:
            obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj


# Exact-match table of well-known code system URIs (https://terminology.hl7.org/external_terminologies.html).
KNOWN_VOCABULARIES = {
    "http://loinc.org": "LOINC",
    "http://snomed.info/sct": "SNOMED",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "RXNORM",
    "http://hl7.org/fhir/sid/icd-10": "ICD10",
    "http://hl7.org/fhir/sid/icd-10-cm": "ICD10CM",
    "http://www.cms.gov/medicare/coding/icd10": "ICD10PCS",
    "http://hl7.org/fhir/sid/icd-9-cm": "ICD9CM",
    "http://hl7.org/fhir/sid/cvx": "CVX",
    "http://hl7.org/fhir/sid/ndc": "NDC",
    "http://www.ama-assn.org/go/cpt": "CPT",
    "urn:oid:2.16.840.1.113883.6.238": "CDCREC",
    "http://unitsofmeasure.org": "UCUM",
}


def extract_vocab(system_url):
    """Return a short, upper-case vocabulary name for a FHIR code system URI.

    Examples:
        >>> [extract_vocab(f"http://hl7.org/fhir/sid/{s}") for s in ("icd-10-cm", "icd-9-cm")]
        ['ICD10CM', 'ICD9CM']
        >>> extract_vocab("http://loinc.org"), extract_vocab("https://loinc.org/")
        ('LOINC', 'LOINC')
        >>> extract_vocab("http://fhir.mimic.mit.edu/CodeSystem/mimic-diagnosis-icd10")
        'ICD10'
        >>> extract_vocab("http://fhir.mimic.mit.edu/CodeSystem/d-items")
        'D-ITEMS'
        >>> extract_vocab(None)
        ''
    """
    if not system_url:
        return ""
    normalized = system_url.rstrip("/").replace("https://", "http://")
    if normalized in KNOWN_VOCABULARIES:
        return KNOWN_VOCABULARIES[normalized]
    lowered = normalized.lower()
    if "loinc" in lowered:
        return "LOINC"
    if "snomed" in lowered:
        return "SNOMED"
    last = normalized.split("/")[-1]
    if "icd" in last.lower():
        # e.g. MIMIC's .../mimic-diagnosis-icd9 -> ICD9; keep the version so ICD-9 and ICD-10 never collide.
        return last.split("-")[-1].upper()
    return last.upper()


def _resolve_subject_id(resource, uuid_to_int):
    patient_id = get_patient_id(resource, known_patient_ids=uuid_to_int or None)
    if patient_id is None:
        return None
    if uuid_to_int:
        return uuid_to_int.get(patient_id)
    return stable_subject_id(patient_id)


def build_event(resource, config, uuid_to_int=None, default_config=None):
    """Build one MEDS event dict from a FHIR resource dict and its event config.

    ``default_config`` fills keys missing from ``config``; neither is modified. ``code`` is None when any
    ``col()`` component of the code is missing, so callers can drop (and count) such events instead of
    emitting partial codes like ``Condition//SNOMED//``.

    Examples:
        >>> cfg = {"code": ["const(resourceType)", "const(//)", "vocab(code.coding[0].system)", "const(//)",
        ...                 "col(code.coding[0].code)"], "time": ["col(effectiveDateTime)", "col(issued)"]}
        >>> obs = {"resourceType": "Observation", "subject": {"reference": "Patient/p"},
        ...        "issued": "2020-01-01",
        ...        "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]}}
        >>> build_event(obs, cfg, {"p": 1})
        {'code': 'Observation//LOINC//8867-4', 'time': '2020-01-01', 'subject_id': 1}
        >>> build_event({**obs, "code": {}}, cfg, {"p": 1})["code"] is None
        True
    """
    event = {}
    config = {**(default_config or {}), **config}
    rtype = (
        resource.get("resourceType")
        if isinstance(resource, dict)
        else getattr(resource, "resource_type", None)
    )
    for key, exprs in config.items():
        if key == "subject_id":
            continue
        if key == "code" and isinstance(exprs, list):
            parts = []
            for expr in exprs:
                if expr.startswith("const("):
                    val = expr[6:-1]
                    parts.append(rtype if val == "resourceType" else val)
                elif expr.startswith("col("):
                    val = extract_path(resource, expr[4:-1])
                    if val in (None, ""):
                        parts = None
                        break
                    parts.append(str(val))
                elif expr.startswith("vocab("):
                    parts.append(extract_vocab(extract_path(resource, expr[6:-1])) or "UNKNOWN")
            event[key] = "".join(parts) if parts is not None else None
        elif isinstance(exprs, list):
            for expr in exprs:
                if expr.startswith("col("):
                    val = extract_path(resource, expr[4:-1])
                    if val is not None:
                        event[key] = val
                        break
        elif isinstance(exprs, str) and exprs.startswith("col("):
            event[key] = extract_path(resource, exprs[4:-1])
        else:
            event[key] = exprs
    event["subject_id"] = _resolve_subject_id(resource, uuid_to_int)
    return event
