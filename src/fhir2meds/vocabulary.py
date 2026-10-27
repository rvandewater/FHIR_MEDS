"""
vocabulary.py
-------------
Short vocabulary names for FHIR code system URIs, used as the middle component of MEDS codes.
"""

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
        >>> extract_vocab("http://hl7.org/fhir/sid/icd-10-cm"), extract_vocab("http://hl7.org/fhir/sid/icd-9-cm")
        ('ICD10CM', 'ICD9CM')
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
