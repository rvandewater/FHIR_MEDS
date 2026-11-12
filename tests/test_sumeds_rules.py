"""The packaged suMEDS rules load in suMEDS and match the codes the converter actually writes.

suMEDS validates the rules file itself (``sumeds.load_rules``) and resolves codes with the rule objects its
enrichment uses, so these tests only state what fhir2meds' codes must resolve to. Nothing is read over the
network.
"""

from importlib.resources import files
from pathlib import Path

import polars as pl
from sumeds import load_rules, registered_rules

from fhir2meds.vocabulary import extract_vocab

RULES_FP = Path(__file__).parents[1] / "src" / "fhir2meds" / "sumeds_rules.yaml"

# The rule that codes of each vocabulary in the mixed fixture run (MIMIC-like fixture plus the Synthea sample)
# resolve through. Codes of every other vocabulary (MIMIC's d-items, lab items, medication names, ...) and
# codes without one (MEDS_BIRTH, GENDER//...) must match no rule.
RULE_BY_VOCAB = {
    "ICD10CM": "fhir-icd10cm",
    "ICD9CM": "fhir-icd9cm",
    "LOINC": "fhir-loinc",
    "SNOMED": "fhir-snomed",
    "RXNORM": "fhir-rxnorm",
    "CVX": "fhir-cvx",
}
# MIMIC writes ICD codes without dots; the rules dot them, as the packs spell them.
EXPECTED_ICD = {
    "Condition//ICD10CM//N179": ("fhir-icd10cm", "N17.9"),
    "Condition//ICD10CM//I10": ("fhir-icd10cm", "I10"),
    "Condition//ICD9CM//5849": ("fhir-icd9cm", "584.9"),
}


def _code(resource_type: str, system: str, code: str) -> str:
    """A code as messy.yaml builds it from a FHIR coding."""
    return f"{resource_type}//{extract_vocab(system)}//{code}"


MIMIC = "http://fhir.mimic.mit.edu/CodeSystem"
# Code layouts of the MIMIC-IV FHIR demo and of Synthea that the small fixtures do not produce.
EXPECTED_LAYOUTS = {
    _code("Condition", f"{MIMIC}/diagnosis-icd10", "A0472"): ("fhir-condition-icd10", "A04.72"),
    _code("Condition", f"{MIMIC}/mimic-diagnosis-icd10", "I10"): ("fhir-condition-icd10", "I10"),
    _code("Condition", f"{MIMIC}/diagnosis-icd9", "V4581"): ("fhir-condition-icd9", "V45.81"),
    _code("Condition", f"{MIMIC}/diagnosis-icd9", "E8490"): ("fhir-condition-icd9", "E849.0"),
    _code("Condition", f"{MIMIC}/diagnosis-icd9", "4019"): ("fhir-condition-icd9", "401.9"),
    _code("Procedure", f"{MIMIC}/procedure-icd10", "0DTJ4ZZ"): ("fhir-procedure-icd10", "0DTJ4ZZ"),
    _code("Procedure", f"{MIMIC}/procedure-icd9", "3961"): ("fhir-procedure-icd9", "39.61"),
    _code("MedicationRequest", f"{MIMIC}/medication-ndc", "00002324033"): ("fhir-ndc", "00002324033"),
    _code("Encounter", f"{MIMIC}/hcpcs-cd", "99219"): ("fhir-cpt4", "99219"),
    _code("Encounter", f"{MIMIC}/hcpcs-cd", "G0378"): ("fhir-hcpcs", "G0378"),
    _code("Condition", "http://hl7.org/fhir/sid/icd-10-cm", "I50.23"): ("fhir-icd10cm", "I50.23"),
    _code("Condition", "http://hl7.org/fhir/sid/icd-9-cm", "428.0"): ("fhir-icd9cm", "428.0"),
    _code("Procedure", "http://www.cms.gov/medicare/coding/icd10", "0DTJ4ZZ"): ("fhir-icd10pcs", "0DTJ4ZZ"),
    _code("Procedure", "http://www.ama-assn.org/go/cpt", "99213"): ("fhir-cpt4", "99213"),
    _code("MedicationDispense", "http://hl7.org/fhir/sid/ndc", "00002324033"): ("fhir-ndc", "00002324033"),
    _code("MedicationRequest", "http://www.nlm.nih.gov/research/umls/rxnorm", "314076"): (
        "fhir-rxnorm",
        "314076",
    ),
    _code("DiagnosticReport", "http://loinc.org", "51990-0"): ("fhir-loinc", "51990-0"),
}
# Codes that look close but are not vocabulary codes.
UNMATCHED = [
    "Observation//LOINC//LP14355-9",  # a LOINC Part, not a term
    "Observation//ICD10//A047",  # ICD-10 outside a Condition or Procedure
    "Procedure//ICD10//A047",  # not an ICD-10-PCS code
    "MedicationRequest//MEDICATION-NDC//0002-3240-33",  # NDC with hyphens is not the 11-digit form
    "GENDER//female",
    "MEDS_BIRTH",
]


def resolve(code: str) -> list[tuple[str, str]]:
    return [
        (rule.id, value)
        for rule in load_rules(RULES_FP).ontology
        if (value := rule.resolve(code)) is not None
    ]


def test_rules_file_is_packaged():
    assert files("fhir2meds").joinpath("sumeds_rules.yaml").is_file()


def test_rules_are_registered_as_fhir():
    """fhir2meds and `--rules FHIR` find the rules by this entry point."""
    assert "FHIR" in registered_rules()
    assert registered_rules()["FHIR"] == "fhir2meds:sumeds_rules.yaml"
    assert load_rules("FHIR") == load_rules(RULES_FP)


def test_rules_load_in_sumeds():
    """The rules parse in suMEDS (which rejects malformed rules) and every pattern spans a whole code."""
    rules = load_rules(RULES_FP)
    for rule in (*rules.vocabulary_rules, *rules.hierarchy):
        if rule.pattern is not None:
            assert rule.pattern.pattern.startswith("^"), rule.id
            assert rule.pattern.pattern.endswith("$"), rule.id


def test_ontology_rules_match_the_converter_codes(plain_cohort: Path):
    codes = pl.read_parquet(plain_cohort / "metadata" / "codes.parquet")["code"].to_list()
    resolved = {}
    for code in codes:
        parts = code.split("//")
        expected_rule = RULE_BY_VOCAB.get(parts[1]) if len(parts) == 3 else None
        hits = resolve(code)
        if expected_rule is None:
            assert hits == [], code
            continue
        assert [rule_id for rule_id, _ in hits] == [expected_rule], code
        resolved[code] = hits[0]
    assert {code.split("//")[1] for code in resolved} == set(RULE_BY_VOCAB)
    assert {code: hit for code, hit in resolved.items() if "//ICD" in code} == EXPECTED_ICD
    # Other vocabulary codes are used as written.
    assert all(
        value == code.split("//")[2] for code, (_, value) in resolved.items() if code not in EXPECTED_ICD
    )


def test_ontology_rules_match_mimic_and_synthea_layouts():
    for code, expected in EXPECTED_LAYOUTS.items():
        assert resolve(code) == [expected], code
    for code in UNMATCHED:
        assert resolve(code) == [], code


def test_every_rule_is_exercised():
    rules = load_rules(RULES_FP).ontology
    used = {*RULE_BY_VOCAB.values(), *(rule_id for rule_id, _ in EXPECTED_LAYOUTS.values())}
    assert used == {rule.id for rule in rules}
