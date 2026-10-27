"""Tiny, hand-written FHIR fixtures shaped like the MIMIC-IV FHIR demo and like Synthea output."""

import gzip
import json
from pathlib import Path

import pytest

# A value that only appears inside resource content; it must never show up in logs or stdout.
SENTINEL = "SENTINEL_PHI_DO_NOT_LOG"

MIMIC_PATIENT_UUID = "0a8eebfd-a352-522e-89f0-1d4a13abdebc"
MIMIC_SUBJECT_ID = 10000032


def _write_ndjson(path: Path, resources: list[dict]) -> None:
    text = "".join(json.dumps(r) + "\n" for r in resources)
    if path.name.endswith(".gz"):
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)


def mimic_like_resources() -> dict[str, list[dict]]:
    """Resources keyed by file name, mirroring the MIMIC-IV FHIR demo 2.1 layout (``Mimic*.ndjson.gz``)."""
    subject = {"reference": f"Patient/{MIMIC_PATIENT_UUID}"}
    specimen = {
        "resourceType": "Specimen",
        "id": "spec-1",
        "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/specimen-lab", "value": "9001"}],
        "type": {"coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/spec-type", "code": "BLOOD"}]},
        "subject": subject,
        "collection": {"collectedDateTime": "2180-05-06T22:23:00-04:00"},
    }
    return {
        "MimicPatient.ndjson.gz": [
            {
                "resourceType": "Patient",
                "id": MIMIC_PATIENT_UUID,
                "identifier": [
                    {"system": "http://mimic.mit.edu/fhir/mimic/identifier/patient", "value": str(MIMIC_SUBJECT_ID)}
                ],
                "name": [{"family": SENTINEL}],
                "gender": "female",
                "birthDate": "2128-05-06",
            }
        ],
        "MimicMedication.ndjson.gz": [
            {
                "resourceType": "Medication",
                "id": "med-1",
                "code": {
                    "coding": [
                        {
                            "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/medication-name",
                            "code": "Furosemide",
                        }
                    ]
                },
            }
        ],
        "MimicMedicationRequest.ndjson.gz": [
            {
                "resourceType": "MedicationRequest",
                "id": "mr-1",
                "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/medication-request", "value": "1"}],
                "medicationReference": {"reference": "Medication/med-1"},
                "subject": subject,
                "authoredOn": "2180-05-07T10:00:00-04:00",
                "note": [{"text": SENTINEL}],
            }
        ],
        "MimicMedicationAdministrationICU.ndjson.gz": [
            {
                "resourceType": "MedicationAdministration",
                "id": "ma-1",
                "medicationCodeableConcept": {
                    "coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-items", "code": "225158"}]
                },
                "subject": subject,
                "effectivePeriod": {"start": "2180-05-07T11:00:00-04:00", "end": "2180-05-07T12:00:00-04:00"},
            }
        ],
        "MimicObservationLabevents.ndjson.gz": [
            {
                "resourceType": "Observation",
                "id": "obs-1",
                "code": {"coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-labitems", "code": "50912"}]},
                "subject": subject,
                "effectiveDateTime": "2180-05-06T22:23:00-04:00",
                "valueQuantity": {"value": 1.2, "unit": "mg/dL"},
            },
            {
                "resourceType": "Observation",
                "id": "obs-2",
                "code": {"coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-labitems", "code": "51006"}]},
                "subject": subject,
                "effectiveDateTime": "2180-05-06T22:23:00-04:00",
                "valueString": "HEMOLYZED",
            },
        ],
        "MimicProcedureICU.ndjson.gz": [
            {
                "resourceType": "Procedure",
                "id": "proc-1",
                "code": {"coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-items", "code": "225792"}]},
                "subject": subject,
                "performedPeriod": {"start": "2180-05-07T09:00:00-04:00", "end": "2180-05-07T09:30:00-04:00"},
            }
        ],
        "MimicCondition.ndjson.gz": [
            {
                "resourceType": "Condition",
                "id": "cond-1",
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": "N179"}]},
                "subject": subject,
                "recordedDate": "2180-05-08",
            },
            {
                "resourceType": "Condition",
                "id": "cond-2",
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-9-cm", "code": "5849"}]},
                "subject": subject,
                "recordedDate": "2180-05-08",
            },
        ],
        # The MIMIC-IV FHIR demo ships identical Specimen and SpecimenLab files.
        "MimicSpecimen.ndjson.gz": [specimen],
        "MimicSpecimenLab.ndjson.gz": [specimen],
    }


SYNTHEA_PATIENT_UUID = "b426b062-8273-4b93-a907-de3176c0567d"


def synthea_like_bundle() -> dict:
    """A Synthea-style transaction Bundle: ``urn:uuid:`` references, no MIMIC-style integer identifier."""
    patient_ref = {"reference": f"urn:uuid:{SYNTHEA_PATIENT_UUID}"}
    encounter_ref = {"reference": "urn:uuid:enc-1"}
    resources = [
        {
            "resourceType": "Patient",
            "id": SYNTHEA_PATIENT_UUID,
            "identifier": [
                {"system": "https://github.com/synthetichealth/synthea", "value": SYNTHEA_PATIENT_UUID},
                {"system": "http://hospital.smarthealthit.org", "value": SENTINEL},
            ],
            "name": [{"family": SENTINEL}],
            "gender": "male",
            "birthDate": "1971-03",
        },
        {
            "resourceType": "Encounter",
            "id": "enc-1",
            "type": [{"coding": [{"system": "http://snomed.info/sct", "code": "162673000"}]}],
            "subject": patient_ref,
            "period": {"start": "2020-02-03T09:00:00-05:00", "end": "2020-02-03T09:30:00-05:00"},
        },
        {
            "resourceType": "Observation",
            "id": "obs-bmi",
            "code": {"coding": [{"system": "http://loinc.org", "code": "39156-5", "display": "Body mass index"}]},
            "subject": patient_ref,
            "encounter": encounter_ref,
            "effectiveDateTime": "2020-02-03T09:05:00-05:00",
            "valueQuantity": {"value": 27.4, "unit": "kg/m2"},
        },
        {
            "resourceType": "Procedure",
            "id": "proc-1",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "430193006"}]},
            "subject": patient_ref,
            "performedPeriod": {"start": "2020-02-03T09:10:00-05:00", "end": "2020-02-03T09:20:00-05:00"},
        },
        {
            "resourceType": "Immunization",
            "id": "imm-1",
            "vaccineCode": {"coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "140"}]},
            "patient": patient_ref,
            "occurrenceDateTime": "2020-02-03T09:15:00-05:00",
        },
        {
            "resourceType": "Condition",
            "id": "cond-1",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "444814009"}]},
            "subject": patient_ref,
            "onsetDateTime": "2020-02-03T09:00:00-05:00",
        },
    ]
    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [{"fullUrl": f"urn:uuid:{r['id']}", "resource": r} for r in resources],
    }


@pytest.fixture
def mimic_like_dir(tmp_path: Path) -> Path:
    root = tmp_path / "mimic_like"
    root.mkdir()
    for fname, resources in mimic_like_resources().items():
        _write_ndjson(root / fname, resources)
    return root


@pytest.fixture
def synthea_like_dir(tmp_path: Path) -> Path:
    root = tmp_path / "synthea_like"
    root.mkdir()
    # Synthea writes one pretty-printed (multi-line) Bundle per patient.
    (root / f"Jane_Doe_{SYNTHEA_PATIENT_UUID}.json").write_text(json.dumps(synthea_like_bundle(), indent=2))
    return root
