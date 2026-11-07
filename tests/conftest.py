"""Tiny, hand-written FHIR fixtures shaped like the MIMIC-IV FHIR demo and like Synthea output, and tiny
suMEDS ontology packs, so that the suMEDS enrichment step that ends every fhir2meds run stays offline."""

import csv
import gzip
import hashlib
import io
import json
import shutil
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
        "identifier": [
            {"system": "http://mimic.mit.edu/fhir/mimic/identifier/specimen-lab", "value": "9001"}
        ],
        "type": {
            "coding": [{"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/spec-type", "code": "BLOOD"}]
        },
        "subject": subject,
        "collection": {"collectedDateTime": "2180-05-06T22:23:00-04:00"},
    }
    return {
        "MimicPatient.ndjson.gz": [
            {
                "resourceType": "Patient",
                "id": MIMIC_PATIENT_UUID,
                "identifier": [
                    {
                        "system": "http://mimic.mit.edu/fhir/mimic/identifier/patient",
                        "value": str(MIMIC_SUBJECT_ID),
                    }
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
                "identifier": [
                    {"system": "http://mimic.mit.edu/fhir/mimic/identifier/medication-request", "value": "1"}
                ],
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
                    "coding": [
                        {"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-items", "code": "225158"}
                    ]
                },
                "subject": subject,
                "effectivePeriod": {"start": "2180-05-07T11:00:00-04:00", "end": "2180-05-07T12:00:00-04:00"},
            }
        ],
        "MimicObservationLabevents.ndjson.gz": [
            {
                "resourceType": "Observation",
                "id": "obs-1",
                "code": {
                    "coding": [
                        {"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-labitems", "code": "50912"}
                    ]
                },
                "subject": subject,
                "effectiveDateTime": "2180-05-06T22:23:00-04:00",
                "valueQuantity": {"value": 1.2, "unit": "mg/dL"},
            },
            {
                "resourceType": "Observation",
                "id": "obs-2",
                "code": {
                    "coding": [
                        {"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-labitems", "code": "51006"}
                    ]
                },
                "subject": subject,
                "effectiveDateTime": "2180-05-06T22:23:00-04:00",
                "valueString": "HEMOLYZED",
            },
        ],
        "MimicProcedureICU.ndjson.gz": [
            {
                "resourceType": "Procedure",
                "id": "proc-1",
                "code": {
                    "coding": [
                        {"system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/d-items", "code": "225792"}
                    ]
                },
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
        "MimicEncounter.ndjson.gz": [
            {
                "resourceType": "Encounter",
                "id": "enc-1",
                "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP"},
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://mimic.mit.edu/fhir/mimic/CodeSystem/admission-type",
                                "code": "URGENT",
                            }
                        ]
                    }
                ],
                "subject": subject,
                "period": {"start": "2180-05-06T22:00:00-04:00", "end": "2180-05-09T14:00:00-04:00"},
            }
        ],
        "MimicConditionNoTime.ndjson.gz": [
            # MIMIC diagnoses have no time of their own; they are dated at the encounter's discharge.
            {
                "resourceType": "Condition",
                "id": "cond-3",
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": "I10"}]},
                "subject": subject,
                "encounter": {"reference": "Encounter/enc-1"},
            }
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
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "39156-5", "display": "Body mass index"}]
            },
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


def write_mimic_like(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for fname, resources in mimic_like_resources().items():
        _write_ndjson(root / fname, resources)
    return root


def write_synthea_like(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    # Synthea writes one pretty-printed (multi-line) Bundle per patient.
    (root / f"Jane_Doe_{SYNTHEA_PATIENT_UUID}.json").write_text(json.dumps(synthea_like_bundle(), indent=2))
    return root


@pytest.fixture
def mimic_like_dir(tmp_path: Path) -> Path:
    return write_mimic_like(tmp_path / "mimic_like")


@pytest.fixture
def synthea_like_dir(tmp_path: Path) -> Path:
    return write_synthea_like(tmp_path / "synthea_like")


# Tiny ontology packs in suMEDS's `sumeds-ontologies/0.1` layout: vocabulary -> (release, {code: parent}),
# with made-up titles. They hold the MIMIC-like ICD codes (and their categories), one LOINC code of the
# Synthea sample, and a code of each other pack vocabulary the rules use, since suMEDS fails when a rule's
# pack is missing from the source. Codes that are not in a pack are left unenriched.
PACKS = {
    "ICD10CM": ("fy2016-fy2027", {"N17": None, "N17.9": "N17", "I10": None}),
    "ICD9CM": ("v32", {"584": None, "584.9": "584"}),
    "ICD10PCS": ("fy2016-fy2027", {"0DTJ4ZZ": None}),
    "ICD9Proc": ("v32", {"39": None, "39.61": "39"}),
    "LOINC": ("2.83", {"39156-5": None}),
}


def pack_title(vocabulary: str, code: str) -> str:
    return f"Synthetic {vocabulary} title {code}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_ontology_packs(root: Path) -> Path:
    """Write ``PACKS`` as a local ontology pack repository (index, manifests, CSV shards) under ``root``."""
    columns = ["code", "description", "parent"]
    vocabularies = {}
    for vocabulary, (release, rows) in PACKS.items():
        directory = root / vocabulary / release
        directory.mkdir(parents=True)
        by_shard: dict[str, list[list[str]]] = {}
        for code, parent in rows.items():
            by_shard.setdefault(code[:1], []).append([code, pack_title(vocabulary, code), parent or ""])
        shards = {}
        for key, shard_rows in by_shard.items():
            buffer = io.StringIO()
            csv.writer(buffer, lineterminator="\n").writerows([columns, *shard_rows])
            data = buffer.getvalue().encode()
            (directory / f"{key}.csv").write_bytes(data)
            shards[key] = {
                "file": f"{key}.csv",
                "rows": len(shard_rows),
                "bytes": len(data),
                "sha256": _sha256(data),
            }
        manifest = {
            "schema": "sumeds-ontology-pack/0.1",
            "vocabulary": vocabulary,
            "release": release,
            "columns": columns,
            "fields": {"code": "code", "description": "description", "parent": "parent"},
            "shard_key_length": 1,
            "rows": len(rows),
            "bytes": sum(s["bytes"] for s in shards.values()),
            "shards": shards,
        }
        manifest_bytes = json.dumps(manifest).encode()
        (directory / "manifest.json").write_bytes(manifest_bytes)
        vocabularies[vocabulary] = {
            "manifest": f"{vocabulary}/{release}/manifest.json",
            "sha256": _sha256(manifest_bytes),
            "release": release,
            "rows": manifest["rows"],
            "bytes": manifest["bytes"],
        }
    (root / "index.json").write_text(
        json.dumps({"schema": "sumeds-ontologies/0.1", "vocabularies": vocabularies})
    )
    return root


@pytest.fixture(scope="session")
def ontology_source(tmp_path_factory) -> Path:
    return write_ontology_packs(tmp_path_factory.mktemp("ontologies"))


@pytest.fixture(scope="session", autouse=True)
def offline_ontology_packs(ontology_source: Path):
    """fhir2meds ends with suMEDS enrichment, which fetches the published packs by default. Point it at the
    tiny local packs instead, for every run the tests start."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("SUMEDS_ONTOLOGY_SOURCE", str(ontology_source))
        yield


@pytest.fixture(scope="session")
def mixed_raw_dir(tmp_path_factory) -> Path:
    """The MIMIC-like fixture and the Synthea sample (fixtures/synthea/bulk) as one input, so that one run
    writes the code layouts of both. (The Synthea-like Bundle would clash: it reuses the MIMIC-like ids.)"""
    root = tmp_path_factory.mktemp("raw")
    write_mimic_like(root / "mimic_like")
    shutil.copytree(Path(__file__).parent / "fixtures" / "synthea" / "bulk", root / "synthea")
    return root


@pytest.fixture(scope="session")
def plain_cohort(tmp_path_factory, mixed_raw_dir: Path) -> Path:
    """The MEDS cohort of ``mixed_raw_dir`` without suMEDS enrichment (``do_enrich=false``)."""
    from tests.test_e2e import SINGLE_SPLIT, run_fhir2meds

    root = tmp_path_factory.mktemp("plain")
    result = run_fhir2meds(mixed_raw_dir, root, SINGLE_SPLIT, "do_enrich=false")
    assert result.returncode == 0, result.stderr[-3000:]
    return root / "MEDS_cohort"
