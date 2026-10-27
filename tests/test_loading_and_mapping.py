"""Regression tests for FHIR loading, patient-id resolution and event mapping."""

import gzip
import json
import logging
from collections import Counter

from fhir2meds.event_conversion import (
    build_event,
    build_patient_id_map,
    extract_vocab,
    resolve_medication_references,
    stable_subject_id,
)
from fhir2meds.fhir_parser import (
    filter_subject_resources_by_type,
    iter_fhir_files,
    iter_resources,
    load_event_config,
    load_fhir_resources_by_type,
)
from tests.conftest import MIMIC_PATIENT_UUID, MIMIC_SUBJECT_ID, SENTINEL, SYNTHEA_PATIENT_UUID

R4 = load_event_config(fhir_version="R4")


def _event(resource, uuid_to_int):
    return build_event(resource, R4.get(resource["resourceType"], R4["default"]), uuid_to_int, R4["default"])


def test_reads_gzipped_ndjson_and_dedupes_resources(mimic_like_dir):
    resources = load_fhir_resources_by_type(str(mimic_like_dir), R4)
    assert len(resources["Patient"]) == 1
    # MimicSpecimen and MimicSpecimenLab contain the same resource; it must only be loaded once.
    assert len(resources["Specimen"]) == 1
    assert len(resources["Observation"]) == 2


def test_reads_pretty_printed_bundles(synthea_like_dir):
    resources = load_fhir_resources_by_type(str(synthea_like_dir), R4)
    assert {k: len(v) for k, v in resources.items()} == {
        "Patient": 1,
        "Encounter": 1,
        "Observation": 1,
        "Procedure": 1,
        "Immunization": 1,
        "Condition": 1,
    }
    # urn:uuid: references must count as subject-associated.
    filtered = filter_subject_resources_by_type(resources)
    assert sum(len(v) for v in filtered.values()) == 6


def test_file_iteration_is_sorted_and_filters_suffixes(tmp_path):
    for name in ["b.ndjson", "a.json.gz", "c.txt", "sub/d.ndjson.gz"]:
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).touch()
    assert [p.relative_to(tmp_path).as_posix() for p in iter_fhir_files(tmp_path)] == [
        "a.json.gz",
        "b.ndjson",
        "sub/d.ndjson.gz",
    ]


def test_unparseable_lines_are_counted_not_logged(tmp_path, caplog):
    fpath = tmp_path / "Observation.ndjson.gz"
    with gzip.open(fpath, "wt") as f:
        f.write(json.dumps({"resourceType": "Observation", "id": "1"}) + "\n")
        f.write(f'{{"broken": "{SENTINEL}"\n')
    stats = Counter()
    with caplog.at_level(logging.DEBUG):
        assert [r["id"] for r in iter_resources(fpath, stats)] == ["1"]
    assert stats["unparseable_lines"] == 1
    assert SENTINEL not in caplog.text


def test_patient_id_map_mimic_and_synthea(mimic_like_dir, synthea_like_dir):
    mimic = load_fhir_resources_by_type(str(mimic_like_dir), R4)["Patient"]
    synthea = load_fhir_resources_by_type(str(synthea_like_dir), R4)["Patient"]
    assert build_patient_id_map(mimic) == {MIMIC_PATIENT_UUID: MIMIC_SUBJECT_ID}
    # Synthea has no integer patient identifier: fall back to a stable hash rather than dropping every event.
    assert build_patient_id_map(synthea) == {SYNTHEA_PATIENT_UUID: stable_subject_id(SYNTHEA_PATIENT_UUID)}


def test_icd_versions_do_not_collide():
    assert extract_vocab("http://hl7.org/fhir/sid/icd-10-cm") != extract_vocab("http://hl7.org/fhir/sid/icd-9-cm")


def test_mimic_like_events(mimic_like_dir):
    resources = filter_subject_resources_by_type(load_fhir_resources_by_type(str(mimic_like_dir), R4))
    uuid_to_int = build_patient_id_map(resources["Patient"])
    medications = load_fhir_resources_by_type(str(mimic_like_dir), R4)["Medication"]
    assert resolve_medication_references(resources["MedicationRequest"], medications) == 1

    events = {r["id"]: _event(r, uuid_to_int) for rs in resources.values() for r in rs}
    assert all(e["subject_id"] == MIMIC_SUBJECT_ID for e in events.values())
    # Medication codes come from the medication, not from the per-row request identifier.
    assert events["mr-1"]["code"] == "MedicationRequest//MEDICATION-NAME//Furosemide"
    assert events["ma-1"]["code"] == "MedicationAdministration//D-ITEMS//225158"
    # Times that previously resolved to null.
    assert events["ma-1"]["time"] == "2180-05-07T11:00:00-04:00"
    assert events["proc-1"]["time"] == "2180-05-07T09:00:00-04:00"
    assert events["spec-1"]["time"] == "2180-05-06T22:23:00-04:00"
    assert events["spec-1"]["code"] == "Specimen//SPEC-TYPE//BLOOD"
    assert events["cond-1"]["code"] == "Condition//ICD10CM//N179"
    assert events["cond-2"]["code"] == "Condition//ICD9CM//5849"
    assert events["cond-1"]["time"] == "2180-05-08"
    assert events["obs-1"]["numeric_value"] == 1.2
    assert events["obs-2"]["text_value"] == "HEMOLYZED"


def test_synthea_like_events(synthea_like_dir):
    resources = filter_subject_resources_by_type(load_fhir_resources_by_type(str(synthea_like_dir), R4))
    uuid_to_int = build_patient_id_map(resources["Patient"])
    events = {r["id"]: _event(r, uuid_to_int) for rs in resources.values() for r in rs}
    assert {e["subject_id"] for e in events.values()} == {stable_subject_id(SYNTHEA_PATIENT_UUID)}
    assert events["proc-1"]["time"] == "2020-02-03T09:10:00-05:00"
    assert events["imm-1"]["code"] == "Immunization//CVX//140"
    assert events["imm-1"]["time"] == "2020-02-03T09:15:00-05:00"
    assert events["obs-bmi"]["description"] == "Body mass index"


def test_build_event_does_not_mutate_configs():
    default = {"code": ["const(X)"], "time": "col(t)", "numeric_value": "col(v)"}
    specific = {"code": ["const(Y)"]}
    build_event({"resourceType": "Patient", "id": "p"}, specific, {"p": 1}, default)
    assert specific == {"code": ["const(Y)"]}
