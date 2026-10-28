"""Tests for FHIR loading and the pre-MEDS flattening stage."""

import datetime
import gzip
import json
import logging
from collections import Counter

import polars as pl

from fhir2meds.fhir_parser import (
    iter_fhir_files,
    iter_resources,
    load_fhir_resources_by_type,
)
from fhir2meds.pre_MEDS import run_pre_MEDS
from fhir2meds.subject_ids import stable_subject_id
from fhir2meds.vocabulary import extract_vocab
from tests.conftest import MIMIC_SUBJECT_ID, SENTINEL, SYNTHEA_PATIENT_UUID


def test_reads_pretty_printed_bundles(synthea_like_dir):
    resources = load_fhir_resources_by_type(str(synthea_like_dir))
    assert {k: len(v) for k, v in resources.items()} == {
        "Patient": 1,
        "Encounter": 1,
        "Observation": 1,
        "Procedure": 1,
        "Immunization": 1,
        "Condition": 1,
    }


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


def test_icd_versions_do_not_collide():
    assert extract_vocab("http://hl7.org/fhir/sid/icd-10-cm") != extract_vocab(
        "http://hl7.org/fhir/sid/icd-9-cm"
    )


def _read(pre_MEDS_dir, table):
    return {r["resource_id"]: r for r in pl.read_parquet(pre_MEDS_dir / f"{table}.parquet").to_dicts()}


def test_mimic_like_pre_MEDS(mimic_like_dir, tmp_path, caplog):
    out = tmp_path / "pre_MEDS"
    with caplog.at_level(logging.DEBUG):
        counts = run_pre_MEDS(mimic_like_dir, out)
    assert SENTINEL not in caplog.text
    # MimicSpecimen and MimicSpecimenLab contain the same resource; it must only appear once.
    assert counts["Specimen"] == 1

    patient = pl.read_parquet(out / "Patient.parquet").to_dicts()[0]
    assert patient["subject_id"] == MIMIC_SUBJECT_ID
    assert patient["birth_date"] == datetime.datetime(2128, 5, 6)

    meds = _read(out, "MedicationRequest")
    # medicationReference is resolved through Medication.code.
    assert (meds["mr-1"]["code_vocab"], meds["mr-1"]["code"]) == ("MEDICATION-NAME", "Furosemide")

    admin = _read(out, "MedicationAdministration")["ma-1"]
    # Offsets are converted to UTC by default (-04:00 -> +4h).
    assert admin["start"] == datetime.datetime(2180, 5, 7, 15, 0)
    assert admin["end"] == datetime.datetime(2180, 5, 7, 16, 0)

    conditions = _read(out, "Condition")
    assert conditions["cond-1"]["code_vocab"] == "ICD10CM"
    assert conditions["cond-2"]["code_vocab"] == "ICD9CM"
    # No time of its own: falls back to the encounter's end (discharge).
    assert conditions["cond-3"]["start"] == datetime.datetime(2180, 5, 9, 18, 0)

    assert _read(out, "Procedure")["proc-1"]["start"] == datetime.datetime(2180, 5, 7, 13, 0)
    assert _read(out, "Specimen")["spec-1"]["start"] == datetime.datetime(2180, 5, 7, 2, 23)
    obs = _read(out, "Observation")
    assert obs["obs-1"]["value_num"] == 1.2 and obs["obs-1"]["value_unit"] == "mg/dL"
    assert obs["obs-2"]["value_text"] == "HEMOLYZED"


def test_keep_local_time(mimic_like_dir, tmp_path):
    run_pre_MEDS(mimic_like_dir, tmp_path / "pre_MEDS", keep_local_time=True)
    admin = _read(tmp_path / "pre_MEDS", "MedicationAdministration")["ma-1"]
    assert admin["start"] == datetime.datetime(2180, 5, 7, 11, 0)


def test_synthea_like_pre_MEDS(synthea_like_dir, tmp_path):
    out = tmp_path / "pre_MEDS"
    run_pre_MEDS(synthea_like_dir, out)
    expected_id = stable_subject_id(SYNTHEA_PATIENT_UUID)
    patient = pl.read_parquet(out / "Patient.parquet").to_dicts()[0]
    # Synthea has no integer patient identifier: fall back to a stable hash rather than dropping every event.
    assert patient["subject_id"] == expected_id
    # Partial birth dates are padded.
    assert patient["birth_date"] == datetime.datetime(1971, 3, 1)
    for table in ("Encounter", "Observation", "Procedure", "Immunization", "Condition"):
        df = pl.read_parquet(out / f"{table}.parquet")
        assert df.height == 1, table
        # urn:uuid: references resolve to the patient.
        assert df["subject_id"].to_list() == [expected_id], table
        assert df["start"].null_count() == 0, table
    assert _read(out, "Immunization")["imm-1"]["code_vocab"] == "CVX"
    assert _read(out, "Observation")["obs-bmi"]["code_display"] == "Body mass index"


def test_resources_for_unknown_patients_are_dropped(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    lines = [
        {"resourceType": "Patient", "id": "p1"},
        {
            "resourceType": "Observation",
            "id": "o1",
            "subject": {"reference": "Patient/p1"},
            "code": {"coding": [{"code": "1"}]},
            "effectiveDateTime": "2020-01-01",
        },
        {
            "resourceType": "Observation",
            "id": "o2",
            "subject": {"reference": "Patient/ghost"},
            "code": {"coding": [{"code": "1"}]},
            "effectiveDateTime": "2020-01-01",
        },
    ]
    (raw / "all.ndjson").write_text("".join(json.dumps(r) + "\n" for r in lines))
    counts = run_pre_MEDS(raw, tmp_path / "pre_MEDS")
    assert counts["Observation"] == 1
