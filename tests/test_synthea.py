"""End-to-end tests on real Synthea output (see tests/fixtures/synthea/README.md).

The expected events are computed directly from the raw FHIR JSON with the standard library, independently
of the fhir2meds code under test.
"""

import datetime
import gzip
import io
import json
import os
import urllib.request
import zipfile
from pathlib import Path

import polars as pl
import pytest

from fhir2meds.subject_ids import stable_subject_id
from tests.test_e2e import assert_valid_meds_cohort, run_fhir2meds

FIXTURES = Path(__file__).parent / "fixtures" / "synthea"
SYNTHEA_VOCABS = {
    "http://snomed.info/sct": "SNOMED",
    "http://loinc.org": "LOINC",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "RXNORM",
    "http://hl7.org/fhir/sid/cvx": "CVX",
}
# resource type -> (code element, time element)
ORACLE_FIELDS = {
    "Condition": ("code", "onsetDateTime"),
    "Encounter": ("type", "period.start"),
    "Immunization": ("vaccineCode", "occurrenceDateTime"),
    "Procedure": ("code", "performedPeriod.start"),
    "MedicationRequest": ("medicationCodeableConcept", "authoredOn"),
}


def _raw_resources(rtype: str) -> list[dict]:
    path = FIXTURES / "bulk" / f"{rtype}.000.ndjson.gz"
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f if line.strip()]


def _get(obj, dotted):
    for part in dotted.split("."):
        obj = obj.get(part) if isinstance(obj, dict) else None
    return obj


def _utc(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value).astimezone(datetime.UTC).replace(tzinfo=None)


def expected_events(rtype: str) -> set[tuple[int, str, datetime.datetime]]:
    """(subject_id, code, time) for every resource of ``rtype`` whose patient is in the export."""
    patients = {p["id"] for p in _raw_resources("Patient")}
    code_field, time_field = ORACLE_FIELDS[rtype]
    out = set()
    for r in _raw_resources(rtype):
        pid = (r.get("subject") or r.get("patient") or {})["reference"].split("/")[-1]
        if pid not in patients:
            continue
        concept = r[code_field][0] if isinstance(r[code_field], list) else r[code_field]
        coding = concept["coding"][0]
        code = f"{rtype}//{SYNTHEA_VOCABS[coding['system']]}//{coding['code']}"
        out.add((stable_subject_id(pid), code, _utc(_get(r, time_field))))
    return out


@pytest.fixture(scope="module")
def synthea_outputs(tmp_path_factory):
    """Run fhir2meds once on the bulk NDJSON fixture and once on the equivalent per-patient Bundles."""
    outputs = {}
    for layout in ("bulk", "bundles"):
        root = tmp_path_factory.mktemp(layout)
        result = run_fhir2meds(FIXTURES / layout, root, "dataset_name=SYNTHEA_FIXTURE")
        assert result.returncode == 0, result.stderr[-3000:]
        outputs[layout] = assert_valid_meds_cohort(root / "MEDS_cohort")
    return outputs


def test_synthea_subjects_and_patient_events(synthea_outputs):
    data = synthea_outputs["bulk"]
    patients = _raw_resources("Patient")
    assert set(data["subject_id"].unique().to_list()) == {stable_subject_id(p["id"]) for p in patients}
    births = data.filter(pl.col("code") == "MEDS_BIRTH")
    assert births.height == len(patients)
    deaths = data.filter(pl.col("code") == "MEDS_DEATH")
    deceased = [p for p in patients if p.get("deceasedDateTime")]
    assert deaths.height == len(deceased) >= 1
    assert deaths["time"].to_list() == [_utc(p["deceasedDateTime"]) for p in deceased]
    for prefix in ("GENDER//", "RACE//", "ETHNICITY//"):
        assert data.filter(pl.col("code").str.starts_with(prefix)).height == len(patients), prefix


@pytest.mark.parametrize("rtype", sorted(ORACLE_FIELDS))
def test_synthea_events_match_raw_fhir(synthea_outputs, rtype):
    data = synthea_outputs["bulk"]
    got = set(
        data.filter(pl.col("code").str.starts_with(f"{rtype}//"))
        .select("subject_id", "code", "time")
        .iter_rows()
    )
    assert got == expected_events(rtype)


def test_synthea_observations(synthea_outputs):
    data = synthea_outputs["bulk"]
    obs = data.filter(pl.col("code").str.starts_with("Observation//"))
    assert set(obs["code"].str.split("//").list.get(1).unique().to_list()) <= set(SYNTHEA_VOCABS.values())
    # Blood pressure panels are exploded into their systolic/diastolic components, with values.
    for loinc in ("8480-6", "8462-4"):
        bp = obs.filter(pl.col("code") == f"Observation//LOINC//{loinc}")
        assert bp.height > 0 and bp["numeric_value"].null_count() == 0, loinc

    # Spot-check one numeric observation against the raw resource.
    raw = next(r for r in _raw_resources("Observation") if "valueQuantity" in r)
    row = obs.filter(
        (pl.col("subject_id") == stable_subject_id(raw["subject"]["reference"].split("/")[-1]))
        & (pl.col("code") == f"Observation//LOINC//{raw['code']['coding'][0]['code']}")
        & (pl.col("time") == _utc(raw["effectiveDateTime"]))
    )
    assert row["numeric_value"].to_list() == pytest.approx([raw["valueQuantity"]["value"]], rel=1e-6)


def test_bundles_and_bulk_export_give_identical_meds(synthea_outputs):
    cols = ["subject_id", "time", "code", "numeric_value", "text_value"]
    bulk, bundles = (synthea_outputs[k].sort(cols, nulls_last=False) for k in ("bulk", "bundles"))
    assert bulk.equals(bundles)


# Larger public Synthea datasets, pinned to fixed versions. Opt-in because they download tens of MB.
EXTERNAL_DATASETS = {
    "smart-100-bulk-ndjson": (
        "https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/archive/"
        "47b9d0b7f1fb0da5b31a7b674f7e24b5ed03ecee.zip"
    ),
    "synthea-sample-r4-bundles": (
        "https://github.com/synthetichealth/synthea-sample-data/raw/"
        "84f59805d481b9f01f3141ef39e68bdbbeb80132/downloads/latest/synthea_sample_data_fhir_latest.zip"
    ),
}


@pytest.mark.skipif(
    os.environ.get("FHIR_MEDS_RUN_DOWNLOAD_TESTS") != "1",
    reason="Downloads public Synthea datasets; set FHIR_MEDS_RUN_DOWNLOAD_TESTS=1 to run.",
)
@pytest.mark.parametrize("name", sorted(EXTERNAL_DATASETS))
def test_external_synthea_dataset(name, tmp_path):
    raw = tmp_path / "raw"
    with urllib.request.urlopen(EXTERNAL_DATASETS[name]) as response:
        zipfile.ZipFile(io.BytesIO(response.read())).extractall(raw)
    result = run_fhir2meds(raw, tmp_path / "out", f"dataset_name={name}")
    assert result.returncode == 0, result.stderr[-3000:]
    data = assert_valid_meds_cohort(tmp_path / "out" / "MEDS_cohort")
    n_patients = pl.read_parquet(tmp_path / "out" / "pre_MEDS" / "Patient.parquet").height
    assert data["subject_id"].n_unique() == n_patients >= 10
    assert data.filter(pl.col("code") == "MEDS_BIRTH").height == n_patients
