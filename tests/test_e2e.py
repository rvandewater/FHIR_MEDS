"""End-to-end tests: FHIR files -> fhir2meds CLI -> a schema-valid MEDS cohort."""

import json
import subprocess
import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema, DataSchema, SubjectSplitSchema

from tests.conftest import SENTINEL

# The tiny fixtures have a single patient, which cannot populate the default train/tuning/held_out splits.
SINGLE_SPLIT = "etl_options={split_fracs:{train:1.0}}"


def run_fhir2meds(raw_dir: Path, root: Path, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "fhir2meds", f"raw_input_dir={raw_dir}", f"root_output_dir={root}", *extra],
        capture_output=True,
        text=True,
    )


def assert_valid_meds_cohort(cohort: Path) -> pl.DataFrame:
    """Validate every MEDS file in ``cohort`` and return all data rows."""
    shards = sorted((cohort / "data").rglob("*.parquet"))
    assert shards, f"no data shards in {cohort}"
    dfs, seen = [], set()
    for shard in shards:
        table = pq.read_table(shard)
        DataSchema.validate(table)
        df = pl.from_arrow(table)
        subjects = set(df["subject_id"].to_list())
        assert not subjects & seen, f"{shard}: a subject is split across shards"
        seen |= subjects
        assert df.select("subject_id", "time").equals(
            df.select("subject_id", "time").sort("subject_id", "time", nulls_last=False)
        ), f"{shard} is not sorted by (subject_id, time)"
        dfs.append(df.select("subject_id", "time", "code", "numeric_value", "text_value"))
    CodeMetadataSchema.validate(pq.read_table(cohort / "metadata" / "codes.parquet"))
    SubjectSplitSchema.validate(pq.read_table(cohort / "metadata" / "subject_splits.parquet"))
    json.loads((cohort / "metadata" / "dataset.json").read_text())
    return pl.concat(dfs)


@pytest.mark.parametrize("fixture_name", ["mimic_like_dir", "synthea_like_dir"])
def test_cli_end_to_end(fixture_name, request, tmp_path):
    raw_dir: Path = request.getfixturevalue(fixture_name)
    result = run_fhir2meds(
        raw_dir, tmp_path / "out", SINGLE_SPLIT, "dataset_name=TEST", "dataset_version=0.1"
    )
    assert result.returncode == 0, result.stderr[-3000:]
    # Resource content must never be echoed to the console.
    assert SENTINEL not in result.stdout + result.stderr

    cohort = tmp_path / "out" / "MEDS_cohort"
    data = assert_valid_meds_cohort(cohort)
    codes = set(data["code"].to_list())
    assert "MEDS_BIRTH" in codes
    assert any(c.startswith("Encounter//") for c in codes)
    assert any(c.startswith("Encounter_END//") for c in codes)
    metadata = json.loads((cohort / "metadata" / "dataset.json").read_text())
    assert metadata["dataset_name"] == "TEST"
    assert metadata["etl_name"] == "fhir2meds"
    # The effective MESSY spec is saved for provenance.
    assert (tmp_path / "out" / "pre_MEDS" / "messy.yaml").exists()


def test_refuses_to_overwrite_without_flag(synthea_like_dir, tmp_path):
    assert run_fhir2meds(synthea_like_dir, tmp_path / "out", SINGLE_SPLIT).returncode == 0
    second = run_fhir2meds(synthea_like_dir, tmp_path / "out", SINGLE_SPLIT)
    assert second.returncode != 0
    assert "do_overwrite" in second.stderr
    assert (
        run_fhir2meds(synthea_like_dir, tmp_path / "out", SINGLE_SPLIT, "do_overwrite=true").returncode == 0
    )
