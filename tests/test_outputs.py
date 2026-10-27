"""Regression tests for the MEDS data/metadata writers and the end-to-end CLI."""

import json
import subprocess
import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema, DataSchema, SubjectSplitSchema

from fhir2meds.meds_writer import write_meds_sharded_parquet
from fhir2meds.metadata_writer import (
    write_codes_metadata,
    write_dataset_metadata,
    write_subject_splits,
)
from tests.conftest import SENTINEL


def _events():
    return [
        {"subject_id": 2, "time": "2020-01-02T00:00:00", "code": "B", "numeric_value": 1.0},
        {"subject_id": 1, "time": "2020-01-03T00:00:00", "code": "A"},
        {"subject_id": 2, "time": "2020-01-01T00:00:00", "code": "A", "text_value": "x"},
        {"subject_id": 1, "time": None, "code": "STATIC"},
        {"subject_id": 3, "time": "2020-01-01", "code": "MEDS_BIRTH"},
    ]


def test_shards_keep_subjects_together_and_are_sorted(tmp_path):
    write_meds_sharded_parquet(_events(), str(tmp_path), shard_size=2)
    shards = sorted((tmp_path / "data").glob("*.parquet"))
    seen_subjects = set()
    for shard in shards:
        table = pq.read_table(shard)
        DataSchema.validate(table)
        df = pl.from_arrow(table)
        subjects = set(df["subject_id"])
        assert not subjects & seen_subjects, "a subject was split across shards"
        seen_subjects |= subjects
        assert df.equals(df.sort("subject_id", "time", nulls_last=False, maintain_order=True))
    assert seen_subjects == {1, 2, 3}
    first = pl.read_parquet(shards[0])
    assert first["code"].to_list()[:2] == ["STATIC", "A"], "static (null-time) events must sort first"


def test_shard_write_failures_are_raised(tmp_path):
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("")
    with pytest.raises(Exception):
        write_meds_sharded_parquet(_events(), str(blocker))


def test_metadata_files_match_meds_schemas(tmp_path):
    write_dataset_metadata(tmp_path, dataset_name="d", etl_name="fhir2meds", location_uri=tmp_path)
    metadata = json.loads((tmp_path / "metadata" / "dataset.json").read_text())
    assert "None" not in metadata.values()
    assert metadata["location_uri"] == str(tmp_path)

    write_codes_metadata(tmp_path, _events(), {"A": "Code A"})
    codes = pq.read_table(tmp_path / "metadata" / "codes.parquet")
    CodeMetadataSchema.validate(codes)
    assert dict(zip(codes["code"].to_pylist(), codes["description"].to_pylist()))["A"] == "Code A"

    write_subject_splits(tmp_path, _events())
    SubjectSplitSchema.validate(pq.read_table(tmp_path / "metadata" / "subject_splits.parquet"))


@pytest.mark.parametrize("fixture_name", ["mimic_like_dir", "synthea_like_dir"])
def test_cli_end_to_end(fixture_name, request, tmp_path):
    raw_dir: Path = request.getfixturevalue(fixture_name)
    out_dir = tmp_path / "out"
    result = subprocess.run(
        [sys.executable, "-m", "fhir2meds", f"raw_input_dir={raw_dir}", f"root_output_dir={out_dir}"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    # Resource content must never be echoed to the console.
    assert SENTINEL not in result.stdout + result.stderr

    data = pl.concat([pl.read_parquet(f) for f in sorted((out_dir / "data").glob("*.parquet"))])
    assert data.height > 0
    assert data["subject_id"].null_count() == 0
    for name, schema in [
        ("codes.parquet", CodeMetadataSchema),
        ("subject_splits.parquet", SubjectSplitSchema),
    ]:
        schema.validate(pq.read_table(out_dir / "metadata" / name))
    assert (out_dir / "subject_id_map.csv").exists()
