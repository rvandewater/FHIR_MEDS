"""End-to-end tests: FHIR files -> `meds-extract-run spec=FHIR` -> a schema-valid MEDS cohort."""

import json
import os
import subprocess
import sysconfig
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema, DataSchema, SubjectSplitSchema

from fhir2meds import MESSY_CFG, __version__
from fhir2meds.stages import PRE_MEDS_STAGE
from tests.conftest import SENTINEL

SCRIPTS_DIR = Path(sysconfig.get_path("scripts"))


def run_fhir(
    raw_dir: Path | None, out: Path, *extra: str, spec: str = "FHIR", env: dict | None = None
) -> subprocess.CompletedProcess:
    """`meds-extract-run spec=SPEC output_dir=OUT do_download=false input_dir=RAW_DIR EXTRA...`."""
    command = [str(SCRIPTS_DIR / "meds-extract-run"), f"spec={spec}", f"output_dir={out}"]
    if raw_dir is not None:
        command += ["do_download=false", f"input_dir={raw_dir}"]
    # MEDS-Transforms starts every stage as `MEDS_transform-stage` from PATH.
    path = os.pathsep.join([str(SCRIPTS_DIR), os.environ.get("PATH", "")])
    return subprocess.run(
        [*command, *extra], capture_output=True, text=True, env={**os.environ, "PATH": path, **(env or {})}
    )


def single_split_spec(directory: Path) -> str:
    """The `FHIR` spec with every subject in the train split, for the one-patient fixtures, which cannot
    populate the default train/tuning/held_out splits (`split_fracs` is a mapping, which `overrides=` merges
    rather than replaces)."""
    text = MESSY_CFG.read_text()
    assert "\netl:\n" in text
    spec = directory / "messy_single_split.yaml"
    spec.write_text(text.replace("\netl:\n", "\netl:\n  split_fracs: {train: 1.0}\n", 1))
    return str(spec)


def pipeline_log(out: Path) -> str:
    log = out / ".logs" / "pipeline.log"
    return log.read_text() if log.is_file() else ""


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
    out = tmp_path / "out"
    result = run_fhir(
        raw_dir,
        out,
        "dataset_version=0.1",
        "overrides=['etl_metadata.dataset_name=TEST']",
        spec=single_split_spec(tmp_path),
    )
    assert result.returncode == 0, result.stderr[-3000:]
    # Resource content must never be echoed to the console or the pipeline log.
    assert SENTINEL not in result.stdout + result.stderr + pipeline_log(out)

    data = assert_valid_meds_cohort(out)
    codes = set(data["code"].to_list())
    assert "MEDS_BIRTH" in codes
    assert any(c.startswith("Encounter//") for c in codes)
    assert any(c.startswith("Encounter_END//") for c in codes)
    metadata = json.loads((out / "metadata" / "dataset.json").read_text())
    assert metadata["dataset_name"] == "TEST"
    assert metadata["dataset_version"] == "0.1"
    assert metadata["code_modifier_columns"] == ["unit"]
    # The flattened tables stay beside the cohort; the synthesized pipeline config is the run's provenance.
    assert (out / PRE_MEDS_STAGE / "Patient.parquet").exists()
    assert (out / ".meds_extract_run" / "pipeline.yaml").exists()


def test_resource_types_the_input_lacks_are_written_without_rows(synthea_like_dir, tmp_path):
    """The pre-stage writes every table the spec reads, so one spec fits every FHIR export."""
    out = tmp_path / "out"
    result = run_fhir(synthea_like_dir, out, "skip_stages=[sumeds_enrich]", spec=single_split_spec(tmp_path))
    assert result.returncode == 0, result.stderr[-3000:]
    # Resource types the input lacks are written without rows.
    assert pl.read_parquet(out / PRE_MEDS_STAGE / "CarePlan.parquet").height == 0
    metadata = json.loads((out / "metadata" / "dataset.json").read_text())
    assert metadata["dataset_name"] == "FHIR"


def test_a_second_run_resumes_instead_of_redoing(synthea_like_dir, tmp_path):
    """The same command on a finished cohort exits at once (MEDS-Extract's done markers): to convert again,
    use a new output_dir."""
    out, spec = tmp_path / "out", single_split_spec(tmp_path)
    args = (synthea_like_dir, out, "skip_stages=[sumeds_enrich]")
    assert run_fhir(*args, spec=spec).returncode == 0
    shards = {p: p.stat().st_mtime_ns for p in (out / "data").rglob("*.parquet")}
    assert shards and (out / ".logs" / "_all_stages.done").exists()

    assert run_fhir(*args, spec=spec).returncode == 0
    assert {p: p.stat().st_mtime_ns for p in (out / "data").rglob("*.parquet")} == shards


def test_registered_spec_stamps_its_name_and_the_package_version(plain_cohort):
    metadata = json.loads((plain_cohort / "metadata" / "dataset.json").read_text())
    assert metadata["dataset_name"] == "FHIR"
    # `<raw version>:<package version>`; a FHIR export of your own has no known version (dataset_version=).
    assert metadata["dataset_version"] == f"unknown:{__version__}"
