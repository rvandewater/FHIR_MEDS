"""`meds-extract-run spec=FHIR` ends with suMEDS enrichment of `metadata/codes.parquet`, offline.

The enrichment uses the rules this package registers as `FHIR` in suMEDS's `sumeds.rules` entry-point group.
The ontology source is the tiny pack repository of ``conftest.py`` (made-up titles), so the test reads nothing
over the network and does not depend on the published packs.

`metadata/dataset.json` records the enrichment. A failed enrichment leaves the plain output, records the
failure and does not fail the run unless the stage's `strict` option is set; `suMEDS-enrich OUTPUT_DIR` then
enriches it as the run would have. `skip_stages=[sumeds_enrich]` leaves it out (the plain cohort).
"""

import json
import shutil
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema, DatasetMetadataSchema
from sumeds.cli import enrich_main

from tests.conftest import pack_title
from tests.test_e2e import pipeline_log, run_fhir


def stage_options(**options) -> str:
    """`overrides=` setting options of the `sumeds_enrich` stage for one run."""
    return "overrides=[" + ",".join(f"'++stage_cfg.{k}={v}'" for k, v in options.items()) + "]"


@pytest.fixture(scope="module")
def enriched_cohort(tmp_path_factory, mixed_raw_dir: Path, ontology_source: Path) -> Path:
    out = tmp_path_factory.mktemp("enriched") / "MEDS_cohort"
    # The default run, with a stage option for one run. The environment points suMEDS at a missing pack
    # source, so the run only enriches (see test_dataset_json_records_the_enrichment) if `ontology_source`
    # reaches the stage.
    result = run_fhir(
        mixed_raw_dir,
        out,
        stage_options(ontology_source=ontology_source),
        env={"SUMEDS_ONTOLOGY_SOURCE": str(out.parent / "no_such_source")},
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return out


@pytest.fixture(scope="module")
def codes(enriched_cohort: Path) -> dict[str, dict]:
    return {
        row["code"]: row for row in pl.read_parquet(enriched_cohort / "metadata" / "codes.parquet").to_dicts()
    }


def names(value) -> dict[str, str]:
    """A code-to-name map column value: a list of key/value structs on Polars 1, a dict on Polars 2."""
    return value if isinstance(value, dict) else {entry["key"]: entry["value"] for entry in value or []}


def test_codes_parquet_is_replaced_by_valid_meds_metadata(enriched_cohort: Path):
    metadata = enriched_cohort / "metadata"
    CodeMetadataSchema.validate(pq.read_table(metadata / "codes.parquet"))
    assert not list(metadata.glob(".codes.parquet.*")), "the temporary enrichment output was left behind"


def test_data_matches_the_plain_run(enriched_cohort: Path, plain_cohort: Path):
    def events(cohort: Path) -> pl.DataFrame:
        cols = ["subject_id", "time", "code", "numeric_value", "text_value"]
        shards = sorted((cohort / "data").rglob("*.parquet"))
        return pl.concat([pl.read_parquet(fp, columns=cols) for fp in shards]).sort(cols, nulls_last=False)

    assert events(enriched_cohort).equals(events(plain_cohort))


def test_enrichment_is_non_destructive(codes: dict[str, dict], plain_cohort: Path):
    """Every code of the plain run is kept, with its description, and its parent_codes (if any) start the new
    list."""
    plain = pl.read_parquet(plain_cohort / "metadata" / "codes.parquet")
    assert set(plain["code"]) == set(codes)
    for row in plain.to_dicts():
        new = codes[row["code"]]
        if row["description"] is not None:
            assert new["description"] == row["description"], row["code"]
        old_parents = row.get("parent_codes") or []
        assert (new["parent_codes"] or [])[: len(old_parents)] == old_parents, row["code"]


def test_icd_codes_get_titles_and_parents(codes: dict[str, dict]):
    # No FHIR display: the pack title fills the description. MIMIC's undotted codes are found dotted.
    aki = codes["Condition//ICD10CM//N179"]
    assert aki["description"] == pack_title("ICD10CM", "N17.9")
    assert aki["parent_codes"] == ["ICD10CM//N17.9", "ICD10CM//N17"]
    assert names(aki["parent_names"]) == {
        "ICD10CM//N17.9": pack_title("ICD10CM", "N17.9"),
        "ICD10CM//N17": pack_title("ICD10CM", "N17"),
    }
    assert codes["Condition//ICD9CM//5849"]["parent_codes"] == ["ICD9CM//584.9", "ICD9CM//584"]
    assert codes["Condition//ICD10CM//I10"]["parent_codes"] == ["ICD10CM//I10"]


def test_loinc_description_from_fhir_is_kept(codes: dict[str, dict]):
    bmi = codes["Observation//LOINC//39156-5"]
    assert bmi["description"] == "Body mass index (BMI) [Ratio]"  # the FHIR display, not the pack title
    assert bmi["parent_codes"] == ["LOINC//39156-5"]
    assert names(bmi["parent_names"]) == {"LOINC//39156-5": pack_title("LOINC", "39156-5")}


def test_other_codes_are_left_alone(codes: dict[str, dict]):
    """Codes outside the packs get no parents: MIMIC's local systems, LOINC codes missing from the pack, and
    SNOMED, RxNorm and CVX codes, which need an Athena download."""
    for code in (
        "Observation//D-LABITEMS//50912",
        "Observation//LOINC//8480-6",
        "Encounter//SNOMED//162673000",
        "Immunization//CVX//140",
    ):
        assert not codes[code]["parent_codes"], code
    assert not any(row["parent_codes"] for code, row in codes.items() if "//RXNORM//" in code)


def test_unknown_stage_options_fail_before_the_pipeline(mixed_raw_dir: Path, tmp_path: Path):
    """The spec's stage options are checked against the stage before any work (MEDS-Extract)."""
    spec = tmp_path / "spec.yaml"
    text = (Path(__file__).parents[1] / "src/fhir2meds/configs/messy.yaml").read_text()
    spec.write_text(
        text.replace("        rules: FHIR\n", "        rules: FHIR\n        no_such_option: 1\n", 1)
    )
    result = run_fhir(mixed_raw_dir, tmp_path / "out", spec=str(spec))
    assert result.returncode != 0
    assert "no_such_option" in result.stdout + result.stderr
    assert not (tmp_path / "out" / "fhir_pre_meds").exists()


def _dataset(cohort: Path) -> dict:
    """`metadata/dataset.json`, which must stay valid MEDS dataset metadata."""
    dataset = json.loads((cohort / "metadata" / "dataset.json").read_text())
    DatasetMetadataSchema.validate(dataset)
    return dataset


def _codes_bytes(cohort: Path) -> bytes:
    return (cohort / "metadata" / "codes.parquet").read_bytes()


def test_dataset_json_records_the_enrichment(enriched_cohort: Path, plain_cohort: Path, codes: dict):
    assert "sumeds_enrichment" not in _dataset(plain_cohort)
    record = _dataset(enriched_cohort)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "FHIR"
    assert record["ontology_source"] == "local"
    coverage = record["coverage"]
    assert coverage["codes"] == len(codes)
    assert coverage["described"][0] < coverage["described"][1]
    assert coverage["with_parent_codes"][0] < coverage["with_parent_codes"][1]


def test_skipped_enrichment_leaves_no_record_and_is_enriched_later(
    tmp_path: Path, plain_cohort: Path, enriched_cohort: Path, ontology_source: Path
):
    assert "sumeds_enrichment" not in _dataset(plain_cohort)
    assert not (plain_cohort / ".logs" / "sumeds_enrich.done").exists()
    # Without a record, `suMEDS-enrich OUTPUT_DIR` needs the rules.
    cohort = tmp_path / "plain"
    shutil.copytree(plain_cohort, cohort)
    assert enrich_main([str(cohort), "--rules", "FHIR", "--ontology-source", str(ontology_source)]) == 0
    assert pl.read_parquet(cohort / "metadata" / "codes.parquet").equals(
        pl.read_parquet(enriched_cohort / "metadata" / "codes.parquet")
    )
    assert _dataset(cohort)["sumeds_enrichment"]["enriched"] is True


def test_failed_enrichment_keeps_the_etl_output_and_is_enriched_later(
    mixed_raw_dir: Path,
    tmp_path: Path,
    ontology_source: Path,
    plain_cohort: Path,
    enriched_cohort: Path,
    monkeypatch,
):
    """No ontology data (as without internet): the run succeeds, unenriched, and says so."""
    monkeypatch.delenv("SUMEDS_ONTOLOGY_SOURCE")
    missing = tmp_path / "no_ontologies"
    cohort = tmp_path / "MEDS_cohort"
    result = run_fhir(mixed_raw_dir, cohort, stage_options(ontology_source=missing))
    assert result.returncode == 0, result.stderr[-3000:]
    # The warning, with the command to finish the enrichment, goes to the pipeline log.
    assert "enrichment failed" in pipeline_log(cohort)
    assert f"suMEDS-enrich {cohort}" in pipeline_log(cohort)
    assert _codes_bytes(cohort) == _codes_bytes(plain_cohort)
    record = _dataset(cohort)["sumeds_enrichment"]
    assert record["enriched"] is False
    assert record["rules"] == "FHIR"
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --ontology-source <PACKS_DIR>"
    assert str(missing) not in json.dumps(record)
    # The stage is done, so running the same command again does not retry it.
    assert (cohort / ".logs" / "sumeds_enrich.done").exists()

    # `suMEDS-enrich OUTPUT_DIR` reuses the recorded rules and options; the local pack source is a path,
    # so it is given again.
    assert enrich_main([str(cohort), "--ontology-source", str(ontology_source)]) == 0
    assert pl.read_parquet(cohort / "metadata" / "codes.parquet").equals(
        pl.read_parquet(enriched_cohort / "metadata" / "codes.parquet")
    )
    record = _dataset(cohort)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "FHIR"


def test_strict_enrichment_fails_the_run(mixed_raw_dir: Path, tmp_path: Path, plain_cohort: Path):
    missing = tmp_path / "no_ontologies"
    cohort = tmp_path / "MEDS_cohort"
    result = run_fhir(mixed_raw_dir, cohort, stage_options(strict="true", ontology_source=missing))
    assert result.returncode != 0
    assert "suMEDS-enrich: error:" in pipeline_log(cohort)
    assert _codes_bytes(cohort) == _codes_bytes(plain_cohort)
    assert "sumeds_enrichment" not in _dataset(cohort)
